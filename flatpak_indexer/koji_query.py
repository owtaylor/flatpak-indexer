from datetime import datetime, timedelta, timezone
from functools import partial
import logging
from typing import cast, List, TypeVar

import koji

from .models import (BinaryPackage, FlatpakBuildModel,
                     ImageBuildModel, ImageModel, KojiBuildModel,
                     ModuleBuildModel, PackageBuildModel)
from .session import Session
from .utils import format_date, parse_date


logger = logging.getLogger(__name__)


# This is the maximum amount of time we'll ask Koji for all new images.
# If we haven't updated our image information for longer than this, then
# we'll go Flatpak by Flatpak.
ALL_IMAGES_MAX_INTERVAL = timedelta(days=1)


# When querying Koji for events that happened since we last queried, we
# allow a timestamp offset of this much
TIMESTAMP_FUZZ = timedelta(minutes=1)


# build:<nvr> - json representation of the build
KEY_PREFIX_BUILD = 'build:'
# hash of <build id> => <nvr>
KEY_BUILD_ID_TO_NVR = 'build-id-to-nvr'
# hash of <flatpak name> => last queried timestamp string (as per utils.format_date())
# all builds older than this query time will be in build-by-entity:flatpak
KEY_BUILD_CACHE_FLATPAK = 'build-cache:flatpak'
# sorted set of <n>:<v>-<r> of flatpaks (scores are all zero, lexical ordering used)
# build:<nvr> keys are created along with these, so will typically exist, though
# the code will backfill as necessary for missing keys or ones invalidated by
# schema evolution
KEY_BUILDS_BY_ENTITY_FLATPAK = 'builds-by-entity:flatpak'
# sorted set of <tag>:<n>:<v>-<r> with the latest builds tagged into <tag>
# build:<nvr> keys do *not* necessarily exist for these builds
KEY_BUILDS_BY_TAG = 'builds-by-tag'
# hash of <package/module/flatpak name> => koji package ID
KEY_ENTITY_NAME_TO_PACKAGE_ID = 'entity-name-to-package-id'
# hash from old-style module NVR (<name>-<stream>-<version>) to newer
# <name>-<stream>-<version>.context
KEY_MODULE_NVR_TO_NVRC = 'module-nvr-to-nvrc'
# hash of <tag> => event id. This is the koji event ID corresponding to the current
# state of <tag> in builds-by-tag
KEY_TAG_BUILD_CACHE = 'tag-build-cache'


B = TypeVar("B", bound="KojiBuildModel")


def _get_build(session: Session, build_info, build_cls: type[B]) -> B:
    completion_time = datetime.fromtimestamp(build_info['completion_ts'], tz=timezone.utc)

    kwargs = dict(name=build_info['name'],
                  build_id=build_info['build_id'],
                  nvr=build_info['nvr'],
                  source=build_info['source'],
                  user_name=build_info['owner_name'],
                  completion_time=completion_time)

    is_flatpak = False
    if build_cls == ImageBuildModel:
        extra = build_info.get('extra')
        if extra:
            image_extra = extra.get('image')
            if image_extra and image_extra.get('flatpak', False):
                is_flatpak = True
    elif build_cls == ModuleBuildModel:
        kwargs['modulemd'] = build_info['extra']['typeinfo']['module']['modulemd_str']

    if is_flatpak:
        build = cast(B, FlatpakBuildModel(**kwargs))
    else:
        build = build_cls(**kwargs)

    if isinstance(build, ImageBuildModel):
        logger.info("Calling koji.listArchives(%s); nvr=%s",
                    build_info['build_id'], build_info['nvr'])
        archives = session.koji_session.listArchives(build_info['build_id'])

        for archive in archives:
            if isinstance(build, FlatpakBuildModel):
                if archive['extra']['image']['arch'] == 'x86_64':
                    # Archives should differ only in architecture,
                    # use the x86 build to get the package list
                    logger.info("Calling koji.listRPMs(%s)", archive['id'])
                    components = session.koji_session.listRPMs(imageID=archive['id'])

                    seen = set()
                    for c in components:
                        if c['build_id'] in seen:
                            continue
                        seen.add(c['build_id'])

                        package_build = _query_package_build_by_id(
                            session, c['build_id']
                        )
                        build.package_builds.append(
                            BinaryPackage(nvr=c['nvr'], source_nvr=package_build.nvr)
                        )

            docker_info = None
            archive_extra = archive.get('extra')
            if archive_extra:
                docker_info = archive_extra.get('docker')
            if not docker_info:
                continue

            config = docker_info['config']
            digests = docker_info['digests']

            for media_type in ('application/vnd.oci.image.manifest.v1+json',
                               'application/vnd.docker.distribution.manifest.v2+json'):
                digest = digests.get(media_type)
                if digest:
                    break
            else:
                raise RuntimeError("Can't find OCI or docker digest in image")

            pull_spec = docker_info['repositories'][0]

            # Now make the image
            build.images.append(ImageModel(digest=digest,
                                           media_type=media_type,
                                           os=config['os'],
                                           architecture=config['architecture'],
                                           labels=config['config'].get('Labels', {}),
                                           diff_ids=config['rootfs']['diff_ids'],
                                           pull_spec=pull_spec))

        if isinstance(build, FlatpakBuildModel):
            for m in build_info['extra']['image']['modules']:
                module_build = query_module_build(session, m)
                build.module_builds.append(module_build.nvr)

            build.module_builds.sort()
            build.package_builds.sort(key=lambda pb: pb.nvr)

    elif isinstance(build, ModuleBuildModel):
        logger.info("Calling koji.listArchives(%s); nvr=%s",
                    build_info['build_id'], build_info['nvr'])
        archives = session.koji_session.listArchives(build_info['build_id'])
        # The RPM list for the 'modulemd.txt' archive has all the RPMs, recent
        # versions of MBS also write upload 'modulemd.<arch>.txt' archives with
        # architecture subsets.
        archives = [a for a in archives if a['filename'] == 'modulemd.txt']
        assert len(archives) == 1
        logger.info("Calling koji.listRPMs(%s)", archives[0]['id'])
        components = session.koji_session.listRPMs(imageID=archives[0]['id'])

        seen = set()
        for c in components:
            if c['build_id'] in seen:
                continue
            seen.add(c['build_id'])
            package_build = _query_package_build_by_id(session, c['build_id'])
            build.package_builds.append(BinaryPackage(nvr=c['nvr'], source_nvr=package_build.nvr))

        build.package_builds.sort(key=lambda pb: pb.nvr)

    session.redis_client.set(KEY_PREFIX_BUILD + build.nvr, build.to_json_text())
    session.redis_client.hset(KEY_BUILD_ID_TO_NVR, build.build_id, build.nvr)

    return build


def _query_flatpak_builds(
    session: Session, flatpak_name=None, include_only=None, complete_after=None
):
    result = []

    kwargs = {
        'type': 'image',
        'state': koji.BUILD_STATES['COMPLETE']
    }

    if flatpak_name is not None:
        kwargs['packageID'] = get_package_id(session, flatpak_name)
    if complete_after is not None:
        kwargs['completeAfter'] = complete_after.replace(tzinfo=timezone.utc).timestamp()

    logger.info("Calling koji.listBuilds(%s)", kwargs)
    builds = session.koji_session.listBuilds(**kwargs)
    for build_info in builds:
        if include_only is not None and not build_info['name'] in include_only:
            continue

        if not session.redis_client.exists(KEY_PREFIX_BUILD + build_info['nvr']):
            result.append(_get_build(session, build_info, FlatpakBuildModel))

    return result


def refresh_flatpak_builds(session: Session, flatpaks):
    to_query = set(flatpaks)
    to_refresh = set()

    current_ts = datetime.utcnow().replace(tzinfo=timezone.utc)

    queried_ts = session.redis_client.hmget(KEY_BUILD_CACHE_FLATPAK, *flatpaks)
    parsed_ts = [parse_date(ts.decode("utf-8")) if ts else None for ts in queried_ts]

    refresh_ts = max((ts for ts in parsed_ts if ts is not None), default=None)
    if refresh_ts is not None:
        if current_ts - refresh_ts < ALL_IMAGES_MAX_INTERVAL:
            for flatpak_name, ts in zip(flatpaks, parsed_ts):
                if ts == refresh_ts:
                    to_refresh.add(flatpak_name)
                    to_query.discard(flatpak_name)

    results = []
    if len(to_refresh) > 0:
        results += _query_flatpak_builds(session, include_only=to_refresh,
                                         complete_after=refresh_ts - TIMESTAMP_FUZZ)

    for flatpak_name in to_query:
        results += _query_flatpak_builds(session, flatpak_name=flatpak_name)

    with session.redis_client.pipeline() as pipe:
        pipe.multi()
        if results:
            pipe.zadd(KEY_BUILDS_BY_ENTITY_FLATPAK, {
                f"{n}:{v}-{r}": 0 for n, v, r in (b.nvr.rsplit('-', 2) for b in results)
            })

        formatted_current_ts = format_date(current_ts)
        pipe.hset(KEY_BUILD_CACHE_FLATPAK, mapping={
            n: formatted_current_ts for n in flatpaks
        })
        pipe.execute()


def list_flatpak_builds(session: Session, flatpak: str) -> List[FlatpakBuildModel]:
    matches = session.redis_client.zrangebylex(
        KEY_BUILDS_BY_ENTITY_FLATPAK, '[' + flatpak + ':', '(' + flatpak + ';'
    )

    def matches_to_keys():
        for m in matches:
            name, vr = m.decode("utf-8").split(":")
            yield KEY_PREFIX_BUILD + name + '-' + vr

    result_json = session.redis_client.mget(matches_to_keys())
    result = []
    for i, item_json in enumerate(result_json):
        if item_json:
            flatpak_build = FlatpakBuildModel.from_json_text(item_json, check_current=True)
        else:
            flatpak_build = None

        if flatpak_build:
            result.append(flatpak_build)
        else:
            name, vr = matches[i].decode("utf-8").split(":")
            image_build = query_image_build(session, name + '-' + vr)
            assert isinstance(image_build, FlatpakBuildModel)
            result.append(image_build)

    return result


def get_package_id(session: Session, entity_name):
    package_id = session.redis_client.hget(KEY_ENTITY_NAME_TO_PACKAGE_ID, entity_name)
    if package_id:
        return int(package_id)

    logger.info("Calling koji.getPackageID(%s)", entity_name)
    package_id = session.koji_session.getPackageID(entity_name)
    if package_id is None:
        raise RuntimeError(f"Could not look up package ID for {entity_name}")

    session.redis_client.hset(KEY_ENTITY_NAME_TO_PACKAGE_ID, entity_name, package_id)

    return package_id


def _query_build(session: Session, nvr, build_cls: type[B]) -> B:
    raw = session.redis_client.get(KEY_PREFIX_BUILD + nvr)
    if raw:
        build = build_cls.from_json_text(raw, check_current=True)
        if build:
            return build

    logger.info("Calling koji.getBuild(%s)", nvr)
    build_info = session.koji_session.getBuild(nvr)
    if build_info is None:
        raise RuntimeError(f"Could not look up {nvr} in Koji")

    return _get_build(session, build_info, build_cls)


def _query_module_build_no_context(session: Session, nvr):
    full_nvr = session.redis_client.hget(KEY_MODULE_NVR_TO_NVRC, nvr)
    if full_nvr:
        return _query_build(session, full_nvr.decode("utf-8"), ModuleBuildModel)

    n, v, r = nvr.rsplit('-', 2)

    package_id = get_package_id(session, n)
    logger.info("Calling koji.listBuilds(%s, type='module')", package_id)
    builds = session.koji_session.listBuilds(package_id, type='module')

    builds = [b for b in builds if b['nvr'].startswith(nvr)]
    if len(builds) == 0:
        raise RuntimeError(f"Could not look up {nvr} in Koji")
    elif len(builds) > 1:
        # If a user builds the same git commit of a module with different platform buildrequires
        # (8.4.0 and 8.4.0-z, for example), then we can end up with multiple contexts.
        # The guess here corresponds to what ODCS will do (use the most recent), but the
        # better thing would be to avoid this situation and always use the full NSVC to
        # look up a build.
        logger.warning(f"More than one context for {nvr}, using most recent!")
        builds.sort(key=lambda b: b['creation_ts'], reverse=True)

    module_build = _get_build(session, builds[0], ModuleBuildModel)
    session.redis_client.hset(KEY_MODULE_NVR_TO_NVRC, nvr, module_build.nvr)

    return module_build


def _query_package_build_by_id(session: Session, build_id):
    nvr = session.redis_client.hget(KEY_BUILD_ID_TO_NVR, build_id)
    if nvr:
        return query_package_build(session, nvr.decode("utf-8"))

    logger.info("Calling koji.getBuild(%s)", build_id)
    build_info = session.koji_session.getBuild(build_id)
    if build_info is None:
        raise RuntimeError(f"Could not look up build ID {build_id} in Koji")

    return _get_build(session, build_info, PackageBuildModel)


def query_image_build(session: Session, nvr):
    return _query_build(session, nvr, ImageBuildModel)


def query_module_build(session: Session, nvr) -> ModuleBuildModel:
    n, v, r = nvr.rsplit('-', 2)
    if '.' not in r:
        return _query_module_build_no_context(session, nvr)
    else:
        return _query_build(session, nvr, ModuleBuildModel)


def query_package_build(session: Session, nvr) -> PackageBuildModel:
    return _query_build(session, nvr, PackageBuildModel)


def _refresh_tag_builds(session: Session, tag, pipe):
    pipe.watch(KEY_TAG_BUILD_CACHE)

    latest_event_raw = pipe.hget(KEY_TAG_BUILD_CACHE, tag)
    latest_event = int(latest_event_raw) if latest_event_raw is not None else None

    old_keys = set(pipe.zrangebylex(KEY_BUILDS_BY_TAG,
                                    '[' + tag + ':', '(' + tag + ';'))
    new_keys = set(old_keys)

    kwargs = {
        'tables': ['tag_listing'],
        'tag': tag,
    }
    if latest_event is not None:
        kwargs['afterEvent'] = latest_event

    logger.info("Calling koji.queryHistory(%s)", kwargs)
    result = session.koji_session.queryHistory(**kwargs)['tag_listing']
    for r in result:
        create_event = r.get('create_event', None)
        revoke_event = r.get('revoke_event', None)

        if latest_event is None or create_event > latest_event:
            latest_event = create_event
        if latest_event is None or (revoke_event and revoke_event > latest_event):
            latest_event = revoke_event

        key = (tag + ":" + r['name'] + ":" + r['version'] + '-' + r['release']).encode("utf-8")
        if revoke_event:
            new_keys.discard(key)
        else:
            new_keys.add(key)

    pipe.multi()
    to_remove = old_keys - new_keys
    if to_remove:
        pipe.zrem(KEY_BUILDS_BY_TAG, *to_remove)
    to_add = new_keys - old_keys
    if to_add:
        pipe.zadd(KEY_BUILDS_BY_TAG, {key: 0 for key in to_add})
    if latest_event:
        pipe.hset(KEY_TAG_BUILD_CACHE, tag, latest_event)
    pipe.execute()


def refresh_tag_builds(session: Session, tag):
    session.redis_client.transaction(partial(_refresh_tag_builds, session, tag))


def query_tag_builds(session: Session, tag, entity_name):
    key = tag + ':' + entity_name
    matches = session.redis_client.zrangebylex(
        KEY_BUILDS_BY_TAG, '[' + key + ':', '(' + key + ';'
    )

    def matches_to_nvr():
        for m in matches:
            _, name, vr = m.decode("utf-8").split(":")
            yield name + '-' + vr

    return list(matches_to_nvr())
