from datetime import datetime, timedelta, timezone
from functools import partial
import logging

import koji

from .utils import format_date, parse_date
from .models import (FlatpakBuildModel,
                     ImageBuildModel, ImageModel,
                     ModuleBuildModel, PackageBuildModel)


logger = logging.getLogger(__name__)


# This is the maximum amount of time we'll ask Koji for all new images.
# If we haven't updated our image information for longer than this, then
# we'll go Flatpak by Flatpak.
ALL_IMAGES_MAX_INTERVAL = timedelta(days=1)


# When querying Koji for events that happened since we last queried, we
# allow a timestamp offset of this much
TIMESTAMP_FUZZ = timedelta(minutes=1)


def _get_build(koji_session, redis_client, build_info, build_cls):
    completion_time = datetime.fromtimestamp(build_info['completion_ts'], tz=timezone.utc)

    kwargs = dict(name=build_info['name'],
                  build_id=build_info['build_id'],
                  nvr=build_info['nvr'],
                  source=build_info['source'],
                  user_name=build_info['owner_name'],
                  completion_time=completion_time)

    if issubclass(build_cls,  ImageBuildModel):
        image_extra = build_info['extra']['image']
        if image_extra.get('flatpak', False):
            build_cls = FlatpakBuildModel

    if build_cls == ModuleBuildModel:
        kwargs['modulemd'] = build_info['extra']['typeinfo']['module']['modulemd_str']

    build = build_cls(**kwargs)

    if issubclass(build_cls,  ImageBuildModel):
        logger.info("Calling koji.listArchives(%s); nvr=%s",
                    build_info['build_id'], build_info['nvr'])
        archives = koji_session.listArchives(build_info['build_id'])

        for archive in archives:
            if build_cls == FlatpakBuildModel:
                if archive['extra']['image']['arch'] == 'x86_64':
                    # Archives should differ only in architecture,
                    # use the x86 build to get the package list
                    logger.info("Calling koji.listRPMs(%s)", archive['id'])
                    components = koji_session.listRPMs(imageID=archive['id'])

                    seen = set()
                    for c in components:
                        if c['build_id'] in seen:
                            continue
                        seen.add(c['build_id'])

                        build.package_builds.append(c['nvr'])

                        _query_package_build_by_id(koji_session, redis_client, c['build_id'])

            docker_info = archive['extra']['docker']
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

        if build_cls == FlatpakBuildModel:
            for m in build_info['extra']['image']['modules']:
                module_build = query_module_build(koji_session, redis_client, m)
                build.module_builds.append(module_build.nvr)

            build.module_builds.sort()
            build.package_builds.sort()

    elif build_cls == ModuleBuildModel:
        logger.info("Calling koji.listArchives(%s); nvr=%s",
                    build_info['build_id'], build_info['nvr'])
        archives = koji_session.listArchives(build_info['build_id'])
        # The RPM list for the 'modulemd.txt' archive has all the RPMs, recent
        # versions of MBS also write upload 'modulemd.<arch>.txt' archives with
        # architecture subsets.
        archives = [a for a in archives if a['filename'] == 'modulemd.txt']
        assert len(archives) == 1
        logger.info("Calling koji.listRPMs(%s)", archives[0]['id'])
        components = koji_session.listRPMs(imageID=archives[0]['id'])

        seen = set()
        for c in components:
            if c['build_id'] in seen:
                continue
            seen.add(c['build_id'])
            package_build = _query_package_build_by_id(koji_session, redis_client, c['build_id'])
            build.package_builds.append(package_build.nvr)

        build.package_builds.sort()

    redis_client.set('build:' + build.nvr, build.to_json_text())
    redis_client.hset('build-id-to-nvr', build.build_id, build.nvr)

    return build


def _query_flatpak_builds(koji_session, redis_client,
                          flatpak_name=None, include_only=None, complete_after=None):
    result = []

    kwargs = {
        'type': 'image',
        'state': koji.BUILD_STATES['COMPLETE']
    }

    if flatpak_name is not None:
        kwargs['packageID'] = get_package_id(koji_session, redis_client, flatpak_name)
    if complete_after is not None:
        kwargs['completeAfter'] = complete_after.replace(tzinfo=timezone.utc).timestamp()

    logger.info("Calling koji.listBuilds(%s)", kwargs)
    builds = koji_session.listBuilds(**kwargs)
    for build_info in builds:
        if include_only is not None and not build_info['name'] in include_only:
            continue

        if not redis_client.exists('build:' + build_info['nvr']):
            result.append(_get_build(koji_session, redis_client, build_info, FlatpakBuildModel))

    return result


def refresh_flatpak_builds(koji_session, redis_client, flatpaks):
    to_query = set(flatpaks)
    to_refresh = set()

    current_ts = datetime.utcnow().replace(tzinfo=timezone.utc)

    queried_ts = redis_client.hmget('build-cache:flatpak', *flatpaks)
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
        results += _query_flatpak_builds(koji_session, redis_client, include_only=to_refresh,
                                         complete_after=refresh_ts - TIMESTAMP_FUZZ)

    for flatpak_name in to_query:
        results += _query_flatpak_builds(koji_session, redis_client, flatpak_name=flatpak_name)

    with redis_client.pipeline() as pipe:
        pipe.multi()
        pipe.zadd('builds-by-entity:flatpak', {
            f"{n}:{v}-{r}": 0 for n, v, r in (b.nvr.rsplit('-', 2) for b in results)
        })

        formatted_current_ts = format_date(current_ts)
        redis_client.hset('build-cache:flatpak', mapping={
            n: formatted_current_ts for n in flatpaks
        })
        pipe.execute()


def list_flatpak_builds(redis_client, flatpak):
    matches = redis_client.zrangebylex('builds-by-entity:flatpak',
                                       '[' + flatpak + ':', '(' + flatpak + ';')

    def matches_to_keys():
        for m in matches:
            name, vr = m.decode("utf-8").split(":")
            yield 'build:' + name + '-' + vr

    return [FlatpakBuildModel.from_json_text(x)
            for x in redis_client.mget(matches_to_keys())]


def get_package_id(koji_session, redis_client, entity_name):
    package_id = redis_client.hget('entity-name-to-package-id', entity_name)
    if package_id:
        return int(package_id)

    logger.info("Calling koji.getPackageID(%s)", entity_name)
    package_id = koji_session.getPackageID(entity_name)
    if package_id is None:
        raise RuntimeError(f"Could not look up package ID for {entity_name}")

    redis_client.hset('entity-name-to-package-id', entity_name, package_id)

    return package_id


def _query_build(koji_session, redis_client, nvr, build_cls):
    raw = redis_client.get('build:' + nvr)
    if raw is not None:
        return build_cls.from_json_text(raw)

    logger.info("Calling koji.getBuild(%s)", nvr)
    build_info = koji_session.getBuild(nvr)
    if build_info is None:
        raise RuntimeError(f"Could not look up {nvr} in Koji")

    return _get_build(koji_session, redis_client, build_info, build_cls)


def _query_module_build_no_context(koji_session, redis_client, nvr):
    full_nvr = redis_client.hget('module-nvr-to-nvrc', nvr)
    if full_nvr:
        return _query_build(koji_session, redis_client, full_nvr.decode("utf-8"), ModuleBuildModel)

    n, v, r = nvr.rsplit('-', 2)

    package_id = get_package_id(koji_session, redis_client, n)
    logger.info("Calling koji.listBuilds(%s, type='module')", package_id)
    builds = koji_session.listBuilds(package_id, type='module')

    builds = [b for b in builds if b['nvr'].startswith(nvr)]
    if len(builds) == 0:
        raise RuntimeError(f"Could not look up {nvr} in Koji")
    elif len(builds) > 1:
        raise RuntimeError(f"More than one context for {nvr}!")

    module_build = _get_build(koji_session, redis_client, builds[0], ModuleBuildModel)
    redis_client.hset('module-nvr-to-nvrc', nvr, module_build.nvr)

    return module_build


def _query_package_build_by_id(koji_session, redis_client, build_id):
    nvr = redis_client.hget('build-id-to-nvr', build_id)
    if nvr:
        return query_package_build(koji_session, redis_client, nvr.decode("utf-8"))

    logger.info("Calling koji.getBuild(%s)", build_id)
    build_info = koji_session.getBuild(build_id)
    if build_info is None:
        raise RuntimeError(f"Could not look up build ID {build_id} in Koji")

    return _get_build(koji_session, redis_client, build_info, PackageBuildModel)


def query_image_build(koji_session, redis_client, nvr):
    return _query_build(koji_session, redis_client, nvr, ImageBuildModel)


def query_module_build(koji_session, redis_client, nvr):
    n, v, r = nvr.rsplit('-', 2)
    if '.' not in r:
        return _query_module_build_no_context(koji_session, redis_client, nvr)
    else:
        return _query_build(koji_session, redis_client, nvr, ModuleBuildModel)


def query_package_build(koji_session, redis_client, nvr):
    return _query_build(koji_session, redis_client, nvr, PackageBuildModel)


def _refresh_tag_builds(koji_session, tag, pipe):
    pipe.watch('tag-build-cache')

    latest_event_raw = pipe.hget('tag-build-cache', tag)
    latest_event = int(latest_event_raw) if latest_event_raw is not None else None

    old_keys = set(pipe.zrangebylex('builds-by-tag',
                                    '[' + tag + ':', '(' + tag + ';'))
    new_keys = set(old_keys)

    kwargs = {
        'tables': ['tag_listing'],
        'tag': tag,
    }
    if latest_event is not None:
        kwargs['afterEvent'] = latest_event

    logger.info("Calling koji.queryHistory(%s)", kwargs)
    result = koji_session.queryHistory(**kwargs)['tag_listing']
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
        pipe.zrem('builds-by-tag', *to_remove)
    to_add = new_keys - old_keys
    if to_add:
        pipe.zadd('builds-by-tag', {key: 0 for key in to_add})
    pipe.hset('tag-build-cache', tag, latest_event)
    pipe.execute()


def refresh_tag_builds(koji_session, redis_client, tag):
    redis_client.transaction(partial(_refresh_tag_builds, koji_session, tag))


def query_tag_builds(redis_client, tag, entity_name):
    key = tag + ':' + entity_name
    matches = redis_client.zrangebylex('builds-by-tag',
                                       '[' + key + ':', '(' + key + ';')

    def matches_to_nvr():
        for m in matches:
            _, name, vr = m.decode("utf-8").split(":")
            yield name + '-' + vr

    return list(matches_to_nvr())
