import codecs
import base64
from datetime import datetime, timezone
import hashlib
import koji
import logging
import json
import os
import re
from tempfile import NamedTemporaryFile
from urllib.parse import urljoin

from .utils import get_retrying_requests_session


logger = logging.getLogger(__name__)

MEDIA_TYPE_MANIFEST_V2 = 'application/vnd.docker.distribution.manifest.v2+json'

DATA_URI_PREFIX = 'data:image/png;base64,'


class IconStore(object):
    def __init__(self, icons_dir, icons_uri):
        self.icons_dir = icons_dir
        self.icons_uri = icons_uri

        self.old_icons = {}
        for f in os.listdir(icons_dir):
            fullpath = os.path.join(icons_dir, f)
            if os.path.isdir(fullpath):
                for filename in os.listdir(fullpath):
                    self.old_icons[(f, filename)] = True

        self.icons = {}

    def store(self, uri):
        if not uri.startswith(DATA_URI_PREFIX):
            return None

        decoded = base64.b64decode(uri[len(DATA_URI_PREFIX):])

        h = hashlib.sha256()
        h.update(decoded)
        digest = h.hexdigest()
        subdir = digest[:2]
        filename = digest[2:] + '.png'

        key = (subdir, filename)
        if key in self.icons:
            pass
        elif key in self.old_icons:
            self.icons[key] = True
        else:
            if not os.path.exists(os.path.join(self.icons_dir, subdir)):
                os.mkdir(os.path.join(self.icons_dir, subdir))
            fullpath = os.path.join(self.icons_dir, subdir, filename)
            logger.info("Storing icon: %s", fullpath)
            with open(os.path.join(self.icons_dir, subdir, filename), 'wb') as f:
                f.write(decoded)
            self.icons[key] = True

        return urljoin(self.icons_uri, subdir + '/' + filename)

    def clean(self):
        for key in self.old_icons:
            if key not in self.icons:
                subdir, filename = key
                fullpath = os.path.join(self.icons_dir, subdir, filename)
                os.unlink(fullpath)
                logger.info("Removing icon: %s", fullpath)


class Index:
    def __init__(self, conf, registry_config, icon_store=None):
        self.registry_config = registry_config
        self.config = conf
        self.icon_store = icon_store
        self.repos = {}

    def extract_icon(self, labels, key):
        if not self.config.extract_icons:
            return

        value = labels.get(key)
        if value is None:
            return

        uri = self.icon_store.store(value)
        if uri is not None:
            labels[key] = uri

    def make_image(self, name, image_info, all_tags, digest):
        arch = image_info['architecture']
        os = image_info['parsed_data']['os']

        labels = {label['name']: label['value']
                  for label in image_info['parsed_data'].get('labels', [])}

        if self.registry_config.force_flatpak_token:
            # This string the base64-encoding GVariant holding a variant
            # holding the int32 1.
            labels['org.flatpak.commit-metadata.xa.token-type'] = 'AQAAAABp'

        self.extract_icon(labels, 'org.freedesktop.appstream.icon-64')
        self.extract_icon(labels, 'org.freedesktop.appstream.icon-128')

        if digest is None:
            digest = image_info['docker_image_id']

        image = {
            'Digest': digest,
            'MediaType': MEDIA_TYPE_MANIFEST_V2,
            'OS': os,
            'Architecture':  arch,
            'Labels': labels,
        }

        image['Tags'] = all_tags

        return image

    def add_image(self, name, image_info, all_tags, digest=None):
        if name not in self.repos:
            self.repos[name] = {
                "Name": name,
                "Images": [],
                "Lists": [],
            }

        repo = self.repos[name]

        image = self.make_image(name, image_info, all_tags, digest)
        if image:
            repo["Images"].append(image)

    def write(self):
        # We auto-create only one level and don't use os.makedirs,
        # to better catch configuration mistakes
        output_dir = os.path.dirname(self.config.output)
        if not os.path.isdir(output_dir):
            os.mkdir(output_dir)

        tmpfile = NamedTemporaryFile(delete=False,
                                     dir=output_dir,
                                     prefix=os.path.basename(self.config.output))
        success = False
        try:
            filtered_repos = (v for v in self.repos.values() if v['Images'] or v['Lists'])
            sorted_repos = sorted(filtered_repos, key=lambda r: r['Name'])
            for repo in sorted_repos:
                repo["Images"].sort(key=lambda x: x["Tags"])
                repo["Lists"].sort(key=lambda x: x["Tags"])

            writer = codecs.getwriter("utf-8")(tmpfile)
            json.dump({
                'Registry': self.registry_config.public_url,
                'Results': sorted_repos,
            }, writer, sort_keys=True, indent=4, ensure_ascii=False)
            writer.close()
            tmpfile.close()

            # We don't overwrite unchanged files, so that the modtime and
            # httpd-computed ETag stay the same.

            changed = True
            if os.path.exists(self.config.output):
                h1 = hashlib.sha256()
                with open(self.config.output, "rb") as f:
                    h1.update(f.read())
                h2 = hashlib.sha256()
                with open(tmpfile.name, "rb") as f:
                    h2.update(f.read())

                if h1.digest() == h2.digest():
                    changed = False

            if changed:
                # Atomically write over result
                os.chmod(tmpfile.name, 0o644)
                os.rename(tmpfile.name, self.config.output)
                logger.info("Wrote %s", self.config.output)
            else:
                logger.info("%s is unchanged", self.config.output)
                os.unlink(tmpfile.name)

            success = True
        finally:
            if not success:
                tmpfile.close()
                os.unlink(tmpfile.name)


class Registry:
    def __init__(self, name, global_config, page_size):
        self.name = name
        self.global_config = global_config
        self.config = global_config.registries[name]
        self.page_size = page_size
        self.tag_indexes = []
        self.koji_indexes = {}

    def add_index(self, index):
        if index.config.koji_tag:
            indexes = self.koji_indexes.setdefault(index.config.koji_tag, [])
            indexes.append(index)
        else:
            self.tag_indexes.append(index)

    def _do_iterate_pyxis_results(self, session, url):
        page_size = self.page_size
        page = 0
        while True:
            sep = '&' if '?' in url else '?'
            paginated_url = url + sep + 'page_size={page_size}&page={page}'.format(
                page_size=page_size,
                page=page)
            logger.info("Requesting {}".format(paginated_url))

            kwargs = {
            }

            if self.global_config.pyxis_cert is None:
                kwargs['verify'] = True
            else:
                kwargs['verify'] = self.global_config.pyxis_cert

            if self.global_config.pyxis_client_cert:
                kwargs['cert'] = (self.global_config.pyxis_client_cert,
                                  self.global_config.pyxis_client_key)

            response = session.get(paginated_url, headers={'Accept': 'application/json'}, **kwargs)
            response.raise_for_status()

            response_json = response.json()

            for item in response_json['data']:
                yield item

            if response_json['total'] <= page_size * page + len(response_json['data']):
                break

            page += 1

    def _iterate_all_repository_images(self, repository):
        logger.info("Getting all images for {}/{}".format(self.name, repository))

        session = get_retrying_requests_session()

        url = '{api_url}repositories/registry/{registry}/repository/{repository}/images'.format(
            api_url=self.global_config.pyxis_url,
            registry=self.name,
            repository=repository)

        yield from self._do_iterate_pyxis_results(session, url)

    def _iterate_repository_images(self, repository, desired_tags):
        image_by_tag_arch = {}

        for image_info in self._iterate_all_repository_images(repository):
            arch = image_info['architecture']

            repository_info = None
            for ri in image_info['repositories']:
                if ri['repository'] == repository and \
                   ri['registry'] == self.name:
                    repository_info = ri
                    break

            if repository_info:
                for tag in repository_info['tags']:
                    tag_name = tag['name']
                    tag_date = parse_date(tag['added_date'])
                    if tag_name in desired_tags:
                        key = tag_name, arch
                        info = image_by_tag_arch.get(key)
                        if info is None or tag_date > info[0]:
                            image_by_tag_arch[key] = (tag_date,
                                                      image_info,
                                                      repository_info)

        for (tag_name, arch), (_, image_info, repository_info) in image_by_tag_arch.items():
            all_tags = sorted({tag["name"] for tag in repository_info['tags']})

            yield tag_name, arch, image_info, all_tags

    def _iterate_repositories(self):
        if self.config.repositories:
            yield from self.config.repositories
            return

        session = get_retrying_requests_session()
        url = '{api_url}repositories?image_usage_type=Flatpak'.format(
            api_url=self.global_config.pyxis_url)

        for item in self._do_iterate_pyxis_results(session, url):
            if item['registry'] == self.name:
                yield item['repository']

    def iterate_images(self, desired_tags):
        for repository in self._iterate_repositories():
            for tag_name, arch, image_info, all_tags in \
                    self._iterate_repository_images(repository, desired_tags):

                yield repository, tag_name, arch, image_info, all_tags

    def _iterate_images_for_nvr(self, session, nvr):
        logger.info("Getting images for {}".format(nvr))

        url = '{api_url}images/nvr/{nvr}'.format(api_url=self.global_config.pyxis_url,
                                                 nvr=nvr)

        yield from self._do_iterate_pyxis_results(session, url)

    def _get_arch_digest_map(self, koji_session, build_id):
        # This is a workaround for an atomic-reactor bug where, when an image is
        # converted from OCI to Docker at upload, the 'id' in the Koji metadata
        # refers to the hash of the original image, not the converted hash.
        # The digest gets imported that way into Pyxis, but at least when we
        # are reading data from koji, we can find the right digest for each
        # architecture.
        arch_digest_map = {}

        for archive_info in koji_session.listArchives(build_id):
            docker_info = archive_info.get('extra', {}).get('docker')
            if docker_info:
                arch = docker_info['config']['architecture']
                digests = docker_info['digests']

                digest = digests.get('application/vnd.oci.image.manifest.v1+json')
                if digest is None:
                    digest = digests['application/vnd.docker.distribution.manifest.v2+json']

                arch_digest_map[arch] = digest

        return arch_digest_map

    def _iterate_nvrs(self, koji_tag):
        options = koji.read_config(profile_name=self.config.koji_config)
        koji_session_opts = koji.grab_session_options(options)
        koji_session = koji.ClientSession(options['server'], koji_session_opts)

        tagged_builds = koji_session.listTagged(koji_tag, type='image', latest=True)
        for tagged_build in tagged_builds:
            build_id = tagged_build['build_id']
            build = koji_session.getBuild(build_id)
            image_extra = build['extra']['image']
            is_flatpak = image_extra.get('flatpak', False)
            if is_flatpak:
                pull_specs = image_extra['index']['pull']
                # All the pull specs should have the same repository,
                # so which one we use is arbitrary
                base, tag = re.compile(r'[:@]').split(pull_specs[0], 1)
                _, repository = base.split('/', 1)

                arch_digest_map = self._get_arch_digest_map(koji_session, build_id)
                all_tags = [tag['name'] for tag in koji_session.listTags(build_id)]

                yield build['nvr'], repository, all_tags, arch_digest_map

    def iterate_koji_images(self, koji_tag):
        session = get_retrying_requests_session()

        for nvr, repository, all_tags, arch_digest_map in self._iterate_nvrs(koji_tag):
            for image_info in self._iterate_images_for_nvr(session, nvr):
                arch = image_info['architecture']
                yield repository, koji_tag, arch, image_info, all_tags, arch_digest_map[arch]


def parse_date(date_str):
    dt = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f+00:00')
    dt.replace(tzinfo=timezone.utc)

    return dt


class Indexer(object):
    def __init__(self, config, page_size=50):
        self.conf = config
        self.page_size = page_size

    def index(self):
        icon_store = None
        if self.conf.icons_dir is not None:
            icon_store = IconStore(self.conf.icons_dir, self.conf.icons_uri)

        registries = {}
        for index_config in self.conf.indexes:
            registry_name = index_config.registry
            if registry_name not in registries:
                registries[registry_name] = Registry(registry_name,
                                                     self.conf,
                                                     self.page_size)

            registry = registries[registry_name]

            index = Index(index_config,
                          registry.config,
                          icon_store=icon_store)
            registry.add_index(index)

        for registry in registries.values():
            desired_tags = {index.config.tag for index in registry.tag_indexes}

            if len(registry.tag_indexes) > 0:
                for repository, tag_name, arch, image_info, all_tags in \
                        registry.iterate_images(desired_tags):

                    for index in registry.tag_indexes:
                        if (tag_name == index.config.tag and
                            (index.config.architecture is None or
                             arch == index.config.architecture)):

                            index.add_image(repository, image_info, all_tags)

            for index in registry.tag_indexes:
                index.write()

            for koji_tag, indexes in registry.koji_indexes.items():
                for repository, tag_name, arch, image_info, all_tags, digest in \
                        registry.iterate_koji_images(koji_tag):

                    for index in indexes:
                        if index.config.architecture is None or \
                               arch == index.config.architecture:
                            index.add_image(repository, image_info, all_tags, digest=digest)

                for index in indexes:
                    index.write()

        if icon_store is not None:
            icon_store.clean()
