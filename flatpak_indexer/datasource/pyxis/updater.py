import koji
import logging
import json
import os
import re

from ...utils import atomic_writer, get_retrying_requests_session, parse_date
from ...models import ImageModel, RegistryModel


logger = logging.getLogger(__name__)

MEDIA_TYPE_MANIFEST_V2 = 'application/vnd.docker.distribution.manifest.v2+json'


class Registry:
    def __init__(self, name, global_config, page_size):
        self.name = name
        self.global_config = global_config
        self.config = global_config.registries[name]
        self.page_size = page_size
        self.tag_indexes = []
        self.koji_indexes = {}
        self.registry = RegistryModel()

    def make_image(self, name, image_info, all_tags, digest):
        arch = image_info['architecture']
        os = image_info['parsed_data']['os']

        labels = {label['name']: label['value']
                  for label in image_info['parsed_data'].get('labels', [])}

        return ImageModel(digest=digest,
                          media_type=MEDIA_TYPE_MANIFEST_V2,
                          os=os,
                          architecture=arch,
                          annotations={},
                          labels=labels,
                          tags=all_tags)

    def add_image(self, name, image_info, all_tags, digest=None):
        image = self.make_image(name, image_info, all_tags, digest)
        self.registry.add_image(name, image)

    def add_index(self, index_config):
        if index_config.koji_tag:
            indexes = self.koji_indexes.setdefault(index_config.koji_tag, [])
            indexes.append(index_config)
        else:
            self.tag_indexes.append(index_config)

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
        options = koji.read_config(profile_name=self.global_config.koji_config)
        koji_session_opts = koji.grab_session_options(options)
        koji_session = koji.ClientSession(options['server'], koji_session_opts)

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

        nvr_to_arch_digest_cache = {}

        for (tag_name, arch), (_, image_info, repository_info) in image_by_tag_arch.items():
            nvr = image_info["brew"]["build"]
            arch_digest_map = nvr_to_arch_digest_cache.get(nvr)
            if arch_digest_map is None:
                build_id = koji_session.getBuild(nvr)['build_id']
                arch_digest_map = self._get_arch_digest_map(koji_session, build_id)
                nvr_to_arch_digest_cache[nvr] = arch_digest_map

            all_tags = sorted({tag["name"] for tag in repository_info['tags']})

            yield tag_name, arch, image_info, all_tags, arch_digest_map[arch]

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
            for tag_name, arch, image_info, all_tags, digest in \
                    self._iterate_repository_images(repository, desired_tags):

                yield repository, tag_name, arch, image_info, all_tags, digest

    def _iterate_images_for_nvr(self, session, nvr):
        logger.info("Getting images for {}".format(nvr))

        url = '{api_url}images/nvr/{nvr}'.format(api_url=self.global_config.pyxis_url,
                                                 nvr=nvr)

        yield from self._do_iterate_pyxis_results(session, url)

    def _get_arch_digest_map(self, koji_session, build_id):
        # The data that Pyxis returns doesn't actually contain the manifest digest
        # of that image that would be necessary to look it up in the registry.
        # To work around this, we go back to Koji and use the metadata stored
        # there to find the correct manifest digest.
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
        options = koji.read_config(profile_name=self.global_config.koji_config)
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

    def write(self):
        filename = os.path.join(self.global_config.work_dir, self.config.name + ".json")
        with atomic_writer(filename) as writer:
            json.dump(self.registry.to_json(),
                      writer, sort_keys=True, indent=4, ensure_ascii=False)


class PyxisUpdater(object):
    def __init__(self, config, page_size=50):
        self.conf = config
        self.page_size = page_size

    def start(self):
        pass

    def update(self):
        registries = {}
        for index_config in self.conf.indexes:
            registry_name = index_config.registry
            if self.conf.registries[registry_name].datasource != 'pyxis':
                continue

            if registry_name not in registries:
                registries[registry_name] = Registry(registry_name,
                                                     self.conf,
                                                     self.page_size)

            registry = registries[registry_name]
            registry.add_index(index_config)

        for registry in registries.values():
            desired_tags = {index_config.tag for index in registry.tag_indexes}

            if len(registry.tag_indexes) > 0:
                for repository, tag_name, arch, image_info, all_tags, digest in \
                        registry.iterate_images(desired_tags):

                    for index_config in registry.tag_indexes:
                        if (tag_name == index_config.tag and
                            (index_config.architecture is None or
                             arch == index_config.architecture)):

                            registry.add_image(repository, image_info, all_tags, digest=digest)
                            break

            for koji_tag, indexes in registry.koji_indexes.items():
                for repository, tag_name, arch, image_info, all_tags, digest in \
                        registry.iterate_koji_images(koji_tag):

                    for index_config in indexes:
                        if index_config.architecture is None or \
                               arch == index_config.architecture:
                            registry.add_image(repository, image_info, all_tags, digest=digest)
                            break

            registry.write()

    def stop(self):
        pass
