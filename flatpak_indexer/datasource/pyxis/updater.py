from collections import defaultdict
from datetime import datetime, timezone
import logging

import koji
import requests

from ...koji_query import query_image_build
from ...models import (FlatpakBuildModel, RegistryModel,
                       TagHistoryItemModel, TagHistoryModel)
from ...redis_utils import get_redis_client
from ...utils import get_retrying_requests_session, parse_date, rpm_nvr_compare


logger = logging.getLogger(__name__)

MEDIA_TYPE_MANIFEST_V2 = 'application/vnd.docker.distribution.manifest.v2+json'


class Registry:
    def __init__(self, name, global_config, page_size):
        self.name = name
        self.global_config = global_config
        self.config = global_config.registries[name]
        self.page_size = page_size
        self.tag_indexes = []
        self.koji_indexes = []
        self.registry = RegistryModel()

        self.session = get_retrying_requests_session()

        options = koji.read_config(profile_name=global_config.koji_config)
        koji_session_opts = koji.grab_session_options(options)
        self.koji_session = koji.ClientSession(options['server'], koji_session_opts)

        self.redis_client = get_redis_client(global_config)

    def add_index(self, index_config):
        if index_config.koji_tags:
            self.koji_indexes.append(index_config)
        else:
            self.tag_indexes.append(index_config)

    def _get_pyxis_url(self, url):
        kwargs = {
        }

        cert = self.global_config.find_local_cert(self.global_config.pyxis_url)
        if cert:
            kwargs['verify'] = cert
        else:
            kwargs['verify'] = True

        if self.global_config.pyxis_client_cert:
            kwargs['cert'] = (self.global_config.pyxis_client_cert,
                              self.global_config.pyxis_client_key)

        response = self.session.get(url, headers={'Accept': 'application/json'}, **kwargs)
        response.raise_for_status()

        return response.json()

    def _do_iterate_pyxis_results(self, url):
        page_size = self.page_size
        page = 0
        while True:
            sep = '&' if '?' in url else '?'
            paginated_url = url + sep + 'page_size={page_size}&page={page}'.format(
                page_size=page_size,
                page=page)
            logger.info("Requesting {}".format(paginated_url))

            response_json = self._get_pyxis_url(paginated_url)

            for item in response_json['data']:
                yield item

            if response_json['total'] <= page_size * page + len(response_json['data']):
                break

            page += 1

    def _get_tag_history(self, repository, tag):
        api_url = self.global_config.pyxis_url
        url = f'{api_url}tag-history/registry/{self.name}/repository/{repository}/tag/{tag}'

        try:
            tag_history = self._get_pyxis_url(url)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return []

        return [(item['brew_build'], parse_date(item['start_date']))
                for item in tag_history['history']]

    def _iterate_repositories(self):
        if self.config.repositories:
            yield from self.config.repositories
            return

        url = '{api_url}repositories?image_usage_type=Flatpak'.format(
            api_url=self.global_config.pyxis_url)

        for item in self._do_iterate_pyxis_results(url):
            if item['registry'] == self.name:
                yield item['repository']

    def _iterate_flatpak_builds(self, koji_tag):
        if koji_tag.endswith('+'):
            koji_tag = koji_tag[0:-1]
            inherit = True
        else:
            inherit = False

        tagged_builds = self.koji_session.listTagged(koji_tag, type='image',
                                                     inherit=inherit, latest=True)
        for tagged_build in tagged_builds:
            build = query_image_build(self.koji_session, self.redis_client, tagged_build['nvr'])
            if isinstance(build, FlatpakBuildModel):
                yield build

    def _add_build_history(self, repository_name, tag, architectures, build_items):
        tag_history = TagHistoryModel(name=tag)

        for build, start_date in build_items:
            n, v, r = build.nvr.rsplit('-', 2)

            for image in build.images:
                if not (None in architectures or image.architecture in architectures):
                    continue

                repository = self.registry.repositories.get(repository_name)
                old_image = repository.images.get(image.digest) if repository else None
                if old_image:
                    if build == build_items[0][0]:
                        old_image.tags.append(tag)
                else:
                    image.tags = [v, f"{v}-{r}"]
                    if build == build_items[0][0]:
                        image.tags.append(tag)

                    self.registry.add_image(repository_name, image)

                item = TagHistoryItemModel(architecture=image.architecture,
                                           date=start_date,
                                           digest=image.digest)
                tag_history.items.append(item)

        if len(tag_history.items):
            self.registry.repositories[repository_name].tag_histories[tag] = tag_history

    def find_images(self):
        desired_architectures = defaultdict(set)
        for index_config in self.tag_indexes:
            desired_architectures[index_config.tag].add(index_config.architecture)

        if len(desired_architectures) > 0:
            for repository in self._iterate_repositories():
                for tag, architectures in desired_architectures.items():
                    history_items = self._get_tag_history(repository, tag)
                    if len(history_items) == 0:
                        continue

                    build_items = [(query_image_build(self.koji_session,
                                                      self.redis_client,
                                                      nvr), start_date)
                                   for (nvr, start_date) in history_items]

                    self._add_build_history(repository, tag, architectures, build_items)

        desired_architectures_koji = defaultdict(set)
        tag_koji_tags = {}
        for index_config in self.koji_indexes:
            # config.py enforces that the tag => koji_tags mapping is consistent for
            # multiple indexes with the same 'tag'
            tag_koji_tags[index_config.tag] = index_config.koji_tags
            desired_architectures_koji[index_config.tag].add(index_config.architecture)

        # Cache the builds for each tag
        koji_tag_builds = {}

        koji_tag_start_date = datetime.fromtimestamp(0, timezone.utc)

        for tag, koji_tags in tag_koji_tags.items():
            # if multiple koji_tags are configured for the index, we merge them keeping
            # only the latest build for each name
            builds_by_name = {}
            for koji_tag in koji_tags:
                if koji_tag not in koji_tag_builds:
                    koji_tag_builds[koji_tag] = list(self._iterate_flatpak_builds(koji_tag))

                for build in koji_tag_builds[koji_tag]:
                    name, _, _ = build.nvr.rsplit('-', 2)
                    if (name not in builds_by_name or
                            rpm_nvr_compare(builds_by_name[name].nvr, build.nvr) < 0):
                        builds_by_name[name] = build

            architectures = desired_architectures_koji[tag]

            for build in builds_by_name.values():
                build_items = [(build, koji_tag_start_date)]
                self._add_build_history(build.repository, tag,
                                        architectures, build_items)


class PyxisUpdater(object):
    def __init__(self, config, page_size=50):
        self.conf = config
        self.page_size = page_size

    def start(self):
        pass

    def update(self, registry_data):
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

        for registry_name, registry in registries.items():
            registry.find_images()
            registry_data[registry_name] = registry.registry

    def stop(self):
        pass
