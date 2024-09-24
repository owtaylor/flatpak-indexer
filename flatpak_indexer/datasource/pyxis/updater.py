from collections import defaultdict
import logging
from urllib.parse import urlencode

import requests

from .. import Updater
from ...models import (RegistryModel, TagHistoryItemModel, TagHistoryModel)
from ...session import Session
from ...utils import parse_date

logger = logging.getLogger(__name__)

MEDIA_TYPE_MANIFEST_V2 = 'application/vnd.docker.distribution.manifest.v2+json'


class Registry:
    def __init__(self, name, global_config, page_size):
        self.name = name
        self.global_config = global_config
        self.config = global_config.registries[name]
        self.page_size = page_size
        self.tag_indexes = []
        self.registry = RegistryModel()
        self.image_to_build = dict()

        self.requests_session = global_config.get_requests_session()
        self.session = Session(global_config)

    def add_index(self, index_config):
        self.tag_indexes.append(index_config)

    def _get_pyxis_url(self, url):
        kwargs = {
        }

        if self.global_config.pyxis_client_cert:
            kwargs['cert'] = (self.global_config.pyxis_client_cert,
                              self.global_config.pyxis_client_key)

        response = self.requests_session.get(url, headers={'Accept': 'application/json'}, **kwargs)
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
            else:
                raise

        return [(item['brew_build'], parse_date(item['start_date']))
                for item in tag_history['history']]

    def _iterate_repositories(self):
        if self.config.repositories:
            yield from self.config.repositories
            return

        url = '{api_url}repositories?{query}'.format(
            api_url=self.global_config.pyxis_url,
            query=urlencode({
                'filter': 'build_categories=in=(Flatpak)'
            })
        )

        for item in self._do_iterate_pyxis_results(url):
            if item['registry'] == self.name:
                yield item['repository']

    def _add_build_history(self, repository_name, tag, architectures, build_items):
        tag_history = TagHistoryModel(name=tag)

        for build, start_date in build_items:
            for image in build.images:
                if not (None in architectures or image.architecture in architectures):
                    continue

                repository = self.registry.repositories.get(repository_name)
                old_image = repository.images.get(image.digest) if repository else None
                if old_image:
                    if build == build_items[0][0]:
                        old_image.tags.append(tag)
                else:
                    image.tags = [build.nvr.version, f"{build.nvr.version}-{build.nvr.release}"]
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

                    build_items = [(self.session.build_cache.get_image_build(nvr), start_date)
                                   for (nvr, start_date) in history_items]

                    self._add_build_history(repository, tag, architectures, build_items)


class PyxisUpdater(Updater):
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
