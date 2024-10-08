from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
import logging
from typing import List, Optional

from .. import Updater
from ...config import Config, PyxisRegistryConfig
from ...models import (ImageModel, RegistryModel, TagHistoryItemModel, TagHistoryModel)
from ...registry_client import RegistryClient
from ...session import Session
from ...utils import parse_date

logger = logging.getLogger(__name__)

MEDIA_TYPE_MANIFEST_V2 = 'application/vnd.docker.distribution.manifest.v2+json'


REPOSITORY_QUERY = """\
query ($page: Int, $page_size: Int)
{
  find_repositories(filter: {build_categories: {in:["Flatpak"]}},
                    page: $page, page_size: $page_size) {
    error {
      detail
      status
    }

    total

    data {
      registry
      repository
    }
  }
}
"""


REPOSITORY_IMAGE_QUERY = """\
query ($registry: String, $repository: String, $page: Int, $page_size: Int)
{
  find_repository_images_by_registry_path(registry: $registry, repository: $repository,
                                          page: $page, page_size: $page_size) {
    error {
      detail
      status
    }

    total

    data {
      architecture
      brew {
        build
      }
      image_id
      repositories {
        push_date
        registry
        repository
        tags {
          name
        }
      }
    }
  }
}"""


@dataclass
class HistoryItem:
    start_date: datetime
    digest: str
    brew_build: Optional[str]
    architecture: str
    tags: List[str]


class Registry:
    def __init__(self, name, global_config: Config, page_size):
        self.name = name
        self.global_config = global_config
        registry_config = global_config.registries[name]
        assert isinstance(registry_config, PyxisRegistryConfig)
        self.config = registry_config
        self.page_size = page_size
        self.tag_indexes = []
        self.registry = RegistryModel()
        self.image_to_build = dict()
        self.registry_client = RegistryClient(self.config.public_url,
                                              session=self.global_config.get_requests_session())

        self.requests_session = global_config.get_requests_session()
        self.session = Session(global_config)

    def add_index(self, index_config):
        self.tag_indexes.append(index_config)

    def _do_pyxis_graphql_query(self, query, variables):
        body = {"query": query, "variables": variables}

        kwargs = {
        }

        if self.config.pyxis_client_cert:
            kwargs['cert'] = (self.config.pyxis_client_cert,
                              self.config.pyxis_client_key)

        response = self.requests_session.post(self.config.pyxis_url, json=body, **kwargs)
        json = response.json()

        if "errors" in json:
            logger.error("Error querying pyxis: %s", json["errors"])
        response.raise_for_status()

        return json

    def _do_iterate_pyxis_results(self, query, variables):
        page_size = self.page_size
        page = 0
        while True:
            paginated_variables = {
                "page": page,
                "page_size": page_size
            }
            paginated_variables.update(variables)

            response_json = self._do_pyxis_graphql_query(query, paginated_variables)
            for query_name, query_result in response_json["data"].items():
                for item in query_result['data']:
                    yield item

            if query_result['total'] <= page_size * page + len(query_result['data']):
                break

            page += 1

    def _get_repository_history(self, repository_name, tag_name):
        history: List[HistoryItem] = []

        logger.info("%s: listing images, registry=%s, repository=%s",
                    self.config.pyxis_url, self.config.pyxis_registry, repository_name)
        for item in self._do_iterate_pyxis_results(REPOSITORY_IMAGE_QUERY,
                                                   {
                                                       "registry": self.config.pyxis_registry,
                                                       "repository": repository_name
                                                   }):
            for repository in item["repositories"]:
                if (
                    repository["registry"] != self.config.pyxis_registry or
                    repository["repository"] != repository_name
                ):  # pragma: no cover
                    # Incorrectly marked uncovered for python-3.9 (fine with 3.12)
                    # https://github.com/nedbat/coveragepy/issues/198
                    continue

                tags = [tag["name"] for tag in repository["tags"]]
                brew = item.get("brew")
                brew_build = brew.get("build") if brew else None
                history.append(HistoryItem(
                    start_date=parse_date(repository["push_date"]),
                    digest=item["image_id"],
                    brew_build=brew_build,
                    architecture=item["architecture"],
                    tags=tags,
                ))

        if len(history) > 0:
            history.sort(key=lambda item: item.start_date, reverse=True)
            if tag_name not in history[0].tags:
                logger.error(
                    "%s/%s: %s is not applied to the latest build, can't determine history",
                    self.config.name, repository_name, tag_name)
                return [i for i in history if tag_name in i.tags]

        return history

    def _iterate_repositories(self):
        if self.config.repositories:
            yield from self.config.repositories
            return

        logger.info("%s: listing repositories", self.config.pyxis_url)
        for item in self._do_iterate_pyxis_results(REPOSITORY_QUERY, {}):
            if item['registry'] == self.config.pyxis_registry:
                yield item['repository']

    def _get_image_from_brew(self, history_item: HistoryItem):
        assert history_item.brew_build is not None
        build = self.session.build_cache.get_image_build(history_item.brew_build)
        matched_images = [i for i in build.images
                          if i.digest == history_item.digest]
        if len(matched_images) == 0:
            logger.error("No image for %s with digest %s",
                         history_item.brew_build, history_item.digest)
            return None

        image = matched_images[0]
        image.tags = history_item.tags
        return image

    def _get_image_from_registry(self, repository_name, history_item: HistoryItem):
        logger.info("Fetching manifest and config for repository=%s, tags=%s, arch=%s",
                    repository_name, history_item.tags, history_item.architecture)
        manifest = self.registry_client.get_manifest(repository_name, history_item.digest)
        config = self.registry_client.get_config(repository_name, manifest)

        pull_spec = (
            self.config.public_url.removeprefix('https://').removesuffix("/") +
            "/" +
            repository_name +
            "@" +
            history_item.digest
        )

        image = ImageModel(
            digest=history_item.digest,
            media_type=manifest["mediaType"],
            os=config["os"],
            architecture=history_item.architecture,
            labels=config["config"]["Labels"],
            annotations={},
            tags=history_item.tags,
            pull_spec=pull_spec,
            diff_ids=config["rootfs"]["diff_ids"]
        )

        # Mark that there's no brew build to look up
        image.no_koji = True

        return image

    def _add_build_history(
            self, repository_name: str, tag: str, architectures, history_items: List[HistoryItem]
    ):
        tag_history = TagHistoryModel(name=tag)

        repository_name = self.config.adjust_repository(repository_name)

        for history_item in history_items:
            if not (None in architectures or history_item.architecture in architectures):
                continue

            repository = self.registry.repositories.get(repository_name)
            old_image = repository.images.get(history_item.digest) if repository else None

            if not old_image:
                if history_item.brew_build is not None:
                    image = self._get_image_from_brew(history_item)
                else:
                    image = self._get_image_from_registry(repository_name, history_item)

                if not image:
                    continue

                self.registry.add_image(repository_name, image)

            item = TagHistoryItemModel(architecture=history_item.architecture,
                                       date=history_item.start_date,
                                       digest=history_item.digest)
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
                    history_items = self._get_repository_history(repository, tag)
                    if len(history_items) == 0:
                        continue

                    self._add_build_history(repository, tag, architectures, history_items)


class PyxisUpdater(Updater):
    def __init__(self, config, page_size=50):
        self.conf = config
        self.page_size = page_size

    def start(self):
        pass

    def update(self, registry_data):
        registries = {}
        for index_config in self.conf.get_indexes_for_datasource('pyxis'):
            registry_name = index_config.registry

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
