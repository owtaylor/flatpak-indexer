from datetime import datetime
import logging

from ...config import Config, DirectRegistryConfig, IndexConfig
from ...models import RegistryModel, TagHistoryItemModel, TagHistoryModel
from ...registry_client import RegistryClient
from ...registry_query import make_registry_image
from .. import Updater

logger = logging.getLogger(__name__)


class Registry:
    def __init__(
        self,
        name,
        global_config: Config,
    ):
        self.name = name
        config = global_config.registries[name]
        assert isinstance(config, DirectRegistryConfig)
        self.config = config
        self.global_config = global_config
        self.registry_client = RegistryClient(
            self.config.public_url, session=self.global_config.get_requests_session()
        )

        self.registry = RegistryModel(name=name, images={})

        self.indexes: list[IndexConfig] = []

    def add_index(self, index_config: IndexConfig):
        self.indexes.append(index_config)

    def find_images(self):
        # Determine which architectures to fetch
        architectures: set[str] = set()
        all_architectures: bool = False

        for index in self.indexes:
            if not index.architecture:
                all_architectures = True
            else:
                architectures.add(index.architecture)

        # Collect all unique tags needed from all indexes
        tags_needed: set[str] = set()
        for index in self.indexes:
            tags_needed.add(index.tag)

        # Fetch images for each repository:tag combination
        for repository in self.config.repositories:
            for tag in tags_needed:
                logger.debug(f"Finding images for {repository}:{tag} in registry {self.name}")
                manifests = self.registry_client.fetch_manifests_by_architecture(repository, tag)

                tag_history = TagHistoryModel(name=tag)

                for architecture, (digest, manifest, config) in manifests.items():
                    if all_architectures or architecture in architectures:
                        # Check if we already have this image (same digest)
                        if (
                            repository in self.registry.repositories
                            and digest in self.registry.repositories[repository].images
                        ):
                            # Image already exists, just add the tag
                            image = self.registry.repositories[repository].images[digest]
                            if tag not in image.tags:
                                image.tags.append(tag)
                        else:
                            # Create new image and add it
                            image = make_registry_image(manifest, config, digest=digest)
                            image.tags.append(tag)
                            self.registry.add_image(repository, image)

                        date = datetime.fromisoformat(config["created"])
                        logger.debug(f"Adding image for architecture {architecture}, date: {date}")
                        tag_history.items.append(
                            TagHistoryItemModel(
                                architecture=architecture,
                                date=date,
                                digest=image.digest,
                            )
                        )

                # Only add tag history if we actually added images
                if repository in self.registry.repositories:
                    self.registry.repositories[repository].tag_histories[tag] = tag_history


class DirectUpdater(Updater):
    def __init__(self, config: Config):
        self.conf = config

    def start(self):
        pass

    def update(self, registry_data):
        registries: dict[str, Registry] = {}
        for index_config in self.conf.get_indexes_for_datasource("direct"):
            registry_name = index_config.registry

            if registry_name not in registries:
                registries[registry_name] = Registry(registry_name, self.conf)

            registry = registries[registry_name]
            registry.add_index(index_config)

        for registry_name, registry in registries.items():
            registry.find_images()
            registry_data[registry_name] = registry.registry

    def stop(self):
        pass
