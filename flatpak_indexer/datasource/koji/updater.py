from collections import defaultdict
from datetime import datetime, timezone
import logging
from typing import Dict, List

from .. import Updater
from ...config import Config, KojiRegistryConfig
from ...models import (FlatpakBuildModel, ImageBuildModel, RegistryModel,
                       TagHistoryItemModel, TagHistoryModel)
from ...session import Session

logger = logging.getLogger(__name__)

MEDIA_TYPE_MANIFEST_V2 = 'application/vnd.docker.distribution.manifest.v2+json'


class Registry:
    def __init__(self, name, global_config: Config):
        self.name = name
        self.global_config = global_config
        registry_config = global_config.registries[name]
        assert isinstance(registry_config, KojiRegistryConfig)
        self.config = registry_config
        self.koji_indexes = []
        self.registry = RegistryModel()
        self.requests_session = global_config.get_requests_session()
        self.session = Session(global_config)

    def add_index(self, index_config):
        self.koji_indexes.append(index_config)

    def _add_build_history(
            self, repository_name: str, tag: str, architectures, build: ImageBuildModel
    ):
        tag_history = TagHistoryModel(name=tag)

        for image in build.images:
            if not (None in architectures or image.architecture in architectures):
                continue

            repository = self.registry.repositories.get(repository_name)
            old_image = repository.images.get(image.digest) if repository else None
            if old_image:
                old_image.tags.append(tag)
            else:
                image.tags = [build.nvr.version, f"{build.nvr.version}-{build.nvr.release}"]
                image.tags.append(tag)
                self.registry.add_image(repository_name, image)

            item = TagHistoryItemModel(architecture=image.architecture,
                                       date=datetime.fromtimestamp(0, timezone.utc),
                                       digest=image.digest)
            tag_history.items.append(item)

        if len(tag_history.items):
            self.registry.repositories[repository_name].tag_histories[tag] = tag_history

    def _iterate_flatpak_builds(self, koji_tag):
        if koji_tag.endswith('+'):
            koji_tag = koji_tag[0:-1]
            inherit = True
        else:
            inherit = False

        tagged_builds = self.session.koji_session.listTagged(koji_tag, type='image',
                                                             inherit=inherit, latest=True)
        for tagged_build in tagged_builds:
            build = self.session.build_cache.get_image_build(tagged_build['nvr'])
            if isinstance(build, FlatpakBuildModel):
                yield build

    def find_images(self):
        desired_architectures_koji = defaultdict(set)
        tag_koji_tags = {}
        for index_config in self.koji_indexes:
            # config.py enforces that the tag => koji_tags mapping is consistent for
            # multiple indexes with the same 'tag'
            tag_koji_tags[index_config.tag] = index_config.koji_tags
            desired_architectures_koji[index_config.tag].add(index_config.architecture)

        # Cache the builds for each tag
        koji_tag_builds: Dict[str, List[FlatpakBuildModel]] = {}

        for tag, koji_tags in tag_koji_tags.items():
            # if multiple koji_tags are configured for the index, we merge them keeping
            # only the latest build for each name
            builds_by_name: Dict[str, FlatpakBuildModel] = {}
            for koji_tag in koji_tags:
                if koji_tag not in koji_tag_builds:
                    koji_tag_builds[koji_tag] = list(self._iterate_flatpak_builds(koji_tag))

                for build in koji_tag_builds[koji_tag]:
                    name = build.nvr.name
                    if (name not in builds_by_name or
                            builds_by_name[name].nvr < build.nvr):
                        builds_by_name[name] = build

            architectures = desired_architectures_koji[tag]

            for build in builds_by_name.values():
                self._add_build_history(build.repository, tag,
                                        architectures,
                                        build)


class KojiUpdater(Updater):
    def __init__(self, config: Config):
        self.conf = config

    def start(self):
        pass

    def update(self, registry_data):
        registries = {}
        for index_config in self.conf.get_indexes_for_datasource('koji'):
            registry_name = index_config.registry

            if registry_name not in registries:
                registries[registry_name] = Registry(registry_name,
                                                     self.conf)

            registry = registries[registry_name]
            registry.add_index(index_config)

        for registry_name, registry in registries.items():
            registry.find_images()
            registry_data[registry_name] = registry.registry

    def stop(self):
        pass
