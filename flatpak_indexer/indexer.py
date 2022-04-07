import base64
from collections import defaultdict
import copy
import hashlib
import logging
import json
import os
from typing import DefaultDict, Dict, Optional, Set

from .cleaner import Cleaner
from .config import Config, IndexConfig, RegistryConfig
from .delta_generator import DeltaGenerator
from .koji_query import query_image_build, query_module_build
from .koji_utils import get_koji_session
from .redis_utils import get_redis_client
from .utils import atomic_writer, path_for_digest, pseudo_atomic_dir_writer, uri_for_digest
from .models import (
    FlatpakBuildModel, ImageModel, ImageBuildModel, ModuleBuildModel, ModuleStreamContentsModel,
    RegistryModel
)


logger = logging.getLogger(__name__)

DATA_URI_PREFIX = 'data:image/png;base64,'


class IconStore(object):
    def __init__(self, icons_dir: str, icons_uri: str, cleaner: Cleaner):
        self.icons_dir = icons_dir
        self.icons_uri = icons_uri
        self.cleaner = cleaner

    def store(self, uri: str):
        if not uri.startswith(DATA_URI_PREFIX):
            return None

        decoded = base64.b64decode(uri[len(DATA_URI_PREFIX):])

        h = hashlib.sha256()
        h.update(decoded)
        digest = 'sha256:' + h.hexdigest()

        fullpath = path_for_digest(self.icons_dir, digest, ".png",
                                   create_subdir=True)

        if not os.path.exists(fullpath):
            logger.info("Storing icon: %s", fullpath)
            with open(fullpath, 'wb') as f:
                f.write(decoded)

        self.cleaner.reference(fullpath)

        return uri_for_digest(self.icons_uri, digest, ".png")


class BuildCache:
    image_builds: Dict[str, ImageBuildModel]
    module_builds: Dict[str, ModuleBuildModel]

    def __init__(self, global_config: Config):
        self.koji_session = get_koji_session(global_config)
        self.redis_client = get_redis_client(global_config)
        self.image_builds = {}
        self.module_builds = {}

    def get_image_build(self, nvr: str):
        image_build = self.image_builds.get(nvr)
        if image_build:
            return image_build

        image_build = query_image_build(self.koji_session, self.redis_client, nvr)
        self.image_builds[nvr] = image_build
        return image_build

    def get_module_build(self, nvr: str):
        module_build = self.module_builds.get(nvr)
        if module_build:
            return module_build

        module_build = query_module_build(self.koji_session, self.redis_client, nvr)
        self.module_builds[nvr] = module_build
        return module_build


class IndexWriter:
    def __init__(
        self, conf: IndexConfig, registry_config: RegistryConfig,
        build_cache: BuildCache, icon_store: Optional[IconStore]
    ):
        self.registry_config = registry_config
        self.config = conf
        self.build_cache = build_cache
        self.icon_store = icon_store
        self.registry = RegistryModel()

    def extract_icon(self, labels: Dict[str, str], key: str):
        if not self.config.extract_icons:
            return
        # config.py checks that icons_dir and icons_uri are set if extract_icons
        # is set for any Index
        assert self.icon_store

        value = labels.get(key)
        if value is None:
            return

        uri = self.icon_store.store(value)
        if uri is not None:
            labels[key] = uri

    def move_flatpak_labels(self, image: ImageModel):
        to_move = [k for k in image.labels.keys()
                   if k.startswith('org.flatpak.') or k.startswith('org.freedesktop.')]

        for k in to_move:
            image.annotations[k] = image.labels[k]
            del image.labels[k]

    def add_image(self, name: str, image: ImageModel, delta_manifest_url: Optional[str]):
        image = copy.copy(image)

        # Clean up some information we don't want in the final output
        image.diff_ids = []
        image.pull_spec = None

        image.annotations = copy.copy(image.annotations)
        image.labels = copy.copy(image.labels)

        if self.registry_config.force_flatpak_token:
            # This string the base64-encoding GVariant holding a variant
            # holding the int32 1.
            image.labels['org.flatpak.commit-metadata.xa.token-type'] = 'AQAAAABp'

        if delta_manifest_url:
            image.labels['io.github.containers.DeltaUrl'] = delta_manifest_url

        self.extract_icon(image.labels, 'org.freedesktop.appstream.icon-64')
        self.extract_icon(image.labels, 'org.freedesktop.appstream.icon-128')

        if self.config.flatpak_annotations:
            self.move_flatpak_labels(image)

        self.registry.add_image(name, image)

    def iter_images(self):
        for repository in self.registry.repositories.values():
            for image in repository.images.values():
                yield image

            # We don't currently export lists for anything
            assert not repository.lists
            # for list_ in repository.lists.values():
            #     for image in list_.images:
            #         yield image

    def iter_image_builds(self):
        seen: Set[str] = set()
        for image in self.iter_images():
            nvr = image.nvr
            if nvr and nvr not in seen:
                seen.add(nvr)
                yield self.build_cache.get_image_build(nvr)

    def write_contents(self):
        contents_dir = self.config.contents
        if contents_dir is None:
            return

        module_stream_contents: DefaultDict[str, ModuleStreamContentsModel] = \
            defaultdict(ModuleStreamContentsModel)

        for build in self.iter_image_builds():
            # We only index Flatpak builds currently
            assert isinstance(build, FlatpakBuildModel)
            # if not isinstance(build, FlatpakBuildModel):
            #     continue

            package_to_module: Dict[str, ModuleBuildModel] = {}
            for module_nvr in build.module_builds:
                module_build = self.build_cache.get_module_build(module_nvr)
                for binary_package in module_build.package_builds:
                    package_to_module[binary_package.nvr] = module_build

            for binary_package in build.package_builds:
                module = package_to_module.get(binary_package.nvr)
                if module:
                    n, v, _ = module.nvr.rsplit('-', 2)
                    name_stream = n + ":" + v
                    stream_contents = module_stream_contents[name_stream]
                    stream_contents.add_package_build(build.nvr, module.nvr, binary_package)

        # We auto-create only one level and don't use os.makedirs,
        # to better catch configuration mistakes
        output_dir = os.path.dirname(contents_dir)
        if not os.path.isdir(output_dir):
            os.mkdir(output_dir)

        with pseudo_atomic_dir_writer(contents_dir) as tempdir:
            modules_dir = os.path.join(tempdir, "modules")
            os.mkdir(modules_dir)

            for name_stream, contents in module_stream_contents.items():
                path = os.path.join(modules_dir, name_stream + ".json")
                with open(path, "w", encoding="UTF-8") as f:
                    json.dump(
                        contents.to_json(), f, sort_keys=True, indent=4, ensure_ascii=False
                    )

    def write(self):
        # We auto-create only one level and don't use os.makedirs,
        # to better catch configuration mistakes
        output_dir = os.path.dirname(self.config.output)
        if not os.path.isdir(output_dir):
            os.mkdir(output_dir)

        filtered_repos = (v for v in self.registry.repositories.values() if v.images or v.lists)
        sorted_repos = sorted(filtered_repos, key=lambda r: r.name)

        with atomic_writer(self.config.output) as writer:
            json.dump({
                'Registry': self.registry_config.public_url,
                'Results': [r.to_json() for r in sorted_repos],
            }, writer, sort_keys=True, indent=4, ensure_ascii=False)

        self.write_contents()


class Indexer:
    def __init__(self, config: Config, cleaner: Optional[Cleaner] = None):
        self.conf = config
        self.build_cache = BuildCache(config)
        if cleaner is None:
            cleaner = Cleaner(self.conf)
        self.cleaner = cleaner

    def index(self, registry_data: Dict[str, RegistryModel]):
        icon_store = None
        if self.conf.icons_dir and self.conf.icons_uri:
            icon_store = IconStore(self.conf.icons_dir, self.conf.icons_uri, self.cleaner)

        delta_generator = None
        if any(index_config.delta_keep.total_seconds() > 0 for index_config in self.conf.indexes):
            delta_generator = DeltaGenerator(self.conf, cleaner=self.cleaner)

            for index_config in self.conf.indexes:
                registry_info = registry_data.get(index_config.registry)
                if registry_info is None:
                    continue

                registry_config = self.conf.registries[index_config.registry]
                for repository in registry_info.repositories.values():
                    tag_history = repository.tag_histories.get(index_config.tag)
                    if tag_history:
                        delta_generator.add_tag_history(repository, tag_history, index_config)

            delta_generator.generate()

        for index_config in self.conf.indexes:
            registry_name = index_config.registry
            registry_config = self.conf.registries[registry_name]

            registry_info = registry_data.get(registry_name)
            if registry_info is None:
                logger.debug("No queried information found for %s", registry_name)
                continue

            index = IndexWriter(index_config,
                                registry_config,
                                self.build_cache,
                                icon_store)

            for repository in registry_info.repositories.values():
                for image in repository.images.values():
                    if (index_config.tag in image.tags and
                        (index.config.architecture is None or
                         image.architecture == index.config.architecture)):

                        if delta_generator:
                            delta_manifest_url = \
                                delta_generator.get_delta_manifest_url(image.digest)
                        else:
                            delta_manifest_url = None

                        index.add_image(repository.name, image, delta_manifest_url)

            index.write()
