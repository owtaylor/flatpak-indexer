import base64
from collections import defaultdict
import copy
import hashlib
import json
import logging
import os
from typing import DefaultDict, Dict, Optional, Set

from .cleaner import Cleaner
from .config import Config, IndexConfig, RegistryConfig
from .delta_generator import DeltaGenerator
from .models import (
    FlatpakBuildModel, ImageModel, ModuleBuildModel, ModuleStreamContentsModel,
    RegistryModel
)
from .session import Session
from .utils import atomic_writer, path_for_digest, pseudo_atomic_dir_writer, uri_for_digest


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


class IndexWriter:
    def __init__(
        self, conf: IndexConfig, registry_config: RegistryConfig,
        source_registry: RegistryModel,
        session: Session, icon_store: Optional[IconStore]
    ):
        self.registry_config = registry_config
        self.config = conf
        self.session = session
        self.icon_store = icon_store
        self.source_registry = source_registry
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

    def add_image(self, name: str, image: ImageModel):
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

        self.extract_icon(image.labels, 'org.freedesktop.appstream.icon-64')
        self.extract_icon(image.labels, 'org.freedesktop.appstream.icon-128')

        if self.config.flatpak_annotations:
            self.move_flatpak_labels(image)

        self.registry.add_image(name, image)

    def find_images(self):
        for repository in self.source_registry.repositories.values():
            for image in repository.images.values():
                if (self.config.tag in image.tags and
                    (self.config.architecture is None or
                        image.architecture == self.config.architecture)):

                    self.add_image(repository.name, image)

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
                yield self.session.build_cache.get_image_build(nvr)

    def setup_delta_generator(self, delta_generator: DeltaGenerator):
        for repository in self.registry.repositories.values():
            source_repository = self.source_registry.repositories[repository.name]
            tag_history = source_repository.tag_histories.get(self.config.tag)
            if tag_history:
                delta_generator.add_tag_history(
                    source_repository, tag_history, self.config
                )

    def add_delta_urls(self, delta_generator: DeltaGenerator):
        for image in self.iter_images():
            delta_manifest_url = \
                delta_generator.get_delta_manifest_url(image.digest)
            if delta_manifest_url:
                image.labels['io.github.containers.DeltaUrl'] = delta_manifest_url

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
                module_build = self.session.build_cache.get_module_build(module_nvr)
                for binary_package in module_build.package_builds:
                    package_to_module[binary_package.nvr] = module_build

            for binary_package in build.package_builds:
                module = package_to_module.get(binary_package.nvr)
                if module:
                    name_stream = module.nvr.name + ":" + module.nvr.version
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
        if cleaner is None:
            cleaner = Cleaner(self.conf)
        self.cleaner = cleaner

    def index(self, registry_data: Dict[str, RegistryModel]):
        session = Session(self.conf)

        icon_store = None
        if self.conf.icons_dir and self.conf.icons_uri:
            icon_store = IconStore(self.conf.icons_dir, self.conf.icons_uri, self.cleaner)

        index_writers: Dict[str, IndexWriter] = {}
        for index_config in self.conf.indexes:
            registry_name = index_config.registry
            registry_config = self.conf.registries[registry_name]

            source_registry = registry_data.get(registry_name)
            if source_registry is None:
                logger.debug("No queried information found for %s", registry_name)
                continue

            index_writer = IndexWriter(index_config,
                                       registry_config,
                                       source_registry,
                                       session,
                                       icon_store)
            index_writers[index_config.name] = index_writer

            index_writer.find_images()

        delta_generator = None
        if any(index_config.delta_keep.total_seconds() > 0 for index_config in self.conf.indexes):
            delta_generator = DeltaGenerator(self.conf, cleaner=self.cleaner)

            for index_writer in index_writers.values():
                index_writer.setup_delta_generator(delta_generator)

            delta_generator.generate()

            for index_writer in index_writers.values():
                index_writer.add_delta_urls(delta_generator)

        for index_writer in index_writers.values():
            index_writer.write()
