import base64
import copy
import hashlib
import logging
import json
import os

from .delta_generator import DeltaGenerator
from .utils import atomic_writer, path_for_digest, uri_for_digest
from .models import RegistryModel


logger = logging.getLogger(__name__)

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
        digest = 'sha256:' + h.hexdigest()

        fullpath = path_for_digest(self.icons_dir, digest, ".png",
                                   create_subdir=True)
        key = tuple(fullpath.split("/")[-2:])

        if key in self.icons:
            pass
        elif key in self.old_icons:
            self.icons[key] = True
        else:
            logger.info("Storing icon: %s", fullpath)
            with open(fullpath, 'wb') as f:
                f.write(decoded)
            self.icons[key] = True

        return uri_for_digest(self.icons_uri, digest, ".png")

    def clean(self):
        for key in self.old_icons:
            if key not in self.icons:
                subdir, filename = key
                fullpath = os.path.join(self.icons_dir, subdir, filename)
                os.unlink(fullpath)
                logger.info("Removing icon: %s", fullpath)


class IndexWriter:
    def __init__(self, conf, registry_config, icon_store=None):
        self.registry_config = registry_config
        self.config = conf
        self.icon_store = icon_store
        self.registry = RegistryModel()

    def extract_icon(self, labels, key):
        if not self.config.extract_icons:
            return

        value = labels.get(key)
        if value is None:
            return

        uri = self.icon_store.store(value)
        if uri is not None:
            labels[key] = uri

    def add_image(self, name, image, delta_manifest_url):
        image = copy.copy(image)
        image.labels = copy.copy(image.labels)

        if self.registry_config.force_flatpak_token:
            # This string the base64-encoding GVariant holding a variant
            # holding the int32 1.
            image.labels['org.flatpak.commit-metadata.xa.token-type'] = 'AQAAAABp'

        if delta_manifest_url:
            image.labels['io.github.containers.DeltaUrl'] = delta_manifest_url

        self.extract_icon(image.labels, 'org.freedesktop.appstream.icon-64')
        self.extract_icon(image.labels, 'org.freedesktop.appstream.icon-128')

        self.registry.add_image(name, image)

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


class Indexer:
    def __init__(self, config):
        self.conf = config

    def load_registry(self, registry_name):
        filename = os.path.join(self.conf.work_dir, registry_name + ".json")
        try:
            with open(filename, "rb") as f:
                mtime = os.fstat(f.fileno()).st_mtime
                raw = json.load(f)
                return RegistryModel.from_json(raw), mtime
        except FileNotFoundError:
            return None, None

    def index(self):
        if len(self.conf.indexes) == 0:
            return

        registries = {}

        index_mtimes = []
        registry_mtimes = []
        for index_config in self.conf.indexes:
            try:
                index_mtimes.append(os.stat(index_config.output).st_mtime)
            except FileNotFoundError:
                pass

            registry_name = index_config.registry
            registry_config = self.conf.registries[registry_name]

            if registry_name not in registries:
                registries[registry_name], registry_mtime = self.load_registry(registry_name)
                if registry_mtime is not None:
                    registry_mtimes.append(registry_mtime)

        if index_mtimes and registry_mtimes and max(registry_mtimes) < min(index_mtimes):
            logger.debug("Skipping indexing, intermediate files have not been updated")
            return

        icon_store = None
        if self.conf.icons_dir is not None:
            icon_store = IconStore(self.conf.icons_dir, self.conf.icons_uri)

        delta_generator = None
        if any(index_config.delta_keep_days > 0 for index_config in self.conf.indexes):
            delta_generator = DeltaGenerator(self.conf)

            for index_config in self.conf.indexes:
                tag = index_config.output_tag

                registry_info = registries[index_config.registry]
                if registry_info is None:
                    # No intermediate file, skip
                    continue

                registry_config = self.conf.registries[index_config.registry]
                for repository in registry_info.repositories.values():
                    tag_history = repository.tag_histories.get(tag)
                    if tag_history:
                        delta_generator.add_tag_history(repository, tag_history, index_config)

            delta_generator.generate()

        for index_config in self.conf.indexes:
            tag = index_config.output_tag

            registry_name = index_config.registry
            registry_config = self.conf.registries[registry_name]

            registry_info = registries[registry_name]
            if registry_info is None:
                logger.debug("No intermediate file found for %s", registry_name)
                continue

            index = IndexWriter(index_config,
                                registry_config,
                                icon_store=icon_store)

            for repository in registry_info.repositories.values():
                for image in repository.images.values():
                    if (tag in image.tags and
                        (index.config.architecture is None or
                         image.architecture == index.config.architecture)):

                        if delta_generator:
                            delta_manifest_url = \
                                delta_generator.get_delta_manifest_url(image.digest)
                        else:
                            delta_manifest_url = None

                        # Clean up some information we don't want in the final output
                        image.diff_ids = []
                        image.pull_spec = None

                        index.add_image(repository.name, image, delta_manifest_url)

            index.write()

        if icon_store is not None:
            icon_store.clean()
