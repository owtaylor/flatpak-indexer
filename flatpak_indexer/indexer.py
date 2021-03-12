import base64
import copy
import hashlib
import logging
import json
import os

from .cleaner import Cleaner
from .delta_generator import DeltaGenerator
from .utils import atomic_writer, path_for_digest, uri_for_digest
from .models import RegistryModel


logger = logging.getLogger(__name__)

DATA_URI_PREFIX = 'data:image/png;base64,'


class IconStore(object):
    def __init__(self, icons_dir, icons_uri, cleaner=None):
        self.icons_dir = icons_dir
        self.icons_uri = icons_uri
        self.cleaner = cleaner

    def store(self, uri):
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

    def move_flatpak_labels(self, image):
        to_move = [k for k in image.labels.keys()
                   if k.startswith('org.flatpak.') or k.startswith('org.freedesktop.')]

        for k in to_move:
            image.annotations[k] = image.labels[k]
            del image.labels[k]

    def add_image(self, name, image, delta_manifest_url):
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
    def __init__(self, config, cleaner=None):
        self.conf = config
        self.last_registry_data_hash = None
        if cleaner is None:
            cleaner = Cleaner(self.conf)
        self.cleaner = cleaner

    def _check_for_unchanged_data(self, registry_data):
        h = hashlib.sha256()
        for registry_name in sorted(registry_data.keys()):
            registry = registry_data[registry_name]
            h.update(registry_name.encode('utf-8'))
            h.update(json.dumps(registry.to_json(),
                                sort_keys=True, ensure_ascii=False).encode('utf-8'))

        unchanged = (h.digest() == self.last_registry_data_hash)
        self.last_registry_data_hash = h.digest()

        return unchanged

    def index(self, registry_data):
        # We always write a fresh index on the first run of the indexer, which makes
        # sure that the index is up-to-date with our code and config files. But on
        # subsequent runs, we short-circuit if nothing changed
        if self._check_for_unchanged_data(registry_data):
            logger.debug("Skipping indexing, queried data has not changed")
            return

        icon_store = None
        if self.conf.icons_dir is not None:
            icon_store = IconStore(self.conf.icons_dir, self.conf.icons_uri, cleaner=self.cleaner)

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
                                icon_store=icon_store)

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
