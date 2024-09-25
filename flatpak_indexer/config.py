from datetime import timedelta
import os
import re
from typing import Dict, List, Optional, Tuple

from flatpak_indexer.koji_utils import KojiConfig
from flatpak_indexer.odcs_query import OdcsConfig
from flatpak_indexer.redis_utils import RedisConfig
from .base_config import BaseConfig, ConfigError, configfield, Lookup


class RegistryConfig(BaseConfig):
    name: str
    datasource: str
    public_url: str
    force_flatpak_token: bool = False

    def __init__(self, name: str, lookup: Lookup):
        self.name = name
        super().__init__(lookup)


class PyxisRegistryConfig(RegistryConfig):
    pyxis_url: str = configfield(force_trailing_slash=True)
    pyxis_client_cert: Optional[str] = None
    pyxis_client_key: Optional[str] = None
    pyxis_registry: str

    repositories: List[str] = []
    repository_parse: Optional[str] = None
    repository_replace: Optional[str] = None

    def __init__(self, name: str, lookup: Lookup):
        super().__init__(name, lookup)

        if (not self.pyxis_client_cert) != (not self.pyxis_client_key):
            raise ConfigError("pyxis_client_cert and pyxis_client_key must be set together")

        if self.pyxis_client_cert and self.pyxis_client_key:
            if not os.path.exists(self.pyxis_client_cert):
                raise ConfigError(
                    "pyxis_client_cert: {} does not exist".format(self.pyxis_client_cert))
            if not os.path.exists(self.pyxis_client_key):
                raise ConfigError(
                    "pyxis_client_key: {} does not exist".format(self.pyxis_client_key))

        if (not self.repository_parse) != (not self.repository_replace):
            raise ConfigError("repository_parse and repository_replace must be set together")

    def adjust_repository(self, repository_name: str):
        if self.repository_parse and self.repository_replace:
            return re.sub('^' + self.repository_parse + '$',
                          self.repository_replace,
                          repository_name)
        else:
            return repository_name


class FedoraRegistryConfig(RegistryConfig):
    pass


class KojiRegistryConfig(RegistryConfig):
    pass


class IndexConfig(BaseConfig):
    name: str
    output: str
    contents: Optional[str] = None
    registry: str
    tag: str
    koji_tags: List[str] = []
    repository_priority: List[re.Pattern] = []
    bodhi_status: Optional[str] = None
    architecture: Optional[str] = None
    delta_keep: timedelta = configfield(skip=True)
    extract_icons: bool = False
    flatpak_annotations: bool = False

    def __init__(self, name: str, lookup: Lookup):
        self.name = name
        super().__init__(lookup)

        delta_keep = lookup.get_timedelta('delta_keep', None)
        if delta_keep is None:
            delta_keep_days = lookup.get_int('delta_keep_days', 0)
            delta_keep = timedelta(days=delta_keep_days)
        self.delta_keep = delta_keep

    def repository_priority_key(self, repository_name: str):
        for i, pattern in enumerate(self.repository_priority):
            if pattern.fullmatch(repository_name):
                return (i, repository_name)

        return (len(self.repository_priority), repository_name)


class DaemonConfig(BaseConfig):
    update_interval: timedelta = configfield(default=timedelta(minutes=30), force_suffix=False)

    def __init__(self, lookup: Lookup):
        super().__init__(lookup)


class Config(KojiConfig, OdcsConfig, RedisConfig):
    indexes: List[IndexConfig] = configfield(skip=True)
    registries: Dict[str, RegistryConfig] = configfield(skip=True)

    icons_dir: Optional[str] = None
    icons_uri: Optional[str] = configfield(default=None, force_trailing_slash=True)

    deltas_dir: Optional[str] = None
    deltas_uri: Optional[str] = configfield(default=None, force_trailing_slash=True)

    clean_files_after: timedelta = timedelta(days=1)

    daemon: DaemonConfig

    def __init__(self, lookup: Lookup):
        super().__init__(lookup)

        self.indexes = []
        self.registries = {}

        if self.icons_dir is not None and self.icons_uri is None:
            raise ConfigError("icons_dir is configured, but not icons_uri")

        if self.deltas_dir is not None and self.deltas_uri is None:
            raise ConfigError("deltas_dir is configured, but not deltas_uri")

        for name, sublookup in lookup.iterate_objects('registries'):
            datasource = sublookup.get_str('datasource')

            if datasource == "pyxis":
                registry_config = PyxisRegistryConfig(name, sublookup)
            elif datasource == "koji":
                registry_config = KojiRegistryConfig(name, sublookup)
            elif datasource == "fedora":
                registry_config = FedoraRegistryConfig(name, sublookup)
            else:
                raise ConfigError("registry/{}: datasource must be 'pyxis', 'koji', or 'fedora'"
                                  .format(name))

            self.registries[name] = registry_config

        tag_koji_tags: Dict[str, Tuple[str, List[str]]] = dict()

        for name, sublookup in lookup.iterate_objects('indexes'):
            index_config = IndexConfig(name, sublookup)
            self.indexes.append(index_config)

            registry_config2 = self.registries.get(index_config.registry)
            if not registry_config2:
                raise ConfigError("indexes/{}: No registry config found for {}"
                                  .format(index_config.name, index_config.registry))

            if index_config.extract_icons and self.icons_dir is None:
                raise ConfigError("indexes/{}: extract_icons is set, but no icons_dir is configured"
                                  .format(index_config.name))

            if index_config.delta_keep.total_seconds() > 0:
                if self.deltas_dir is None:
                    raise ConfigError(("indexes/{}: delta_keep is set, " +
                                       "but no deltas_dir is configured")
                                      .format(index_config.name))

            if registry_config2.datasource == 'pyxis':
                if index_config.bodhi_status is not None:
                    raise ConfigError(("indexes/{}: bodhi_status can only be set " +
                                       "for the fedora datasource")
                                      .format(index_config.name))

            if registry_config2.datasource == 'fedora':
                if index_config.bodhi_status not in ('testing', 'stable'):
                    raise ConfigError(("indexes/{}: bodhi_status must be set " +
                                       "to 'testing' or 'stable'")
                                      .format(index_config.name))

                if index_config.koji_tags:
                    raise ConfigError(("indexes/{}: koji_tags can only be set " +
                                       "for the pyxis datasource")
                                      .format(index_config.name))

            if index_config.tag in tag_koji_tags:
                old_name, old_koji_tags = tag_koji_tags[index_config.tag]
                if set(old_koji_tags) != set(index_config.koji_tags):
                    raise ConfigError(f"indexes/{old_name}, indexes/{index_config.name}: "
                                      "koji_tags must be consistent for indexes with the same tag")
            else:
                tag_koji_tags[index_config.tag] = (index_config.name, index_config.koji_tags)

    def get_indexes_for_datasource(self, datasource: str):
        return [
            i for i in self.indexes if self.registries[i.registry].datasource == datasource
        ]
