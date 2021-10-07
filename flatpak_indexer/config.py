from datetime import timedelta
import os
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse


from .base_config import BaseConfig, ConfigError, configfield, Lookup


class RegistryConfig(BaseConfig):
    name: str
    datasource: str
    public_url: str
    repositories: List[str] = []
    force_flatpak_token: bool = False

    def __init__(self, name: str, lookup: Lookup):
        self.name = name
        super().__init__(lookup)


class IndexConfig(BaseConfig):
    name: str
    output: str
    contents: Optional[str] = None
    registry: str
    tag: str
    koji_tags: List[str] = []
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


class DaemonConfig(BaseConfig):
    update_interval: timedelta = configfield(default=timedelta(minutes=30), force_suffix=False)

    def __init__(self, lookup: Lookup):
        super().__init__(lookup)


class Config(BaseConfig):
    indexes: List[IndexConfig] = configfield(skip=True)
    registries: Dict[str, RegistryConfig] = configfield(skip=True)

    koji_config: str
    pyxis_client_cert: Optional[str] = None
    pyxis_client_key: Optional[str] = None
    pyxis_url: Optional[str] = configfield(default=None, force_trailing_slash=True)

    redis_url: str
    redis_password: Optional[str] = None

    local_certs: Dict[str, str] = configfield(skip=True)

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

        if (not self.pyxis_client_cert) != (not self.pyxis_client_key):
            raise ConfigError("pyxis_client_cert and pyxis_client_key must be set together")

        if self.pyxis_client_cert and self.pyxis_client_key:
            if not os.path.exists(self.pyxis_client_cert):
                raise ConfigError(
                    "pyxis_client_cert: {} does not exist".format(self.pyxis_client_cert))
            if not os.path.exists(self.pyxis_client_key):
                raise ConfigError(
                    "pyxis_client_key: {} does not exist".format(self.pyxis_client_key))

        local_certs = lookup.get_str_dict('local_certs', {})
        self.local_certs = {}
        for k, v in local_certs.items():
            if not os.path.isabs(v):
                cert_dir = os.path.join(os.path.dirname(__file__), 'certs')
                v = os.path.join(cert_dir, v)

            if not os.path.exists(v):
                raise ConfigError("local_certs: {} does not exist".format(v))

            self.local_certs[k] = v

        if self.icons_dir is not None and self.icons_uri is None:
            raise ConfigError("icons_dir is configured, but not icons_uri")

        if self.deltas_dir is not None and self.deltas_uri is None:
            raise ConfigError("deltas_dir is configured, but not deltas_uri")

        for name, sublookup in lookup.iterate_objects('registries'):
            registry_config = RegistryConfig(name, sublookup)
            self.registries[name] = registry_config

            if registry_config.datasource not in ('pyxis', 'fedora'):
                raise ConfigError("registry/{}: datasource must be 'pyxis' or 'fedora'"
                                  .format(registry_config.name))

            if registry_config.datasource == 'pyxis':
                if self.pyxis_url is None:
                    raise ConfigError(("registry/{}: " +
                                       "pyxis_url must be configured for the pyxis datasource")
                                      .format(registry_config.name))

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

    def find_local_cert(self, url: str):
        hostname = urlparse(url).hostname
        if hostname is None:
            return None
        return self.local_certs.get(hostname)
