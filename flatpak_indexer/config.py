from enum import Enum
import os
import yaml


from .utils import substitute_env_vars


class ConfigError(Exception):
    pass


class Defaults(Enum):
    REQUIRED = 1


class RegistryConfig:
    def __init__(self, name, attrs):
        self.name = name
        self.public_url = attrs.get_str('public_url')
        self.repositories = attrs.get_str_list('repositories', [])
        self.koji_config = attrs.get_str('koji_config', None)


class IndexConfig:
    def __init__(self, name, lookup):
        self.name = name
        self.output = lookup.get_str('output')
        self.registry = lookup.get_str('registry')
        self.tag = lookup.get_str('tag', None)
        self.koji_tag = lookup.get_str('koji_tag', None)
        self.architecture = lookup.get_str('architecture', None)
        self.extract_icons = lookup.get_bool('extract_icons', False)


class DaemonConfig:
    def __init__(self, lookup):
        self.update_interval = lookup.get_int('update_interval', 1800)


class Lookup:
    def __init__(self, attrs, path=None):
        self.path = path
        self.attrs = attrs

    def _get_path(self, key):
        if self.path is not None:
            return self.path + '/' + key
        else:
            return key

    def iterate_objects(self, parent_key):
        objects = self.attrs.get(parent_key)
        if not objects:
            return

        for name, attrs in objects.items():
            yield name, Lookup(attrs, parent_key + '/' + name)

    def _get(self, key, default):
        if default is Defaults.REQUIRED:
            try:
                return self.attrs[key]
            except KeyError:
                raise ConfigError("A value is required for {}".format(self._get_path(key)))
        else:
            return self.attrs.get(key, default)

    def get_str(self, key, default=Defaults.REQUIRED):
        val = self._get(key, default)
        if default is None and val is None:
            return None

        if not isinstance(val, str):
            raise ConfigError("{} must be a string".format(self._get_path(key)))

        return substitute_env_vars(val)

    def get_bool(self, key, default=Defaults.REQUIRED):
        val = self._get(key, default)
        if not isinstance(val, bool):
            raise ConfigError("{} must be a boolean".format(self._get_path(key)))

        return val

    def get_int(self, key, default=Defaults.REQUIRED):
        val = self._get(key, default)
        if not isinstance(val, int):
            raise ConfigError("{} must be an integer".format(self._get_path(key)))

        return val

    def get_str_list(self, key, default=Defaults.REQUIRED):
        val = self._get(key, default)
        if not isinstance(val, list) or not all(isinstance(v, str) for v in val):
            raise ConfigError("{} must be a list of strings".format(self._get_path(key)))

        return [substitute_env_vars(v) for v in val]


class Config:
    def __init__(self, path):
        self.indexes = []
        self.registries = {}
        with open(path, 'r') as f:
            yml = yaml.safe_load(f)

        if not isinstance(yml, dict):
            raise ConfigError("Top level of config.yaml must be an object with keys")

        lookup = Lookup(yml)

        self.pyxis_url = lookup.get_str('pyxis_url')
        if not self.pyxis_url.endswith('/'):
            self.pyxis_url += '/'
        self.pyxis_cert = lookup.get_str('pyxis_cert', None)
        if self.pyxis_cert is not None:
            if not os.path.isabs(self.pyxis_cert):
                cert_dir = os.path.join(os.path.dirname(__file__), 'certs')
                self.pyxis_cert = os.path.join(cert_dir, self.pyxis_cert)

            if not os.path.exists(self.pyxis_cert):
                raise ConfigError("pyxis_cert: {} does not exist".format(self.pyxis_cert))
        self.pyxis_client_cert = lookup.get_str('pyxis_client_cert', None)
        self.pyxis_client_key = lookup.get_str('pyxis_client_key', None)

        if (not self.pyxis_client_cert) != (not self.pyxis_client_key):
            raise ConfigError("pyxis_client_cert and pyxis_client_key must be set together")

        if self.pyxis_client_cert:
            if not os.path.exists(self.pyxis_client_cert):
                raise ConfigError(
                    "pyxis_client_cert: {} does not exist".format(self.pyxis_client_cert))
            if not os.path.exists(self.pyxis_client_key):
                raise ConfigError(
                    "pyxis_client_key: {} does not exist".format(self.pyxis_client_key))

        self.icons_dir = lookup.get_str('icons_dir', None)
        self.icons_uri = lookup.get_str('icons_uri', None)
        if self.icons_uri and not self.icons_uri.endswith('/'):
            self.icons_uri += '/'

        if self.icons_dir is not None and self.icons_uri is None:
            raise ConfigError("icons_dir is configured, but not icons_uri")

        for name, sublookup in lookup.iterate_objects('registries'):
            registry_config = RegistryConfig(name, sublookup)
            self.registries[name] = registry_config

            if registry_config.koji_config and registry_config.repositories:
                raise ConfigError("registries/{}: koji_config and repositories cannot both be set"
                                  .format(registry_config.name))

        for name, sublookup in lookup.iterate_objects('indexes'):
            index_config = IndexConfig(name, sublookup)
            self.indexes.append(index_config)

            registry_config = self.registries.get(index_config.registry)
            if not registry_config:
                raise ConfigError("indexes/{}: No registry config found for {}"
                                  .format(index_config.name, index_config.registry))

            if index_config.koji_tag and not registry_config.koji_config:
                raise ConfigError(
                    "indexes/{}: koji_tag is set, but koji_config missing for registry"
                    .format(index_config.name))

            if index_config.tag and index_config.koji_tag:
                raise ConfigError("indexes/{}: tag and koji_tag cannot both be set"
                                  .format(index_config.name))

            if not (index_config.tag or index_config.koji_tag):
                raise ConfigError("indexes/{}: One of tag or koji_tag must be set"
                                  .format(index_config.name))

            if index_config.extract_icons and self.icons_dir is None:
                raise ConfigError("indexes/{}: extract_icons is set, but no icons_dir is configured"
                                  .format(index_config.name))

        self.daemon = DaemonConfig(Lookup(yml.get('daemon', {}), 'daemon'))
