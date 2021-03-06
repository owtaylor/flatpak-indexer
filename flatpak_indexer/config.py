import datetime
from enum import Enum
import os
import re
from urllib.parse import urlparse

import yaml

from .utils import substitute_env_vars


class ConfigError(Exception):
    pass


class Defaults(Enum):
    REQUIRED = 1


class RegistryConfig:
    def __init__(self, name, attrs):
        self.name = name
        self.datasource = attrs.get_str('datasource')
        self.public_url = attrs.get_str('public_url')
        self.repositories = attrs.get_str_list('repositories', [])
        self.force_flatpak_token = attrs.get_bool('force_flatpak_token', False)


class IndexConfig:
    def __init__(self, name, lookup):
        self.name = name
        self.output = lookup.get_str('output')
        self.registry = lookup.get_str('registry')
        self.tag = lookup.get_str('tag')
        self.koji_tags = lookup.get_str_list('koji_tags', [])
        self.bodhi_status = lookup.get_str('bodhi_status', None)
        self.architecture = lookup.get_str('architecture', None)
        self.delta_keep = lookup.get_timedelta('delta_keep', None)
        if self.delta_keep is None:
            delta_keep_days = lookup.get_int('delta_keep_days', 0)
            self.delta_keep = datetime.timedelta(days=delta_keep_days)
        self.extract_icons = lookup.get_bool('extract_icons', False)
        self.flatpak_annotations = lookup.get_bool('flatpak_annotations', False)


class DaemonConfig:
    def __init__(self, lookup):
        self.update_interval = lookup.get_timedelta('update_interval', '30m',
                                                    force_suffix=False)


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
                raise ConfigError("A value is required for {}".format(self._get_path(key))) \
                    from None
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

    def get_str_dict(self, key, default=Defaults.REQUIRED):
        val = self._get(key, default)
        if not isinstance(val, dict) or not all(isinstance(v, str) for v in val.values()):
            raise ConfigError("{} must be a mapping with string values".format(self._get_path(key)))

        return {substitute_env_vars(k): substitute_env_vars(v) for k, v in val.items()}

    def get_timedelta(self, key, default=Defaults.REQUIRED, force_suffix=True):
        val = self._get(key, default)
        if default is None and val is None:
            return None

        if isinstance(val, int) and not force_suffix:
            return datetime.timedelta(seconds=val)

        if isinstance(val, str):
            m = re.match(r'^(\d+)([dhms])$', val)
            if m:
                if m.group(2) == "d":
                    return datetime.timedelta(days=int(m.group(1)))
                elif m.group(2) == "h":
                    return datetime.timedelta(hours=int(m.group(1)))
                elif m.group(2) == "m":
                    return datetime.timedelta(minutes=int(m.group(1)))
                else:
                    return datetime.timedelta(seconds=int(m.group(1)))

        raise ConfigError("{} should be a time interval of the form <digits>[dhms]"
                          .format(self._get_path(key)))


class Config:
    def __init__(self, path):
        self.indexes = []
        self.registries = {}
        with open(path, 'r') as f:
            yml = yaml.safe_load(f)

        if not isinstance(yml, dict):
            raise ConfigError("Top level of config.yaml must be an object with keys")

        lookup = Lookup(yml)

        self.pyxis_url = lookup.get_str('pyxis_url', None)
        if self.pyxis_url is not None and not self.pyxis_url.endswith('/'):
            self.pyxis_url += '/'

        self.pyxis_client_cert = lookup.get_str('pyxis_client_cert', None)
        self.pyxis_client_key = lookup.get_str('pyxis_client_key', None)

        self.redis_url = lookup.get_str('redis_url')
        self.redis_password = lookup.get_str('redis_password', None)

        if (not self.pyxis_client_cert) != (not self.pyxis_client_key):
            raise ConfigError("pyxis_client_cert and pyxis_client_key must be set together")

        if self.pyxis_client_cert:
            if not os.path.exists(self.pyxis_client_cert):
                raise ConfigError(
                    "pyxis_client_cert: {} does not exist".format(self.pyxis_client_cert))
            if not os.path.exists(self.pyxis_client_key):
                raise ConfigError(
                    "pyxis_client_key: {} does not exist".format(self.pyxis_client_key))

        self.koji_config = lookup.get_str('koji_config')

        local_certs = lookup.get_str_dict('local_certs', {})
        self.local_certs = {}
        for k, v in local_certs.items():
            if not os.path.isabs(v):
                cert_dir = os.path.join(os.path.dirname(__file__), 'certs')
                v = os.path.join(cert_dir, v)

            if not os.path.exists(v):
                raise ConfigError("local_certs: {} does not exist".format(v))

            self.local_certs[k] = v

        self.icons_dir = lookup.get_str('icons_dir', None)
        self.icons_uri = lookup.get_str('icons_uri', None)
        if self.icons_uri and not self.icons_uri.endswith('/'):
            self.icons_uri += '/'

        if self.icons_dir is not None and self.icons_uri is None:
            raise ConfigError("icons_dir is configured, but not icons_uri")

        self.deltas_dir = lookup.get_str('deltas_dir', None)
        self.deltas_uri = lookup.get_str('deltas_uri', None)
        if self.deltas_uri and not self.deltas_uri.endswith('/'):
            self.deltas_uri += '/'

        if self.deltas_dir is not None and self.deltas_uri is None:
            raise ConfigError("deltas_dir is configured, but not deltas_uri")

        self.clean_files_after = lookup.get_timedelta('clean_files_after', '1d')

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

        tag_koji_tags = dict()

        for name, sublookup in lookup.iterate_objects('indexes'):
            index_config = IndexConfig(name, sublookup)
            self.indexes.append(index_config)

            registry_config = self.registries.get(index_config.registry)
            if not registry_config:
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

            if registry_config.datasource == 'pyxis':
                if index_config.bodhi_status is not None:
                    raise ConfigError(("indexes/{}: bodhi_status can only be set " +
                                       "for the fedora datasource")
                                      .format(index_config.name))

            if registry_config.datasource == 'fedora':
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

        self.daemon = DaemonConfig(Lookup(yml.get('daemon', {}), 'daemon'))

    def find_local_cert(self, url):
        return self.local_certs.get(urlparse(url).hostname)
