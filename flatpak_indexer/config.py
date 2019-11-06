from enum import Enum
import yaml


class Defaults(Enum):
    REQUIRED = 1


class RegistryConfig:
    def __init__(self, name, attrs):
        self.name = name
        self.public_url = attrs.get_str('public_url', None)
        self.repositories = attrs.get_str_list('repositories', None)

    def __repr__(self):
        return 'IndexConfig(%r)' % self.__dict__


class IndexConfig:
    def __init__(self, name, lookup):
        self.name = name
        self.output = lookup.get_str('output')
        self.registry = lookup.get_str('registry')
        self.tag = lookup.get_str('tag')
        self.architecture = lookup.get_str('architecture', None)
        self.extract_icons = lookup.get_bool('extract_icons', False)

    def __repr__(self):
        return 'IndexConfig(%r)' % self.__dict__


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
                raise RuntimeError("A value is required for {}".format(self._get_path(key)))
        else:
            return self.attrs.get(key, default)

    def get_str(self, key, default=Defaults.REQUIRED):
        val = self._get(key, default)
        if not (isinstance(val, str) or (default is None and val is None)):
            raise RuntimeError("{} must be a string".format(self._get_path(key)))

        return val

    def get_bool(self, key, default=Defaults.REQUIRED):
        val = self._get(key, default)
        if not isinstance(val, bool):
            raise RuntimeError("{} must be a boolean".format(self._get_path(key)))

        return val

    def get_int(self, key, default=Defaults.REQUIRED):
        val = self._get(key, default)
        if not isinstance(val, int):
            raise RuntimeError("{} must be an integer".format(self._get_path(key)))

        return val

    def get_str_list(self, key, default=Defaults.REQUIRED):
        val = self._get(key, default)
        if not isinstance(val, list) or not all(isinstance(v, str) for v in val):
            raise RuntimeError("{} must be a list of strings".format(self._get_path(key)))

        return val


class Config:
    def __init__(self, path):
        self.indexes = []
        self.registries = {}
        with open(path, 'r') as f:
            yml = yaml.safe_load(f)
            lookup = Lookup(yml)

            self.pyxis_url = lookup.get_str('pyxis_url')
            if not self.pyxis_url.endswith('/'):
                self.pyxis_url += '/'
            self.pyxis_cert = lookup.get_str('pyxis_cert', None)
            self.icons_dir = lookup.get_str('icons_dir', None)
            self.icons_uri = lookup.get_str('icons_uri', None)
            if self.icons_uri and not self.icons_uri.endswith('/'):
                self.icons_uri += '/'
            for name, sublookup in lookup.iterate_objects('registries'):
                self.registries[name] = RegistryConfig(name, sublookup)
            for name, sublookup in lookup.iterate_objects('indexes'):
                self.indexes.append(IndexConfig(name, sublookup))

        self.daemon = DaemonConfig(Lookup(yml.get('daemon', {}), 'daemon'))
