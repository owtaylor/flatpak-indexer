import yaml


class RegistryConfig:
    def __init__(self, name, attrs):
        self.name = name
        self.public_url = attrs['public_url']
        self.repositories = attrs['repositories']

    def __repr__(self):
        return 'IndexConfig(%r)' % self.__dict__


class IndexConfig:
    def __init__(self, name, attrs):
        self.name = name
        self.output = attrs['output']
        self.registry = attrs['registry']
        self.tag = attrs['tag']
        self.architecture = attrs.get('architecture', None)
        self.extract_icons = attrs.get('extract_icons', False)

    def __repr__(self):
        return 'IndexConfig(%r)' % self.__dict__


class DaemonConfig:
    def __init__(self, attrs):
        self.update_interval = attrs.get('update_interval', 1800)


class Config:
    def __init__(self, path):
        self.indexes = []
        self.registries = {}
        with open(path, 'r') as f:
            yml = yaml.safe_load(f)
            self.pyxis_url = yml['pyxis_url']
            if not self.pyxis_url.endswith('/'):
                self.pyxis_url += '/'
            self.pyxis_cert = yml.get('pyxis_cert')
            self.icons_dir = yml.get('icons_dir', None)
            self.icons_uri = yml.get('icons_uri', None)
            if self.icons_uri and not self.icons_uri.endswith('/'):
                self.icons_uri += '/'
            registries = yml.get('registries')
            if registries:
                for name, attrs in registries.items():
                    self.registries[name] = RegistryConfig(name, attrs)
            indexes = yml.get('indexes')
            if indexes:
                for name, attrs in indexes.items():
                    self.indexes.append(IndexConfig(name, attrs))

        self.daemon = DaemonConfig(yml.get('daemon', {}))
