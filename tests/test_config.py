from copy import deepcopy

from pytest import raises
import yaml

from flatpak_indexer.config import ConfigError, PyxisRegistryConfig
from .utils import get_config, setup_client_cert

BASIC_CONFIG = yaml.safe_load("""
redis_url: redis://localhost
koji_config: brew
deltas_dir: /flatpaks/deltas/
deltas_uri: https://flatpaks.example.com/deltas
icons_dir: /flatpaks/icons/
icons_uri: https://flatpaks.example.com/icons
daemon:
    update_interval: 30m
registries:
    production:
        repositories: ['repo1', 'repo2']
        public_url: https://registry.example.com/
        datasource: pyxis
        pyxis_url: https://pyxis.example.com/v1
        pyxis_registry: registry.example.com
    production2:
        public_url: https://registry.example.com/
        datasource: pyxis
        pyxis_url: https://pyxis.example.com/v1
        pyxis_registry: registry.example.com
        repository_parse: (?P<namespace>[^/]+)/(?P<name>.*)
        repository_replace: pending/\\g<namespace>----\\g<name>
    brew:
        public_url: https://private.example.com/
        datasource: koji
    fedora:
        public_url: https://registry.fedoraproject.org/
        datasource: fedora
indexes:
    amd64:
        architecture: amd64
        registry: production
        output: /flatpaks/flatpak-amd64.json
        repository_priority: ["rhel9/.*", "rhel10/.*"]
        tag: latest
        extract_icons: true
    beta:
        registry: production
        output: /flatpaks/flatpak-beta.json
        tag: latest
        repository_include: [".*-beta/.*"]
        repository_exclude: ["rhel7-beta/.*"]
    brew-rc:
        registry: brew
        output: /flatpaks/rc-amd64.json
        tag: release-candidate
        koji_tags: [release-candidate]
    brew-rc-amd64:
        architecture: amd64
        registry: brew
        output: /flatpaks/rc-amd64.json
        tag: release-candidate
        koji_tags: [release-candidate]
    fedora-testing:
        registry: fedora
        output: /fedora/flatpak-testing.json
        bodhi_status: testing
        tag: testing
        delta_keep: 7d
""")


def test_config_basic(tmp_path):
    conf = get_config(tmp_path, BASIC_CONFIG)
    registry = conf.registries["production"]
    assert isinstance(registry, PyxisRegistryConfig)
    assert registry.pyxis_url == "https://pyxis.example.com/v1/"

    index_conf = next(i for i in conf.indexes if i.name == "amd64")
    assert index_conf.repository_priority_key("rhel9/inkscape") == (0, "rhel9/inkscape")
    assert index_conf.repository_priority_key("foobar") == (2, "foobar")

    index_conf = next(i for i in conf.indexes if i.name == "beta")
    assert index_conf.should_include_repository("rhel9/inkscape") is False
    assert index_conf.should_include_repository("rhel10-beta/inkscape") is True
    assert index_conf.should_include_repository("rhel7-beta/inkscape") is False

    registry_conf = conf.registries["production"]
    assert isinstance(registry_conf, PyxisRegistryConfig)
    assert registry_conf.adjust_repository("product/repo") == "product/repo"

    registry_conf2 = conf.registries["production2"]
    assert isinstance(registry_conf2, PyxisRegistryConfig)
    assert registry_conf2.adjust_repository("product/repo") == "pending/product----repo"

    fedora_indexes = conf.get_indexes_for_datasource("fedora")
    assert len(fedora_indexes) == 1
    assert fedora_indexes[0].name == "fedora-testing"


def create_client_key_config(tmp_path, create_cert=True, create_key=True):
    config_data = deepcopy(BASIC_CONFIG)
    registry_config_data = config_data['registries']['production']
    registry_config_data['pyxis_client_cert'], registry_config_data['pyxis_client_key'] = \
        setup_client_cert(tmp_path, create_cert=create_cert, create_key=create_key)

    return config_data


def test_client_cert(tmp_path):
    config_data = create_client_key_config(tmp_path)

    config = get_config(tmp_path, config_data)
    registry = config.registries["production"]
    assert isinstance(registry, PyxisRegistryConfig)
    assert registry.pyxis_client_cert == str(tmp_path / "client.crt")
    assert registry.pyxis_client_key == str(tmp_path / "client.key")


def test_client_cert_missing(tmp_path):
    config_data = create_client_key_config(tmp_path, create_cert=False)

    with raises(ConfigError,
                match="client.crt does not exist"):
        get_config(tmp_path, config_data)


def test_client_key_missing(tmp_path):
    config_data = create_client_key_config(tmp_path, create_key=False)

    with raises(ConfigError,
                match="client.key does not exist"):
        get_config(tmp_path, config_data)


def test_client_key_mismatch(tmp_path):
    config_data = create_client_key_config(tmp_path)
    del config_data['registries']['production']['pyxis_client_cert']

    with raises(ConfigError,
                match="pyxis_client_cert and pyxis_client_key must be set together"):
        get_config(tmp_path, config_data)


def test_pyxis_url_missing(tmp_path):
    config_data = deepcopy(BASIC_CONFIG)
    del config_data['registries']['production']['pyxis_url']

    with raises(ConfigError,
                match=r'A value is required for registries/production/pyxis_url'):
        get_config(tmp_path, config_data)


def test_datasource_invalid(tmp_path):
    config_data = deepcopy(BASIC_CONFIG)
    config_data['registries']['production']['datasource'] = 'INVALID'
    with raises(ConfigError,
                match=("registry/production: "
                       "datasource must be 'pyxis', 'koji', or 'fedora'")):
        get_config(tmp_path, config_data)


def test_repository_parse_replace_mismatched(tmp_path):
    config_data = deepcopy(BASIC_CONFIG)
    del config_data['registries']['production2']['repository_replace']
    with raises(ConfigError,
                match=(r"registries/production2: "
                       r"repository_parse and repository_replace must be set together")):
        get_config(tmp_path, config_data)


def test_public_url_not_https(tmp_path):
    config_data = deepcopy(BASIC_CONFIG)
    config_data['registries']['production']['public_url'] = 'ftp://registry.example.com'
    with raises(ConfigError,
                match=(r"registries/production: "
                       r"public_url must be a https:// URL")):
        get_config(tmp_path, config_data)


def test_registry_missing(tmp_path):
    config_data = deepcopy(BASIC_CONFIG)
    del config_data['registries']['production']
    with raises(ConfigError,
                match="indexes/amd64: No registry config found for production"):
        get_config(tmp_path, config_data)


def test_koji_tags_consistent(tmp_path):
    config_data = deepcopy(BASIC_CONFIG)
    config_data['indexes']['brew-rc-amd64']['koji_tags'] = ['release-candidate', 'released']
    with raises(ConfigError,
                match=("indexes/brew-rc, indexes/brew-rc-amd64: "
                       "koji_tags must be consistent for indexes with the same tag")):
        get_config(tmp_path, config_data)


def test_koji_tags_extra(tmp_path):
    config_data = deepcopy(BASIC_CONFIG)
    config_data['indexes']['fedora-testing']['koji_tags'] = ['f30']
    with raises(ConfigError,
                match="indexes/fedora-testing: koji_tags can only be set for the pyxis datasource"):
        get_config(tmp_path, config_data)


def test_bodhi_status_extra(tmp_path):
    config_data = deepcopy(BASIC_CONFIG)
    config_data['indexes']['amd64']['bodhi_status'] = 'stable'
    with raises(ConfigError,
                match="indexes/amd64: bodhi_status can only be set for the fedora datasource"):
        get_config(tmp_path, config_data)


def test_bodhi_status_invalid(tmp_path):
    config_data = deepcopy(BASIC_CONFIG)
    config_data['indexes']['fedora-testing']['bodhi_status'] = 'INVALID'
    with raises(ConfigError,
                match="indexes/fedora-testing: bodhi_status must be set to 'testing' or 'stable'"):
        get_config(tmp_path, config_data)


def test_icons_dir_missing(tmp_path):
    config_data = deepcopy(BASIC_CONFIG)
    del config_data['icons_dir']
    with raises(ConfigError,
                match="indexes/amd64: extract_icons is set, but no icons_dir is configured"):
        get_config(tmp_path, config_data)


def test_icons_uri_missing(tmp_path):
    config_data = deepcopy(BASIC_CONFIG)
    del config_data['icons_uri']
    with raises(ConfigError, match="icons_dir is configured, but not icons_uri"):
        get_config(tmp_path, config_data)


def test_deltas_dir_missing(tmp_path):
    config_data = deepcopy(BASIC_CONFIG)
    del config_data['deltas_dir']
    with raises(ConfigError,
                match=("indexes/fedora-testing: " +
                       "delta_keep is set, but no deltas_dir is configured")):
        get_config(tmp_path, config_data)


def test_deltas_uri_missing(tmp_path):
    config_data = deepcopy(BASIC_CONFIG)
    del config_data['deltas_uri']
    with raises(ConfigError, match="deltas_dir is configured, but not deltas_uri"):
        get_config(tmp_path, config_data)
