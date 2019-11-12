from copy import deepcopy
import os

from pytest import raises
import yaml

from flatpak_indexer.config import ConfigError
from .utils import get_config

BASIC_CONFIG = yaml.safe_load("""
pyxis_url: https://pyxis.example.com/v1
icons_dir: /flatpaks/icons/
icons_uri: https://flatpaks.example.com/icons
daemon:
    update_interval: 1800
registries:
    registry.example.com:
        repositories: ['repo1', 'repo2']
indexes:
    amd64:
        architecture: amd64
        registry: registry.example.com
        output: /flatpaks/flatpak-amd64.json
        tag: latest
        extract_icons: true
""")


def test_config_empty(tmp_path):
    with raises(ConfigError, match="Top level of config.yaml must be an object with keys"):
        get_config(tmp_path, None)


def test_config_basic(tmp_path):
    conf = get_config(tmp_path, BASIC_CONFIG)
    assert conf.pyxis_url == "https://pyxis.example.com/v1/"


def test_key_missing(tmp_path):
    config_data = deepcopy(BASIC_CONFIG)
    del config_data['pyxis_url']
    with raises(ConfigError, match="A value is required for pyxis_url"):
        get_config(tmp_path, config_data)


def test_str_type(tmp_path):
    config_data = deepcopy(BASIC_CONFIG)
    config_data['pyxis_url'] = 42
    with raises(ConfigError, match="pyxis_url must be a string"):
        get_config(tmp_path, config_data)


def test_bool_type(tmp_path):
    config_data = deepcopy(BASIC_CONFIG)
    config_data['indexes']['amd64']['extract_icons'] = 42
    with raises(ConfigError, match="indexes/amd64/extract_icons must be a boolean"):
        get_config(tmp_path, config_data)


def test_int_type(tmp_path):
    config_data = deepcopy(BASIC_CONFIG)
    config_data['daemon']['update_interval'] = "FOO"
    with raises(ConfigError, match="daemon/update_interval must be an integer"):
        get_config(tmp_path, config_data)


def test_str_list_type(tmp_path):
    config_data = deepcopy(BASIC_CONFIG)
    config_data['registries']['registry.example.com']['repositories'] = "FOO"
    with raises(ConfigError,
                match="registries/registry.example.com/repositories must be a list of strings"):
        get_config(tmp_path, config_data)


def test_environment_variable(tmp_path):
    os.environ["DOMAIN_NAME"] = 'pyxis.example.com'
    CONFIG = {
        'pyxis_url': 'https://${DOMAIN_NAME}/v1'
    }
    conf = get_config(tmp_path, CONFIG)
    assert conf.pyxis_url == "https://pyxis.example.com/v1/"


def test_environment_variable_default(tmp_path):
    if "DOMAIN_NAME" in os.environ:
        del os.environ["DOMAIN_NAME"]
    CONFIG = {
        'pyxis_url': 'https://${DOMAIN_NAME:pyxis.example.com}/v1'
    }
    conf = get_config(tmp_path, CONFIG)
    assert conf.pyxis_url == "https://pyxis.example.com/v1/"


def test_environment_variable_missing(tmp_path):
    if "DOMAIN_NAME" in os.environ:
        del os.environ["DOMAIN_NAME"]
    CONFIG = {
        'pyxis_url': 'https://${DOMAIN_NAME}/v1'
    }
    with raises(ConfigError, match=r'environment variable DOMAIN_NAME is not set'):
        get_config(tmp_path, CONFIG)


def test_cert_relative(tmp_path):
    config_data = deepcopy(BASIC_CONFIG)
    config_data['pyxis_cert'] = 'test.crt'
    conf = get_config(tmp_path, config_data)
    assert os.path.isabs(conf.pyxis_cert)
    assert os.path.exists(conf.pyxis_cert)


def test_cert_missing(tmp_path):
    config_data = deepcopy(BASIC_CONFIG)
    config_data['pyxis_cert'] = str(tmp_path / "nothere.crt")
    with raises(ConfigError, match="nothere.crt does not exist"):
        get_config(tmp_path, config_data)


def test_registry_missing(tmp_path):
    config_data = deepcopy(BASIC_CONFIG)
    del config_data['registries']['registry.example.com']
    with raises(ConfigError,
                match="indexes/amd64: No registry config found for registry.example.com"):
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
