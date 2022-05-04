import copy
from datetime import timedelta
import os
from textwrap import dedent
from typing import Dict, List, Optional

import pytest
from pytest import raises
import yaml

from flatpak_indexer.base_config import BaseConfig, ConfigError, configfield, Lookup
from flatpak_indexer.utils import SubstitutionError


CONFIG = """
str_field: foo
int_field: 42
bool_field: False
str_list_field: [foo, bar, baz]
str_dict_field:
    foo: bar
    baz: wow
timedelta_field: 42s
subconfig:
    str_field: baz
subconfigs:
    a:
        str_field: a
    b:
        str_field: b
"""

CONFIG_DATA = yaml.safe_load(CONFIG)


class SubConfig(BaseConfig):
    str_field: str


class Config(BaseConfig):
    str_field: str
    int_field: int
    bool_field: bool
    str_list_field: List[str]
    str_dict_field: Dict[str, str]
    timedelta_field: timedelta

    subconfig: SubConfig
    subconfigs: Dict[str, SubConfig] = configfield(skip=True)


@pytest.fixture
def config_data():
    return copy.deepcopy(CONFIG_DATA)


def test_basic(config_data):
    lookup = Lookup(config_data)
    config = Config(lookup)

    config.subconfigs = {}
    for name, sublookup in lookup.iterate_objects("subconfigs"):
        config.subconfigs[name] = SubConfig(sublookup)

    assert config.str_field == "foo"
    assert config.int_field == 42
    assert config.bool_field is False
    assert config.str_list_field == ["foo", "bar", "baz"]
    assert config.str_dict_field == {"foo": "bar", "baz": "wow"}
    assert config.timedelta_field.total_seconds() == 42
    assert config.subconfig.str_field == "baz"
    assert config.subconfigs["a"].str_field == "a"


def test_from_path(tmpdir):
    config_file = tmpdir / "config.yaml"
    with open(config_file, "w") as f:
        f.write(CONFIG)

    config = Config.from_path(config_file)
    assert config.str_field == "foo"


def test_from_path_not_object(tmpdir):
    config_file = tmpdir / "testconfig.yaml"
    with open(config_file, "w") as f:
        f.write("42")

    with raises(ConfigError, match="Top level of testconfig.yaml must be an object with keys"):
        Config.from_path(config_file)


def test_from_path_empty(tmpdir):
    class Config(BaseConfig):
        str_field: str = "foo"

    config_file = tmpdir / "testconfig.yaml"
    with open(config_file, "w"):
        pass

    config = Config.from_path(config_file)
    assert config.str_field == "foo"


def test_from_str(tmpdir):
    config = Config.from_str(CONFIG)
    assert config.str_field == "foo"


def test_from_str_empty():
    class Config(BaseConfig):
        str_field: str = "foo"

    config = Config.from_str("")
    assert config.str_field == "foo"


def test_from_str_not_object(tmpdir):
    with raises(ConfigError, match="Top level of config must be an object with keys"):
        Config.from_str("42")


def test_field_defaults():
    class Config(BaseConfig):
        str_field: str = "foo_default"

    lookup = Lookup({})
    config = Config(lookup)

    assert config.str_field == "foo_default"


def test_inheritance():
    class Config1(BaseConfig):
        field1: str

    class Config2(BaseConfig):
        field2: str

    class Config3(Config1, Config2):
        pass

    config = Config3.from_str(dedent("""
        field1: foo
        field2: bar
    """))

    assert config.field1 == "foo"
    assert config.field2 == "bar"


def test_config_field():
    class Config(BaseConfig):
        str_field: str = configfield(default="foo_default")
        skip_field: str = configfield(skip=True)

    lookup = Lookup({})
    config = Config(lookup)

    assert config.str_field == "foo_default"


def test_missing_field(config_data):
    del config_data["str_field"]
    lookup = Lookup(config_data)
    with raises(ConfigError, match=r"A value is required for str_field"):
        Config(lookup)


def test_str_field_invalid(config_data):
    config_data["str_field"] = 42
    lookup = Lookup(config_data)
    with raises(ConfigError, match=r"str_field must be a string"):
        Config(lookup)


def test_bool_field_invalid(config_data):
    config_data["bool_field"] = "foo"
    lookup = Lookup(config_data)
    with raises(ConfigError, match=r"bool_field must be a boolean"):
        Config(lookup)


def test_int_field_invalid(config_data):
    config_data["int_field"] = "foo"
    lookup = Lookup(config_data)
    with raises(ConfigError, match=r"int_field must be an integer"):
        Config(lookup)


def test_str_list_field_invalid(config_data):
    config_data["str_list_field"] = "foo"
    lookup = Lookup(config_data)
    with raises(ConfigError, match=r"str_list_field must be a list of strings"):
        Config(lookup)

    config_data["str_list_field"] = [1, 2, 3]
    lookup = Lookup(config_data)
    with raises(ConfigError, match=r"str_list_field must be a list of strings"):
        Config(lookup)


def test_str_dict_field_invalid(config_data):
    config_data["str_dict_field"] = "foo"
    lookup = Lookup(config_data)
    with raises(ConfigError, match=r"str_dict_field must be a mapping with string values"):
        Config(lookup)

    config_data["str_dict_field"] = {"foo": 1}
    lookup = Lookup(config_data)
    with raises(ConfigError, match=r"str_dict_field must be a mapping with string values"):
        Config(lookup)


def test_timedelta_field_invalid(config_data):
    config_data["timedelta_field"] = 100
    lookup = Lookup(config_data)
    with raises(
            ConfigError,
            match=r"timedelta_field should be a time interval of the form \<digits\>\[dhms\]"):
        Config(lookup)


def test_subconfig_invalid(config_data):
    config_data["subconfigs"]["a"]["str_field"] = 42
    lookup = Lookup(config_data)

    with pytest.raises(ConfigError, match=r"subconfigs/a/str_field must be a string"):
        for name, sublookup in lookup.iterate_objects("subconfigs"):
            SubConfig(sublookup)


def test_str_field_optional():
    class Config(BaseConfig):
        str_field: Optional[str] = None

    lookup = Lookup({})
    assert Config(lookup).str_field is None
    lookup = Lookup({"str_field": "foo"})
    assert Config(lookup).str_field == "foo"


def test_trailing_slash():
    class Config(BaseConfig):
        str_field: str = configfield(force_trailing_slash=True)

    lookup = Lookup({"str_field": "ab"})
    assert Config(lookup).str_field == "ab/"

    lookup = Lookup({"str_field": "ab/"})
    assert Config(lookup).str_field == "ab/"


@pytest.mark.parametrize('input,expected_seconds', [
    ('1s', 1),
    ('1m', 60),
    ('1h', 60 * 60),
    ('1d', 24 * 60 * 60),
    (42, 42),
])
def test_timedelta_formats(input, expected_seconds):
    class Config(BaseConfig):
        timedelta_field: timedelta = configfield(force_suffix=False)

    lookup = Lookup({"timedelta_field": input})
    assert Config(lookup).timedelta_field.total_seconds() == expected_seconds


def test_timedelta_default():
    class Config(BaseConfig):
        timedelta_field: timedelta = configfield(default=timedelta(days=1))

    lookup = Lookup({})
    assert Config(lookup).timedelta_field == timedelta(days=1)


def test_timedelta_optional():
    class Config(BaseConfig):
        timedelta_field: Optional[timedelta] = None

    lookup = Lookup({})
    assert Config(lookup).timedelta_field is None
    lookup = Lookup({"timedelta_field": "42s"})
    timedelta_field = Config(lookup).timedelta_field
    assert timedelta_field is not None
    assert timedelta_field.total_seconds() == 42


def test_iterate_objects_none():
    lookup = Lookup({})
    result = [x for x in lookup.iterate_objects("not_there")]
    assert result == []


def test_environment_variable(config_data):
    config_data["str_field"] = "<${FOO}>"
    os.environ["FOO"] = 'foo'
    lookup = Lookup(config_data)
    assert Config(lookup).str_field == "<foo>"


def test_environment_variable_default(config_data):
    if "FOO" in os.environ:
        del os.environ["FOO"]
    config_data["str_field"] = "<${FOO:default}>"
    lookup = Lookup(config_data)
    assert Config(lookup).str_field == "<default>"


def test_environment_variable_missing(config_data):
    if "FOO" in os.environ:
        del os.environ["FOO"]
    config_data["str_field"] = "<${FOO}>"
    lookup = Lookup(config_data)
    with raises(SubstitutionError, match=r'environment variable FOO is not set'):
        Config(lookup)


def test_bad_field_type():
    class Config(BaseConfig):
        set_field: set

    lookup = Lookup({})
    with raises(RuntimeError, match=r"Don't know how to handle config field of type <class 'set'>"):
        Config(lookup)
