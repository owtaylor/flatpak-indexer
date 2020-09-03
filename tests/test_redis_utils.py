import pytest
import yaml

from flatpak_indexer.redis_utils import get_redis_client

from .redis import mock_redis
from .utils import get_config


CONFIG = yaml.safe_load("""
redis_url: redis://localhost:6379
redis_password: BRICK+SPINE+HORSE
koji_config: brew
""")


@pytest.fixture
def config(tmp_path):
    return get_config(tmp_path, CONFIG)


@mock_redis(expect_url='redis://:BRICK%2BSPINE%2BHORSE@localhost:6379')
def test_get_redis_client(config):
    redis_client = get_redis_client(config)
    redis_client.set("foo", b'42')
    assert redis_client.get("foo") == b'42'
