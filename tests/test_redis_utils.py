import pytest
import yaml

from flatpak_indexer.redis_utils import get_redis_client

from .redis import mock_redis
from .utils import get_config


CONFIG = yaml.safe_load("""
redis_url: redis://localhost
redis_password: BRICK_SPINE_HORSE
koji_config: brew
""")


@pytest.fixture
def config(tmp_path):
    return get_config(tmp_path, CONFIG)


@mock_redis
def test_get_redis_client(config):
    redis_client = get_redis_client(config)
    redis_client.set("foo", b'42')
    assert redis_client.get("foo") == b'42'
