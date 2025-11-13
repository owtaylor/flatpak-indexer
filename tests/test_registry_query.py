from textwrap import dedent
import logging

from pytest import fixture

from flatpak_indexer.redis_utils import RedisConfig
from flatpak_indexer.registry_client import RegistryClient
from flatpak_indexer.registry_query import query_registry_image
from flatpak_indexer.session import Session
from flatpak_indexer.test.redis import mock_redis

from .registry import MockRegistry, mock_registry


@fixture
def session():
    config = RedisConfig.from_str(
        dedent("""
        redis_url: redis://localhost
""")
    )
    yield Session(config)


@mock_registry
@mock_redis
def test_query_registry_image(session, registry_mock: MockRegistry, caplog):
    manifest_digest, _ = registry_mock.add_fake_image(
        "repo1", "latest", labels={"org.flatpak.ref": "app/com.example.MyProg/x86_64/stable"}
    )
    caplog.set_level(logging.INFO)
    registry_client = RegistryClient("https://registry.example.com")

    image = query_registry_image(session, registry_client, "repo1", manifest_digest, "fake image")
    assert image.digest == manifest_digest

    assert "Fetching manifest and config for fake image" in caplog.text
    caplog.clear()

    # Try again, make sure cached in redis
    image = query_registry_image(session, registry_client, "repo1", manifest_digest, "fake image")
    assert image.digest == manifest_digest
    assert "Fetching manifest and config for fake image" not in caplog.text
