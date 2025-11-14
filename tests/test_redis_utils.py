from typing import Any, Dict, Optional
from unittest.mock import MagicMock, patch
import logging
import threading
import time

import pytest
import redis.client

from flatpak_indexer.redis_utils import RedisConfig, do_pubsub_work, get_redis_client
from flatpak_indexer.test.redis import mock_redis
import redis

CONFIG = """
redis_url: redis://localhost:6379
redis_password: BRICK+SPINE+HORSE
koji_config: brew
"""


@pytest.fixture
def config():
    return RedisConfig.from_str(CONFIG)


@mock_redis(expect_url="redis://:BRICK%2BSPINE%2BHORSE@localhost:6379")
def test_get_redis_client(config):
    redis_client = get_redis_client(config)
    redis_client.set("foo", b"42")
    assert redis_client.get("foo") == b"42"


class IffyPubSub(redis.client.PubSub):
    def __init__(self, fail_method, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if fail_method:
            method_mock = MagicMock(wraps=getattr(self, fail_method))
            setattr(self, fail_method, method_mock)
            method_mock.side_effect = redis.ConnectionError("Failed!")


@mock_redis
@pytest.mark.parametrize("fail_first_method", (None, "subscribe", "get_message"))
def test_do_pubsub_work(config, fail_first_method, caplog):
    def run_thread():
        redis_client = redis.Redis.from_url("redis://localhost")

        time.sleep(0.1)
        redis_client.publish("test:queue", b"foo")

    fill_queue_thread = threading.Thread(target=run_thread, name="fill-queue")

    fail_method = fail_first_method

    def get_pubsub(*args, **kwargs):
        nonlocal fail_method
        result = IffyPubSub(fail_method, *args, **kwargs)
        fail_method = None

        return result

    caplog.set_level(logging.INFO)

    with patch("redis.client.PubSub", get_pubsub):
        redis_client = get_redis_client(config)

        found_message: Optional[Dict[str, Any]] = None

        def do_work(pubsub: redis.client.PubSub):
            nonlocal found_message
            msg = pubsub.get_message()

            if msg and msg["type"] == "message":
                found_message = msg

            return found_message is None

        fill_queue_thread.start()
        if fail_first_method is None:
            do_pubsub_work(redis_client, "test:queue", do_work)
        else:
            do_pubsub_work(redis_client, "test:queue", do_work, initial_reconnect_timeout=0.05)

        assert found_message is not None
        assert found_message["data"] == b"foo"

        if fail_first_method is None:
            assert "sleeping" not in caplog.text
        elif fail_first_method == "subscribe":
            assert "Failed to connect to Redis, sleeping for 0.05 seconds" in caplog.text
        elif fail_first_method == "get_message":
            assert "Disconnected from Redis, sleeping for 0.05 seconds" in caplog.text
