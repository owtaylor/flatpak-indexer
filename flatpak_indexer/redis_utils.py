import logging
import time
from typing import Optional
from urllib.parse import quote, urlparse, urlunparse

import redis

from .base_config import BaseConfig

logger = logging.getLogger(__name__)


class RedisConfig(BaseConfig):
    redis_url: str
    redis_password: Optional[str] = None


def get_redis_client(config: RedisConfig) -> "redis.Redis[bytes]":
    url = config.redis_url

    # redis.Redis.from_url() doesn't support passing the password separately
    # https://github.com/andymccurdy/redis-py/issues/1347

    password = config.redis_password
    if password:
        parts = urlparse(url)
        netloc = f':{quote(password)}@{parts.hostname}'
        if parts.port is not None:
            netloc += f':{parts.port}'

        url = urlunparse((parts.scheme, netloc,
                          parts.path, parts.params, parts.query, parts.fragment))

    return redis.Redis.from_url(url, decode_responses=False)  # type: ignore


def do_pubsub_work(redis_client, topic, callback, initial_reconnect_timeout=None):
    pubsub = None

    INITIAL_RECONNECT_TIMEOUT = 5
    MAX_RECONNECT_TIMEOUT = 120

    reconnect_timeout = initial_reconnect_timeout
    if reconnect_timeout is None:
        reconnect_timeout = INITIAL_RECONNECT_TIMEOUT

    try:
        while True:
            try:
                if pubsub is None:
                    pubsub = redis_client.pubsub()
                    pubsub.subscribe(topic)

                    logger.info("Subscribed to %s", topic)

                if not callback(pubsub):
                    break

                reconnect_timeout = INITIAL_RECONNECT_TIMEOUT
            except redis.ConnectionError:
                if pubsub and pubsub.connection:
                    logger.info("Disconnected from Redis, sleeping for %g seconds",
                                reconnect_timeout)
                else:
                    logger.info("Failed to connect to Redis, sleeping for %g seconds",
                                reconnect_timeout)

                if pubsub:
                    pubsub.close()
                    pubsub = None

                time.sleep(reconnect_timeout)
                reconnect_timeout = min(MAX_RECONNECT_TIMEOUT, 2 * reconnect_timeout)
    finally:
        if pubsub:
            pubsub.close()
