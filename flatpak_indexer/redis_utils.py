from typing import (
    Callable,
    ContextManager,
    Iterable,
    List,
    Literal,
    Mapping,
    Optional,
    Protocol,
    TypeVar,
    overload,
)
from urllib.parse import quote, urlparse, urlunparse
import logging
import time

from redis.typing import EncodableT, ExpiryT, FieldT, KeysT, KeyT, ZScoreBoundT
import redis.client

import redis

from .base_config import BaseConfig

logger = logging.getLogger(__name__)


class RedisConfig(BaseConfig):
    redis_url: str
    redis_password: Optional[str] = None


ScoreT = TypeVar("ScoreT")


class TypedRedis(Protocol):
    """
    A typed subset of redis.Redis methods used by flatpak-indexer.

    The redis-py type annotations are a mess: async and sync return types
    are mixed together, and there is no representation of the fact that
    the client *might* decode the responses from bytes to str, or not,
    depending on how it was constructed.

    This Protocol represents the methods we actually use, with more precise
    types - we simply cast the redis.Redis instances we get to this type.
    """

    def delete(self, *names: KeyT) -> int: ...

    def execute(self) -> List[object]: ...

    def exists(self, name: KeyT) -> bool: ...

    def hmget(self, name: KeyT, keys: KeysT, *args: KeyT) -> list[bytes | None]: ...

    def hget(self, name: KeyT, key: KeyT) -> bytes | None: ...

    def get(self, name: KeyT) -> Optional[bytes]: ...

    @overload
    def hset(self, name: KeyT, key: KeyT, value: EncodableT) -> int: ...

    @overload
    def hset(self, name: KeyT, mapping: Mapping[str, EncodableT]) -> int: ...

    def mget(self, keys: KeysT, *args: KeyT) -> list[bytes | None]: ...

    def multi(self): ...

    def pipeline(self, *args, **kwargs) -> "TypedPipeline": ...

    def publish(self, channel: KeyT, message: EncodableT) -> int: ...

    def sadd(self, name: KeyT, *values: FieldT) -> int: ...

    def scan_iter(self, match: Optional[EncodableT] = None) -> Iterable[str]: ...

    def scard(self, name: KeyT) -> int: ...

    def set(self, name: KeyT, value: EncodableT) -> bool: ...

    def setex(self, name: KeyT, time: ExpiryT, value: EncodableT) -> bool: ...

    @overload
    def srandmember(self, name: KeyT, number: None = None) -> Optional[bytes]: ...

    @overload
    def srandmember(self, name: KeyT, number: int) -> List[bytes]: ...

    def srem(self, name: KeyT, *values: FieldT) -> int: ...

    def transaction(self, func: Callable[["TypedPipeline"], None]): ...

    def watch(self, keys: KeysT): ...

    def zadd(self, name: KeyT, mapping: dict[EncodableT, float], xx: bool = False) -> int: ...

    def zcard(self, name: KeyT) -> int: ...

    @overload
    def zrange(
        self,
        name: KeyT,
        start: int,
        end: int,
        *,
        desc: bool = False,
        withscores: Literal[False] = False,
        score_cast_func: type | Callable = float,
        byscore: bool = False,
        bylex: bool = False,
        offset: Optional[int] = None,
        num: Optional[int] = None,
    ) -> list[bytes]: ...

    @overload
    def zrange(
        self,
        name: KeyT,
        start: int,
        end: int,
        *,
        desc: bool = False,
        withscores: Literal[True],
        score_cast_func: type | Callable[[float], ScoreT] = float,
        byscore: bool = False,
        bylex: bool = False,
        offset: Optional[int] = None,
        num: Optional[int] = None,
    ) -> list[tuple[bytes, ScoreT]]: ...

    def zrangebylex(
        self,
        name: KeyT,
        min: EncodableT,
        max: EncodableT,
        start: Optional[int] = None,
        num: Optional[int] = None,
    ) -> list[bytes]: ...

    @overload
    def zrangebyscore(
        self,
        name: KeyT,
        min: ZScoreBoundT,
        max: ZScoreBoundT,
        *,
        start: Optional[int] = None,
        num: Optional[int] = None,
        withscores: Literal[False] = False,
        score_cast_func: type | Callable = float,
    ) -> list[bytes]: ...

    @overload
    def zrangebyscore(
        self,
        name: KeyT,
        min: ZScoreBoundT,
        max: ZScoreBoundT,
        *,
        start: Optional[int] = None,
        num: Optional[int] = None,
        withscores: Literal[True],
        score_cast_func: type | Callable[[float], ScoreT] = float,
    ) -> list[tuple[bytes, ScoreT]]: ...

    def zrem(self, name: KeyT, *values: EncodableT) -> int: ...

    def zremrangebyscore(
        self,
        name: KeyT,
        min: ZScoreBoundT,
        max: ZScoreBoundT,
    ) -> int: ...

    def zscore(self, name: KeyT, value: EncodableT) -> float | None: ...


class TypedPipeline(TypedRedis, ContextManager["TypedPipeline"]):
    pass


def get_redis_client(config: RedisConfig) -> TypedRedis:
    url = config.redis_url

    # redis.Redis.from_url() doesn't support passing the password separately
    # https://github.com/andymccurdy/redis-py/issues/1347

    password = config.redis_password
    if password:
        parts = urlparse(url)
        netloc = f":{quote(password)}@{parts.hostname}"
        if parts.port is not None:
            netloc += f":{parts.port}"

        url = urlunparse(
            (parts.scheme, netloc, parts.path, parts.params, parts.query, parts.fragment)
        )

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
                    logger.info(
                        "Disconnected from Redis, sleeping for %g seconds", reconnect_timeout
                    )
                else:
                    logger.info(
                        "Failed to connect to Redis, sleeping for %g seconds", reconnect_timeout
                    )

                if pubsub:
                    pubsub.close()
                    pubsub = None

                time.sleep(reconnect_timeout)
                reconnect_timeout = min(MAX_RECONNECT_TIMEOUT, 2 * reconnect_timeout)
    finally:
        if pubsub:
            pubsub.close()
