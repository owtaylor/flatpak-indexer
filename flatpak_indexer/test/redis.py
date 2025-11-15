from functools import wraps
from typing import Callable, ParamSpec, TypeVar, overload
from unittest.mock import patch

import fakeredis


def make_redis_client():
    return fakeredis.FakeStrictRedis()


P = ParamSpec("P")
R = TypeVar("R")


@overload
def mock_redis(f: Callable[P, R], expect_url: str | None = None) -> Callable[P, R]: ...


@overload
def mock_redis(
    f: None = None, expect_url: str | None = None
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def mock_redis(
    f: Callable[P, R] | None = None, expect_url=None
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    if f is None:
        # Called with arguments, return a version with the arguments bound
        # to be used as the actual decorator.
        def decorator(
            f: Callable[P, R],
        ) -> Callable[P, R]:
            return mock_redis(f, expect_url=expect_url)

        return decorator

    @wraps(f)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        assert f is not None  # pyrefly needs this

        server = fakeredis.FakeServer()

        def from_url(url, **kwargs):
            if expect_url is not None:
                assert url == expect_url

            return fakeredis.FakeStrictRedis(server=server)  # type: ignore

        with patch("redis.Redis.from_url", side_effect=from_url):
            return f(*args, **kwargs)

    return wrapper
