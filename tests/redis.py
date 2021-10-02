import fakeredis
from functools import partial, wraps
from unittest.mock import patch


def make_redis_client():
    return fakeredis.FakeStrictRedis()


def mock_redis(f=None, expect_url=None):
    if f is None:
        # Called with arguments, return a version with the arguments bound
        # to be used as the actual decorator.
        return partial(mock_redis, expect_url=expect_url)

    @wraps(f)
    def wrapper(*args, **kwargs):
        server = fakeredis.FakeServer()

        def from_url(url, **kwargs):
            if expect_url is not None:
                assert url == expect_url

            return fakeredis.FakeStrictRedis(server=server)  # type: ignore

        with patch('redis.Redis.from_url', side_effect=from_url):
            return f(*args, **kwargs)

    return wrapper
