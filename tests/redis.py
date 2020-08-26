import fakeredis
from functools import wraps
from unittest.mock import patch


def make_redis_client():
    return fakeredis.FakeStrictRedis()


def mock_redis(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        server = fakeredis.FakeServer()

        def from_url(url):
            return fakeredis.FakeStrictRedis(server=server)

        with patch('redis.Redis.from_url', side_effect=from_url):
            return f(*args, **kwargs)

    return wrapper
