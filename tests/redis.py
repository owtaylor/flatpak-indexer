import fakeredis
from functools import wraps
from unittest.mock import patch


def make_redis_client():
    return fakeredis.FakeStrictRedis()


def mock_redis(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        with patch('redis.Redis.from_url', return_value=make_redis_client()):
            return f(*args, **kwargs)

    return wrapper
