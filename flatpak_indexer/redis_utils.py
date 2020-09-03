from urllib.parse import quote, urlparse, urlunparse

import redis

def get_redis_client(config):
    url = config.redis_url

    # redis.Redis.from_url() doesn't support passing the password separately
    # https://github.com/andymccurdy/redis-py/issues/1347

    password = config.redis_password
    if password:
        parts = urlparse(url)
        netloc = f':{quote(password)}@{parts.hostname}'
        if parts.port is not None:
            netloc += f':{parts.port}'

        url = urlunparse((parts.scheme, netloc, parts.path, parts.params, parts.query, parts.fragment))

    return redis.Redis.from_url(url, decode_components=True)
