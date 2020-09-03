import redis

def get_redis_client(config):
    return redis.Redis.from_url(config.redis_url,
                                password=config.redis_password)
