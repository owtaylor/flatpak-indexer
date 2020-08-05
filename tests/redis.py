import fakeredis


def make_redis_client():
    return fakeredis.FakeStrictRedis()
