from functools import cached_property

from .koji_utils import get_koji_session
from .redis_utils import get_redis_client


class Session:
    def __init__(self, config):
        self.config = config

    @cached_property
    def koji_session(self):
        return get_koji_session(self.config)

    @cached_property
    def redis_client(self):
        return get_redis_client(self.config)

    @cached_property
    def build_cache(self):
        # Avoid a circular import
        from flatpak_indexer.build_cache import BuildCache
        return BuildCache(self)

    @cached_property
    def fedora_releases(self):
        # Avoid a circular import
        from flatpak_indexer.bodhi_query import query_releases
        return query_releases(self)
