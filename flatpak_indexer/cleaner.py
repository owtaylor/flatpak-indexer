from datetime import datetime
import logging
import os

from .redis_utils import get_redis_client


logger = logging.getLogger(__name__)

FILES_USED_KEY = 'files:used'


class Cleaner:
    """
    Used to remove unused files in icons_dir and deltas_dir. Files are considered
    unused if:

    * They were last referenced by an index longer ago than config.clean_files_after
    * They were not referenced on the last index creation run

    The second part allows clean_files_after: 0d to mean "no grace period" instead
    of "delete all files immediately"
    """
    def __init__(self, config):
        self.config = config
        self.redis = get_redis_client(config)
        self.this_cycle = set()

    def reset(self):
        """Mark the beginning of a new index creation run"""
        self.this_cycle = set()

    def reference(self, path):
        """Mark a file as referenced within the current run, and record the reference time"""
        if path not in self.this_cycle:
            self.this_cycle.add(path)
            self.redis.zadd(FILES_USED_KEY, {path: datetime.now().timestamp()})

    def _find_files_recurse(self, dir, result):
        with os.scandir(dir) as iter:
            for dirent in iter:
                if dirent.is_dir():
                    self._find_files_recurse(dirent.path, result)
                else:
                    result.append(dirent.path)

    def _find_files(self):
        result = []

        if self.config.icons_dir and os.path.exists(self.config.icons_dir):
            self._find_files_recurse(self.config.icons_dir, result)
        if self.config.deltas_dir and os.path.exists(self.config.deltas_dir):
            self._find_files_recurse(self.config.deltas_dir, result)

        return result

    def clean(self):
        """Removes no longer used files"""
        files = self._find_files()
        keep_since = (datetime.now() - self.config.clean_files_after).timestamp()
        # Remove all stale elements from redis tracking
        self.redis.zremrangebyscore(FILES_USED_KEY, 0, keep_since)
        # Remaining ones are the ones that have been referenced in the last extra_keep_seconds
        current_raw = self.redis.zrange(FILES_USED_KEY, 0, -1)
        current = {k.decode("utf-8") for k in current_raw}
        for f in files:
            if f not in self.this_cycle and f not in current:
                os.remove(f)
                logger.info("Removing unused file: %s", f)
