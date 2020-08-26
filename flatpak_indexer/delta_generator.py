from datetime import datetime, timedelta, timezone
import json
import logging

import redis
import time

from .models import TardiffImageModel, TardiffResultModel, TardiffSpecModel
from .utils import atomic_writer, path_for_digest, uri_for_digest


logger = logging.getLogger(__name__)


class DeltaGenerator:
    def __init__(self, config, progress_timeout_seconds=60):
        self.config = config
        self.redis_client = redis.Redis.from_url(config.redis_url)
        self.progress_timeout_seconds = progress_timeout_seconds

        self.now = datetime.utcnow().replace(tzinfo=timezone.utc)
        self.deltas = {}
        self.image_info = {}
        self.delta_manifest_urls = {}

    def add_tag_history(self, registry_url, repository, tag_history, index_config):
        keep = timedelta(days=index_config.delta_keep_days)
        arch_map = {}

        for item in tag_history.items:
            latest_date = tag_history.items[0].date
            if item.date == latest_date:
                arch_map[item.architecture] = [item]
            elif item.architecture in arch_map:
                next_item = arch_map[item.architecture][-1]
                if self.now - next_item.date <= keep:
                    arch_map[item.architecture].append(item)
                    self._add_delta(registry_url, repository,
                                    item, arch_map[item.architecture][0])

    def generate(self):
        specs = self._get_specs()
        results = self._wait_for_tardiffs(specs)

        self._write_manifests(results)

    def get_delta_manifest_url(self, digest):
        return self.delta_manifest_urls.get(digest)

    def _add_delta(self, registry_url, repository, from_item, to_item):
        if to_item.digest not in self.deltas:
            self.deltas[to_item.digest] = set()

        self.deltas[to_item.digest].add(from_item.digest)

        self._add_image(registry_url, repository, from_item)
        self._add_image(registry_url, repository, to_item)

    def _add_image(self, registry_url, repository, history_item):
        image = repository.images[history_item.digest]
        image_model = TardiffImageModel(registry=registry_url,
                                        repository=repository.name,
                                        ref=history_item.digest)
        self.image_info[history_item.digest] = (image, image_model)

    def _get_specs(self):
        specs = {}
        for to_digest, from_digests in self.deltas.items():
            to_image, to_image_model = self.image_info[to_digest]
            assert len(to_image.diff_ids) == 1
            to_diff_id = to_image.diff_ids[0]

            for from_digest in from_digests:
                from_image, from_image_model = self.image_info[from_digest]
                assert len(from_image.diff_ids) == 1
                from_diff_id = from_image.diff_ids[0]

                key = f"{from_diff_id}:{to_diff_id}"
                specs[key] = TardiffSpecModel(from_image=from_image_model,
                                              from_diff_id=from_diff_id,
                                              to_image=to_image_model,
                                              to_diff_id=to_diff_id)

        return specs

    def _wait_for_tardiffs(self, specs):
        success = {}
        to_fetch = {}
        failure = {}

        keys = specs.keys()

        if keys:
            old_results = self.redis_client.mget(*(f"tardiff:result:{k}" for k in keys))
        else:
            old_results = []

        for key, result_raw in zip(keys, old_results):
            spec = specs[key]

            if result_raw is None:
                to_fetch[key] = spec
            else:
                result = TardiffResultModel.from_json_text(result_raw)
                if result.status == "success":
                    success[key] = result
                elif result.status in ("download-error", "queue-error"):
                    to_fetch[key] = spec
                else:
                    # diff-error is not retriable
                    failure[key] = result

        if success:
            now = datetime.now().timestamp()
            self.redis_client.zadd('tardiff:active', {key: now for key in success}, xx=True)

            # Old diffs that have been cleaned up no longer appear in tardiff:active, so check
            # to see what keys we actually managed to update (xx=True means "only existing")
            # We allow some slop in case another indexer is running and updated the keys
            # with a slightly older timestamp.

            updated_raw = self.redis_client.zrangebyscore('tardiff:active', now - 60, float("inf"))
            updated = set(r.decode('utf-8') for r in updated_raw)
            expired = set(success) - updated
            logger.info("success=%s, updated=%s, expired=%s", set(success), updated, expired)
            for key in expired:
                del success[key]
                to_fetch[key] = specs[key]

        if to_fetch:
            for key, spec in to_fetch.items():
                logger.info("Requesting generation of a delta for %s/%s/%s => %s/%s/%s",
                            spec.from_image.repository, spec.from_image.ref, spec.from_diff_id,
                            spec.to_image.repository, spec.to_image.ref, spec.from_diff_id)
                self.redis_client.setex(f"tardiff:spec:{key}",
                                        timedelta(days=1),
                                        spec.to_json_text())
                self.redis_client.sadd('tardiff:pending', key)

            self.redis_client.publish('tardiff:queued', b'')

            pubsub = self.redis_client.pubsub()
            pubsub.subscribe("tardiff:complete")

            last_counts = None
            while True:
                pending_count = self.redis_client.scard("tardiff:pending")
                progress_count = self.redis_client.zcard("tardiff:progress")
                counts = (pending_count, progress_count)
                if counts != last_counts:
                    logger.info("Pending Tasks: %d  In Progress Tasks: %d",
                                pending_count, progress_count)
                    last_counts = counts

                if pending_count == 0 and progress_count == 0:
                    break

                now = time.time()
                next_expire = now + self.progress_timeout_seconds
                with self.redis_client.pipeline() as pipe:
                    pipe.watch('tardiff:progress')
                    stale = pipe.zrangebyscore('tardiff:progress',
                                               0, now - self.progress_timeout_seconds)
                    if len(stale) > 0:
                        for key in stale:
                            logger.info("Task %s timed out, requeueing", key)

                        pipe.multi()
                        pipe.zrem('tardiff:progress', *stale)
                        pipe.sadd('tardiff:pending', *stale)
                        pipe.publish('tardiff:queued', b'')
                        try:
                            pipe.execute()
                        except redis.WatchError:  # pragma: no cover
                            # progress was modified, immediately try again
                            continue
                    else:
                        oldest = pipe.zrange('tardiff:progress', 0, 0, withscores=True)
                        if len(oldest) > 0:
                            next_expire = oldest[0][1] + self.progress_timeout_seconds
                            logger.debug("Oldest task is %s, expires in %f seconds",
                                         oldest[0][0].decode("utf-8"), next_expire - now)

                while True:
                    timeout = max(0, next_expire - now)
                    logger.debug("Waiting for a message for %f seconds", timeout)
                    try:
                        message = pubsub.get_message(timeout=timeout)
                        if message is None:
                            # timed out
                            break
                        elif (message['type'] == 'message' and
                              message['channel'] == b'tardiff:complete'):
                            logger.debug("Got tardiff:complete message")
                            break
                        else:
                            logger.debug("Ignoring message %s", message)
                    except redis.ConnectionError:
                        pubsub.connection.disconnect()
                        pubsub = self.redis_client.pubsub()
                        pubsub.subscribe("tardiff:complete")
                        break

                    now = time.time()

            new_keys = list(to_fetch.keys())
            new_results = self.redis_client.mget(*(f"tardiff:result:{k}" for k in new_keys))

            for key, result_raw in zip(new_keys, new_results):
                if result_raw is None:
                    logger.info("No result for key %s, but no longer in queue", key)
                    failure[key] = TardiffResultModel(status="queue-error",
                                                      digest="",
                                                      size=0,
                                                      message="Missing result")
                else:
                    result = TardiffResultModel.from_json_text(result_raw)
                    if result.status == "success":
                        success[key] = result
                    else:
                        failure[key] = result

        results = dict(success)
        results.update(failure)

        return results

    def _write_manifests(self, results):
        for to_digest, from_digests in self.deltas.items():
            to_image, _ = self.image_info[to_digest]
            assert len(to_image.diff_ids) == 1
            to_diff_id = to_image.diff_ids[0]

            delta_layers = []
            for from_digest in from_digests:
                from_image, from_image_model = self.image_info[from_digest]
                from_diff_id = from_image.diff_ids[0]

                key = f"{from_diff_id}:{to_diff_id}"

                result = results[key]
                if result.status == "success":
                    delta_layers.append({
                        "mediaType": "application/vnd.redhat.tar-diff",
                        "size": result.size,
                        "digest": result.digest,
                        "urls": [uri_for_digest(self.config.deltas_uri, result.digest, '.tardiff')],
                        "annotations": {
                            "io.github.containers.delta.from": from_diff_id,
                            "io.github.containers.delta.to": to_diff_id
                        }
                    })

            if len(delta_layers) > 0:
                filename = path_for_digest(self.config.deltas_dir,
                                           to_image.digest, '.json', create_subdir=True)

                manifest = {
                    "schemaVersion": 1,
                    "config": {
                        "mediaType": "application/vnd.redhat.delta.config.v1+json",
                        "size": 2,
                        "digest":
                            ("sha256:" +
                             "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a")
                    },
                    "annotations": {
                        "io.github.containers.delta.target": to_image.digest,
                    },
                    "layers": delta_layers
                }

                with atomic_writer(filename) as writer:
                    json.dump(manifest, writer,
                              sort_keys=True, indent=4, ensure_ascii=False)

                self.delta_manifest_urls[to_image.digest] = uri_for_digest(self.config.deltas_uri,
                                                                           to_image.digest, '.json')
