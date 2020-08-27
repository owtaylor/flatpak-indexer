from datetime import datetime
import logging
import os
import subprocess
import tempfile
import time

import redis

from .models import TardiffSpecModel, TardiffResultModel
from .registry_client import RegistryClient
from .utils import path_for_digest, TemporaryPathname


logger = logging.getLogger(__name__)


class Differ:
    def __init__(self, config):
        self.config = config
        self.redis_client = redis.Redis.from_url(self.config.redis_url)

    def _wait_for_task(self, pubsub):
        got_message = False
        while not got_message:
            # Eat all available messages
            while True:
                message = pubsub.get_message(timeout=0)
                logging.info('%s', message)
                if not message:
                    break
                if message['type'] == 'message':
                    got_message = True
            if not got_message:
                # Do a blocking wait
                message = pubsub.get_message(timeout=60*60)
                if message and message['type'] == 'message':
                    got_message = True

    def _get_task(self):
        with self.redis_client.pipeline() as pipe:
            pipe.watch('tardiff:pending')
            task_raw = pipe.srandmember('tardiff:pending')
            if task_raw is None:
                return None

            task = task_raw.decode("utf-8")

            pipe.multi()
            pipe.srem('tardiff:pending', task)
            pipe.zadd('tardiff:progress', {task: datetime.now().timestamp()})
            try:
                pipe.execute()
                return task
            except redis.WatchError:  # pragma: no cover
                return None

    def _download_layer(self, image, diff_id, output_path, progress_callback=None):
        client = RegistryClient(image.registry)
        client.download_layer(image.repository, image.ref, diff_id, output_path,
                              progress_callback=progress_callback)

    def _process_task(self, task):
        spec_raw = self.redis_client.get(f"tardiff:spec:{task}")
        spec = TardiffSpecModel.from_json_text(spec_raw)

        logger.info("Processing task from=%s/%s@%s (DiffId=%s), to=%s/%s@%s (DiffId=%s)",
                    spec.from_image.registry, spec.from_image.repository, spec.from_image.ref,
                    spec.from_diff_id,
                    spec.to_image.registry, spec.to_image.repository, spec.to_image.ref,
                    spec.to_diff_id)

        with tempfile.TemporaryDirectory(prefix="flatpak-indexer-differ-") as tempdir, \
             TemporaryPathname(dir=self.config.deltas_dir, suffix=".tardiff") as result_path:
            from_path = os.path.join(tempdir, "from-layer")
            to_path = os.path.join(tempdir, "to-layer")

            def progress(*args):
                self.redis_client.zadd('tardiff:progress', {task: datetime.now().timestamp()})

            self._download_layer(spec.from_image, spec.from_diff_id,
                                 from_path, progress_callback=progress)
            self._download_layer(spec.to_image, spec.to_diff_id,
                                 to_path, progress_callback=progress)
            args = ["tar-diff", from_path, to_path, result_path.name]
            logger.info("Calling %s", args)
            p = subprocess.Popen(args)
            while True:
                try:
                    result = p.wait(timeout=1)
                    break
                except subprocess.TimeoutExpired:
                    pass

                progress()

            if result == 0:
                output = subprocess.check_output(["sha256sum", result_path.name], encoding="utf-8")
                digest = 'sha256:' + output.strip().split()[0]
                size = os.stat(result_path.name).st_size

                final_path = path_for_digest(self.config.deltas_dir,
                                             digest, '.tardiff', create_subdir=True)
                os.rename(result_path.name, final_path)
                result_path.delete = False

                logger.info("Successfully processed task")
                result = TardiffResultModel(status="success",
                                            digest=digest,
                                            size=size,
                                            message="")
            else:
                logger.info("tar-diff exited with status=%d", result)
                result = TardiffResultModel(status="diff-error",
                                            digest="",
                                            size=0,
                                            message="tardiff failed")

            return result

    def _finish_task(self, task, result):
        with self.redis_client.pipeline() as pipe:
            pipe.multi()
            pipe.set(f'tardiff:result:{task}', result.to_json_text())
            pipe.zrem('tardiff:progress', task)
            if result.status == 'success':
                pipe.zadd('tardiff:active', {task: datetime.now().timestamp()})
            pipe.publish("tardiff:complete", b'')
            pipe.execute()

    def run(self, max_tasks=-1, initial_reconnect_timeout=None):
        task_count = 0
        pubsub = None

        INITIAL_RECONNECT_TIMEOUT = 5
        MAX_RECONNECT_TIMEOUT = 120

        reconnect_timeout = initial_reconnect_timeout
        if reconnect_timeout is None:
            reconnect_timeout = INITIAL_RECONNECT_TIMEOUT

        while True:
            try:
                if pubsub is None:
                    pubsub = self.redis_client.pubsub()
                    pubsub.subscribe('tardiff:queued')

                    logger.info("Connected to Redis, waiting for tasks")

                if max_tasks >= 0 and task_count >= max_tasks:
                    break

                task = self._get_task()
                if task:
                    result = self._process_task(task)
                    self._finish_task(task, result)
                    task_count += 1
                else:
                    self._wait_for_task(pubsub)

                reconnect_timeout = INITIAL_RECONNECT_TIMEOUT
            except redis.ConnectionError:
                if pubsub and pubsub.connection:
                    logger.info("Disconnected from Redis, sleeping for %g seconds",
                                reconnect_timeout)
                else:
                    logger.info("Failed to connect to Redis, sleeping for %g seconds",
                                reconnect_timeout)

                if pubsub:
                    if pubsub.connection:
                        pubsub.connection.disconnect()
                    pubsub = None

                time.sleep(reconnect_timeout)
                reconnect_timeout = min(MAX_RECONNECT_TIMEOUT, 2 * reconnect_timeout)
