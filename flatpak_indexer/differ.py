from datetime import datetime
from typing import cast
import logging
import os
import subprocess
import tempfile

import requests

import redis

from .models import TardiffResultModel, TardiffSpecModel
from .redis_utils import do_pubsub_work, get_redis_client
from .registry_client import RegistryClient
from .utils import TemporaryPathname, path_for_digest, run_with_stats

logger = logging.getLogger(__name__)


class Differ:
    def __init__(self, config):
        self.config = config
        self.redis_client = get_redis_client(config)

    def _wait_for_task(self, pubsub):
        got_message = False
        while not got_message:
            # Eat all available messages
            while True:
                message = pubsub.get_message(timeout=0)
                logging.info("%s", message)
                if not message:
                    break
                if message["type"] == "message":
                    got_message = True
            if not got_message:
                # Do a blocking wait
                message = pubsub.get_message(timeout=60 * 60)
                if message and message["type"] == "message":
                    got_message = True

    def _get_task(self):
        with self.redis_client.pipeline() as pipe:
            pipe.watch("tardiff:pending")
            pre = cast("redis.Redis[bytes]", pipe)
            task_raw = pre.srandmember("tardiff:pending")
            if task_raw is None:
                return None

            task = cast(bytes, task_raw).decode("utf-8")

            pipe.multi()
            pipe.srem("tardiff:pending", task)
            pipe.zadd("tardiff:progress", {task: datetime.now().timestamp()})
            try:
                pipe.execute()
                return task
            except redis.WatchError:  # pragma: no cover
                return None

    def _download_layer(self, image, diff_id, output_path, progress_callback=None):
        client = RegistryClient(image.registry, session=self.config.get_requests_session())
        client.download_layer(
            image.repository, image.ref, diff_id, output_path, progress_callback=progress_callback
        )

    def _process_task(self, task):
        spec_raw = self.redis_client.get(f"tardiff:spec:{task}")
        if spec_raw is None:
            logger.warning("Can't find spec for '%s', ignoring task", task)
            return TardiffResultModel(
                status="no-spec-error", digest="", size=0, message="failed to find spec"
            )
        spec = TardiffSpecModel.from_json_text(spec_raw)

        logger.info(
            "Processing task from=%s/%s@%s (DiffId=%s), to=%s/%s@%s (DiffId=%s)",
            spec.from_image.registry,
            spec.from_image.repository,
            spec.from_image.ref,
            spec.from_diff_id,
            spec.to_image.registry,
            spec.to_image.repository,
            spec.to_image.ref,
            spec.to_diff_id,
        )

        with (
            tempfile.TemporaryDirectory(prefix="flatpak-indexer-differ-") as tempdir,
            TemporaryPathname(dir=self.config.deltas_dir, suffix=".tardiff") as result_path,
        ):
            from_path = os.path.join(tempdir, "from-layer")
            to_path = os.path.join(tempdir, "to-layer")

            def progress(*args):
                self.redis_client.zadd("tardiff:progress", {task: datetime.now().timestamp()})

            try:
                self._download_layer(
                    spec.from_image, spec.from_diff_id, from_path, progress_callback=progress
                )
            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError,
                requests.exceptions.SSLError,
            ) as e:
                logger.info("Failed to download from layer: %s", str(e))
                return TardiffResultModel(
                    status="download-error",
                    digest="",
                    size=0,
                    message="downloading from layer failed",
                )

            from_size = os.stat(from_path).st_size

            try:
                self._download_layer(
                    spec.to_image, spec.to_diff_id, to_path, progress_callback=progress
                )
            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError,
                requests.exceptions.SSLError,
            ) as e:
                logger.info("Failed to download to layer: %s", str(e))
                return TardiffResultModel(
                    status="download-error",
                    digest="",
                    size=0,
                    message="downloading to layer failed",
                )

            to_size = os.stat(to_path).st_size

            args = ["tar-diff", from_path, to_path, result_path.name]
            logger.info("Calling %s", args)
            result, stats = run_with_stats(args, progress_callback=progress)

            if result == 0:
                logger.info(
                    "tar-diff: elapsed=%ss, user=%ss, system=%ss, maximum RSS=%s kIB",
                    stats.elapsed_time_s,
                    stats.user_time_s,
                    stats.system_time_s,
                    stats.max_mem_kib,
                )

                output = subprocess.check_output(["sha256sum", result_path.name], encoding="utf-8")
                digest = "sha256:" + output.strip().split()[0]
                size = os.stat(result_path.name).st_size

                final_path = path_for_digest(
                    self.config.deltas_dir, digest, ".tardiff", create_subdir=True
                )
                os.chmod(result_path.name, 0o644)
                os.rename(result_path.name, final_path)
                result_path.delete = False

                logger.info("Successfully processed task")
                result = TardiffResultModel(
                    status="success",
                    digest=digest,
                    size=size,
                    message="",
                    from_size=from_size,
                    to_size=to_size,
                    max_mem_kib=stats.max_mem_kib,
                    elapsed_time_s=stats.elapsed_time_s,
                    user_time_s=stats.user_time_s,
                    system_time_s=stats.system_time_s,
                )
            else:
                logger.info("tar-diff exited with status=%d", result)
                result = TardiffResultModel(
                    status="diff-error", digest="", size=0, message="tardiff failed"
                )

            return result

    def _finish_task(self, task, result):
        with self.redis_client.pipeline() as pipe:
            pipe.multi()
            pipe.set(f"tardiff:result:{task}", result.to_json_text())
            pipe.zrem("tardiff:progress", task)
            if result.status == "success":
                pipe.zadd("tardiff:active", {task: datetime.now().timestamp()})
            pipe.publish("tardiff:complete", b"")
            pipe.execute()

    def run(self, max_tasks=-1, initial_reconnect_timeout=None):
        task_count = 0

        def do_work(pubsub):
            nonlocal task_count

            if max_tasks >= 0 and task_count >= max_tasks:
                return False

            task = self._get_task()
            if task:
                result = self._process_task(task)
                self._finish_task(task, result)
                task_count += 1
            else:
                self._wait_for_task(pubsub)

            return True

        do_pubsub_work(
            self.redis_client,
            "tardiff:queued",
            do_work,
            initial_reconnect_timeout=initial_reconnect_timeout,
        )
