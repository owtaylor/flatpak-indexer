from datetime import datetime
import hashlib
import json
import logging
import os
import threading
from unittest.mock import MagicMock, patch

import pytest
import redis
import yaml

from flatpak_indexer.delta_generator import DeltaGenerator
from flatpak_indexer.models import RepositoryModel, TardiffSpecModel, TardiffResultModel
from flatpak_indexer.utils import path_for_digest

from .redis import mock_redis
from .utils import get_config


CONFIG = yaml.safe_load("""
redis_url: redis://localhost
work_dir: ${OUTPUT_DIR}/work
deltas_dir: ${OUTPUT_DIR}/deltas
deltas_uri: https://flatpaks.fedoraproject.org/deltas
koji_config: fedora
registries:
    fedora:
        public_url: https://registry.fedoraproject.org/
        datasource: fedora
indexes:
    stable:
        registry: fedora
        delta_keep_days: 100000
        output: out/test/flatpak.json
        bodhi_status: stable
        tag: latest
""")


IMAGE1 = {
    'Digest': 'sha256:a1b1c1d1e1f1',
    'MediaType': 'application/vnd.oci.image.manifest.v1+json',
    'OS': 'linux',
    'Architecture': 'amd64',
    'DiffIds': [
        'sha256:image1_layer0',
    ],
}

IMAGE2 = {
    'Digest': 'sha256:a2b2c2d2e2f2g2',
    'MediaType': 'application/vnd.oci.image.manifest.v1+json',
    'OS': 'linux',
    'Architecture': 'amd64',
    'DiffIds': [
        'sha256:image2_layer0',
    ],
}

IMAGE3 = {
    'Digest': 'sha256:a3b3c3d3e3f3',
    'MediaType': 'application/vnd.oci.image.manifest.v1+json',
    'OS': 'linux',
    'Architecture': 'amd64',
    'Tags': ['latest'],
    'DiffIds': [
        'sha256:image3_layer0',
    ],
}

REPOSITORY = {
    'Name': 'baobab',
    'Images': [
        IMAGE1,
        IMAGE2,
        IMAGE3
    ],
    'TagHistories': [
        {
            'Name': "latest",
            'Items': [
                {
                    'Architecture': 'amd64',
                    'Date': '2020-08-21T01:02:03.00000+00:00',
                    'Digest': IMAGE3['Digest'],
                },
                {
                    'Architecture': 'amd64',
                    'Date': '2020-08-14T01:02:03.00000+00:00',
                    'Digest': IMAGE2['Digest'],
                },
                {
                    'Architecture': 'amd64',
                    'Date': '2020-08-07T01:02:03.00000+00:00',
                    'Digest': IMAGE1['Digest'],
                }
            ]
        }
    ]
}


class FakeDiffer:
    def __init__(self, config, task_destinies={}):
        self.config = config
        self.thread = threading.Thread(target=self.run, name="fake-differ")
        self.logger = logging.getLogger(FakeDiffer.__qualname__)
        self.task_destinies = task_destinies

    def __enter__(self):
        self.start()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def start(self):
        self.thread.start()

    def stop(self):
        redis_client = redis.Redis.from_url(self.config.redis_url)
        redis_client.publish("fake-differ-exit", b'')
        self.thread.join()

    def get_task(self, redis_client):
        with redis_client.pipeline() as pipe:
            pipe.watch('tardiff:pending')
            task_raw = redis_client.srandmember('tardiff:pending')
            if not task_raw:
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

    def do_task(self, redis_client, task):
        spec_raw = redis_client.get(f"tardiff:spec:{task}")
        spec = TardiffSpecModel.from_json_text(spec_raw)

        destiny = self.task_destinies.get(task)
        if destiny:
            del self.task_destinies[task]

        if destiny == "diff-error":
            # Should not be retried - if retried, we'll succeed unexpectedly
            return TardiffResultModel(status="diff-error",
                                      digest="", size=0,
                                      message="tardiff failed")
        elif destiny == "download-error":
            # download errors are transient and should be tried
            return TardiffResultModel(status="download-error",
                                      digest="", size=0,
                                      message="downloading layer failed")
        elif destiny == "stick":
            # Task sticks in queue, needs to be removed and retried immediately
            self.logger.info("Leaving task stuck in the progress state")
            return
        elif destiny == "swallow":
            # Task vanishes without a trace, should be retried on next run
            self.logger.info("Making task disappear into the limbo")
            redis_client.zrem('tardiff:progress', task)
            return
        else:
            assert destiny is None

            out_contents = f"Tardiff({spec.from_diff_id}, {spec.to_diff_id})".encode("UTF-8")
            h = hashlib.sha256()
            h.update(out_contents)
            digest = 'sha256:' + h.hexdigest()

            with open(path_for_digest(self.config.deltas_dir,
                                      digest, ".tardiff", create_subdir=True), "wb") as f:
                f.write(out_contents)

            return TardiffResultModel(status="success",
                                      digest=digest, size=len(out_contents),
                                      message="")

    def finish_task(self, redis_client, task, result):
        with redis_client.pipeline() as pipe:
            pipe.multi()
            pipe.set(f'tardiff:result:{task}', result.to_json_text())
            pipe.zrem('tardiff:progress', task)
            if result.status == 'success':
                pipe.zadd('tardiff:active', {task: datetime.now().timestamp()})
            pipe.publish("tardiff:complete", b'')
            pipe.execute()

    def _run(self):
        redis_client = redis.Redis.from_url(self.config.redis_url)
        pubsub = redis_client.pubsub()
        pubsub.subscribe("fake-differ-exit")
        pubsub.subscribe('tardiff:queued')

        for message in pubsub.listen():
            self.logger.info("Got message: %s", message)
            if message['type'] == 'message':
                if message['channel'] == b'fake-differ-exit':
                    break

            while redis_client.scard('tardiff:pending') > 0:
                task = self.get_task(redis_client)
                logging.info("Got task %s", task)
                if task:
                    result = self.do_task(redis_client, task)
                    if result:
                        self.finish_task(redis_client, task, result)
                        logging.info("Completed task %s: %s", task, result.to_json_text())

    def run(self):
        try:
            self._run()
        except Exception:
            self.logger.exception("Failed to process tasks")
            raise


@mock_redis
@pytest.mark.parametrize('iterations', (1, 2))
@pytest.mark.parametrize('task_destiny,layer_counts', [
    (None,             (2, 2)),
    ('diff-error',     (1, 1)),  # Should not be retried
    ('download-error', (1, 2)),  # Should be retried on next pass
    ('swallow',        (1, 2)),  # Should be retried on the next pass
    ('stick',          (2, 2)),  # Should be removed and retried immediately
])
def test_delta_generator(tmp_path, iterations, task_destiny, layer_counts):
    os.environ["OUTPUT_DIR"] = str(tmp_path)
    os.mkdir(tmp_path / "deltas")

    config = get_config(tmp_path, CONFIG)

    generator = DeltaGenerator(config, progress_timeout_seconds=0.1)

    index_config = next(index for index in config.indexes if index.name == 'stable')
    repository = RepositoryModel.from_json(REPOSITORY)

    generator.add_tag_history('https://registry.fedoraproject.org',
                              repository,
                              repository.tag_histories['latest'],
                              index_config)

    task_destinies = {
        'sha256:image2_layer0:sha256:image3_layer0': task_destiny
    }

    with FakeDiffer(config, task_destinies=task_destinies):
        for i in range(0, iterations):
            generator.generate()

    url = generator.get_delta_manifest_url(IMAGE3['Digest'])
    assert url.startswith("https://flatpaks.fedoraproject.org/deltas/")

    path = tmp_path / "deltas" / "a3" / "b3c3d3e3f3.json"
    with open(path, "rb") as f:
        manifest = json.load(f)

    assert len(manifest["layers"]) == layer_counts[iterations - 1]

    layer = next(lyr for lyr in manifest["layers"]
                 if lyr["annotations"]["io.github.containers.delta.from"] == "sha256:image1_layer0")

    assert len(layer["urls"]) == 1
    assert layer["annotations"]["io.github.containers.delta.to"] == "sha256:image3_layer0"

    digest_url = layer["urls"][0]
    digest_path = digest_url.replace("https://flatpaks.fedoraproject.org/deltas",
                                     str(tmp_path / "deltas"))

    assert os.path.exists(digest_path)


@mock_redis
def test_delta_generator_expire(tmp_path):
    os.environ["OUTPUT_DIR"] = str(tmp_path)
    os.mkdir(tmp_path / "deltas")

    config = get_config(tmp_path, CONFIG)

    generator = DeltaGenerator(config)

    index_config = next(index for index in config.indexes if index.name == 'stable')
    repository = RepositoryModel.from_json(REPOSITORY)

    generator.add_tag_history('https://registry.fedoraproject.org',
                              repository,
                              repository.tag_histories['latest'],
                              index_config)

    with FakeDiffer(config):
        generator.generate()

        # Expire the deltas we just generated
        redis_client = redis.Redis.from_url(config.redis_url)
        all_tardiffs_raw = redis_client.zrangebyscore('tardiff:active', 0, float("inf"))
        all_tardiffs = (k.decode("utf-8") for k in all_tardiffs_raw)
        for result_raw in redis_client.mget(*(f"tardiff:result:{k}" for k in all_tardiffs)):
            result = TardiffResultModel.from_json_text(result_raw)
            os.unlink(path_for_digest(config.deltas_dir, result.digest, ".tardiff"))
        redis_client.delete('tardiff:active')

        generator.generate()

    path = tmp_path / "deltas" / "a3" / "b3c3d3e3f3.json"
    with open(path, "rb") as f:
        manifest = json.load(f)

    assert len(manifest["layers"]) == 2

    layer = next(lyr for lyr in manifest["layers"]
                 if lyr["annotations"]["io.github.containers.delta.from"] == "sha256:image1_layer0")

    digest_url = layer["urls"][0]
    digest_path = digest_url.replace("https://flatpaks.fedoraproject.org/deltas",
                                     str(tmp_path / "deltas"))

    assert os.path.exists(digest_path)


class IffyPubSub(redis.client.PubSub):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.get_message = MagicMock(wraps=self.get_message)
        self.get_message.side_effect = self.fail_first_get_message

    def fail_first_get_message(self, *args, **kwargs):
        self.get_message.side_effect = None
        raise redis.ConnectionError("Failed!")


@mock_redis
def test_delta_generator_connection_error(tmp_path):
    os.environ["OUTPUT_DIR"] = str(tmp_path)
    os.mkdir(tmp_path / "deltas")

    config = get_config(tmp_path, CONFIG)

    generator = DeltaGenerator(config)

    index_config = next(index for index in config.indexes if index.name == 'stable')
    repository = RepositoryModel.from_json(REPOSITORY)

    generator.add_tag_history('https://registry.fedoraproject.org',
                              repository,
                              repository.tag_histories['latest'],
                              index_config)

    with FakeDiffer(config):
        with patch('redis.client.PubSub', IffyPubSub):
            generator.generate()

    path = tmp_path / "deltas" / "a3" / "b3c3d3e3f3.json"
    with open(path, "rb") as f:
        manifest = json.load(f)

    assert len(manifest["layers"]) == 2
