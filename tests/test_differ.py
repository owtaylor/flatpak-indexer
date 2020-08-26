import logging
import os
import subprocess
import threading
import time
from unittest.mock import MagicMock, patch

import pytest
import redis
import yaml

from flatpak_indexer.differ import Differ
from flatpak_indexer.models import TardiffImageModel, TardiffResultModel, TardiffSpecModel

from .redis import mock_redis
from .registry import mock_registry
from .utils import get_config


CONFIG = yaml.safe_load("""
redis_url: redis://localhost
work_dir: /flatpak-work
deltas_dir: ${OUTPUT_DIR}/deltas
deltas_uri: https://flatpaks.fedoraproject.org/deltas
koji_config: fedora
registries:
    fedora:
        public_url: https://registry.fedoraproject.org/
        datasource: fedora
""")


@pytest.fixture
def config(tmp_path):
    os.environ["OUTPUT_DIR"] = str(tmp_path)
    os.mkdir(tmp_path / "deltas")

    return get_config(tmp_path, CONFIG)


def image_model(ref):
    return TardiffImageModel(registry='https://registry.fedoraproject.org',
                             repository='ghex',
                             ref=ref)


def queue_task(from_ref, from_diff_id, to_ref, to_diff_id,
               redis_client=None):
    if not redis_client:
        redis_client = redis.Redis.from_url("redis://localhost")

    spec = TardiffSpecModel(from_image=image_model(from_ref),
                            from_diff_id=from_diff_id,
                            to_image=image_model(to_ref),
                            to_diff_id=to_diff_id)

    key = f"{from_diff_id}:{to_diff_id}"

    redis_client.set(f"tardiff:spec:{key}", spec.to_json_text())
    redis_client.sadd("tardiff:pending", key)

    return key


def check_success(key):
    redis_client = redis.Redis.from_url("redis://localhost")

    assert redis_client.scard('tardiff:pending') == 0
    assert redis_client.zscore('tardiff:progress', key) is None
    assert redis_client.zscore('tardiff:active', key) is not None

    result_raw = redis_client.get(f"tardiff:result:{key}")
    result = TardiffResultModel.from_json_text(result_raw)

    assert result.status == "success"
    assert result.digest.startswith("sha256")
    assert result.message == ""


def check_failure(key):
    redis_client = redis.Redis.from_url("redis://localhost")

    assert redis_client.scard('tardiff:pending') == 0
    assert redis_client.zscore('tardiff:progress', key) is None
    assert redis_client.zscore('tardiff:active', key) is None

    result_raw = redis_client.get(f"tardiff:result:{key}")
    result = TardiffResultModel.from_json_text(result_raw)

    assert result.status == "diff-error"
    assert result.digest == ""
    assert result.message == "tardiff failed"


@mock_redis
@mock_registry(registry='registry.fedoraproject.org')
def test_differ(registry, config):
    old_manifest_digest, old_layer = registry.add_fake_image('ghex', 'latest')
    new_manifest_digest, new_layer = registry.add_fake_image('ghex', 'latest')

    key = f"{old_layer.diff_id}:{new_layer.diff_id}"

    key = queue_task(old_manifest_digest, old_layer.diff_id,
                     new_manifest_digest, new_layer.diff_id)

    differ = Differ(config)
    differ.run(max_tasks=1)

    check_success(key)


@mock_redis
@mock_registry(registry='registry.fedoraproject.org')
def test_differ_tardiff_failure(registry, config):
    old_manifest_digest, old_layer = registry.add_fake_image('ghex', 'latest',
                                                             layer_contents=b"GARBAGE")
    new_manifest_digest, new_layer = registry.add_fake_image('ghex', 'latest')

    key = queue_task(old_manifest_digest, old_layer.diff_id,
                     new_manifest_digest, new_layer.diff_id)

    differ = Differ(config)
    differ.run(max_tasks=1)

    check_failure(key)


@mock_redis
@mock_registry(registry='registry.fedoraproject.org')
def test_differ_tardiff_slow(registry, config):
    old_manifest_digest, old_layer = registry.add_fake_image('ghex', 'latest',
                                                             layer_contents=b"GARBAGE")
    new_manifest_digest, new_layer = registry.add_fake_image('ghex', 'latest')

    key = queue_task(old_manifest_digest, old_layer.diff_id,
                     new_manifest_digest, new_layer.diff_id)

    count = 0

    def timeout_wait(timeout=0):
        nonlocal count
        count += 1
        if count == 1:
            raise subprocess.TimeoutExpired("something", timeout)
        else:
            return 0

    with patch('subprocess.Popen.wait') as m:
        m.side_effect = timeout_wait

        differ = Differ(config)
        differ.run(max_tasks=1)

    check_success(key)


@mock_redis
@mock_registry(registry='registry.fedoraproject.org')
def test_differ_wait(registry, config):
    old_manifest_digest, old_layer = registry.add_fake_image('ghex', 'latest')
    new_manifest_digest, new_layer = registry.add_fake_image('ghex', 'latest')

    key = f"{old_layer.diff_id}:{new_layer.diff_id}"

    def run_thread():
        redis_client = redis.Redis.from_url("redis://localhost")

        time.sleep(0.1)

        # "stale old messages"
        redis_client.publish('tardiff:queued', b'')
        redis_client.publish('tardiff:queued', b'')

        time.sleep(0.1)

        queue_task(old_manifest_digest, old_layer.diff_id,
                   new_manifest_digest, new_layer.diff_id,
                   redis_client=redis_client)

        redis_client.publish('tardiff:queued', b'')

    fill_queue_thread = threading.Thread(target=run_thread, name="fill-queue")

    differ = Differ(config)

    fill_queue_thread.start()
    try:
        differ.run(max_tasks=1)
    finally:
        fill_queue_thread.join()

    check_success(key)


class IffyPubSub(redis.client.PubSub):
    def __init__(self, fail_method, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if fail_method:
            method_mock = MagicMock(wraps=getattr(self, fail_method))
            setattr(self, fail_method, method_mock)
            method_mock.side_effect = redis.ConnectionError("Failed!")


@mock_redis
@mock_registry(registry='registry.fedoraproject.org')
@pytest.mark.parametrize('fail_first_method', ('subscribe', 'get_message'))
def test_differ_connection_error(registry, config, fail_first_method, caplog):
    old_manifest_digest, old_layer = registry.add_fake_image('ghex', 'latest')
    new_manifest_digest, new_layer = registry.add_fake_image('ghex', 'latest')

    key = f"{old_layer.diff_id}:{new_layer.diff_id}"

    def run_thread():
        redis_client = redis.Redis.from_url("redis://localhost")

        time.sleep(0.1)

        queue_task(old_manifest_digest, old_layer.diff_id,
                   new_manifest_digest, new_layer.diff_id,
                   redis_client=redis_client)

        redis_client.publish('tardiff:queued', b'')

    fill_queue_thread = threading.Thread(target=run_thread, name="fill-queue")

    fail_method = fail_first_method

    def get_pubsub(*args, **kwargs):
        nonlocal fail_method
        result = IffyPubSub(fail_method, *args, **kwargs)
        fail_method = None

        return result

    caplog.set_level(logging.INFO)

    with patch('redis.client.PubSub', get_pubsub):
        differ = Differ(config)

        fill_queue_thread.start()
        try:
            differ.run(max_tasks=1, initial_reconnect_timeout=0.05)
        finally:
            fill_queue_thread.join()

        if fail_first_method == 'subscribe':
            assert "Failed to connect to Redis, sleeping for 0.05 seconds" in caplog.text
        else:
            assert "Disconnected from Redis, sleeping for 0.05 seconds" in caplog.text

    check_success(key)
