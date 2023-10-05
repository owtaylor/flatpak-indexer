import os
import threading
import time

import pytest
import redis
import yaml

from flatpak_indexer.differ import Differ
from flatpak_indexer.models import TardiffImageModel, TardiffResultModel, TardiffSpecModel
from flatpak_indexer.test.redis import mock_redis
from .registry import mock_registry
from .utils import get_config, timeout_first_popen_wait


CONFIG = yaml.safe_load("""
redis_url: redis://localhost
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
               redis_client=None, skip_spec=False):
    if not redis_client:
        redis_client = redis.Redis.from_url("redis://localhost")

    spec = TardiffSpecModel(from_image=image_model(from_ref),
                            from_diff_id=from_diff_id,
                            to_image=image_model(to_ref),
                            to_diff_id=to_diff_id)

    key = f"{from_diff_id}:{to_diff_id}"

    if not skip_spec:
        redis_client.set(f"tardiff:spec:{key}", spec.to_json_text())
    redis_client.sadd("tardiff:pending", key)

    return key


def check_success(key, old_layer, new_layer):
    redis_client = redis.Redis.from_url("redis://localhost")

    assert redis_client.scard('tardiff:pending') == 0
    assert redis_client.zscore('tardiff:progress', key) is None
    assert redis_client.zscore('tardiff:active', key) is not None

    result_raw = redis_client.get(f"tardiff:result:{key}")
    assert result_raw is not None
    result = TardiffResultModel.from_json_text(result_raw)

    assert result.status == "success"
    assert result.digest.startswith("sha256")
    assert result.from_size == old_layer.size
    assert result.to_size == new_layer.size
    assert result.message == ""
    assert result.max_mem_kib is not None
    assert result.max_mem_kib > 0
    assert type(result.elapsed_time_s) is float
    assert type(result.user_time_s) is float
    assert type(result.system_time_s) is float


def check_failure(key, status, message):
    redis_client = redis.Redis.from_url("redis://localhost")

    assert redis_client.scard('tardiff:pending') == 0
    assert redis_client.zscore('tardiff:progress', key) is None
    assert redis_client.zscore('tardiff:active', key) is None

    result_raw = redis_client.get(f"tardiff:result:{key}")
    assert result_raw is not None
    result = TardiffResultModel.from_json_text(result_raw)

    assert result.status == status
    assert result.digest == ""
    assert result.message == message


@mock_redis
@mock_registry(registry='registry.fedoraproject.org')
def test_differ(registry_mock, config):
    old_manifest_digest, old_layer = registry_mock.add_fake_image('ghex', 'latest')
    new_manifest_digest, new_layer = registry_mock.add_fake_image('ghex', 'latest')

    key = f"{old_layer.diff_id}:{new_layer.diff_id}"

    key = queue_task(old_manifest_digest, old_layer.diff_id,
                     new_manifest_digest, new_layer.diff_id)

    differ = Differ(config)
    differ.run(max_tasks=1)

    check_success(key, old_layer, new_layer)


@mock_redis
@mock_registry(registry='registry.fedoraproject.org')
def test_differ_no_spec(registry_mock, config):
    old_manifest_digest, old_layer = registry_mock.add_fake_image('ghex', 'latest')
    new_manifest_digest, new_layer = registry_mock.add_fake_image('ghex', 'latest')

    key = f"{old_layer.diff_id}:{new_layer.diff_id}"

    key = queue_task(old_manifest_digest, old_layer.diff_id,
                     new_manifest_digest, new_layer.diff_id,
                     skip_spec=True)

    differ = Differ(config)
    differ.run(max_tasks=1)

    check_failure(key, 'no-spec-error',
                  "failed to find spec")


@mock_redis
@mock_registry(registry='registry.fedoraproject.org')
@pytest.mark.parametrize('fail_from, fail_to', [
    (True, False),
    (False, True),
])
def test_differ_tardiff_download_error(registry_mock, config, fail_from, fail_to):
    if fail_from:
        old_manifest_digest, old_diff_id = 'sha256:not-there', 'whatever'
    else:
        old_manifest_digest, old_layer = registry_mock.add_fake_image('ghex', 'latest',
                                                                      layer_contents=b"GARBAGE")
        old_diff_id = old_layer.diff_id

    if fail_to:
        new_manifest_digest, new_diff_id = 'sha256:not-there', 'whatever'
    else:
        new_manifest_digest, new_layer = registry_mock.add_fake_image('ghex', 'latest',
                                                                      layer_contents=b"GARBAGE")
        new_diff_id = new_layer.diff_id

    key = queue_task(old_manifest_digest, old_diff_id,
                     new_manifest_digest, new_diff_id)

    differ = Differ(config)
    differ.run(max_tasks=1)

    check_failure(key, 'download-error',
                  "downloading from layer failed" if fail_from else "downloading to layer failed")


@mock_redis
@mock_registry(registry='registry.fedoraproject.org')
def test_differ_tardiff_failure(registry_mock, config):
    old_manifest_digest, old_layer = registry_mock.add_fake_image('ghex', 'latest',
                                                                  layer_contents=b"GARBAGE")
    new_manifest_digest, new_layer = registry_mock.add_fake_image('ghex', 'latest')

    key = queue_task(old_manifest_digest, old_layer.diff_id,
                     new_manifest_digest, new_layer.diff_id)

    differ = Differ(config)
    differ.run(max_tasks=1)

    check_failure(key, 'diff-error', "tardiff failed")


@mock_redis
@mock_registry(registry='registry.fedoraproject.org')
def test_differ_tardiff_slow(registry_mock, config):
    old_manifest_digest, old_layer = registry_mock.add_fake_image('ghex', 'latest')
    new_manifest_digest, new_layer = registry_mock.add_fake_image('ghex', 'latest')

    key = queue_task(old_manifest_digest, old_layer.diff_id,
                     new_manifest_digest, new_layer.diff_id)

    with timeout_first_popen_wait():
        differ = Differ(config)
        differ.run(max_tasks=1)

    check_success(key, old_layer, new_layer)


@mock_redis
@mock_registry(registry='registry.fedoraproject.org')
def test_differ_wait(registry_mock, config):
    old_manifest_digest, old_layer = registry_mock.add_fake_image('ghex', 'latest')
    new_manifest_digest, new_layer = registry_mock.add_fake_image('ghex', 'latest')

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

    check_success(key, old_layer, new_layer)
