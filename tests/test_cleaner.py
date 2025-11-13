from datetime import datetime, timedelta
from unittest.mock import patch
import os

import yaml

from flatpak_indexer.cleaner import Cleaner
from flatpak_indexer.models import TardiffResultModel
from flatpak_indexer.test.redis import mock_redis
from flatpak_indexer.utils import path_for_digest

from .utils import get_config

CONFIG = yaml.safe_load("""
koji_config: brew
redis_url: redis://localhost
deltas_dir: ${OUTPUT_DIR}/deltas
deltas_uri: https://registry.fedoraproject.org/deltas
icons_dir: ${OUTPUT_DIR}/icons/
icons_uri: https://flatpaks.example.com/icons
clean_files_after: 1s
registries: []
indexes: []
""")


@mock_redis
def test_cleaner(tmp_path):
    os.environ["OUTPUT_DIR"] = str(tmp_path)

    config = get_config(tmp_path, CONFIG)

    os.mkdir(tmp_path / "deltas")
    os.mkdir(tmp_path / "icons")
    os.mkdir(tmp_path / "icons" / "aa")

    cleaner = Cleaner(config)

    with open(tmp_path / "deltas" / "abc.tardiff", "w"):
        pass
    with open(tmp_path / "deltas" / "xyz.tardiff", "w"):
        pass
    with open(tmp_path / "icons" / "aa" / "xyz.png", "w"):
        pass

    cleaner.reference(str(tmp_path / "deltas" / "abc.tardiff"))

    with patch("flatpak_indexer.cleaner.datetime") as mock:
        mock.now.return_value = datetime.now() + timedelta(seconds=100)

        cleaner.clean()

        # abc.tardiff is kept (even though it is old), because it is from the current cycle
        assert os.path.exists(tmp_path / "deltas" / "abc.tardiff")

        # but xyz.tardiff and aa/xyz.png are deleted
        assert not os.path.exists(tmp_path / "deltas" / "xyz.tardiff")
        assert not os.path.exists(tmp_path / "icons" / "aa" / "xyz.png")

        # in a new cycle, abc.tardiff is seen as stale and deleted
        cleaner.reset()
        cleaner.clean()
        assert not os.path.exists(tmp_path / "deltas" / "abc.tardiff")


@mock_redis
def test_clean_tardiff_results(tmp_path):
    os.environ["OUTPUT_DIR"] = str(tmp_path)

    config = get_config(tmp_path, CONFIG)

    os.mkdir(tmp_path / "deltas")

    cleaner = Cleaner(config)

    result1 = TardiffResultModel(status="success", digest="sha256:1234", size=42, message="")
    path1 = path_for_digest(config.deltas_dir, result1.digest, ".tardiff")
    os.makedirs(os.path.dirname(path1))
    with open(path1, "w"):
        pass
    cleaner.redis.set("tardiff:result:task1", result1.to_json_text())

    result2 = TardiffResultModel(status="success", digest="sha256:abcd", size=42, message="")
    path2 = path_for_digest(config.deltas_dir, result2.digest, ".tardiff")
    os.makedirs(os.path.dirname(path2))
    with open(path2, "w"):
        pass
    cleaner.redis.set("tardiff:result:task2", result2.to_json_text())

    cleaner.reference(path1)

    with (
        patch("flatpak_indexer.cleaner.datetime") as mock,
        patch("flatpak_indexer.cleaner.CLEAN_RESULTS_BATCH_SIZE", 1),
    ):
        mock.now.return_value = datetime.now() + timedelta(seconds=100)

        cleaner.clean()

    assert os.path.exists(path1)
    assert not os.path.exists(path2)

    assert cleaner.redis.get("tardiff:result:task1") is not None
    assert cleaner.redis.get("tardiff:result:task2") is None
