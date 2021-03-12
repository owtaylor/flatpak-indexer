from datetime import datetime, timedelta
import os
from unittest.mock import patch

import yaml

from flatpak_indexer.cleaner import Cleaner

from .redis import mock_redis
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

    with patch('flatpak_indexer.cleaner.datetime') as mock:
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
