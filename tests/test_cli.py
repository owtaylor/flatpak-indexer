from unittest.mock import patch
import os

from click.testing import CliRunner
import pytest
import sys
import yaml

from flatpak_indexer.cli import cli

from .pyxis import mock_pyxis
from .redis import mock_redis
from .utils import write_config, mock_brew

CONFIG = yaml.safe_load("""
pyxis_url: https://pyxis.example.com/v1
redis_url: redis://localhost
koji_config: brew
icons_dir: ${OUTPUT_DIR}/icons/
icons_uri: https://flatpaks.example.com/icons
registries:
    registry.example.com:
        repositories: ['aisleriot']
        public_url: https://registry.example.com/
        datasource: pyxis
indexes:
    amd64:
        registry: registry.example.com
        output: ${OUTPUT_DIR}/flatpak-amd64.json
        architecture: amd64
        tag: latest
        extract_icons: false
""")


@mock_brew
@mock_pyxis
@mock_redis
def test_daemon(tmp_path):
    config_path = write_config(tmp_path, CONFIG)

    sleep_count = 0

    os.mkdir(tmp_path / "index")
    os.mkdir(tmp_path / "icons")
    os.environ["OUTPUT_DIR"] = str(tmp_path)
    runner = CliRunner()

    def mock_sleep(secs):
        nonlocal sleep_count
        sleep_count += 1
        if sleep_count == 2:
            sys.exit(42)

    with patch('time.sleep', side_effect=mock_sleep):
        result = runner.invoke(cli, ['--config-file', config_path, 'daemon'],
                               catch_exceptions=False)
        assert result.exit_code == 42


@mock_brew
@mock_pyxis
@mock_redis
@pytest.mark.parametrize('where',
                         ['flatpak_indexer.indexer.Indexer.index',
                          'flatpak_indexer.datasource.pyxis.updater.PyxisUpdater.update',
                          'flatpak_indexer.cleaner.Cleaner.clean'])
def test_daemon_exception(tmp_path, where):
    config_path = write_config(tmp_path, CONFIG)

    os.mkdir(tmp_path / "index")
    os.mkdir(tmp_path / "icons")
    os.environ["OUTPUT_DIR"] = str(tmp_path)
    runner = CliRunner()

    exception_count = 0
    sleep_count = 0

    def mock_failure(*args):
        nonlocal exception_count
        exception_count += 1
        raise RuntimeError("Didn't work!")

    def mock_sleep(secs):
        nonlocal sleep_count
        sleep_count += 1
        if sleep_count == 2:
            sys.exit(42)

    with patch('time.sleep', side_effect=mock_sleep):
        with patch(where, side_effect=mock_failure):
            result = runner.invoke(cli, ['--config-file', config_path, 'daemon'],
                                   catch_exceptions=False)
            assert result.exit_code == 42
            assert exception_count == 2


@mock_brew
@mock_pyxis
@mock_redis
@pytest.mark.parametrize('verbose', [False, True])
def test_index(tmp_path, caplog, verbose):
    config_path = write_config(tmp_path, CONFIG)

    os.mkdir(tmp_path / "index")
    os.mkdir(tmp_path / "icons")
    os.environ["OUTPUT_DIR"] = str(tmp_path)
    runner = CliRunner()
    args = ['--config-file', config_path, 'index']
    if verbose:
        args.insert(0, '--verbose')
    result = runner.invoke(cli, args, catch_exceptions=False)
    os.unlink(config_path)
    if verbose:
        assert 'Calling koji.getBuild' in caplog.text
    else:
        assert 'Calling koji.getBuild' not in caplog.text
    assert result.exit_code == 0


DIFFER_CONFIG = yaml.safe_load("""
pyxis_url: https://pyxis.example.com/v1
redis_url: redis://localhost
koji_config: brew
deltas_dir: ${OUTPUT_DIR}/deltas/
deltas_uri: https://flatpaks.fedoraproject.org/deltas
""")


@mock_redis
def test_differ(tmp_path):
    config_path = write_config(tmp_path, DIFFER_CONFIG)

    os.environ["OUTPUT_DIR"] = str(tmp_path)
    os.mkdir(tmp_path / "deltas")

    runner = CliRunner()

    def mock_get_message(*args, **kwargs):
        sys.exit(42)

    with patch('redis.client.PubSub.get_message', side_effect=mock_get_message):
        result = runner.invoke(cli, ['--config-file', config_path, 'differ'],
                               catch_exceptions=False)
        assert result.exit_code == 42
