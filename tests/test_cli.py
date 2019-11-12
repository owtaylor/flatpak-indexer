from unittest.mock import patch
import os

from click.testing import CliRunner
import pytest
import responses
import sys
import yaml

from flatpak_indexer.cli import cli

from .utils import write_config, mock_pyxis

CONFIG = yaml.safe_load("""
pyxis_url: https://pyxis.example.com/v1
icons_dir: ${OUTPUT_DIR}/icons/
icons_uri: https://flatpaks.example.com/icons
registries:
    registry.example.com:
        repositories: ['aisleriot']
indexes:
    amd64:
        registry: registry.example.com
        output: ${OUTPUT_DIR}/flatpak-amd64.json
        architecture: amd64
        tag: latest
        extract_icons: false
""")


@responses.activate
def test_daemon(tmp_path):
    mock_pyxis()
    config_path = write_config(tmp_path, CONFIG)

    data = {
        'sleep_count': 0
    }

    os.mkdir(tmp_path / "index")
    os.mkdir(tmp_path / "icons")
    os.environ["OUTPUT_DIR"] = str(tmp_path)
    runner = CliRunner()

    def mock_sleep(secs):
        data['sleep_count'] += 1
        if data['sleep_count'] == 2:
            sys.exit(42)
    with patch('time.sleep', side_effect=mock_sleep):
        result = runner.invoke(cli, ['--config-file', config_path, 'daemon'],
                               catch_exceptions=False)
        assert result.exit_code == 42


@responses.activate
@pytest.mark.parametrize('verbose', [False, True])
def test_index(tmp_path, caplog, verbose):
    mock_pyxis()
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
        assert 'Getting all images for registry.example.com/aisleriot' in caplog.text
    else:
        assert 'Getting all images for registry.example.com/aisleriot' not in caplog.text
    assert result.exit_code == 0
