import json
import os
from unittest.mock import patch

import pytest
import responses
import yaml

from flatpak_indexer.indexer import Indexer
from .utils import get_config, mock_koji, mock_pyxis, setup_client_cert


CONFIG = yaml.safe_load("""
pyxis_url: https://pyxis.example.com/v1
icons_dir: ${OUTPUT_DIR}/icons/
icons_uri: https://flatpaks.example.com/icons
registries:
    registry.example.com:
        public_url: https://registry.example.com/
indexes:
    amd64:
        architecture: amd64
        registry: registry.example.com
        output: ${OUTPUT_DIR}/test/flatpak-amd64.json
        tag: latest
        extract_icons: true
    all:
        registry: registry.example.com
        output: ${OUTPUT_DIR}/test/flatpak.json
        tag: latest
        extract_icons: false
""")


@pytest.mark.parametrize("server_cert,client_cert",
                         [(False, False),
                          (True,  False),
                          (False, True)])
@responses.activate
def test_indexer(tmp_path, server_cert, client_cert):
    mock_pyxis()

    os.environ["OUTPUT_DIR"] = str(tmp_path)

    os.makedirs(tmp_path / "icons" / "ba")
    with open(tmp_path / "icons" / "ba" / "bbled.png", "w"):
        pass

    config = get_config(tmp_path, CONFIG)
    if server_cert:
        config.pyxis_cert = 'test.crt'
    if client_cert:
        config.pyxis_client_cert, config.pyxis_client_key = setup_client_cert(tmp_path)

    indexer = Indexer(config, page_size=1)

    indexer.index()
    indexer.index()

    with open(tmp_path / "test/flatpak-amd64.json") as f:
        amd64_data = json.load(f)

    assert amd64_data['Registry'] == 'https://registry.example.com/'
    assert len(amd64_data['Results']) == 2
    aisleriot_repository = [r for r in amd64_data['Results'] if r['Name'] == 'aisleriot'][0]
    assert len(aisleriot_repository['Images']) == 1
    aisleriot_image = aisleriot_repository['Images'][0]
    assert aisleriot_image['Digest'] == \
        'sha256:527dda0ec4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb'
    assert aisleriot_image['Labels']['org.flatpak.ref'] == \
        'app/org.gnome.Aisleriot/x86_64/stable'
    assert aisleriot_image['Labels']['org.freedesktop.appstream.icon-128'] == \
        "https://www.example.com/icons/aisleriot.png"

    icon_url = aisleriot_image['Labels']['org.freedesktop.appstream.icon-64']
    assert icon_url.startswith('https://flatpaks.example.com/icons')
    icon_subpath = icon_url.split('/')[-2:]
    assert (tmp_path / 'icons' / icon_subpath[0] / icon_subpath[1]).exists()

    assert not (tmp_path / "icons" / "ba" / "bbled.png").exists()


@responses.activate
def test_indexer_write_failure(tmp_path):
    mock_pyxis()

    os.makedirs(tmp_path / "icons")

    os.environ["OUTPUT_DIR"] = str(tmp_path)
    config = get_config(tmp_path, CONFIG)
    indexer = Indexer(config, page_size=1)

    with patch('json.dump', side_effect=IOError):
        with pytest.raises(IOError):
            indexer.index()

    assert os.listdir(tmp_path / "test") == []


REPOSITORY_OVERRIDE_CONFIG = yaml.safe_load("""
pyxis_url: https://pyxis.example.com/v1
registries:
    registry.example.com:
        public_url: https://registry.example.com/
        repositories: ['testrepo']
indexes:
    amd64:
        architecture: amd64
        registry: registry.example.com
        output: ${OUTPUT_DIR}/test/flatpak-amd64.json
        tag: latest
""")


@mock_koji
@responses.activate
def test_indexer_repository_override(tmp_path):
    mock_pyxis()

    os.environ["OUTPUT_DIR"] = str(tmp_path)

    config = get_config(tmp_path, REPOSITORY_OVERRIDE_CONFIG)
    indexer = Indexer(config)

    indexer.index()

    with open(tmp_path / "test/flatpak-amd64.json") as f:
        amd64_data = json.load(f)

    assert amd64_data['Registry'] == 'https://registry.example.com/'
    assert len(amd64_data['Results']) == 1
    testrepo_repository = amd64_data['Results'][0]
    assert testrepo_repository['Name'] == 'testrepo'



KOJI_CONFIG = yaml.safe_load("""
pyxis_url: https://pyxis.example.com/v1
registries:
    brew:
        public_url: https://internal.example.com/
        koji_config: brew
indexes:
    brew-rc:
        registry: brew
        architecture: amd64
        output: ${OUTPUT_DIR}/test/brew.json
        koji_tag: release-candidate
""")


@mock_koji
@responses.activate
def test_indexer_koji(tmp_path):
    mock_pyxis()

    os.environ["OUTPUT_DIR"] = str(tmp_path)

    config = get_config(tmp_path, KOJI_CONFIG)
    indexer = Indexer(config)

    indexer.index()

    with open(tmp_path / "test/brew.json") as f:
        data = json.load(f)

    assert data['Registry'] == 'https://internal.example.com/'
    assert len(data['Results']) == 1
    aisleriot_repository = data['Results'][0]
    assert aisleriot_repository['Name'] == 'rh-osbs/aisleriot'
    print([i['Architecture'] for i in aisleriot_repository['Images']])
    assert len(aisleriot_repository['Images']) == 1
    aisleriot_image = aisleriot_repository['Images'][0]
    assert aisleriot_image['Digest'] == \
        'sha256:527dda0ec4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb'
