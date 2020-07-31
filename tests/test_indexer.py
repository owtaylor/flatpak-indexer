import json
import os

import responses
import yaml

from flatpak_indexer.indexer import Indexer
from flatpak_indexer.datasource.pyxis import PyxisUpdater
from .utils import get_config, mock_koji, mock_pyxis


CONFIG = yaml.safe_load("""
pyxis_url: https://pyxis.example.com/v1
koji_config: brew
work_dir: ${OUTPUT_DIR}/work
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


@mock_koji
@responses.activate
def test_indexer(tmp_path):
    mock_pyxis()

    os.environ["OUTPUT_DIR"] = str(tmp_path)

    os.mkdir(tmp_path / "work")

    os.makedirs(tmp_path / "icons" / "ba")
    with open(tmp_path / "icons" / "ba" / "bbled.png", "w"):
        pass

    config = get_config(tmp_path, CONFIG)

    updater = PyxisUpdater(config)
    updater.update()

    indexer = Indexer(config)
    indexer.index()

    # No-op, datasource hasn't updated
    indexer.index()

    # Fake an update
    intermediate = tmp_path / "work" / "registry.example.com.json"
    modified = os.stat(intermediate).st_mtime
    os.utime(intermediate, (modified + 1, modified + 1))

    # Now the index will be rewritten
    indexer.index()

    with open(tmp_path / "test/flatpak-amd64.json") as f:
        amd64_data = json.load(f)

    assert amd64_data['Registry'] == 'https://registry.example.com/'
    assert len(amd64_data['Results']) == 2
    aisleriot_repository = [r for r in amd64_data['Results'] if r['Name'] == 'aisleriot'][0]
    assert len(aisleriot_repository['Images']) == 1
    aisleriot_image = aisleriot_repository['Images'][0]
    assert aisleriot_image['Digest'] == \
        'sha256:bo1dfacec4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb'
    assert aisleriot_image['Labels']['org.flatpak.ref'] == \
        'app/org.gnome.Aisleriot/x86_64/stable'
    assert aisleriot_image['Labels']['org.freedesktop.appstream.icon-128'] == \
        "https://www.example.com/icons/aisleriot.png"

    icon_url = aisleriot_image['Labels']['org.freedesktop.appstream.icon-64']
    assert icon_url.startswith('https://flatpaks.example.com/icons')
    icon_subpath = icon_url.split('/')[-2:]
    assert (tmp_path / 'icons' / icon_subpath[0] / icon_subpath[1]).exists()

    assert not (tmp_path / "icons" / "ba" / "bbled.png").exists()


def test_indexer_empty(tmp_path):
    os.environ["OUTPUT_DIR"] = str(tmp_path)

    config = get_config(tmp_path, CONFIG)
    config.indexes = []

    indexer = Indexer(config)
    indexer.index()


def test_indexer_missing_data_source(tmp_path):
    os.environ["OUTPUT_DIR"] = str(tmp_path)
    os.mkdir(tmp_path / "work")
    os.mkdir(tmp_path / "icons")

    config = get_config(tmp_path, CONFIG)

    indexer = Indexer(config)
    indexer.index()


KOJI_CONFIG = yaml.safe_load("""
pyxis_url: https://pyxis.example.com/v1
koji_config: brew
work_dir: ${OUTPUT_DIR}/work
registries:
    brew:
        public_url: https://internal.example.com/
        force_flatpak_token: true
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

    os.mkdir(tmp_path / "work")

    config = get_config(tmp_path, KOJI_CONFIG)

    updater = PyxisUpdater(config)
    updater.update()

    indexer = Indexer(config)
    indexer.index()

    with open(tmp_path / "test/brew.json") as f:
        data = json.load(f)

    assert data['Registry'] == 'https://internal.example.com/'
    assert len(data['Results']) == 1
    aisleriot_repository = data['Results'][0]
    assert aisleriot_repository['Name'] == 'rh-osbs/aisleriot'
    assert len(aisleriot_repository['Images']) == 1
    aisleriot_image = aisleriot_repository['Images'][0]
    assert aisleriot_image['Digest'] == \
        'sha256:bo1dfacec4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb'
    assert aisleriot_image['Labels']['org.flatpak.commit-metadata.xa.token-type'] == \
        'AQAAAABp'
