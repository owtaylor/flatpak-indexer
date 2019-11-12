import json
import os

import pytest
import responses
import yaml

from flatpak_indexer.indexer import Indexer
from .utils import get_config, mock_pyxis


CONFIG = yaml.safe_load("""
pyxis_url: https://pyxis.example.com/v1
icons_dir: ${OUTPUT_DIR}/icons/
icons_uri: https://flatpaks.example.com/icons
registries:
    registry.example.com:
        public_url: https://registry.example.com/
        repositories: ['aisleriot', 'aisleriot2', 'testrepo']
indexes:
    amd64:
        architecture: amd64
        registry: registry.example.com
        output: ${OUTPUT_DIR}/index/flatpak-amd64.json
        tag: latest
        extract_icons: true
    all:
        registry: registry.example.com
        output: ${OUTPUT_DIR}/index/flatpak.json
        tag: latest
        extract_icons: false
""")


@pytest.mark.parametrize("pyxis_cert",
                         ["test.crt",
                          None])
@responses.activate
def test_indexer(tmp_path, pyxis_cert):
    mock_pyxis()

    os.environ["OUTPUT_DIR"] = str(tmp_path)

    os.mkdir(tmp_path / "index")
    os.makedirs(tmp_path / "icons" / "ba")
    with open(tmp_path / "icons" / "ba" / "bbled.png", "w"):
        pass

    config = get_config(tmp_path, CONFIG)
    config.pyxis_cert = pyxis_cert
    indexer = Indexer(config, page_size=1)

    indexer.index()
    indexer.index()

    with open(tmp_path / "index/flatpak-amd64.json") as f:
        amd64_data = json.load(f)

    assert amd64_data['Registry'] == 'https://registry.example.com/'
    assert len(amd64_data['Results']) == 3
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
