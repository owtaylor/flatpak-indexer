import json
import os

import pytest
import responses
import yaml

from flatpak_indexer.datasource.pyxis import PyxisUpdater
from .utils import get_config, mock_brew, mock_pyxis, setup_client_cert

from .redis import mock_redis

CONFIG = yaml.safe_load("""
work_dir: ${WORK_DIR}
pyxis_url: https://pyxis.example.com/v1
redis_url: redis://localhost
koji_config: brew
registries:
    registry.example.com:
        public_url: https://registry.example.com/
        datasource: pyxis
    fedora:
        public_url: https://registry.fedoraproject.org
        datasource: fedora
indexes:
    amd64:
        architecture: amd64
        registry: registry.example.com
        output: out/test/flatpak-amd64.json
        tag: latest
    all:
        registry: registry.example.com
        output: out/test/flatpak.json
        tag: latest
    # Not a Pyxis-backed index
    fedora-latest:
        registry: fedora
        output: out/fedora/flatpak.json
        bodhi_status: stable
        tag: latest
""")


@pytest.mark.parametrize("server_cert,client_cert",
                         [(False, False),
                          (True,  False),
                          (False, True)])
@mock_brew
@mock_redis
@responses.activate
def test_pyxis_updater(tmp_path, server_cert, client_cert):
    mock_pyxis()

    os.environ["WORK_DIR"] = str(tmp_path)

    config = get_config(tmp_path, CONFIG)
    if server_cert:
        config.local_certs['pyxis.example.com'] = 'test.crt'
    if client_cert:
        config.pyxis_client_cert, config.pyxis_client_key = setup_client_cert(tmp_path)

    updater = PyxisUpdater(config, page_size=1)

    updater.start()
    try:
        updater.update()
    finally:
        updater.stop()

    with open(tmp_path / "registry.example.com.json") as f:
        data = json.load(f)

    assert len(data['Repositories']) == 2
    aisleriot_repository = [r for r in data['Repositories'] if r['Name'] == 'aisleriot'][0]
    assert len(aisleriot_repository['Images']) == 1
    aisleriot_image = aisleriot_repository['Images'][0]
    assert aisleriot_image['Digest'] == \
        'sha256:bo1dfacec4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb'
    assert aisleriot_image['Labels']['org.flatpak.ref'] == \
        'app/org.gnome.Aisleriot/x86_64/stable'
    assert aisleriot_image['Labels']['org.freedesktop.appstream.icon-128'] == \
        "https://www.example.com/icons/aisleriot.png"


REPOSITORY_OVERRIDE_CONFIG = yaml.safe_load("""
work_dir: ${WORK_DIR}
pyxis_url: https://pyxis.example.com/v1
redis_url: redis://localhost
koji_config: brew
registries:
    registry.example.com:
        public_url: https://registry.example.com/
        repositories: ['testrepo']
        datasource: pyxis
indexes:
    amd64:
        architecture: amd64
        registry: registry.example.com
        output: out/test/flatpak-amd64.json
        tag: latest
""")


@mock_brew
@mock_redis
@responses.activate
def test_pyxis_updater_repository_override(tmp_path):
    mock_pyxis()

    os.environ["WORK_DIR"] = str(tmp_path)

    config = get_config(tmp_path, REPOSITORY_OVERRIDE_CONFIG)
    updater = PyxisUpdater(config)

    updater.start()
    try:
        updater.update()
    finally:
        updater.stop()

    with open(tmp_path / "registry.example.com.json") as f:
        amd64_data = json.load(f)

    assert len(amd64_data['Repositories']) == 1
    testrepo_repository = amd64_data['Repositories'][0]
    assert testrepo_repository['Name'] == 'testrepo'


KOJI_CONFIG = yaml.safe_load("""
work_dir: ${WORK_DIR}
pyxis_url: https://pyxis.example.com/v1
redis_url: redis://localhost
koji_config: brew
registries:
    brew:
        public_url: https://internal.example.com/
        datasource: pyxis
indexes:
    brew-rc:
        registry: brew
        architecture: amd64
        output: out/test/brew.json
        koji_tag: release-candidate
""")


@mock_brew
@mock_redis
@responses.activate
def test_pyxis_updater_koji(tmp_path):
    mock_pyxis()

    os.environ["WORK_DIR"] = str(tmp_path)

    config = get_config(tmp_path, KOJI_CONFIG)
    updater = PyxisUpdater(config)

    updater.start()
    try:
        updater.update()
    finally:
        updater.stop()

    with open(tmp_path / "brew.json") as f:
        data = json.load(f)

    assert len(data['Repositories']) == 1
    aisleriot_repository = data['Repositories'][0]
    assert aisleriot_repository['Name'] == 'rh-osbs/aisleriot'
    assert len(aisleriot_repository['Images']) == 1
    aisleriot_image = aisleriot_repository['Images'][0]
    assert aisleriot_image['Digest'] == \
        'sha256:bo1dfacec4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb'
