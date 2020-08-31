import copy
import json
import os

import responses
import yaml

from flatpak_indexer.datasource import load_updaters
from flatpak_indexer.indexer import Indexer
from .bodhi import mock_bodhi
from .fedora_messaging import mock_fedora_messaging
from .koji import mock_koji
from .redis import mock_redis
from .test_delta_generator import FakeDiffer
from .utils import get_config, mock_brew, mock_pyxis


def run_update(config):
    for updater in load_updaters(config):
        updater.start()
        updater.update()
        updater.stop()


CONFIG = yaml.safe_load("""
pyxis_url: https://pyxis.example.com/v1
koji_config: brew
work_dir: ${OUTPUT_DIR}/work
deltas_dir: ${OUTPUT_DIR}/deltas
deltas_uri: https://registry.fedoraproject.org/deltas
redis_url: redis://localhost
icons_dir: ${OUTPUT_DIR}/icons/
icons_uri: https://flatpaks.example.com/icons
registries:
    registry.example.com:
        public_url: https://registry.example.com/
        datasource: pyxis
indexes:
    amd64:
        delta_keep_days: 7
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


@mock_brew
@responses.activate
def test_indexer(tmp_path):
    mock_pyxis()

    os.environ["OUTPUT_DIR"] = str(tmp_path)

    os.mkdir(tmp_path / "work")

    os.makedirs(tmp_path / "icons" / "ba")
    with open(tmp_path / "icons" / "ba" / "bbled.png", "w"):
        pass

    config = get_config(tmp_path, CONFIG)
    run_update(config)

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

    assert 'PullSpec' not in aisleriot_image
    assert 'DiffIds' not in aisleriot_image

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
        datasource: pyxis
indexes:
    brew-rc:
        registry: brew
        architecture: amd64
        output: ${OUTPUT_DIR}/test/brew.json
        koji_tag: release-candidate
""")


@mock_brew
@responses.activate
def test_indexer_koji(tmp_path):
    mock_pyxis()

    os.environ["OUTPUT_DIR"] = str(tmp_path)

    os.mkdir(tmp_path / "work")

    config = get_config(tmp_path, KOJI_CONFIG)
    run_update(config)

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


FEDORA_CONFIG = yaml.safe_load("""
koji_config: brew
redis_url: redis://localhost
deltas_dir: ${OUTPUT_DIR}/deltas
deltas_uri: https://registry.fedoraproject.org/deltas
work_dir: ${OUTPUT_DIR}/work
registries:
    fedora:
        public_url: https://registry.fedoraproject.org/
        datasource: fedora
        force_flatpak_token: true
indexes:
    latest:
        registry: fedora
        output: ${OUTPUT_DIR}/test/flatpak-latest.json
        tag: latest
        bodhi_status: stable
        delta_keep_days: 10000
    testing:
        registry: fedora
        output: ${OUTPUT_DIR}/test/flatpak-testing.json
        tag: testing
        bodhi_status: testing
        delta_keep_days: 10000
""")


@mock_fedora_messaging
@mock_koji
@mock_redis
@responses.activate
def test_indexer_fedora(mock_connection, tmp_path):
    mock_connection.put_inactivity_timeout()

    def modify_statuses(update):
        # This build is now obsoleted by a build not in our test date, mark it testing so that
        # we have a repository with different stable/testing
        if update['builds'][0]['nvr'] == 'feedreader-master-2920190201081220.1':
            update = copy.copy(update)
            update['status'] = 'testing'

        return update

    mock_bodhi(modify=modify_statuses)

    os.environ["OUTPUT_DIR"] = str(tmp_path)

    os.mkdir(tmp_path / "work")
    os.mkdir(tmp_path / "deltas")

    config = get_config(tmp_path, FEDORA_CONFIG)
    run_update(config)

    with FakeDiffer(config):
        indexer = Indexer(config)
        indexer.index()

    with open(tmp_path / "test/flatpak-latest.json") as f:
        data = json.load(f)

    assert data['Registry'] == 'https://registry.fedoraproject.org/'
    assert len(data['Results']) == 5

    eog_repository = [r for r in data['Results'] if r['Name'] == 'eog'][0]
    assert len(eog_repository['Images']) == 1
    assert eog_repository['Images'][0]['Tags'] == ['latest', 'testing']

    feedreader_repository = [r for r in data['Results'] if r['Name'] == 'feedreader'][0]
    assert len(feedreader_repository['Images']) == 1
    assert feedreader_repository['Images'][0]['Tags'] == ['latest']
