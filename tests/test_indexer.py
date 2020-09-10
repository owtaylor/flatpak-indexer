import copy
import json
import os

import responses
import yaml

from flatpak_indexer.datasource import load_updaters
from flatpak_indexer.indexer import Indexer
from flatpak_indexer.models import RegistryModel
from .bodhi import mock_bodhi
from .fedora_messaging import mock_fedora_messaging
from .koji import mock_koji
from .redis import mock_redis
from .test_delta_generator import FakeDiffer
from .utils import get_config, mock_brew, mock_pyxis


def run_update(config):
    registry_data = {}
    for updater in load_updaters(config):
        updater.start()
        updater.update(registry_data)
        updater.stop()

    return registry_data


CONFIG = yaml.safe_load("""
pyxis_url: https://pyxis.example.com/v1
koji_config: brew
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
@mock_redis
@responses.activate
def test_indexer(tmp_path):
    mock_pyxis()

    os.environ["OUTPUT_DIR"] = str(tmp_path)

    os.makedirs(tmp_path / "icons" / "ba")
    with open(tmp_path / "icons" / "ba" / "bbled.png", "w"):
        pass

    config = get_config(tmp_path, CONFIG)
    registry_data = run_update(config)

    indexer = Indexer(config)
    indexer.index(registry_data)

    # No-op, datasource hasn't updated
    indexer.index(registry_data)

    # Fake an update
    registry_data['foo.example.com'] = RegistryModel()

    # Now the index will be rewritten
    indexer.index(registry_data)

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


def test_indexer_missing_data_source(tmp_path):
    os.environ["OUTPUT_DIR"] = str(tmp_path)
    os.mkdir(tmp_path / "icons")

    config = get_config(tmp_path, CONFIG)

    indexer = Indexer(config)
    indexer.index({})


KOJI_CONFIG = yaml.safe_load("""
pyxis_url: https://pyxis.example.com/v1
koji_config: brew
redis_url: redis://localhost
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
@mock_redis
@responses.activate
def test_indexer_koji(tmp_path):
    mock_pyxis()

    os.environ["OUTPUT_DIR"] = str(tmp_path)

    config = get_config(tmp_path, KOJI_CONFIG)
    registry_data = run_update(config)

    indexer = Indexer(config)
    indexer.index(registry_data)

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
    latest-annotations:
        registry: fedora
        output: ${OUTPUT_DIR}/test/flatpak-latest-annotations.json
        tag: latest
        bodhi_status: stable
        flatpak_annotations: true
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

    os.mkdir(tmp_path / "deltas")

    config = get_config(tmp_path, FEDORA_CONFIG)
    registry_data = run_update(config)

    with FakeDiffer(config):
        indexer = Indexer(config)
        indexer.index(registry_data)

    with open(tmp_path / "test/flatpak-latest.json") as f:
        data = json.load(f)

    assert data['Registry'] == 'https://registry.fedoraproject.org/'
    assert len(data['Results']) == 6

    eog_repository = [r for r in data['Results'] if r['Name'] == 'eog'][0]
    assert len(eog_repository['Images']) == 1
    assert eog_repository['Images'][0]['Tags'] == ['latest', 'testing']

    feedreader_repository = [r for r in data['Results'] if r['Name'] == 'feedreader'][0]
    assert len(feedreader_repository['Images']) == 1
    assert feedreader_repository['Images'][0]['Tags'] == ['latest']

    baobab_repository = [r for r in data['Results'] if r['Name'] == 'baobab'][0]
    assert len(baobab_repository['Images']) == 1

    baobab_image = baobab_repository['Images'][0]
    assert baobab_image['Labels']['org.flatpak.ref'] == 'app/org.gnome.Baobab/x86_64/stable'
    assert 'Annotations' not in baobab_image

    # Now check that the index with flatpak_annotations set has the Flatpak
    # metadata in the annotations, not in the labels.

    with open(tmp_path / "test/flatpak-latest-annotations.json") as f:
        data = json.load(f)

    baobab_repository = [r for r in data['Results'] if r['Name'] == 'baobab'][0]
    assert len(baobab_repository['Images']) == 1

    baobab_image = baobab_repository['Images'][0]
    assert 'org.gnome.baobab' not in baobab_image['Labels']
    assert baobab_image['Annotations']['org.flatpak.ref'] == 'app/org.gnome.Baobab/x86_64/stable'
