import copy
import json
import os
from typing import Dict

import yaml

from flatpak_indexer.cleaner import Cleaner
from flatpak_indexer.datasource import load_updaters
from flatpak_indexer.indexer import Indexer
from flatpak_indexer.models import RegistryModel
from flatpak_indexer.test.bodhi import mock_bodhi
from flatpak_indexer.test.koji import mock_koji
from flatpak_indexer.test.redis import mock_redis
from .fedora_messaging import mock_fedora_messaging
from .pyxis import mock_pyxis
from .test_delta_generator import FakeDiffer
from .utils import get_config, mock_brew, mock_odcs


def run_update(config):
    registry_data: Dict[str, RegistryModel] = {}
    for updater in load_updaters(config):
        updater.start()
        updater.update(registry_data)
        updater.stop()

    return registry_data


CONFIG = yaml.safe_load("""
koji_config: brew
odcs_uri: https://odcs.example.com/
deltas_dir: ${OUTPUT_DIR}/deltas
deltas_uri: https://registry.fedoraproject.org/deltas
redis_url: redis://localhost
icons_dir: ${OUTPUT_DIR}/icons/
icons_uri: https://flatpaks.example.com/icons
clean_files_after: 0s
registries:
    registry.example.com:
        public_url: https://registry.example.com/
        datasource: pyxis
        pyxis_url: https://pyxis.example.com/graphql
indexes:
    amd64:
        delta_keep: 7d
        architecture: amd64
        registry: registry.example.com
        output: ${OUTPUT_DIR}/test/flatpak-amd64.json
        tag: latest
        extract_icons: true
        # Prefer el8 repositories to el9 repositories
        repository_priority: ['el8/.*', 'el9/.*']
    amd64-reversed:
        delta_keep: 7d
        architecture: amd64
        registry: registry.example.com
        output: ${OUTPUT_DIR}/test/flatpak-amd64-reversed.json
        tag: latest
        extract_icons: true
        # Prefer el9 repositories to el8 repositories (more natural)
        repository_priority: ['el9/.*', 'el8/.*']
    amd64-annotations:
        delta_keep: 7d
        architecture: amd64
        registry: registry.example.com
        output: ${OUTPUT_DIR}/test/flatpak-amd64-annotations.json
        tag: latest
        extract_icons: true
        flatpak_annotations: true
    all:
        registry: registry.example.com
        output: ${OUTPUT_DIR}/test/flatpak.json
        tag: latest
        extract_icons: false
""")


@mock_brew
@mock_odcs
@mock_pyxis
@mock_redis
def test_indexer(tmp_path):
    os.environ["OUTPUT_DIR"] = str(tmp_path)

    os.makedirs(tmp_path / "icons" / "ba")
    with open(tmp_path / "icons" / "ba" / "bbled.png", "w"):
        pass

    config = get_config(tmp_path, CONFIG)
    registry_data = run_update(config)

    cleaner = Cleaner(config)
    indexer = Indexer(config, cleaner=cleaner)
    indexer.index(registry_data)

    indexer.index(registry_data)
    cleaner.clean()

    with open(tmp_path / "test/flatpak-amd64.json") as f:
        amd64_data = json.load(f)

    assert amd64_data['Registry'] == 'https://registry.example.com/'
    assert len(amd64_data['Results']) == 2
    aisleriot_repository = [r for r in amd64_data['Results'] if r['Name'] == 'el8/aisleriot'][0]
    assert len(aisleriot_repository['Images']) == 1
    aisleriot_image = aisleriot_repository['Images'][0]
    assert aisleriot_image['Digest'] == \
        'sha256:bo1dfacec4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb'
    assert aisleriot_image['Labels']['org.flatpak.ref'] == \
        'app/org.gnome.Aisleriot/x86_64/stable'
    assert aisleriot_image['Labels']['org.freedesktop.appstream.icon-128'] == \
        "https://www.example.com/icons/aisleriot.png"

    assert aisleriot_image.get('Annotations', {}).get('org.flatpak.ref') is None

    assert 'PullSpec' not in aisleriot_image
    assert 'DiffIds' not in aisleriot_image

    icon_url = aisleriot_image['Labels']['org.freedesktop.appstream.icon-64']
    assert icon_url.startswith('https://flatpaks.example.com/icons')
    icon_subpath = icon_url.split('/')[-2:]
    assert (tmp_path / 'icons' / icon_subpath[0] / icon_subpath[1]).exists()

    assert not (tmp_path / "icons" / "ba" / "bbled.png").exists()

    # Check that when we reverse the repository priority we get different images

    with open(tmp_path / "test/flatpak-amd64-reversed.json") as f:
        reversed_data = json.load(f)

    assert len(reversed_data['Results']) == 2
    print(reversed_data)
    aisleriot_repository = [r for r in reversed_data['Results'] if r['Name'] == 'el9/aisleriot'][0]
    assert len(aisleriot_repository['Images']) == 1
    aisleriot_image = aisleriot_repository['Images'][0]
    assert aisleriot_image['Digest'] == \
        'sha256:ba5eba11c4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb'

    # Now check that the index with flatpak_annotations set has the Flatpak
    # metadata in the annotations, not in the labels.

    with open(tmp_path / "test/flatpak-amd64-annotations.json") as f:
        amd64_data = json.load(f)

    aisleriot_repository = [r for r in amd64_data['Results'] if r['Name'] == 'el8/aisleriot'][0]
    aisleriot_image = aisleriot_repository['Images'][0]
    assert aisleriot_image['Annotations']['org.flatpak.ref'] == \
        'app/org.gnome.Aisleriot/x86_64/stable'
    assert aisleriot_image.get('Labels', {}).get('org.flatpak.ref') is None

    icon_url = aisleriot_image['Annotations']['org.freedesktop.appstream.icon-64']
    assert icon_url.startswith('https://flatpaks.example.com/icons')


@mock_brew
def test_indexer_missing_data_source(tmp_path):
    os.environ["OUTPUT_DIR"] = str(tmp_path)
    os.mkdir(tmp_path / "icons")

    config = get_config(tmp_path, CONFIG)

    indexer = Indexer(config)
    indexer.index({})


KOJI_CONFIG = yaml.safe_load("""
koji_config: brew
odcs_uri: https://odcs.example.com/
redis_url: redis://localhost
registries:
    brew:
        public_url: https://internal.example.com/
        force_flatpak_token: true
        datasource: koji
indexes:
    brew-rc:
        registry: brew
        architecture: amd64
        output: ${OUTPUT_DIR}/test/brew.json
        tag: release-candidate
        koji_tags: [release-candidate]
""")


@mock_brew
@mock_odcs
@mock_pyxis
@mock_redis
def test_indexer_koji(tmp_path):
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
        contents: ${OUTPUT_DIR}/test/contents/latest
        tag: latest
        bodhi_status: stable
        delta_keep: 10000d
    testing:
        registry: fedora
        output: ${OUTPUT_DIR}/test/flatpak-testing.json
        tag: testing
        bodhi_status: testing
        delta_keep: 10000d
""")


def modify_statuses(update):
    # This build is now obsoleted by a build not in our test date, mark it testing so that
    # we have a repository with different stable/testing
    if update['builds'][0]['nvr'] == 'feedreader-master-2920190201081220.1':
        update = copy.copy(update)
        update['status'] = 'testing'

    return update


@mock_bodhi(modify=modify_statuses)
@mock_fedora_messaging
@mock_koji
@mock_redis
def test_indexer_fedora(connection_mock, tmp_path):
    connection_mock.put_inactivity_timeout()

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
    assert len(data['Results']) == 4

    eog_repository = [r for r in data['Results'] if r['Name'] == 'eog'][0]
    assert len(eog_repository['Images']) == 2
    assert sorted(i['Architecture'] for i in eog_repository['Images']) == ['amd64', 'arm64']
    assert eog_repository['Images'][0]['Tags'] == ['latest', 'testing']

    feedreader_repository = [r for r in data['Results'] if r['Name'] == 'feedreader'][0]
    assert len(feedreader_repository['Images']) == 2
    assert sorted(i['Architecture'] for i in feedreader_repository['Images']) == ['amd64', 'arm64']
    assert feedreader_repository['Images'][0]['Tags'] == ['latest', 'testing']

    baobab_repository = [r for r in data['Results'] if r['Name'] == 'baobab'][0]
    assert len(baobab_repository['Images']) == 2

    baobab_image = [i for i in baobab_repository['Images'] if i['Architecture'] == 'amd64'][0]
    assert baobab_image['Labels']['org.flatpak.ref'] == 'app/org.gnome.baobab/x86_64/stable'

    with open(tmp_path / "test/contents/latest/modules/baobab:stable.json") as f:
        module_data = json.load(f)

    assert module_data == {
        'Images': [{
            'ImageNvr': 'baobab-stable-3620220517102805.1',
            'ModuleNvr': 'baobab-stable-3620220517102805.cab77b58',
            'PackageBuilds': [
                  {'Nvr': 'baobab-42.0-1.module_f36+14451+219d93a5',
                   'SourceNvr': 'baobab-42.0-1.module_f36+14451+219d93a5'}
              ]
        }]
    }
