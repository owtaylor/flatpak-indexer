from copy import deepcopy

from requests.exceptions import HTTPError
import pytest
import yaml

from flatpak_indexer.datasource.pyxis import PyxisUpdater

from .pyxis import mock_pyxis
from .redis import mock_redis
from .utils import get_config, mock_brew, setup_client_cert


def run_update(updater):
    registry_data = {}

    updater.start()
    try:
        updater.update(registry_data)
    finally:
        updater.stop()

    return registry_data


CONFIG = yaml.safe_load("""
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
@mock_pyxis
@mock_redis
def test_pyxis_updater(tmp_path, server_cert, client_cert):
    config = get_config(tmp_path, CONFIG)
    if server_cert:
        config.local_certs['pyxis.example.com'] = 'test.crt'
    if client_cert:
        config.pyxis_client_cert, config.pyxis_client_key = setup_client_cert(tmp_path)

    updater = PyxisUpdater(config, page_size=1)

    registry_data = run_update(updater)
    data = registry_data['registry.example.com']

    assert len(data.repositories) == 2
    aisleriot_repository = data.repositories['aisleriot']
    assert len(aisleriot_repository.images) == 1
    aisleriot_image = next(iter(aisleriot_repository.images.values()))
    assert aisleriot_image.digest == \
        'sha256:bo1dfacec4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb'
    assert aisleriot_image.labels['org.flatpak.ref'] == \
        'app/org.gnome.Aisleriot/x86_64/stable'
    assert aisleriot_image.labels['org.freedesktop.appstream.icon-128'] == \
        "https://www.example.com/icons/aisleriot.png"


@mock_brew
@mock_pyxis(fail_tag_history=True)
@mock_redis
def test_pyxis_updater_tag_history_exception(tmp_path):
    config = get_config(tmp_path, CONFIG)
    updater = PyxisUpdater(config, page_size=1)

    with pytest.raises(HTTPError, match=r"403 Client Error"):
        run_update(updater)


REPOSITORY_OVERRIDE_CONFIG = yaml.safe_load("""
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
@mock_pyxis
@mock_redis
def test_pyxis_updater_repository_override(tmp_path):

    config = get_config(tmp_path, REPOSITORY_OVERRIDE_CONFIG)
    updater = PyxisUpdater(config)

    registry_data = run_update(updater)
    amd64_data = registry_data['registry.example.com']

    assert len(amd64_data.repositories) == 1
    testrepo_repository = amd64_data.repositories['testrepo']
    assert testrepo_repository.name == 'testrepo'


KOJI_CONFIG = yaml.safe_load("""
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
        tag: release-candidate
        koji_tags: [release-candidate, release-candidate-2]
    brew-rc-2:
        registry: brew
        architecture: amd64
        output: out/test/brew-2.json
        tag: release-candidate-2
        koji_tags: [release-candidate-2]
""")


@pytest.mark.parametrize("inherit", (False, True))
@mock_brew
@mock_pyxis
@mock_redis
def test_pyxis_updater_koji(tmp_path, inherit):
    cfg = deepcopy(KOJI_CONFIG)
    if inherit:
        cfg['indexes']['brew-rc']['koji_tags'] = ['release-candidate-3+']

    config = get_config(tmp_path, cfg)

    updater = PyxisUpdater(config)

    registry_data = run_update(updater)
    data = registry_data['brew']

    assert len(data.repositories) == 1
    aisleriot_repository = data.repositories['rh-osbs/aisleriot']
    assert aisleriot_repository.name == 'rh-osbs/aisleriot'
    assert len(aisleriot_repository.images) == 1
    aisleriot_image = next(iter(aisleriot_repository.images.values()))
    assert aisleriot_image.digest == \
        'sha256:fade1e55c4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb'
    assert aisleriot_image.tags == [
        'el8', 'el8-8020020200121102609.2', 'release-candidate', 'release-candidate-2'
    ]
