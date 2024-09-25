import json
from typing import Dict
from unittest.mock import patch

import pytest
import requests
import yaml

from flatpak_indexer.config import PyxisRegistryConfig
from flatpak_indexer.datasource.pyxis import PyxisUpdater
from flatpak_indexer.models import RegistryModel
from flatpak_indexer.test.redis import mock_redis
from .pyxis import _REPO_IMAGES, mock_pyxis
from .registry import mock_registry, MockRegistry
from .utils import _KOJI_BUILDS, get_config, mock_brew, mock_odcs, setup_client_cert


def run_update(updater):
    registry_data: Dict[str, RegistryModel] = {}

    updater.start()
    try:
        updater.update(registry_data)
    finally:
        updater.stop()

    return registry_data


CONFIG = yaml.safe_load("""
redis_url: redis://localhost
koji_config: brew
odcs_uri: https://odcs.example.com/
registries:
    production:
        public_url: https://registry.example.com/
        datasource: pyxis
        pyxis_url: https://pyxis.example.com/graphql
        pyxis_registry: registry.example.com
    fedora:
        public_url: https://registry.fedoraproject.org
        datasource: fedora
indexes:
    amd64:
        architecture: amd64
        registry: production
        output: out/test/flatpak-amd64.json
        tag: latest
    all:
        registry: production
        output: out/test/flatpak.json
        tag: latest
    # Test of indexes that overlap with different tags
    rhel8:
        registry: production
        output: out/test/flatpak-rhel8.json
        tag: rhel8
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
@mock_odcs
@mock_pyxis
@mock_redis
def test_pyxis_updater(tmp_path, server_cert, client_cert):
    config = get_config(tmp_path, CONFIG)
    if server_cert:
        config.local_certs['pyxis.example.com'] = 'test.crt'
    if client_cert:
        registry_config = config.registries['production']
        assert isinstance(registry_config, PyxisRegistryConfig)
        registry_config.pyxis_client_cert, registry_config.pyxis_client_key = \
            setup_client_cert(tmp_path)

    updater = PyxisUpdater(config, page_size=1)

    registry_data = run_update(updater)
    data = registry_data['production']

    assert len(data.repositories) == 3
    aisleriot_repository = data.repositories['el8/aisleriot']
    assert len(aisleriot_repository.images) == 1
    aisleriot_image = next(iter(aisleriot_repository.images.values()))
    assert aisleriot_image.digest == \
        'sha256:bo1dfacec4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb'
    assert aisleriot_image.labels['org.flatpak.ref'] == \
        'app/org.gnome.Aisleriot/x86_64/stable'
    assert aisleriot_image.labels['org.freedesktop.appstream.icon-128'] == \
        "https://www.example.com/icons/aisleriot.png"


@mock_brew
@mock_odcs
@mock_pyxis(no_brew_builds=True)
@mock_redis
@mock_registry
def test_pyxis_updater_from_registry(tmp_path, registry_mock: MockRegistry):
    config = get_config(tmp_path, CONFIG)

    archive_for_digest = {}

    # Import the builds from our Koji data into the mock registry

    # Get the Koji info for each image ID
    for build in _KOJI_BUILDS:
        for archive in build["_ARCHIVES"]:
            if archive.get("btype") == "image":
                digest = next(v for v in archive["extra"]["docker"]["digests"].values())
                archive_for_digest[digest] = archive

    # Now using our mocked Pyxis data, figure out what repositories to import it in -
    # the repositories don't match the pull specs in Koji
    for container_image in _REPO_IMAGES:
        for repository in container_image.repositories:
            if repository.registry == "registry.example.com":
                archive = archive_for_digest[container_image.image_id]

                config_data = archive["extra"]["docker"]["config"]
                config_digest, config_size = registry_mock.add_blob(
                    repository.repository, json.dumps(config_data)
                )

                # Create a fake manifest with only the information we need. This
                # won't match our digests (which are just random data), so we
                # use the fake_digest option to registry_mock.add_manifest()
                manifest_data = {
                    "schemaVersion": 2,
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "config": {
                        "mediaType": "application/vnd.oci.image.config.v1+json",
                        "digest": config_digest,
                        "size": config_size
                    }
                }

                for tag in repository.tags:
                    digest = registry_mock.add_manifest(repository.repository, tag.name,
                                                        json.dumps(manifest_data),
                                                        fake_digest=container_image.image_id)

    updater = PyxisUpdater(config, page_size=1)

    registry_data = run_update(updater)
    data = registry_data['production']

    assert len(data.repositories) == 3
    aisleriot_repository = data.repositories['el8/aisleriot']
    assert len(aisleriot_repository.images) == 1
    aisleriot_image = next(iter(aisleriot_repository.images.values()))
    assert aisleriot_image.digest == \
        'sha256:bo1dfacec4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb'
    assert aisleriot_image.labels['org.flatpak.ref'] == \
        'app/org.gnome.Aisleriot/x86_64/stable'
    assert aisleriot_image.labels['org.freedesktop.appstream.icon-128'] == \
        "https://www.example.com/icons/aisleriot.png"


@mock_brew
@mock_odcs
@mock_pyxis
@mock_redis
@patch(
    "flatpak_indexer.datasource.pyxis.updater.REPOSITORY_QUERY",
    "Not a query"
)
def test_pyxis_updater_bad_query(tmp_path, caplog):
    config = get_config(tmp_path, CONFIG)

    updater = PyxisUpdater(config, page_size=1)

    with pytest.raises(requests.exceptions.HTTPError, match=r'400 Client Error'):
        run_update(updater)

    assert "Error querying pyxis: [{'message': \"Syntax Error:" in caplog.text


@mock_brew
@mock_odcs
@mock_pyxis(bad_digests=True)
@mock_redis
def test_pyxis_updater_bad_digests(tmp_path, caplog):
    config = get_config(tmp_path, CONFIG)

    updater = PyxisUpdater(config, page_size=1)

    run_update(updater)
    assert ("No image for aisleriot-container-el8-8020020200121102609.1 "
            "with digest sha256:deadbeef"
            in caplog.text)


@mock_brew
@mock_odcs
@mock_pyxis(newer_untagged_image=True)
@mock_redis
def test_pyxis_updater_newer_untagged_image(tmp_path, caplog):
    config = get_config(tmp_path, CONFIG)

    updater = PyxisUpdater(config, page_size=1)

    run_update(updater)
    assert ("production/el8/aisleriot: "
            "latest is not applied to the latest build, can't determine history"
            in caplog.text)


REPOSITORY_OVERRIDE_CONFIG = yaml.safe_load("""
redis_url: redis://localhost
koji_config: brew
registries:
    production:
        public_url: https://registry.example.com/
        repositories: ['testrepo']
        datasource: pyxis
        pyxis_url: https://pyxis.example.com/graphql
        pyxis_registry: registry.example.com
indexes:
    amd64:
        architecture: amd64
        registry: production
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
    amd64_data = registry_data['production']

    assert len(amd64_data.repositories) == 1
    testrepo_repository = amd64_data.repositories['testrepo']
    assert testrepo_repository.name == 'testrepo'
