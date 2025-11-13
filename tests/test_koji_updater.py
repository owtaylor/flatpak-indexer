from copy import deepcopy
from typing import Dict

import pytest
import yaml

from flatpak_indexer.datasource.koji import KojiUpdater
from flatpak_indexer.models import RegistryModel
from flatpak_indexer.test.redis import mock_redis

from .pyxis import mock_pyxis
from .utils import get_config, mock_brew, mock_odcs


def run_update(updater):
    registry_data: Dict[str, RegistryModel] = {}

    updater.start()
    try:
        updater.update(registry_data)
    finally:
        updater.stop()

    return registry_data


KOJI_CONFIG = yaml.safe_load("""
redis_url: redis://localhost
koji_config: brew
odcs_uri: https://odcs.example.com/
registries:
    brew:
        public_url: https://internal.example.com/
        datasource: koji
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
    brew-rc-4-ppc64le:
        registry: brew
        architecture: ppc64le
        output: out/test/brew-4.json
        tag: release-candidate-4
        koji_tags: [release-candidate-4]
""")


@pytest.mark.parametrize("inherit", (False, True))
@mock_brew
@mock_odcs
@mock_pyxis
@mock_redis
def test_pyxis_updater_koji(tmp_path, inherit):
    cfg = deepcopy(KOJI_CONFIG)
    if inherit:
        cfg["indexes"]["brew-rc"]["koji_tags"] = ["release-candidate-3+"]

    config = get_config(tmp_path, cfg)

    updater = KojiUpdater(config)

    registry_data = run_update(updater)
    data = registry_data["brew"]

    assert len(data.repositories) == 1
    aisleriot_repository = data.repositories["rh-osbs/aisleriot"]
    assert aisleriot_repository.name == "rh-osbs/aisleriot"
    assert len(aisleriot_repository.images) == 1
    aisleriot_image = next(iter(aisleriot_repository.images.values()))
    assert (
        aisleriot_image.digest
        == "sha256:fade1e55c4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb"
    )
    assert aisleriot_image.tags == [
        "el8",
        "el8-8020020200121102609.2",
        "release-candidate",
        "release-candidate-2",
    ]
