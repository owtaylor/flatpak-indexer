from typing import Dict
import copy

import pytest
import yaml

from flatpak_indexer.datasource.fedora import FedoraUpdater
from flatpak_indexer.models import RegistryModel
from flatpak_indexer.test.bodhi import mock_bodhi
from flatpak_indexer.test.koji import mock_koji
from flatpak_indexer.test.redis import mock_redis
import redis

from .fedora_messaging import mock_fedora_messaging
from .utils import get_config

CONFIG = yaml.safe_load("""
redis_url: redis://localhost
koji_config: brew
registries:
    registry.example.com:
        public_url: https://registry.example.com/
        datasource: fedora
    notregistry.example.com:
        public_url: https://notregistry.example.com/
        datasource: pyxis
        pyxis_url: https://pyxis.example.com/v1
        pyxis_registry: notregistry.example.com
indexes:
    testing:
        registry: registry.example.com
        output: out/test/flatpak-testing.json
        bodhi_status: testing
        tag: testing
    stable:
        registry: registry.example.com
        output: out/test/flatpak.json
        bodhi_status: stable
        tag: latest
    # Not a Bodhi-backed index
    stray:
        registry: notregistry.example.com
        output: out/bah/flatpak.json
        tag: latest
""")


def modify_statuses(update):
    # This build is now obsoleted by a build not in our test date, mark it testing so that
    # we have a repository with different stable/testing
    if update["builds"][0]["nvr"] == "feedreader-master-2920190201081220.1":
        update = copy.copy(update)
        update["status"] = "testing"

    return update


@mock_bodhi(modify=modify_statuses)
@mock_fedora_messaging
@mock_koji
@mock_redis
def test_fedora_updater(connection_mock, bodhi_mock, tmp_path):
    def modify_update(update):
        if update["updateid"] == "FEDORA-FLATPAK-2021-927f4d44b8":
            update_copy = copy.deepcopy(update)
            update_copy["status"] = "testing"
            update_copy["date_stable"] = None

            return update_copy
        else:
            return update

    bodhi_mock.modify = modify_update

    connection_mock.put_update_message("fedora-2018-12456789")
    connection_mock.put_inactivity_timeout()

    config = get_config(tmp_path, CONFIG)

    updater = FedoraUpdater(config)

    registry_data: Dict[str, RegistryModel] = {}

    updater.start()
    try:
        updater.update(registry_data)
    finally:
        updater.stop()

    data = registry_data["registry.example.com"]

    assert len(data.repositories) == 4

    eog_repository = data.repositories["eog"]
    assert len(eog_repository.images) == 12
    assert eog_repository.images[
        "sha256:94263405624c5709f0efeeb4b4e640ae866f08795fa0d7f3d10a616b4fd3a6a1"
    ].tags == ["latest", "testing"]

    feedreader_repository = data.repositories["feedreader"]
    assert len(feedreader_repository.images) == 6
    assert feedreader_repository.images[
        "sha256:5c4cc0501671de5a46a5f69c56a33b11f6398a0bdaf4a12f92b5680d0f496e10"
    ].tags == ["testing"]
    assert feedreader_repository.images[
        "sha256:658508916a66bae008cff1a49ac1befed64e019f738241fd0bf30f963acafb49"
    ].tags == ["latest"]


def modify_no_stable_no_testing(update):
    update = copy.copy(update)
    update["date_testing"] = None
    update["date_stable"] = None
    return update


@mock_bodhi(modify=modify_no_stable_no_testing)
@mock_fedora_messaging
@mock_koji
@mock_redis
def test_fedora_updater_no_stable_no_testing(connection_mock, tmp_path):
    connection_mock.put_update_message("fedora-2018-12456789")
    connection_mock.put_inactivity_timeout()

    config = get_config(tmp_path, CONFIG)

    updater = FedoraUpdater(config)

    registry_data: Dict[str, RegistryModel] = {}

    updater.start()
    try:
        updater.update(registry_data)
    finally:
        updater.stop()

    data = registry_data["registry.example.com"]

    assert len(data.repositories) == 0


@mock_bodhi
@mock_fedora_messaging
@mock_koji
@mock_redis
@pytest.mark.parametrize("passive_behavior", ["exist", "not_exist"])
def test_fedora_updater_bodhi_changes(connection_mock, tmp_path, passive_behavior):
    """Test the code interface with FedoraMonitor"""

    connection_mock.passive_behavior = passive_behavior

    connection_mock.put_update_message("fedora-2018-12456789")
    connection_mock.put_inactivity_timeout()

    config = get_config(tmp_path, CONFIG)

    redis_client = redis.Redis.from_url(config.redis_url)
    redis_client.set("fedora-messaging-queue", "MYQUEUE")

    updater = FedoraUpdater(config)

    registry_data: Dict[str, RegistryModel] = {}

    updater.start()
    try:
        updater.update(registry_data)
    finally:
        updater.stop()

    data = registry_data["registry.example.com"]

    assert len(data.repositories) == 4
