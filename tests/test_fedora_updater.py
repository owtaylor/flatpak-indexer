import copy
import json
import os

import responses
import yaml

from flatpak_indexer.datasource.fedora import FedoraUpdater

from .bodhi import mock_bodhi
from .fedora_messaging import mock_fedora_messaging
from .koji import mock_koji
from .redis import mock_redis
from .utils import get_config


CONFIG = yaml.safe_load("""
work_dir: ${WORK_DIR}
pyxis_url: https://pyxis.example.com/v1
redis_url: redis://localhost
koji_config: brew
registries:
    registry.example.com:
        public_url: https://registry.example.com/
        datasource: fedora
    notregistry.example.com:
        public_url: https://notregistry.example.com/
        datasource: pyxis
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


@mock_fedora_messaging
@mock_koji
@mock_redis
@responses.activate
def test_fedora_updater(mock_connection, tmp_path):
    mock_connection.put_update_message('fedora-2018-12456789')
    mock_connection.put_inactivity_timeout()

    def modify_statuses(update):
        # This build is now obsoleted by a build not in our test date, mark it testing so that
        # we have a repository with different stable/testing
        if update['builds'][0]['nvr'] == 'feedreader-master-2920190201081220.1':
            update = copy.copy(update)
            update['status'] = 'testing'

        return update

    mock_bodhi(modify=modify_statuses)

    os.environ["WORK_DIR"] = str(tmp_path)

    config = get_config(tmp_path, CONFIG)

    updater = FedoraUpdater(config)

    updater.start()
    try:
        updater.update()
    finally:
        updater.stop()

    with open(tmp_path / "registry.example.com.json") as f:
        data = json.load(f)

    assert len(data['Repositories']) == 5

    eog_repository = [r for r in data['Repositories'] if r['Name'] == 'eog'][0]
    assert len(eog_repository['Images']) == 1
    assert eog_repository['Images'][0]['Tags'] == ['latest', 'testing']

    feedreader_repository = [r for r in data['Repositories'] if r['Name'] == 'feedreader'][0]
    assert len(feedreader_repository['Images']) == 2
    assert feedreader_repository['Images'][0]['Tags'] == ['testing']
    assert feedreader_repository['Images'][1]['Tags'] == ['latest']

    gnome_clocks_repository = [r for r in data['Repositories'] if r['Name'] == 'gnome-clocks'][0]
    assert len(gnome_clocks_repository['Images']) == 2
    assert gnome_clocks_repository['Images'][0]['Tags'] == ['latest', 'testing']

    gnome_weather_repository = [r for r in data['Repositories'] if r['Name'] == 'gnome-weather'][0]
    assert len(gnome_weather_repository['Images']) == 2
    assert gnome_weather_repository['Images'][1]['Tags'] == ['latest', 'testing']
