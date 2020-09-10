import copy

import responses
import yaml

from flatpak_indexer.datasource.fedora import FedoraUpdater

from .bodhi import mock_bodhi
from .fedora_messaging import mock_fedora_messaging
from .koji import mock_koji
from .redis import mock_redis
from .utils import get_config


CONFIG = yaml.safe_load("""
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

    config = get_config(tmp_path, CONFIG)

    updater = FedoraUpdater(config)

    registry_data = {}

    updater.start()
    try:
        updater.update(registry_data)
    finally:
        updater.stop()

    data = registry_data['registry.example.com']

    assert len(data.repositories) == 6

    eog_repository = data.repositories['eog']
    assert len(eog_repository.images) == 1
    assert eog_repository.\
        images['sha256:6b440190b0454c95e64e185ceb5778e150909df495bd4fee88ef98fe199c814e'].tags \
        == ['latest', 'testing']

    feedreader_repository = data.repositories['feedreader']
    assert len(feedreader_repository.images) == 2
    assert feedreader_repository.\
        images['sha256:4f728b11e92366ea1643e0516382ffa327e6fe944d11b9b297c312de8859dcc1'].tags \
        == ['testing']
    assert feedreader_repository.\
        images['sha256:d3518175c2c78c27a705b642b05e59d91416227f1dcae2d12347a51753d11152'].tags \
        == ['latest']

    gnome_clocks_repository = data.repositories['gnome-clocks']
    assert len(gnome_clocks_repository.images) == 2
    assert gnome_clocks_repository.\
        images['sha256:80bd2c514d1f8930f94c21b80ff70c796032961dd7a9e5dfda4b03f5e95c0cbc'].tags \
        == ['latest', 'testing']

    gnome_weather_repository = data.repositories['gnome-weather']
    print(list(gnome_weather_repository.images.keys()))
    assert len(gnome_weather_repository.images) == 2
    assert gnome_weather_repository.\
        images['sha256:eabc978690e7ed1e2a27f713658efc6bcf4ff5d4080d143bcc018e4014718922'].tags \
        == ['latest', 'testing']
