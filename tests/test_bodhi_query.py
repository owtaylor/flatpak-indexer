import copy
from datetime import datetime
import logging

import pytest

from flatpak_indexer.bodhi_query import (
    list_updates, refresh_all_updates, refresh_update_status, refresh_updates, reset_update_cache
)
from flatpak_indexer.models import BodhiUpdateModel
from .bodhi import mock_bodhi
from .koji import make_koji_session
from .redis import make_redis_client


@mock_bodhi
def test_bodhi_query_package_updates():
    redis_client = make_redis_client()
    koji_session = make_koji_session()

    refresh_updates(koji_session, redis_client, 'rpm', entities=['bubblewrap'])

    updates = list_updates(redis_client, 'rpm', 'bubblewrap')
    assert len(updates) == 3

    update = [x for x in updates if 'bubblewrap-0.3.0-2.fc28' in x.builds][0]

    assert update.user_name == 'walters'
    assert update.date_submitted.strftime("%Y-%m-%d %H:%M:%S") == '2018-07-26 18:59:31'
    assert update.date_testing is not None
    assert update.date_testing.strftime("%Y-%m-%d %H:%M:%S") == '2018-07-27 18:14:33'
    assert update.date_stable is not None
    assert update.date_stable.strftime("%Y-%m-%d %H:%M:%S") == '2018-08-03 20:44:52'

    assert update.builds == ['bubblewrap-0.3.0-2.fc28']
    assert update.status == 'stable'
    assert update.type == 'enhancement'


@mock_bodhi
def test_bodhi_query_package_updates_many():
    redis_client = make_redis_client()
    koji_session = make_koji_session()

    # aisleriot picks up multi-package updates
    # eog picks up Flatpak updates
    #   (since we don't specify content_type=rpm to avoid pessimizing a bodhi query)
    entities = [str(n) + 'bubblewrap' for n in range(0, 9)] + ['aisleriot', 'bubblewrap', 'eog']

    refresh_updates(koji_session, redis_client, 'rpm', entities, rows_per_page=1)

    updates = list_updates(redis_client, 'rpm', 'bubblewrap')
    assert len(updates) == 3

    updates = list_updates(redis_client, 'rpm', 'aisleriot')
    assert len(updates) == 3


@mock_bodhi
def test_bodhi_query_update_changed(bodhi_mock):
    def modify_update(update):
        if update['updateid'] == 'FEDORA-2018-1a0cf961a1':
            update_copy = copy.deepcopy(update)

            update_copy['builds'] = [b for b in update_copy['builds']
                                     if not b['nvr'].startswith('bijiben-')]
            update_copy['date_modified'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

            return update_copy
        else:
            return update

    redis_client = make_redis_client()
    koji_session = make_koji_session()

    refresh_updates(koji_session, redis_client, 'rpm', ['aisleriot', 'bijiben'])

    updates = list_updates(redis_client, 'rpm', 'aisleriot')
    assert len(updates) == 3
    updates = list_updates(redis_client, 'rpm', 'bijiben')
    assert len(updates) == 5

    bodhi_mock.modify = modify_update
    refresh_updates(koji_session, redis_client, 'rpm', ['aisleriot', 'bijiben'])

    updates = list_updates(redis_client, 'rpm', 'aisleriot')
    assert len(updates) == 3
    updates = list_updates(redis_client, 'rpm', 'bijiben')
    assert len(updates) == 4


@mock_bodhi
def test_list_updates_by_release():
    redis_client = make_redis_client()
    koji_session = make_koji_session()

    refresh_updates(koji_session, redis_client, 'rpm', entities=['bubblewrap'])

    updates = list_updates(redis_client, 'rpm', 'bubblewrap', release_branch='f29')
    assert len(updates) == 1

    assert updates[0].builds == ['bubblewrap-0.3.1-1.fc29']

    updates = list_updates(redis_client, 'rpm', release_branch='f29')
    assert len(updates) == 1

    assert updates[0].builds == ['bubblewrap-0.3.1-1.fc29']


@mock_bodhi
def test_bodhi_query_flatpak_updates():
    redis_client = make_redis_client()
    koji_session = make_koji_session()

    refresh_all_updates(koji_session, redis_client, 'flatpak')

    updates = list_updates(redis_client, 'flatpak', 'feedreader')
    assert len(updates) == 3

    update = [x for x in updates if 'feedreader-master-2920190201225359.1' in x.builds][0]

    assert isinstance(update, BodhiUpdateModel)

    assert update.user_name == 'pwalter'
    assert update.date_submitted.strftime("%Y-%m-%d %H:%M:%S") == '2019-02-03 21:08:49'
    assert update.builds == ['feedreader-master-2920190201225359.1']
    assert update.status == 'obsolete'
    assert update.type == 'bugfix'

    refresh_all_updates(koji_session, redis_client, 'flatpak')


@mock_bodhi
@pytest.mark.parametrize('flags', [
    [],
    ['bad_total'],
    ['ghost_updates'],
])
def test_bodhi_query_flatpak_updates_all(bodhi_mock, flags):
    bodhi_mock.flags = flags

    redis_client = make_redis_client()
    koji_session = make_koji_session()

    refresh_all_updates(koji_session, redis_client, 'flatpak')

    updates = list_updates(redis_client, 'flatpak')
    assert len(updates) == 10

    build_map = {u.update_id: [b.rsplit('-', 2)[0] for b in u.builds] for u in updates}

    assert build_map == {
        'FEDORA-FLATPAK-2018-2f1988821e': ['eog'],
        'FEDORA-FLATPAK-2018-aecd5ddc46': ['feedreader'],
        'FEDORA-FLATPAK-2018-b653073d2f': ['quadrapassel'],
        'FEDORA-FLATPAK-2019-1c04884fc8': ['gnome-clocks', 'gnome-weather'],
        'FEDORA-FLATPAK-2019-a922b417ed': ['feedreader'],
        'FEDORA-FLATPAK-2019-adc833ad33': ['gnome-weather'],
        'FEDORA-FLATPAK-2019-d84b882193': ['feedreader'],
        'FEDORA-FLATPAK-2019-f531f062df': ['gnome-clocks'],
        'FEDORA-FLATPAK-2020-c3101996a6': ['baobab'],
        'FEDORA-FLATPAK-2020-dfd7272b06': ['baobab'],
    }


@mock_bodhi
def test_bodhi_refresh_update_status():
    redis_client = make_redis_client()
    koji_session = make_koji_session()

    update_id = 'FEDORA-FLATPAK-2018-aecd5ddc46'

    refresh_all_updates(koji_session, redis_client, 'flatpak')
    update_raw = redis_client.get('update:' + update_id)
    update = BodhiUpdateModel.from_json_text(update_raw)
    update.status = 'pending'
    redis_client.set('update:' + update_id, update.to_json_text())

    refresh_update_status(koji_session, redis_client, update_id)

    update_raw = redis_client.get('update:' + update_id)
    update = BodhiUpdateModel.from_json_text(update_raw)
    assert update.status == 'stable'

    # This should do nothing
    refresh_update_status(koji_session, redis_client, 'NO_SUCH_UPDATE')


@mock_bodhi
def test_bodhi_update_cache_global(caplog):
    caplog.set_level(logging.INFO)

    redis_client = make_redis_client()
    koji_session = make_koji_session()

    refresh_all_updates(koji_session, redis_client, 'flatpak')
    assert "submitted_since" not in caplog.text
    caplog.clear()

    refresh_all_updates(koji_session, redis_client, 'flatpak')
    assert "submitted_since" in caplog.text
    caplog.clear()

    reset_update_cache(redis_client)

    refresh_all_updates(koji_session, redis_client, 'flatpak')
    assert "submitted_since" not in caplog.text
    caplog.clear()


@mock_bodhi
def test_bodhi_update_cache_per_package(caplog):
    caplog.set_level(logging.INFO)

    redis_client = make_redis_client()
    koji_session = make_koji_session()

    refresh_updates(koji_session, redis_client, 'flatpak', entities=['feedreader'])
    assert "submitted_since" not in caplog.text
    caplog.clear()

    refresh_updates(koji_session, redis_client, 'flatpak', entities=['feedreader'])
    assert "submitted_since" in caplog.text
    caplog.clear()

    reset_update_cache(redis_client)

    refresh_updates(koji_session, redis_client, 'flatpak', entities=['feedreader'])
    assert "submitted_since" not in caplog.text
    caplog.clear()
