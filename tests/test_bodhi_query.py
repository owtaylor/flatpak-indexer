import copy
from datetime import datetime
import logging

import responses

from flatpak_indexer.datasource.fedora.bodhi_query import (list_updates, refresh_all_updates,
                                                           refresh_update_status, refresh_updates,
                                                           reset_update_cache)
from flatpak_indexer.datasource.fedora.models import BodhiUpdateModel
from .bodhi import mock_bodhi
from .koji import make_koji_session
from .redis import make_redis_client


@responses.activate
def test_bodhi_query_package_updates():
    redis_client = make_redis_client()
    koji_session = make_koji_session()
    mock_bodhi()

    refresh_updates(koji_session, redis_client, 'rpm', entities=['bubblewrap'])

    updates = list_updates(redis_client, 'rpm', 'bubblewrap')
    assert len(updates) == 3

    update = [x for x in updates if 'bubblewrap-0.3.0-2.fc28' in x.builds][0]

    assert update.user_name == 'walters'
    assert update.date_submitted.strftime("%Y-%m-%d %H:%M:%S") == '2018-07-26 18:59:31'
    assert update.date_testing.strftime("%Y-%m-%d %H:%M:%S") == '2018-07-27 18:14:33'
    assert update.date_stable.strftime("%Y-%m-%d %H:%M:%S") == '2018-08-03 20:44:52'

    assert update.builds == ['bubblewrap-0.3.0-2.fc28']
    assert update.status == 'stable'
    assert update.type == 'enhancement'


@responses.activate
def test_bodhi_query_package_updates_many():
    redis_client = make_redis_client()
    koji_session = make_koji_session()
    mock_bodhi()

    # aisleriot picks up multi-package updates
    # eog picks up Flatpak updates
    #   (since we don't specify content_type=rpm to avoid pessimizing a bodhi query)
    entities = [str(n) + 'bubblewrap' for n in range(0, 9)] + ['aisleriot', 'bubblewrap', 'eog']

    refresh_updates(koji_session, redis_client, 'rpm', entities, rows_per_page=1)

    updates = list_updates(redis_client, 'rpm', 'bubblewrap')
    assert len(updates) == 3

    updates = list_updates(redis_client, 'rpm', 'aisleriot')
    assert len(updates) == 3


@responses.activate
def test_bodhi_query_update_changed():
    modify = False

    def modify_update(update):
        if modify and update['updateid'] == 'FEDORA-2018-1a0cf961a1':
            update_copy = copy.deepcopy(update)

            update_copy['builds'] = [b for b in update_copy['builds']
                                     if not b['nvr'].startswith('bijiben-')]
            update_copy['date_modified'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

            return update_copy
        else:
            return update

    redis_client = make_redis_client()
    koji_session = make_koji_session()
    mock_bodhi(modify=modify_update)

    refresh_updates(koji_session, redis_client, 'rpm', ['aisleriot', 'bijiben'])

    updates = list_updates(redis_client, 'rpm', 'aisleriot')
    assert len(updates) == 3
    updates = list_updates(redis_client, 'rpm', 'bijiben')
    assert len(updates) == 5

    modify = True
    refresh_updates(koji_session, redis_client, 'rpm', ['aisleriot', 'bijiben'])

    updates = list_updates(redis_client, 'rpm', 'aisleriot')
    assert len(updates) == 3
    updates = list_updates(redis_client, 'rpm', 'bijiben')
    assert len(updates) == 4


@responses.activate
def test_list_updates_by_release():
    redis_client = make_redis_client()
    koji_session = make_koji_session()
    mock_bodhi()

    refresh_updates(koji_session, redis_client, 'rpm', entities=['bubblewrap'])

    updates = list_updates(redis_client, 'rpm', 'bubblewrap', release_branch='f29')
    assert len(updates) == 1

    assert updates[0].builds == ['bubblewrap-0.3.1-1.fc29']

    updates = list_updates(redis_client, 'rpm', release_branch='f29')
    assert len(updates) == 1

    assert updates[0].builds == ['bubblewrap-0.3.1-1.fc29']


@responses.activate
def test_bodhi_query_flatpak_updates():
    redis_client = make_redis_client()
    koji_session = make_koji_session()
    mock_bodhi()

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


@responses.activate
def test_bodhi_refresh_update_status():
    redis_client = make_redis_client()
    koji_session = make_koji_session()
    mock_bodhi()

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


@responses.activate
def test_bodhi_update_cache_global(caplog):
    caplog.set_level(logging.INFO)

    redis_client = make_redis_client()
    koji_session = make_koji_session()
    mock_bodhi()

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


@responses.activate
def test_bodhi_update_cache_per_package(caplog):
    caplog.set_level(logging.INFO)

    redis_client = make_redis_client()
    koji_session = make_koji_session()
    mock_bodhi()

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