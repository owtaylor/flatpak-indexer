import copy
from datetime import datetime
import logging
from textwrap import dedent

import pytest

from flatpak_indexer.bodhi_query import (
    list_updates, query_releases,
    refresh_all_updates, refresh_update_status, refresh_updates, reset_update_cache
)
from flatpak_indexer.http_utils import HttpConfig
from flatpak_indexer.koji_utils import KojiConfig
from flatpak_indexer.models import BodhiUpdateModel
from flatpak_indexer.redis_utils import RedisConfig
from flatpak_indexer.release_info import ReleaseStatus
from flatpak_indexer.session import Session
from flatpak_indexer.test.bodhi import mock_bodhi
from flatpak_indexer.test.koji import mock_koji
from flatpak_indexer.test.redis import mock_redis


class TestConfig(HttpConfig, KojiConfig, RedisConfig):
    pass


@pytest.fixture
def session():
    config = TestConfig.from_str(dedent("""
        koji_config: fedora
        redis_url: redis://localhost
"""))
    return Session(config)


@mock_koji
@mock_redis
@mock_bodhi
def test_bodhi_query_package_updates(session):
    refresh_updates(session, 'rpm', entities=['bubblewrap'])

    updates = list_updates(session, 'rpm', 'bubblewrap')
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


@mock_koji
@mock_redis
@mock_bodhi
def test_bodhi_query_package_updates_many(session):
    # aisleriot picks up multi-package updates
    # eog picks up Flatpak updates
    #   (since we don't specify content_type=rpm to avoid pessimizing a bodhi query)
    entities = [str(n) + 'bubblewrap' for n in range(0, 9)] + ['aisleriot', 'bubblewrap', 'eog']

    refresh_updates(session, 'rpm', entities, rows_per_page=1)

    updates = list_updates(session, 'rpm', 'bubblewrap')
    assert len(updates) == 3

    updates = list_updates(session, 'rpm', 'aisleriot')
    assert len(updates) == 3


@mock_koji
@mock_redis
@mock_bodhi
def test_bodhi_query_update_changed(bodhi_mock, session):
    def modify_update(update):
        if update['updateid'] == 'FEDORA-2018-1a0cf961a1':
            update_copy = copy.deepcopy(update)

            update_copy['builds'] = [b for b in update_copy['builds']
                                     if not b['nvr'].startswith('bijiben-')]
            update_copy['date_modified'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

            return update_copy
        else:
            return update

    refresh_updates(session, 'rpm', ['aisleriot', 'bijiben'])

    updates = list_updates(session, 'rpm', 'aisleriot')
    assert len(updates) == 3
    updates = list_updates(session, 'rpm', 'bijiben')
    assert len(updates) == 5

    bodhi_mock.modify = modify_update
    refresh_updates(session, 'rpm', ['aisleriot', 'bijiben'])

    updates = list_updates(session, 'rpm', 'aisleriot')
    assert len(updates) == 3
    updates = list_updates(session, 'rpm', 'bijiben')
    assert len(updates) == 4


@mock_koji
@mock_redis
@mock_bodhi
def test_list_updates_by_release(session):
    refresh_updates(session, 'rpm', entities=['bubblewrap'])

    updates = list_updates(session, 'rpm', 'bubblewrap', release_branch='f29')
    assert len(updates) == 1

    assert updates[0].builds == ['bubblewrap-0.3.1-1.fc29']

    updates = list_updates(session, 'rpm', release_branch='f29')
    assert len(updates) == 1

    assert updates[0].builds == ['bubblewrap-0.3.1-1.fc29']


@mock_koji
@mock_redis
@mock_bodhi
def test_bodhi_query_flatpak_updates(session):
    refresh_all_updates(session, 'flatpak')

    updates = list_updates(session, 'flatpak', 'feedreader')
    assert len(updates) == 3

    update = [x for x in updates if 'feedreader-master-2920190201225359.1' in x.builds][0]

    assert isinstance(update, BodhiUpdateModel)

    assert update.user_name == 'pwalter'
    assert update.date_submitted.strftime("%Y-%m-%d %H:%M:%S") == '2019-02-03 21:08:49'
    assert update.builds == ['feedreader-master-2920190201225359.1']
    assert update.status == 'obsolete'
    assert update.type == 'bugfix'

    refresh_all_updates(session, 'flatpak')


@mock_koji
@mock_redis
@mock_bodhi
@pytest.mark.parametrize('flags', [
    [],
    ['bad_total'],
    ['ghost_updates'],
])
def test_bodhi_query_flatpak_updates_all(session, bodhi_mock, flags):
    bodhi_mock.flags = flags

    refresh_all_updates(session, 'flatpak')

    updates = list_updates(session, 'flatpak')
    assert len(updates) == 10

    build_map = {u.update_id: [b.name for b in u.builds] for u in updates}

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


@mock_koji
@mock_redis
@mock_bodhi
def test_bodhi_refresh_update_status(session):
    update_id = 'FEDORA-FLATPAK-2018-aecd5ddc46'

    refresh_all_updates(session, 'flatpak')
    update_raw = session.redis_client.get('update:' + update_id)
    update = BodhiUpdateModel.from_json_text(update_raw)
    update.status = 'pending'
    session.redis_client.set('update:' + update_id, update.to_json_text())

    refresh_update_status(session, update_id)

    update_raw = session.redis_client.get('update:' + update_id)
    update = BodhiUpdateModel.from_json_text(update_raw)
    assert update.status == 'stable'

    # This should do nothing
    refresh_update_status(session, 'NO_SUCH_UPDATE')


@mock_koji
@mock_redis
@mock_bodhi
def test_bodhi_update_cache_global(session, caplog):
    caplog.set_level(logging.INFO)

    refresh_all_updates(session, 'flatpak')
    assert "submitted_since" not in caplog.text
    caplog.clear()

    refresh_all_updates(session, 'flatpak')
    assert "submitted_since" in caplog.text
    caplog.clear()

    reset_update_cache(session)

    refresh_all_updates(session, 'flatpak')
    assert "submitted_since" not in caplog.text
    caplog.clear()


@mock_koji
@mock_redis
@mock_bodhi
def test_bodhi_update_cache_per_package(session, caplog):
    caplog.set_level(logging.INFO)

    refresh_updates(session, 'flatpak', entities=['feedreader'])
    assert "submitted_since" not in caplog.text
    caplog.clear()

    refresh_updates(session, 'flatpak', entities=['feedreader'])
    assert "submitted_since" in caplog.text
    caplog.clear()

    reset_update_cache(session)

    refresh_updates(session, 'flatpak', entities=['feedreader'])
    assert "submitted_since" not in caplog.text
    caplog.clear()


@mock_bodhi
def test_query_releases(session):
    releases = query_releases(session)

    release = next(r for r in releases if r.name == "F29")
    assert release.branch == "f29"
    assert release.tag == "f29"
    assert release.status == ReleaseStatus.GA


@mock_bodhi
def test_query_releases_corner_cases(session, bodhi_mock, caplog):
    def modify_releases(release_json):
        # Branched
        release_json.append({
            "name": "F90",
            "branch": "f90",
            "dist_tag": "f90",
            "state": "pending",
        })
        # Branched and frozen
        release_json.append({
            "name": "F91",
            "branch": "f91",
            "dist_tag": "f91",
            "state": "frozen",
        })
        # Skip because not F\d+$
        release_json.append({
            "name": "F91F",
            "branch": "f91",
            "dist_tag": "f90-flatpak",
            "state": "current",
        })
        # Skip because disabled
        release_json.append({
            "name": "F92",
            "branch": "f92",
            "dist_tag": "f92",
            "state": "disabled",
        })
        # Skip because unknown state
        release_json.append({
            "name": "F99",
            "branch": "f99",
            "dist_tag": "f99",
            "state": "weird",
        })

    bodhi_mock.modify_releases = modify_releases
    releases = query_releases(session)

    release = next(r for r in releases if r.name == "F90")
    assert release.branch == "f90"
    assert release.tag == "f90"
    assert release.status == ReleaseStatus.BRANCHED

    release = next(r for r in releases if r.name == "F91")
    assert release.branch == "f91"
    assert release.tag == "f91"
    assert release.status == ReleaseStatus.BRANCHED

    assert not any(r.name == "F91F" for r in releases)
    assert not any(r.name == "F92" for r in releases)

    assert "Unknown state for release F99: weird" in caplog.text
