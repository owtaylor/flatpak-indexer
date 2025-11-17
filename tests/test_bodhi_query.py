from datetime import datetime, timezone
from textwrap import dedent
import copy
import logging

import pytest

from flatpak_indexer.bodhi_query import (
    list_updates,
    query_releases,
    refresh_all_updates,
    refresh_update_status,
    refresh_updates,
    reset_update_cache,
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
    config = TestConfig.from_str(
        dedent("""
        koji_config: fedora
        redis_url: redis://localhost
""")
    )
    return Session(config)


@mock_koji
@mock_redis
@mock_bodhi
def test_bodhi_query_package_updates(session):
    refresh_updates(session, "rpm", entities=["libpeas"])

    updates = list_updates(session, "rpm", "libpeas")
    assert len(updates) == 4

    update = [x for x in updates if "libpeas-1.30.0-4.fc35" in x.builds][0]

    assert update.user_name == "hadess"
    assert update.date_submitted.strftime("%Y-%m-%d %H:%M:%S") == "2021-06-14 08:37:11"
    assert update.date_testing is not None
    assert update.date_testing.strftime("%Y-%m-%d %H:%M:%S") == "2021-06-14 08:37:32"
    assert update.date_stable is not None
    assert update.date_stable.strftime("%Y-%m-%d %H:%M:%S") == "2021-06-14 08:38:35"

    assert update.builds == ["libpeas-1.30.0-4.fc35"]
    assert update.status == "stable"
    assert update.type == "unspecified"


@mock_koji
@mock_redis
@mock_bodhi
def test_bodhi_query_package_updates_many(session):
    # gnome-weather picks up multi-package updates
    # eog picks up Flatpak updates
    #   (since we don't specify content_type=rpm to avoid pessimizing a bodhi query)
    entities = [str(n) + "libpeas" for n in range(0, 9)] + ["pango", "libpeas", "eog"]

    refresh_updates(session, "rpm", entities, rows_per_page=1)

    updates = list_updates(session, "rpm", "libpeas")
    assert len(updates) == 4

    updates = list_updates(session, "rpm", "gnome-weather")
    assert len(updates) == 4


@mock_koji
@mock_redis
@mock_bodhi
def test_bodhi_query_update_changed(bodhi_mock, session):
    def modify_update(update):
        if update["updateid"] == "FEDORA-2021-511edcde29":
            update_copy = copy.deepcopy(update)

            update_copy["builds"] = [
                b for b in update_copy["builds"] if not b["nvr"].startswith("sushi-")
            ]
            update_copy["date_modified"] = datetime.now(tz=timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            return update_copy
        else:
            return update

    refresh_updates(session, "rpm", ["gnome-weather", "bijiben"])

    updates = list_updates(session, "rpm", "gnome-weather")
    assert len(updates) == 4
    updates = list_updates(session, "rpm", "sushi")
    assert len(updates) == 1

    bodhi_mock.modify = modify_update
    refresh_updates(session, "rpm", ["gnome-weather", "sushi"])

    updates = list_updates(session, "rpm", "gnome-weather")
    assert len(updates) == 4
    updates = list_updates(session, "rpm", "sushi")
    assert len(updates) == 0


@mock_koji
@mock_redis
@mock_bodhi
def test_list_updates_by_release(session):
    refresh_updates(session, "rpm", entities=["eog"])

    updates = list_updates(session, "rpm", "eog", release_branch="f36")
    assert len(updates) == 13

    assert updates[0].builds == ["eog-41~beta-1.fc36"]

    updates = list_updates(session, "rpm", release_branch="f36")
    assert len(updates) == 13

    assert updates[0].builds == ["eog-41~beta-1.fc36"]


@mock_koji
@mock_redis
@mock_bodhi
def test_bodhi_query_flatpak_updates(session):
    refresh_all_updates(session, "flatpak")

    updates = list_updates(session, "flatpak", "feedreader")
    assert len(updates) == 3

    update = [x for x in updates if "feedreader-stable-3520211013075828.3" in x.builds][0]

    assert isinstance(update, BodhiUpdateModel)

    assert update.user_name == "pwalter"
    assert update.date_submitted.strftime("%Y-%m-%d %H:%M:%S") == "2021-11-07 22:59:34"
    assert update.builds == ["feedreader-stable-3520211013075828.3"]
    assert update.status == "stable"
    assert update.type == "unspecified"

    refresh_all_updates(session, "flatpak")


@mock_koji
@mock_redis
@mock_bodhi
@pytest.mark.parametrize(
    "flags",
    [
        [],
        ["bad_total"],
        ["duplicated_updates"],
        ["ghost_updates"],
    ],
)
def test_bodhi_query_flatpak_updates_all(session, bodhi_mock, flags):
    bodhi_mock.flags = flags

    refresh_all_updates(session, "flatpak")

    updates = list_updates(session, "flatpak")
    assert len(updates) == 11

    build_map = {u.update_id: [b.name for b in u.builds] for u in updates}

    assert build_map == {
        "FEDORA-FLATPAK-2021-1bda39b4d9": ["feedreader"],
        "FEDORA-FLATPAK-2021-2db97225fd": ["eog"],
        "FEDORA-FLATPAK-2021-47ba6c9e6e": ["feedreader"],
        "FEDORA-FLATPAK-2021-4df18ed3c8": ["baobab"],
        "FEDORA-FLATPAK-2021-56cf8de552": ["eog"],
        "FEDORA-FLATPAK-2021-927f4d44b8": ["feedreader"],
        "FEDORA-FLATPAK-2021-be7df1d070": ["eog"],
        "FEDORA-FLATPAK-2021-cb6b3c0cad": ["quadrapassel"],
        "FEDORA-FLATPAK-2022-274a792493": ["baobab", "eog"],
        "FEDORA-FLATPAK-2022-6e191c5e67": ["eog"],
        "FEDORA-FLATPAK-2022-efcca6b48a": ["eog"],
    }


@mock_koji
@mock_redis
@mock_bodhi
def test_bodhi_refresh_update_status(session):
    update_id = "FEDORA-FLATPAK-2022-274a792493"

    refresh_all_updates(session, "flatpak")
    update_raw = session.redis_client.get("update:" + update_id)
    update = BodhiUpdateModel.from_json_text(update_raw)
    update.status = "pending"
    session.redis_client.set("update:" + update_id, update.to_json_text())

    refresh_update_status(session, update_id)

    update_raw = session.redis_client.get("update:" + update_id)
    update = BodhiUpdateModel.from_json_text(update_raw)
    assert update.status == "stable"

    # This should do nothing
    refresh_update_status(session, "NO_SUCH_UPDATE")


@mock_koji
@mock_redis
@mock_bodhi
def test_bodhi_update_cache_global(session, caplog):
    caplog.set_level(logging.INFO)

    refresh_all_updates(session, "flatpak")
    assert "submitted_since" not in caplog.text
    caplog.clear()

    refresh_all_updates(session, "flatpak")
    assert "submitted_since" in caplog.text
    caplog.clear()

    reset_update_cache(session)

    refresh_all_updates(session, "flatpak")
    assert "submitted_since" not in caplog.text
    caplog.clear()


@mock_koji
@mock_redis
@mock_bodhi
def test_bodhi_update_cache_per_package(session, caplog):
    caplog.set_level(logging.INFO)

    refresh_updates(session, "flatpak", entities=["feedreader"])
    assert "submitted_since" not in caplog.text
    caplog.clear()

    refresh_updates(session, "flatpak", entities=["feedreader"])
    assert "submitted_since" in caplog.text
    caplog.clear()

    reset_update_cache(session)

    refresh_updates(session, "flatpak", entities=["feedreader"])
    assert "submitted_since" not in caplog.text
    caplog.clear()


@mock_bodhi
def test_query_releases(session):
    releases = query_releases(session)

    release = next(r for r in releases if r.name == "F36")
    assert release.branch == "f36"
    assert release.tag == "f36"
    assert release.status == ReleaseStatus.GA


@mock_bodhi
def test_query_releases_corner_cases(session, bodhi_mock, caplog):
    def modify_releases(release_json):
        # Branched
        release_json.append(
            {
                "name": "F90",
                "branch": "f90",
                "dist_tag": "f90",
                "state": "pending",
            }
        )
        # Branched and frozen
        release_json.append(
            {
                "name": "F91",
                "branch": "f91",
                "dist_tag": "f91",
                "state": "frozen",
            }
        )
        # Skip because not F\d+$
        release_json.append(
            {
                "name": "F91F",
                "branch": "f91",
                "dist_tag": "f90-flatpak",
                "state": "current",
            }
        )
        # Skip because disabled
        release_json.append(
            {
                "name": "F92",
                "branch": "f92",
                "dist_tag": "f92",
                "state": "disabled",
            }
        )
        # Skip because unknown state
        release_json.append(
            {
                "name": "F99",
                "branch": "f99",
                "dist_tag": "f99",
                "state": "weird",
            }
        )

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
