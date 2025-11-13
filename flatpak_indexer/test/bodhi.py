from contextlib import contextmanager
from copy import deepcopy
from typing import List
from urllib.parse import parse_qs, urlparse
import gzip
import json
import re

from iso8601 import iso8601
import responses

from ..bodhi_query import parse_date_value
from . import get_test_data_path
from .decorators import WithArgDecorator

_updates: List[dict] = []

RELEASES = [
    {
        "name": "F34",
        "branch": "f34",
        "dist_tag": "f34",
        "state": "archived",
    },
    {
        "name": "F35",
        "branch": "f35",
        "dist_tag": "f35",
        "state": "current",
    },
    {
        "name": "F36",
        "branch": "f36",
        "dist_tag": "f36",
        "state": "current",
    },
    {
        "name": "F37",
        "branch": "rawhide",
        "dist_tag": "f37",
        "state": "pending",
    },
]


def load_updates():
    if len(_updates) == 0:
        build_dir = get_test_data_path() / "builds"
        build_nvrs = {f.name[0:-8] for f in build_dir.iterdir()}

        data_dir = get_test_data_path() / "updates"
        for child in data_dir.iterdir():
            if not child.name.endswith(".json.gz"):
                continue

            with gzip.open(child, "rt") as f:
                data = json.load(f)

                # Strip out any builds not in the test data
                data["builds"] = [x for x in data["builds"] if x["nvr"] in build_nvrs]

                _updates.append(data)

    return _updates


def _parse_date_param(params, name):
    values = params.get(name)
    if not values:
        return None
    return iso8601.parse_date(values[0])


def _check_date(update, name, since):
    if since is None:
        return True

    value = update.get(name)
    if value is None:
        return False
    if parse_date_value(value) < since:
        return False

    return True


class MockBodhi:
    def __init__(self, flags=None, modify=None):
        self.flags = flags or []
        self.modify = modify
        self.modify_releases = None

    def get_updates_callback(self, request):
        params = parse_qs(urlparse(request.url).query)

        page = int(params["page"][0])
        rows_per_page = int(params["rows_per_page"][0])
        content_type = params.get("content_type", (None,))[0]
        packages = params.get("packages")

        submitted_since = _parse_date_param(params, "submitted_since")
        modified_since = _parse_date_param(params, "modified_since")
        pushed_since = _parse_date_param(params, "pushed_since")

        updates = load_updates()

        matched_updates = []
        for update in updates:
            if self.modify:
                update = self.modify(update)

            if content_type is not None and update["content_type"] != content_type:
                continue
            if packages is not None:
                found = False
                for b in update["builds"]:
                    n, v, r = b["nvr"].rsplit("-", 2)
                    if n in packages:
                        found = True
                if not found:
                    continue

            if not _check_date(update, "date_submitted", submitted_since):
                continue
            if not _check_date(update, "date_modified", modified_since):
                continue
            if not _check_date(update, "date_pushed", pushed_since):
                continue

            matched_updates.append(update)

        # The ghost_update flags emulates the problem in
        # https://github.com/fedora-infra/bodhi/issues/4130 where deduplication happens
        # after paging.
        if "ghost_updates" in self.flags:
            duplicated_update = matched_updates[rows_per_page - 1]
            matched_updates[rows_per_page - 1 : 0] = [duplicated_update] * 3

        # Sort in descending order by date_submitted
        matched_updates.sort(key=lambda x: parse_date_value(update["date_submitted"]), reverse=True)

        pages = (len(matched_updates) + rows_per_page - 1) // rows_per_page
        paged_updates = matched_updates[(page - 1) * rows_per_page : page * rows_per_page]

        if "ghost_updates" in self.flags:
            # Deduplicate preserving order
            paged_updates = list({x["updateid"]: x for x in paged_updates}.values())

        # If something changes during page requests, total could be greater than the
        # number of updates we see

        total = len(matched_updates)
        if "bad_total" in self.flags:
            total += 1

        return (
            200,
            {},
            json.dumps(
                {
                    "page": page,
                    "pages": pages,
                    "rows_per_page": rows_per_page,
                    "total": total,
                    "updates": paged_updates,
                }
            ),
        )

    def get_update_callback(self, request):
        path = urlparse(request.url).path
        update_id = path.split("/")[-1]

        updates = load_updates()
        for update in updates:
            if update["updateid"] == update_id:
                if self.modify:
                    update = self.modify(update)

                return (
                    200,
                    {},
                    json.dumps(
                        {
                            "update": update,
                            "can_edit": False,
                        }
                    ),
                )

        return (
            404,
            {},
            json.dumps(
                {
                    "status": "error",
                    "errors": [
                        {"location": "url", "name": "id", "description": "Invalid update id"}
                    ],
                }
            ),
        )

    def get_releases_callback(self, request):
        params = parse_qs(urlparse(request.url).query)

        page = int(params["page"][0])
        rows_per_page = int(params["rows_per_page"][0])

        if self.modify_releases:
            releases = deepcopy(RELEASES)
            self.modify_releases(releases)
        else:
            releases = RELEASES

        pages = (len(releases) + rows_per_page - 1) // rows_per_page
        paged_releases = releases[(page - 1) * rows_per_page : page * rows_per_page]

        return (
            200,
            {},
            json.dumps(
                {
                    "page": page,
                    "pages": pages,
                    "rows_per_page": rows_per_page,
                    "total": len(releases),
                    "releases": paged_releases,
                }
            ),
        )


@contextmanager
def _setup_bodhi(**kwargs):
    with responses._default_mock:
        bodhi_mock = MockBodhi(**kwargs)

        responses.add_callback(
            method=responses.GET,
            url="https://bodhi.fedoraproject.org/updates/",
            callback=bodhi_mock.get_updates_callback,
            content_type="application/json",
            match_querystring=False,
        )
        responses.add_callback(
            method=responses.GET,
            url=re.compile("https://bodhi.fedoraproject.org/updates/([a-zA-Z0-9-]+)"),
            callback=bodhi_mock.get_update_callback,
            content_type="application/json",
            match_querystring=False,
        )
        responses.add_callback(
            method=responses.GET,
            url="https://bodhi.fedoraproject.org/releases/",
            callback=bodhi_mock.get_releases_callback,
            content_type="application/json",
            match_querystring=False,
        )
        yield bodhi_mock


mock_bodhi = WithArgDecorator("bodhi_mock", _setup_bodhi)
