from copy import deepcopy
from functools import wraps
from typing import Any, Callable, Dict, List
from unittest.mock import DEFAULT, Mock, create_autospec, patch
import gzip
import json

from koji import ClientSession

from . import get_test_data_path

_builds: List[dict] = []
_tags: Dict[str, dict] = {}


def _load_builds():
    if len(_builds) == 0:
        for child in (get_test_data_path() / "builds").iterdir():
            if not child.name.endswith(".json.gz"):
                continue
            with gzip.open(child, "rt") as f:
                _builds.append(json.load(f))

    return _builds


def _load_tags():
    if len(_tags) == 0:
        for child in (get_test_data_path() / "tags").iterdir():
            if not child.name.endswith(".json.gz"):
                continue
            tag = child.name[:-8]
            with gzip.open(child, "rt") as f:
                _tags[tag] = json.load(f)

    return _tags


class MockKojiContext:
    def __init__(
        self,
        filter_archives: Callable[[dict[str, Any], List[dict[str, Any]]], List[dict[str, Any]]]
        | None = None,
        filter_build: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
        tag_query_timestamp=None,
    ):
        self.filter_archives = filter_archives
        self.filter_build = filter_build
        self.tag_query_timestamp = tag_query_timestamp

    def get_build(self, nvr):
        if isinstance(nvr, int):
            for b in _load_builds():
                if b["id"] == nvr:
                    return b
        else:
            for b in _load_builds():
                if b["nvr"] == nvr:
                    return b

        return None

    def get_package_id(self, name):
        return {
            "baobab": 1369,
            "bubblewrap": 22617,
            "django": 26201,
            "eog": 303,
            "exempi": 4845,
            "feedreader": 20956,
            "flatpak-rpm-macros": 24301,
            "flatpak-common": 27629,
            "flatpak-runtime": 25428,
            "gnome-clocks": 14779,
            "gnome-desktop3": 10518,
            "gnome-weather": 15839,
            "libpeas": 10531,
            "quadrapassel": 16135,
        }.get(name)

    def list_archives(self, build_id):
        for b in _load_builds():
            if b["id"] == build_id:
                archives = deepcopy(b["archives"])
                for a in archives:
                    del a["components"]

                if self.filter_archives:
                    archives = self.filter_archives(b, archives)
                return archives

        raise RuntimeError(f"Build id={build_id} not found")

    def list_builds(self, packageID=None, type=None, state=None, completeAfter=None):
        result = []
        for b in _load_builds():
            if self.filter_build:
                b = self.filter_build(b)
                if b is None:
                    continue

            extra = b.get("extra")
            if extra:
                typeinfo = extra.get("typeinfo")
            else:
                typeinfo = None

            if extra and extra.get("image"):
                btype = "image"
            elif typeinfo and typeinfo.get("module"):
                btype = "module"
            else:
                btype = "rpm"

            if type is not None and btype != type:
                continue
            if packageID is not None and b["package_id"] != packageID:
                continue
            if state is not None and b["state"] != state:
                continue

            if completeAfter is not None and b["completion_ts"] <= completeAfter:
                continue

            b2 = deepcopy(b)
            if "archives" in b:
                del b2["archives"]

            result.append(b)

        # Descending order by build_id seems to match what koji does, in any case
        # we don't want to order in readdir order
        result.sort(key=lambda x: x["build_id"], reverse=True)

        return result

    def list_rpms(self, imageID=None):
        if imageID is None:
            raise RuntimeError("listRPMs - only lookup by imageID is implemented")

        for b in _load_builds():
            if "archives" not in b:
                continue
            for archive in b["archives"]:
                if archive["id"] == imageID:
                    return archive["components"]

        raise RuntimeError(f"Image id={imageID} not found")

    def query_history(self, tables=None, tag=None, afterEvent=None):
        assert tables == ["tag_listing"]
        assert tag is not None

        result = []
        tags = _load_tags()
        for item in tags.get(tag, ()):
            if self.tag_query_timestamp:
                if item["create_ts"] > self.tag_query_timestamp:
                    continue

                if item["revoke_ts"] is not None and item["revoke_ts"] > self.tag_query_timestamp:
                    item = item.copy()
                    item["revoke_ts"] = None
                    item["revoke_event"] = None
                    item["revoker_id"] = None
                    item["rovoker_name"] = None

            if afterEvent:
                if not (
                    item["create_event"] > afterEvent
                    or item["revoke_event"]
                    and item["revoke_event"] > afterEvent
                ):
                    continue

            result.append(item)

        return {"tag_listing": result}


def make_koji_session(**kwargs):
    ctx = MockKojiContext(**kwargs)

    session = create_autospec(ClientSession)
    session.getBuild = Mock()
    session.getBuild.side_effect = ctx.get_build
    session.getPackageID = Mock()
    session.getPackageID.side_effect = ctx.get_package_id
    session.listArchives = Mock()
    session.listArchives.side_effect = ctx.list_archives
    session.listBuilds = Mock()
    session.listBuilds.side_effect = ctx.list_builds
    session.listRPMs = Mock()
    session.listRPMs.side_effect = ctx.list_rpms
    session.queryHistory = Mock()
    session.queryHistory.side_effect = ctx.query_history

    return session


def mock_koji(f):
    session = make_koji_session()

    @wraps(f)
    def wrapper(*args, **kwargs):
        with patch.multiple(
            "koji", read_config=DEFAULT, grab_session_options=DEFAULT, ClientSession=DEFAULT
        ) as mocks:
            mocks["ClientSession"].return_value = session

            return f(*args, **kwargs)

    return wrapper
