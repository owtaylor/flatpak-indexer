from textwrap import dedent
import copy
import json
import logging
import time

from pytest import fixture, raises

from flatpak_indexer.koji_query import (
    _query_package_build_by_id,
    list_flatpak_builds,
    query_image_build,
    query_module_build,
    query_tag_builds,
    refresh_flatpak_builds,
    refresh_tag_builds,
)
from flatpak_indexer.koji_utils import KojiConfig
from flatpak_indexer.models import FlatpakBuildModel
from flatpak_indexer.redis_utils import RedisConfig
from flatpak_indexer.session import Session
from flatpak_indexer.test.koji import make_koji_session, mock_koji
from flatpak_indexer.test.redis import make_redis_client, mock_redis


class TestConfig(KojiConfig, RedisConfig):
    pass


@fixture
def session():
    config = TestConfig.from_str(
        dedent("""
        koji_config: fedora
        redis_url: redis://localhost
""")
    )
    yield Session(config)


@mock_koji
@mock_redis
def test_query_builds(session, caplog):
    caplog.set_level(logging.INFO)

    def sort_builds(builds):
        builds.sort(key=lambda x: x.build_id)

    # First try, we query from scratch from Koji
    caplog.clear()
    refresh_flatpak_builds(session, ["eog"])
    assert "Calling koji.listBuilds({'type': 'image', 'state': 1, 'packageID': 303})" in caplog.text

    builds = list_flatpak_builds(session, "eog")
    sort_builds(builds)
    assert len(builds) == 6
    assert builds[0].nvr == "eog-stable-3520211004195602.1"
    assert builds[0].user_name == "kalev"
    assert builds[0].completion_time.strftime("%Y-%m-%d %H:%M:%S") == "2021-10-04 23:13:09"

    # Add quadrapassel to the set we request
    caplog.clear()
    refresh_flatpak_builds(session, ["eog", "quadrapassel"])
    assert (
        "Calling koji.listBuilds({'type': 'image', 'state': 1, 'packageID': 303})"
        not in caplog.text
    )

    new_builds = list_flatpak_builds(session, "eog")
    sort_builds(new_builds)
    assert len(new_builds) == 6

    new_builds = list_flatpak_builds(session, "quadrapassel")
    assert len(new_builds) == 1
    assert new_builds[0].nvr == "quadrapassel-stable-3520211005192946.1"


@mock_koji
@mock_redis
def test_query_builds_refresh(session):
    EOG_NVR = "eog-stable-3520211004195602.1"
    QUADRAPASSEL_NVR = "quadrapassel-stable-3520211005192946.1"

    def filter_build_1(build):
        if build["nvr"] in (EOG_NVR, QUADRAPASSEL_NVR):
            return None

        return build

    session.koji_session = make_koji_session(filter_build=filter_build_1)
    refresh_flatpak_builds(session, ["eog"])

    builds = list_flatpak_builds(session, "eog")
    assert len(builds) == 5

    current_ts = time.time()

    def filter_build_2(build):
        if build["nvr"] in (EOG_NVR, QUADRAPASSEL_NVR):
            build = copy.copy(build)
            build["completion_ts"] = current_ts

        return build

    session.koji_session = make_koji_session(filter_build=filter_build_2)
    refresh_flatpak_builds(session, ["eog"])

    builds = list_flatpak_builds(session, "eog")
    assert len(builds) == 6


@mock_koji
@mock_redis
def test_query_builds_refetch(session, caplog):
    caplog.set_level(logging.INFO)

    def sort_builds(builds):
        builds.sort(key=lambda x: x.build_id)

    # query from scratch from Koji to prime the redis cache
    refresh_flatpak_builds(session, ["eog"])
    assert "Calling koji.listBuilds({'type': 'image', 'state': 1, 'packageID': 303})" in caplog.text

    builds = list_flatpak_builds(session, "eog")
    sort_builds(builds)
    assert len(builds) == 6

    # Now we simulate having old schemas and missing data
    raw = session.redis_client.get("build:" + builds[0].nvr)
    assert raw
    data = json.loads(raw)
    data["PackageBuilds"] = [pb["Nvr"] for pb in data["PackageBuilds"]]
    session.redis_client.set("build:" + builds[0].nvr, json.dumps(data))

    session.redis_client.delete("build:" + builds[1].nvr)

    caplog.clear()
    new_builds = list_flatpak_builds(session, "eog")

    sort_builds(new_builds)
    assert len(new_builds) == 6
    assert new_builds[0].nvr == builds[0].nvr
    assert new_builds[1].nvr == builds[1].nvr

    assert "Calling koji.getBuild(eog-stable-3520211004195602.1)" in caplog.text
    assert "Calling koji.getBuild(eog-stable-3520211004195602.2)" in caplog.text


@mock_koji
@mock_redis
def test_query_image_build(session, caplog):
    caplog.set_level(logging.INFO)

    caplog.clear()
    build = query_image_build(session, "baobab-stable-3620220517102805.1")
    assert "Calling koji.getBuild" in caplog.text

    assert isinstance(build, FlatpakBuildModel)

    assert build.nvr == "baobab-stable-3620220517102805.1"
    assert build.repository == "baobab"

    assert len(build.images) == 2
    image = [i for i in build.images if i.architecture == "amd64"][0]
    assert image.digest == "sha256:6cca1bdcabf459f3510ccd6a4d196e5a9bde7e049468d2abdb5a404d67ad028c"
    assert image.media_type == "application/vnd.oci.image.manifest.v1+json"
    assert image.labels["org.flatpak.ref"] == "app/org.gnome.baobab/x86_64/stable"
    assert image.diff_ids == [
        "sha256:c9e7b6727e409ad4648854bf4922b1760a3cc8de32b17b28a802b6bea9c2d2da"
    ]

    caplog.clear()
    build2 = query_image_build(session, "baobab-stable-3620220517102805.1")
    assert "Calling koji.getBuild" not in caplog.text

    assert isinstance(build2, FlatpakBuildModel)


@mock_koji
@mock_redis
def test_query_image_build_package_builds(session, caplog):
    build = query_image_build(session, "eog-stable-3620220905044417.1")
    assert isinstance(build, FlatpakBuildModel)
    assert [pb.nvr.name for pb in build.package_builds] == [
        "eog",
        "exempi",
        "gnome-desktop3",
        "libpeas",
        "libpeas-gtk",
        "libportal",
        "libportal-gtk3",
    ]


@mock_koji
@mock_redis
def test_query_image_build_no_images(session):
    def filter_archives(build, archives):
        archives = copy.deepcopy(archives)
        for a in archives:
            a["extra"].pop("docker", None)

        return archives

    session.koji_session = make_koji_session(filter_archives=filter_archives)
    build = query_image_build(session, "baobab-stable-3620220517102805.1")
    assert build.images == []


@mock_koji
@mock_redis
def test_query_image_build_missing_digest(session):
    def filter_archives(build, archives):
        archives = copy.deepcopy(archives)
        for a in archives:
            a["extra"]["docker"]["digests"] = {}

        return archives

    session.koji_session = make_koji_session(filter_archives=filter_archives)

    with raises(RuntimeError, match=r"Can't find OCI or docker digest in image"):
        query_image_build(session, "baobab-stable-3620220517102805.1")


@mock_koji
@mock_redis
def test_query_module_build(session, caplog):
    caplog.set_level(logging.INFO)

    # Try with a context
    caplog.clear()
    build = query_module_build(session, "eog-stable-3620220905044417.ff2200aa")
    assert build.nvr == "eog-stable-3620220905044417.ff2200aa"
    assert "Calling koji.getBuild" in caplog.text

    caplog.clear()
    build = query_module_build(session, "eog-stable-3620220905044417.ff2200aa")
    assert build.nvr == "eog-stable-3620220905044417.ff2200aa"
    assert "Calling koji.getBuild" not in caplog.text

    # And without a context (again from scratch)
    session.redis_client = make_redis_client()

    caplog.clear()
    build = query_module_build(session, "eog-stable-3620220905044417")
    assert "Calling koji.getPackageID" in caplog.text
    assert "Calling koji.listBuilds" in caplog.text

    caplog.clear()
    build = query_module_build(session, "eog-stable-3620220905044417")
    assert build.nvr == "eog-stable-3620220905044417.ff2200aa"
    assert "Calling koji.getPackageID" not in caplog.text
    assert "Calling koji.listBuilds" not in caplog.text


@mock_koji
@mock_redis
def test_query_module_build_package_builds(session, caplog):
    build = query_module_build(session, "eog-stable-3620220905044417.ff2200aa")
    assert [pb.nvr.name for pb in build.package_builds] == [
        "eog",
        "eog-debuginfo",
        "eog-debugsource",
        "eog-devel",
        "eog-tests",
        "exempi",
        "exempi-debuginfo",
        "exempi-debugsource",
        "exempi-devel",
        "libportal",
        "libportal-debuginfo",
        "libportal-debugsource",
        "libportal-devel",
        "libportal-devel-doc",
        "libportal-gtk3",
        "libportal-gtk3-debuginfo",
        "libportal-gtk3-devel",
        "libportal-gtk4",
        "libportal-gtk4-debuginfo",
        "libportal-gtk4-devel",
        "libportal-qt5",
        "libportal-qt5-debuginfo",
        "libportal-qt5-devel",
    ]


@mock_koji
@mock_redis
def test_query_module_build_multiple_contexts(session, caplog):
    caplog.clear()
    build = query_module_build(session, "django-1.6-20180828135711")
    assert build.nvr == "django-1.6-20180828135711.a5b0195c"
    assert "More than one context for django-1.6-20180828135711, using most recent!" in caplog.text


@mock_koji
@mock_redis
def test_query_package_build_by_bad_id(session):
    with raises(RuntimeError, match="Could not look up build ID -1 in Koji"):
        _query_package_build_by_id(session, -1)


@mock_koji
@mock_redis
def test_query_build_missing(session):
    with raises(RuntimeError, match="Could not look up BAH-1-1 in Koji"):
        query_image_build(session, "BAH-1-1")

    with raises(RuntimeError, match="Could not look up package ID for BAH"):
        query_module_build(session, "BAH-1-1")

    with raises(RuntimeError, match="Could not look up eog-1-1 in Koji"):
        query_module_build(session, "eog-1-1")


@mock_koji
@mock_redis
def test_query_tag_builds(session):
    refresh_tag_builds(session, "f36")
    build_names = sorted(query_tag_builds(session, "f36", "quadrapassel"))

    assert build_names == [
        "quadrapassel-40.2-2.fc35",
        "quadrapassel-40.2-3.fc36",
    ]


@mock_koji
@mock_redis
def test_query_tag_builds_incremental(session):
    # Start off with a koji session that will return tag history
    # mid-way through the f28 development cycle
    session.koji_session = make_koji_session(tag_query_timestamp=1643846400)

    refresh_tag_builds(session, "f36")

    build_names = sorted(query_tag_builds(session, "f36", "gnome-desktop3"))
    assert build_names == [
        "gnome-desktop3-41.3-1.fc36",
        "gnome-desktop3-42~alpha.1-1.fc36",
        "gnome-desktop3-42~alpha.1-2.fc36",
        "gnome-desktop3-42~alpha.1-3.fc36",
    ]

    # Now switch to a koji session without that limitation,
    # check that when we refresh, we add and remove builds properly
    session.koji_session = make_koji_session()

    refresh_tag_builds(session, "f36")

    build_names = sorted(query_tag_builds(session, "f36", "gnome-desktop3"))
    assert build_names == [
        "gnome-desktop3-42~alpha.1-3.fc36",
        "gnome-desktop3-42~beta-3.fc36",
        "gnome-desktop3-42.0-1.fc36",
    ]
