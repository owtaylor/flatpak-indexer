import copy
import json
import logging
from textwrap import dedent
import time

from pytest import fixture, raises

from flatpak_indexer.koji_query import (
    _query_package_build_by_id,
    list_flatpak_builds,
    query_image_build,
    query_module_build,
    query_tag_builds,
    refresh_flatpak_builds,
    refresh_tag_builds
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
    config = TestConfig.from_str(dedent("""
        koji_config: fedora
        redis_url: redis://localhost
"""))
    yield Session(config)


@mock_koji
@mock_redis
def test_query_builds(session, caplog):
    caplog.set_level(logging.INFO)

    def sort_builds(builds):
        builds.sort(key=lambda x: x.build_id)

    # First try, we query from scratch from Koji
    caplog.clear()
    refresh_flatpak_builds(session, ['eog'])
    assert "Calling koji.listBuilds({'type': 'image', 'state': 1, 'packageID': 303})" in caplog.text

    builds = list_flatpak_builds(session, 'eog')
    sort_builds(builds)
    assert len(builds) == 2
    assert builds[0].nvr == 'eog-master-20180821163756.2'
    assert builds[0].user_name == 'otaylor'
    assert builds[0].completion_time.strftime("%Y-%m-%d %H:%M:%S") == '2018-10-08 14:01:05'

    # Add quadrapassel to the set we request
    caplog.clear()
    refresh_flatpak_builds(session, ['eog', 'quadrapassel'])
    assert ("Calling koji.listBuilds({'type': 'image', 'state': 1, 'packageID': 303})"
            not in caplog.text)

    new_builds = list_flatpak_builds(session, 'eog')
    sort_builds(new_builds)
    assert len(new_builds) == 2

    new_builds = list_flatpak_builds(session, 'quadrapassel')
    assert len(new_builds) == 1
    assert new_builds[0].nvr == 'quadrapassel-master-20181203181243.2'


@mock_koji
@mock_redis
def test_query_builds_refresh(session):
    EOG_NVR = 'eog-master-20180821163756.2'
    QUADRAPASSEL_NVR = 'quadrapassel-master-20181203181243.2'

    def filter_build_1(build):
        if build['nvr'] in (EOG_NVR, QUADRAPASSEL_NVR):
            return None

        return build

    session.koji_session = make_koji_session(filter_build=filter_build_1)
    refresh_flatpak_builds(session, ['eog'])

    builds = list_flatpak_builds(session, 'eog')
    assert len(builds) == 1

    current_ts = time.time()

    def filter_build_2(build):
        if build['nvr'] in (EOG_NVR, QUADRAPASSEL_NVR):
            build = copy.copy(build)
            build['completion_ts'] = current_ts

        return build

    session.koji_session = make_koji_session(filter_build=filter_build_2)
    refresh_flatpak_builds(session, ['eog'])

    builds = list_flatpak_builds(session, 'eog')
    assert len(builds) == 2


@mock_koji
@mock_redis
def test_query_builds_refetch(session, caplog):
    caplog.set_level(logging.INFO)

    def sort_builds(builds):
        builds.sort(key=lambda x: x.build_id)

    # query from scratch from Koji to prime the redis cache
    refresh_flatpak_builds(session, ['eog'])
    assert "Calling koji.listBuilds({'type': 'image', 'state': 1, 'packageID': 303})" in caplog.text

    builds = list_flatpak_builds(session, 'eog')
    sort_builds(builds)
    assert len(builds) == 2

    # Now we simulate having old schemas and missing data
    raw = session.redis_client.get('build:' + builds[0].nvr)
    assert raw
    data = json.loads(raw)
    data["PackageBuilds"] = [pb["Nvr"] for pb in data["PackageBuilds"]]
    session.redis_client.set('build:' + builds[0].nvr, json.dumps(data))

    session.redis_client.delete('build:' + builds[1].nvr)

    caplog.clear()
    new_builds = list_flatpak_builds(session, 'eog')
    sort_builds(new_builds)
    assert len(new_builds) == 2
    assert new_builds[0].nvr == builds[0].nvr
    assert new_builds[1].nvr == builds[1].nvr

    assert "Calling koji.getBuild(eog-master-20180821163756.2)" in caplog.text
    assert "Calling koji.getBuild(eog-master-20181128204005.1)" in caplog.text


@mock_koji
@mock_redis
def test_query_image_build(session, caplog):
    caplog.set_level(logging.INFO)

    caplog.clear()
    build = query_image_build(session, 'baobab-master-3220200331145937.2')
    assert "Calling koji.getBuild" in caplog.text

    assert isinstance(build, FlatpakBuildModel)

    assert build.nvr == 'baobab-master-3220200331145937.2'
    assert build.repository == 'baobab'

    assert len(build.images) == 1
    image = build.images[0]
    assert image.digest == 'sha256:358650781c10de5983b46303b6accbd411c1177990d1e036ee905f15ed60e65a'
    assert image.media_type == 'application/vnd.oci.image.manifest.v1+json'
    assert image.labels['org.flatpak.ref'] == 'app/org.gnome.Baobab/x86_64/stable'
    assert image.diff_ids == [
        'sha256:d9b3dc4fc51451185b7754d66155513e89d24374a26a3b270f9b99649eb33d22'
    ]

    caplog.clear()
    build2 = query_image_build(session, 'baobab-master-3220200331145937.2')
    assert "Calling koji.getBuild" not in caplog.text

    assert isinstance(build2, FlatpakBuildModel)


@mock_koji
@mock_redis
def test_query_image_build_no_images(session):
    def filter_archives(build, archives):
        archives = copy.deepcopy(archives)
        for a in archives:
            a['extra'].pop('docker', None)

        return archives

    session.koji_session = make_koji_session(filter_archives=filter_archives)
    build = query_image_build(session, 'baobab-master-3220200331145937.2')
    assert build.images == []


@mock_koji
@mock_redis
def test_query_image_build_missing_digest(session):
    def filter_archives(build, archives):
        archives = copy.deepcopy(archives)
        for a in archives:
            a['extra']['docker']['digests'] = {}

        return archives

    session.koji_session = make_koji_session(filter_archives=filter_archives)

    with raises(RuntimeError, match=r"Can't find OCI or docker digest in image"):
        query_image_build(session, 'baobab-master-3220200331145937.2')


@mock_koji
@mock_redis
def test_query_module_build(session, caplog):
    caplog.set_level(logging.INFO)

    # Try with a context
    caplog.clear()
    build = query_module_build(session, 'eog-master-20180821163756.775baa8e')
    assert build.nvr == 'eog-master-20180821163756.775baa8e'
    assert "Calling koji.getBuild" in caplog.text

    caplog.clear()
    build = query_module_build(session, 'eog-master-20180821163756.775baa8e')
    assert build.nvr == 'eog-master-20180821163756.775baa8e'
    assert "Calling koji.getBuild" not in caplog.text

    # And without a context (again from scratch)
    session.redis_client = make_redis_client()

    caplog.clear()
    build = query_module_build(session, 'eog-master-20180821163756')
    assert "Calling koji.getPackageID" in caplog.text
    assert "Calling koji.listBuilds" in caplog.text

    caplog.clear()
    build = query_module_build(session, 'eog-master-20180821163756')
    assert build.nvr == 'eog-master-20180821163756.775baa8e'
    assert "Calling koji.getPackageID" not in caplog.text
    assert "Calling koji.listBuilds" not in caplog.text


@mock_koji
@mock_redis
def test_query_module_build_multiple_contexts(session, caplog):
    caplog.clear()
    build = query_module_build(session, 'django-1.6-20180828135711')
    assert build.nvr == 'django-1.6-20180828135711.a5b0195c'
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
        query_image_build(session, 'BAH-1-1')

    with raises(RuntimeError, match="Could not look up package ID for BAH"):
        query_module_build(session, 'BAH-1-1')

    with raises(RuntimeError, match="Could not look up eog-1-1 in Koji"):
        query_module_build(session, 'eog-1-1')


@mock_koji
@mock_redis
def test_query_tag_builds(session):
    refresh_tag_builds(session, 'f28')
    build_names = sorted(query_tag_builds(session, 'f28', 'quadrapassel'))

    assert build_names == [
        'quadrapassel-3.22.0-2.fc26',
        'quadrapassel-3.22.0-5.fc28',
        'quadrapassel-3.22.0-6.fc28',
    ]


@mock_koji
@mock_redis
def test_query_tag_builds_incremental(session):
    # Start off with a koji session that will return tag history
    # mid-way through the f28 development cycle
    session.koji_session = make_koji_session(tag_query_timestamp=1521520000)

    refresh_tag_builds(session, 'f28')

    build_names = sorted(query_tag_builds(session, 'f28', 'gnome-desktop3'))
    assert build_names == [
        'gnome-desktop3-3.26.1-1.fc28',
        'gnome-desktop3-3.26.2-1.fc28',
        'gnome-desktop3-3.27.90-1.fc28',
    ]

    # Now switch to a koji session without that limitation,
    # check that when we refresh, we add and remove builds properly
    session.koji_session = make_koji_session()

    refresh_tag_builds(session, 'f28')

    build_names = sorted(query_tag_builds(session, 'f28', 'gnome-desktop3'))
    assert build_names == [
        'gnome-desktop3-3.27.90-1.fc28',
        'gnome-desktop3-3.28.0-1.fc28',
        'gnome-desktop3-3.28.1-1.fc28',
    ]
