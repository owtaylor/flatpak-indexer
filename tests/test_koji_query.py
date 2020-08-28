import copy
import logging
import time

from pytest import raises

from flatpak_indexer.koji_query import (list_flatpak_builds,
                                        query_flatpak_build,
                                        query_module_build,
                                        _query_package_build_by_id,
                                        query_tag_builds,
                                        refresh_flatpak_builds,
                                        refresh_tag_builds)
from .koji import make_koji_session
from .redis import make_redis_client


def test_query_builds(caplog):
    caplog.set_level(logging.INFO)

    def sort_builds(builds):
        builds.sort(key=lambda x: x.build_id)

    koji_session = make_koji_session()
    redis_client = make_redis_client()

    # First try, we query from scratch from Koji
    caplog.clear()
    refresh_flatpak_builds(koji_session, redis_client, ['eog'])
    assert "Calling koji.listBuilds({'type': 'image', 'state': 1, 'packageID': 303})" in caplog.text

    builds = list_flatpak_builds(redis_client, 'eog')
    sort_builds(builds)
    assert len(builds) == 2
    assert builds[0].nvr == 'eog-master-20180821163756.2'
    assert builds[0].user_name == 'otaylor'
    assert builds[0].completion_time.strftime("%Y-%m-%d %H:%M:%S") == '2018-10-08 14:01:05'

    # Add quadrapassel to the set we request
    caplog.clear()
    refresh_flatpak_builds(koji_session, redis_client, ['eog', 'quadrapassel'])
    assert ("Calling koji.listBuilds({'type': 'image', 'state': 1, 'packageID': 303})"
            not in caplog.text)

    new_builds = list_flatpak_builds(redis_client, 'eog')
    sort_builds(new_builds)
    assert len(new_builds) == 2

    new_builds = list_flatpak_builds(redis_client, 'quadrapassel')
    assert len(new_builds) == 1
    assert new_builds[0].nvr == 'quadrapassel-master-20181203181243.2'


def test_query_builds_refresh():
    EOG_NVR = 'eog-master-20180821163756.2'
    QUADRAPASSEL_NVR = 'quadrapassel-master-20181203181243.2'

    redis_client = make_redis_client()

    def filter_build_1(build):
        if build['nvr'] in (EOG_NVR, QUADRAPASSEL_NVR):
            return None

        return build

    koji_session = make_koji_session(filter_build=filter_build_1)
    refresh_flatpak_builds(koji_session, redis_client, ['eog'])

    builds = list_flatpak_builds(redis_client, 'eog')
    assert len(builds) == 1

    current_ts = time.time()

    def filter_build_2(build):
        if build['nvr'] in (EOG_NVR, QUADRAPASSEL_NVR):
            build = copy.copy(build)
            build['completion_ts'] = current_ts

        return build

    koji_session = make_koji_session(filter_build=filter_build_2)
    refresh_flatpak_builds(koji_session, redis_client, ['eog'])

    builds = list_flatpak_builds(redis_client, 'eog')
    assert len(builds) == 2


def test_query_flatpak_build(caplog):
    caplog.set_level(logging.INFO)

    koji_session = make_koji_session()
    redis_client = make_redis_client()

    caplog.clear()
    build = query_flatpak_build(koji_session, redis_client, 'baobab-master-3220200331145937.2')
    assert "Calling koji.getBuild" in caplog.text

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
    query_flatpak_build(koji_session, redis_client, 'baobab-master-3220200331145937.2')
    assert "Calling koji.getBuild" not in caplog.text


def test_query_flatpak_build_missing_digest():
    def filter_archives(build, archives):
        archives = copy.deepcopy(archives)
        for a in archives:
            a['extra']['docker']['digests'] = {}

        return archives

    koji_session = make_koji_session(filter_archives=filter_archives)
    redis_client = make_redis_client()

    with raises(RuntimeError, match=r"Can't find OCI or docker digest in image"):
        query_flatpak_build(koji_session, redis_client, 'baobab-master-3220200331145937.2')


def test_query_module_build(caplog):
    caplog.set_level(logging.INFO)

    koji_session = make_koji_session()
    redis_client = make_redis_client()

    # Try with a context
    caplog.clear()
    build = query_module_build(koji_session, redis_client, 'eog-master-20180821163756.775baa8e')
    assert build.nvr == 'eog-master-20180821163756.775baa8e'
    assert "Calling koji.getBuild" in caplog.text

    caplog.clear()
    build = query_module_build(koji_session, redis_client, 'eog-master-20180821163756.775baa8e')
    assert build.nvr == 'eog-master-20180821163756.775baa8e'
    assert "Calling koji.getBuild" not in caplog.text

    # And without a context (again from scratch)
    redis_client = make_redis_client()

    caplog.clear()
    build = query_module_build(koji_session, redis_client, 'eog-master-20180821163756')
    assert "Calling koji.getPackageID" in caplog.text
    assert "Calling koji.listBuilds" in caplog.text

    caplog.clear()
    build = query_module_build(koji_session, redis_client, 'eog-master-20180821163756')
    assert build.nvr == 'eog-master-20180821163756.775baa8e'
    assert "Calling koji.getPackageID" not in caplog.text
    assert "Calling koji.listBuilds" not in caplog.text


def test_query_module_build_multiple_contexts():
    koji_session = make_koji_session()
    redis_client = make_redis_client()

    with raises(RuntimeError, match="More than one context for django-1.6-20180828135711"):
        query_module_build(koji_session, redis_client, 'django-1.6-20180828135711')


def test_query_package_build_by_bad_id():
    koji_session = make_koji_session()
    redis_client = make_redis_client()

    with raises(RuntimeError, match="Could not look up build ID -1 in Koji"):
        _query_package_build_by_id(koji_session, redis_client, -1)


def test_query_build_missing():
    koji_session = make_koji_session()
    redis_client = make_redis_client()

    with raises(RuntimeError, match="Could not look up BAH-1-1 in Koji"):
        query_flatpak_build(koji_session, redis_client, 'BAH-1-1')

    with raises(RuntimeError, match="Could not look up package ID for BAH"):
        query_module_build(koji_session, redis_client, 'BAH-1-1')

    with raises(RuntimeError, match="Could not look up eog-1-1 in Koji"):
        query_module_build(koji_session, redis_client, 'eog-1-1')


def test_query_tag_builds():
    koji_session = make_koji_session()
    redis_client = make_redis_client()

    refresh_tag_builds(koji_session, redis_client, 'f28')
    build_names = sorted(query_tag_builds(redis_client, 'f28', 'quadrapassel'))

    assert build_names == [
        'quadrapassel-3.22.0-2.fc26',
        'quadrapassel-3.22.0-5.fc28',
        'quadrapassel-3.22.0-6.fc28',
    ]


def test_query_tag_builds_incremental():
    # Start off with a koji session that will return tag history
    # mid-way through the f28 development cycle
    koji_session = make_koji_session(tag_query_timestamp=1521520000)
    redis_client = make_redis_client()

    refresh_tag_builds(koji_session, redis_client, 'f28')

    build_names = sorted(query_tag_builds(redis_client, 'f28', 'gnome-desktop3'))
    assert build_names == [
        'gnome-desktop3-3.26.1-1.fc28',
        'gnome-desktop3-3.26.2-1.fc28',
        'gnome-desktop3-3.27.90-1.fc28',
    ]

    # Now switch to a koji session without that limitation,
    # check that when we refresh, we add and remove builds properly
    koji_session = make_koji_session()

    refresh_tag_builds(koji_session, redis_client, 'f28')

    build_names = sorted(query_tag_builds(redis_client, 'f28', 'gnome-desktop3'))
    assert build_names == [
        'gnome-desktop3-3.27.90-1.fc28',
        'gnome-desktop3-3.28.0-1.fc28',
        'gnome-desktop3-3.28.1-1.fc28',
    ]
