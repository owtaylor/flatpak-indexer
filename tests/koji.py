from copy import deepcopy
import gzip
import json
import os
from unittest.mock import create_autospec, Mock

import koji


_builds = []
_tags = {}


def data_dir(subdir):
    return os.path.join(os.path.dirname(__file__), '../test-data', subdir)


def _load_builds():
    if len(_builds) == 0:
        for child in os.scandir(data_dir('builds')):
            if not child.name.endswith('.json.gz'):
                continue
            with gzip.open(child.path, 'rt') as f:
                _builds.append(json.load(f))

    return _builds


def _load_tags():
    if len(_tags) == 0:
        for child in os.scandir(data_dir('tags')):
            if not child.name.endswith('.json.gz'):
                continue
            tag = child.name[:-8]
            with gzip.open(child.path, 'rt') as f:
                _tags[tag] = json.load(f)

    return _tags


def mock_get_package_id(name):
    return {
        'baobab': 1369,
        'bubblewrap': 22617,
        'django': 26201,
        'eog': 303,
        'exempi': 4845,
        'feedreader': 20956,
        'flatpak-rpm-macros': 24301,
        'flatpak-common': 27629,
        'flatpak-runtime': 25428,
        'gnome-desktop3': 10518,
        'libpeas': 10531,
        'quadrapassel': 16135,
    }.get(name)


def make_mock_list_builds(filter_build):
    def mock_list_builds(packageID=None, type=None, state=None, completeAfter=None):
        result = []
        for b in _load_builds():
            if filter_build is not None:
                b = filter_build(b)
                if b is None:
                    continue

            extra = b.get('extra')
            if extra:
                typeinfo = extra.get('typeinfo')
            else:
                typeinfo = None

            if extra and extra.get('image'):
                btype = 'image'
            elif typeinfo and typeinfo.get('module'):
                btype = 'module'
            else:
                btype = 'rpm'

            if type is not None and btype != type:
                continue
            if packageID is not None and b['package_id'] != packageID:
                continue
            if state is not None and b['state'] != state:
                continue

            if completeAfter is not None and b['completion_ts'] <= completeAfter:
                continue

            b2 = deepcopy(b)
            if 'archives' in b:
                del b2['archives']

            result.append(b)

        # Descending order by build_id seems to match what koji does, in any case
        # we don't want to order in readdir order
        result.sort(key=lambda x: x['build_id'], reverse=True)

        return result

    return mock_list_builds


def mock_get_build(nvr):
    if isinstance(nvr, int):
        for b in _load_builds():
            if b['id'] == nvr:
                return b
    else:
        for b in _load_builds():
            if b['nvr'] == nvr:
                return b

    return None


def mock_list_archives(build_id):
    for b in _load_builds():
        if b['id'] == build_id:
            archives = deepcopy(b['archives'])
            for a in archives:
                del a['components']

            return archives

    raise RuntimeError(f"Build id={build_id} not found")


def mock_list_rpms(imageID=None):
    if imageID is None:
        raise RuntimeError("listRPMs - only lookup by imageID is implemented")

    for b in _load_builds():
        if 'archives' not in b:
            continue
        for archive in b['archives']:
            if archive['id'] == imageID:
                return archive['components']

    raise RuntimeError(f"Image id={imageID} not found")


def make_mock_query_history(tagQueryTimestamp):
    def mock_query_history(tables=None, tag=None, afterEvent=None):
        assert tables == ['tag_listing']
        assert tag is not None

        result = []
        tags = _load_tags()
        for item in tags.get(tag, ()):
            if tagQueryTimestamp:
                if item['create_ts'] > tagQueryTimestamp:
                    continue

                if item['revoke_ts'] is not None and item['revoke_ts'] > tagQueryTimestamp:
                    item = item.copy()
                    item['revoke_ts'] = None
                    item['revoke_event'] = None
                    item['revoker_id'] = None
                    item['rovoker_name'] = None

            if afterEvent:
                if not (item['create_event'] > afterEvent or
                        item['revoke_event'] and item['revoke_event'] > afterEvent):
                    continue

            result.append(item)

        return {'tag_listing': result}

    return mock_query_history


def make_koji_session(tagQueryTimestamp=None, filter_build=None):
    session = create_autospec(koji.ClientSession)
    session.getPackageID = Mock()
    session.getPackageID.side_effect = mock_get_package_id
    session.listBuilds = Mock()
    session.listBuilds.side_effect = make_mock_list_builds(filter_build)
    session.getBuild = Mock()
    session.getBuild.side_effect = mock_get_build
    session.listArchives = Mock()
    session.listArchives.side_effect = mock_list_archives
    session.listRPMs = Mock()
    session.listRPMs.side_effect = mock_list_rpms
    session.queryHistory = Mock()
    session.queryHistory.side_effect = make_mock_query_history(tagQueryTimestamp)

    return session
