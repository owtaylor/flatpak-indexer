from functools import wraps
import json
import os
import re
from tempfile import NamedTemporaryFile
from unittest.mock import DEFAULT, MagicMock, patch
from urllib.parse import parse_qs, urlparse

import responses
import yaml

from flatpak_indexer.config import Config


def write_config(tmp_path, content):
    tmpfile = NamedTemporaryFile(delete=False, prefix="config-", suffix=".yaml", dir=tmp_path)
    yaml.dump(content, tmpfile, encoding="UTF=8")
    tmpfile.close()
    return tmpfile.name


def get_config(tmp_path, content):
    path = write_config(tmp_path, content)
    conf = Config(path)
    os.unlink(path)
    return conf


def setup_client_cert(tmp_path, create_cert=True, create_key=True):
    cert_path = str(tmp_path / "client.crt")
    if create_cert:
        with open(cert_path, 'w'):
            pass

    key_path = str(tmp_path / "client.key")
    if create_key:
        with open(key_path, 'w'):
            pass

    return cert_path, key_path


_TEST_ICON_DATA = \
    """iVBORw0KGgoAAAANSUhEUgAAAEAAAABACAIAAAAlC+aJAAABeklEQVRo3t3ay3KEMAxEUdH//8/O
    JplMgDGy3JLb8YqCBfdQPAqXD/sZrTXbZxzH8b3xvncXw6v+DNjC8F5vZjgVnw6L17fWcL3qsoZr
    vZnh9s4RNNzW/wLEDZ/q/wBkDZ36M0DQ0K+/AUgZHuvvASIGT/1HwHKDs74HWGjw1z8AlhiG6p8B
    xYbRehegzBCo9wIKDLH6AUCqIVw/BkgyzNQPA+iGyfoIgGiYrw8CKAZKfRwwaWDVTwHCBmL9LCBg
    4NYTAEMGej0H4DRk1NMAj4akeiagY8irN7ODPpfYf47pp4OxRycxY+KVD/gUmjRtnAKoHCkAz2tU
    F+D/kCkCrm/M7H9RpNYX/E8juz7bgIL6VANq6vMMKKtPMqCyPsOA4nq6AfX1XAOW1BMNWFXPMmBh
    PcWAtfXzBiyvnzRAoX7GAJH6sAE69TEDpOoDBqjVjxogWD9kgGa93wDZeqcByvUeA8TrHw3Qr+8b
    sEV9x7D90uN/tPh70+X3X82aXIZ8Z5vMAAAAAElFTkSuQmCC""".replace('\n', '')


_KOJI_BUILDS = [
    {
        'build_id': 1063042,
        'extra': {
            'image': {
                'flatpak': True,
                'index': {
                    'digests': {'application/vnd.docker.distribution.manifest.list.v2+json':
                                'sha256:' +
                                '9849e17af5db2f38650970c2ce0f2897b6e552e5b7e67adfb53ab51243b5f5f5'},

                    'floating_tags': ['latest', 'el8'],
                    'pull': [
                        'registry-proxy.engineering.redhat.com/rh-osbs/aisleriot@' +
                        'sha256:9849e17af5db2f38650970c2ce0f2897b6e552e5b7e67adfb53ab51243b5f5f5',
                        'registry-proxy.engineering.redhat.com/rh-osbs/aisleriot:' +
                        'el8-8020020200121102609.1'],
                    'tags': ['el8-8020020200121102609.1'],
                    'unique_tags': ['rhel-8.2.0-candidate-73622-20200121110852']},
            }
        },
        'nvr': 'aisleriot-container-el8-8020020200121102609.1',
        '_TAGS': 'release-candidate',
    }
]


_REPOSITORIES = [
    {
        'registry': 'registry2.example.com',
        'repository': 'testrepo',
        'image_usage_type': 'Standalone Image',
    },
    {
        'registry': 'registry.example.com',
        'repository': 'testrepo',
        'image_usage_type': 'Standalone Image',
    },
    {
        'registry': 'registry.example.com',
        'repository': 'aisleriot',
        'image_usage_type': 'Flatpak',
    },
    {
        'registry': 'registry.example.com',
        'repository': 'aisleriot2',
        'image_usage_type': 'Flatpak',
    }
]


_REPO_IMAGES = [
    {
        'architecture': 'amd64',
        'brew': {
            'build': 'testrepo-container-1.2.3-1',
        },
        'docker_image_id':
            'sha256:506dd421c0061b81c511fac731877d66df20aea32e901b0baff5bbcbe020367f',
        'parsed_data': {
            'architecture': 'amd64',
            'os': 'linux',
            'labels': {
            },
        },
        'repositories': [
            {
                'registry': 'registry2.example.com',
                'repository': 'testrepo',
                'tags': [],
            },
            {
                'registry': 'registry.example.com',
                'repository': 'testrepo',
                'tags': [
                    {
                        'name': 'latest',
                        'added_date': '2019-04-25T18:50:02.708000+00:00',
                    }
                ]
            }
        ]
    },
    {
        'architecture': 'amd64',
        'brew': {
            'build': 'aisleriot-container-el8-8020020200121102609.1',
        },
        'docker_image_id':
            'sha256:527dda0ec4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb',
        'parsed_data': {
            'architecture': 'amd64',
            'os': 'linux',
            'labels': [
                {
                    "name": "org.flatpak.ref",
                    "value": "app/org.gnome.Aisleriot/x86_64/stable",
                },
                {
                    "name": "org.freedesktop.appstream.icon-64",
                    "value": "data:image/png;base64," + _TEST_ICON_DATA,
                },
                {
                    "name": "org.freedesktop.appstream.icon-128",
                    "value": "https://www.example.com/icons/aisleriot.png",
                }
            ],
        },
        'repositories': [
            {
                'registry': 'registry.example.com',
                'repository': 'aisleriot',
                'tags': [
                    {
                        'name': 'latest',
                        'added_date': '2019-04-25T18:50:02.708000+00:00',
                    }
                ]
            }
        ]
    },
    {
        'architecture': 'amd64',
        'brew': {
            'build': 'aisleriot2-container-el8-8020020200121102609.1',
        },
        'docker_image_id':
            'sha256:527dda0ec4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb',
        'parsed_data': {
            'architecture': 'amd64',
            'os': 'linux',
            'labels': [
                {
                    "name": "org.flatpak.ref",
                    "value": "app/org.gnome.Aisleriot2/x86_64/stable",
                },
                {
                    "name": "org.freedesktop.appstream.icon-64",
                    "value": "data:image/png;base64," + _TEST_ICON_DATA,
                },
            ],
        },
        'repositories': [
            {
                'registry': 'registry.example.com',
                'repository': 'aisleriot2',
                'tags': [
                    {
                        'name': 'latest',
                        'added_date': '2019-04-25T18:50:02.708000+00:00',
                    }
                ]
            }
        ]
    },
    {
        'architecture': 'ppc64le',
        'brew': {
            'build': 'testrepo-container-1.2.3-1',
        },
        'docker_image_id': 'sha256:asdfasdfasdfasdf',
        'parsed_data': {
            'architecture': 'ppc64le',
            'os': 'linux',
            'labels': {
            },
        },
        'repositories': [
            {
                'registry': 'registry.example.com',
                'repository': 'testrepo',
                'tags': [
                    {
                        'name': 'latest',
                        'added_date': '2019-04-25T18:50:02.708000+00:00',
                    }
                ]
            }
        ]
    }
]


def _paged_result(params, all_results):
    page = int(params['page'][0])
    page_size = int(params['page_size'][0])

    return (200, {}, json.dumps({
        'data': all_results[page * page_size:page * page_size + page_size],
        'page': page,
        'page_size': page_size,
        'total': len(all_results),
    }))


_GET_IMAGES_RE = re.compile(
    r'^https://pyxis.example.com/' +
    r'v1/repositories/registry/([A-Za-z0-9.]+)/repository/([A-Za-z0-9.]+)/images')


def _get_images(request):
    parsed = urlparse(request.url)
    params = parse_qs(parsed.query)

    m = _GET_IMAGES_RE.match('https://pyxis.example.com' + parsed.path)
    assert m is not None
    registry = m.group(1)
    repository = m.group(2)

    images = [i for i in _REPO_IMAGES if
              any((r['registry'], r['repository']) ==
                  (registry, repository) for r in i['repositories'])]

    return _paged_result(params, images)


_GET_IMAGES_NVR_RE = re.compile(
    r'^https://pyxis.example.com/v1/images/nvr/([A-Za-z0-9_.-]+)')


def _get_images_nvr(request):
    parsed = urlparse(request.url)
    params = parse_qs(parsed.query)

    m = _GET_IMAGES_NVR_RE.match('https://pyxis.example.com' + parsed.path)
    assert m is not None
    nvr = m.group(1)

    images = [i for i in _REPO_IMAGES if i['brew']['build'] == nvr]

    return _paged_result(params, images)


_GET_REPOSITORIES_RE = re.compile(
    r'^https://pyxis.example.com/v1/repositories(\?|$)')


def _get_repositories(request):
    parsed = urlparse(request.url)
    params = parse_qs(parsed.query)

    m = _GET_REPOSITORIES_RE.match('https://pyxis.example.com' + parsed.path)
    assert m is not None

    if 'image_usage_type' in params:
        image_usage_type = params['image_usage_type'][0]
        repos = [r for r in _REPOSITORIES if r['image_usage_type'] == image_usage_type]
    else:
        repos = _REPOSITORIES

    return _paged_result(params, repos)


def mock_pyxis():
    responses.add_callback(responses.GET,
                           _GET_IMAGES_RE,
                           callback=_get_images,
                           content_type='application/json',
                           match_querystring=False)
    responses.add_callback(responses.GET,
                           _GET_IMAGES_NVR_RE,
                           callback=_get_images_nvr,
                           content_type='application/json',
                           match_querystring=False)
    responses.add_callback(responses.GET,
                           _GET_REPOSITORIES_RE,
                           callback=_get_repositories,
                           content_type='application/json',
                           match_querystring=False)


def _koji_list_tagged(tag, type, latest):
    assert latest is True
    assert type == 'image'

    result = []
    for build in _KOJI_BUILDS:
        if tag in build['_TAGS']:
            result.append({
                'build_id': build['build_id'],
                'nvr': build['nvr'],
            })

    return result


def _koji_get_build(build_id):
    for build in _KOJI_BUILDS:
        if build['build_id'] == build_id:
            return build

    raise RuntimeError("Build {} not found".format(build_id))


def _koji_list_tags(build_id):
    for build in _KOJI_BUILDS:
        if build['build_id'] == build_id:
            return [{'name': t} for t in build['_TAGS']]

    raise RuntimeError("Build {} not found".format(build_id))


def mock_koji(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        with patch.multiple('koji',
                            read_config=DEFAULT,
                            grab_session_options=DEFAULT,
                            ClientSession=DEFAULT) as mocks:
            ClientSession = mocks['ClientSession']

            session = MagicMock()
            ClientSession.return_value = session

            session.listTagged.side_effect = _koji_list_tagged
            session.getBuild.side_effect = _koji_get_build
            session.listTags.side_effect = _koji_list_tags

            return f(*args, **kwargs)

    return wrapper
