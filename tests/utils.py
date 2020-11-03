from contextlib import contextmanager
from functools import partial, update_wrapper, wraps
import inspect
import json
import os
import re
import subprocess
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


_AISLERIOT_LABELS = {
    "org.flatpak.ref": "app/org.gnome.Aisleriot/x86_64/stable",
    "org.freedesktop.appstream.icon-64": "data:image/png;base64," + _TEST_ICON_DATA,
    "org.freedesktop.appstream.icon-128": "https://www.example.com/icons/aisleriot.png",
}


_AISLERIOT2_LABELS = {
    "org.flatpak.ref": "app/org.gnome.Aisleriot2/x86_64/stable",
    "org.freedesktop.appstream.icon-64": "data:image/png;base64," + _TEST_ICON_DATA,
}


def _pyxis_labels(labels):
    return [
        {
            'name': key,
            'value': value
        }
        for key, value in labels.items()
    ]


_KOJI_BUILDS = [
    {
        'build_id': 1063042,
        'completion_ts': 1598464962.42521,
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
                },
                'modules': ['aisleriot-el8-8020020200121102609'],
            }
        },
        'name': 'aisleriot-container',
        'nvr': 'aisleriot-container-el8-8020020200121102609.1',
        'owner_name': 'jdoe',
        'package_id': 22,
        'source': 'git://pkgs.devel.redhat.com/containers/aisleriot#AISLERIOT_GIT_DIGEST',
        '_TYPE': 'image',
        '_TAGS': ['release-candidate'],
        '_ARCHIVES': [
            {
                'extra': {
                    'docker': {
                        'config': {
                            'architecture': 'amd64',
                            'config': {
                                'Labels': _AISLERIOT_LABELS,
                            },
                            'os': 'linux',
                            'rootfs': {
                                'diff_ids': ['sha256:5a1ad']
                            },
                        },
                        'digests': {
                            'application/vnd.docker.distribution.manifest.v2+json':
                            'sha256:' +
                                'bo1dfacec4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb',
                        },
                        'repositories': [
                            'registry-proxy.engineering.redhat/rh-osbs/aisleriot:build-1234-x86_64'
                        ]
                    },
                    'image': {
                        'arch': 'x86_64',
                    }
                },
                'id': 15321,
            }
        ]
    },
    {
        'build_id': 1063052,
        'completion_ts': 1598465962.42521,
        'extra': {
            'image': {
                'flatpak': True,
                'index': {
                    'digests': {'application/vnd.docker.distribution.manifest.list.v2+json':
                                'sha256:' +
                                'b0b51edaf5db2f38650970c2ce0f2897b6e552e5b7e67adfb53ab51243b5f5f5'},

                    'floating_tags': ['latest', 'el8'],
                    'pull': [
                        'registry-proxy.engineering.redhat.com/rh-osbs/aisleriot@' +
                        'sha256:b0b51edaf5db2f38650970c2ce0f2897b6e552e5b7e67adfb53ab51243b5f5f5',
                        'registry-proxy.engineering.redhat.com/rh-osbs/aisleriot:' +
                        'el8-8020020200121102609.1'],
                    'tags': ['el8-8020020200121102609.1'],
                },
                'modules': ['aisleriot-el8-8020020200121102609'],
            }
        },
        'name': 'aisleriot-container',
        'nvr': 'aisleriot-container-el8-8020020200121102609.2',
        'owner_name': 'jdoe',
        'package_id': 22,
        'source': 'git://pkgs.devel.redhat.com/containers/aisleriot#AISLERIOT_GIT_DIGEST',
        '_TYPE': 'image',
        '_TAGS': ['release-candidate-2'],
        '_ARCHIVES': [
            {
                'extra': {
                    'docker': {
                        'config': {
                            'architecture': 'amd64',
                            'config': {
                                'Labels': _AISLERIOT_LABELS,
                            },
                            'os': 'linux',
                            'rootfs': {
                                'diff_ids': ['sha256:5a1ad']
                            },
                        },
                        'digests': {
                            'application/vnd.docker.distribution.manifest.v2+json':
                            'sha256:' +
                                'fade1e55c4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb',
                        },
                        'repositories': [
                            'registry-proxy.engineering.redhat/rh-osbs/aisleriot:build-1235-x86_64'
                        ]
                    },
                    'image': {
                        'arch': 'x86_64',
                    }
                },
                'id': 15322,
            }
        ]
    },
    {
        'build_id': 54321,
        'completion_ts': 1598464000.,
        'extra': {
            'typeinfo': {
                'module': {
                    'modulemd_str': 'xxxxx',
                },
            }
        },
        'name': 'aisleriot',
        'nvr': 'aisleriot-el8-8020020200121102609.73699f59',
        'owner_name': 'jdoe',
        'source': 'git://pkgs.devel.redhat.com/modules/aisleriot#AISLERIOT_MODULE_DIGEST',
        'package_id': 21,
        '_TYPE': 'module',
        '_TAGS': [],
        '_ARCHIVES': [
            {
                'filename': 'modulemd.txt',
                'id': 1001,
            },
        ],
    },
    {
        'build_id': 1063043,
        'completion_ts': 1598465000.0,
        'extra': {
            'image': {
                'flatpak': True,
                'index': {
                    'digests': {'application/vnd.docker.distribution.manifest.list.v2+json':
                                'sha256:' +
                                'AISLERIOT2_DIGEST'},

                    'floating_tags': ['latest', 'el8'],
                    'pull': [
                        'registry-proxy.engineering.redhat.com/rh-osbs/aisleriot2@' +
                        'sha256:AISLERIOT2_DIGEST',
                        'registry-proxy.engineering.redhat.com/rh-osbs/aisleriot2:' +
                        'el8-8020020200121102609.1'],
                    'tags': ['el8-8020020200121102609.1'],
                },
                'modules': ['aisleriot-el8-8020020200121102609'],
            }
        },
        'name': 'aisleriot2-container',
        'nvr': 'aisleriot2-container-el8-8020020200121102609.1',
        'owner_name': 'jdoe',
        'package_id': 23,
        'source': 'git://pkgs.devel.redhat.com/containers/aisleriot2#AISLERIOT2_GIT_DIGEST',
        '_TYPE': 'image',
        '_TAGS': [],
        '_ARCHIVES': [
            {
                'extra': {
                    'docker': {
                        'config': {
                            'architecture': 'amd64',
                            'config': {
                                'Labels': _AISLERIOT2_LABELS,
                            },
                            'os': 'linux',
                            'rootfs': {
                                'diff_ids': ['sha256:5a1ad']
                            },
                        },
                        'digests': {
                            'application/vnd.docker.distribution.manifest.v2+json':
                            'sha256:' +
                                '5eaf00d1c4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb',
                        },
                        'repositories': [
                            'registry-proxy.engineering.redhat/rh-osbs/aisleriot2:build-3456-x86_64'
                        ]
                    },
                    'image': {
                        'arch': 'x86_64',
                    }
                },
                'id': 16321,
            }
        ]
    },
    {
        'build_id': 1063044,
        'completion_ts': 1598466000.0,
        'extra': {
            'image': {
                'index': {
                    'digests': {'application/vnd.docker.distribution.manifest.list.v2+json':
                                'sha256:TESTREPO_DIGEST'},

                    'floating_tags': ['latest', 'el8'],
                    'pull': [
                        'registry-proxy.engineering.redhat.com/rh-osbs/testrepo@' +
                        'sha256:TESTREPO_DIGEST',
                        'registry-proxy.engineering.redhat.com/rh-osbs/testrepo:' +
                        '1.2.3-1'],
                    'tags': ['1.2.3-1'],
                }
            }
        },
        'name': 'testrepo-container',
        'nvr': 'testrepo-container-1.2.3-1',
        'owner_name': 'jdoe',
        'package_id': 24,
        'source': 'git://pkgs.devel.redhat.com/containers/testrepo#TESTREPO_GIT_DIGEST',
        '_TYPE': 'image',
        '_TAGS': [],
        '_ARCHIVES': [
            {
                'extra': {
                    'docker': {
                        'config': {
                            'architecture': 'amd64',
                            'config': {
                            },
                            'os': 'linux',
                            'rootfs': {
                                'diff_ids': ['sha256:5a1ad']
                            },
                        },
                        'digests': {
                            'application/vnd.docker.distribution.manifest.v2+json':
                            'sha256:' +
                                'babb1ed1c4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb',
                        },
                        'repositories': [
                            'registry-proxy.engineering.redhat/rh-osbs/testrepo:build-6789-x86_64'
                        ]
                    },
                    'image': {
                        'arch': 'x86_64',
                    }
                },
                'id': 17321,
            },
            {
                'extra': {
                    'docker': {
                        'config': {
                            'architecture': 'ppc64le',
                            'config': {
                            },
                            'os': 'linux',
                            'rootfs': {
                                'diff_ids': ['sha256:5a1ad']
                            },
                        },
                        'digests': {
                            'application/vnd.docker.distribution.manifest.v2+json':
                            'sha256:' +
                                'fl055ed1c4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb',
                        },
                        'repositories': [
                            'registry-proxy.engineering.redhat/rh-osbs/testrepo:build-6789-ppc64le'
                        ]
                    },
                    'image': {
                        'arch': 'ppc64le',
                    }
                },
                'id': 17322,
            },
        ]
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
    },
    {
        'registry': 'registry.example.com',
        'repository': 'aisleriot3',
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
            'labels': _pyxis_labels(_AISLERIOT_LABELS)
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
            'labels': _pyxis_labels(_AISLERIOT2_LABELS),
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


_TAG_HISTORIES = [
    {
        "history": [
            {
                "brew_build": "aisleriot-container-el8-8020020200121102609.1",
                "start_date": "2020-07-23T19:30:04+00:00"
            },
        ],
        "registry": "registry.example.com",
        "repository": "aisleriot",
        "tag": "latest",
    },
    {
        "history": [
            {
                "brew_build": "aisleriot2-container-el8-8020020200121102609.1",
                "start_date": "2020-07-23T19:30:04+00:00"
            },
        ],
        "registry": "registry.example.com",
        "repository": "aisleriot2",
        "tag": "latest",
    },
    {
        "history": [
            {
                "brew_build": "testrepo-container-1.2.3-1",
                "start_date": "2020-07-23T19:30:04+00:00"
            },
        ],
        "registry": "registry.example.com",
        "repository": "testrepo",
        "tag": "latest",
    },
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


_GET_TAG_HISTORY_RE = re.compile(
    r'^https://pyxis.example.com/' +
    r'v1/tag-history/registry/([A-Za-z0-9.]+)/repository/([A-Za-z0-9.]+)/tag/([A-Za-z0-9.]+)')


def _get_tag_history(request):
    parsed = urlparse(request.url)

    m = _GET_TAG_HISTORY_RE.match('https://pyxis.example.com' + parsed.path)
    assert m is not None

    for tag_history in _TAG_HISTORIES:
        if (tag_history['registry'] == m.group(1) and
                tag_history['repository'] == m.group(2) and
                tag_history['tag'] == m.group(3)):
            return (200, {}, json.dumps(tag_history))

    return (404, {}, json.dumps({
        "detail": "The requested URL was not found on the server.",
        "status": 404,
        "title": "Not Found",
        "type": "about:blank"
    }))


def mock_pyxis(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        with responses._default_mock:
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
            responses.add_callback(responses.GET,
                                   _GET_TAG_HISTORY_RE,
                                   callback=_get_tag_history,
                                   content_type='application/json',
                                   match_querystring=False)

            return f(*args, **kwargs)

    return wrapper


def _koji_get_build(build_id):
    if isinstance(build_id, int):
        key = 'build_id'
    else:
        key = 'nvr'

    for build in _KOJI_BUILDS:
        if build[key] == build_id:
            return build

    raise RuntimeError("Build {} not found".format(build_id))


def _koji_get_package_id(name):
    for build in _KOJI_BUILDS:
        if build['name'] == name:
            return build['package_id']

    raise RuntimeError("Package {} not found".format(name))


def _koji_list_archives(build_id):
    for build in _KOJI_BUILDS:
        if build['build_id'] == build_id:
            return build['_ARCHIVES']

    raise RuntimeError("Build {} not found".format(build_id))


def _koji_list_builds(build_id, type=None):
    result = []
    for build in _KOJI_BUILDS:
        if type is None or build['_TYPE'] == type:
            result.append(build)

    return result


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


def _koji_list_tags(build_id):
    for build in _KOJI_BUILDS:
        if build['build_id'] == build_id:
            return [{'name': t} for t in build['_TAGS']]

    raise RuntimeError("Build {} not found".format(build_id))


def mock_brew(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        with patch.multiple('koji',
                            read_config=DEFAULT,
                            grab_session_options=DEFAULT,
                            ClientSession=DEFAULT) as mocks:
            ClientSession = mocks['ClientSession']

            session = MagicMock()
            ClientSession.return_value = session

            session.getBuild.side_effect = _koji_get_build
            session.getPackageID.side_effect = _koji_get_package_id
            session.listArchives.side_effect = _koji_list_archives
            session.listBuilds.side_effect = _koji_list_builds
            session.listTagged.side_effect = _koji_list_tagged
            session.listTags.side_effect = _koji_list_tags

            return f(*args, **kwargs)

    return wrapper


class WithArgDecorator:
    """
    A impenetrable piece of metaprogramming to easily define a decorator that does
    some setup/teardown around a test case, and optionally passes a named argument
    into the test case.
    """

    def __init__(self, arg_name, setup):
        self.arg_name = arg_name
        self.setup = setup

    def __call__(self, f=None, **target_kwargs):
        if f is None:
            # Handle arguments to the decorator: when called with only kwargs, return a function
            # that when called wth single function argument, invokes this function
            # including the function *and* target_kwargs
            return partial(self, **target_kwargs)

        sig = inspect.signature(f)
        need_arg = self.arg_name in sig.parameters

        def wrapper(*args, **kwargs):
            with self.setup(**target_kwargs) as arg_object:
                if need_arg:
                    kwargs[self.arg_name] = arg_object

                return f(*args, **kwargs)

        update_wrapper(wrapper, f)

        if need_arg:
            # We need the computed signature of the final function to not include the
            # extra argument, since pytest will think it's a fixture.
            # We remove the extra from the function we return using functools.partial.
            #
            # functools.update_wrapper does things we need, like updating __dict__ with
            # the pytest marks from the original function. But it also sets result.__wrapped__
            # to point back to the original function, and this results in inspect.signature
            # using the original function for the signature, bringing back the extra
            # argument.

            result = partial(wrapper, **{self.arg_name: None})
            update_wrapper(result, wrapper)
            del result.__dict__['__wrapped__']

            return result
        else:
            return wrapper


class ImpatientPopen(subprocess.Popen):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._orig_wait = self.wait
        self.wait = MagicMock(wraps=self.wait)
        self.wait.side_effect = self.fail_first_wait

    def fail_first_wait(self, timeout=None):
        if timeout is not None:
            self.wait.side_effect = None
            raise subprocess.TimeoutExpired(str(self.args), timeout)
        else:
            self._orig_wait()


@contextmanager
def timeout_first_popen_wait():
    with patch('subprocess.Popen', side_effect=ImpatientPopen):
        yield
