from contextlib import contextmanager
from functools import partial, update_wrapper, wraps
import inspect
import os
import subprocess
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List
from unittest.mock import DEFAULT, MagicMock, patch

import yaml

from flatpak_indexer.config import Config


def write_config(tmp_path, content):
    tmpfile = NamedTemporaryFile(delete=False, prefix="config-", suffix=".yaml", dir=tmp_path,
                                 encoding="UTF-8", mode="w")
    yaml.dump(content, tmpfile)
    tmpfile.close()
    return tmpfile.name


def get_config(tmp_path, content):
    path = write_config(tmp_path, content)
    conf = Config.from_path(path)
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


_KOJI_BUILDS: List[Dict[str, Any]] = [
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
    },
    # An image, but not a container image
    {
        'build_id': 636564,
        'completion_ts': 1515068116.53497,
        'extra': None,
        'name': 'rhel-guest-image',
        'nvr': 'rhel-guest-image-8.0-1114',
        'owner_name': 'jdoe',
        'package_id': 42902,
        'source': None,
        '_TYPE': 'image',
        '_TAGS': ['release-candidate'],
        '_ARCHIVES': [
            {
                'extra': None,
                'filename': 'rhel-guest-image-8.0-1114.x86_64.qcow2',
                'id': 2217961,
            }
        ]
    }
]


_KOJI_TAGS: Dict[str, List[str]] = {
    'release-candidate': [],
    'release-candidate-2': [],
    'release-candidate-3': ['release-candidate', 'release-candidate-2'],
}


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


def _koji_list_tagged(tag, type, latest=False, inherit=False):
    assert latest is True
    assert type == 'image'

    if inherit:
        all_tags = set()

        def _add_tag(t):
            all_tags.add(t)
            for inherited_tag in _KOJI_TAGS[t]:
                _add_tag(inherited_tag)

        _add_tag(tag)
    else:
        all_tags = {tag}

    result = []
    for build in _KOJI_BUILDS:
        for t in all_tags:
            if t in build['_TAGS']:
                result.append({
                    'build_id': build['build_id'],
                    'nvr': build['nvr'],
                })
                continue

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
