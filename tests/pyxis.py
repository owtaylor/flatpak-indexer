from contextlib import contextmanager
import json
import re
import responses
from urllib.parse import parse_qs, urlparse

from .utils import _AISLERIOT_LABELS, _AISLERIOT2_LABELS, WithArgDecorator


def _pyxis_labels(labels):
    return [
        {
            'name': key,
            'value': value
        }
        for key, value in labels.items()
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
_GET_IMAGES_NVR_RE = re.compile(
    r'^https://pyxis.example.com/v1/images/nvr/([A-Za-z0-9_.-]+)')
_GET_REPOSITORIES_RE = re.compile(
    r'^https://pyxis.example.com/v1/repositories(\?|$)')
_GET_TAG_HISTORY_RE = re.compile(
    r'^https://pyxis.example.com/' +
    r'v1/tag-history/registry/([A-Za-z0-9.]+)/repository/([A-Za-z0-9.]+)/tag/([A-Za-z0-9.]+)')


class MockPyxis:
    def __init__(self, fail_tag_history=False):
        self.fail_tag_history = fail_tag_history

    def get_images(self, request):
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

    def get_images_nvr(self, request):
        parsed = urlparse(request.url)
        params = parse_qs(parsed.query)

        m = _GET_IMAGES_NVR_RE.match('https://pyxis.example.com' + parsed.path)
        assert m is not None
        nvr = m.group(1)

        images = [i for i in _REPO_IMAGES if i['brew']['build'] == nvr]

        return _paged_result(params, images)

    def get_repositories(self, request):
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

    def get_tag_history(self, request):
        if self.fail_tag_history:
            return (403, {}, json.dumps({
                "detail": "No tag histories for you.",
                "status": 403,
                "title": "Forbidden",
                "type": "about:blank"
            }))

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


@contextmanager
def _setup_pyxis(**kwargs):
    with responses._default_mock:
        pyxis_mock = MockPyxis(**kwargs)

        responses.add_callback(responses.GET,
                               _GET_IMAGES_RE,
                               callback=pyxis_mock.get_images,
                               content_type='application/json',
                               match_querystring=False)
        responses.add_callback(responses.GET,
                               _GET_IMAGES_NVR_RE,
                               callback=pyxis_mock.get_images_nvr,
                               content_type='application/json',
                               match_querystring=False)
        responses.add_callback(responses.GET,
                               _GET_REPOSITORIES_RE,
                               callback=pyxis_mock.get_repositories,
                               content_type='application/json',
                               match_querystring=False)
        responses.add_callback(responses.GET,
                               _GET_TAG_HISTORY_RE,
                               callback=pyxis_mock.get_tag_history,
                               content_type='application/json',
                               match_querystring=False)

        yield pyxis_mock


mock_pyxis = WithArgDecorator('pyxis_mock', _setup_pyxis)
