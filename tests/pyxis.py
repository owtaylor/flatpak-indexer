from contextlib import contextmanager
import copy
from dataclasses import dataclass
from datetime import datetime
import json
import re
from typing import List, Optional

import graphql
import responses

from flatpak_indexer.test.decorators import WithArgDecorator


@dataclass
class Repository:
    registry: str
    repository: str
    build_categories: List[str]
    eol_date: Optional[datetime] = None


@dataclass
class Brew:
    build: Optional[str]


@dataclass
class ContainerImageRepoTag:
    name: str


@dataclass
class ContainerImageRepo:
    push_date: str
    registry: str
    repository: str
    tags: Optional[List[ContainerImageRepoTag]]


@dataclass
class ContainerImage:
    architecture: str
    image_id: str
    brew: Brew
    repositories: List[ContainerImageRepo]


@dataclass
class GetRepositories:
    data: List[Repository]
    total: int


@dataclass
class GetImagesForRepository:
    data: List[ContainerImage]
    total: int


_REPOSITORIES = [
    Repository(registry='registry2.example.com',
               repository='testrepo',
               build_categories=['Standalone Image']),
    Repository(registry='registry.example.com',
               repository='testrepo',
               build_categories=['Standalone Image']),
    Repository(registry='registry.example.com',
               repository='el8/aisleriot',
               build_categories=['Flatpak']),
    Repository(registry='registry.example.com',
               repository='el9/aisleriot',
               build_categories=['Flatpak']),
    Repository(registry='registry.example.com',
               repository='aisleriot2',
               build_categories=['Flatpak']),
    Repository(registry='registry.example.com',
               repository='aisleriot3',
               build_categories=['Flatpak']),
]


_REPO_IMAGES = [
    ContainerImage(
        architecture='amd64',
        brew=Brew(
            build='testrepo-container-1.2.3-1'
        ),
        image_id='sha256:babb1ed1c4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb',
        repositories=[
            ContainerImageRepo(
                registry='registry2.example.com',
                repository='testrepo',
                push_date='2019-04-25T18:50:02.708000+00:00',
                tags=[]
            ),
            ContainerImageRepo(
                registry='registry.example.com',
                repository='testrepo',
                push_date='2019-04-25T18:50:02.708000+00:00',
                tags=[
                    ContainerImageRepoTag(name='latest')
                ]
            ),
        ]
    ),
    ContainerImage(
        architecture='amd64',
        brew=Brew(
            build='aisleriot-container-el8-8020020200121102609.1'
        ),
        image_id='sha256:bo1dfacec4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb',
        repositories=[
            ContainerImageRepo(
                registry='registry.example.com',
                repository='el8/aisleriot',
                push_date='2019-04-25T18:50:02.708000+00:00',
                tags=[
                    ContainerImageRepoTag(name='latest'),
                    ContainerImageRepoTag(name='rhel8')
                ]
            )
        ]
    ),
    ContainerImage(
        architecture='amd64',
        brew=Brew(
            build='aisleriot-container-el9-9010020220121102609.1'
        ),
        image_id='sha256:ba5eba11c4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb',
        repositories=[
            ContainerImageRepo(
                registry='registry.example.com',
                repository='el9/aisleriot',
                push_date='2019-04-25T18:50:02.708000+00:00',
                tags=[
                    ContainerImageRepoTag(name='latest')
                ]
            )
        ]
    ),
    ContainerImage(
        architecture='amd64',
        brew=Brew(
            build='aisleriot-container-el9-9010020220121102609.2'
        ),
        image_id='sha256:AISLERIOT_EL9_2_MANIFEST_DIGEST',
        repositories=[
            ContainerImageRepo(
                registry='registry.example.com',
                repository='el9/aisleriot',
                push_date='2019-04-25T18:50:02.708000+00:00',
                tags=None
            )
        ]
    ),
    ContainerImage(
        architecture='amd64',
        brew=Brew(
            build='aisleriot2-container-el8-8020020200121102609.1',
        ),
        image_id='sha256:5eaf00d1c4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb',
        repositories=[
            ContainerImageRepo(
                registry='registry.example.com',
                repository='aisleriot2',
                push_date='2019-04-25T18:50:02.708000+00:00',
                tags=[
                    ContainerImageRepoTag(name='latest'),
                    ContainerImageRepoTag(name='rhel8')
                ]
            )
        ]
    ),
    ContainerImage(
        architecture='ppc64le',
        brew=Brew(
            build='testrepo-container-1.2.3-1',
        ),
        image_id='sha256:fl055ed1c4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb',
        repositories=[
            ContainerImageRepo(
                registry='registry.example.com',
                repository='testrepo',
                push_date='2019-04-25T18:50:02.708000+00:00',
                tags=[
                    ContainerImageRepoTag(name='latest')
                ]
            )
        ]
    )
]


_NEWER_UNTAGGED_IMAGE = ContainerImage(
        architecture='amd64',
        brew=Brew(
            build='aisleriot-container-el8-8020020200121102609.42'
        ),
        image_id='sha256:bo1dfacec4d226da18ec4a6386263d8b2125fc874c8b4f4f97b31593037ea0bb',
        repositories=[
            ContainerImageRepo(
                registry='registry.example.com',
                repository='el8/aisleriot',
                push_date='2024-04-25T18:50:02.708000+00:00',
                tags=[]
            )
        ]
    )


schema = graphql.utilities.build_schema("""
type Repository {
    registry: String
    repository: String
    build_categories: [String]
    eol_date: String
}

type Brew {
    build: String
}

type ContainerImageRepoTag {
    name: String
}

type ContainerImageRepo {
    push_date: String
    registry: String
    repository: String
    tags: [ContainerImageRepoTag]
}

type ContainerImage {
    architecture: String
    image_id: String
    brew: Brew
    repositories: [ContainerImageRepo]
}

type ResponseError {
    status: Int
    detail: String
}

type GetRepositories {
    error: ResponseError
    data: [Repository]
    total: Int
}

type GetImagesForRepository {
    error: ResponseError
    data: [ContainerImage]
    total: Int
}

input RepositoryFilterBuildCategories {
    in: [String]
}

input RepositoryFilterEolDate {
    gt: String
    eq: String
}

input RepositoryFilter {
    and: [RepositoryFilter]
    or: [RepositoryFilter]
    build_categories: RepositoryFilterBuildCategories
    eol_date: RepositoryFilterEolDate
}

type Query {
   find_repositories(filter: RepositoryFilter, page: Int, page_size: Int): GetRepositories
   find_repository_images_by_registry_path(registry: String, repository: String,
                                           page: Int, page_size: Int): GetImagesForRepository
}
""")


class User:
    def __init__(self, id, name):
        self.id = id
        self.name = name


def paginate(results, page, page_size):
    return results[page * page_size:page * page_size + page_size]


class MockPyxis:
    def __init__(self, bad_digests=False, newer_untagged_image=False, no_brew_builds=False):
        self.repositories = _REPOSITORIES
        self.repo_images = _REPO_IMAGES

        if bad_digests:
            self.repo_images = copy.deepcopy(self.repo_images)
            for ci in self.repo_images:
                ci.image_id = 'sha256:deadbeef'

        if newer_untagged_image:
            self.repo_images = copy.copy(self.repo_images)
            self.repo_images.append(_NEWER_UNTAGGED_IMAGE)

        if no_brew_builds:
            self.repo_images = copy.deepcopy(self.repo_images)
            for image in self.repo_images:
                image.brew.build = None

    def graphql(self, request):
        parsed = json.loads(request.body)
        result = graphql.graphql_sync(schema, parsed["query"], self,
                                      variable_values=parsed["variables"])
        if result.errors:
            return (400, {}, json.dumps({
                'errors': [
                    {
                        'message': e.message,
                        'locations': [
                            {
                                "line": loc.line,
                                "column": loc.column,
                            }
                            for loc in e.locations or []
                        ]
                    } for e in result.errors
                ]
            }))
        else:
            return (200, {}, json.dumps({
                'data': result.data
            }))

    def find_repositories(self, info, page: int = 0, page_size: int = 50, filter=None):
        EXPECTED_FILTER = {
            "and": [
                {
                    "or": [
                        {
                            "eol_date": {
                                "eq": None
                            }
                        },
                        {
                            "eol_date": {
                                "gt": "DATE"
                            }
                        }
                    ]
                },
                {
                    "build_categories": {
                        "in": [
                            "Flatpak"
                        ]
                    }
                }
            ]
        }
        expected_filter_regexp = re.escape(json.dumps(EXPECTED_FILTER)).replace("DATE", "([^\"]+)")

        if filter is not None:
            m = re.match(expected_filter_regexp, json.dumps(filter))
            assert m
            eol_date = datetime.fromisoformat(m.group(1))
            data = [
                r for r in self.repositories
                if (r.eol_date is None or r.eol_date > eol_date)
                and "Flatpak" in r.build_categories
            ]
        else:
            data = self.repositories

        return GetRepositories(data=paginate(data, page, page_size), total=len(data))

    def find_repository_images_by_registry_path(self, info, registry: str, repository: str,
                                                page: int = 0, page_size: int = 50):
        data = [ci for ci in self.repo_images
                if any(r.registry == registry and r.repository == repository
                       for r in ci.repositories)]

        return GetImagesForRepository(data=paginate(data, page, page_size), total=len(data))


@contextmanager
def _setup_pyxis(**kwargs):
    with responses._default_mock:
        pyxis_mock = MockPyxis(**kwargs)

        responses.add_callback(responses.POST,
                               "https://pyxis.example.com/graphql/",
                               callback=pyxis_mock.graphql,
                               content_type='application/json')

        yield pyxis_mock


mock_pyxis = WithArgDecorator('pyxis_mock', _setup_pyxis)
