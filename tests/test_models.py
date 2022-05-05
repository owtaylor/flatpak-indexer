from copy import deepcopy

from flatpak_indexer.models import (
    FlatpakBuildModel,
    ImageBuildModel,
    ImageModel,
    RegistryModel
)

IMAGE1 = {
    "Annotations": {"key1": "value1"},
    "Architecture": "amd64",
    "Digest": "sha256:baabaa",
    "Labels": {
        "com.redhat.component": "baobob",
        "version": "master",
        "release": "3220200331145937.2"
    },
    "MediaType": "application/vnd.docker.distribution.manifest.v2+json",
    "OS": "linux",
    "Tags": ["tag1"],
    'PullSpec': 'candidate-registry.fedoraproject.org/baobab@sha256:12345'
}

IMAGE2 = {
    "Annotations": {"key1": "value1"},
    "Architecture": "ppc64le",
    "Digest": "sha256:beebee",
    "Labels": {"key2": "value2"},
    "MediaType": "application/vnd.docker.distribution.manifest.v2+json",
    "OS": "linux",
    "Tags": ["tag2"]
}

LIST1 = {
    "Digest": "sha256:booboo",
    "MediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
    "Images": [IMAGE1, IMAGE2],
    "Tags": ["latest"],
}

REGISTRY = {
    "Repositories": [
        {
            "Name": "aisleriot",
            "Images": [
                IMAGE1,
                IMAGE2,
            ],
            "Lists": [
                LIST1
            ],
        }
    ]
}

IMAGE_BUILD = {
    'BuildId': 12345,
    'Nvr': 'testrepo-1.2.3-1',
    'Source': 'git://src.fedoraproject.org/flatpaks/baobab#BAOBAB_GIT_DIGEST',
    'CompletionTime': '2020-07-31T16:26:22+00:00',
    'UserName': 'jdoe',
    'Images': [IMAGE1]
}

FLATPAK_BUILD = {
    'BuildId': 12345,
    'Nvr': 'testrepo-1.2.3-1',
    'Source': 'git://src.fedoraproject.org/flatpaks/baobab#BAOBAB_GIT_DIGEST',
    'CompletionTime': '2020-07-31T16:26:22+00:00',
    'UserName': 'jdoe',
    'Images': [IMAGE1],
    'ModuleBuilds': ['baobab-1.2.3-3020190603102507'],
    'PackageBuilds': [{'Nvr': 'baobab-1.2.3-1', 'SourceNvr': 'baobab-1.2.3-1'}],
}


def test_registry_model():
    model = RegistryModel.from_json(REGISTRY)
    json = model.to_json()

    assert json == REGISTRY


def test_registry_model_add_image():
    model = RegistryModel.from_json(REGISTRY)

    image = ImageModel.from_json(IMAGE1)
    model.add_image('aisleriot2', image)

    assert model.repositories['aisleriot2'].images[image.digest] == image


def test_image_nvr():
    image = ImageModel.from_json(IMAGE1)

    assert image.nvr == 'baobob-master-3220200331145937.2'

    image.labels = {}
    assert image.nvr is None


def test_image_build_repository():
    image = ImageBuildModel.from_json(IMAGE_BUILD)
    assert image.repository == 'baobab'


def test_image_build_from_json():
    image = ImageBuildModel.from_json(IMAGE_BUILD)
    assert isinstance(image, ImageBuildModel)

    flatpak = ImageBuildModel.from_json(FLATPAK_BUILD)
    assert isinstance(flatpak, FlatpakBuildModel)


def test_koji_build_model_is_json_current():
    assert FlatpakBuildModel.from_json(FLATPAK_BUILD, check_current=True) is not None

    json = deepcopy(FLATPAK_BUILD)
    json["PackageBuilds"] = [pb["Nvr"] for pb in json["PackageBuilds"]]
    assert FlatpakBuildModel.from_json(json, check_current=True) is None
