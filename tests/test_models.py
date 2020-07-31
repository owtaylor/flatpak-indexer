from flatpak_indexer.models import ImageModel, RegistryModel


IMAGE1 = {
    "Annotations": {"key1": "value1"},
    "Architecture": "amd64",
    "Digest": "sha256:baabaa",
    "Labels": {"key2": "value2"},
    "MediaType": "application/vnd.docker.distribution.manifest.v2+json",
    "OS": "linux",
    "Tags": ["tag1"]
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


def test_registry_model():
    model = RegistryModel.from_json(REGISTRY)
    json = model.to_json()

    assert json == REGISTRY


def test_registry_model_add_image():
    model = RegistryModel.from_json(REGISTRY)

    image = ImageModel.from_json(IMAGE1)
    model.add_image('aisleriot2', image)

    assert model.repositories['aisleriot2'].images[image.digest] == image
