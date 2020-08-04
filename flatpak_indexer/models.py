from typing import Dict, List

from .json_model import BaseModel, IndexedList, Rename


class ImageModel(BaseModel):
    digest: str
    media_type: str
    os: Rename[str, "OS"]  # noqa: F821
    architecture: str
    labels: Dict[str, str]
    annotations: Dict[str, str]
    tags: List[str]


class ListModel(BaseModel):
    digest: str
    media_type: str
    images: List[ImageModel]
    tags: List[str]


class RepositoryModel(BaseModel):
    name: str
    images: IndexedList[ImageModel, "digest"]  # noqa: F821
    lists: IndexedList[ListModel, "digest"]  # noqa: F821


class RegistryModel(BaseModel):
    repositories: IndexedList[RepositoryModel, "name"]  # noqa: F821

    def add_image(self, name, image):
        if name not in self.repositories:
            self.repositories[name] = RepositoryModel(name=name)

        self.repositories[name].images[image.digest] = image
