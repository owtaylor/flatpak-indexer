from datetime import datetime
from typing import Dict, List

from .json_model import BaseModel, IndexedList, Rename


class TagHistoryItemModel(BaseModel):
    architecture: str
    date: datetime
    digest: str


class TagHistoryModel(BaseModel):
    name: str
    items: List[TagHistoryItemModel]


class ImageModel(BaseModel):
    digest: str
    media_type: str
    os: Rename[str, "OS"]  # noqa: F821
    architecture: str
    labels: Dict[str, str]
    annotations: Dict[str, str]
    tags: List[str]

    diff_ids: List[str]


class ListModel(BaseModel):
    digest: str
    media_type: str
    images: List[ImageModel]
    tags: List[str]


class RepositoryModel(BaseModel):
    name: str
    images: IndexedList[ImageModel, "digest"]  # noqa: F821
    lists: IndexedList[ListModel, "digest"]  # noqa: F821
    tag_histories: IndexedList[TagHistoryModel, "name"]  # noqa: F821


class RegistryModel(BaseModel):
    repositories: IndexedList[RepositoryModel, "name"]  # noqa: F821

    def add_image(self, name, image):
        if name not in self.repositories:
            self.repositories[name] = RepositoryModel(name=name)

        self.repositories[name].images[image.digest] = image


class TardiffImageModel(BaseModel):
    registry: str
    repository: str
    ref: str


class TardiffSpecModel(BaseModel):
    from_image: TardiffImageModel
    from_diff_id: str
    to_image: TardiffImageModel
    to_diff_id: str


class TardiffResultModel(BaseModel):
    status: str
    digest: str
    size: int
    message: str
