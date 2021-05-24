from datetime import datetime
from functools import cached_property
from typing import Any, Dict, List, Optional

from .json_model import BaseModel, field
from .utils import parse_pull_spec


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
    os: str = field(json_name="OS")
    architecture: str
    labels: Dict[str, str]
    annotations: Dict[str, str]
    tags: List[str]

    diff_ids: List[str]

    # This is the place where the image was uploaded when built, which may differ
    # from the public location of the image.
    pull_spec: Optional[str]


class ListModel(BaseModel):
    digest: str
    media_type: str
    images: List[ImageModel]
    tags: List[str]


class RepositoryModel(BaseModel):
    name: str
    images: Dict[str, ImageModel] = field(index="digest")
    lists: Dict[str, ListModel] = field(index="digest")
    tag_histories: Dict[str, TagHistoryModel] = field(index="name")


class RegistryModel(BaseModel):
    repositories: Dict[str, RepositoryModel] = field(index="name")

    def add_image(self, name: str, image: ImageModel):
        if name not in self.repositories:
            self.repositories[name] = RepositoryModel(name=name)

        self.repositories[name].images[image.digest] = image


class KojiBuildModel(BaseModel):
    build_id: str
    nvr: str
    source: Optional[str]
    completion_time: datetime
    user_name: str


class ImageBuildModel(KojiBuildModel):
    images: List[ImageModel]

    @classmethod
    def class_from_json(cls, data: Dict[str, Any]):
        if 'ModuleBuilds' in data:
            return FlatpakBuildModel
        else:
            return ImageBuildModel

    @cached_property
    def repository(self):
        _, repository, _ = parse_pull_spec(self.images[0].pull_spec)
        return repository


class FlatpakBuildModel(ImageBuildModel):
    module_builds: List[str]
    package_builds: List[str]


class ModuleBuildModel(KojiBuildModel):
    modulemd: str

    package_builds: List[str]


class PackageBuildModel(KojiBuildModel):
    pass


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

    from_size: Optional[int]
    to_size: Optional[int]

    max_mem_kib: Optional[float] = field(json_name="MaxMemKiB")
    elapsed_time_s: Optional[float]
    user_time_s: Optional[float]
    system_time_s: Optional[float]
