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

    @property
    def nvr(self):
        name = self.labels.get("com.redhat.component")
        if name:
            version = self.labels["version"]
            release = self.labels["release"]
            return f"{name}-{version}-{release}"
        else:
            return None


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


class BodhiUpdateModel(BaseModel):
    update_id: str
    release_name: str
    release_branch: str
    status: str
    type: str
    date_submitted: datetime
    date_testing: Optional[datetime]
    date_stable: Optional[datetime]
    user_name: str
    builds: List[str]


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


class ModuleImageContentsModel(BaseModel):
    image_nvr: str
    module_nvr: str
    package_builds: List[str]


class ModuleStreamContentsModel(BaseModel):
    images: Dict[str, ModuleImageContentsModel] = field(index="image_nvr")

    def add_package_build(self, image_nvr: str, module_nvr: str, package_nvr: str):
        if image_nvr not in self.images:
            self.images[image_nvr] = ModuleImageContentsModel(image_nvr=image_nvr,
                                                              module_nvr=module_nvr)
        self.images[image_nvr].package_builds.append(package_nvr)
