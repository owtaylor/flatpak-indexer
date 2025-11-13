from datetime import datetime
from functools import cached_property
from typing import Any, Dict, List, Optional

from .json_model import BaseModel, field
from .nvr import NVR
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

    # This allows marking an Image that we can't look it up in Koji by nvr.
    # Making nvr a stored property would be simpler and cleaner, but
    # causes a migration problem with previously cached Images and also
    # would require some more formal hide-from-output mechanism - the hack
    # in indexer doesn't work because indexer is using image.nvr.
    @property
    def no_koji(self):
        return getattr(self, "_no_koji", False)

    @no_koji.setter
    def no_koji(self, no_koji: bool):
        self._no_koji = no_koji

    @property
    def nvr(self):
        if self.no_koji:
            return None
        else:
            name = self.labels.get("com.redhat.component")
            if name:
                version = self.labels["version"]
                release = self.labels["release"]
                return NVR(f"{name}-{version}-{release}")
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
    nvr: NVR
    source: Optional[str]
    completion_time: datetime
    user_name: str

    @classmethod
    def check_json_current(cls, data):
        # For both ImageBuildModule and ModuleBuildModule
        # PackageBuilds changed from <nvr> to { Nvr: <nvr>, SourceNvr: <nvr> }
        package_builds = data.get("PackageBuilds")
        if package_builds:
            if not isinstance(package_builds[0], dict):
                return False

        return True


class ImageBuildModel(KojiBuildModel):
    images: List[ImageModel]

    @classmethod
    def class_from_json(cls, data: Dict[str, Any]):
        if "ModuleBuilds" in data:
            return FlatpakBuildModel
        else:
            return ImageBuildModel

    @cached_property
    def repository(self):
        _, repository, _ = parse_pull_spec(self.images[0].pull_spec)
        return repository


class BinaryPackage(BaseModel):
    nvr: NVR
    source_nvr: NVR


class FlatpakBuildModel(ImageBuildModel):
    module_builds: List[NVR]
    package_builds: List[BinaryPackage]


class ModuleBuildModel(KojiBuildModel):
    modulemd: str

    package_builds: List[BinaryPackage]


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
    builds: List[NVR]


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
    image_nvr: NVR
    module_nvr: NVR
    package_builds: List[BinaryPackage]


class ModuleStreamContentsModel(BaseModel):
    images: Dict[str, ModuleImageContentsModel] = field(index="image_nvr")

    def add_package_build(self, image_nvr: str, module_nvr: str, binary_package: BinaryPackage):
        if image_nvr not in self.images:
            self.images[image_nvr] = ModuleImageContentsModel(
                image_nvr=image_nvr, module_nvr=module_nvr
            )
        self.images[image_nvr].package_builds.append(binary_package)
