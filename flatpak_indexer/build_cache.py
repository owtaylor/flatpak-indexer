from typing import Dict

from .models import (
    ImageBuildModel, ModuleBuildModel, PackageBuildModel
)

from .koji_query import query_image_build, query_module_build, query_package_build
from .koji_utils import get_koji_session
from .redis_utils import get_redis_client


class BuildCache:
    image_builds: Dict[str, ImageBuildModel]
    module_builds: Dict[str, ModuleBuildModel]

    def __init__(self, global_config):
        self.koji_session = get_koji_session(global_config)
        self.redis_client = get_redis_client(global_config)
        self.image_builds = {}
        self.module_builds = {}
        self.package_builds = {}

    def get_image_build(self, nvr: str):
        image_build = self.image_builds.get(nvr)
        if image_build:
            return image_build

        image_build = query_image_build(self.koji_session, self.redis_client, nvr)
        self.image_builds[nvr] = image_build
        return image_build

    def get_module_build(self, nvr: str):
        module_build = self.module_builds.get(nvr)
        if module_build:
            return module_build

        module_build = query_module_build(self.koji_session, self.redis_client, nvr)
        self.module_builds[nvr] = module_build
        return module_build

    def get_package_build(self, nvr: str) -> PackageBuildModel:
        package_build = self.package_builds.get(nvr)
        if package_build:
            return package_build

        package_build = query_package_build(self.koji_session, self.redis_client, nvr)
        self.package_builds[nvr] = package_build
        return package_build
