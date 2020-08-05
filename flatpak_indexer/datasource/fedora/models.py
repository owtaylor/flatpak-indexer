from datetime import datetime
from typing import List

from ...json_model import BaseModel


class BodhiUpdateModel(BaseModel):
    update_id: str
    release_name: str
    release_branch: str
    status: str
    type: str
    date_submitted: datetime
    date_testing: datetime
    date_stable: datetime
    user_name: str
    builds: List[str]


class KojiBuildModel(BaseModel):
    build_id: str
    nvr: str
    source: str
    completion_time: datetime
    user_name: str


class FlatpakBuildModel(KojiBuildModel):
    module_builds: List[str]
    package_builds: List[str]


class ModuleBuildModel(KojiBuildModel):
    modulemd: str

    package_builds: List[str]


class PackageBuildModel(KojiBuildModel):
    pass
