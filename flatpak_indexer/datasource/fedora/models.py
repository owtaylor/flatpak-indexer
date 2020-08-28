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
