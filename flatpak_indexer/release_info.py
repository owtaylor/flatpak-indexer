from dataclasses import dataclass
from enum import Enum


class ReleaseStatus(Enum):
    RAWHIDE = 1
    BRANCHED = 2
    GA = 3
    EOL = 4


@dataclass
class Release:
    name: str
    branch: str
    tag: str
    status: ReleaseStatus
