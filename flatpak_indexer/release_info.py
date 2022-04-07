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


releases = [
    Release(name='F26', branch='f26',     tag='f26', status=ReleaseStatus.EOL),
    Release(name='F27', branch='f27',     tag='f27', status=ReleaseStatus.EOL),
    Release(name='F28', branch='f28',     tag='f28', status=ReleaseStatus.EOL),
    Release(name='F29', branch='f29',     tag='f29', status=ReleaseStatus.EOL),
    Release(name='F30', branch='f30',     tag='f30', status=ReleaseStatus.EOL),
    Release(name='F31', branch='f31',     tag='f31', status=ReleaseStatus.EOL),
    Release(name='F32', branch='f32',     tag='f32', status=ReleaseStatus.EOL),
    Release(name='F33', branch='f33',     tag='f33', status=ReleaseStatus.EOL),
    Release(name='F34', branch='f34',     tag='f34', status=ReleaseStatus.GA),
    Release(name='F35', branch='f35',     tag='f35', status=ReleaseStatus.GA),
    Release(name='F36', branch='rawhide', tag='f36', status=ReleaseStatus.BRANCHED),
]
