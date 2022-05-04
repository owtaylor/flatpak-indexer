from typing import List, Optional, Set

from .http_utils import HttpConfig
from .session import Session


class OdcsConfig(HttpConfig):
    odcs_uri: Optional[str] = None


def composes_to_modules(session: Session, composes: List[int]):
    assert isinstance(session.config, OdcsConfig)
    assert session.config.odcs_uri

    requests_session = session.config.get_requests_session()

    result: Set[str] = set()

    for compose in composes:
        compose_uri = f"{session.config.odcs_uri}api/1/composes/{compose}"
        response = requests_session.get(compose_uri)
        response.raise_for_status()

        data = response.json()
        if data["source_type"] != 2:  # module
            continue

        for module in data["source"].split():
            result.add(module)

    return sorted(result)
