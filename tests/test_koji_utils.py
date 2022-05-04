from flatpak_indexer.koji_utils import get_koji_session, KojiConfig
from .koji import mock_koji


CONFIG = """
koji_config: fedora
"""


@mock_koji
def test_get_koji_session():
    config = KojiConfig.from_str(CONFIG)
    session = get_koji_session(config)
    assert session.getPackageID("eog") == 303
