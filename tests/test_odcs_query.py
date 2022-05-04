from flatpak_indexer.odcs_query import composes_to_modules, OdcsConfig
from flatpak_indexer.session import Session
from .utils import mock_odcs


@mock_odcs
def test_composes_to_modules():
    config = OdcsConfig.from_str("odcs_uri: https://odcs.example.com/")
    session = Session(config)

    modules = composes_to_modules(session, [12345, 34567])
    assert modules == ['aisleriot:el8:8020020200121102609:73699f59']
