import os
from socket import error as SocketError
from unittest.mock import ANY, patch

from pytest import raises
import yaml

from flatpak_indexer.base_config import ConfigError
from flatpak_indexer.http_utils import HttpConfig


def get_config(pyxis_cert):
    config_data = {
        'local_certs': {
            'pyxis.example.com': pyxis_cert
        }
    }
    return HttpConfig.from_str(yaml.safe_dump(config_data))


def test_cert_relative(tmp_path):
    conf = get_config('test.crt')
    cert = conf.find_local_cert('https://pyxis.example.com')
    assert cert is not None
    assert os.path.isabs(cert)
    assert os.path.exists(cert)


def test_cert_missing(tmp_path):
    with raises(ConfigError, match="nothere.crt does not exist"):
        get_config(str(tmp_path / "nothere.crt"))


def test_cert_no_host(tmp_path):
    conf = get_config('test.crt')
    cert = conf.find_local_cert('/no/host')
    assert cert is None


def test_get_requests_session(tmp_path):
    config = get_config('test.crt')
    session = config.get_requests_session(backoff_factor=0)

    with patch("urllib3.connectionpool.HTTPConnectionPool._make_request",
               side_effect=SocketError), \
         patch("requests.adapters.HTTPAdapter.cert_verify") as p:

        with raises(Exception, match="Max retries exceeded with url"):
            session.get('https://pyxis.example.com/')

        p.assert_called_once()
        assert p.call_args[0][2].endswith("/test.crt")

        p.reset_mock()

        # Check that we pass verify=True for other URLs
        with raises(Exception, match="Max retries exceeded with url"):
            session.get('https://other.example.com/')

        ca_bundle = os.environ.get("REQUESTS_CA_BUNDLE")  # None or path to system ca bundle
        p.assert_called_once_with(ANY, 'https://other.example.com/', True, ca_bundle)
