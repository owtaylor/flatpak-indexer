from socket import error as SocketError
from unittest.mock import patch

from pytest import raises

from flatpak_indexer.utils import get_retrying_requests_session


def test_retrying_requests_session():
    session = get_retrying_requests_session(backoff_factor=0.0)

    # Doing an actual test of successful completion would require
    # mocking out the internals of urllib3 in a complicated way -
    # so we just test that we retry on SocketError until we
    # hit the maximum.
    with raises(Exception, match="Max retries exceeded with url"):
        with patch("urllib3.connectionpool.HTTPConnectionPool._make_request",
                   side_effect=SocketError):
            session.get('http://www.example.com/')
