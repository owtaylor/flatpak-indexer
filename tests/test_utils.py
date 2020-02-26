import os
from socket import error as SocketError
from unittest.mock import patch

import pytest
from pytest import raises

from flatpak_indexer.utils import (get_retrying_requests_session,
                                   substitute_env_vars,
                                   SubstitutionError)


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


@pytest.mark.parametrize('val, expected, exception',
                         [('foo', 'foo', None),
                          ('foo${SET}foo', 'foosetfoo', None),
                          ('foo${UNSET}foo', None, 'environment variable UNSET is not set'),
                          ('foo${UNSET:xxx}foo', 'fooxxxfoo', None),
                          ('foo${UNSET:${SET}}foo', 'foosetfoo', None),
                          ('$SET', '$SET', None),
                          ('${@}', None, 'at position 2 in field: expected variable name'),
                          ('${@}', None, 'at position 2 in field: expected variable name'),
                          ('${A@}', None, 'at position 3 in field: expected : or }'),
                          ('${', None, 'unclosed variable reference'),
                          ('${A', None, 'unclosed variable reference'),
                          ('${A:', None, 'unclosed variable reference')])
def test_substitute_env_vars(val, expected, exception):
    os.environ['SET'] = 'set'
    if 'UNSET' in os.environ:
        del os.environ['UNSET']

    if exception is None:
        result = substitute_env_vars(val)
        assert result == expected
    else:
        with raises(SubstitutionError, match=exception):
            substitute_env_vars(val)