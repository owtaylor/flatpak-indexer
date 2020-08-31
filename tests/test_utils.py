import datetime
import os
from socket import error as SocketError
from unittest.mock import patch

import pytest
from pytest import raises

from flatpak_indexer.utils import (atomic_writer,
                                   format_date,
                                   get_retrying_requests_session,
                                   parse_date,
                                   parse_pull_spec,
                                   unparse_pull_spec,
                                   path_for_digest,
                                   substitute_env_vars,
                                   SubstitutionError,
                                   TemporaryPathname,
                                   uri_for_digest)


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


def test_atomic_writer_basic(tmp_path):
    output_path = str(tmp_path / 'out.json')

    def expect(val):
        with open(output_path, "rb") as f:
            assert f.read() == val

    with atomic_writer(output_path) as writer:
        writer.write("HELLO")
    os.utime(output_path, (42, 42))
    expect(b"HELLO")

    with atomic_writer(output_path) as writer:
        writer.write("HELLO")
    expect(b"HELLO")
    assert os.stat(output_path).st_mtime == 42

    with atomic_writer(output_path) as writer:
        writer.write("GOODBYE")
    expect(b"GOODBYE")


def test_atomic_writer_write_failure(tmp_path):
    output_path = str(tmp_path / 'out.json')

    with pytest.raises(IOError):
        with atomic_writer(output_path) as writer:
            writer.write("HELLO")
            raise IOError()

    assert os.listdir(tmp_path) == []


@pytest.mark.parametrize('steal', (True, False))
def test_temporary_pathname(tmp_path, steal):
    with TemporaryPathname(dir=tmp_path, prefix="foo-", suffix=".txt") as path:
        assert os.path.dirname(path.name) == str(tmp_path)
        assert os.path.basename(path.name).startswith("foo-")
        assert path.name.endswith(".txt")

        if steal:
            os.rename(path.name, tmp_path / "foo.txt")
            path.delete = False

    if steal:
        assert (tmp_path / "foo.txt").exists()
    assert not os.path.exists(path.name)


def test_format_date():
    dt = datetime.datetime.fromtimestamp(1596212782,
                                         datetime.timezone.utc)
    assert format_date(dt) == '2020-07-31T16:26:22.000000+00:00'

    dt = datetime.datetime.fromtimestamp(1596212782,
                                         datetime.timezone(datetime.timedelta(hours=-4)))
    assert format_date(dt) == '2020-07-31T16:26:22.000000+00:00'

    # Naive timestamps are assumed to represent local time (this test will blindly succeed
    # if TZ=utc)
    dt = datetime.datetime.fromtimestamp(1596212782)
    assert format_date(dt) == '2020-07-31T16:26:22.000000+00:00'


def test_parse_date():
    dt = parse_date('2020-07-31T16:26:22.000000+00:00')
    assert dt.timestamp() == 1596212782
    assert dt.year == 2020
    assert dt.month == 7
    assert dt.day == 31
    assert dt.hour == 16
    assert dt.minute == 26
    assert dt.second == 22
    assert dt.tzinfo.utcoffset(None) == datetime.timedelta(0)


def test_parse_pull_spec():
    assert parse_pull_spec('registry.example.com/some/repo:latest') == (
        'https://registry.example.com', 'some/repo', 'latest'
    )
    assert parse_pull_spec('registry.example.com/some/repo@sha256:12345') == (
        'https://registry.example.com', 'some/repo', 'sha256:12345'
    )


def test_unparse_pull_spec():
    assert (unparse_pull_spec('https://registry.example.com', 'some/repo', 'latest') ==
            'registry.example.com/some/repo:latest')
    assert (unparse_pull_spec('https://registry.example.com/', 'some/repo', 'latest') ==
            'registry.example.com/some/repo:latest')
    assert (unparse_pull_spec('https://registry.example.com', 'some/repo', 'sha256:12345') ==
            'registry.example.com/some/repo@sha256:12345')


def test_path_for_digest(tmp_path):
    assert (path_for_digest(str(tmp_path), 'sha256:abcd', '.png') ==
            str(tmp_path / "ab/cd.png"))
    assert not (tmp_path / "ab").exists()

    assert (path_for_digest(str(tmp_path), 'sha256:abcd', '.png', create_subdir=True) ==
            str(tmp_path / "ab/cd.png"))
    assert (tmp_path / "ab").exists()


def test_uri_for_digest():
    assert (uri_for_digest('https://example.com/files/', 'sha256:abcd', '.png') ==
            "https://example.com/files/ab/cd.png")
