import datetime
import os
from socket import error as SocketError
from typing import List, Optional
from unittest.mock import ANY, patch

import pytest
from pytest import raises

from flatpak_indexer.utils import (
    atomic_writer,
    format_date,
    get_retrying_requests_session,
    parse_date,
    parse_pull_spec,
    path_for_digest,
    pseudo_atomic_dir_writer,
    resolve_type,
    rpm_nvr_compare,
    run_with_stats,
    substitute_env_vars,
    SubstitutionError,
    TemporaryPathname,
    unparse_pull_spec,
    uri_for_digest
)
from .utils import timeout_first_popen_wait


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


def test_retrying_requests_session_find_ca_cert():
    def find_ca_cert(url):
        if url.startswith("https://www.example.com"):
            return "test.crt"

    session = get_retrying_requests_session(backoff_factor=0.0, find_ca_cert=find_ca_cert)

    with patch("urllib3.connectionpool.HTTPConnectionPool._make_request",
               side_effect=SocketError), \
         patch("requests.adapters.HTTPAdapter.cert_verify") as p:

        with raises(Exception, match="Max retries exceeded with url"):
            session.get('https://www.example.com/')

        p.assert_called_once_with(ANY, 'https://www.example.com/', "test.crt", None)

        p.reset_mock()

        # Check that we pass verify=True for other URLs
        with raises(Exception, match="Max retries exceeded with url"):
            session.get('https://other.example.com/')

        p.assert_called_once_with(ANY, 'https://other.example.com/', True, None)


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


def test_pseudo_atomic_dir_writer(tmp_path):
    output_path = str(tmp_path / 'out')
    with pseudo_atomic_dir_writer(output_path) as tempdir:
        with open(os.path.join(tempdir, "a"), "w") as f:
            print("Hello", file=f)

    assert os.listdir(tmp_path) == ["out"]
    assert os.listdir(output_path) == ["a"]

    with pseudo_atomic_dir_writer(output_path) as tempdir:
        with open(os.path.join(tempdir, "b"), "w") as f:
            print("Hello", file=f)

    assert os.listdir(tmp_path) == ["out"]
    assert os.listdir(output_path) == ["b"]

    with pytest.raises(RuntimeError, match=r"fell apart"):
        with pseudo_atomic_dir_writer(output_path) as tempdir:
            raise RuntimeError("fell apart")

    assert os.listdir(tmp_path) == ["out"]
    assert os.listdir(output_path) == ["b"]


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
    dt = parse_date('2020-07-31T16:26:22.123456+00:00')
    assert round(dt.timestamp() * 1000000) == 1596212782123456
    assert dt.year == 2020
    assert dt.month == 7
    assert dt.day == 31
    assert dt.hour == 16
    assert dt.minute == 26
    assert dt.second == 22
    assert dt.microsecond == 123456
    assert dt.tzinfo is not None
    assert dt.tzinfo.utcoffset(None) == datetime.timedelta(0)

    dt = parse_date('2020-07-31T16:26:22+00:00')
    assert dt.timestamp() == 1596212782
    assert dt.second == 22
    assert dt.microsecond == 0


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


def test_run_with_stats():
    res, stats = run_with_stats(['/bin/true'])
    assert res == 0
    assert stats.max_mem_kib > 0
    assert type(stats.elapsed_time_s) == float
    assert type(stats.system_time_s) == float
    assert type(stats.user_time_s) == float

    res, stats = run_with_stats(['/bin/false'])
    assert res != 0
    assert stats is not None

    with timeout_first_popen_wait():
        progress_called = False

        def progress():
            nonlocal progress_called
            progress_called = True

        res, stats = run_with_stats(['/bin/true'], progress_callback=progress)
        assert res == 0
        assert stats is not None
        assert progress_called


@pytest.mark.parametrize('nvr_a, nvr_b, result, exception', [
    ('x-1-1', 'x-2-1', -1, None),
    ('x-1-1', 'x-1-1',  0, None),
    ('x-2-1', 'x-1-1',  1, None),
    ('x-1-1', 'x-1-2', -1, None),
    ('x-1~alpha-1', 'x-1-1',  -1, None),
    ('x-1-1', 'y-1-1',  0, r'x-1-1 and y-1-1 have different names'),
])
def test_rpm_nvr_compare(nvr_a, nvr_b, result, exception):
    if exception:
        with raises(ValueError, match=exception):
            rpm_nvr_compare(nvr_a, nvr_b)
    else:
        assert rpm_nvr_compare(nvr_a, nvr_b) == result


def test_resolve_type():
    class X:
        a: int
        b: Optional[str]
        c: List[str]

    annotations = X.__annotations__
    assert resolve_type(annotations['a']) == (int, (), False)
    assert resolve_type(annotations['b']) == (str, (), True)
    assert resolve_type(annotations['c']) == (list, (str,), False)
