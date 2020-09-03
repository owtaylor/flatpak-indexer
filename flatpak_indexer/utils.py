import codecs
from collections import namedtuple
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import reduce
import hashlib
import logging
import os
import re
import subprocess
from tempfile import NamedTemporaryFile
from urllib.parse import urljoin

from requests import Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util import Retry


logger = logging.getLogger(__name__)

_RETRY_MAX_TIMES = 3
_RETRY_STATUSES = (
    408,  # Request Timeout
    500,  # Internal Server Error
    502,  # Bad Gateway
    503,  # Service Unavailable
    504   # Gateway Timeout
)


# If we want to retry POST, etc, need to set method_whitelist here
def get_retrying_requests_session(backoff_factor=3):
    retry = Retry(
        backoff_factor=backoff_factor,
        raise_on_status=True,
        status_forcelist=_RETRY_STATUSES,
        total=_RETRY_MAX_TIMES,
    )
    session = Session()
    session.mount('http://', HTTPAdapter(max_retries=retry))
    session.mount('https://', HTTPAdapter(max_retries=retry))

    return session


_ENV_VAR_TOKEN_RE = re.compile(r"\$\{|(?P<varname>[A-Za-z_][A-Za-z0-9_]*)|.")


class SubstitutionError(Exception):
    pass


def _substitute_env_vars(itr, outer=True):
    result = ""
    while True:
        m = next(itr, None)
        if m is None:
            if not outer:
                raise SubstitutionError("unclosed variable reference")
            return result
        elif m.group(0) == "${":
            m = next(itr, None)
            if m is None:
                raise SubstitutionError("unclosed variable reference")
            elif m.group('varname'):
                varname = m.group(0)
                m = next(itr, None)
                if m is None:
                    raise SubstitutionError("unclosed variable reference")
                elif m.group(0) == ":":
                    fallback = _substitute_env_vars(itr, outer=False)
                    result += os.environ.get(varname, fallback)
                elif m.group(0) == "}":
                    try:
                        result += os.environ[varname]
                    except KeyError:
                        raise SubstitutionError(
                            "environment variable {} is not set".format(varname)) from None
                else:
                    raise SubstitutionError(
                        "at position {} in field: expected : or }}".format(m.start()))
            else:
                raise SubstitutionError(
                    "at position {} in field: expected variable name".format(m.start()))
        elif m.group(0) == "}" and not outer:
            return result
        else:
            result += m.group(0)


@contextmanager
def atomic_writer(output_path):
    output_dir = os.path.dirname(output_path)
    tmpfile = NamedTemporaryFile(delete=False,
                                 dir=output_dir,
                                 prefix=os.path.basename(output_path))
    success = False
    try:
        writer = codecs.getwriter("utf-8")(tmpfile)
        yield writer
        writer.close()
        tmpfile.close()

        # We don't overwrite unchanged files, so that the modtime and
        # httpd-computed ETag stay the same.

        changed = True
        if os.path.exists(output_path):
            h1 = hashlib.sha256()
            with open(output_path, "rb") as f:
                h1.update(f.read())
            h2 = hashlib.sha256()
            with open(tmpfile.name, "rb") as f:
                h2.update(f.read())

            if h1.digest() == h2.digest():
                changed = False

        if changed:
            # Atomically write over result
            os.chmod(tmpfile.name, 0o644)
            os.rename(tmpfile.name, output_path)
            logger.info("Wrote %s", output_path)
        else:
            logger.info("%s is unchanged", output_path)
            os.unlink(tmpfile.name)

        success = True
    finally:
        if not success:
            tmpfile.close()
            os.unlink(tmpfile.name)


class TemporaryPathname:
    def __init__(self, suffix=None, prefix=None, dir=None):
        tmpfile = NamedTemporaryFile(delete=False,
                                     dir=dir,
                                     prefix=prefix,
                                     suffix=suffix)
        tmpfile.close()

        self.name = tmpfile.name
        self.delete = True

    def __enter__(self):
        return self

    def __exit__(self, exc, value, tb):
        if self.delete:
            os.unlink(self.name)


def substitute_env_vars(val):
    return _substitute_env_vars(_ENV_VAR_TOKEN_RE.finditer(val))


def format_date(dt):
    """Format date into the format that parse_date() understands.

    Naive (no-timezone) dates are interpreted as the local timezone
    """
    utc_dt = dt.astimezone(timezone.utc)
    return utc_dt.strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')


def parse_date(date_str):
    """Parse date from a fixed format.

    This format is the format that Pyxis writes, but we also use it for
    storing dates into JSON ourselves.
    """
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f+00:00')
    except ValueError:
        dt = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S+00:00')

    return dt.replace(tzinfo=timezone.utc)


def parse_pull_spec(spec):
    """Parse <registry>[:port]/<repository>[@<digest>|:tag]"""

    server_port, repository_ref = spec.split('/', 1)
    if '@' in repository_ref:
        repository, ref = repository_ref.rsplit('@', 1)
    else:
        repository, ref = repository_ref.rsplit(':', 1)

    return 'https://' + server_port, repository, ref


def unparse_pull_spec(registry_url, repository, ref):
    assert registry_url.startswith('https://')
    server_port = registry_url[8:]
    if not server_port.endswith('/'):
        server_port += '/'

    if ref.startswith('sha256:'):
        return f'{server_port}{repository}@{ref}'
    else:
        return f'{server_port}{repository}:{ref}'


def uri_for_digest(base_uri, digest, extension):
    assert digest.startswith("sha256:")
    subdir = digest[7:9]
    filename = digest[9:] + extension
    return urljoin(base_uri, subdir + '/' + filename)


def path_for_digest(base_dir, digest, extension, create_subdir=False):
    assert digest.startswith("sha256:")
    subdir = digest[7:9]
    filename = digest[9:] + extension

    if create_subdir:
        full_subdir = os.path.join(base_dir, subdir)
        if not os.path.exists(full_subdir):
            os.mkdir(full_subdir)

    return os.path.join(base_dir, subdir, filename)


ProcessStats = namedtuple('ProcessStas',
                          ['max_mem_kib',
                           'elapsed_time_s',
                           'system_time_s',
                           'user_time_s'])


def run_with_stats(args, progress_callback=None):
    with TemporaryPathname() as time_file:
        time_args = ['time', '-q', '--format=%M %e %S %U', f'--output={time_file.name}'] + args
        p = subprocess.Popen(time_args)

        while True:
            try:
                result = p.wait(timeout=1)
                break
            except subprocess.TimeoutExpired:
                pass

            if progress_callback:
                progress_callback()

        with open(time_file.name, "r") as f:
            y = f.read()
            max_mem, elapsed_time, system_time, user_time = \
                [x for x in y.strip().split()]

            stats = ProcessStats(max_mem_kib=float(max_mem),
                                 elapsed_time_s=float(elapsed_time),
                                 system_time_s=float(system_time),
                                 user_time_s=float(user_time))

        return result, stats
