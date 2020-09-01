# Derived from
# https://github.com/fedora-infra/bodhi/blob/develop/bodhi/server/scripts/skopeo_lite.py
# and relicensed under the MIT license
#
# Copyright © 2018-2020 Red Hat, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:

# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

from contextlib import contextmanager
from unittest import mock
import os

import pytest
import requests

from flatpak_indexer.registry_client import RegistryClient

from .registry import mock_registry


@mock_registry
def test_download_layer(registry, tmp_path):
    """
    Basic test of downloading a layer
    """
    manifest_digest, test_layer = registry.add_fake_image('repo1', 'latest')

    registry_client = RegistryClient('https://registry.example.com',
                                     ca_cert='test.crt')
    out_path = tmp_path / "layer"
    registry_client.download_layer('repo1', manifest_digest,
                                   test_layer.diff_id,
                                   str(out_path))
    test_layer.verify(out_path)


@mock_registry
def test_download_layer_progress(registry, tmp_path):
    """
    Testing layer download with a progress callback
    """
    manifest_digest, test_layer = registry.add_fake_image('repo1', 'latest')

    bytes_read = None
    total_bytes = None

    def progress_callback(bytes_read_, total_bytes_):
        nonlocal bytes_read, total_bytes
        bytes_read = bytes_read_
        total_bytes = total_bytes_

    registry_client = RegistryClient('https://registry.example.com')
    out_path = tmp_path / "layer"
    with mock.patch('flatpak_indexer.registry_client.CHUNK_SIZE', 128), \
         mock.patch('flatpak_indexer.registry_client.PROGRESS_INTERVAL', 0):
        registry_client.download_layer('repo1', manifest_digest,
                                       test_layer.diff_id,
                                       str(out_path),
                                       progress_callback=progress_callback)

    assert total_bytes > 0
    # Expect a callback for every chunk
    assert bytes_read == total_bytes


@mock_registry(flags='bearer_auth')
def test_download_layer_bearer_auth(registry, tmp_path):
    """
    Test redirecting in response to UNAUTHORIZED
    """
    manifest_digest, layer = registry.add_fake_image('repo1', 'latest')

    registry_client = RegistryClient('https://registry.example.com')
    registry_client.download_layer('repo1', manifest_digest, layer.diff_id,
                                   str(tmp_path / "layer"))


@pytest.mark.parametrize('flags', [
    # Test when we don't have creds to get a token
    'bearer_auth',
    # Or with an WWW-Authenticate header we can't handle
    'bearer_auth_unknown_type',
    # Or another type of WWW-Authenticate we can't handle
    'bearer_auth_no_realm',
])
@mock_registry(required_creds=('someuser', 'somepassword'))
def test_download_layer_bearer_auth_unauthorized(registry, tmp_path, flags):
    """
    Test bad outcomes when redirecting in response to UNAUTHORIZED
    """
    registry.flags = flags
    manifest_digest, layer = registry.add_fake_image('repo1', 'latest')

    registry_client = RegistryClient('https://registry.example.com')
    with pytest.raises(requests.exceptions.HTTPError, match=r'401 Client Error'):
        registry_client.download_layer('repo1', manifest_digest, layer.diff_id,
                                       str(tmp_path / "layer"))


@mock_registry(required_creds=('someuser', 'somepassword'))
def test_download_layer_username_password(registry, tmp_path):
    """
    Testing authentication with username and password
    """
    manifest_digest, layer = registry.add_fake_image('repo1', 'latest')

    registry_client = RegistryClient('https://registry.example.com',
                                     creds="someuser:somepassword")
    registry_client.download_layer('repo1', manifest_digest, layer.diff_id,
                                   str(tmp_path / "layer"))


@contextmanager
def check_certificates(cert=None):
    old_get = requests.Session.get

    def checked_get(self, *args, **kwargs):
        if kwargs.get('cert') != cert:
            raise RuntimeError("Wrong/missing cert for GET")

        return old_get(self, *args, **kwargs)

    with mock.patch('requests.Session.get', autospec=True, side_effect=checked_get):
        yield


@mock_registry
@pytest.mark.parametrize(('breakage', 'error'), [
    (None, None),
    ('missing_cert', 'Cannot find certificate file'),
    ('missing_key', 'Cannot find key file'),
    ('missing_cert_and_key', 'Wrong/missing cert'),
])
def test_download_layer_username_certs(registry, tmp_path, breakage, error):
    """
    Test authentication with a certificate
    """
    cert_dir = tmp_path / "certs"
    os.mkdir(cert_dir)

    cert_file = cert_dir / 'registry.example.com.cert'
    if breakage not in ('missing_cert', 'missing_cert_and_key'):
        with open(cert_file, 'w'):
            pass
    key_file = cert_dir / 'registry.example.com.key'
    if breakage not in ('missing_key', 'missing_cert_and_key'):
        with open(key_file, 'w'):
            pass
    cert = (str(cert_file), str(key_file))

    # Ensure RegistrySession._find_cert() encounters a file to skip
    # over.
    if not breakage:
        with open(cert_dir / 'dummy', 'w'):
            pass

    manifest_digest, layer = registry.add_fake_image('repo1', 'latest')

    with check_certificates(cert):
        if not breakage:
            registry_client = RegistryClient('https://registry.example.com',
                                             cert_dir=str(cert_dir))
            registry_client.download_layer('repo1', manifest_digest, layer.diff_id,
                                           str(tmp_path / "layer"))
        else:
            with pytest.raises(Exception) as excinfo:
                registry_client = RegistryClient('https://registry.example.com',
                                                 cert_dir=str(cert_dir))
                registry_client.download_layer('repo1', manifest_digest, layer.diff_id,
                                               str(tmp_path / "layer"))
            assert error in str(excinfo.value)


@contextmanager
def mock_system_certs():
    old_isdir = os.path.isdir
    old_listdir = os.listdir
    old_exists = os.path.exists

    def isdir(path):
        if isinstance(path, str) and path.startswith('/etc/'):
            return path == '/etc/docker/certs.d/registry.example.com'
        else:
            return old_isdir(path)

    def listdir(path):
        if isinstance(path, str) and path.startswith('/etc/'):
            if path == '/etc/docker/certs.d/registry.example.com':
                return ('client.cert', 'client.key')
            else:
                return None
        else:
            return old_listdir(path)

    def exists(path):
        if isinstance(path, str) and path.startswith('/etc/'):
            return path in ('/etc/docker/certs.d/registry.example.com/client.cert',
                            '/etc/docker/certs.d/registry.example.com/client.key')
        else:
            return old_exists(path)

    with mock.patch('os.path.isdir', side_effect=isdir):
        with mock.patch('os.listdir', side_effect=listdir):
            with mock.patch('os.path.exists', side_effect=exists):
                yield


@mock_registry
def test_download_layer_system_cert(registry, tmp_path):
    """
    Test using a certificate from a system directory
    """
    manifest_digest, layer = registry.add_fake_image('repo1', 'latest')

    with mock_system_certs():
        with check_certificates(('/etc/docker/certs.d/registry.example.com/client.cert',
                                 '/etc/docker/certs.d/registry.example.com/client.key')):
            registry_client = RegistryClient('https://registry.example.com')
            registry_client.download_layer('repo1', manifest_digest, layer.diff_id,
                                           str(tmp_path / "layer"))


@mock_registry
def test_download_layer_write_failure(registry, tmp_path):
    """
    Test using a certificate from a system directory
    """
    manifest_digest, layer = registry.add_fake_image('repo1', 'latest')

    with mock.patch('tempfile.NamedTemporaryFile') as m:
        tempfile = mock.Mock()
        m.return_value = tempfile
        tempfile.name = str(tmp_path / "tmpfile")
        with open(tempfile.name, "w"):
            pass
        tempfile.write.side_effect = IOError("write failed")

        with pytest.raises(IOError):
            registry_client = RegistryClient('https://registry.example.com')
            registry_client.download_layer('repo1', manifest_digest, layer.diff_id,
                                           str(tmp_path / "layer"))


@mock_registry
def test_download_layer_bad_diff_ids(registry, tmp_path):
    """
    Test using a certificate from a system directory
    """
    manifest_digest, layer = registry.add_fake_image('repo1', 'latest', diff_ids=[])

    registry_client = RegistryClient('https://registry.example.com')

    with pytest.raises(RuntimeError,
                       match=r"repo1:sha256:[a-f0-9]+: Mismatch between DiffIDs and layers"):
        registry_client.download_layer('repo1', manifest_digest, layer.diff_id,
                                       str(tmp_path / "layer"))

    manifest_digest, layer = registry.add_fake_image('repo2', 'latest',
                                                     diff_ids=['sha256:ba5eba11'])

    with pytest.raises(RuntimeError,
                       match=r"repo2:sha256:[a-f0-9]+: Can't find DiffID sha256:[a-f0-9]+"):
        registry_client.download_layer('repo2', manifest_digest, layer.diff_id,
                                       str(tmp_path / "layer"))
