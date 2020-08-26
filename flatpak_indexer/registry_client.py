# Derived from
# https://github.com/fedora-infra/bodhi/blob/develop/bodhi/tests/server/scripts/test_skopeo_lite.py
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

import logging
import os
import tempfile
import time
from urllib.parse import urlparse

import requests

from .utils import get_retrying_requests_session


logger = logging.getLogger(__name__)


MEDIA_TYPE_MANIFEST_V2 = 'application/vnd.docker.distribution.manifest.v2+json'
MEDIA_TYPE_OCI = 'application/vnd.oci.image.manifest.v1+json'

# Layer download: chunk size
CHUNK_SIZE = 64 * 1024

# Layer download: Seconds after which to call progress callback
PROGRESS_INTERVAL = 1


class RegistrySession(object):
    """Wrapper around requests.Session adding docker-specific behavior."""

    def __init__(self, registry, insecure=False, creds=None, cert_dir=None):
        """
        Initialize the RegistrySession.

        Args:
            registry_url (str): the base URL for the registry
            creds (str): user:password (may be None).
            cert_dir (str): A path to directory holding client certificates, or None.
        """
        self.registry_url = registry

        parsed = urlparse(self.registry_url)
        self.registry_hostport = parsed.hostname + (f":{parsed.port}" if parsed.port else "")

        self.insecure = insecure

        self.cert = self._find_cert(cert_dir)

        self.auth = None
        if creds is not None:
            username, password = creds.split(':', 1)
            self.auth = requests.auth.HTTPBasicAuth(username, password)

        self.session = get_retrying_requests_session()

    def _find_cert_dir(self):
        """
        Return a path to a directory containing TLS client certificates to use for authentication.

        Returns:
            str or None: If a path is found, it is returned. Otherwise None is returned.
        """

        for d in ('/etc/containers/certs.d', '/etc/docker/certs.d'):
            certs_dir = os.path.join(d, self.registry_hostport)
            if os.path.isdir(certs_dir):
                return certs_dir

        return None

    def _find_cert(self, cert_dir):
        """
        Return a TLS client certificate to be used to authenticate to servers.

        Args:
            cert_dir (str or None): A directory to look for certs in. None indicates to use
               find_cert_dir() to find the path. Defaults to None.
        Returns:
            tuple or None: If no certificate is found, None is returned, otherwise, a 2-tuple
               is returned, the first element is the path to a certificate, the second element
               is the path to the matching key.
        Raises:
            RuntimeError: If a key is found without a matching certificate or vice versa.
        """
        if cert_dir is None:
            cert_dir = self._find_cert_dir()

        if cert_dir is None:
            return None

        for d in sorted(os.listdir(cert_dir)):
            if d.endswith('.cert'):
                certpath = os.path.join(cert_dir, d)
                keypath = certpath[:-5] + '.key'
                if not os.path.exists(keypath):
                    raise RuntimeError("Cannot find key file for {}".format(certpath))
                return (certpath, keypath)
            elif d.endswith('.key'):
                # Should have found <x>.cert first
                keypath = os.path.join(cert_dir, d)
                raise RuntimeError("Cannot find certificate file for {}".format(keypath))

        return None

    def _wrap_method(self, f, relative_url, *args, **kwargs):
        """
        Perform an HTTP request with appropriate options and fallback handling.

        This is used to implement methods like get, head, etc. It modifies
        kwargs, tries to do the operation, then if a TLS request fails and
        TLS validation is not required, tries again with a non-TLS URL.

        Args:
            f (callable): callback to actually perform the request.
            relative_url (str): URL relative to the toplevel hostname.
            kwargs: Additional arguments passed to requests.Session.get.
        Returns:
            requests.Response: The response object.
        """
        kwargs['auth'] = self.auth
        kwargs['cert'] = self.cert

        return f(self.registry_url + relative_url, *args, **kwargs)

    def get(self, relative_url, **kwargs):
        """
        Do a HTTP GET.

        Args:
            relative_url (str): URL relative to the toplevel hostname.
            kwargs: Additional arguments passed to requests.Session.get.
        Returns:
            requests.Response: The response object.
        """
        return self._wrap_method(self.session.get, relative_url, **kwargs)


class RegistryClient(object):
    """The source or destination of a copy operation to a docker registry."""

    def __init__(self, registry_url, creds=None, cert_dir=None):
        """
        Initialize the registry spec.

        Args:
           registry_url (str): the base URL for the registry
           creds (str): user:password (may be None).
           cert_dir (str): A path to directory holding client certificates, or None.
        """
        self.session = RegistrySession(registry_url, creds=creds, cert_dir=cert_dir)

    def download_blob(self, repository, digest, size, blob_path,
                      progress_callback=None):
        """
        Download a blob from the registry to a local file.

        Args:
            digest (str): The digest of the blob to download.
            size (int): The size of blob.
            blob_path (str): The local path to write the blob to.
            progress_callback (function): Progress callback to call periodically during the download
        """
        logger.info("%s: Downloading %s:%s (size=%s)",
                    self.session.registry_hostport, repository, digest, size)

        url = "/v2/{}/blobs/{}".format(repository, digest)
        result = self.session.get(url, stream=True)
        result.raise_for_status()

        output_dir = os.path.dirname(blob_path)
        tmpfile = tempfile.NamedTemporaryFile(delete=False,
                                              dir=output_dir,
                                              prefix=os.path.basename(blob_path))

        success = False
        try:
            bytes_read = 0
            last_progress = time.time()
            for block in result.iter_content(CHUNK_SIZE):
                bytes_read += len(block)
                now = time.time()
                if now - last_progress > PROGRESS_INTERVAL:
                    last_progress = now
                    if progress_callback:
                        progress_callback(bytes_read, size)
                tmpfile.write(block)

            os.chmod(tmpfile.name, 0o644)
            os.rename(tmpfile.name, blob_path)

            success = True
        finally:
            if not success:
                tmpfile.close()
                os.unlink(tmpfile.name)

    def get_manifest(self, repository, ref):
        """
        Download a manifest from a registry.

        Args:
            repository (str): The repository to download from.
            ref (str): A digest, or a tag.
        Returns:
            dict: decoded JSON content of manifest
        """
        logger.debug("%s: Retrieving manifest for %s:%s",
                     self.session.registry_hostport, repository, ref)

        headers = {
            'Accept': ', '.join((
                MEDIA_TYPE_MANIFEST_V2,
                MEDIA_TYPE_OCI,
            ))
        }

        url = '/v2/{}/manifests/{}'.format(repository, ref)
        response = self.session.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    def get_config(self, repository, manifest):
        """
        Download the JSON config for the manifest

        Args:
            repository (str): The repository to download from.
            manifest (dict): decoded contents of the manifest
        Returns:
            dict: decoded JSON content of config
        """
        descriptor = manifest['config']
        logger.debug("%s: Downloading config %s:%s (size=%s)",
                     self.session.registry_hostport, repository,
                     descriptor['digest'], descriptor['size'])

        url = "/v2/{}/blobs/{}".format(repository, descriptor['digest'])
        result = self.session.get(url, stream=True)
        result.raise_for_status()
        return result.json()

    def download_layer(self, repository, ref, diff_id, blob_path,
                       progress_callback=None):
        """
        Download a layer from a registry given a repository:ref and diff_id

        Args:
            repository (str): The repository to download from.
            ref (str): A digest, or a tag
            diff_id (str): The DiffID of the layer to dowload
            blob_path (str): The local path to write the layer to
            progress_callback (function): Progress callback to call periodically during the download
        """

        manifest = self.get_manifest(repository, ref)
        config = self.get_config(repository, manifest)

        diff_ids = config.get('rootfs', {}).get('diff_ids', [])
        if len(diff_ids) != len(manifest['layers']):
            raise RuntimeError(f"{repository}:{ref}: Mismatch between DiffIDs and layers")

        try:
            layer_index = diff_ids.index(diff_id)
        except ValueError:
            raise RuntimeError(f"{repository}:{ref}: Can't find DiffID {diff_id}")

        descriptor = manifest['layers'][layer_index]
        self.download_blob(repository, descriptor['digest'], descriptor['size'], blob_path,
                           progress_callback=progress_callback)
