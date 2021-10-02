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
from urllib.parse import urlencode, urlparse

import requests
import requests.auth
import www_authenticate

from .utils import get_retrying_requests_session


logger = logging.getLogger(__name__)


MEDIA_TYPE_MANIFEST_V2 = 'application/vnd.docker.distribution.manifest.v2+json'
MEDIA_TYPE_OCI = 'application/vnd.oci.image.manifest.v1+json'

# Layer download: chunk size
CHUNK_SIZE = 64 * 1024

# Layer download: Seconds after which to call progress callback
PROGRESS_INTERVAL = 1


class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers['Authorization'] = 'Bearer ' + self.token
        return r


class RegistrySession(object):
    """Wrapper around requests.Session adding docker-specific behavior."""

    def __init__(self, registry, creds=None, cert_dir=None, ca_cert=None):
        """
        Initialize the RegistrySession.

        Args:
            registry_url (str): the base URL for the registry
            creds (str): user:password (may be None).
            cert_dir (str): A path to directory holding client certificates, or None.
           ca_cert (str): A path to a bundle of trusted ca certificates
        """
        self.registry_url = registry

        parsed = urlparse(self.registry_url)
        assert parsed.hostname is not None
        self.registry_hostport = parsed.hostname + (f":{parsed.port}" if parsed.port else "")

        self.cert = self._find_cert(cert_dir)
        self.ca_cert = ca_cert

        self.auth = None
        if creds is not None:
            username, password = creds.split(':', 1)
            self.auth = requests.auth.HTTPBasicAuth(username, password)

        self.orig_auth = self.auth

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

    def _kwargs(self, orginal_kwargs):
        result = dict(orginal_kwargs)

        result['auth'] = self.auth
        result['cert'] = self.cert
        if self.ca_cert:
            result['verify'] = self.ca_cert

        return result

    def _get_token_auth(self, res, repository):
        parsed = www_authenticate.parse(res.headers['www-authenticate'])
        if 'bearer' not in parsed:
            return

        challenge = parsed['bearer']
        realm = challenge.get('realm')
        service = challenge.get('service')
        scope = challenge.get('scope')
        if scope is None and repository:
            scope = f'repository:{repository}:pull'

        logger.info("Getting token auth, realm=%s, service=%s, scope=%s",
                    realm, service, scope)

        if not realm:
            return False

        self.auth = self.orig_auth

        params = []
        if service:
            params.append(('service', service))
        if scope:
            params.append(('scope', scope))

        url = realm + '?' + urlencode(params)
        res = requests.get(url, **self._kwargs(dict()))
        if res.status_code != 200:
            return False

        token = res.json()['token']
        self.auth = BearerAuth(token)

        return True

    def _wrap_method(self, f, relative_url, *args, repository=None, **kwargs):
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
        kwargs = self._kwargs(kwargs)

        res = f(self.registry_url + relative_url, *args, **kwargs)
        if res.status_code == requests.codes.UNAUTHORIZED:
            if self._get_token_auth(res, repository):
                kwargs['auth'] = self.auth
                res = f(self.registry_url + relative_url, *args, **kwargs)

        return res

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

    def __init__(self, registry_url, creds=None, cert_dir=None, ca_cert=None):
        """
        Initialize the registry spec.

        Args:
           registry_url (str): the base URL for the registry
           creds (str): user:password (may be None).
           cert_dir (str): A path to directory holding client certificates, or None.
           ca_cert (str): A path to a bundle of trusted ca certificates
        """
        self.session = RegistrySession(registry_url,
                                       creds=creds, cert_dir=cert_dir, ca_cert=ca_cert)

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
        result = self.session.get(url, stream=True, repository=repository)
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
        response = self.session.get(url, headers=headers, repository=repository)
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
        result = self.session.get(url, stream=True, repository=repository)
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
