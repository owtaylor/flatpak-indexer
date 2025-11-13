from base64 import b64encode
from contextlib import contextmanager
from io import BytesIO
from urllib.parse import parse_qs, urlparse
import gzip
import hashlib
import json
import re
import tarfile

import requests
import responses

from flatpak_indexer.test.decorators import WithArgDecorator

MEDIA_TYPE_OCI = "application/vnd.oci.image.manifest.v1+json"


def registry_hostname(registry):
    """
    Strip a reference to a registry to just the hostname:port
    """
    if registry.startswith("http:") or registry.startswith("https:"):
        return urlparse(registry).netloc
    else:
        return registry


def to_bytes(value):
    if isinstance(value, bytes):
        return value
    else:
        return value.encode("utf-8")


def json_bytes(value):
    return json.dumps(value).encode("utf-8")


def make_digest(blob):
    # Abbreviate the hexdigest for readability of debugging output if things fail
    return "sha256:" + hashlib.sha256(to_bytes(blob)).hexdigest()[0:10]


class TestLayer:
    def __init__(self, filename, file_contents):
        tar_out = BytesIO()
        with tarfile.open(mode="w", fileobj=tar_out) as tf:
            tarinfo = tarfile.TarInfo(filename)
            tarinfo.size = len(file_contents)
            with BytesIO(file_contents) as file_contents_file:
                tf.addfile(tarinfo, file_contents_file)

        gzip_out = BytesIO()
        with gzip.GzipFile(fileobj=gzip_out, mode="w") as f:
            f.write(tar_out.getvalue())

        self.set_contents(gzip_out.getvalue())
        self.diff_id = make_digest(tar_out.getvalue())

    def set_contents(self, contents):
        self.contents = contents
        self.digest = make_digest(self.contents)
        self.size = len(self.contents)

    def verify(self, path):
        with open(path, "rb") as f:
            contents = f.read()

        assert contents == self.contents


class MockRegistry:
    """
    This class mocks a subset of the v2 Docker Registry protocol. It also has methods to inject
    and test content in the registry.
    """

    def __init__(self, registry="registry.example.com", required_creds=None, flags=""):
        self.hostname = registry_hostname(registry)
        self.repos = {}
        self.required_creds = required_creds
        self.flags = flags
        self._add_pattern(responses.GET, r"/v2/(.*)/manifests/([^/]+)", self._get_manifest)
        self._add_pattern(responses.GET, r"/v2/(.*)/blobs/([^/]+)", self._get_blob)
        self._add_pattern(responses.GET, r"/token?.*", self._get_token)

    def get_repo(self, name):
        return self.repos.setdefault(
            name,
            {
                "blobs": {},
                "manifests": {},
                "tags": {},
                "uploads": {},
            },
        )

    def add_blob(self, name, blob):
        repo = self.get_repo(name)
        digest = make_digest(blob)
        repo["blobs"][digest] = blob
        return digest, len(blob)

    def get_blob(self, name, digest):
        return self.get_repo(name)["blobs"][digest]

    # If fake_digest is set, we pretend the contents have that
    # digest, even if they don't.
    def add_manifest(self, name, ref, manifest, fake_digest=None):
        repo = self.get_repo(name)
        if fake_digest:
            digest = fake_digest
        else:
            digest = make_digest(manifest)
        repo["manifests"][digest] = manifest
        if ref is None:
            pass
        elif ref.startswith("sha256:"):
            assert ref == digest
        else:
            repo["tags"][ref] = digest
        return digest

    def get_manifest(self, name, ref):
        repo = self.get_repo(name)
        if not ref.startswith("sha256:"):
            ref = repo["tags"][ref]
        return repo["manifests"][ref]

    def _check_creds(self, req):
        if self.required_creds and (
            "bearer_auth" not in self.flags
            or req.url.startswith("https://registry.example.com/token")
        ):
            username, password = self.required_creds

            authorization = req.headers.get("Authorization")
            ok = False
            if authorization:
                pieces = authorization.strip().split()
                if pieces[0] == "Basic" and to_bytes(pieces[1]) == b64encode(
                    to_bytes(username + ":" + password)
                ):
                    ok = True

            if not ok:
                return (requests.codes.UNAUTHORIZED, {}, "")

    def _check_bearer_auth(self, req, repo):
        if "bearer_auth" in self.flags:
            authorization = req.headers.get("Authorization")
            if authorization != f"Bearer GOLDEN_LLAMA_{repo}":
                if "bearer_auth_unknown_type" in self.flags:
                    return (requests.codes.UNAUTHORIZED, {"WWW-Authenticate": "FeeFiFoFum"}, "")
                elif "bearer_auth_no_realm" in self.flags:
                    return (
                        requests.codes.UNAUTHORIZED,
                        {"WWW-Authenticate": 'Bearer service="registry.example.com"'},
                        "",
                    )
                else:
                    return (
                        requests.codes.UNAUTHORIZED,
                        {
                            "WWW-Authenticate": (
                                'Bearer realm="https://registry.example.com/token",'
                                + 'service="registry.example.com"'
                            )
                        },
                        "",
                    )

    def _add_pattern(self, method, pattern, callback):
        url = "https://" + self.hostname
        pat = re.compile("^" + url + pattern + "$")

        def do_it(req):
            auth_response = self._check_creds(req)
            if auth_response:
                return auth_response

            m = pat.match(req.url)
            assert m
            status, headers, body = callback(req, *(m.groups()))
            if method == responses.HEAD:
                return status, headers, ""
            else:
                return status, headers, body

        responses.add_callback(method, pat, do_it, match_querystring=True)

    def _get_manifest(self, req, name, ref):
        auth_response = self._check_bearer_auth(req, name)
        if auth_response:
            return auth_response

        repo = self.get_repo(name)
        if not ref.startswith("sha256:"):
            try:
                ref = repo["tags"][ref]
            except KeyError:
                return (requests.codes.NOT_FOUND, {}, json_bytes({"error": "NOT_FOUND"}))

        try:
            blob = repo["manifests"][ref]
        except KeyError:
            return (requests.codes.NOT_FOUND, {}, json_bytes({"error": "NOT_FOUND"}))

        decoded = json.loads(blob)
        content_type = decoded.get("mediaType")
        if content_type is None:  # OCI
            content_type = MEDIA_TYPE_OCI

        accepts = re.split(r"\s*,\s*", req.headers["Accept"])
        assert content_type in accepts

        if "bad_content_type" in self.flags:
            if content_type == MEDIA_TYPE_OCI:
                content_type = "application/json"

        headers = {
            "Docker-Content-Digest": ref,
            "Content-Type": content_type,
            "Content-Length": str(len(blob)),
        }
        return (200, headers, blob)

    def _get_blob(self, req, name, digest):
        auth_response = self._check_bearer_auth(req, name)
        if auth_response:
            return auth_response

        repo = self.get_repo(name)
        assert digest.startswith("sha256:")

        try:
            blob = repo["blobs"][digest]
        except KeyError:
            return (requests.codes.NOT_FOUND, {}, json_bytes({"error": "NOT_FOUND"}))

        headers = {
            "Docker-Content-Digest": digest,
            "Content-Type": "application/json",
            "Content-Length": str(len(blob)),
        }
        return (200, headers, blob)

    def _get_token(self, req):
        params = parse_qs(urlparse(req.url).query)
        assert params["service"][0] == "registry.example.com"
        m = re.match(r"repository:(.*):pull$", params["scope"][0])
        assert m
        repo = m.group(1)

        return (200, {}, json.dumps({"token": f"GOLDEN_LLAMA_{repo}"}))

    def add_fake_image(self, name, tag, diff_ids=None, layer_contents=None, labels=None):
        layer = TestLayer("test", b"42")
        if layer_contents:
            layer.set_contents(layer_contents)
        layers = [layer]
        for layer in layers:
            digest, _ = self.add_blob(name, layer.contents)
            assert digest == layer.digest

        if diff_ids is None:
            diff_ids = [layer.diff_id for layer in layers]

        config = {
            "architecture": "amd64",
            "os": "linux",
            "rootfs": {
                "type": "layers",
                "diff_ids": diff_ids,
            },
        }

        if labels is not None:
            config["config"] = {"Labels": labels}

        config_bytes = json_bytes(config)
        config_digest, config_size = self.add_blob(name, config_bytes)

        manifest = {
            "schemaVersion": 2,
            "mediaType": MEDIA_TYPE_OCI,
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": config_digest,
                "size": config_size,
            },
            "layers": [
                {
                    "mediaType": "application/vnd.oci.image.layer.v1.tar.gz",
                    "digest": layer.digest,
                    "size": layer.size,
                }
                for layer in layers
            ],
        }

        manifest_bytes = json_bytes(manifest)
        manifest_digest = self.add_manifest(name, tag, manifest_bytes)

        return manifest_digest, layers[0]


@contextmanager
def _setup_registry(**kwargs):
    with responses._default_mock:
        yield MockRegistry(**kwargs)


mock_registry = WithArgDecorator("registry_mock", _setup_registry)
