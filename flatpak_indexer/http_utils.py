import os
from typing import Dict
from urllib.parse import urlparse

from requests import Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util import Retry  # type: ignore

from .base_config import BaseConfig, ConfigError, configfield, Lookup


_RETRY_MAX_TIMES = 3
_RETRY_STATUSES = (
    408,  # Request Timeout
    500,  # Internal Server Error
    502,  # Bad Gateway
    503,  # Service Unavailable
    504   # Gateway Timeout
)


class _FindCACertAdapter(HTTPAdapter):
    def __init__(self, find_ca_cert=None, default_timeout=None, **kwargs):
        super().__init__(**kwargs)
        self.default_timeout = default_timeout
        self.find_ca_cert = find_ca_cert

    def cert_verify(self, conn, url, verify, cert):
        if url.lower().startswith('https') and verify and self.find_ca_cert:
            ca_cert = self.find_ca_cert(url)
            if ca_cert is not None:
                verify = ca_cert

        return super().cert_verify(conn, url, verify, cert)

    def send(self, request, **kwargs):
        arg_timeout = kwargs.get("timeout")
        if arg_timeout is None:
            kwargs["timeout"] = self.default_timeout

        return super().send(request, **kwargs)


class HttpConfig(BaseConfig):
    local_certs: Dict[str, str] = configfield(skip=True)
    connect_timeout: int = 30
    read_timeout: int = 50

    def __init__(self, lookup: Lookup):
        super().__init__(lookup)

        local_certs = lookup.get_str_dict('local_certs', {})
        self.local_certs = {}
        for k, v in local_certs.items():
            if not os.path.isabs(v):
                cert_dir = os.path.join(os.path.dirname(__file__), 'certs')
                v = os.path.join(cert_dir, v)

            if not os.path.exists(v):
                raise ConfigError("local_certs: {} does not exist".format(v))

            self.local_certs[k] = v

    def find_local_cert(self, url: str):
        hostname = urlparse(url).hostname
        if hostname is None:
            return None
        return self.local_certs.get(hostname)

    def get_requests_session(self, backoff_factor: float = 3):
        """
        Get a requests.Session object with appropriate modifications

        Args:
            backoff_factor (float): factor by which to increase delay - here
                so we can override for tests.
            find_local_cert (function): Function to get the CA cert for an URL
        """

        # If we want to retry POST, etc, need to set method_whitelist here
        retry = Retry(
            backoff_factor=backoff_factor,
            raise_on_status=True,
            status_forcelist=_RETRY_STATUSES,
            total=_RETRY_MAX_TIMES,
        )
        session = Session()

        session.mount(
            'http://',
            _FindCACertAdapter(max_retries=retry,
                               default_timeout=(self.connect_timeout, self.read_timeout),
                               find_ca_cert=self.find_local_cert)
        )
        session.mount(
            'https://',
            _FindCACertAdapter(max_retries=retry,
                               default_timeout=(self.connect_timeout, self.read_timeout),
                               find_ca_cert=self.find_local_cert)
        )

        return session
