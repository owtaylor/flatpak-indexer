from requests import Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util import Retry


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
