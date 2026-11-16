import logging
import math
import requests


class AuthenticationError(Exception):
    pass


# Seconds to connect, and seconds to wait for the next bytes of a response. The
# read timeout is not a limit on the whole transfer, so large streamed downloads
# and uploads are not cut off.
DEFAULT_TIMEOUT = (10, 300)

# Default of the request methods' timeout argument, meaning none was passed and
# the connector's timeout applies. None cannot mean that: for requests, None is
# no limit, which a single request may still ask for.
_CONNECTOR_TIMEOUT = object()


def _check_timeout(timeout):
    """Reject a timeout requests would not accept as a limit: None (no limit),
    seconds, or a (connect, read) pair of seconds or None."""
    if timeout is None:
        return timeout
    if isinstance(timeout, tuple):
        if len(timeout) != 2:
            raise ValueError(f"A timeout is seconds or a (connect, read) pair, not {timeout!r}")
        parts = [part for part in timeout if part is not None]
    else:
        parts = [timeout]
    for part in parts:
        if isinstance(part, bool) or not isinstance(part, (int, float)) or not math.isfinite(part) or part <= 0:
            raise ValueError(f"A timeout must be a finite, positive number of seconds, not {timeout!r}")
    return timeout


class Connector:
    """Sends authenticated requests to DocAPI.

    timeout: how long a request may wait, as requests takes it: seconds, or a
    (connect, read) pair; DEFAULT_TIMEOUT unless given. A timed-out request
    raises requests.Timeout. None waits without limit. Each request method
    also takes a timeout of its own.
    """

    def __init__(self, api_key, user_agent="DocAPI-Client/1.0.0", timeout=DEFAULT_TIMEOUT):
        self.api_key = api_key
        self.user_agent = user_agent
        self.timeout = _check_timeout(timeout)

        # Create and configure session for connection reuse
        self.session = requests.Session()
        self.session.headers.update(self._get_auth_header())
        self.session.headers.update(self._get_user_agent_header())

        self._logger = logging.getLogger(__name__)

    def _timeout(self, timeout):
        return self.timeout if timeout is _CONNECTOR_TIMEOUT else _check_timeout(timeout)

    def get(self, url, params=None, stream=False, timeout=_CONNECTOR_TIMEOUT):
        return self.session.get(url, params=params, stream=stream, timeout=self._timeout(timeout))

    def post(self, url, data=None, json=None, files=None, params=None, timeout=_CONNECTOR_TIMEOUT):
        return self.session.post(url, data=data, json=json, files=files, params=params,
                                 timeout=self._timeout(timeout))

    def put(self, url, json=None, files=None, timeout=_CONNECTOR_TIMEOUT):
        return self.session.put(url, json=json, files=files, timeout=self._timeout(timeout))

    def patch(self, url, json=None, timeout=_CONNECTOR_TIMEOUT):
        return self.session.patch(url, json=json, timeout=self._timeout(timeout))

    def delete(self, url, timeout=_CONNECTOR_TIMEOUT):
        return self.session.delete(url, timeout=self._timeout(timeout))

    def _get_auth_header(self):
        return {'X-API-Key': self.api_key}

    def _get_user_agent_header(self):
        return {'User-Agent': self.user_agent}
