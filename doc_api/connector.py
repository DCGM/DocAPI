import logging
import requests


class AuthenticationError(Exception):
    pass


class Connector:
    def __init__(self, worker_key, user_agent="DocAPI-Client/1.0.0"):
        self.worker_key = worker_key
        self.user_agent = user_agent

        # Create and configure session for connection reuse
        self.session = requests.Session()
        self.session.headers.update(self._get_auth_header())
        self.session.headers.update(self._get_user_agent_header())

        self._logger = logging.getLogger(__name__)

    def get(self, url, params=None):
        return self.session.get(url, params=params)

    def post(self, url, data=None, json=None, files=None, params=None):
        return self.session.post(url, data=data, json=json, files=files, params=params)

    def put(self, url, json=None, files=None):
        return self.session.put(url, json=json, files=files)

    def patch(self, url, json=None):
        return self.session.patch(url, json=json)

    def _get_auth_header(self):
        return {'X-API-Key': self.worker_key}

    def _get_user_agent_header(self):
        return {'User-Agent': self.user_agent}
