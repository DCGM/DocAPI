import logging
import requests


class AuthenticationError(Exception):
    pass


class Connector:
    def __init__(self, worker_key, user_agent="AnnoPageWorker/1.0"):
        self.worker_key = worker_key
        self.user_agent = user_agent

        # TODO: Use sessions for connection

        self._logger = logging.getLogger(__name__)

    def get(self, url, params=None):
        response = requests.get(url, params=params, headers=self._get_headers())
        return response

    def post(self, url, data=None, json=None, files=None, params=None):
        response = requests.post(url, data=data, json=json, files=files, params=params, headers=self._get_headers())
        return response

    def put(self, url, json=None, files=None):
        response = requests.put(url, json=json, files=files, headers=self._get_headers())
        return response

    def patch(self, url, json=None):
        response = requests.patch(url, json=json, headers=self._get_headers())
        return response

    def _get_headers(self):
        headers = requests.utils.default_headers()
        headers.update(self._get_auth_header())
        headers.update(self._get_user_agent_header())

        return headers

    def _get_auth_header(self):
        return {'X-API-Key': self.worker_key}

    def _get_user_agent_header(self):
        return {'User-Agent': self.user_agent}
