import socket
import threading
from unittest import mock

import pytest
import requests

from doc_api.connector import DEFAULT_TIMEOUT, Connector


def _connector(**kwargs):
    connector = Connector("key", **kwargs)
    connector.session = mock.Mock()
    return connector


REQUESTS = [("get", ()), ("post", ()), ("put", ()), ("patch", ()), ("delete", ())]


@pytest.mark.parametrize("method,args", REQUESTS)
def test_every_request_has_the_connector_timeout(method, args):
    connector = _connector()
    getattr(connector, method)("http://docapi.example.org/v1/jobs", *args)
    assert getattr(connector.session, method).call_args.kwargs["timeout"] == DEFAULT_TIMEOUT == (10, 300)

    connector = _connector(timeout=(3, 30))
    getattr(connector, method)("http://docapi.example.org/v1/jobs", *args)
    assert getattr(connector.session, method).call_args.kwargs["timeout"] == (3, 30)


@pytest.mark.parametrize("method,args", REQUESTS)
def test_a_request_can_set_its_own_timeout(method, args):
    connector = _connector()
    getattr(connector, method)("http://docapi.example.org/v1/jobs", *args, timeout=900)
    assert getattr(connector.session, method).call_args.kwargs["timeout"] == 900
    # None waits without limit, but only when asked for.
    getattr(connector, method)("http://docapi.example.org/v1/jobs", *args, timeout=None)
    assert getattr(connector.session, method).call_args.kwargs["timeout"] is None
    assert _connector(timeout=None).timeout is None


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan"), "10", True, (10,), (10, 20, 30), (0, 10)])
def test_invalid_timeouts_are_rejected(timeout):
    with pytest.raises(ValueError):
        Connector("key", timeout=timeout)
    with pytest.raises(ValueError):
        _connector().get("http://docapi.example.org/v1/jobs", timeout=timeout)


def test_a_connect_or_read_part_may_be_unlimited():
    assert Connector("key", timeout=(10, None)).timeout == (10, None)


@pytest.fixture
def silent_server():
    """A local server that accepts connections and never answers."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    accepted, stop = [], threading.Event()

    def accept():
        listener.settimeout(0.1)
        while not stop.is_set():
            try:
                accepted.append(listener.accept()[0])
            except OSError:
                pass

    thread = threading.Thread(target=accept, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{listener.getsockname()[1]}/"
    stop.set()
    thread.join()
    for connection in accepted:
        connection.close()
    listener.close()


def test_a_server_that_never_answers_times_out(silent_server):
    with pytest.raises(requests.Timeout):
        Connector("key", timeout=(1, 0.2)).get(silent_server + "v1/jobs")


def test_the_user_agent_is_requests_own_unless_set():
    connector = Connector(api_key="k")
    assert connector.session.headers["User-Agent"] == requests.utils.default_user_agent()
    connector.set_user_agent("OCR worker prod")
    assert (connector.user_agent, connector.session.headers["User-Agent"]) == ("OCR worker prod", "OCR worker prod")
    connector.set_user_agent(None)
    assert connector.session.headers["User-Agent"] == requests.utils.default_user_agent()
    assert Connector(api_key="k", user_agent="myapp/2").session.headers["User-Agent"] == "myapp/2"
