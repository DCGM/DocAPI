"""The worker's identity: given, or the label of its API key on DocAPI, and its User-Agent."""
import types
from unittest import mock

import pytest
import requests

from doc_api.connector import Connector
from doc_worker.doc_worker_wrapper import DocWorkerWrapper, WorkerResponse


class Worker(DocWorkerWrapper):
    def process_job(self, *args, **kwargs):
        return WorkerResponse.ok()


def me(label="OCR worker prod", status=200):
    data = {"status": status, "code": "API_KEY_VALID", "data": {"label": label}} if status == 200 else None
    return types.SimpleNamespace(is_success=status == 200, status=status, data=data)


def make_worker(tmp_path, *answers, connector=None, **kwargs):
    worker = Worker("http://docapi.example.org", connector or Connector(api_key="k"), base_dir=str(tmp_path),
                    **kwargs)
    worker.adapter = mock.Mock()
    worker.adapter.get_me.side_effect = list(answers)
    return worker


def run(worker, passes=1):
    """start() for this many loop passes."""
    worker.run_job_pipeline = mock.Mock(side_effect=[None] * passes + [KeyboardInterrupt])
    with mock.patch("time.sleep"):
        worker.start()


def user_agent(worker):
    return worker._connector.session.headers["User-Agent"]


def test_a_given_identity_is_used_without_asking_docapi(tmp_path):
    worker = make_worker(tmp_path, identity="OCR worker\nX-Injected: 1")
    assert user_agent(worker) == "OCR worker X-Injected: 1"  # One line, no extra header.
    run(worker)
    worker.adapter.get_me.assert_not_called()
    assert worker.identity == "OCR worker\nX-Injected: 1"


def test_without_an_identity_the_key_label_is_used(tmp_path):
    worker = make_worker(tmp_path, me("OCR worker prod"))
    assert (worker.identity, user_agent(worker)) == ("Worker", "Worker")  # Until DocAPI answers.
    run(worker, passes=3)
    assert (worker.identity, user_agent(worker)) == ("OCR worker prod", "OCR worker prod")
    worker.adapter.get_me.assert_called_once()


@pytest.mark.parametrize("failure", [requests.ConnectionError("refused"), me(status=503)])
def test_the_label_is_read_once_docapi_answers(tmp_path, caplog, failure):
    worker = make_worker(tmp_path, failure, failure, me("OCR worker prod"))
    run(worker, passes=4)
    assert worker.identity == "OCR worker prod"
    assert worker.adapter.get_me.call_count == 3
    assert sum("Could not read the API key's label" in record.message for record in caplog.records) == 1


def test_an_answer_without_a_label_keeps_the_class_name(tmp_path):
    worker = make_worker(tmp_path, me(""))
    run(worker, passes=3)
    assert worker.identity == "Worker"
    worker.adapter.get_me.assert_called_once()


def test_the_identity_replaces_a_connector_user_agent(tmp_path):
    worker = make_worker(tmp_path, me("OCR worker prod"), connector=Connector(api_key="k", user_agent="myapp/2"))
    assert user_agent(worker) == "Worker"
    run(worker)
    assert user_agent(worker) == "OCR worker prod"


def test_the_startup_mail_uses_the_key_label(tmp_path):
    worker = make_worker(tmp_path, me("OCR worker prod"), mail_sender=mock.Mock(),
                         mail_receivers=["admin@example.org"], hostname="host",
                         alive_mail_interval_seconds=0)
    run(worker)
    subject = worker.worker_mail_sender.sender.send.call_args.args[0]
    assert subject == "[OCR worker prod] ALIVE (host)"
