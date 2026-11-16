"""The worker mails a DocAPI outage once, with reminders and a reconnect notice.

The real Adapter talks to a fake connector, so every request goes through the
worker's reachability check as it does in production.
"""
import types
from unittest import mock

import pytest
import requests

from doc_worker.doc_worker_wrapper import DocWorkerWrapper, WorkerResponse


class FakeResponse:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = {"Content-Type": "application/json"}
        self.text = str(payload)

    def json(self):
        return self._payload


class FakeServer:
    """A connector whose server is up, down (no connection) or answers with
    one status for every request."""

    def __init__(self):
        self.state = "up"
        self.report_status = 200
        self.calls = []

    def _answer(self, method, url):
        self.calls.append((method, url.split("/v1/", 1)[1]))
        if self.state == "down":
            raise requests.ConnectionError("Connection refused")
        if isinstance(self.state, int):
            return FakeResponse(self.state)
        if method == "PATCH":
            return FakeResponse(self.report_status, {"status": self.report_status, "code": "JOB_UPDATED",
                                                     "detail": "updated"})
        return FakeResponse(200, {"status": 200, "code": "JOB_QUEUE_EMPTY", "detail": "No job"})

    def post(self, url, **kwargs):
        return self._answer("POST", url)

    def patch(self, url, **kwargs):
        return self._answer("PATCH", url)

    def get(self, url, **kwargs):
        return self._answer("GET", url)


class Worker(DocWorkerWrapper):
    def process_job(self, *args, **kwargs):
        return WorkerResponse.ok()


@pytest.fixture
def clock(monkeypatch):
    now = [1000.0]
    monkeypatch.setattr("doc_worker.doc_worker_wrapper.time.monotonic", lambda: now[0])
    return now


@pytest.fixture
def server():
    return FakeServer()


@pytest.fixture
def worker(tmp_path, server, clock):
    return Worker("http://docapi.example.org/", server, base_dir=str(tmp_path), mail_sender=mock.Mock(),
                  mail_receivers=["admin@example.org"], identity="OCR worker", hostname="host",
                  disconnected_grace_seconds=60, disconnected_mail_interval_seconds=3600)


def _mails(worker):
    # Bodies with their wrapped lines joined.
    return [(call.args[0], " ".join(call.args[1].split())) for call in worker.mail_sender.send.call_args_list]


def _subjects(worker):
    return [subject for subject, _ in _mails(worker)]


def test_a_blip_while_idle_within_the_grace_period_sends_nothing(worker, server, clock):
    server.state = "down"
    assert worker.run_job_pipeline() is None
    clock[0] += 59
    assert worker.run_job_pipeline() is None
    server.state = "up"
    assert worker.run_job_pipeline() is None
    assert _subjects(worker) == []


def test_a_long_idle_outage_is_mailed_once_with_reminders_and_a_reconnect(worker, server, clock):
    server.state = "down"
    for step in (0, 30, 30):  # Polls at 0, 30 and 60 s: DISCONNECTED once the grace is over.
        clock[0] += step
        assert worker.run_job_pipeline() is None
    assert _subjects(worker) == ["[OCR worker] DISCONNECTED (host)"]
    body = _mails(worker)[0][1]
    assert "DocAPI cannot be reached." in body
    assert "POST /v1/jobs/lease" in body and "ConnectionError: Connection refused" in body
    assert "job-" not in body

    # Polls in between are only logged; the reminder follows the interval.
    clock[0] += 3599
    worker.run_job_pipeline()
    assert len(_subjects(worker)) == 1
    clock[0] += 1
    worker.run_job_pipeline()
    assert _subjects(worker)[-1] == "[OCR worker] STILL DISCONNECTED (host)"
    reminder = _mails(worker)[-1][1]
    # Sent an interval after DISCONNECTED, which went out 60 s into the outage.
    assert "DocAPI is still unreachable." in reminder and "for 1h 1m 0s" in reminder and "5 requests" in reminder

    server.state = "up"
    worker.run_job_pipeline()
    assert _subjects(worker)[-1] == "[OCR worker] RECONNECTED (host)"
    reconnected = _mails(worker)[-1][1]
    assert "DocAPI is reachable again." in reconnected and "lasted 1h 1m 0s" in reconnected
    assert "5 requests" in reconnected
    worker.run_job_pipeline()
    assert len(_subjects(worker)) == 3


@pytest.mark.parametrize("status", [502, 503, 504])
def test_gateway_errors_count_as_an_outage(worker, server, clock, status):
    server.state = status
    worker.run_job_pipeline()
    clock[0] += 60
    worker.run_job_pipeline()
    assert _subjects(worker) == ["[OCR worker] DISCONNECTED (host)"]
    assert f"HTTP {status}" in _mails(worker)[0][1]


@pytest.mark.parametrize("status", [401, 500])
def test_other_errors_are_not_an_outage(worker, server, status):
    server.state = status
    worker.run_job_pipeline()
    # The usual failure mail, at once and without the grace period.
    assert _subjects(worker) == ["[OCR worker] FAILURE (host)"]


def _lose_a_job(worker, server, response):
    """A job's outcome report cannot reach DocAPI."""
    worker.current_job = types.SimpleNamespace(id="job-1")
    server.state = "down"
    assert worker._report_job_response(response) is False
    worker.current_job = None


def test_an_outage_during_a_job_is_mailed_at_once_and_its_outcome_reported_first(worker, server):
    _lose_a_job(worker, server, WorkerResponse.error(final_log="download failed"))
    # No grace period during a job.
    assert _subjects(worker) == ["[OCR worker] DISCONNECTED (host)"]
    assert "job-1: it cannot be finished" in _mails(worker)[0][1]
    assert "PATCH /v1/jobs/job-1" in _mails(worker)[0][1]

    # While DocAPI stays down the worker retries the report and does not poll.
    server.calls.clear()
    assert worker.run_job_pipeline() is None
    assert server.calls == [("PATCH", "jobs/job-1")]

    server.state = "up"
    server.calls.clear()
    worker.run_job_pipeline()
    # The outcome goes first, then the worker polls again.
    assert server.calls == [("PATCH", "jobs/job-1"), ("POST", "jobs/lease")]
    assert _subjects(worker) == ["[OCR worker] DISCONNECTED (host)", "[OCR worker] RECONNECTED (host)"]
    assert "job-1: reported as error after the outage" in _mails(worker)[-1][1]


def test_a_processed_job_whose_finish_was_lost_is_reported_done(worker, server):
    _lose_a_job(worker, server, WorkerResponse.ok(final_log="all pages"))
    server.state = "up"
    worker.run_job_pipeline()
    assert "job-1: reported as ok after the outage" in _mails(worker)[-1][1]


def test_an_outcome_the_server_no_longer_accepts_changes_nothing(worker, server):
    _lose_a_job(worker, server, WorkerResponse.error(final_log="upload failed"))
    server.state, server.report_status = "up", 409
    worker.run_job_pipeline()
    reconnected = _mails(worker)[-1][1]
    assert "was not accepted (status 409" in reconnected and "already requeued or" in reconnected
    # It is not retried.
    server.calls.clear()
    worker.run_job_pipeline()
    assert server.calls == [("POST", "jobs/lease")]


def test_only_unreachable_reports_are_held(worker, server):
    worker.current_job = types.SimpleNamespace(id="job-1")
    server.state = 500
    assert worker._report_job_response(WorkerResponse.error(final_log="failed")) is False
    assert worker._lost_report is None
    assert _subjects(worker) == ["[OCR worker] FAILURE (host)"]


@pytest.mark.parametrize("settings", [dict(disconnected_grace_seconds=-1),
                                      dict(disconnected_grace_seconds=float("inf")),
                                      dict(disconnected_mail_interval_seconds=0),
                                      dict(disconnected_mail_interval_seconds=float("nan"))])
def test_invalid_outage_settings_are_rejected(tmp_path, settings):
    with pytest.raises(ValueError):
        Worker("http://docapi.example.org/", FakeServer(), base_dir=str(tmp_path), **settings)
