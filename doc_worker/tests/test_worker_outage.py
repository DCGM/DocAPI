"""The worker mails a DocAPI outage once, with reminders and a reconnect notice.

The real Adapter talks to a fake connector, so every request goes through the
worker's reachability check as it does in production.
"""
import os
import types
from datetime import datetime
from unittest import mock

import pytest
import requests

from doc_worker.doc_worker_wrapper import DocWorkerWrapper, WorkerResponse
from doc_worker.outage_monitor import OutageMonitor, SettledJob
from doc_worker.worker_mail_sender import WorkerMailSender


class FakeResponse:
    def __init__(self, status_code, payload=None, content_type="application/json"):
        self.status_code = status_code
        self._payload = payload
        self.headers = {"Content-Type": content_type}
        self.text = str(payload)

    def json(self):
        if not self.headers["Content-Type"].startswith("application/json"):
            raise ValueError("not JSON")
        return self._payload


# What a reverse proxy answers when it has no route to DocAPI, e.g. while
# DocAPI is shut down.
PROXY_404 = FakeResponse(404, "404 page not found", content_type="text/plain; charset=utf-8")
DOCAPI_404 = FakeResponse(404, {"status": 404, "code": "JOB_NOT_FOUND", "detail": "Job not found"})


class FakeServer:
    """A connector whose server is up, down (no connection), too slow for every
    request ("timeout"), or answers every request with one status or one
    FakeResponse. upload_status "timeout" makes only the result upload too slow."""

    def __init__(self):
        self.state = "up"
        self.report_status = 200
        self.upload_status = 200
        self.calls = []
        self.reported_states = []

    def _answer(self, method, url):
        self.calls.append((method, url.split("/v1/", 1)[1]))
        if self.state == "down":
            raise requests.ConnectionError("Connection refused")
        if self.state == "timeout":
            raise requests.ReadTimeout("Read timed out")
        if isinstance(self.state, int):
            return FakeResponse(self.state)
        if isinstance(self.state, FakeResponse):
            return self.state
        if method == "PATCH":
            return self._status(self.report_status, "JOB_UPDATED")
        if method == "POST" and url.endswith("/result"):
            if self.upload_status == "timeout":
                raise requests.ReadTimeout("Read timed out")
            return self._status(self.upload_status, "RESULT_UPLOADED")
        return FakeResponse(200, {"status": 200, "code": "JOB_QUEUE_EMPTY", "detail": "No job"})

    @staticmethod
    def _status(status, code):
        """A DocAPI answer, or the proxy's for a status it sends instead of DocAPI."""
        if status in (502, 503, 504):
            return FakeResponse(status)
        return FakeResponse(status, {"status": status, "code": code, "detail": code})

    def set_user_agent(self, user_agent):
        self.user_agent = user_agent

    def post(self, url, **kwargs):
        return self._answer("POST", url)

    def patch(self, url, **kwargs):
        self.reported_states.append(kwargs.get("json", {}).get("state"))
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
                  outage_grace_seconds=60, outage_mail_interval_seconds=3600)


def _mails(worker):
    # Bodies with their wrapped lines joined.
    calls = worker.worker_mail_sender.sender.send.call_args_list
    return [(call.args[0], " ".join(call.args[1].split())) for call in calls]


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
    assert "DocAPI is still unreachable." in reminder and "for 1h 1m 0s" in reminder and "failed" not in reminder

    server.state = "up"
    worker.run_job_pipeline()
    assert _subjects(worker)[-1] == "[OCR worker] RECONNECTED (host)"
    reconnected = _mails(worker)[-1][1]
    assert "DocAPI is reachable again." in reconnected and "lasted 1h 1m 0s" in reconnected
    assert "failed" not in reconnected and "job id" not in reconnected
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


def test_a_proxy_404_counts_as_an_outage(worker, server, clock):
    server.state = PROXY_404
    for step in (0, 30, 30):
        clock[0] += step
        assert worker.run_job_pipeline() is None
    # No FAILURE: one DISCONNECTED once the grace period is over.
    assert _subjects(worker) == ["[OCR worker] DISCONNECTED (host)"]
    assert "HTTP 404 without a DocAPI code" in _mails(worker)[0][1]

    server.state = "up"
    worker.run_job_pipeline()
    assert _subjects(worker)[-1] == "[OCR worker] RECONNECTED (host)"


def test_a_proxy_404_within_the_grace_period_sends_nothing(worker, server, clock):
    server.state = PROXY_404
    worker.run_job_pipeline()
    clock[0] += 59
    worker.run_job_pipeline()
    server.state = "up"
    worker.run_job_pipeline()
    assert _subjects(worker) == []


@pytest.mark.parametrize("response", [
    DOCAPI_404,  # DocAPI's own 404 has a code.
    FakeResponse(401), FakeResponse(500),
    # Errors without a code other than 404 may be permanent.
    FakeResponse(413, "Request Entity Too Large", content_type="text/html"),
    FakeResponse(429, "Too Many Requests", content_type="text/plain"),
])
def test_other_errors_are_not_an_outage(worker, server, response):
    server.state = response
    worker.run_job_pipeline()
    # The usual failure mail, at once and without the grace period.
    assert _subjects(worker) == ["[OCR worker] FAILURE (host)"]


def _job():
    return types.SimpleNamespace(
        id="job-1", created_date=None, started_date=datetime(2026, 11, 19, 12, 0), previous_attempts=0,
        engine_name="engine", engine_version="v1", images=[], alto_required=False, page_required=False,
        meta_json_required=False)


def _lose_a_job(worker, server, response):
    """A job's outcome report cannot reach DocAPI."""
    worker.current_job = _job()
    server.state = "down"
    assert worker._report_job_response(response) is False
    worker.current_job = None


def test_an_outage_during_a_job_is_mailed_at_once_and_its_outcome_reported_first(worker, server):
    _lose_a_job(worker, server, WorkerResponse.error(final_log="download failed"))
    # No grace period during a job.
    assert _subjects(worker) == ["[OCR worker] DISCONNECTED (host)"]
    assert "job id job-1 Its results and outcome are sent once DocAPI is reachable again" in _mails(worker)[0][1]
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
    assert "job id job-1 outcome reported as error" in _mails(worker)[-1][1]


def test_a_processed_job_whose_finish_was_lost_is_reported_done(worker, server):
    _lose_a_job(worker, server, WorkerResponse.ok(final_log="all pages"))
    server.state = "up"
    worker.run_job_pipeline()
    assert "job id job-1 outcome reported as ok" in _mails(worker)[-1][1]


def test_an_outcome_the_server_no_longer_accepts_changes_nothing(worker, server):
    _lose_a_job(worker, server, WorkerResponse.error(final_log="upload failed"))
    server.state, server.report_status = "up", 409
    worker.run_job_pipeline()
    reconnected = _mails(worker)[-1][1]
    assert "job id job-1 outcome error, not accepted by DocAPI (status 409" in reconnected
    assert "DocAPI had already requeued or failed the job" in reconnected
    # It is not retried.
    server.calls.clear()
    worker.run_job_pipeline()
    assert server.calls == [("POST", "jobs/lease")]


def test_an_outcome_report_answered_by_the_proxy_is_held(worker, server):
    worker.current_job = _job()
    server.state = PROXY_404
    assert worker._report_job_response(WorkerResponse.ok(final_log="all pages")) is False
    worker.current_job = None
    assert worker._lost_report is not None
    assert _subjects(worker) == ["[OCR worker] DISCONNECTED (host)"]

    server.state = "up"
    worker.run_job_pipeline()
    assert "job id job-1 outcome reported as ok" in _mails(worker)[-1][1]


@pytest.mark.parametrize("status", [500, 404])
def test_only_unreachable_reports_are_held(worker, server, status):
    worker.current_job = _job()
    server.state = DOCAPI_404 if status == 404 else status
    assert worker._report_job_response(WorkerResponse.error(final_log="failed")) is False
    assert worker._lost_report is None
    assert _subjects(worker) == ["[OCR worker] FAILURE (host)"]


def test_a_request_that_times_out_is_an_outage(tmp_path, clock):
    import socket
    from doc_api.connector import Connector
    # A local server that accepts the connection and never answers.
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        worker = Worker(f"http://127.0.0.1:{listener.getsockname()[1]}/", Connector("key", timeout=(1, 0.2)),
                        base_dir=str(tmp_path), mail_sender=mock.Mock(), mail_receivers=["admin@example.org"],
                        identity="OCR worker", hostname="host", outage_grace_seconds=0)
        assert worker.run_job_pipeline() is None
    assert _subjects(worker) == ["[OCR worker] DISCONNECTED (host)"]
    assert "ReadTimeout" in _mails(worker)[0][1]


@pytest.mark.parametrize("settings", [dict(outage_grace_seconds=-1),
                                      dict(outage_grace_seconds=float("inf")),
                                      dict(outage_mail_interval_seconds=0),
                                      dict(outage_mail_interval_seconds=float("nan"))])
def test_invalid_outage_settings_are_rejected(tmp_path, settings):
    with pytest.raises(ValueError):
        Worker("http://docapi.example.org/", FakeServer(), base_dir=str(tmp_path), **settings)


# --- OutageMonitor on its own ---

def test_an_outage_without_mail_is_only_logged(caplog):
    outages = OutageMonitor(None, grace_seconds=0)
    outages.unreachable("GET /v1/me", "ConnectionError: refused")
    outages.reached()
    assert "DocAPI unreachable: GET /v1/me" in caplog.text and "DocAPI is reachable again" in caplog.text


def test_reconnected_waits_for_a_pending_report():
    pending = [True]
    mail = WorkerMailSender(mock.Mock(), ["admin@example.org"], identity="worker abc", hostname="host")
    outages = OutageMonitor(mail, grace_seconds=0, report_pending=lambda: pending[0])
    outages.unreachable("PATCH /v1/jobs/job-1/finish", "HTTP 503")
    outages.reached()
    assert [call.args[0] for call in mail.sender.send.call_args_list] == ["[worker abc] DISCONNECTED (host)"]
    pending[0] = False
    outages.report_settled(SettledJob("job-1", "ok"))
    subject, body = mail.sender.send.call_args.args[:2]
    assert subject == "[worker abc] RECONNECTED (host)"
    assert "job id job-1 outcome reported as ok" in " ".join(body.split())


@pytest.mark.parametrize("settings", [dict(grace_seconds=-1), dict(mail_interval_seconds=0)])
def test_invalid_outage_timing_is_rejected(settings):
    with pytest.raises(ValueError):
        OutageMonitor(None, **settings)


# --- Results whose upload an outage cut off ---

class PipelineWorker(Worker):
    """Leases one job without DocAPI; the server goes down while it is processed."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.outage_during_processing = None
        self.leased = False

    def request_job(self):
        if self.leased:
            return None
        self.leased = True
        self.current_job = _job()
        return WorkerResponse.ok()

    def _check_and_download_engine_files(self):
        return WorkerResponse.ok()

    def _download_job_data(self):
        os.makedirs(os.path.join(self.get_job_data_path(), "images"), exist_ok=True)
        return WorkerResponse.ok()

    def get_engine_data_path(self):
        return None

    def process_job(self, job, images_dir, result_dir, **kwargs):
        with open(os.path.join(result_dir, "page.txt"), "w") as file:
            file.write("text")
        self.server.state = self.outage_during_processing
        return WorkerResponse.ok(final_log="all pages")


@pytest.fixture
def pipeline(tmp_path, server, clock):
    worker = PipelineWorker("http://docapi.example.org/", server, base_dir=str(tmp_path),
                            mail_sender=mock.Mock(), mail_receivers=["admin@example.org"], identity="OCR worker",
                            hostname="host", cleanup_job_dir=True)
    worker.server = server
    return worker


def _job_dirs(worker):
    return os.listdir(worker.jobs_dir)


@pytest.mark.parametrize("outage", ["down", 502, PROXY_404])
def test_results_an_outage_kept_from_docapi_are_uploaded_after_it_without_processing_again(pipeline, server,
                                                                                          outage):
    pipeline.outage_during_processing = outage
    assert pipeline.run_job_pipeline() is None
    # Neither an error nor anything else is reported for the job, and its results are kept.
    assert server.calls == [("POST", "jobs/job-1/result")]
    assert _subjects(pipeline) == ["[OCR worker] DISCONNECTED (host)"]
    assert _job_dirs(pipeline)

    # While DocAPI stays unreachable only the upload is retried.
    server.calls.clear()
    assert pipeline.run_job_pipeline() is None
    assert server.calls == [("POST", "jobs/job-1/result")]

    server.state = "up"
    server.calls.clear()
    pipeline.run_job_pipeline()
    assert server.calls == [("POST", "jobs/job-1/result"), ("PATCH", "jobs/job-1")]
    assert server.reported_states == ["done"]
    assert _subjects(pipeline)[-1] == "[OCR worker] RECONNECTED (host)"
    assert "job id job-1 results uploaded after the outage outcome reported as ok" in _mails(pipeline)[-1][1]
    assert _job_dirs(pipeline) == []


def test_uploaded_results_are_not_uploaded_again_when_the_report_meets_another_outage(pipeline, server):
    pipeline.outage_during_processing = "down"
    pipeline.run_job_pipeline()
    server.state, server.report_status = "up", 502
    server.calls.clear()
    assert pipeline.run_job_pipeline() is None
    assert server.calls == [("POST", "jobs/job-1/result"), ("PATCH", "jobs/job-1")]

    server.report_status = 200
    server.calls.clear()
    pipeline.run_job_pipeline()
    assert server.calls == [("PATCH", "jobs/job-1")]
    assert "job id job-1 outcome reported as ok" in _mails(pipeline)[-1][1]


def test_results_docapi_rejects_after_the_outage_are_reported_as_an_error_not_uploaded_again(pipeline, server):
    pipeline.outage_during_processing = "down"
    pipeline.run_job_pipeline()
    server.state, server.upload_status = "up", 409
    server.calls.clear()
    pipeline.run_job_pipeline()
    assert server.calls == [("POST", "jobs/job-1/result"), ("PATCH", "jobs/job-1")]
    assert server.reported_states == ["error"]
    reconnected = _mails(pipeline)[-1][1]
    assert "job id job-1 results not taken by DocAPI (status 409" in reconnected
    assert "the job is reported as error instead outcome reported as error" in reconnected
    assert _job_dirs(pipeline)
    assert pipeline._lost_report is None


def test_an_outcome_docapi_rejects_after_the_upload_keeps_the_job_directory(pipeline, server):
    pipeline.outage_during_processing = "down"
    pipeline.run_job_pipeline()
    server.state, server.report_status = "up", 409
    pipeline.run_job_pipeline()
    reconnected = _mails(pipeline)[-1][1]
    assert "results uploaded after the outage outcome ok, not accepted by DocAPI (status 409" in reconnected
    assert _job_dirs(pipeline)


def test_results_a_reachable_docapi_rejects_are_a_retryable_error(pipeline, server):
    server.upload_status = 413
    assert pipeline.run_job_pipeline() is None
    assert server.calls == [("POST", "jobs/job-1/result"), ("PATCH", "jobs/job-1")]
    assert server.reported_states == ["error"]
    assert pipeline._lost_report is None
    assert _job_dirs(pipeline)


def test_a_job_whose_finish_was_lost_is_cleaned_up_once_reported(pipeline, server):
    server.report_status = 502
    assert pipeline.run_job_pipeline() is None
    assert server.calls == [("POST", "jobs/job-1/result"), ("PATCH", "jobs/job-1")]
    assert _job_dirs(pipeline)

    server.report_status = 200
    server.calls.clear()
    pipeline.run_job_pipeline()
    assert server.calls == [("PATCH", "jobs/job-1")]
    assert "job id job-1 outcome reported as ok" in _mails(pipeline)[-1][1]
    assert _job_dirs(pipeline) == []


def test_a_held_error_keeps_the_job_directory(pipeline, server):
    pipeline.process_job = lambda **kwargs: WorkerResponse.error(final_log="bad pages")
    server.report_status = 502
    pipeline.run_job_pipeline()
    server.report_status = 200
    pipeline.run_job_pipeline()
    assert "job id job-1 outcome reported as error" in _mails(pipeline)[-1][1]
    assert _job_dirs(pipeline)


# --- A request that was only slow is not an outage ---

@pytest.mark.parametrize("slow", ["timeout", 504])
def test_a_slow_upload_while_docapi_answers_is_an_error_not_an_outage(pipeline, server, slow):
    server.upload_status = slow
    assert pipeline.run_job_pipeline() is None
    # The probe finds DocAPI answering, so the job is reported at once.
    assert server.calls == [("POST", "jobs/job-1/result"), ("GET", "me"), ("PATCH", "jobs/job-1")]
    assert server.reported_states == ["error"]
    assert pipeline._lost_report is None
    assert _subjects(pipeline) == []


def test_a_slow_upload_after_an_outage_is_reported_as_an_error(pipeline, server):
    pipeline.outage_during_processing = "down"
    pipeline.run_job_pipeline()
    server.state, server.upload_status = "up", "timeout"
    server.calls.clear()
    pipeline.run_job_pipeline()
    assert server.calls == [("POST", "jobs/job-1/result"), ("GET", "me"), ("PATCH", "jobs/job-1")]
    assert server.reported_states == ["error"]
    reconnected = _mails(pipeline)[-1][1]
    assert "job id job-1 results could not be uploaded (ReadTimeout: Read timed out)" in reconnected
    assert "the job is reported as error instead outcome reported as error" in reconnected
    # The upload is not tried again.
    server.calls.clear()
    pipeline.run_job_pipeline()
    assert ("POST", "jobs/job-1/result") not in server.calls


def test_a_timeout_docapi_does_not_answer_the_probe_for_is_an_outage(worker, server, clock):
    server.state = "timeout"
    worker.run_job_pipeline()
    assert server.calls == [("POST", "jobs/lease"), ("GET", "me")]
    clock[0] += 60
    worker.run_job_pipeline()
    assert _subjects(worker) == ["[OCR worker] DISCONNECTED (host)"]
    assert "ReadTimeout" in _mails(worker)[0][1]


def test_the_probe_waits_at_most_its_own_read_timeout(tmp_path):
    connector = mock.Mock(timeout=(10, 300))
    connector.post.side_effect = requests.ReadTimeout("Read timed out")
    connector.get.return_value = FakeResponse(200, {"status": 200, "code": "USER_INFO", "detail": "me"})
    worker = Worker("http://docapi.example.org/", connector, base_dir=str(tmp_path), identity="OCR worker")
    with pytest.raises(requests.ReadTimeout) as slow:
        worker.adapter.connector.post("http://docapi.example.org/v1/jobs/job-1/result")
    connector.get.assert_called_once_with("http://docapi.example.org/v1/me", timeout=(10, 30))
    # DocAPI answered the probe, so the timeout is not marked as an outage.
    assert not worker._unreachable(slow.value)

    connector.get.side_effect = requests.ReadTimeout("Read timed out")
    with pytest.raises(requests.ReadTimeout) as outage:
        worker.adapter.connector.post("http://docapi.example.org/v1/jobs/job-1/result")
    assert worker._unreachable(outage.value)
