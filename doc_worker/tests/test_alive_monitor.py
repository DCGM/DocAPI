"""The ALIVE reports: AliveMonitor at startup and at an interval, without a worker."""
import types
from unittest import mock

import pytest

from doc_worker.alive_monitor import AliveMonitor
from doc_worker.worker_mail import WorkerMail


def _mail(**kwargs):
    return WorkerMail(mock.Mock(), **{"receivers": ["admin@example.org"], "identity": "worker abc",
                                      "hostname": "host", **kwargs})


def _sent(mail):
    return [call.args[:2] for call in mail.sender.send.call_args_list]


@pytest.fixture
def alive():
    return AliveMonitor(_mail())


def test_startup_mail_and_periodic_alive_reports(alive):
    with mock.patch("threading.Thread") as thread:
        alive.start()
    [(subject, _)] = _sent(alive.mail)
    assert subject == "[worker abc] ALIVE (host)"
    thread.assert_called_once_with(target=alive._run, name=AliveMonitor.THREAD_NAME, daemon=True)
    thread.return_value.start.assert_called_once()
    alive._stop = mock.Mock()
    alive._stop.wait.side_effect = [False, True]
    alive.pipeline_returned()
    alive.active_job = lambda: "job-1"
    alive._run()
    subject, body = _sent(alive.mail)[-1]
    assert "ALIVE" in subject
    assert "last job" in body and "active job" in body and "job-1" in body
    alive.close()
    alive._stop.set.assert_called_once()
    thread.return_value.join.assert_called_once()


def test_alive_mail_can_be_disabled(alive):
    alive.mail_startup = False
    alive.mail_interval_seconds = 0
    with mock.patch("threading.Thread") as thread:
        alive.start()
    alive.mail.sender.send.assert_not_called()
    thread.assert_not_called()


@pytest.mark.parametrize("mail", [None, WorkerMail(mock.Mock())])
def test_without_mail_or_receivers_nothing_is_sent_or_started(mail):
    alive = AliveMonitor(mail)
    assert not alive.on
    with mock.patch("threading.Thread") as thread:
        alive.start()
    thread.assert_not_called()
    if mail is not None:
        mail.sender.send.assert_not_called()


@pytest.mark.parametrize("value", [-1, float("nan"), float("inf")])
def test_invalid_intervals_are_rejected(value):
    with pytest.raises(ValueError, match="mail_interval_seconds"):
        AliveMonitor(_mail(), mail_interval_seconds=value)


def test_alive_mail_identifies_idle_state(alive):
    alive.send_alive()
    body = _sent(alive.mail)[0][1]
    assert "none active" in body and "job-1" not in body


def test_startup_and_periodic_reports_share_the_same_statistics(alive, tmp_path):
    from datetime import datetime, timedelta, timezone
    from doc_worker.doc_worker_wrapper import JobHistory
    history = JobHistory(tmp_path / "history.jsonl")
    now = datetime.now(timezone.utc)
    history.record(types.SimpleNamespace(id="job-1", images=[]), started_at=now - timedelta(seconds=10),
                   finished_at=now, duration_seconds=10, status="ok")
    alive.job_history = lambda: JobHistory(history.path)
    alive.mail_interval_seconds = 0
    alive.start()
    alive.send_alive()
    startup, periodic = _sent(alive.mail)
    assert startup[0] == periodic[0] and "ALIVE" in startup[0]
    for body in (startup[1], periodic[1]):
        for label in ("all time", "last 7 days", "last 24 hours"):
            section = body.split("\n" + label + "\n", 1)[1].split("\n\n", 1)[0]
            rows = [" ".join(line.split()) for line in section.splitlines()]
            assert "attempts 1" in rows and "unique jobs 1" in rows and "ok 1" in rows


def test_corrupt_history_does_not_prevent_alive_mail(alive):
    history = mock.Mock()
    history.statistics.side_effect = ValueError("corrupt history")
    alive.job_history = lambda: history
    alive.send_alive()
    assert "unavailable; inspect worker log" in _sent(alive.mail)[0][1]


def test_alive_report_wraps_description_and_uses_compact_durations(alive):
    alive.mail.identity = "OCR worker with a long descriptive identity for a development instance"
    alive.started = 0
    alive.last_pipeline_return = 172555
    with mock.patch("time.monotonic", return_value=172800):
        alive.send_alive()
    body = _sent(alive.mail)[0][1]
    assert "2d 0h 0m 0s" in body and "4m 5s ago" in body
    assert all(len(line) <= 60 for line in body.splitlines())


def test_a_failing_alive_report_never_raises(alive):
    alive.mail.sender.send.side_effect = OSError("smtp unavailable")
    alive.send_alive()
