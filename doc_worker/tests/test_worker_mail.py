"""The worker's own mail: WorkerMail, and the wrapper's mail and failure mail."""
import types
from unittest import mock

import pytest

from doc_worker.alive_monitor import AliveMonitor
from doc_worker.worker_mail import WorkerMail


def _mail(**kwargs):
    return WorkerMail(mock.Mock(), **{"receivers": ["admin@example.org"], "identity": "worker abc",
                                      "hostname": "host", **kwargs})


def _sent(mail):
    """The (subject, body) of each email sent, in order."""
    return [call.args[:2] for call in mail.sender.send.call_args_list]


@pytest.fixture
def wrapper(tmp_path):
    from doc_worker.doc_worker_wrapper import DocWorkerWrapper, WorkerResponse

    class Worker(DocWorkerWrapper):
        def process_job(self, *args, **kwargs):
            return WorkerResponse.ok()

    worker = Worker("http://localhost", mock.Mock(), base_dir=str(tmp_path), mail_sender=mock.Mock(),
                    mail_receivers=["admin@example.org"])
    worker.adapter = mock.Mock()
    worker.adapter.patch_job_error.return_value = types.SimpleNamespace(is_success=True)
    worker.adapter.patch_job_fail.return_value = types.SimpleNamespace(is_success=True)
    return worker


# --- WorkerMail ---

def test_worker_mail_has_the_subject_and_a_header():
    mail = _mail(subject="{status} | {identity} | {hostname} | {time:%Y}")
    mail.send("FAILURE", "cannot connect")
    [(subject, body)] = _sent(mail)
    assert subject.startswith("FAILURE | worker abc | host | 20")
    rows = [" ".join(line.split()) for line in body.splitlines()]
    assert rows[:2] == ["worker worker abc", "hostname host"] and rows[3] == "status FAILURE"
    assert rows[-1] == "cannot connect"


def test_worker_mail_goes_to_each_receiver_separately():
    mail = _mail(receivers=["a@example.org", "b@example.org"])
    mail.send("ALIVE", "fine")
    assert [call.args[2] for call in mail.sender.send.call_args_list] == ["a@example.org", "b@example.org"]


def test_worker_mail_without_receivers_is_off(caplog):
    mail = _mail(receivers=[])
    assert "Worker mail is off: the worker has a mail sender but no mail receivers" in caplog.text
    mail.send("FAILURE", "cannot connect")
    mail.sender.send.assert_not_called()


def test_a_failing_send_never_raises():
    mail = _mail()
    mail.sender.send.side_effect = OSError("smtp unavailable")
    mail.send("FAILURE", "cannot connect")
    mail.sender.send.assert_called_once()


def test_an_invalid_subject_is_rejected():
    with pytest.raises(KeyError, match="unknown"):
        _mail(subject="{unknown}")


# --- The wrapper's mail ---

def test_alive_mail_is_on_by_default_with_mail(wrapper):
    assert wrapper.alive.on and (wrapper.alive.mail_startup, wrapper.alive.mail_interval_seconds) == (True, 86400)
    wrapper.run_job_pipeline = mock.Mock(side_effect=[None, KeyboardInterrupt])
    with mock.patch("time.sleep"), mock.patch("threading.Thread") as thread:
        wrapper.start()
    assert [subject for subject, _ in _sent(wrapper.mail)] == [f"[{wrapper.identity}] ALIVE ({wrapper.mail.hostname})"]
    thread.return_value.start.assert_called_once()
    thread.return_value.join.assert_called_once()  # Stopped with the worker.


def test_alive_mail_can_be_turned_off(wrapper, tmp_path):
    worker = type(wrapper)("http://localhost", mock.Mock(), base_dir=str(tmp_path), mail_sender=mock.Mock(),
                           mail_receivers=["admin@example.org"], identity="My worker",
                           alive_mail_startup=False, alive_mail_interval_seconds=0)
    worker.run_job_pipeline = mock.Mock(side_effect=[None, KeyboardInterrupt])
    with mock.patch("time.sleep"), mock.patch("threading.Thread") as thread:
        worker.start()
    worker.mail.sender.send.assert_not_called()
    thread.assert_not_called()


def test_wrapper_builds_its_mail_and_monitors_from_its_arguments(wrapper, tmp_path):
    worker = type(wrapper)("http://localhost", mock.Mock(), base_dir=str(tmp_path),
                           mail_sender=mock.Mock(), mail_receivers=["admin@example.org"], identity="My worker",
                           alive_mail_interval_seconds=60, outage_grace_seconds=5, outage_mail_interval_seconds=600)
    assert worker.alive.mail is worker.mail and worker.alive.mail_interval_seconds == 60
    assert worker.outages.mail is worker.mail
    assert (worker.outages.grace_seconds, worker.outages.mail_interval_seconds) == (5, 600)
    assert (worker.identity, worker.mail.identity) == ("My worker", "My worker")


def test_a_new_identity_reaches_the_mail(wrapper):
    wrapper.identity = "OCR worker prod"
    assert wrapper.mail.identity == "OCR worker prod"


def test_a_worker_without_a_mail_sender_has_no_mail(wrapper, tmp_path):
    worker = type(wrapper)("http://localhost", mock.Mock(), base_dir=str(tmp_path))
    assert worker.mail is None and worker.outages.mail is None
    worker._notify_worker_failure("failure")  # Only logged.


@pytest.mark.parametrize("failure", [KeyboardInterrupt, RuntimeError, SystemExit])
def test_wrapper_loop_manages_alive_reports_and_reports_unexpected_termination(wrapper, failure):
    wrapper.alive = mock.Mock()
    wrapper.run_job_pipeline = mock.Mock(side_effect=[None, failure("loop broke")])
    with mock.patch("time.sleep"):
        if failure is KeyboardInterrupt:
            wrapper.start()
            wrapper.mail.sender.send.assert_not_called()
        else:
            with pytest.raises(failure):
                wrapper.start()
            wrapper.mail.sender.send.assert_called_once()
            body = wrapper.mail.sender.send.call_args.args[1]
            assert "loop broke" in body and "Traceback" in body
    wrapper.alive.start.assert_called_once()
    wrapper.alive.pipeline_returned.assert_called_once()
    wrapper.alive.close.assert_called_once()


def test_each_failure_is_mailed(wrapper):
    wrapper._notify_worker_failure("first")
    wrapper._notify_worker_failure("second")
    assert wrapper.mail.sender.send.call_count == 2
    assert "second" in wrapper.mail.sender.send.call_args.args[1]


def test_wrapper_reports_successful_job_errors_without_worker_alert(wrapper):
    from doc_worker.doc_worker_wrapper import WorkerResponse
    wrapper.current_job = types.SimpleNamespace(id="job-1")
    wrapper._report_job_response(WorkerResponse.error(final_log="technical", final_log_user="user"))
    wrapper.adapter.patch_job_error.assert_called_once_with(final_log="technical", final_log_user="user", job_id="job-1")
    wrapper.mail.sender.send.assert_not_called()


@pytest.mark.parametrize("raises", [False, True])
def test_wrapper_alerts_when_job_error_cannot_reach_docapi(wrapper, raises):
    from doc_worker.doc_worker_wrapper import WorkerResponse
    wrapper.current_job = types.SimpleNamespace(id="job-1")
    if raises:
        wrapper.adapter.patch_job_error.side_effect = OSError("unavailable")
    else:
        wrapper.adapter.patch_job_error.return_value = types.SimpleNamespace(is_success=False, status=500, code=None)
    wrapper._report_job_response(WorkerResponse.error(final_log="technical"))
    wrapper.mail.sender.send.assert_called_once()
    if raises:
        body = wrapper.mail.sender.send.call_args.args[1]
        assert "OSError: unavailable" in body and "Traceback" in body


@pytest.mark.parametrize("raises", [False, True])
def test_wrapper_alerts_when_polling_fails_without_a_job(wrapper, raises):
    if raises:
        wrapper.adapter.post_job_lease.side_effect = OSError("DocAPI unreachable")
    else:
        wrapper.adapter.post_job_lease.return_value = types.SimpleNamespace(
            is_success=False, data=None, status=500, code=None, response=None)
    with mock.patch.object(wrapper, "_report_job_response") as report:
        assert wrapper.run_job_pipeline() is None
    report.assert_not_called()
    wrapper.adapter.patch_job_error.assert_not_called()
    wrapper.mail.sender.send.assert_called_once()
    message = wrapper.mail.sender.send.call_args.args[1]
    assert ("Failed to request job" if raises else "Failed to request job lease") in message
    assert ("DocAPI unreachable" if raises else "500") in message
    assert "without a job ID" not in message
    assert wrapper.current_job is None and wrapper.current_lease is None


@pytest.mark.parametrize("raises", [False, True])
def test_wrapper_reports_job_fetch_failure_after_acquiring_a_lease(wrapper, raises):
    wrapper.adapter.post_job_lease.return_value = types.SimpleNamespace(
        is_success=True, data=types.SimpleNamespace(id="new-job", lease_expire_at=None))
    if raises:
        wrapper.adapter.get_job.side_effect = OSError("Cannot fetch job")
    else:
        wrapper.adapter.get_job.return_value = types.SimpleNamespace(
            is_success=False, data=None, status=503, code=None, response=None)
    assert wrapper.run_job_pipeline() is None
    wrapper.adapter.patch_job_error.assert_called_once()
    assert wrapper.adapter.patch_job_error.call_args.kwargs["job_id"] == "new-job"
    wrapper.mail.sender.send.assert_not_called()
    assert wrapper.current_job is None and wrapper.current_lease is None


def test_wrapper_clears_previous_job_before_new_poll(wrapper):
    wrapper.current_job = types.SimpleNamespace(id="old-job")
    wrapper.current_lease = types.SimpleNamespace(id="old-job")
    wrapper.adapter.post_job_lease.return_value = types.SimpleNamespace(is_success=False, data=None, status=503, code=None, response=None)
    assert wrapper.request_job() is not None
    assert wrapper.current_job is None and wrapper.current_lease is None


def test_wrapper_reports_a_failed_job_fetch_using_the_new_lease(wrapper):
    from doc_worker.doc_worker_wrapper import WorkerResponse
    wrapper.current_lease = types.SimpleNamespace(id="new-job")
    wrapper._report_job_response(WorkerResponse.error(final_log="Cannot fetch job"))
    assert wrapper.adapter.patch_job_error.call_args.kwargs["job_id"] == "new-job"


@pytest.mark.parametrize("settings", [dict(alive_mail_interval_seconds=-1), dict(outage_grace_seconds=-1)])
def test_invalid_monitor_settings_are_rejected_before_side_effects(wrapper, tmp_path, settings):
    directory = tmp_path / "not-created"
    with mock.patch("doc_worker.doc_worker_wrapper.Adapter") as adapter:
        with pytest.raises(ValueError):
            type(wrapper)("http://localhost", mock.Mock(), base_dir=str(directory), **settings)
    adapter.assert_not_called()
    assert not directory.exists()


def test_failure_mail_is_sent_as_written(wrapper):
    wrapper._notify_worker_failure("unknown page key: p1, contains_keyword=True")
    subject, body, _ = wrapper.mail.sender.send.call_args.args
    assert "FAILURE" in subject
    assert "unknown page key: p1, contains_keyword=True" in body


def test_mail_sender_subclass_filters_all_worker_mail(wrapper):
    from doc_api.tools.mail_sender import MailSender

    class RedactingMailSender(MailSender):
        def send(self, subject, body, receiver, headers=None):
            self.sent = (subject.replace("p1", "[id]"), body.replace("p1", "[id]"))
            return True
    wrapper.mail.sender = RedactingMailSender(server="smtp.example.org", sender_mail="worker@example.org")
    wrapper.identity = "host p1"
    wrapper._notify_worker_failure("page p1 failed")
    assert "p1" not in "".join(wrapper.mail.sender.sent)
    assert wrapper.send_mail("note p1", "body p1") is True
    assert wrapper.mail.sender.sent == ("note [id]", "body [id]")


def test_failure_mail_includes_exception_chain(wrapper):
    try:
        try:
            raise OSError("disk full at page_key=p1")
        except OSError as cause:
            raise RuntimeError("cannot save history") from cause
    except RuntimeError as exception:
        wrapper._notify_worker_failure("History failure", exception=exception)
    body = wrapper.mail.sender.send.call_args.args[1]
    assert "History failure" in body
    assert "OSError: disk full" in body
    assert "RuntimeError: cannot save history" in body
    assert "Traceback" in body
    assert "disk full at page_key=p1" in body


def test_wrapper_defaults_identity_and_detects_hostname(wrapper, tmp_path):
    with mock.patch("doc_worker.worker_mail.socket.gethostname", return_value="automatic-host"):
        worker = type(wrapper)("http://localhost", mock.Mock(), base_dir=str(tmp_path), mail_sender=mock.Mock())
    assert worker.identity == type(worker).__name__
    assert worker.mail.hostname == "automatic-host"


def test_default_subject_for_alive_and_failure(wrapper):
    wrapper.alive.send_alive()
    wrapper._notify_worker_failure("cannot connect")
    assert [subject for subject, _ in _sent(wrapper.mail)] == [
        f"[{wrapper.identity}] ALIVE ({wrapper.mail.hostname})",
        f"[{wrapper.identity}] FAILURE ({wrapper.mail.hostname})",
    ]


def test_all_worker_mail_uses_constructor_identity_hostname_and_subject(wrapper, tmp_path):
    worker = type(wrapper)("http://localhost", mock.Mock(), base_dir=str(tmp_path),
                           mail_sender=mock.Mock(), mail_receivers=["admin@example.org"],
                           identity="OCR production", hostname="host-override",
                           mail_subject="{status} | {identity} | {hostname} | {time:%Y-%m-%d}")
    worker.alive.send_alive()
    worker._notify_worker_failure("cannot connect")
    assert worker.mail.sender.send.call_count == 2
    for (subject, body), status in zip(_sent(worker.mail), ("ALIVE", "FAILURE")):
        assert subject.startswith(f"{status} | OCR production | host-override | ")
        assert "OCR production" in body and "host-override" in body
        assert status in body


def test_invalid_mail_subject_is_rejected_before_side_effects(wrapper, tmp_path):
    with mock.patch("doc_worker.doc_worker_wrapper.Adapter") as adapter:
        with pytest.raises(KeyError, match="unknown"):
            type(wrapper)("http://localhost", mock.Mock(), base_dir=str(tmp_path / "unused"),
                          mail_subject="{unknown}")
    adapter.assert_not_called()
    assert not (tmp_path / "unused").exists()


def test_the_alive_report_shows_the_worker_s_job_and_history(wrapper, tmp_path):
    from doc_worker.doc_worker_wrapper import JobHistory
    worker = type(wrapper)("http://localhost", mock.Mock(), base_dir=str(tmp_path), mail_sender=mock.Mock(),
                           mail_receivers=["admin@example.org"], job_history_path=str(tmp_path / "history.jsonl"))
    worker.current_lease = types.SimpleNamespace(id="leased")
    worker.alive.send_alive()
    assert "none active" in worker.mail.sender.send.call_args.args[1]  # A lease alone is not a job.
    worker.current_job = types.SimpleNamespace(id="job-1")
    worker.alive.send_alive()
    body = worker.mail.sender.send.call_args.args[1]
    assert "job-1" in body and "all time" in body
    assert isinstance(worker.alive.job_history(), JobHistory)
