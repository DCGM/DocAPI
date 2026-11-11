import types
from unittest import mock

import pytest

from doc_worker.doc_worker_wrapper import WorkerHealth, WorkerHealthConfig


@pytest.fixture
def health():
    from doc_worker.doc_worker_wrapper import DocWorkerWrapper
    worker = types.SimpleNamespace(mail_sender=mock.Mock(), send_mail=mock.Mock(), current_job=None,
                                   job_history=None, identity="worker abc", hostname="host",
                                   mail_subject="[{identity}] {status}: {hostname}")
    worker._send_worker_mail = types.MethodType(DocWorkerWrapper._send_worker_mail, worker)
    return WorkerHealth(worker, WorkerHealthConfig())


def test_startup_mail_and_periodic_alive_checks(health):
    with mock.patch("threading.Thread") as thread:
        health.start()
    health.worker.send_mail.assert_called_once()
    assert "ALIVE" in health.worker.send_mail.call_args.args[0]
    thread.return_value.start.assert_called_once()
    health.worker.send_mail.reset_mock()
    health._stop = mock.Mock()
    health._stop.wait.side_effect = [False, True]
    health.pipeline_returned()
    health.worker.current_job = types.SimpleNamespace(id="job-1")
    health._run()
    subject, body = health.worker.send_mail.call_args.args
    assert "ALIVE" in subject
    assert "last job" in body and "active job" in body and "job-1" in body
    health.close()
    health._stop.set.assert_called_once()
    thread.return_value.join.assert_called_once()


def test_health_mail_can_be_disabled(health):
    health.mail_startup = False
    health.alive_interval = 0
    with mock.patch("threading.Thread") as thread:
        health.start()
    health.worker.send_mail.assert_not_called()
    thread.assert_not_called()


def test_health_start_rejects_missing_mail_sender(health):
    health.worker.mail_sender = None
    with mock.patch("threading.Thread") as thread:
        with pytest.raises(ValueError, match="health requires a mail sender"):
            health.start()
    thread.assert_not_called()
    health.worker.send_mail.assert_not_called()


def test_each_failure_is_mailed_without_health(wrapper):
    assert wrapper.health is None
    wrapper._notify_worker_failure("first")
    wrapper._notify_worker_failure("second")
    assert wrapper.mail_sender.send.call_count == 2
    assert "second" in wrapper.mail_sender.send.call_args.args[1]


def test_failure_mail_requires_only_a_mail_sender(wrapper):
    wrapper.mail_sender = None
    wrapper._notify_worker_failure("failure")


def test_mail_failure_cannot_crash_the_worker(health, wrapper):
    health.worker.send_mail.side_effect = OSError("smtp unavailable")
    health.send_alive()
    wrapper.mail_sender.send.side_effect = OSError("smtp unavailable")
    wrapper._notify_worker_failure("failure")
    wrapper.mail_sender.send.assert_called_once()


@pytest.mark.parametrize("field", ["alive_interval_seconds"])
@pytest.mark.parametrize("value", [-1, float("nan"), float("inf")])
def test_config_rejects_invalid_intervals(field, value):
    with pytest.raises(ValueError, match=field):
        WorkerHealthConfig(**{field: value})


def test_subject_and_body_are_masked_and_prefix_is_configurable(health):
    health.worker.mail_subject = "[My worker] {status}: {identity}"
    health.worker.identity = "host api_key=private-value"
    health.send_alive()
    subject, body = health.worker.send_mail.call_args.args
    assert subject.startswith("[My worker] ALIVE:")
    assert "private-value" not in subject + body


@pytest.fixture
def wrapper(tmp_path):
    from doc_worker.doc_worker_wrapper import DocWorkerWrapper, WorkerResponse

    class Worker(DocWorkerWrapper):
        def process_job(self, *args, **kwargs):
            return WorkerResponse.ok()

    worker = Worker("http://localhost", mock.Mock(), base_dir=str(tmp_path), mail_sender=mock.Mock())
    worker.adapter = mock.Mock()
    worker.adapter.patch_job_error.return_value = types.SimpleNamespace(is_success=True)
    worker.adapter.patch_job_fail.return_value = types.SimpleNamespace(is_success=True)
    return worker


def test_wrapper_health_is_disabled_by_default(wrapper):
    assert wrapper.health is None
    wrapper.run_job_pipeline = mock.Mock(side_effect=[None, KeyboardInterrupt])
    with mock.patch("time.sleep"):
        wrapper.start()
    wrapper.mail_sender.send.assert_not_called()


def test_wrapper_constructs_health_from_explicit_config(wrapper, tmp_path):
    worker = type(wrapper)("http://localhost", mock.Mock(), base_dir=str(tmp_path),
                           mail_sender=mock.Mock(), identity="My worker", health_config=WorkerHealthConfig())
    assert isinstance(worker.health, WorkerHealth)
    assert worker.identity == "My worker"


@pytest.mark.parametrize("failure", [KeyboardInterrupt, RuntimeError, SystemExit])
def test_wrapper_loop_manages_health_and_reports_unexpected_termination(wrapper, failure):
    wrapper.health = mock.Mock()
    wrapper.run_job_pipeline = mock.Mock(side_effect=[None, failure("loop broke")])
    with mock.patch("time.sleep"):
        if failure is KeyboardInterrupt:
            wrapper.start()
            wrapper.mail_sender.send.assert_not_called()
        else:
            with pytest.raises(failure):
                wrapper.start()
            wrapper.mail_sender.send.assert_called_once()
            body = wrapper.mail_sender.send.call_args.args[1]
            assert "loop broke" in body and "Traceback" in body
    wrapper.health.start.assert_called_once()
    wrapper.health.pipeline_returned.assert_called_once()
    wrapper.health.close.assert_called_once()


def test_wrapper_reports_successful_job_errors_without_worker_alert(wrapper):
    from doc_worker.doc_worker_wrapper import WorkerResponse
    wrapper.health = mock.Mock()
    wrapper.current_job = types.SimpleNamespace(id="job-1")
    wrapper._report_job_response(WorkerResponse.error(final_log="technical", final_log_user="user"))
    wrapper.adapter.patch_job_error.assert_called_once_with(final_log="technical", final_log_user="user", job_id="job-1")
    wrapper.mail_sender.send.assert_not_called()


@pytest.mark.parametrize("raises", [False, True])
def test_wrapper_alerts_when_job_error_cannot_reach_docapi(wrapper, raises):
    from doc_worker.doc_worker_wrapper import WorkerResponse
    wrapper.health = mock.Mock()
    wrapper.current_job = types.SimpleNamespace(id="job-1")
    if raises:
        wrapper.adapter.patch_job_error.side_effect = OSError("unavailable")
    else:
        wrapper.adapter.patch_job_error.return_value = types.SimpleNamespace(is_success=False, status=503, code=None)
    wrapper._report_job_response(WorkerResponse.error(final_log="technical"))
    wrapper.mail_sender.send.assert_called_once()
    if raises:
        body = wrapper.mail_sender.send.call_args.args[1]
        assert "OSError: unavailable" in body and "Traceback" in body


@pytest.mark.parametrize("raises", [False, True])
def test_wrapper_alerts_when_polling_fails_without_a_job(wrapper, raises):
    wrapper.health = mock.Mock()
    if raises:
        wrapper.adapter.post_job_lease.side_effect = OSError("DocAPI unreachable")
    else:
        wrapper.adapter.post_job_lease.return_value = types.SimpleNamespace(
            is_success=False, data=None, status=503, code=None, response=None)
    with mock.patch.object(wrapper, "_report_job_response") as report:
        assert wrapper.run_job_pipeline() is None
    report.assert_not_called()
    wrapper.adapter.patch_job_error.assert_not_called()
    wrapper.mail_sender.send.assert_called_once()
    message = wrapper.mail_sender.send.call_args.args[1]
    assert ("Failed to request job" if raises else "Failed to request job lease") in message
    assert ("DocAPI unreachable" if raises else "503") in message
    assert "without a job ID" not in message
    assert wrapper.current_job is None and wrapper.current_lease is None


@pytest.mark.parametrize("raises", [False, True])
def test_wrapper_reports_job_fetch_failure_after_acquiring_a_lease(wrapper, raises):
    wrapper.health = mock.Mock()
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
    wrapper.mail_sender.send.assert_not_called()
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


def test_alive_mail_identifies_idle_state(health):
    health.send_alive()
    body = health.worker.send_mail.call_args.args[1]
    assert 'none active' in body
    assert 'job-1' not in body


def test_startup_and_periodic_reports_share_the_same_statistics(health, tmp_path):
    from datetime import datetime, timedelta, timezone
    from doc_worker.doc_worker_wrapper import JobHistory
    history = JobHistory(tmp_path / 'history.jsonl')
    now = datetime.now(timezone.utc)
    history.record(types.SimpleNamespace(id='job-1', images=[]), started_at=now - timedelta(seconds=10),
                   finished_at=now, duration_seconds=10, status='ok')
    health.worker.job_history = JobHistory(history.path)
    health.alive_interval = 0
    health.start()
    startup = health.worker.send_mail.call_args.args
    health.send_alive()
    periodic = health.worker.send_mail.call_args.args
    assert startup[0] == periodic[0] and 'ALIVE' in startup[0]
    for body in (startup[1], periodic[1]):
        for label in ('all time', 'last 7 days', 'last 24 hours'):
            section = body.split('\n' + label + '\n', 1)[1].split('\n\n', 1)[0]
            rows = [' '.join(line.split()) for line in section.splitlines()]
            assert 'attempts 1' in rows and 'unique jobs 1' in rows and 'ok 1' in rows


def test_corrupt_history_does_not_prevent_alive_mail(health):
    health.worker.job_history = mock.Mock()
    health.worker.job_history.statistics.side_effect = ValueError('corrupt history')
    health.send_alive()
    assert 'unavailable; inspect worker log' in health.worker.send_mail.call_args.args[1]


def test_alive_report_wraps_description_and_uses_compact_durations(health):
    health.worker.identity = 'OCR worker with a long descriptive identity for a development instance'
    health.started = 0
    health.last_pipeline_return = 172555
    with mock.patch('time.monotonic', return_value=172800):
        health.send_alive()
    body = health.worker.send_mail.call_args.args[1]
    assert '2d 0h 0m 0s' in body and '4m 5s ago' in body
    assert all(len(line) <= 60 for line in body.splitlines())


@pytest.mark.parametrize("settings", [{}, {"mail_startup": False, "alive_interval_seconds": 0}])
def test_wrapper_rejects_health_without_mail_sender_before_side_effects(wrapper, tmp_path, caplog, settings):
    directory = tmp_path / "not-created"
    with mock.patch("doc_worker.doc_worker_wrapper.Adapter") as adapter:
        with pytest.raises(ValueError, match="health requires a mail sender"):
            type(wrapper)("http://localhost", mock.Mock(), base_dir=str(directory),
                          health_config=WorkerHealthConfig(**settings))
    adapter.assert_not_called()
    assert not directory.exists()
    assert "Worker health requires a mail sender." in caplog.text


def test_failure_mail_masks_secrets_without_health(wrapper):
    wrapper._notify_worker_failure("password=private-value")
    subject, body, _ = wrapper.mail_sender.send.call_args.args
    assert "private-value" not in subject + body


def test_failure_mail_includes_exception_chain_and_masks_it(wrapper):
    try:
        try:
            raise OSError("disk full password=private-value")
        except OSError as cause:
            raise RuntimeError("cannot save history") from cause
    except RuntimeError as exception:
        wrapper._notify_worker_failure("History failure", exception=exception)
    body = wrapper.mail_sender.send.call_args.args[1]
    assert "History failure" in body
    assert "OSError: disk full" in body
    assert "RuntimeError: cannot save history" in body
    assert "Traceback" in body
    assert "private-value" not in body


def test_wrapper_defaults_identity_and_detects_hostname(wrapper, tmp_path):
    with mock.patch("doc_worker.doc_worker_wrapper.socket.gethostname", return_value="automatic-host"):
        worker = type(wrapper)("http://localhost", mock.Mock(), base_dir=str(tmp_path))
    assert worker.identity == type(worker).__name__
    assert worker.hostname == "automatic-host"


def test_all_worker_mail_uses_constructor_identity_hostname_and_subject(wrapper, tmp_path):
    worker = type(wrapper)("http://localhost", mock.Mock(), base_dir=str(tmp_path),
                           mail_sender=mock.Mock(), identity="OCR production", hostname="host-override",
                           mail_subject="{status} | {identity} | {hostname} | {time:%Y-%m-%d}",
                           health_config=WorkerHealthConfig())
    worker.health.send_alive()
    worker._notify_worker_failure("cannot connect")
    assert worker.mail_sender.send.call_count == 2
    for call, status in zip(worker.mail_sender.send.call_args_list, ("ALIVE", "FAILURE")):
        subject, body, _ = call.args
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
