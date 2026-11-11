import os
import types
from datetime import datetime
from unittest import mock

import pytest

from doc_worker.doc_worker_wrapper import DocWorkerWrapper, WorkerResponse, WorkerResponseStatus


class Worker(DocWorkerWrapper):
    def __init__(self, tmp_path, process=None, **kwargs):
        super().__init__(api_url='http://localhost', connector=mock.Mock(), base_dir=str(tmp_path), **kwargs)
        self.adapter = mock.Mock()
        self.adapter.patch_job_finish.return_value = types.SimpleNamespace(is_success=True)
        self.process = process or (lambda result_dir: WorkerResponse.ok())
        self.finished = []

    def request_job(self):
        self.current_job = types.SimpleNamespace(
            id='job-1', created_date=None, started_date=datetime(2026, 10, 30, 12, 0), previous_attempts=0,
            engine_name='engine', engine_version='v1', images=[], alto_required=False, page_required=False,
            meta_json_required=False)
        return WorkerResponse.ok()

    def _check_and_download_engine_files(self):
        return WorkerResponse.ok()

    def _download_job_data(self):
        return WorkerResponse.ok()

    def _zip_results(self, result_dir):
        return WorkerResponse.ok()

    def _upload_results(self):
        return WorkerResponse.ok()

    def get_engine_data_path(self):
        return None

    def process_job(self, job, job_log_file_handler, images_dir, result_dir, **kwargs):
        return self.process(result_dir)

    def on_job_finished(self, job, response, job_dir, duration_s, job_log_file_handler):
        self.finished.append(dict(job=job, response=response, job_dir=job_dir, duration_s=duration_s,
                                  handler=job_log_file_handler, results=_listdir(os.path.join(job_dir, 'result'))))


def _listdir(path):
    return os.listdir(path) if os.path.isdir(path) else None


def _write_result(result_dir):
    open(os.path.join(result_dir, 'metakat.json'), 'w').close()
    return WorkerResponse.ok()


def test_a_processed_job_is_finished_ok_before_its_directory_is_cleaned_up(tmp_path):
    worker = Worker(tmp_path, process=_write_result, cleanup_job_dir=True)

    assert worker.run_job_pipeline().id == 'job-1'

    [finished] = worker.finished
    assert finished['response'].status == WorkerResponseStatus.OK
    assert finished['job'].id == 'job-1'
    assert finished['results'] == ['metakat.json']
    assert finished['handler'] is not None and finished['duration_s'] >= 0
    assert not os.path.exists(finished['job_dir'])


def test_a_failed_processing_is_finished_with_its_failure(tmp_path):
    error = ValueError('broken')
    worker = Worker(tmp_path, process=lambda result_dir: WorkerResponse.error(final_log='Processing failed', final_log_user='Processing failed', exception=error),
                    cleanup_job_dir=True)

    assert worker.run_job_pipeline() is None

    [finished] = worker.finished
    assert finished['response'].status == WorkerResponseStatus.ERROR
    assert finished['response'].final_log_user == 'Processing failed'
    assert finished['response'].exception is error
    assert os.path.exists(finished['job_dir'])


def test_a_failed_download_is_finished_with_its_exception(tmp_path):
    worker = Worker(tmp_path)
    worker._download_job_data = mock.Mock(side_effect=ConnectionError('refused'))

    assert worker.run_job_pipeline() is None

    [finished] = worker.finished
    assert finished['response'].final_log_user == 'Failed to download job data from server'
    assert isinstance(finished['response'].exception, ConnectionError)


def test_a_failing_hook_does_not_break_the_pipeline(tmp_path):
    worker = Worker(tmp_path)
    worker.on_job_finished = mock.Mock(side_effect=RuntimeError('hook'))

    assert worker.run_job_pipeline().id == 'job-1'
    worker.on_job_finished.assert_called_once()


def test_an_interrupted_job_is_released_not_finished(tmp_path):
    def interrupt(result_dir):
        raise KeyboardInterrupt

    worker = Worker(tmp_path, process=interrupt)

    with pytest.raises(KeyboardInterrupt):
        worker.run_job_pipeline()
    assert worker.finished == []
    worker.adapter.delete_job_lease.assert_called_once()



def test_a_finished_job_sends_its_final_reports(tmp_path):
    response = WorkerResponse.ok(final_log='technical report', final_log_user='user report')
    worker = Worker(tmp_path, process=lambda result_dir: response)

    worker.run_job_pipeline()

    worker.adapter.patch_job_finish.assert_called_once_with(
        final_log='technical report', final_log_user='user report', job_id='job-1')
    assert worker.finished[0]['response'] is response


@pytest.mark.parametrize('status', [WorkerResponseStatus.ERROR, WorkerResponseStatus.FAIL])
def test_an_error_or_failure_sends_separate_final_reports(tmp_path, status):
    response = (WorkerResponse.fail if status == WorkerResponseStatus.FAIL else WorkerResponse.error)(
        exception=ValueError('technical details'),
        final_log='technical report', final_log_user='user report')
    assert response.status == status
    worker = Worker(tmp_path, process=lambda result_dir: response)

    worker.run_job_pipeline()

    report = worker.adapter.patch_job_fail if status == WorkerResponseStatus.FAIL else worker.adapter.patch_job_error
    other_report = worker.adapter.patch_job_error if status == WorkerResponseStatus.FAIL else worker.adapter.patch_job_fail
    report.assert_called_once_with(
        final_log='technical report\n\nException: ValueError: technical details',
        final_log_user='user report', job_id='job-1')
    other_report.assert_not_called()
    worker.adapter.patch_job_finish.assert_not_called()
    assert worker.finished[0]['response'] is response


def test_a_quiet_finished_job_sends_no_final_report(tmp_path):
    worker = Worker(tmp_path)

    assert worker.run_job_pipeline().id == 'job-1'
    worker.adapter.patch_job_finish.assert_called_once_with(final_log=None, final_log_user=None, job_id='job-1')


@pytest.mark.parametrize('factory', [WorkerResponse.ok, WorkerResponse.error, WorkerResponse.fail])
@pytest.mark.parametrize('log,log_user', [('technical only', None), (None, 'user only'), (None, None)])
def test_final_reports_are_explicit_without_fallback(tmp_path, factory, log, log_user):
    response = factory(final_log=log, final_log_user=log_user)
    worker = Worker(tmp_path, process=lambda result_dir: response)
    worker.run_job_pipeline()
    if response.status == WorkerResponseStatus.OK:
        report = worker.adapter.patch_job_finish
    else:
        report = worker.adapter.patch_job_fail if response.status == WorkerResponseStatus.FAIL else worker.adapter.patch_job_error
    report.assert_called_once_with(final_log=log, final_log_user=log_user, job_id='job-1')


def test_automatic_error_messages_can_be_overridden_separately(tmp_path):
    from doc_worker.doc_worker_wrapper import WorkerErrorMessage
    worker = Worker(tmp_path, error_messages={
        'download_job_data': WorkerErrorMessage('Input preparation failed', 'Podklady se nepodařilo stáhnout.')})
    worker._download_job_data = mock.Mock(side_effect=OSError('private connection detail'))
    assert worker.run_job_pipeline() is None
    report = worker.adapter.patch_job_error.call_args.kwargs
    assert report['final_log'].startswith('Input preparation failed\n\nException: OSError: private connection detail')
    assert report['final_log_user'] == 'Podklady se nepodařilo stáhnout.'
    assert worker.finished[0]['response'].exception is worker._download_job_data.side_effect


def test_partial_error_overrides_keep_other_defaults_and_other_workers_unchanged(tmp_path):
    from doc_worker.doc_worker_wrapper import WorkerErrorMessage
    worker = Worker(tmp_path, error_messages={'download_job_data': WorkerErrorMessage('Custom', None)})
    worker._upload_results = mock.Mock(side_effect=OSError('unavailable'))
    worker.run_job_pipeline()
    assert worker.adapter.patch_job_error.call_args.kwargs['final_log_user'] == 'Failed to upload results to server'
    other = Worker(tmp_path)
    other._download_job_data = mock.Mock(side_effect=OSError('unavailable'))
    other.run_job_pipeline()
    assert other.adapter.patch_job_error.call_args.kwargs['final_log_user'] == 'Failed to download job data from server'


def test_exception_diagnostics_are_sent_only_in_technical_log(tmp_path):
    worker = Worker(tmp_path)
    worker._download_job_data = mock.Mock(side_effect=ValueError('private diagnostic'))
    worker.run_job_pipeline()
    report = worker.adapter.patch_job_error.call_args.kwargs
    assert 'Traceback' in report['final_log'] and 'private diagnostic' in report['final_log']
    assert 'private diagnostic' not in report['final_log_user']
    assert worker.finished[0]['response'].exception is worker._download_job_data.side_effect


@pytest.mark.parametrize('messages,exception', [
    ({'typo': 'message'}, ValueError), ({'process_job': 'message'}, TypeError),
])
def test_invalid_error_configuration_is_rejected(tmp_path, messages, exception):
    with pytest.raises(exception):
        Worker(tmp_path, error_messages=messages)


@pytest.mark.parametrize('factory', [WorkerResponse.ok, WorkerResponse.error, WorkerResponse.fail])
def test_cycle_retains_job_through_reporting_and_cleanup_then_clears_it(tmp_path, factory):
    worker = Worker(tmp_path, process=lambda result_dir: factory(), cleanup_job_dir=True)
    request = worker.request_job
    finished = worker.on_job_finished
    observed = []

    def lease():
        response = request()
        worker.current_lease = types.SimpleNamespace(id='job-1', lease_expire_at=None)
        return response

    def on_finished(*args):
        assert worker.current_job is args[0]
        assert worker.current_lease.id == 'job-1'
        assert worker.get_job_data_path() is not None
        finished(*args)
        observed.append('finished')

    def report(**kwargs):
        assert worker.current_job.id == 'job-1'
        assert worker.current_lease.id == 'job-1'
        observed.append('reported')
        return types.SimpleNamespace(is_success=True)

    worker.request_job = lease
    worker.on_job_finished = on_finished
    worker.adapter.patch_job_finish.side_effect = report
    worker.adapter.patch_job_error.side_effect = report
    worker.adapter.patch_job_fail.side_effect = report
    result = worker.run_job_pipeline()
    assert (result.id if result is not None else None) == ('job-1' if factory == WorkerResponse.ok else None)
    assert observed == ['reported', 'finished']
    assert worker.finished[0]['response'].status == factory().status
    assert worker.current_job is None and worker.current_lease is None
    assert worker.get_job_data_path() is None


def test_cycle_clears_lease_when_fetching_the_job_fails(tmp_path):
    worker = Worker(tmp_path)

    def failed_request():
        worker.current_lease = types.SimpleNamespace(id='leased-job')
        return WorkerResponse.error(final_log='Cannot fetch job')

    worker.request_job = failed_request
    worker.run_job_pipeline()
    assert worker.adapter.patch_job_error.call_args.kwargs['job_id'] == 'leased-job'
    assert worker.current_job is None and worker.current_lease is None


def test_interruption_clears_local_job_even_if_lease_release_fails(tmp_path):
    def interrupt(result_dir):
        raise KeyboardInterrupt

    worker = Worker(tmp_path, process=interrupt)
    worker.adapter.delete_job_lease.return_value = types.SimpleNamespace(is_success=False, status=503, code=None)
    with pytest.raises(KeyboardInterrupt):
        worker.run_job_pipeline()
    assert worker.current_job is None and worker.current_lease is None


@pytest.mark.parametrize('factory,status', [(WorkerResponse.ok, 'ok'), (WorkerResponse.error, 'error'),
                                           (WorkerResponse.fail, 'fail')])
def test_pipeline_records_one_persistent_attempt_for_each_outcome(tmp_path, factory, status):
    import json
    path = tmp_path / 'history.jsonl'
    worker = Worker(tmp_path, process=lambda result_dir: factory(), job_history_path=str(path), cleanup_job_dir=True)
    worker.run_job_pipeline()
    [entry] = [json.loads(line) for line in path.read_text().splitlines()]
    assert entry['job_id'] == 'job-1' and entry['attempt'] == 0 and entry['status'] == status
    assert entry['reported_to_api'] is True
    assert entry['duration_seconds'] >= 0
    assert datetime.fromisoformat(entry['started_at']).utcoffset().total_seconds() == 0
    assert datetime.fromisoformat(entry['finished_at']) >= datetime.fromisoformat(entry['started_at'])
    assert worker.current_job is None


@pytest.mark.parametrize('exception,status', [(KeyboardInterrupt, 'interrupted'), (SystemExit, 'crashed')])
def test_interrupted_or_crashed_pipeline_is_recorded(tmp_path, exception, status):
    import json
    path = tmp_path / 'history.jsonl'

    def stop(result_dir):
        raise exception

    worker = Worker(tmp_path, process=stop, job_history_path=str(path))
    with pytest.raises(exception):
        worker.run_job_pipeline()
    assert json.loads(path.read_text().splitlines()[0])['status'] == status
    assert worker.current_job is None


def test_history_failure_does_not_change_success_and_mails_worker_failure(tmp_path):
    worker = Worker(tmp_path, job_history_path=str(tmp_path / 'history.jsonl'))
    worker.mail_sender = mock.Mock()
    worker.job_history.record = mock.Mock(side_effect=OSError('disk full'))
    assert worker.run_job_pipeline().id == 'job-1'
    worker.mail_sender.send.assert_called_once()
    body = worker.mail_sender.send.call_args.args[1]
    assert "OSError: disk full" in body and "Traceback" in body
    assert worker.current_job is None


def test_idle_poll_does_not_create_history(tmp_path):
    path = tmp_path / 'history.jsonl'
    worker = Worker(tmp_path, job_history_path=str(path))
    worker.request_job = mock.Mock(return_value=None)
    assert worker.run_job_pipeline() is None
    assert not path.exists()


def test_success_is_reported_after_result_upload(tmp_path):
    worker = Worker(tmp_path)
    order = []

    def upload():
        order.append('upload')
        return WorkerResponse.ok()

    def finish(**kwargs):
        order.append('finish')
        return types.SimpleNamespace(is_success=True)

    worker._upload_results = upload
    worker.adapter.patch_job_finish.side_effect = finish
    assert worker.run_job_pipeline().id == 'job-1'
    assert order == ['upload', 'finish']


@pytest.mark.parametrize('factory', [WorkerResponse.ok, WorkerResponse.error, WorkerResponse.fail])
@pytest.mark.parametrize('raises', [False, True])
def test_reporting_failure_preserves_pipeline_response_and_is_recorded_separately(tmp_path, factory, raises):
    import json
    original = factory(final_log='technical summary', final_log_user='user summary')
    path = tmp_path / 'history.jsonl'
    worker = Worker(tmp_path, process=lambda result_dir: original, job_history_path=str(path), cleanup_job_dir=True)
    worker.mail_sender = mock.Mock()
    report = (worker.adapter.patch_job_finish if original.status == WorkerResponseStatus.OK else
              worker.adapter.patch_job_error if original.status == WorkerResponseStatus.ERROR else
              worker.adapter.patch_job_fail)
    if raises:
        report.side_effect = OSError('report unavailable')
    else:
        report.return_value = types.SimpleNamespace(is_success=False, status=503, code=None, response=None)
    assert worker.run_job_pipeline() is None
    report.assert_called_once()
    assert (worker.adapter.patch_job_finish.call_count + worker.adapter.patch_job_error.call_count +
            worker.adapter.patch_job_fail.call_count) == 1
    worker.mail_sender.send.assert_called_once()
    assert worker.finished[0]['response'] is original
    assert original.final_log == 'technical summary' and original.final_log_user == 'user summary'
    entry = json.loads(path.read_text())
    assert entry['status'] == original.status.value
    assert entry['reported_to_api'] is False
    assert os.path.isdir(worker.finished[0]['job_dir'])
    stats = worker.job_history.statistics()
    assert stats['total']['outcomes'][original.status.value] == 1
    assert stats['total']['report_failures'] == 1
    assert stats['last_24_hours']['report_failures'] == 1


@pytest.mark.parametrize('factory', [WorkerResponse.ok, WorkerResponse.error, WorkerResponse.fail])
def test_reporting_returns_a_boolean_without_mutating_the_response(tmp_path, factory):
    worker = Worker(tmp_path)
    worker.current_job = types.SimpleNamespace(id='job-1')
    original = factory(final_log='technical', final_log_user='user')
    report = (worker.adapter.patch_job_finish if original.status == WorkerResponseStatus.OK else
              worker.adapter.patch_job_error if original.status == WorkerResponseStatus.ERROR else
              worker.adapter.patch_job_fail)
    report.return_value = types.SimpleNamespace(is_success=True)
    assert worker._report_job_response(original) is True
    assert original.status == factory().status
    assert original.final_log == 'technical' and original.final_log_user == 'user'


@pytest.mark.parametrize('exception', [RuntimeError, SystemExit])
def test_unexpected_failures_use_the_same_configured_message(tmp_path, exception):
    from doc_worker.doc_worker_wrapper import WorkerErrorMessage
    worker = Worker(tmp_path, error_messages={
        'unexpected_processing': WorkerErrorMessage('Unexpected pipeline failure', 'Please try again later')})
    worker.get_job_data_path = mock.Mock(side_effect=exception('unexpected stop'))
    worker.on_job_finished = mock.Mock()
    if exception is SystemExit:
        with pytest.raises(SystemExit):
            worker.run_job_pipeline()
    else:
        assert worker.run_job_pipeline() is None
    response = worker.on_job_finished.call_args.args[1]
    assert response.final_log == 'Unexpected pipeline failure'
    assert response.final_log_user == 'Please try again later'
