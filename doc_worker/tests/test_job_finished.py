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
        final_log='technical report', final_log_user='user report')
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
    worker.adapter.patch_job_finish.assert_called_once_with(final_log=None, final_log_user=None)


@pytest.mark.parametrize('factory', [WorkerResponse.ok, WorkerResponse.error, WorkerResponse.fail])
@pytest.mark.parametrize('log,log_user', [('technical only', None), (None, 'user only'), (None, None)])
def test_final_reports_are_explicit_without_fallback(tmp_path, factory, log, log_user):
    response = factory(final_log=log, final_log_user=log_user)
    worker = Worker(tmp_path, process=lambda result_dir: response)
    worker.run_job_pipeline()
    if response.status == WorkerResponseStatus.OK:
        report = worker.adapter.patch_job_finish
        expected = {}
    else:
        report = worker.adapter.patch_job_fail if response.status == WorkerResponseStatus.FAIL else worker.adapter.patch_job_error
        expected = {'job_id': 'job-1'}
    report.assert_called_once_with(final_log=log, final_log_user=log_user, **expected)
