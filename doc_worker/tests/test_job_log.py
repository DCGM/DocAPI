"""The job's log, worker.log in its directory: everything logged while the job runs, once."""
import logging
import os
import threading
import types
from contextlib import contextmanager
from datetime import datetime
from unittest import mock

import pytest

from doc_worker.doc_worker_wrapper import DocWorkerWrapper, WorkerResponse
from doc_worker.dummy_worker import DummyWorker
from doc_worker.alive_monitor import AliveMonitor

own_logger = logging.getLogger("myapp.worker")
library_logger = logging.getLogger("mylibrary")


class Pipeline:
    """The pipeline without DocAPI: a job with one image, and results that are not uploaded."""

    def __init__(self, tmp_path, **kwargs):
        super().__init__(api_url="http://localhost", connector=mock.Mock(), base_dir=str(tmp_path), **kwargs)
        self.adapter = mock.Mock()
        self.adapter.patch_job_finish.return_value = types.SimpleNamespace(is_success=True)
        self.jobs = 0
        self.job_dirs = []

    def request_job(self):
        self.jobs += 1
        self.current_job = types.SimpleNamespace(
            id=f"job-{self.jobs}", created_date=None, started_date=datetime(2026, 11, 19, 12, 0),
            previous_attempts=0, engine_name="engine", engine_version="v1", images=[], alto_required=False,
            page_required=False, meta_json_required=False)
        self.job_dirs.append(self.get_job_data_path())
        return WorkerResponse.ok()

    def _check_and_download_engine_files(self):
        return WorkerResponse.ok()

    def _download_job_data(self):
        images_dir = os.path.join(self.get_job_data_path(), "images")
        os.makedirs(images_dir, exist_ok=True)
        open(os.path.join(images_dir, "page.jpg"), "wb").close()
        return WorkerResponse.ok()

    def _zip_results(self, result_dir):
        return WorkerResponse.ok()

    def _upload_results(self):
        logging.getLogger("doc_worker.doc_worker_wrapper").info("wrapper uploads %s", self.current_job.id)
        return WorkerResponse.ok()

    def get_engine_data_path(self):
        return None

    def job_log(self, number=1):
        with open(os.path.join(self.job_dirs[number - 1], "worker.log")) as file:
            return file.read()


class Worker(Pipeline, DocWorkerWrapper):
    def process_job(self, job, images_dir, result_dir, **kwargs):
        own_logger.info("own message %s", job.id)
        library_logger.debug("library message %s", job.id)
        for name, text in (("other", "thread message"), (AliveMonitor.THREAD_NAME, "alive message")):
            thread = threading.Thread(target=lambda text=text: own_logger.info("%s %s", text, job.id), name=name)
            thread.start()
            thread.join()
        return WorkerResponse.ok()


class OwnLoggerWorker(Worker):
    """Sends only its own logger into the job's log."""

    @contextmanager
    def job_logging(self, job_log_file_handler):
        own_logger.addHandler(job_log_file_handler)
        try:
            yield
        finally:
            own_logger.removeHandler(job_log_file_handler)


class PipelineDummyWorker(Pipeline, DummyWorker):
    pass


@pytest.fixture(autouse=True)
def debug_logging(caplog):
    """The root lets everything through, as a worker's logging setup that wants it all would."""
    caplog.set_level(logging.DEBUG)


def lines(log, text):
    return sum(text in line for line in log.splitlines())


def test_everything_logged_during_a_job_is_in_its_log_once(tmp_path):
    worker = Worker(tmp_path)
    assert worker.run_job_pipeline().id == "job-1"
    log = worker.job_log()
    for text in ("Job job-1 acquired", "own message job-1", "library message job-1",
                 "thread message job-1", "wrapper uploads job-1"):
        assert lines(log, text) == 1, text


def test_the_alive_reports_are_not_in_the_job_log(tmp_path):
    worker = Worker(tmp_path)
    worker.run_job_pipeline()
    assert "alive message" not in worker.job_log()


def test_nothing_from_outside_the_job_is_in_its_log(tmp_path):
    worker = Worker(tmp_path)
    own_logger.info("before the jobs")
    worker.run_job_pipeline()
    own_logger.info("between the jobs")
    worker.run_job_pipeline()
    own_logger.info("after the jobs")
    first, second = worker.job_log(1), worker.job_log(2)
    assert "own message job-1" in first and "job-2" not in first
    assert "own message job-2" in second and "job-1" not in second
    assert not any(text in first + second for text in ("before the jobs", "between the jobs", "after the jobs"))
    assert not [handler for handler in logging.getLogger().handlers
                if os.path.basename(getattr(handler, "baseFilename", "")) == "worker.log"]


def test_an_overridden_job_logging_replaces_the_default(tmp_path):
    worker = OwnLoggerWorker(tmp_path)
    worker.run_job_pipeline()
    log = worker.job_log()
    assert lines(log, "own message job-1") == 1
    assert "library message" not in log and "Job job-1 acquired" not in log
    assert own_logger.handlers == []


def test_a_failing_job_logging_fails_the_job_not_the_worker(tmp_path):
    worker = Worker(tmp_path)
    worker.job_logging = mock.Mock(side_effect=RuntimeError("no log"))
    assert worker.run_job_pipeline() is None
    worker.adapter.patch_job_error.assert_called_once()


def test_the_dummy_worker_processes_a_job(tmp_path):
    worker = PipelineDummyWorker(tmp_path)
    assert worker.run_job_pipeline().id == "job-1"
    worker.adapter.patch_job_finish.assert_called_once()
    assert os.listdir(os.path.join(worker.job_dirs[0], "result", "images")) == ["page.jpg"]
    assert lines(worker.job_log(), "Dummy processing job job-1") == 1


class FormattedWorker(Worker):
    def job_log_handler(self, path):
        handler = logging.FileHandler(path)
        handler.setFormatter(logging.Formatter("JOB %(levelname)s %(message)s"))
        handler.setLevel(logging.INFO)
        return handler


def test_the_job_log_takes_the_format_of_the_root_handlers(tmp_path, monkeypatch):
    formatter = logging.Formatter("%(message)s")
    unformatted, formatted = logging.StreamHandler(), logging.StreamHandler()
    formatted.setFormatter(formatter)
    monkeypatch.setattr(logging.getLogger(), "handlers", [unformatted, formatted])
    handler = Worker(tmp_path).job_log_handler(str(tmp_path / "a.log"))
    assert handler.formatter is formatter and handler.level == logging.DEBUG
    handler.close()
    monkeypatch.setattr(logging.getLogger(), "handlers", [])
    handler = Worker(tmp_path).job_log_handler(str(tmp_path / "b.log"))
    assert handler.formatter._fmt == "%(asctime)s : %(levelname)s : %(name)s : %(message)s"
    handler.close()


def test_an_overridden_job_log_handler_keeps_the_alive_reports_out(tmp_path):
    worker = FormattedWorker(tmp_path)
    worker.run_job_pipeline()
    log = worker.job_log().splitlines()
    assert "JOB INFO own message job-1" in log
    assert not [line for line in log if "library message" in line or "alive message" in line]
