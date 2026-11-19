import logging
import os
import json
import zipfile
import time
import shutil
import glob
import functools
import inspect
import traceback
import math
import tempfile
import threading
from collections import Counter
from contextlib import ExitStack, contextmanager
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Mapping, Optional, Sequence, Union
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from urllib.parse import urljoin, urlsplit

import cv2
import numpy as np
import requests

from doc_api.adapter import Adapter, AdapterResponse, response_code
from doc_api.api.schemas.base_objects import Job, JobLease
from doc_api.connector import Connector
from doc_api.tools.mail_sender import MailSender
from doc_worker.alive_monitor import AliveMonitor
from doc_worker.outage_monitor import OutageMonitor, SettledJob
from doc_worker.worker_mail_sender import DEFAULT_MAIL_SUBJECT, WorkerMailSender, check_mail_subject

from doc_worker.config import Config


logger = logging.getLogger(__name__)


class WorkerResponseStatus(Enum):
    OK = "ok"
    ERROR = "error"
    FAIL = "fail"


@dataclass
class WorkerResponse:
    """
    Response wrapper for worker operations to clearly indicate success or failure.
    
    Attributes:
        status: Successful completion, retryable error, or permanent failure
        error_adapter_response: Optional AdapterResponse containing API error details
        exception: Optional Exception that caused the failure
        final_log: Final technical report
        final_log_user: Final user-facing report
    """
    status: WorkerResponseStatus
    error_adapter_response: Optional[AdapterResponse] = None
    exception: Optional[Exception] = None
    final_log: Optional[str] = None
    final_log_user: Optional[str] = None
    
    @classmethod
    def ok(cls, final_log: Optional[str] = None, final_log_user: Optional[str] = None) -> 'WorkerResponse':
        """Create a successful response."""
        return cls(status=WorkerResponseStatus.OK, final_log=final_log, final_log_user=final_log_user)
    
    @classmethod
    def error(cls, final_log: Optional[str] = None, final_log_user: Optional[str] = None, *,
              exception: Optional[Exception] = None,
              error_adapter_response: Optional[AdapterResponse] = None) -> 'WorkerResponse':
        """Create a retryable error with explicit technical and user final reports."""
        return cls(status=WorkerResponseStatus.ERROR, final_log=final_log, final_log_user=final_log_user,
                   exception=exception, error_adapter_response=error_adapter_response)

    @classmethod
    def fail(cls, final_log: Optional[str] = None, final_log_user: Optional[str] = None, *,
             exception: Optional[Exception] = None,
             error_adapter_response: Optional[AdapterResponse] = None) -> 'WorkerResponse':
        """Create a permanent failure with explicit technical and user final reports."""
        return cls(status=WorkerResponseStatus.FAIL, final_log=final_log, final_log_user=final_log_user,
                   exception=exception, error_adapter_response=error_adapter_response)


@dataclass(frozen=True)
class WorkerErrorMessage:
    """Separate technical and user messages; None suppresses that message.

    download_input messages support {kind} and {filename} placeholders.
    Exception and API diagnostics are added only to the technical report.
    """
    final_log: Optional[str]
    final_log_user: Optional[str]


DEFAULT_WORKER_ERRORS = MappingProxyType({
    'request_job_lease': WorkerErrorMessage('Failed to request job lease', 'Failed to request job lease'),
    'get_job_details': WorkerErrorMessage('Failed to get job details', 'Failed to get job details'),
    'no_current_job': WorkerErrorMessage('No current job set', 'No current job set'),
    'download_engine_files': WorkerErrorMessage('Failed to download engine files', 'Failed to download engine files'),
    'download_input': WorkerErrorMessage('Failed to download {kind} for {filename}', 'Failed to download {kind} for {filename}'),
    'download_meta_json': WorkerErrorMessage('Failed to download meta JSON', 'Failed to download meta JSON'),
    'job_data_path': WorkerErrorMessage('Failed to get job data path', 'Failed to get job data path'),
    'no_job_for_result_upload': WorkerErrorMessage('No current job to upload results for', 'No current job to upload results for'),
    'upload_results': WorkerErrorMessage('Failed to upload results', 'Failed to upload results'),
    'request_job': WorkerErrorMessage('Failed to request job', 'Failed to request job'),
    'prepare_engine_files': WorkerErrorMessage('Failed to download or prepare engine files', 'Failed to download or prepare engine files'),
    'download_job_data': WorkerErrorMessage('Failed to download job data from server', 'Failed to download job data from server'),
    'create_job_workspace': WorkerErrorMessage('Unable to create job workspace directory', 'Unable to create job workspace directory'),
    'process_job': WorkerErrorMessage('Processing failed due to an internal error', 'Processing failed due to an internal error'),
    'package_results': WorkerErrorMessage('Failed to package processing results', 'Failed to package processing results'),
    'upload_results_to_server': WorkerErrorMessage('Failed to upload results to server', 'Failed to upload results to server'),
    'unexpected_processing': WorkerErrorMessage('An unexpected error occurred during processing', 'An unexpected error occurred during processing'),
})


class DocWorkerWrapper(ABC):
    """
    A worker wrapper that handles job leasing and downloading all required data.
    
    This class implements the core worker functionality except the actual processing,
    which should be implemented by subclasses.
    """
    
    def __init__(self, 
                 api_url: str, 
                 connector: Connector,
                 base_dir: Optional[str] = None,
                 job_history_path: Optional[str] = None,
                 jobs_dir: Optional[str] = None, 
                 cleanup_job_dir: bool = False,
                 engines_dir: Optional[str] = None,
                 cleanup_old_engines: bool = False,
                 polling_interval: float = 5.0,
                 prefer_image_url: bool = False,
                 prefer_alto_url: bool = False,
                 prefer_page_url: bool = False,
                 mail_sender: Optional[MailSender] = None,
                 mail_receivers: Sequence[str] = (),
                 identity: Optional[str] = None,
                 hostname: Optional[str] = None,
                 mail_subject: str = DEFAULT_MAIL_SUBJECT,
                 alive_mail_startup: bool = True,
                 alive_mail_interval_seconds: float = 86400,
                 outage_grace_seconds: float = 60,
                 outage_mail_interval_seconds: float = 3600,
                 error_messages: Optional[Mapping[str, WorkerErrorMessage]] = None):
        """
        Initialize the DocWorker.
        
        Args:
            api_url: The base URL of the API
            connector: The connector instance for API communication
            
            base_dir: Base directory - if specified, creates 'jobs' and 'engines' subdirectories
            job_history_path: Optional JSONL history file, one file per worker.
                Records attempts and keeps a sibling .stats.json cache for ALIVE mail.
                None disables history. Independent of job-directory cleanup.
            jobs_dir: Directory for job data (overrides base_dir/jobs if specified)
            cleanup_job_dir: If True, removes job directory after successful processing
            engines_dir: Directory for engine files (overrides base_dir/engines if specified)
            cleanup_old_engines: If True, removes old engine versions when downloading new ones

            polling_interval: Time in seconds to wait between job requests (default: 5.0)

            prefer_image_url: Prefer a supplied image URL when an uploaded image also exists
            prefer_alto_url: Prefer a supplied ALTO URL when an uploaded ALTO file also exists
            prefer_page_url: Prefer a supplied PAGE URL when an uploaded PAGE file also exists

            mail_sender: Optional mail sender for the worker's mail
                (self.worker_mail_sender, a WorkerMailSender) and send_mail(). Mail is
                sent as written; pass a MailSender subclass whose send() filters
                subject and body to mask or redact it. None sends no mail.
            mail_receivers: Who send_mail() and the wrapper's own mail go to by
                default; each gets a separate email. Without any, the wrapper's own
                mail is off.
            identity: The worker's name, in the subject and "worker" line of its emails and
                as the User-Agent of its requests, which the wrapper sets on the connector.
                None takes the label of the worker's API key from DocAPI when start() runs,
                and uses the class name until DocAPI has answered.
            hostname: Host label used in all wrapper emails; defaults to socket.gethostname().
            mail_subject: Python format string with identity, hostname, status (ALIVE,
                FAILURE, DISCONNECTED, STILL DISCONNECTED or RECONNECTED), and time
                (UTC datetime, supporting date format specifications) placeholders.
            alive_mail_startup: Send an ALIVE report when the worker starts
                (self.alive_monitor, an AliveMonitor). ALIVE reports are sent whenever mail_sender and
                mail_receivers are configured.
            alive_mail_interval_seconds: Seconds between the ALIVE reports, one day by
                default; 0 sends no periodic reports.
            outage_grace_seconds: How long DocAPI may be unreachable while the worker is
                idle before a DISCONNECTED email (self.outage_monitor, an
                OutageMonitor); shorter outages are only logged. An outage during a job is mailed at once.
            outage_mail_interval_seconds: How often a STILL DISCONNECTED reminder is sent
                while DocAPI stays unreachable.
            error_messages: Overrides keyed by DEFAULT_WORKER_ERRORS names. Each
                WorkerErrorMessage sets technical and user text independently.
                Omitted entries retain their existing English defaults.

        DocAPI is unreachable when a request to it fails to connect or times out,
        or gets a 502, 503 or 504, or a 404 without a DocAPI app code, which a
        proxy in front of DocAPI sends when it has no route to it. After a read
        timeout or a 504, a small request to /v1/me checks whether DocAPI answers;
        if it does, the request was only slow, e.g. a large upload, and it fails
        as an ordinary error, not an outage. self.outage_monitor, an
        OutageMonitor, logs and mails outages. A job outcome that could not be
        reported is reported once DocAPI is reachable again, before the worker
        polls for a new job; results that could not be uploaded are uploaded
        first, so the job is not processed again.
        """
        # Settings are checked before the adapter is built or directories are created.
        check_mail_subject(mail_subject)
        self.worker_mail_sender: Optional[WorkerMailSender] = None
        if mail_sender is not None:
            self.worker_mail_sender = WorkerMailSender(mail_sender, mail_receivers, hostname=hostname,
                                                       subject=mail_subject)
        self.identity = type(self).__name__ if identity is None else identity
        # Until DocAPI has said what the key's label is.
        self._identity_from_key = identity is None
        self._identity_warned = False
        self._connector = connector
        self.alive_monitor = AliveMonitor(self.worker_mail_sender, alive_mail_startup, alive_mail_interval_seconds,
                                          active_job=self._current_job_id, job_history=lambda: self.job_history)
        self._lost_report: Optional["_LostReport"] = None
        self.outage_monitor = OutageMonitor(self.worker_mail_sender, outage_grace_seconds,
                                            outage_mail_interval_seconds, active_job=self._active_job_id,
                                            report_pending=lambda: self._lost_report is not None)
        self.error_messages = dict(DEFAULT_WORKER_ERRORS)
        for name, message in (error_messages or {}).items():
            if name not in self.error_messages:
                raise ValueError(f"Unknown worker error message: {name}")
            if not isinstance(message, WorkerErrorMessage):
                raise TypeError(f"{name} must be a WorkerErrorMessage")
            context = {"kind": "image", "filename": "page.jpg"} if name == "download_input" else {}
            for text in (message.final_log, message.final_log_user):
                if text is not None:
                    text.format(**context)
            self.error_messages[name] = message
        self.adapter = Adapter(api_url, _ReachabilityConnector(connector, self.outage_monitor, api_url))
        self._set_user_agent()

        # Setup directory structure
        if base_dir:
            base_path = Path(base_dir)
            self.jobs_dir = jobs_dir or str(base_path / "jobs")
            self.engines_dir = engines_dir or str(base_path / "engines")
        else:
            if not jobs_dir or not engines_dir:
                raise ValueError("Either base_dir must be specified, or both jobs_dir and engines_dir must be provided")
            self.jobs_dir = jobs_dir
            self.engines_dir = engines_dir
            
        # Create directories
        os.makedirs(self.jobs_dir, exist_ok=True)
        os.makedirs(self.engines_dir, exist_ok=True)
        
        self.polling_interval = polling_interval

        self.cleanup_job_dir = cleanup_job_dir
        self.cleanup_old_engines = cleanup_old_engines

        self.current_job: Optional[Job] = None
        self.current_lease: Optional[JobLease] = None

        self.prefer_image_url = prefer_image_url
        self.prefer_alto_url = prefer_alto_url
        self.prefer_page_url = prefer_page_url

        self.job_history = JobHistory(job_history_path) if job_history_path is not None else None

    @property
    def identity(self) -> str:
        """The worker's name, in its emails and as the User-Agent of its requests."""
        return self._identity

    @identity.setter
    def identity(self, identity: str) -> None:
        self._identity = identity
        if self.worker_mail_sender is not None:
            self.worker_mail_sender.identity = identity

    def _active_job_id(self) -> Optional[str]:
        """The job being worked on, also when only its lease is known."""
        job = self.current_job if self.current_job is not None else self.current_lease
        return None if job is None else str(job.id)

    def _current_job_id(self) -> Optional[str]:
        """The job being processed; a lease alone does not count."""
        return None if self.current_job is None else str(self.current_job.id)

    @classmethod
    def from_config(cls, config: Config) -> "DocWorkerWrapper":
        """Create the worker from a doc_worker.config.Config.

        The config was checked when it was built; this only reads it. The
        wrapper's arguments come from wrapper_args(), a subclass's own from
        subclass_args(). To build the worker any other way, pass its arguments
        to the class directly.
        """
        return cls(**cls.wrapper_args(config), **cls.subclass_args(config))

    @classmethod
    def wrapper_args(cls, config: Config) -> dict:
        """The wrapper's constructor arguments, from the settings of Config itself.

        Builds the Connector, the MailSender (when a mail server is set) and
        the error messages.
        """
        if config.WORKER_KEY_FILE:
            with open(config.WORKER_KEY_FILE, encoding="utf-8") as file:
                api_key = file.read().strip()
            if not api_key:
                raise ValueError("WORKER_KEY_FILE is empty")
        else:
            api_key = config.WORKER_KEY
        connector = Connector(api_key=api_key,
                              timeout=(config.API_CONNECT_TIMEOUT_SECONDS, config.API_READ_TIMEOUT_SECONDS))
        mail_sender = None
        if config.MAIL_SERVER:
            mail_sender = MailSender(
                server=config.MAIL_SERVER, port=config.MAIL_PORT, security=config.MAIL_SECURITY,
                ca_file=config.MAIL_CA_FILE, timeout=config.MAIL_TIMEOUT_SECONDS,
                sender_mail=config.MAIL_SENDER_MAIL, sender_name=config.MAIL_SENDER_NAME,
                username=config.MAIL_USERNAME, password=config.MAIL_PASSWORD,
                send_receiver_limit=config.MAIL_SEND_RECEIVER_LIMIT,
                send_receiver_window_seconds=config.MAIL_SEND_RECEIVER_WINDOW_SECONDS,
                send_server_limit=config.MAIL_SEND_SERVER_LIMIT,
                send_server_window_seconds=config.MAIL_SEND_SERVER_WINDOW_SECONDS)
        error_messages = {
            name: WorkerErrorMessage(getattr(config, f"ERROR_{name.upper()}_LOG"),
                                     getattr(config, f"ERROR_{name.upper()}_LOG_USER"))
            for name in DEFAULT_WORKER_ERRORS}
        return dict(
            api_url=config.API_URL, connector=connector,
            base_dir=config.BASE_DIR, jobs_dir=config.JOBS_DIR, engines_dir=config.ENGINES_DIR,
            cleanup_job_dir=config.CLEANUP_JOB_DIR, cleanup_old_engines=config.CLEANUP_OLD_ENGINES,
            job_history_path=config.JOB_HISTORY_PATH, polling_interval=config.POLLING_INTERVAL,
            prefer_image_url=config.PREFER_IMAGE_URL, prefer_alto_url=config.PREFER_ALTO_URL,
            prefer_page_url=config.PREFER_PAGE_URL,
            mail_sender=mail_sender, mail_receivers=config.MAIL_RECEIVER_MAILS,
            identity=config.WORKER_IDENTITY, hostname=config.WORKER_HOSTNAME, mail_subject=config.MAIL_SUBJECT,
            alive_mail_startup=config.ALIVE_MAIL_STARTUP,
            alive_mail_interval_seconds=config.ALIVE_MAIL_INTERVAL_SECONDS,
            outage_grace_seconds=config.OUTAGE_GRACE_SECONDS,
            outage_mail_interval_seconds=config.OUTAGE_MAIL_INTERVAL_SECONDS,
            error_messages=error_messages,
        )

    @classmethod
    def subclass_args(cls, config: Config) -> dict:
        """The constructor arguments of a subclass, from the settings its config adds.

        Each setting a subclass of Config adds goes to the constructor parameter of
        the same name in lower case: MODEL_PATH to model_path. A setting without
        such a parameter is an error, so none is silently left unused. The settings
        of Config itself are for the wrapper alone, even when a parameter has the
        name of one (mail_password).

        A subclass that builds something from its settings before the worker exists,
        e.g. loads a model, overrides this and returns all its arguments itself:
        {"model": load_model(config.MODEL_PATH)}. The wrapper's arguments are built
        as usual; returning one of them fails with "got multiple values", as the
        wrapper's settings are overridden when the config is built.
        """
        named = (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
        wrapper = inspect.signature(DocWorkerWrapper.__init__).parameters
        parameters = set()
        for klass in cls.__mro__[:cls.__mro__.index(DocWorkerWrapper)]:
            if "__init__" in vars(klass):
                parameters |= {name for name, parameter in inspect.signature(klass.__init__).parameters.items()
                               if parameter.kind in named and name not in wrapper}
        own = [name for name in type(config).setting_names() if name not in Config.setting_names()]
        unused = [name for name in own if name.lower() not in parameters]
        if unused:
            raise TypeError(f"{type(config).__name__} settings without a parameter of {cls.__name__}: "
                            f"{', '.join(unused)}; add the parameters, or override subclass_args()")
        return {name.lower(): getattr(config, name) for name in own}

    def job_log_handler(self, path: str) -> logging.FileHandler:
        """Create the handler that writes a job's log to path, worker.log in the job's directory.

        By default it takes the format of the first root handler that has one, else a
        format of its own, and lets every level through: what reaches it is decided by
        the loggers' levels. Override it for another format or level. The wrapper adds a
        filter that leaves out the ALIVE reports, and closes the handler after the job.
        """
        handler = logging.FileHandler(path)
        formatter = next((root_handler.formatter for root_handler in logging.getLogger().handlers
                          if root_handler.formatter is not None), None)
        handler.setFormatter(formatter or logging.Formatter("%(asctime)s : %(levelname)s : %(name)s : %(message)s"))
        handler.setLevel(logging.DEBUG)
        return handler

    @contextmanager
    def job_logging(self, job_log_file_handler: logging.FileHandler):
        """Send everything logged while a job runs into the job's log, worker.log in its directory.

        By default the handler is attached to the root logger from the moment the job is
        acquired until it is done, after on_job_finished. The log then holds the messages of
        the wrapper, of the worker's own code and of the libraries it uses, from every thread
        but the ALIVE reports', as far as their loggers' levels let them through. Override it for a different scope,
        e.g. only some loggers; the handler is closed after this returns.
        """
        root_logger = logging.getLogger()
        root_logger.addHandler(job_log_file_handler)
        try:
            yield
        finally:
            root_logger.removeHandler(job_log_file_handler)

    @abstractmethod
    def process_job(self,
                    job: Job,
                    images_dir: str,
                    result_dir: str,
                    alto_dir: Optional[str] = None,
                    page_xml_dir: Optional[str] = None,
                    meta_file: Optional[str] = None,
                    engine_dir: Optional[str] = None) -> WorkerResponse:
        """
        Process the current job data and save results to the specified directory.
        
        This method must be implemented by subclasses to define the actual processing logic.
        Everything logged meanwhile goes into the job's log (see job_logging).
        
        Args:
            job: The job object containing job metadata
            images_dir: Directory path containing the downloaded images
            result_dir: Directory path where processing results should be saved
            alto_dir: Optional directory path containing ALTO XML files
            page_xml_dir: Optional directory path containing PAGE XML files
            meta_file: Optional path to the meta.json file
            engine_dir: Optional directory path containing engine files
            
        Returns:
            WorkerResponse.ok() on success, WorkerResponse.error() for a retryable
            error, or WorkerResponse.fail() for a permanent failure.
        """
        pass
    
    def _report_job_response(self, response: WorkerResponse, job_id: Optional[str] = None) -> bool:
        """Send this response unchanged; return only whether reporting succeeded.

        Reporting failures trigger a worker failure email. They never replace the
        pipeline response or send a different job state as a fallback.
        """
        if job_id is None:
            if self.current_job is not None:
                job_id = self.current_job.id
            elif self.current_lease is not None:
                job_id = self.current_lease.id
        technical, user = self._prepare_final_logs(response)
        if response.status != WorkerResponseStatus.OK:
            logger.error("%s for job %s", technical or user or response.status.value, job_id,
                         exc_info=(type(response.exception), response.exception, response.exception.__traceback__)
                         if response.exception is not None else None)
        if job_id is None:
            self._notify_worker_failure("Cannot report job outcome without a job ID.")
            return False
        try:
            result = self._send_job_report(response, job_id)
        except Exception as exception:
            if self._unreachable(exception):
                return self._hold_lost_report(job_id, response)
            logger.exception("Could not report the job outcome to DocAPI")
            self._notify_worker_failure(f"Could not report the outcome of job {job_id} to DocAPI",
                                        exception=exception)
            return False
        if self._unreachable(result):
            return self._hold_lost_report(job_id, response)
        if not result.is_success:
            self._notify_worker_failure(f"Could not report the outcome of job {job_id} to DocAPI "
                                        f"(status {result.status}, code {result.code})")
            return False
        return True

    def _send_job_report(self, response: WorkerResponse, job_id: str) -> AdapterResponse:
        technical, user = self._prepare_final_logs(response)
        report = {
            WorkerResponseStatus.OK: self.adapter.patch_job_finish,
            WorkerResponseStatus.ERROR: self.adapter.patch_job_error,
            WorkerResponseStatus.FAIL: self.adapter.patch_job_fail,
        }[response.status]
        return report(final_log=technical, final_log_user=user, job_id=job_id)

    def _hold_lost_report(self, job_id: str, response: WorkerResponse, result_zip_path: Optional[str] = None) -> bool:
        """Keep an outcome DocAPI could not receive, and the results it could not
        receive, if any; both are sent on reconnect."""
        if result_zip_path is not None:
            logger.warning("DocAPI is unreachable; the results of job %s are uploaded and its outcome (%s) "
                           "reported once it is reachable again", job_id, response.status.value)
        else:
            logger.warning("DocAPI is unreachable; the outcome of job %s (%s) is reported once it is reachable again",
                           job_id, response.status.value)
        job_dir = (self.get_job_data_path()
                   if self.current_job is not None and str(self.current_job.id) == str(job_id) else None)
        self._lost_report = _LostReport(str(job_id), response, result_zip_path=result_zip_path, job_dir=job_dir)
        return False

    def _prepare_final_logs(self, response: WorkerResponse) -> tuple[Optional[str], Optional[str]]:
        technical = response.final_log
        if response.error_adapter_response is not None:
            adapter_response = response.error_adapter_response
            details = f"Status: {adapter_response.status}, Code: {adapter_response.code}"
            if adapter_response.response is not None:
                details += f", Response: {adapter_response.response.text}"
            technical = _join(technical, details)
        if response.exception is not None:
            details = f"Exception: {type(response.exception).__name__}: {response.exception}"
            if response.exception.__traceback__ is not None:
                details += "\n" + "".join(traceback.format_exception(response.exception)).rstrip()
            technical = _join(technical, details)
        return technical, response.final_log_user

    def _make_error_response(self, name: str, *, exception: Optional[Exception] = None,
               error_adapter_response: Optional[AdapterResponse] = None, **context) -> WorkerResponse:
        """Build one automatic retryable error from the configured messages."""
        message = self.error_messages[name]
        return WorkerResponse.error(
            final_log=message.final_log.format(**context) if message.final_log is not None else None,
            final_log_user=message.final_log_user.format(**context) if message.final_log_user is not None else None,
            exception=exception, error_adapter_response=error_adapter_response)

    def request_job(self) -> Optional[WorkerResponse]:
        """
        Request a job lease from the API.
        
        Returns:
            WorkerResponse indicating success or failure, None if no jobs available
        """
        logger.debug("Requesting job lease...")
        
        # A failed previous job must not become the target of a later polling
        # error. A lease acquired below still identifies a job if get_job fails.
        self.current_job = None
        self.current_lease = None
        lease_response = self.adapter.post_job_lease()
        if not lease_response.is_success or not lease_response.data:
            if not lease_response.is_success:
                logger.warning(f"Job lease request failed. Status: {lease_response.status}, Code: {lease_response.code}")
                return self._make_error_response("request_job_lease", error_adapter_response=lease_response)
            else:
                logger.debug("No jobs available in queue")
                return None
            
        self.current_lease = lease_response.data
        logger.debug(f"Leased job {lease_response.data.id}, expires at {lease_response.data.lease_expire_at}")
        
        # Get full job details
        job_response = self.adapter.get_job(lease_response.data.id, set_job_if_successful=True)
        if not job_response.is_success:
            return self._make_error_response("get_job_details", error_adapter_response=job_response)
            
        self.current_job = job_response.data
        logger.debug(f"Job {job_response.data.id} details:\n{job_response}")
        
        return WorkerResponse.ok()
    
    def _check_and_download_engine_files(self) -> WorkerResponse:
        """
        Check if engine files are up to date and download if necessary.
        
        Returns:
            WorkerResponse indicating success or failure
        """
        if not self.current_job:
            return self._make_error_response("no_current_job")
            
        if not self.current_job.engine_id or not self.current_job.engine_files_updated:
            logger.debug("No engine files required for this job")
            return WorkerResponse.ok()
            
        engine_id = str(self.current_job.engine_id)
        engine_timestamp = self.current_job.engine_files_updated
        
        # Check if we have current files
        engine_dir = os.path.join(self.engines_dir, f"{engine_timestamp.strftime('%Y-%m-%dT%H:%M:%S')}.{engine_id}")
        
        if os.path.exists(engine_dir):
            logger.debug(f"Engine files for {engine_id} are up to date")
            return WorkerResponse.ok()
                
        logger.debug(f"Downloading engine files for {engine_id}...")
        
        # Clean up old engine versions if flag is enabled
        if self.cleanup_old_engines:
            self._cleanup_engine_versions(engine_id)

        zip_path = os.path.join(engine_dir, "engine.zip")

        # Create engine directory
        os.makedirs(engine_dir, exist_ok=True)

        engine_response = self.adapter.get_engine_files_stream(engine_id=engine_id, output_path=zip_path)
        if not engine_response.is_success:
            return self._make_error_response("download_engine_files", error_adapter_response=engine_response)

        # Extract ZIP contents
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(engine_dir)
            
        # Remove ZIP file after extraction
        os.remove(zip_path)
        
        logger.debug(f"Engine files extracted to {engine_dir}")
        return WorkerResponse.ok() 
    
    def _cleanup_engine_versions(self, engine_id: str) -> None:
        """
        Remove all engine directories for the given engine_id.
        
        Args:
            engine_id: The engine ID to remove all versions for
        """
        # Find all directories matching the pattern *.{engine_id}
        pattern = os.path.join(self.engines_dir, f"*.{engine_id}")
        engine_dirs = glob.glob(pattern)
        
        for engine_dir in engine_dirs:
            if os.path.isdir(engine_dir):
                logger.debug(f"Removing engine directory: {engine_dir}")
                shutil.rmtree(engine_dir)
    
    def _download_job_data(self) -> WorkerResponse:
        """
        Download all required job data (images, ALTO, PAGE, meta JSON).
        
        Returns:
            WorkerResponse indicating success or failure
        """
        if not self.current_job:
            return self._make_error_response("no_current_job")
            
        job_dir = self.get_job_data_path()
        os.makedirs(job_dir, exist_ok=True)
        
        # Create subdirectories for different file types
        images_dir = os.path.join(job_dir, "images")
        os.makedirs(images_dir, exist_ok=True)
        
        if self.current_job.alto_required:
            altos_dir = os.path.join(job_dir, "alto")
            os.makedirs(altos_dir, exist_ok=True)
            
        if self.current_job.page_required:
            pages_dir = os.path.join(job_dir, "page_xml")
            os.makedirs(pages_dir, exist_ok=True)
        
        # Source selection and saving are identical for uploaded and linked inputs.
        for image in self.current_job.images:
            paths = {"image": os.path.join(images_dir, image.name)}
            xml_name = f"{os.path.splitext(image.name)[0]}.xml"
            if self.current_job.alto_required:
                paths["alto"] = os.path.join(altos_dir, xml_name)
            if self.current_job.page_required:
                paths["page"] = os.path.join(pages_dir, xml_name)
            for kind, path in paths.items():
                result = self._download_input(image, kind, path)
                if result.status != WorkerResponseStatus.OK:
                    return result

        # Download meta JSON if required
        if self.current_job.meta_json_required:
            logger.debug("Downloading meta JSON...")
            meta_response = self.adapter.get_meta_json(self.current_job.id)
            if not meta_response.is_success:
                return self._make_error_response("download_meta_json", error_adapter_response=meta_response)
                
            meta_path = os.path.join(job_dir, "meta.json")
            with open(meta_path, 'w', encoding='utf-8') as f:
                f.write(meta_response.data)
            logger.debug(f"Saved meta JSON to {meta_path}")
        
        logger.debug(f"All job data downloaded successfully to {job_dir}")
        return WorkerResponse.ok()

    def _download_input(self, image, kind: str, path: str) -> WorkerResponse:
        """Choose a source, decode its input, and save it in the job directory."""
        label = "image" if kind == "image" else kind.upper()
        source_url = self._get_input_source_url(image, kind)
        if source_url is not None:
            try:
                data = self._download_external_input(source_url, kind)
            except (requests.RequestException, ValueError, cv2.error) as exc:
                return self._make_error_response("download_input", exception=exc, kind=label, filename=image.name)
        else:
            response = getattr(self.adapter, f"get_{kind}")(str(image.id), self.current_job.id)
            if not response.is_success:
                return self._make_error_response("download_input", error_adapter_response=response, kind=label, filename=image.name)
            data = response.data

        if kind == "image":
            cv2.imwrite(path, data)
        else:
            with open(path, "w", encoding="utf-8") as output:
                output.write(data)
        logger.debug(f"Saved {label} to {path}")
        return WorkerResponse.ok()

    def _get_input_source_url(self, image, kind: str) -> Optional[str]:
        """Choose a source for one input; override this policy in a worker.

        None uses DocAPI's uploaded file. Per-input preference flags apply when
        both sources exist; otherwise use the available source.
        """
        source_url = getattr(image, f"{kind}_url")
        if source_url is not None and (getattr(self, f"prefer_{kind}_url") or not getattr(image, f"{kind}_uploaded")):
            return source_url
        return None

    def _download_external_input(self, url: str, kind: str):
        """Download and decode a worker input without using DocAPI credentials."""
        # A fresh request never uses the authenticated DocAPI session or its cookies.
        with requests.get(url, timeout=60) as response:
            response.raise_for_status()
            if kind == "image":
                image = cv2.imdecode(np.frombuffer(response.content, dtype=np.uint8), cv2.IMREAD_COLOR)
                if image is None:
                    raise ValueError("External source did not contain a decodable image")
                return image
            return response.content.decode("utf-8")


    def _zip_results(self, results_dir: str) -> WorkerResponse:
        """
        Create a ZIP file containing all results from the result directory.
        
        Args:
            results_dir: Directory containing the results to zip
            
        Returns:
            WorkerResponse indicating success or failure
        """
        job_dir = self.get_job_data_path()
        if not job_dir:
            return self._make_error_response("job_data_path")
            
        zip_path = os.path.join(job_dir, "result.zip")
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for root, dirs, files in os.walk(results_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    # Create relative path within the ZIP
                    arcname = os.path.relpath(file_path, results_dir)
                    zip_file.write(file_path, arcname)
                    
        logger.debug(f"Results zipped to {zip_path}")
        return WorkerResponse.ok()


    def _upload_results(self) -> WorkerResponse:
        """
        Upload the result ZIP file to the API.
        
        Returns:
            WorkerResponse indicating success or failure
        """
        if not self.current_job:
            return self._make_error_response("no_job_for_result_upload")
            
        job_dir = self.get_job_data_path()
        if not job_dir:
            return self._make_error_response("job_data_path")
            
        zip_path = os.path.join(job_dir, "result.zip")
            
        upload_response = self.adapter.post_result(zip_path, job_id=self.current_job.id)
        if upload_response.is_success:
            logger.debug(f"Results uploaded successfully for job {self.current_job.id}")
            return WorkerResponse.ok()
        else:
            return self._make_error_response("upload_results", error_adapter_response=upload_response)


    def run_job_pipeline(self) -> Optional[Job]:
        """
        Execute the complete job pipeline: request, download, process, zip, and upload.
        This should be called repeatedly in the life cycle of the worker.
        
        Returns:
            Job object if successfully processed and uploaded, None if failed or no jobs available
        """
        try:
            return self._run_job_pipeline()
        finally:
            # Reporting, on_job_finished and file cleanup still need the job.
            # Clear local state only after they finish, for every exit path.
            self.current_job = None
            self.current_lease = None

    def _run_job_pipeline(self) -> Optional[Job]:
        """Run one cycle; run_job_pipeline clears its state after all cleanup."""
        # An outcome lost in an outage goes first, so a new lease never meets it.
        if not self._settle_lost_report():
            return None

        # Request job lease
        try:
            job_response = self.request_job()
        except Exception as e:
            job_response = self._make_error_response("request_job", exception=e)

        if job_response is None:
            logger.info("No jobs available in queue")
            return None
        if job_response.status != WorkerResponseStatus.OK:
            if self.current_lease is not None or self.current_job is not None:
                # A lease exists even if fetching the full job failed.
                self._report_job_response(job_response)
            elif self._unreachable(job_response):
                # The outage mails cover an unreachable server.
                logger.warning("Could not request a job: DocAPI is unreachable")
            else:
                # Polling failed before any job was assigned. There is no job
                # to finalize; mail the actual failure directly.
                technical, user = self._prepare_final_logs(job_response)
                self._notify_worker_failure(technical or user or "Failed to request job")
            return None
            
        if not self.current_job:
            logger.error("Job request succeeded but no current job set")
            return None

        job = self.current_job
        started_at = datetime.now(timezone.utc)
        job_started = time.monotonic()
        job_dir = None
        job_log_file_handler = None
        job_log = ExitStack()
        job_response: Optional[WorkerResponse] = None
        job_response_reported: Optional[bool] = None
        interrupted = False

        try:
            job_dir = self.get_job_data_path()
            os.makedirs(job_dir, exist_ok=True)

            job_log_file_handler = self.job_log_handler(os.path.join(job_dir, "worker.log"))
            # The ALIVE reports run beside every job and are not part of it.
            job_log_file_handler.addFilter(lambda record: record.threadName != AliveMonitor.THREAD_NAME)
            job_log.enter_context(self.job_logging(job_log_file_handler))

            # Log job acquisition summary
            logger.info("")
            logger.info(f"Job {self.current_job.id} acquired")
            logger.info(f"Created: {self.current_job.created_date}")
            logger.info(f"Started: {self.current_job.started_date}")
            logger.info(f"Lease expires: {self.current_lease.lease_expire_at if self.current_lease else 'unknown'}")
            logger.info(f"Engine: name={self.current_job.engine_name}, verison={self.current_job.engine_version}")
            logger.info(f"Images: {len(self.current_job.images)}")
            logger.info(f"Requirements: ALTO={self.current_job.alto_required}, "
                        f"PAGE={self.current_job.page_required}, "
                        f"Meta={self.current_job.meta_json_required}")
            logger.info("")

            # Download engine files if needed
            try:
                logger.debug(f"Checking engine files for job {self.current_job.id}...")
                engine_response = self._check_and_download_engine_files()
                if engine_response.status != WorkerResponseStatus.OK:
                    job_response = engine_response
                    job_response_reported = self._report_job_response(job_response)
                    return None
            except Exception as e:
                response = self._make_error_response("prepare_engine_files", exception=e)
                job_response = response
                job_response_reported = self._report_job_response(job_response)
                return None
            
            # Download all job data
            try:
                logger.debug(f"Downloading job data for job {self.current_job.id}...")
                download_response = self._download_job_data()
                if download_response.status != WorkerResponseStatus.OK:
                    job_response = download_response
                    job_response_reported = self._report_job_response(job_response)
                    return None
            except Exception as e:
                response = self._make_error_response("download_job_data", exception=e)
                job_response = response
                job_response_reported = self._report_job_response(job_response)
                return None
            
            # Create result directory
            try:
                job_dir = self.get_job_data_path()
                if not job_dir:
                    response = self._make_error_response("create_job_workspace")
                    job_response = response
                    job_response_reported = self._report_job_response(job_response)
                    return None
                    
                result_dir = os.path.join(job_dir, "result")
                os.makedirs(result_dir, exist_ok=True)
            except Exception as e:
                response = self._make_error_response("create_job_workspace", exception=e)
                job_response = response
                job_response_reported = self._report_job_response(job_response)
                return None
            
            # Process the job data
            try:
                logger.debug(f"Processing job {self.current_job.id}...")
                
                # Prepare paths for processing
                images_dir = os.path.join(job_dir, "images")
                alto_dir = os.path.join(job_dir, "alto") if self.current_job.alto_required else None
                page_xml_dir = os.path.join(job_dir, "page_xml") if self.current_job.page_required else None
                meta_file = os.path.join(job_dir, "meta.json") if self.current_job.meta_json_required else None
                engine_dir = self.get_engine_data_path()
                
                process_response = self.process_job(
                    job=self.current_job,
                    images_dir=images_dir,
                    result_dir=result_dir,
                    alto_dir=alto_dir,
                    page_xml_dir=page_xml_dir,
                    meta_file=meta_file,
                    engine_dir=engine_dir
                )
                if process_response.status != WorkerResponseStatus.OK:
                    job_response = process_response
                    job_response_reported = self._report_job_response(job_response)
                    return None

            except Exception as e:
                response = self._make_error_response("process_job", exception=e)
                job_response = response
                job_response_reported = self._report_job_response(job_response)
                return None
            
            # Zip the results
            try:
                logger.debug(f"Zipping results for job {self.current_job.id}...")
                zip_response = self._zip_results(result_dir)
                if zip_response.status != WorkerResponseStatus.OK:
                    job_response = zip_response
                    job_response_reported = self._report_job_response(job_response)
                    return None
            except Exception as e:
                response = self._make_error_response("package_results", exception=e)
                job_response = response
                job_response_reported = self._report_job_response(job_response)
                return None
            
            # Upload the results
            logger.debug(f"Uploading results for job {self.current_job.id}...")
            try:
                upload_response = self._upload_results()
            except Exception as e:
                upload_response = self._make_error_response("upload_results_to_server", exception=e)
            if upload_response.status != WorkerResponseStatus.OK:
                if self._unreachable(upload_response):
                    # The results are done; upload them once DocAPI is back
                    # instead of processing the job again.
                    job_response = process_response
                    job_response_reported = self._hold_lost_report(
                        job.id, job_response, result_zip_path=os.path.join(job_dir, "result.zip"))
                else:
                    job_response = upload_response
                    job_response_reported = self._report_job_response(job_response)
                return None
            
            # Report success only after packaging and uploading the results.
            job_response = process_response
            job_response_reported = self._report_job_response(job_response)
            if not job_response_reported:
                return None

            logger.info(f"Job {self.current_job.id} processed successfully")

            return job
            
        except KeyboardInterrupt:
            # Handle Ctrl+C gracefully by releasing the lease instead of failing the job
            logger.info("Pipeline interrupted by user, releasing job lease...")
            interrupted = True
            self.release_lease()
            raise
        except Exception as e:
            response = self._make_error_response("unexpected_processing", exception=e)
            job_response = response
            job_response_reported = self._report_job_response(job_response)
            return None
        finally:
            try:
                if not interrupted:
                    response = job_response if job_response is not None else self._make_error_response("unexpected_processing")
                    try:
                        self.on_job_finished(job, response, job_dir, time.monotonic() - job_started, job_log_file_handler)
                    except Exception:
                        logger.exception("on_job_finished failed")
                # The job directory outlives on_job_finished, which may read the results.
                if (self.cleanup_job_dir and job_response_reported and job_response is not None
                        and job_response.status == WorkerResponseStatus.OK):
                    try:
                        if job_dir and os.path.exists(job_dir):
                            shutil.rmtree(job_dir)
                            logger.debug(f"Cleaned up job directory: {job_dir}")
                    except Exception:
                        logger.exception("Failed to clean up job directory")
                # The job log ends here: nothing logged later belongs to this job.
                try:
                    job_log.close()
                except Exception:
                    logger.exception("Failed to stop the job log")
                if job_log_file_handler is not None:
                    job_log_file_handler.close()
            finally:
                if interrupted:
                    status = "interrupted"
                elif job_response is not None:
                    status = job_response.status.value
                else:
                    status = "crashed"
                self._record_job_history(job, started_at, job_started, status, job_response_reported)

    def _record_job_history(self, job: Job, started_at: datetime, started_monotonic: float,
                            status: str, reported_to_api: Optional[bool]) -> None:
        if self.job_history is None:
            return
        try:
            self.job_history.record(
                job, started_at=started_at, finished_at=datetime.now(timezone.utc),
                duration_seconds=time.monotonic() - started_monotonic, status=status, reported_to_api=reported_to_api)
        except Exception as exception:
            # History is observational. A disk/format problem must not change
            # the job outcome or prevent clearing local pipeline state.
            logger.exception("Could not save worker job history")
            self._notify_worker_failure("Could not save worker job history.", exception=exception)

    def on_job_finished(self,
                        job: Job,
                        response: WorkerResponse,
                        job_dir: Optional[str],
                        duration_s: float,
                        job_log_file_handler: Optional[logging.FileHandler]) -> None:
        """
        Called once for every leased job when it is done, whether it succeeded or failed at any step.

        Does nothing by default; override it e.g. to log or email a summary of the job. It is called
        before the job directory is cleaned up and before the job log file handler is closed.

        Args:
            job: The job
            response: Original pipeline response: OK after processing and upload,
                otherwise the pipeline error/failure with its exception if present.
                Reporting failure does not replace it; delivery status is recorded
                separately in history as reported_to_api.
            job_dir: The job's data directory with its results, None if it was not created
            duration_s: Seconds from leasing the job until now
            job_log_file_handler: File handler of the job-specific log, None if it was not created
        """

    def start(self) -> None:
        """Poll and process jobs, with optional startup and periodic mail.

        KeyboardInterrupt shuts down gracefully. Unexpected failures are logged
        and re-raised after attempting a failure email when a mail sender and
        receivers are configured. A hard kill or host failure requires an
        external monitor.
        """
        logger.info(f"Worker started, polling for jobs every {self.polling_interval} seconds")
        try:
            self._resolve_identity()
            self.alive_monitor.start()
            while True:
                if self._identity_from_key:
                    self._resolve_identity()
                result = self.run_job_pipeline()
                self.alive_monitor.pipeline_returned()
                if result is None:
                    time.sleep(self.polling_interval)
        except KeyboardInterrupt:
            logger.info("Worker shutting down gracefully")
        except BaseException as exception:
            logger.exception("Unexpected error in worker loop")
            self._notify_worker_failure("Worker terminated unexpectedly.", exception=exception)
            raise
        finally:
            self.alive_monitor.close()


    def _set_user_agent(self) -> None:
        """Send the identity as the User-Agent of the worker's requests."""
        # A header value is a single line.
        self._connector.set_user_agent(" ".join(self.identity.split()))

    def _resolve_identity(self) -> None:
        """Take the identity from the key's label on DocAPI, when none was given.

        While DocAPI cannot answer, the class name stays and the next loop pass
        tries again; an answer without a label keeps the class name for good.
        """
        if not self._identity_from_key:
            return
        try:
            response = self.adapter.get_me()
        except requests.RequestException as error:
            if not self._identity_warned:
                logger.warning(f"Could not read the API key's label from DocAPI ({type(error).__name__}); "
                               f"the worker is {self.identity!r} until it can.")
                self._identity_warned = True
            return
        if not response.is_success:
            if not self._identity_warned:
                logger.warning(f"Could not read the API key's label from DocAPI (HTTP {response.status}); "
                               f"the worker is {self.identity!r} until it can.")
                self._identity_warned = True
            return
        self._identity_from_key = False
        data = response.data.get("data") if isinstance(response.data, dict) else None
        label = data.get("label") if isinstance(data, dict) else None
        if isinstance(label, str) and label.strip():
            self.identity = label.strip()
            self._set_user_agent()
            logger.info(f"Worker identity from the API key's label: {self.identity!r}")
        else:
            logger.warning(f"DocAPI returned no label for the API key; the worker stays {self.identity!r}.")

    def get_job_data_path(self) -> Optional[str]:
        """
        Get the path to the current job's data directory.
        
        Returns:
            Path to job data directory, or None if no current job
        """
        if not self.current_job:
            return None
        
        started_timestamp = self.current_job.started_date.isoformat() if self.current_job.started_date else 'unknown'
        return os.path.join(self.jobs_dir, f"{started_timestamp}.{str(self.current_job.previous_attempts)}.{self.current_job.id}")
    
    def get_engine_data_path(self) -> Optional[str]:
        """
        Get the path to the current job's engine directory.
        
        Returns:
            Path to engine directory, or None if no engine required
        """
        if not self.current_job or not self.current_job.engine_id or not self.current_job.engine_files_updated:
            return None
            
        engine_id = str(self.current_job.engine_id)
        timestamp = self.current_job.engine_files_updated.strftime('%Y-%m-%dT%H:%M:%S')
        return os.path.join(self.engines_dir, f"{timestamp}.{engine_id}")
    
    def get_results_data_path(self) -> Optional[str]:
        """
        Get the path to the current job's result directory.
        
        Returns:
            Path to result directory, or None if no current job
        """
        job_dir = self.get_job_data_path()
        if not job_dir:
            return None
        return os.path.join(job_dir, "result")
    
    def update_job_progress(self, progress: Optional[float] = None, log: Optional[str] = None, log_user: Optional[str] = None) -> bool:
        """
        Extend the current job lease, optionally with progress update and logs.
        
        Args:
            progress: Optional progress value (0.0 to 1.0) to update job progress
            log: Optional technical log message
            log_user: Optional user-friendly log message
            
        Returns:
            True if the update succeeded or the lease does not require renewal
        """
        if not self.current_job:
            logger.error("No current job to extend lease for")
            return False
        
        # If progress or logs are provided, use progress update endpoint (which also extends lease)
        if progress is not None or log is not None or log_user is not None:    
            response = self.adapter.patch_job_progress_update(
                progress=progress,
                log=log,
                log_user=log_user,
                job_id=self.current_job.id
            )
            
            if response.is_success and response.data:
                self.current_lease = response.data
                logger.debug(f"Job progress updated and lease extended until {response.data.lease_expire_at}")
                return True
            else:
                logger.error(f"Failed to update progress and extend lease. Status: {response.status}, Code: {response.code}")
                return False
        else:
            if self.current_lease is not None and self.current_lease.lease_expire_at is None:
                # Progress/log updates still reach the API, but an infinite lease needs no heartbeat.
                return True
            lease_response = self.adapter.patch_job_lease(self.current_job.id)
            if lease_response.is_success:
                self.current_lease = lease_response.data
                logger.debug(f"Lease extended until {lease_response.data.lease_expire_at}")
                return True
            else:
                logger.error(f"Failed to extend lease. Status: {lease_response.status}, Code: {lease_response.code}")
                return False
    
    def release_lease(self) -> bool:
        """
        Release the current job lease.
        
        Returns:
            True if lease released successfully
        """
        if not self.current_job:
            logger.error("No current job to release lease for")
            return False
            
        release_response = self.adapter.delete_job_lease(self.current_job.id)
        if release_response.is_success:
            logger.info(f"Released lease for job {self.current_job.id}")
            self.current_job = None
            self.current_lease = None
            return True
        else:
            logger.error(f"Failed to release lease. Status: {release_response.status}, Code: {release_response.code}")
            return False

    def send_mail(self, subject: str, body: str, receiver_mails: Optional[Sequence[str]] = None) -> bool:
        """
        Send an email with the worker's mail sender, a separate one to each
        receiver; the sender logs each with its sender and receiver.

        Args:
            subject: Subject of the email
            body: Plain text body of the email
            receiver_mails: Receivers of the email, the worker's mail_receivers by default

        Returns:
            True if every receiver was sent the email; False otherwise, also when
            no mail sender or no receiver is configured
        """
        if self.worker_mail_sender is None:
            logger.warning(f"Not sending email {subject!r}: the worker has no mail sender")
            return False
        return self.worker_mail_sender.send_raw(subject, body, receiver_mails)

    def _notify_worker_failure(self, message: str, *, exception: Optional[BaseException] = None) -> None:
        """Alert when a failure cannot be communicated through job finalization."""
        if exception is not None:
            message = _join(message, "".join(traceback.format_exception(exception)).rstrip())
        logger.error("%s", message)
        if self.worker_mail_sender is not None:
            self.worker_mail_sender.send("FAILURE", message)

    def _settle_lost_report(self) -> bool:
        """Upload the results and report the outcome lost in an outage; False while
        DocAPI is still unreachable, so the worker does not poll for a new job meanwhile.

        Results DocAPI does not take, e.g. because it rejects them or the upload
        times out while DocAPI answers, are not uploaded again: the job is
        reported as a retryable error instead. An outcome DocAPI rejects, e.g.
        because the lease expired and the job was requeued or failed meanwhile, is
        not reported again. An OK outcome that is reported cleans up the job's
        directory when cleanup_job_dir is on.
        """
        lost = self._lost_report
        if lost is None:
            return True
        results, results_detail = None, ""
        if lost.result_zip_path is not None:
            try:
                result = self.adapter.post_result(lost.result_zip_path, job_id=lost.job_id)
            except Exception as exception:
                if self._unreachable(exception):
                    return False
                logger.exception("Could not upload the results of job %s to DocAPI", lost.job_id)
                results, results_detail = "not_uploaded", f"{type(exception).__name__}: {exception}"
                lost.response = self._make_error_response("upload_results_to_server", exception=exception)
            else:
                if self._unreachable(result):
                    return False
                if result.is_success:
                    logger.info("Uploaded the results of job %s after the outage", lost.job_id)
                    results = "uploaded"
                else:
                    logger.warning("DocAPI did not take the results of job %s after the outage. "
                                   "Status: %s, Code: %s", lost.job_id, result.status, result.code)
                    results, results_detail = "not_taken", f"status {result.status}, code {result.code}"
                    lost.response = self._make_error_response("upload_results", error_adapter_response=result)
            # Only the report is left, should DocAPI drop out again.
            lost.result_zip_path = None
        settled = functools.partial(SettledJob, lost.job_id, lost.response.status.value,
                                    results=results, results_detail=results_detail)
        try:
            result = self._send_job_report(lost.response, lost.job_id)
        except Exception as exception:
            if self._unreachable(exception):
                return False
            logger.exception("Could not report the outcome of job %s to DocAPI", lost.job_id)
            return self._lost_report_settled(settled(report="not_reported",
                                                     report_detail=f"{type(exception).__name__}: {exception}"))
        if self._unreachable(result):
            return False
        if not result.is_success:
            # The server requeued or failed the job meanwhile; nothing changes.
            logger.warning("DocAPI did not accept the outcome of job %s after the outage. Status: %s, Code: %s",
                           lost.job_id, result.status, result.code)
            return self._lost_report_settled(settled(report="not_accepted",
                                                     report_detail=f"status {result.status}, code {result.code}"))
        logger.info("Reported the outcome of job %s (%s) after the outage", lost.job_id, lost.response.status.value)
        if (self.cleanup_job_dir and lost.response.status == WorkerResponseStatus.OK
                and lost.job_dir and os.path.exists(lost.job_dir)):
            try:
                shutil.rmtree(lost.job_dir)
                logger.debug(f"Cleaned up job directory: {lost.job_dir}")
            except Exception:
                logger.exception("Failed to clean up job directory")
        return self._lost_report_settled(settled(report="reported"))

    @staticmethod
    def _unreachable(failure: Union[BaseException, AdapterResponse, WorkerResponse, None]) -> bool:
        """Whether a DocAPI request failed because DocAPI is unreachable, as the
        connector decided it; see _ReachabilityConnector."""
        return _ReachabilityConnector.unreachable(failure)

    def _lost_report_settled(self, settled: SettledJob) -> bool:
        self._lost_report = None
        self.outage_monitor.report_settled(settled)
        return True


# --- DocAPI outages: whether a request reached DocAPI. The connector decides
# it and tells the OutageMonitor; the wrapper holds a job outcome that could not
# be reported. ---

@dataclass
class _LostReport:
    """A job outcome that could not be reported because DocAPI was unreachable."""
    job_id: str
    response: WorkerResponse
    result_zip_path: Optional[str] = None   # The result.zip still to upload before the report; None once uploaded.
    job_dir: Optional[str] = None           # Cleaned up once an OK outcome is reported.


class _ReachabilityConnector:
    """Passes a Connector through, decides for each DocAPI request whether it
    reached DocAPI, and tells the OutageMonitor.

    DocAPI is unreachable when a request fails to connect or times out, or gets
    an answer from a proxy in front of DocAPI instead: a 502, 503 or 504, or a
    404 without a DocAPI app code, which the proxy sends when it has no route to
    DocAPI. Every error DocAPI sends itself has a code; other errors without
    one, such as a 413 or 429, may be permanent, so they stay ordinary failures.

    A read timeout or a 504 may also mean that a request, such as a large
    upload, only took too long. After one, a small probe request tells the two
    apart: if DocAPI answers it, the request was slow and DocAPI is reachable.

    The caller gets the exception or the response unchanged. When DocAPI was
    unreachable, it is marked, and unreachable() reads the mark, also from an
    AdapterResponse or a WorkerResponse that carries it.
    """

    REQUEST_METHODS = ("get", "post", "put", "patch", "delete")
    PROBE_ROUTE = "/v1/me"
    # The longest the probe waits for DocAPI's answer; the connect timeout is the connector's.
    PROBE_READ_TIMEOUT_SECONDS = 30
    # Statuses a proxy answers with when it cannot reach DocAPI.
    PROXY_STATUSES = frozenset({502, 503, 504})
    # Set to True on the exception or response of a request that did not reach DocAPI.
    MARK = "docapi_unreachable"

    def __init__(self, connector: Connector, outage_monitor: OutageMonitor, api_url: str):
        self._connector = connector
        self._outage_monitor = outage_monitor
        self._probe_url = urljoin(api_url, self.PROBE_ROUTE.lstrip("/"))

    @classmethod
    def unreachable(cls, failure: Union[BaseException, AdapterResponse, WorkerResponse, None]) -> bool:
        """Whether this failure came from a request that did not reach DocAPI."""
        if isinstance(failure, WorkerResponse):
            return cls.unreachable(failure.exception) or cls.unreachable(failure.error_adapter_response)
        if isinstance(failure, AdapterResponse):
            failure = failure.response
        return getattr(failure, cls.MARK, False) is True

    def __getattr__(self, name):
        attribute = getattr(self._connector, name)
        if name not in self.REQUEST_METHODS:
            return attribute

        def request(url, *args, **kwargs):
            label = f"{name.upper()} {urlsplit(str(url)).path}"
            try:
                response = attribute(url, *args, **kwargs)
            except (requests.ConnectionError, requests.Timeout) as error:
                slow = isinstance(error, requests.Timeout) and not isinstance(error, requests.ConnectTimeout)
                if self._decide(label, f"{type(error).__name__}: {error}", slow):
                    setattr(error, self.MARK, True)
                raise
            status = getattr(response, "status_code", None)
            if self._from_proxy(response):
                if self._decide(label, f"HTTP {status}" + (" without a DocAPI code" if status == 404 else ""),
                                status == 504):
                    setattr(response, self.MARK, True)
            else:
                self._outage_monitor.reached()
            return response

        return request

    @classmethod
    def _from_proxy(cls, response) -> bool:
        """Whether an HTTP answer came from a proxy in front of DocAPI instead of DocAPI."""
        status = getattr(response, "status_code", None)
        # Only a 404's body is read: its code tells DocAPI's own 404 from a proxy's.
        return status in cls.PROXY_STATUSES or (status == 404 and response_code(response) is None)

    def _decide(self, label: str, error: str, maybe_slow: bool) -> bool:
        """Whether a failed request means DocAPI is unreachable: it is, unless the
        request may only have been slow and DocAPI answers the probe."""
        if maybe_slow and self._probe():
            logger.warning("%s failed (%s), but DocAPI answers: the request was too slow, DocAPI is reachable",
                           label, error)
            self._outage_monitor.reached()
            return False
        self._outage_monitor.unreachable(label, error)
        return True

    def _probe(self) -> bool:
        """Whether DocAPI answers a small request."""
        try:
            timeout = getattr(self._connector, "timeout", None)
            connect, read = timeout if isinstance(timeout, tuple) else (timeout, timeout)
            read = self.PROBE_READ_TIMEOUT_SECONDS if read is None else min(read, self.PROBE_READ_TIMEOUT_SECONDS)
            response = self._connector.get(self._probe_url, timeout=(connect, read))
        except Exception:
            logger.debug("The probe did not reach DocAPI", exc_info=True)
            return False
        return not self._from_proxy(response)


class JobHistory:
    """Append-only JSONL attempts and a rebuildable JSON statistics cache.

    Use one history path per worker. The cache holds cumulative totals, unique
    job IDs and the recent records needed for exact rolling windows. Logs,
    exceptions, URLs and engine definitions are excluded from both files.
    """
    _statuses = ("ok", "error", "fail", "interrupted", "crashed")
    # How many of the unreadable lines found since the last statistics() are kept,
    # with their numbers and reasons, for the next ALIVE report.
    INVALID_LINES_KEPT = 5

    def __init__(self, path):
        self.path = Path(path).expanduser()
        self.stats_path = self.path.with_suffix(".stats.json")
        if self.stats_path == self.path:
            self.stats_path = self.path.with_name(self.path.name + ".stats.json")
        self._lock = threading.Lock()
        # Unreadable lines found since the last statistics(); kept in memory only,
        # so each is reported once.
        self._new_invalid_count = 0
        self._new_invalid_lines = []

    @classmethod
    def _validate_entry(cls, entry):
        if not isinstance(entry, dict) or not isinstance(entry.get("job_id"), str):
            raise ValueError("Invalid worker history entry")
        finished = datetime.fromisoformat(entry["finished_at"])
        if finished.tzinfo is None:
            raise ValueError("Worker history timestamps must include a timezone")
        if entry.get("reported_to_api") is not None and type(entry["reported_to_api"]) is not bool:
            raise ValueError("Invalid worker history reporting status")
        if entry.get("status") not in cls._statuses:
            raise ValueError("Invalid worker history status")
        duration = entry.get("duration_seconds")
        if not isinstance(duration, (int, float)) or not math.isfinite(duration) or duration < 0:
            raise ValueError("Invalid worker history duration")

    def _empty_stats(self, now):
        return {
            "version": 3, "history_bytes": 0, "history_lines": 0, "as_of": now.isoformat(),
            "invalid_records": 0,
            "total": {"attempts": 0, "job_ids": {}, "outcomes": dict.fromkeys(self._statuses, 0),
                      "duration_seconds": 0.0, "report_failures": 0},
            "recent": [],
        }

    def _validate_stats(self, data, size, now, rebuild_on_clock_change):
        if data["version"] != 3 or type(data["history_bytes"]) is not int or not 0 <= data["history_bytes"] <= size:
            raise ValueError("Invalid worker statistics cache")
        as_of = datetime.fromisoformat(data["as_of"])
        if as_of.tzinfo is None or (rebuild_on_clock_change and now < as_of):
            # If the clock moved backwards, rebuild the older rolling window.
            raise ValueError("Worker statistics cache needs rebuilding")
        total = data["total"]
        if not isinstance(total["job_ids"], dict) or not isinstance(data["recent"], list):
            raise ValueError("Invalid worker statistics cache")
        counts = [total["attempts"], total["report_failures"], data["invalid_records"], data["history_lines"],
                  *(total["outcomes"][status] for status in self._statuses)]
        if any(type(count) is not int or count < 0 for count in counts):
            raise ValueError("Invalid worker statistics counts")
        if total["attempts"] != sum(total["outcomes"][status] for status in self._statuses):
            raise ValueError("Invalid worker statistics totals")
        if len(total["job_ids"]) > total["attempts"] or not math.isfinite(total["duration_seconds"]) or total["duration_seconds"] < 0:
            raise ValueError("Invalid worker statistics totals")
        for entry in data["recent"]:
            self._validate_entry(entry)

    def _accumulate(self, data, entry, cutoff):
        total = data["total"]
        total["attempts"] += 1
        total["job_ids"][entry["job_id"]] = True
        total["outcomes"][entry["status"]] += 1
        total["duration_seconds"] += entry["duration_seconds"]
        if entry.get("reported_to_api") is False:
            total["report_failures"] += 1
        if datetime.fromisoformat(entry["finished_at"]) >= cutoff:
            data["recent"].append({name: entry.get(name) for name in
                                   ("job_id", "finished_at", "status", "duration_seconds", "reported_to_api")})

    def _invalid_line(self, data, error):
        """Count a history line the statistics cannot use, and keep its number and
        reason until the next statistics()."""
        number = data["history_lines"]
        if isinstance(error, json.JSONDecodeError):
            reason = f"{error.msg} (column {error.colno})"
        else:
            reason = f"missing field {error}" if isinstance(error, KeyError) else str(error)
        data["invalid_records"] += 1
        self._new_invalid_count += 1
        self._new_invalid_lines = (self._new_invalid_lines
                                   + [{"line": number, "reason": reason}])[-self.INVALID_LINES_KEPT:]
        logger.warning("Ignoring line %d of worker history %s: %s", number, self.path, reason)

    def _load_stats(self, now, *, rebuild_on_clock_change=True):
        size = self.path.stat().st_size if self.path.exists() else 0
        try:
            with self.stats_path.open(encoding="utf-8") as stream:
                data = json.load(stream)
            self._validate_stats(data, size, now, rebuild_on_clock_change)
        except FileNotFoundError:
            data = self._empty_stats(now)
        except (ValueError, TypeError, KeyError):
            logger.warning("Rebuilding invalid worker statistics cache: %s", self.stats_path)
            data = self._empty_stats(now)
        cutoff = now - timedelta(days=7)
        data["recent"] = [entry for entry in data["recent"] if
                          datetime.fromisoformat(entry["finished_at"]) >= cutoff]
        # Normal operation starts at EOF and reads nothing. A missing cache
        # rebuilds once; a stale cache only consumes the unaccounted tail.
        if data["history_bytes"] < size:
            with self.path.open("rb") as stream:
                stream.seek(data["history_bytes"])
                for line in stream:
                    data["history_lines"] += 1
                    if line.strip():
                        try:
                            entry = json.loads(line)
                            self._validate_entry(entry)
                        except (ValueError, TypeError, KeyError, UnicodeError) as error:
                            self._invalid_line(data, error)
                        else:
                            self._accumulate(data, entry, cutoff)
                data["history_bytes"] = stream.tell()
        data["as_of"] = max(now, datetime.fromisoformat(data["as_of"])).isoformat()
        return data

    def _save_stats(self, data):
        self.stats_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=self.stats_path.parent,
                                             prefix=f".{self.stats_path.name}.staging-", suffix=".json",
                                             delete=False) as stream:
                temporary = Path(stream.name)
                json.dump(data, stream, ensure_ascii=False, allow_nan=False)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.stats_path)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def record(self, job, *, started_at, finished_at, duration_seconds, status, reported_to_api=None):
        previous_attempts = getattr(job, "previous_attempts", None)
        entry = {
            "job_id": str(job.id), "attempt": 0 if previous_attempts is None else previous_attempts,
            "started_at": started_at.isoformat(), "finished_at": finished_at.isoformat(),
            "duration_seconds": round(duration_seconds, 3), "status": status, "reported_to_api": reported_to_api,
            "engine_id": str(job.engine_id) if getattr(job, "engine_id", None) is not None else None,
            "engine_name": getattr(job, "engine_name", None), "engine_version": getattr(job, "engine_version", None),
            "image_count": getattr(job, "image_count", None),
        }
        if entry["image_count"] is None:
            entry["image_count"] = len(getattr(job, "images", []))
        self._validate_entry(entry)
        payload = (json.dumps(entry, ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")
        with self._lock:
            data = self._load_stats(finished_at, rebuild_on_clock_change=False)
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a+b") as stream:
                # Only inspect the last byte, to separate a partial final line
                # left by a crash. Existing history is never rewritten.
                stream.seek(0, os.SEEK_END)
                prefix = b""
                if stream.tell():
                    stream.seek(-1, os.SEEK_END)
                    if stream.read(1) != b"\n":
                        prefix = b"\n"
                stream.write(prefix + payload)
                stream.flush()
                os.fsync(stream.fileno())
                data["history_bytes"] = stream.tell()
                data["history_lines"] += 1
            self._accumulate(data, entry, finished_at - timedelta(days=7))
            # History is authoritative. If this replacement fails, the next
            # operation consumes the missing tail rather than double-counting.
            self._save_stats(data)

    def statistics(self, now=None):
        """Read the cache and tally only recent records for rolling windows.

        new_invalid_records counts the unreadable lines found since the last call,
        and new_invalid_lines holds the latest of them with their numbers and
        reasons; each is returned once.
        """
        now = datetime.now(timezone.utc) if now is None else now
        with self._lock:
            data = self._load_stats(now)
            if self.path.exists():
                self._save_stats(data)
            new_invalid_records, new_invalid_lines = self._new_invalid_count, self._new_invalid_lines
            self._new_invalid_count, self._new_invalid_lines = 0, []
        total = data["total"]
        result = {"total": {"attempts": total["attempts"], "jobs": len(total["job_ids"]),
                            "outcomes": dict(total["outcomes"]), "duration_seconds": total["duration_seconds"],
                            "report_failures": total["report_failures"]},
                  "invalid_records": data["invalid_records"],
                  "new_invalid_records": new_invalid_records, "new_invalid_lines": new_invalid_lines,
                  "path": str(self.path)}
        for name, days in (("last_7_days", 7), ("last_24_hours", 1)):
            cutoff = now - timedelta(days=days)
            selected = [entry for entry in data["recent"] if
                        cutoff <= datetime.fromisoformat(entry["finished_at"]) <= now]
            counts = Counter(entry["status"] for entry in selected)
            result[name] = {
                "attempts": len(selected), "jobs": len({entry["job_id"] for entry in selected}),
                "outcomes": {status: counts[status] for status in self._statuses},
                "duration_seconds": sum(entry["duration_seconds"] for entry in selected),
                "report_failures": sum(entry.get("reported_to_api") is False for entry in selected),
            }
        return result


def _join(*parts: Optional[str]) -> str:
    """Join the nonempty text parts with blank lines."""
    return "\n\n".join(part for part in parts if part)
