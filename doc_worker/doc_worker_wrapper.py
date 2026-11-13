import logging
import socket
import textwrap
import os
import json
import zipfile
import time
import shutil
import glob
import traceback
import math
import tempfile
import threading
from collections import Counter
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Mapping, Optional, Union
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType

import cv2
import numpy as np
import requests

from doc_api.adapter import Adapter, AdapterResponse
from doc_api.api.schemas.base_objects import Job, JobLease
from doc_api.connector import Connector
from doc_api.tools.mail.mail_sender import MailSender


logger = logging.getLogger(__name__)


def _join(*parts: Optional[str]) -> str:
    return "\n\n".join(part for part in parts if part)


class WorkerResponseStatus(Enum):
    OK = "ok"
    ERROR = "error"
    FAIL = "fail"


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
class WorkerHealthConfig:
    """Opt-in lifecycle mail settings for DocWorkerWrapper.

    Requires the wrapper's mail_sender. An alive interval of zero disables
    periodic mail. Explicit failure mail is handled independently by the wrapper.
    These notifications do not replace DocAPI's job outcome notifications.

    Enable when constructing a worker subclass::

        worker = MyWorker(
            api_url=url, connector=connector, base_dir=data_dir,
            mail_sender=sender,
            identity="OCR worker", health_config=WorkerHealthConfig(),
        )
        worker.start()
    """
    mail_startup: bool = True
    alive_interval_seconds: float = 86400

    def __post_init__(self):
        if not math.isfinite(self.alive_interval_seconds) or self.alive_interval_seconds < 0:
            raise ValueError("alive_interval_seconds must be a finite, nonnegative number")


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
                 identity: Optional[str] = None,
                 hostname: Optional[str] = None,
                 mail_subject: str = "[{identity}] {status} ({hostname})",
                 health_config: Optional[WorkerHealthConfig] = None,
                 error_messages: Optional[Mapping[str, WorkerErrorMessage]] = None):
        """
        Initialize the DocWorker.
        
        Args:
            api_url: The base URL of the API
            connector: The connector instance for API communication
            
            base_dir: Base directory - if specified, creates 'jobs' and 'engines' subdirectories
            job_history_path: Optional JSONL history file, one file per worker.
                Records attempts and keeps a sibling .stats.json cache for health mail.
                None disables history. Independent of job-directory cleanup.
            jobs_dir: Directory for job data (overrides base_dir/jobs if specified)
            cleanup_job_dir: If True, removes job directory after successful processing
            engines_dir: Directory for engine files (overrides base_dir/engines if specified)
            cleanup_old_engines: If True, removes old engine versions when downloading new ones

            polling_interval: Time in seconds to wait between job requests (default: 5.0)

            prefer_image_url: Prefer a supplied image URL when an uploaded image also exists
            prefer_alto_url: Prefer a supplied ALTO URL when an uploaded ALTO file also exists
            prefer_page_url: Prefer a supplied PAGE URL when an uploaded PAGE file also exists

            mail_sender: Optional mail sender that send_mail() and the wrapper's own
                mail use. Mail is sent as written; pass a MailSender subclass whose
                send() filters subject and body to mask or redact it.
            identity: Worker identity used in all wrapper emails; defaults to the class name.
            hostname: Host label used in all wrapper emails; defaults to socket.gethostname().
            mail_subject: Python format string with identity, hostname, status (ALIVE/FAILURE),
                and time (UTC datetime, supporting date format specifications) placeholders.
            health_config: Opt-in startup and periodic alive mail. Requires mail_sender.
                None disables alive reports. Explicit worker failures are mailed
                whenever mail_sender is configured, independently of health_config.
            error_messages: Overrides keyed by DEFAULT_WORKER_ERRORS names. Each
                WorkerErrorMessage sets technical and user text independently.
                Omitted entries retain their existing English defaults.
        """
        if health_config is not None and mail_sender is None:
            message = "Worker health requires a mail sender."
            logger.warning(message)
            raise ValueError(message)
        self.identity = type(self).__name__ if identity is None else identity
        self.hostname = socket.gethostname() if hostname is None else hostname
        self.mail_subject = mail_subject
        # Reject invalid templates before constructing the adapter or creating directories.
        self.mail_subject.format(identity=self.identity, hostname=self.hostname,
                                 status="ALIVE", time=datetime.now(timezone.utc))
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
        self.adapter = Adapter(api_url, connector)

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
        self.mail_sender = mail_sender
        self.health = WorkerHealth(self, health_config) if health_config is not None else None

    @abstractmethod
    def process_job(self,
                    job: Job,
                    job_log_file_handler: logging.FileHandler,
                    images_dir: str,
                    result_dir: str,
                    alto_dir: Optional[str] = None,
                    page_xml_dir: Optional[str] = None,
                    meta_file: Optional[str] = None,
                    engine_dir: Optional[str] = None) -> WorkerResponse:
        """
        Process the current job data and save results to the specified directory.
        
        This method must be implemented by subclasses to define the actual processing logic.
        
        Args:
            job: The job object containing job metadata
            job_log_file_handler: File handler for logging job processing into job-specific log file
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
        report = {
            WorkerResponseStatus.OK: self.adapter.patch_job_finish,
            WorkerResponseStatus.ERROR: self.adapter.patch_job_error,
            WorkerResponseStatus.FAIL: self.adapter.patch_job_fail,
        }[response.status]
        try:
            result = report(final_log=technical, final_log_user=user, job_id=job_id)
        except Exception as exception:
            logger.exception("Could not report the job outcome to DocAPI")
            self._notify_worker_failure(f"Could not report the outcome of job {job_id} to DocAPI",
                                        exception=exception)
            return False
        if not result.is_success:
            self._notify_worker_failure(f"Could not report the outcome of job {job_id} to DocAPI "
                                        f"(status {result.status}, code {result.code})")
            return False
        return True

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
            
        upload_response = self.adapter.post_result(zip_path)
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
        job_response: Optional[WorkerResponse] = None
        job_response_reported: Optional[bool] = None
        interrupted = False

        try:
            job_dir = self.get_job_data_path()
            os.makedirs(job_dir, exist_ok=True)

            # add file handler to logger for this job
            job_log_path = os.path.join(job_dir, "worker.log")
            job_log_file_handler = logging.FileHandler(job_log_path)
            # inherit formatter from root logger (use first root handler that has a formatter)
            root_logger = logging.getLogger()
            root_formatter = None
            for rh in root_logger.handlers:
                if getattr(rh, "formatter", None):
                    root_formatter = rh.formatter
                    break
            if root_formatter:
                job_log_file_handler.setFormatter(root_formatter)
            else:
                # fallback to a reasonable default formatter if root has none
                job_log_file_handler.setFormatter(logging.Formatter("%(asctime)s : %(levelname)s : %(name)s : %(message)s"))
            job_log_file_handler.setLevel(logging.DEBUG)
            logger.addHandler(job_log_file_handler)

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
                    job_log_file_handler=job_log_file_handler,
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
            try:
                logger.debug(f"Uploading results for job {self.current_job.id}...")
                upload_response = self._upload_results()
                if upload_response.status != WorkerResponseStatus.OK:
                    job_response = upload_response
                    job_response_reported = self._report_job_response(job_response)
                    return None
            except Exception as e:
                response = self._make_error_response("upload_results_to_server", exception=e)
                job_response = response
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
                # Remove job-specific file handler from logger
                if job_log_file_handler is not None:
                    logger.removeHandler(job_log_file_handler)
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
        and re-raised after attempting a failure email when a mailer is configured. A hard
        kill or host failure requires an external monitor.
        """
        logger.info(f"Worker started, polling for jobs every {self.polling_interval} seconds")
        try:
            if self.health is not None:
                self.health.start()
            while True:
                result = self.run_job_pipeline()
                if self.health is not None:
                    self.health.pipeline_returned()
                if result is None:
                    time.sleep(self.polling_interval)
        except KeyboardInterrupt:
            logger.info("Worker shutting down gracefully")
        except BaseException as exception:
            logger.exception("Unexpected error in worker loop")
            self._notify_worker_failure("Worker terminated unexpectedly.", exception=exception)
            raise
        finally:
            if self.health is not None:
                self.health.close()


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

    def send_mail(self, subject: str, body: str, receiver_mails: Optional[List[str]] = None) -> bool:
        """
        Send an email with the worker's mail sender, logging it with its sender and receivers.

        Args:
            subject: Subject of the email
            body: Plain text body of the email
            receiver_mails: Receivers of the email, the mail sender's receivers by default

        Returns:
            True if the email was sent; False if it was not, also when no mail sender is configured
        """
        if self.mail_sender is None:
            logger.warning(f"Not sending email {subject!r}: the worker has no mail sender")
            return False
        return self.mail_sender.send(subject, body, receiver_mails)

    def _notify_worker_failure(self, message: str, *, exception: Optional[BaseException] = None) -> None:
        """Alert when a failure cannot be communicated through job finalization."""
        if exception is not None:
            message = _join(message, "".join(traceback.format_exception(exception)).rstrip())
        logger.error("%s", message)
        self._send_worker_mail("FAILURE", message)

    def _send_worker_mail(self, status: str, message: str) -> None:
        """Send worker mail with consistent identity, host and subject."""
        if self.mail_sender is None:
            return
        now = datetime.now(timezone.utc)
        try:
            subject = self.mail_subject.format(identity=self.identity, hostname=self.hostname,
                                               status=status, time=now)
            fields = (("worker", self.identity), ("hostname", self.hostname),
                      ("time", now.strftime("%Y-%m-%d %H:%M:%S UTC")), ("status", status))
            header = "\n".join(_row(label, value) for label, value in fields)
            self.send_mail(subject, f"{header}\n\n{message}\n")
        except Exception:
            # Alert delivery must not interrupt processing or cause another alert.
            logger.exception("Could not send worker mail")


class JobHistory:
    """Append-only JSONL attempts and a rebuildable JSON statistics cache.

    Use one history path per worker. The cache holds cumulative totals, unique
    job IDs and the recent records needed for exact rolling windows. Logs,
    exceptions, URLs and engine definitions are excluded from both files.
    """
    _statuses = ("ok", "error", "fail", "interrupted", "crashed")

    def __init__(self, path):
        self.path = Path(path).expanduser()
        self.stats_path = self.path.with_suffix(".stats.json")
        if self.stats_path == self.path:
            self.stats_path = self.path.with_name(self.path.name + ".stats.json")
        self._lock = threading.Lock()

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
            "version": 2, "history_bytes": 0, "as_of": now.isoformat(), "invalid_records": 0,
            "total": {"attempts": 0, "job_ids": {}, "outcomes": dict.fromkeys(self._statuses, 0),
                      "duration_seconds": 0.0, "report_failures": 0},
            "recent": [],
        }

    def _validate_stats(self, data, size, now, rebuild_on_clock_change):
        if data["version"] != 2 or type(data["history_bytes"]) is not int or not 0 <= data["history_bytes"] <= size:
            raise ValueError("Invalid worker statistics cache")
        as_of = datetime.fromisoformat(data["as_of"])
        if as_of.tzinfo is None or (rebuild_on_clock_change and now < as_of):
            # If the clock moved backwards, rebuild the older rolling window.
            raise ValueError("Worker statistics cache needs rebuilding")
        total = data["total"]
        if not isinstance(total["job_ids"], dict) or not isinstance(data["recent"], list):
            raise ValueError("Invalid worker statistics cache")
        counts = [total["attempts"], total["report_failures"], data["invalid_records"],
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
                    if line.strip():
                        try:
                            entry = json.loads(line)
                            self._validate_entry(entry)
                        except (ValueError, TypeError, KeyError, UnicodeError):
                            data["invalid_records"] += 1
                            logger.warning("Ignoring malformed worker history record in %s", self.path)
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
            self._accumulate(data, entry, finished_at - timedelta(days=7))
            # History is authoritative. If this replacement fails, the next
            # operation consumes the missing tail rather than double-counting.
            self._save_stats(data)

    def statistics(self, now=None):
        """Read the cache and tally only recent records for rolling windows."""
        now = datetime.now(timezone.utc) if now is None else now
        with self._lock:
            data = self._load_stats(now)
            if self.path.exists():
                self._save_stats(data)
        total = data["total"]
        result = {"total": {"attempts": total["attempts"], "jobs": len(total["job_ids"]),
                            "outcomes": dict(total["outcomes"]), "duration_seconds": total["duration_seconds"],
                            "report_failures": total["report_failures"]},
                  "invalid_records": data["invalid_records"]}
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


class WorkerHealth:
    def __init__(self, worker: "DocWorkerWrapper", config: WorkerHealthConfig):
        self.worker = worker
        self.mail_startup = config.mail_startup
        self.alive_interval = config.alive_interval_seconds
        self.started = time.monotonic()
        self.last_pipeline_return = None
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        if self.worker.mail_sender is None:
            message = "Worker health requires a mail sender."
            logger.warning(message)
            raise ValueError(message)
        if self.mail_startup:
            self.send_alive()
        if self.alive_interval > 0:
            thread = threading.Thread(target=self._run, name="worker-health", daemon=True)
            thread.start()
            self._thread = thread

    def pipeline_returned(self):
        with self._lock:
            self.last_pipeline_return = time.monotonic()

    def send_alive(self):
        now = time.monotonic()
        with self._lock:
            last_return = self.last_pipeline_return
        job = self.worker.current_job
        lines = [
            _row("uptime", _duration(now - self.started)),
            _row("last job", "none yet" if last_return is None else f"{_duration(now - last_return)} ago"),
            _row("active job", str(job.id) if job is not None else "none active"),
        ]
        history = self.worker.job_history
        if history is not None:
            try:
                stats = history.statistics()
                if stats.get("invalid_records", 0):
                    lines += ["", "history", _row("unreadable", str(stats["invalid_records"]) + " records; inspect worker log", " ")]
                for name, label in (("last_24_hours", "last 24 hours"), ("last_7_days", "last 7 days"),
                                    ("total", "all time")):
                    window = stats[name]
                    counts = window["outcomes"]
                    lines += ["", label,
                              _row("attempts", window["attempts"], " "),
                              _row("unique jobs", window["jobs"], " ")]
                    for outcome, outcome_label in (("ok", "ok"), ("error", "error"), ("fail", "failed"),
                                                   ("interrupted", "interrupted"), ("crashed", "crashed")):
                        lines.append(_row(outcome_label, counts[outcome], " "))
                    lines.append(_row("report fail", window["report_failures"], " "))
                    lines.append(_row("runtime", _duration(window["duration_seconds"]), " "))
            except Exception:
                logger.exception("Could not read worker job history statistics")
                lines += ["", "history", _row("statistics", "unavailable; inspect worker log", " ")]
        message = "\n".join(lines) + "\n"
        # This reports process liveness, not proof that inference is progressing.
        self.worker._send_worker_mail("ALIVE", message)

    def _run(self):
        while not self._stop.wait(self.alive_interval):
            self.send_alive()

    def close(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1)


def _row(label: str, value: str, indent: str = "") -> str:
    return textwrap.fill(str(value), width=60,
                         initial_indent=f"{indent}{label:<12}",
                         subsequent_indent=indent + " " * 12,
                         break_long_words=False, break_on_hyphens=False)


def _duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours or days:
        parts.append(f"{hours}h")
    if minutes or hours or days:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    return " ".join(parts)
