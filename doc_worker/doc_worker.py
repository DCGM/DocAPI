import logging
import os
import json
import zipfile
import time
import shutil
import glob
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Optional, Union
from dataclasses import dataclass

import cv2

from doc_api.adapter import Adapter, AdapterResponse
from doc_api.api.schemas.base_objects import Job, JobLease
from doc_api.connector import Connector


logger = logging.getLogger(__name__)


@dataclass
class WorkerResponse:
    """
    Response wrapper for worker operations to clearly indicate success or failure.
    
    Attributes:
        success: True if operation succeeded, False otherwise
        error_message: Human-readable error message (empty string if success)
        error_adapter_response: Optional AdapterResponse containing API error details
        exception: Optional Exception that caused the failure
    """
    success: bool
    error_message: str = ""
    error_adapter_response: Optional[AdapterResponse] = None
    exception: Optional[Exception] = None
    
    @classmethod
    def ok(cls) -> 'WorkerResponse':
        """Create a successful response."""
        return cls(success=True)
    
    @classmethod
    def fail(cls, error_message: str, adapter_response: Optional[AdapterResponse] = None, exception: Optional[Exception] = None) -> 'WorkerResponse':
        """Create a failed response."""
        return cls(success=False, error_message=error_message, error_adapter_response=adapter_response, exception=exception)


class DocWorker(ABC):
    """
    A worker wrapper that handles job leasing and downloading all required data.
    
    This class implements the core worker functionality except the actual processing,
    which should be implemented by subclasses.
    """
    
    def __init__(self, 
                 api_url: str, 
                 connector: Connector,
                 base_dir: Optional[str] = None,
                 jobs_dir: Optional[str] = None, 
                 engines_dir: Optional[str] = None,
                 polling_interval: float = 5.0,
                 cleanup_job_dir: bool = False,
                 cleanup_old_engines: bool = False):
        """
        Initialize the DocWorker.
        
        Args:
            api_url: The base URL of the API
            connector: The connector instance for API communication
            
            base_dir: Base directory - if specified, creates 'jobs' and 'engines' subdirectories
            jobs_dir: Directory for job data (overrides base_dir/jobs if specified)
            engines_dir: Directory for engine files (overrides base_dir/engines if specified)

            polling_interval: Time in seconds to wait between job requests (default: 5.0)
            
            cleanup_job_dir: If True, removes job directory after successful processing
            cleanup_old_engines: If True, removes old engine versions when downloading new ones
        """
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

    @abstractmethod
    def process_job(self, 
                    job: Job,
                    images_dir: str,
                    results_dir: str,
                    alto_dir: Optional[str] = None,
                    page_xml_dir: Optional[str] = None,
                    meta_file: Optional[str] = None,
                    engine_dir: Optional[str] = None) -> WorkerResponse:
        """
        Process the current job data and save results to the specified directory.
        
        This method must be implemented by subclasses to define the actual processing logic.
        
        Args:
            job: The job object containing job metadata
            images_dir: Directory path containing the downloaded images
            results_dir: Directory path where processing results should be saved
            alto_dir: Optional directory path containing ALTO XML files
            page_xml_dir: Optional directory path containing PAGE XML files
            meta_file: Optional path to the meta.json file
            engine_dir: Optional directory path containing engine files
            
        Returns:
            WorkerResponse indicating success or failure
        """
        pass
    
    def _report_error(self, response: WorkerResponse, job_id: Optional[str] = None) -> None:
        """
        Report an error by logging technical details and notifying the API with user-friendly message.
        
        Args:
            response: WorkerResponse containing error details and user message
            job_id: Optional job ID to report the error for (uses current_job.id if not provided)
        """
        job_id = job_id or (self.current_job.id if self.current_job else None)
        
        user_msg = response.error_message
        
        if response.error_adapter_response:
            # Build technical log from AdapterResponse, starting with user message
            tech_log = f"{user_msg} for job {job_id}. Status: {response.error_adapter_response.status}, Code: {response.error_adapter_response.code}"
            if response.error_adapter_response.response:
                tech_log += f", Response: {response.error_adapter_response.response.text}"
        else:
            # Tech log is same as user message when there's no adapter response
            tech_log = f"{user_msg} for job {job_id}"
        
        # Add exception details if present
        if response.exception:
            tech_log += f". Exception: {type(response.exception).__name__}: {str(response.exception)}"
        
        logger.error(tech_log)
        
        # Report failure to API
        if job_id:
            self.adapter.patch_job_fail(log=tech_log, log_user=user_msg, job_id=job_id)
    
    def request_job(self) -> Optional[WorkerResponse]:
        """
        Request a job lease from the API.
        
        Returns:
            WorkerResponse indicating success or failure, None if no jobs available
        """
        logger.debug("Requesting job lease...")
        
        lease_response = self.adapter.post_job_lease()
        if not lease_response.is_success or not lease_response.data:
            if not lease_response.is_success:
                logger.warning(f"Job lease request failed. Status: {lease_response.status}, Code: {lease_response.code}")
                return WorkerResponse.fail("Failed to request job lease", lease_response)
            else:
                logger.debug("No jobs available in queue")
                return None
            
        self.current_lease = lease_response.data
        logger.debug(f"Leased job {lease_response.data.id}, expires at {lease_response.data.lease_expire_at}")
        
        # Get full job details
        job_response = self.adapter.get_job(lease_response.data.id, set_job_if_successful=True)
        if not job_response.is_success:
            return WorkerResponse.fail("Failed to get job details", job_response)
            
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
            return WorkerResponse.fail("No current job set")
            
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
        
        # Download engine files
        engine_response = self.adapter.get_engine_files(engine_id)
        if not engine_response.is_success:
            return WorkerResponse.fail("Failed to download engine files", engine_response)
            
        # Create engine directory
        os.makedirs(engine_dir, exist_ok=True)
        
        # Save and extract ZIP file
        zip_path = os.path.join(engine_dir, "engine.zip")
        with open(zip_path, 'wb') as f:
            f.write(engine_response.data)
            
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
            return WorkerResponse.fail("No current job set")
            
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
        
        # Download images and associated files
        for image in self.current_job.images:
            image_id = str(image.id)
            
            # Download image
            logger.debug(f"Downloading image {image.name} (ID: {image_id})...")
            image_response = self.adapter.get_image(image_id, self.current_job.id)
            if not image_response.is_success:
                return WorkerResponse.fail(f"Failed to download image {image.name}", image_response)
                
            # Save image (adapter returns cv2 image array, we need to save it)
            image_path = os.path.join(images_dir, f"{image.name}")
            cv2.imwrite(image_path, image_response.data)
            logger.debug(f"Saved image to {image_path}")
            
            # Download ALTO if required
            if self.current_job.alto_required:
                logger.debug(f"Downloading ALTO for {image.name}...")
                alto_response = self.adapter.get_alto(image_id, self.current_job.id)
                if not alto_response.is_success:
                    return WorkerResponse.fail(f"Failed to download ALTO for {image.name}", alto_response)
                    
                alto_path = os.path.join(altos_dir, f"{os.path.splitext(image.name)[0]}.xml")
                with open(alto_path, 'w', encoding='utf-8') as f:
                    f.write(alto_response.data)
                logger.debug(f"Saved ALTO to {alto_path}")
            
            # Download PAGE if required  
            if self.current_job.page_required:
                logger.debug(f"Downloading PAGE for {image.name}...")
                page_response = self.adapter.get_page(image_id, self.current_job.id)
                if not page_response.is_success:
                    return WorkerResponse.fail(f"Failed to download PAGE for {image.name}", page_response)
                    
                page_path = os.path.join(pages_dir, f"{os.path.splitext(image.name)[0]}.xml")
                with open(page_path, 'w', encoding='utf-8') as f:
                    f.write(page_response.data)
                logger.debug(f"Saved PAGE to {page_path}")
        
        # Download meta JSON if required
        if self.current_job.meta_json_required:
            logger.debug("Downloading meta JSON...")
            meta_response = self.adapter.get_meta_json(self.current_job.id)
            if not meta_response.is_success:
                return WorkerResponse.fail("Failed to download meta JSON", meta_response)
                
            meta_path = os.path.join(job_dir, "meta.json")
            with open(meta_path, 'w', encoding='utf-8') as f:
                f.write(meta_response.data)
            logger.debug(f"Saved meta JSON to {meta_path}")
        
        logger.debug(f"All job data downloaded successfully to {job_dir}")
        return WorkerResponse.ok()
            
    
    def _zip_results(self, results_dir: str) -> WorkerResponse:
        """
        Create a ZIP file containing all results from the results directory.
        
        Args:
            results_dir: Directory containing the results to zip
            
        Returns:
            WorkerResponse indicating success or failure
        """
        job_dir = self.get_job_data_path()
        if not job_dir:
            return WorkerResponse.fail("Failed to get job data path")
            
        zip_path = os.path.join(job_dir, "results.zip")
        
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
        Upload the results ZIP file to the API.
        
        Returns:
            WorkerResponse indicating success or failure
        """
        if not self.current_job:
            return WorkerResponse.fail("No current job to upload results for")
            
        job_dir = self.get_job_data_path()
        if not job_dir:
            return WorkerResponse.fail("Failed to get job data path")
            
        zip_path = os.path.join(job_dir, "results.zip")
        
        with open(zip_path, 'rb') as f:
            zip_data = f.read()
            
        upload_response = self.adapter.post_artifacts(self.current_job.id, zip_data)
        if upload_response.is_success:
            logger.debug(f"Results uploaded successfully for job {self.current_job.id}")
            return WorkerResponse.ok()
        else:
            return WorkerResponse.fail("Failed to upload results", upload_response)


    def run_job_pipeline(self) -> Optional[Job]:
        """
        Execute the complete job pipeline: request, download, process, zip, and upload.
        This should be called repeatedly in the life cycle of the worker.
        
        Returns:
            Job object if successfully processed and uploaded, None if failed or no jobs available
        """
        # Request job lease
        try:
            job_response = self.request_job()
            if job_response is None:
                # No jobs available
                logger.info("No jobs available in queue")
                return None
            if not job_response.success:
                self._report_error(job_response)
                return None
        except Exception as e:
            response = WorkerResponse.fail("Failed to request job", exception=e)
            self._report_error(response)
            return None
            
        if not self.current_job:
            logger.error("Job request succeeded but no current job set")
            return None
        
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
                        
        try:
            # Download engine files if needed
            try:
                logger.debug(f"Checking engine files for job {self.current_job.id}...")
                engine_response = self._check_and_download_engine_files()
                if not engine_response.success:
                    self._report_error(engine_response)
                    return None
            except Exception as e:
                response = WorkerResponse.fail("Failed to download or prepare engine files", exception=e)
                self._report_error(response)
                return None
            
            # Download all job data
            try:
                logger.debug(f"Downloading job data for job {self.current_job.id}...")
                download_response = self._download_job_data()
                if not download_response.success:
                    self._report_error(download_response)
                    return None
            except Exception as e:
                response = WorkerResponse.fail("Failed to download job data from server", exception=e)
                self._report_error(response)
                return None
            
            # Create results directory
            try:
                job_dir = self.get_job_data_path()
                if not job_dir:
                    response = WorkerResponse.fail("Unable to create job workspace directory")
                    self._report_error(response)
                    return None
                    
                results_dir = os.path.join(job_dir, "results")
                os.makedirs(results_dir, exist_ok=True)
            except Exception as e:
                response = WorkerResponse.fail("Unable to create job workspace directory", exception=e)
                self._report_error(response)
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
                    results_dir=results_dir,
                    alto_dir=alto_dir,
                    page_xml_dir=page_xml_dir,
                    meta_file=meta_file,
                    engine_dir=engine_dir
                )
                if not process_response.success:
                    self._report_error(process_response)
                    return None

            except Exception as e:
                response = WorkerResponse.fail("Processing failed due to an internal error", exception=e)
                self._report_error(response)
                return None
            
            # Zip the results
            try:
                logger.debug(f"Zipping results for job {self.current_job.id}...")
                zip_response = self._zip_results(results_dir)
                if not zip_response.success:
                    self._report_error(zip_response)
                    return None
            except Exception as e:
                response = WorkerResponse.fail("Failed to package processing results", exception=e)
                self._report_error(response)
                return None
            
            # Upload the results
            try:
                logger.debug(f"Uploading results for job {self.current_job.id}...")
                upload_response = self._upload_results()
                if not upload_response.success:
                    self._report_error(upload_response)
                    return None
            except Exception as e:
                response = WorkerResponse.fail("Failed to upload results to server", exception=e)
                self._report_error(response)
                return None
            
            if self.cleanup_job_dir:
                try:
                    job_dir_to_remove = self.get_job_data_path()
                    if job_dir_to_remove and os.path.exists(job_dir_to_remove):
                        shutil.rmtree(job_dir_to_remove)
                        logger.debug(f"Cleaned up job directory: {job_dir_to_remove}")
                except Exception as e:
                    logger.warning(f"Failed to clean up job directory: {e}")
                
            logger.info(f"Job {self.current_job.id} processed successfully")
            return self.current_job
            
        except KeyboardInterrupt:
            # Handle Ctrl+C gracefully by releasing the lease instead of failing the job
            logger.info("Pipeline interrupted by user, releasing job lease...")
            self.release_lease()
            raise
        except Exception as e:
            response = WorkerResponse.fail("An unexpected error occurred during processing", exception=e)
            self._report_error(response)
            return None
    
    def start(self) -> None:
        """
        Start the worker loop that continuously polls for and processes jobs.
        
        This method runs indefinitely until interrupted (e.g., by Ctrl+C).
        It will:
        - Poll for jobs at the configured polling_interval
        - Process any available jobs
        - Handle interruptions gracefully
        """
        logger.info(f"Worker started, polling for jobs every {self.polling_interval} seconds")
        
        try:
            while True:
                # Try to get and process a job
                result = self.run_job_pipeline()
                
                # If no job was available, wait before trying again
                if result is None:
                    time.sleep(self.polling_interval)
                # If a job was processed, immediately check for the next one
                    
        except KeyboardInterrupt:
            logger.info("Worker shutting down gracefully")
        except Exception as e:
            logger.exception(f"Unexpected error in worker loop: {e}")
    

    def get_job_data_path(self) -> Optional[str]:
        """
        Get the path to the current job's data directory.
        
        Returns:
            Path to job data directory, or None if no current job
        """
        if not self.current_job:
            return None
        
        started_timestamp = self.current_job.started_date.isoformat() if self.current_job.started_date else 'unknown'
        return os.path.join(self.jobs_dir, f"{started_timestamp}.{self.current_job.id}")
    
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
        Get the path to the current job's results directory.
        
        Returns:
            Path to results directory, or None if no current job
        """
        job_dir = self.get_job_data_path()
        if not job_dir:
            return None
        return os.path.join(job_dir, "results")
    
    def update_job_progress(self, progress: Optional[float] = None, log: Optional[str] = None, log_user: Optional[str] = None) -> bool:
        """
        Extend the current job lease, optionally with progress update and logs.
        
        Args:
            progress: Optional progress value (0.0 to 1.0) to update job progress
            log: Optional technical log message
            log_user: Optional user-friendly log message
            
        Returns:
            True if lease extended successfully
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