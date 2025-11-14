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
from typing import Optional

import cv2

from doc_api.adapter import Adapter, AdapterResponse
from doc_api.api.schemas.base_objects import Job, JobLease
from doc_api.connector import Connector


logger = logging.getLogger(__name__)


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
                 cleanup_old_engines: bool = False):
        """
        Initialize the DocWorker.
        
        Args:
            api_url: The base URL of the API
            connector: The connector instance for API communication
            base_dir: Base directory - if specified, creates 'jobs' and 'engines' subdirectories
            jobs_dir: Directory for job data (overrides base_dir/jobs if specified)
            engines_dir: Directory for engine files (overrides base_dir/engines if specified)
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
        
        self.cleanup_old_engines = cleanup_old_engines
        self.current_job: Optional[Job] = None
        self.current_lease: Optional[JobLease] = None

    @abstractmethod
    def process_job(self, results_dir: str) -> bool:
        """
        Process the current job data and save results to the specified directory.
        
        This method must be implemented by subclasses to define the actual processing logic.
        
        Args:
            results_dir: Directory path where processing results should be saved
            
        Returns:
            True if processing succeeded, False otherwise
        """
        pass
    
    def request_job(self) -> Optional[AdapterResponse[Job]]:
        """
        Request a job lease from the API.
        
        Returns:
            AdapterResponse[Job] if a job was leased, None if no jobs available
        """
        logger.info("Requesting job lease...")
        
        lease_response = self.adapter.post_job_lease()
        if not lease_response.is_success or not lease_response.data:
            logger.info("No jobs available in queue")
            if not lease_response.is_success:
                logger.warning(f"Job lease request failed. Status: {lease_response.status}, Code: {lease_response.code}")
            return None
            
        self.current_lease = lease_response.data
        logger.info(f"Leased job {lease_response.data.id}, expires at {lease_response.data.lease_expire_at}")
        
        # Get full job details
        job_response = self.adapter.get_job(lease_response.data.id, set_if_successful=True)
        if not job_response.is_success:
            logger.error(f"Failed to get job details for {lease_response.data.id}. Status: {job_response.status}, Code: {job_response.code}")
            return job_response  # Return the failed response
            
        self.current_job = job_response.data
        logger.info(f"Job {job_response.data.id} details retrieved: {len(job_response.data.images)} images, "
                   f"alto_required={job_response.data.alto_required}, page_required={job_response.data.page_required}, "
                   f"meta_json_required={job_response.data.meta_json_required}")
        
        return job_response
    
    def _check_and_download_engine_files(self, job: Job) -> Optional[AdapterResponse[bytes]]:
        """
        Check if engine files are up to date and download if necessary.
        
        Args:
            job: The job object containing engine information
            
        Returns:
            AdapterResponse[bytes] if download was attempted, None if no engine files required or already exist
        """
        if not job.engine_id or not job.engine_files_updated:
            logger.info("No engine files required for this job")
            return None
            
        engine_id = str(job.engine_id)
        engine_timestamp = job.engine_files_updated
        
        # Check if we have current files
        engine_dir = os.path.join(self.engines_dir, f"{engine_timestamp.strftime('%Y-%m-%dT%H:%M:%S')}.{engine_id}")
        
        if os.path.exists(engine_dir):
            logger.info(f"Engine files for {engine_id} are up to date")
            return None
                
        logger.info(f"Downloading engine files for {engine_id}...")
        
        # Clean up old engine versions if flag is enabled
        if self.cleanup_old_engines:
            self._cleanup_engine_versions(engine_id)
        
        # Download engine files
        engine_response = self.adapter.get_engine_files(engine_id)
        if not engine_response.is_success:
            logger.error(f"Failed to download engine files for {engine_id}. Status: {engine_response.status}, Code: {engine_response.code}")
            return engine_response  # Return the failed response
            
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
        
        logger.info(f"Engine files extracted to {engine_dir}")
        return engine_response  # Return the successful response
    
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
                logger.info(f"Removing engine directory: {engine_dir}")
                shutil.rmtree(engine_dir)
    
    def _download_job_data(self, job: Job) -> Optional[AdapterResponse[None]]:
        """
        Download all required job data (images, ALTO, PAGE, meta JSON).
        
        Args:
            job: The job object containing the data requirements
            
        Returns:
            None if all files downloaded successfully,
            AdapterResponse[None] with failure details if any download failed
        """
        try:
            job_dir = os.path.join(self.jobs_dir, f"{job.started_date.isoformat() if job.started_date else 'unknown'}.{job.id}")
            os.makedirs(job_dir, exist_ok=True)
            
            # Create subdirectories for different file types
            images_dir = os.path.join(job_dir, "images")
            os.makedirs(images_dir, exist_ok=True)
            
            if job.alto_required:
                altos_dir = os.path.join(job_dir, "alto")
                os.makedirs(altos_dir, exist_ok=True)
                
            if job.page_required:
                pages_dir = os.path.join(job_dir, "page_xml")
                os.makedirs(pages_dir, exist_ok=True)
            
            # Download images and associated files
            for image in job.images:
                image_id = str(image.id)
                
                # Download image
                logger.info(f"Downloading image {image.name} (ID: {image_id})...")
                image_response = self.adapter.get_image(image_id, job.id)
                if not image_response.is_success:
                    return image_response
                    
                # Save image (adapter returns cv2 image array, we need to save it)
                image_path = os.path.join(images_dir, f"{image.name}")
                cv2.imwrite(image_path, image_response.data)
                logger.info(f"Saved image to {image_path}")
                
                # Download ALTO if required
                if job.alto_required:
                    logger.info(f"Downloading ALTO for {image.name}...")
                    alto_response = self.adapter.get_alto(image_id, job.id)
                    if not alto_response.is_success:
                        return alto_response
                        
                    alto_path = os.path.join(altos_dir, f"{os.path.splitext(image.name)[0]}.xml")
                    with open(alto_path, 'w', encoding='utf-8') as f:
                        f.write(alto_response.data)
                    logger.info(f"Saved ALTO to {alto_path}")
                
                # Download PAGE if required  
                if job.page_required:
                    logger.info(f"Downloading PAGE for {image.name}...")
                    page_response = self.adapter.get_page(image_id, job.id)
                    if not page_response.is_success:
                        return page_response
                        
                    page_path = os.path.join(pages_dir, f"{os.path.splitext(image.name)[0]}.xml")
                    with open(page_path, 'w', encoding='utf-8') as f:
                        f.write(page_response.data)
                    logger.info(f"Saved PAGE to {page_path}")
            
            # Download meta JSON if required
            if job.meta_json_required:
                logger.info("Downloading meta JSON...")
                meta_response = self.adapter.get_meta_json(job.id)
                if not meta_response.is_success:
                    return meta_response
                    
                meta_path = os.path.join(job_dir, "meta.json")
                with open(meta_path, 'w', encoding='utf-8') as f:
                    f.write(meta_response.data)
                logger.info(f"Saved meta JSON to {meta_path}")
            
            logger.info(f"All job data downloaded successfully to {job_dir}")
            return None
            
        except Exception as e:
            logger.error(f"Exception during job data download for job {job.id}: {str(e)}", exc_info=True)
            return AdapterResponse[None](
                data=None,
                status=0,
                message=f"Exception during job data download: {str(e)}"
            )
    
    def _zip_results(self, results_dir: str) -> Optional[str]:
        """
        Create a ZIP file containing all results from the results directory.
        
        Args:
            results_dir: Directory containing the results to zip
            
        Returns:
            Path to the created ZIP file, or None if failed
        """
        job_dir = self.get_job_data_path()
        if not job_dir:
            logger.error("Failed to get job data path")
            return None
            
        zip_path = os.path.join(job_dir, "results.zip")
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for root, dirs, files in os.walk(results_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    # Create relative path within the ZIP
                    arcname = os.path.relpath(file_path, results_dir)
                    zip_file.write(file_path, arcname)
                    
        logger.info(f"Results zipped to {zip_path}")
        return zip_path

    def _upload_results(self, zip_path: str) -> bool:
        """
        Upload the results ZIP file to the API.
        
        Args:
            zip_path: Path to the ZIP file containing results
            
        Returns:
            True if upload succeeded, False otherwise
        """
        if not self.current_job:
            logger.error("No current job to upload results for")
            return False
            
        with open(zip_path, 'rb') as f:
            zip_data = f.read()
            
        upload_response = self.adapter.post_artifacts(self.current_job.id, zip_data)
        if upload_response.is_success:
            logger.info(f"Results uploaded successfully for job {self.current_job.id}")
            return True
        else:
            logger.error(f"Failed to upload results to API. Status: {upload_response.status}, Code: {upload_response.code}")
            return False

    def run_job_pipeline(self) -> Optional[Job]:
        """
        Execute the complete job pipeline: request, download, process, zip, and upload.
        
        Returns:
            Job object if successfully processed and uploaded, None if failed
        """
        # Request job lease
        job_response = self.request_job()
        if not job_response:
            return None
            
        if not job_response.is_success:
            logger.error(f"Failed to request job. Status: {job_response.status}, Code: {job_response.code}")
            return None
            
        job = job_response.data
        if not job:
            logger.error("Job response was successful but no job data received")
            return None
            
        try:
            # Download engine files if needed
            try:
                engine_response = self._check_and_download_engine_files(job)
                if engine_response is not None and not engine_response.is_success:
                    error_msg = "Failed to download or prepare engine files"
                    tech_log = f"{error_msg} for job {job.id}. Status: {engine_response.status}, Code: {engine_response.code}"
                    if engine_response.response:
                        tech_log += f", Response: {engine_response.response.text}"
                    logger.error(tech_log)
                    self.adapter.patch_job_fail(log=tech_log, log_user=error_msg, job_id=job.id)
                    return None
            except Exception as e:
                error_msg = "Failed to download or prepare engine files"
                tech_log = f"{error_msg} for job {job.id}: {str(e)}"
                logger.error(tech_log, exc_info=True)
                self.adapter.patch_job_fail(log=tech_log, log_user=error_msg, job_id=job.id)
                return None
            
            # Download all job data
            try:
                download_response = self._download_job_data(job)
                if download_response is not None:  # Failed download
                    error_msg = "Failed to download job data from server"
                    error_details = f"Status: {download_response.status}, Code: {download_response.code}"
                    if download_response.response:
                        error_details += f", Response: {download_response.response.text}"
                    tech_log = f"{error_msg} for job {job.id}. {error_details}"
                    logger.error(tech_log)
                    self.adapter.patch_job_fail(log=tech_log, log_user=error_msg, job_id=job.id)
                    return None
            except Exception as e:
                error_msg = "Failed to download job data from server"
                tech_log = f"{error_msg} for job {job.id}: {str(e)}"
                logger.error(tech_log, exc_info=True)
                self.adapter.patch_job_fail(log=tech_log, log_user=error_msg, job_id=job.id)
                return None
            
            # Create results directory
            try:
                job_dir = self.get_job_data_path()
                if not job_dir:
                    error_msg = "Unable to create job workspace directory"
                    tech_log = f"Failed to get job data path for job {job.id}"
                    self.adapter.patch_job_fail(log=tech_log, log_user=error_msg, job_id=job.id)
                    return None
                    
                results_dir = os.path.join(job_dir, "results")
                os.makedirs(results_dir, exist_ok=True)
            except Exception as e:
                error_msg = "Unable to create job workspace directory"
                tech_log = f"Failed to create results directory for job {job.id}: {str(e)}"
                logger.error(tech_log, exc_info=True)
                self.adapter.patch_job_fail(log=tech_log, log_user=error_msg, job_id=job.id)
                return None
            
            # Process the job data
            try:
                logger.info(f"Processing job {job.id}...")
                if not self.process_job(results_dir):
                    error_msg = "Processing failed due to an internal error"
                    tech_log = f"Job processing failed for job {job.id}"
                    self.adapter.patch_job_fail(log=tech_log, log_user=error_msg, job_id=job.id)
                    return None
            except Exception as e:
                error_msg = "Processing failed due to an internal error"
                tech_log = f"Job processing failed for job {job.id}: {str(e)}"
                logger.error(tech_log, exc_info=True)
                self.adapter.patch_job_fail(log=tech_log, log_user=error_msg, job_id=job.id)
                return None
            
            # Zip the results
            try:
                logger.info(f"Zipping results for job {job.id}...")
                zip_path = self._zip_results(results_dir)
                if not zip_path:
                    error_msg = "Failed to package processing results"
                    tech_log = f"Failed to zip results for job {job.id}"
                    self.adapter.patch_job_fail(log=tech_log, log_user=error_msg, job_id=job.id)
                    return None
            except Exception as e:
                error_msg = "Failed to package processing results"
                tech_log = f"Failed to zip results for job {job.id}: {str(e)}"
                logger.error(tech_log, exc_info=True)
                self.adapter.patch_job_fail(log=tech_log, log_user=error_msg, job_id=job.id)
                return None
            
            # Upload the results
            try:
                logger.info(f"Uploading results for job {job.id}...")
                if not self._upload_results(zip_path):
                    error_msg = "Failed to upload results to server"
                    tech_log = f"Failed to upload results for job {job.id}"
                    self.adapter.patch_job_fail(log=tech_log, log_user=error_msg, job_id=job.id)
                    return None
            except Exception as e:
                error_msg = "Failed to upload results to server"
                tech_log = f"Failed to upload results for job {job.id}: {str(e)}"
                logger.error(tech_log, exc_info=True)
                self.adapter.patch_job_fail(log=tech_log, log_user=error_msg, job_id=job.id)
                return None
                
            logger.info(f"Job {job.id} fully processed and uploaded successfully")
            return job
            
        except Exception as e:
            error_msg = "An unexpected error occurred during processing"
            tech_log = f"Unexpected error in job pipeline for job {job.id}: {str(e)}"
            logger.error(tech_log, exc_info=True)
            self.adapter.patch_job_fail(log=tech_log, log_user=error_msg, job_id=job.id)
            return None
    
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
    
    def extend_lease(self) -> bool:
        """
        Extend the current job lease.
        
        Returns:
            True if lease extended successfully
        """
        if not self.current_job:
            logger.error("No current job to extend lease for")
            return False
            
        lease_response = self.adapter.patch_job_lease(self.current_job.id)
        if lease_response.is_success:
            self.current_lease = lease_response.data
            logger.info(f"Lease extended until {lease_response.data.lease_expire_at}")
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