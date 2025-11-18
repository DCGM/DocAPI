import logging
import os
import time
import zipfile
from abc import ABC
from pathlib import Path
from typing import Optional

from natsort import natsorted

from doc_api.adapter import Adapter
from doc_api.api.schemas.base_objects import Job
from doc_api.connector import Connector


logger = logging.getLogger(__name__)


class DocClientWrapper(ABC):
    """
    A client wrapper that handles job creation, data upload, and result retrieval.
    
    This class implements the core client functionality for creating jobs,
    uploading required data, polling for results, and downloading them.
    """
    
    def __init__(self, 
                 api_url: str, 
                 connector: Connector,
                 polling_interval: float = 5.0):
        """
        Initialize the DocClient.
        
        Args:
            api_url: The base URL of the API
            connector: The connector instance for API communication
            polling_interval: Time in seconds to wait between result checks (default: 5.0)
        """
        self.adapter = Adapter(api_url, connector)
        self.polling_interval = polling_interval
        self.current_job: Optional[Job] = None
    
    def process_result(self, job: Job, results_dir: str) -> None:
        """
        Process the downloaded results.
        
        This method can be overridden by subclasses to implement custom result processing.
        By default, it does nothing.
        
        Args:
            job: The job object containing job metadata
            results_dir: Directory path containing the downloaded and extracted results
        """
        pass
    
    def run_job_pipeline(
            self,
            images_dir: str,
            results_dir: str,
            alto_dir: Optional[str] = None,
            page_xml_dir: Optional[str] = None,
            meta_file: Optional[str] = None,
            engine_name: Optional[str] = None) -> Optional[Job]:
        """
        Execute the complete client pipeline: create job, upload data, wait for results, download and process.
        
        Args:
            images_dir: Directory containing images to process (required)
            results_dir: Directory where results should be saved (required)
            alto_dir: Optional directory containing ALTO XML files
            page_xml_dir: Optional directory containing PAGE XML files
            meta_file: Optional path to meta.json file
            engine_name: Optional name of the engine to use for processing
            
        Returns:
            Job object if successfully processed, None if failed
        """

        if not os.path.isdir(images_dir):
            logger.error(f"Images directory does not exist: {images_dir}")
            return None
        
        os.makedirs(results_dir, exist_ok=True)
        
        # Validate optional directories and files
        if alto_dir and not os.path.isdir(alto_dir):
            logger.error(f"ALTO directory does not exist: {alto_dir}")
            return None
        
        if page_xml_dir and not os.path.isdir(page_xml_dir):
            logger.error(f"PAGE XML directory does not exist: {page_xml_dir}")
            return None
        
        if meta_file and not os.path.isfile(meta_file):
            logger.error(f"Meta file does not exist: {meta_file}")
            return None
        
        try:
            # Get list of image files
            image_files = self._get_image_files(images_dir)
            if not image_files:
                logger.error(f"No image files found in {images_dir}")
                return None
            
            logger.debug(f"Found {len(image_files)} images to process")
            
            # Create the job
            logger.debug("Creating job...")
            job = self._create_job(
                image_files=image_files,
                engine_name=engine_name,
                alto_required=alto_dir is not None,
                page_required=page_xml_dir is not None,
                meta_json_required=meta_file is not None
            )
            if not job:
                logger.error("Failed to create job")
                return None
            
            self.current_job = job
            
            # Log job creation summary
            logger.info("")
            logger.info(f"Job {job.id} created")
            logger.info(f"Created: {job.created_date}")
            logger.info(f"Engine: name={job.engine_name}, version={job.engine_version}")
            logger.info(f"Images: {len(image_files)}")
            logger.info(f"Requirements: ALTO={alto_dir is not None}, "
                       f"PAGE={page_xml_dir is not None}, "
                       f"Meta={meta_file is not None}")
            logger.info("")
            
            # Upload all data
            logger.debug("Uploading job data...")
            if not self._upload_job_data(images_dir, image_files, alto_dir, page_xml_dir, meta_file):
                logger.error("Failed to upload job data")
                return None
            
            logger.debug("Job data uploaded successfully")
            
            # Wait for results
            logger.info("Waiting for job to complete...")
            completed_job = self._wait_for_results()
            if not completed_job:
                logger.error("Job failed or timed out")
                return None
            
            logger.debug(f"Job completed with status: {completed_job.state}")

            self.current_job = completed_job
            
            # Download and extract results
            logger.debug("Downloading results...")
            if not self._download_and_extract_results(results_dir):
                logger.error("Failed to download results")
                return None
            
            logger.debug(f"Results downloaded and extracted to: {results_dir}")
            
            # Process results (can be overridden by subclass)
            logger.debug("Processing results...")
            self.process_result(completed_job, results_dir)
            
            logger.info(f"Job {completed_job.id} completed successfully")


            job = self.current_job
            self.current_job = None

            return job
            
        except KeyboardInterrupt:
            logger.info("Pipeline interrupted by user")
            return None
        except Exception:
            logger.exception("Unexpected error in pipeline")
            return None

    def _report_error(self, error_message: str, response) -> None:
        """
        Report an error by logging technical details and providing a user-friendly message.

        Args:
            error_message: Human-readable error message
            response: AdapterResponse containing API error details
        """
        job_id = self.current_job.id if self.current_job else None
        tech_log = f"{error_message}"
        if job_id:
            tech_log += f" for job {job_id}"
        tech_log += f". Status: {response.status}, Code: {response.code}"
        if response.response:
            tech_log += f", Response: {response.response.text}"
        logger.error(tech_log)
    
    def _get_image_files(self, images_dir: str) -> list[str]:
        """
        Get sorted list of image files from a directory.
        
        Args:
            images_dir: Directory containing images
            
        Returns:
            Naturally sorted list of image filenames
        """
        image_extensions = {'.jpg', '.jpeg', '.png', '.tif', '.tiff', '.bmp'}
        image_files = [
            file for file in os.listdir(images_dir)
            if Path(file).suffix.lower() in image_extensions
        ]
        return natsorted(image_files)
    
    def _create_job(self,
                   image_files: list[str],
                   engine_name: Optional[str] = None,
                   alto_required: bool = False,
                   page_required: bool = False,
                   meta_json_required: bool = False) -> Optional[Job]:
        """
        Create a new job on the API with a proper job definition.
        
        Args:
            image_files: List of image filenames in desired processing order
            engine_name: Optional name of the engine to use
            alto_required: Whether ALTO XML files will be provided
            page_required: Whether PAGE XML files will be provided
            meta_json_required: Whether meta.json file will be provided
            
        Returns:
            Job object if successful, None otherwise
        """
        # Build job definition
        job_definition = {
            "images": [
                {"name": filename, "order": idx}
                for idx, filename in enumerate(image_files)
            ]
        }
        
        if alto_required:
            job_definition["alto_required"] = True
        if page_required:
            job_definition["page_required"] = True
        if meta_json_required:
            job_definition["meta_json_required"] = True
        if engine_name:
            job_definition["engine_name"] = engine_name
        
        response = self.adapter.post_job(job_definition=job_definition, set_if_successful=True)
        if response.is_success and response.data:
            self.current_job = response.data
            return response.data
        else:
            self._report_error("Failed to create job", response)
            return None
    
    def _upload_job_data(self,
                        images_dir: str,
                        image_files: list[str],
                        alto_dir: Optional[str] = None,
                        page_xml_dir: Optional[str] = None,
                        meta_file: Optional[str] = None) -> bool:
        """
        Upload all job data (images, ALTO, PAGE, meta) to the API.
        
        Args:
            job: The job to upload data for
            images_dir: Directory containing images
            image_files: List of image filenames to upload
            alto_dir: Optional directory containing ALTO XML files
            page_xml_dir: Optional directory containing PAGE XML files
            meta_file: Optional path to meta.json file
            
        Returns:
            True if all uploads successful, False otherwise
            
        Raises:
            FileNotFoundError: If a required ALTO or PAGE file is not found
        """
        # Upload each image and associated files
        for image_file in image_files:
            image_path = os.path.join(images_dir, image_file)
            image_name = os.path.splitext(image_file)[0]
            
            # Upload image
            logger.debug(f"Uploading image: {image_file}")
            response = self.adapter.post_image(image_path)
            if not response.is_success:
                self._report_error(f"Failed to upload image {image_file}", response)
                return False
            
            image_id = response.data.id
            
            # Upload ALTO if directory provided
            if alto_dir:
                alto_file = f"{image_name}.xml"
                alto_path = os.path.join(alto_dir, alto_file)
                if os.path.isfile(alto_path):
                    logger.debug(f"Uploading ALTO: {alto_file}")
                    response = self.adapter.post_alto(alto_path, image_id=image_id)
                    if not response.is_success:
                        self._report_error(f"Failed to upload ALTO {alto_file}", response)
                        return False
                else:
                    raise FileNotFoundError(f"Required ALTO file not found: {alto_path}")
            
            # Upload PAGE if directory provided
            if page_xml_dir:
                page_file = f"{image_name}.xml"
                page_path = os.path.join(page_xml_dir, page_file)
                if os.path.isfile(page_path):
                    logger.debug(f"Uploading PAGE: {page_file}")
                    response = self.adapter.post_page(page_path, image_id=image_id)
                    if not response.is_success:
                        self._report_error(f"Failed to upload PAGE {page_file}", response)
                        return False
                else:
                    raise FileNotFoundError(f"Required PAGE file not found: {page_path}")
        
        # Upload meta JSON if provided
        if meta_file:
            logger.debug("Uploading meta JSON")
            response = self.adapter.post_meta_json(meta_file)
            if not response.is_success:
                self._report_error("Failed to upload meta JSON", response)
                return False
        
        return True
    
    def _wait_for_results(self, timeout: Optional[float] = None) -> Optional[Job]:
        """
        Poll the API until job is completed or failed.
        
        Args:
            timeout: Optional timeout in seconds (None for infinite wait)
            
        Returns:
            Completed Job object if successful, None if failed or timed out
        """
        start_time = time.time()
        
        while True:
            # Check timeout
            if timeout and (time.time() - start_time) > timeout:
                logger.error("Timeout waiting for job")
                return None
            
            # Get job status
            response = self.adapter.get_job()
            if not response.is_success or not response.data:
                self._report_error("Failed to get job status", response)
                return None
            
            job = response.data
            
            # Check if job is complete
            if job.state == "done":
                return job
            elif job.state == "failed":
                logger.error("Job failed")
                return None
            elif job.state == "new":
                raise ValueError("Job is in state 'new', waiting for the results does not make sense")
            else:
                # Job still in progress, wait and try again
                logger.info(f"Job status: {job.state}, progress: {job.progress}")
                time.sleep(self.polling_interval)
    
    def _download_and_extract_results(self, results_dir: str) -> bool:
        """
        Download the results ZIP and extract it to the results directory.
        
        Args:
            results_dir: Directory to extract results to
            
        Returns:
            True if successful, False otherwise
        """
        # Download results ZIP
        response = self.adapter.get_result()
        if not response.is_success or not response.data:
            self._report_error("Failed to download results", response)
            return False
        
        # Save ZIP to temporary file
        zip_path = os.path.join(results_dir, "results.zip")
        with open(zip_path, 'wb') as f:
            f.write(response.data)
        
        # Extract ZIP
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(results_dir)
            
            # Remove ZIP file after extraction
            os.remove(zip_path)
            
            return True
        except Exception:
            logger.exception("Failed to extract results ZIP")
            return False