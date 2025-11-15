"""
Dummy worker implementation for testing purposes.

This worker simply copies all downloaded data to the results directory
without any actual processing.
"""
import argparse
import logging
import os
import shutil
from typing import Optional

from doc_worker.doc_worker import DocWorker, WorkerResponse
from doc_api.api.schemas.base_objects import Job
from doc_api.connector import Connector


logger = logging.getLogger(__name__)


class DummyWorker(DocWorker):
    """
    A dummy worker that copies all job data to results without processing.
    
    Useful for testing the worker pipeline and API integration.
    """
    
    def process_job(self, 
                    job: Job,
                    images_dir: str,
                    results_dir: str,
                    alto_dir: Optional[str] = None,
                    page_xml_dir: Optional[str] = None,
                    meta_file: Optional[str] = None,
                    engine_dir: Optional[str] = None) -> WorkerResponse:
        """
        Copy all downloaded job data to the results directory.
        
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
        try:
            logger.info(f"Dummy processing job {job.id}: copying all data to results...")
            
            # Copy images
            if os.path.exists(images_dir):
                dest_images = os.path.join(results_dir, "images")
                shutil.copytree(images_dir, dest_images)
                logger.debug(f"Copied images to {dest_images}")
            
            # Copy ALTO files if provided
            if alto_dir and os.path.exists(alto_dir):
                dest_alto = os.path.join(results_dir, "alto")
                shutil.copytree(alto_dir, dest_alto)
                logger.debug(f"Copied ALTO files to {dest_alto}")
            
            # Copy PAGE files if provided
            if page_xml_dir and os.path.exists(page_xml_dir):
                dest_page = os.path.join(results_dir, "page_xml")
                shutil.copytree(page_xml_dir, dest_page)
                logger.debug(f"Copied PAGE files to {dest_page}")
            
            # Copy meta JSON if provided
            if meta_file and os.path.exists(meta_file):
                dest_meta = os.path.join(results_dir, "meta.json")
                shutil.copy2(meta_file, dest_meta)
                logger.debug(f"Copied meta.json to {dest_meta}")

            if engine_dir and os.path.exists(engine_dir):
                dest_engine = os.path.join(results_dir, "engine_files")
                shutil.copytree(engine_dir, dest_engine)
                logger.debug(f"Copied engine files to {dest_engine}")

            self.update_job_progress(
                job.id,
                log="Successfully copied all data to results",
                log_user="Successfully copied all data to results"
            )
            
            self.update_job_progress(
                job.id,
                log="Dummy processing completed successfully",
                log_user="Dummy processing completed successfully"
            )
            
            logger.info("Dummy processing completed successfully")

            return WorkerResponse.ok()
            
        except Exception as e:
            return WorkerResponse.fail("Dummy processing failed", exception=e)


def main():
    """Main entry point for the dummy worker."""
    parser = argparse.ArgumentParser(
        description="Dummy worker that copies job data without processing",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Required arguments
    parser.add_argument(
        "--api-url",
        required=True,
        help="Base URL of the DocAPI server"
    )
    parser.add_argument(
        "--api-key",
        required=True,
        help="API key for authentication"
    )
    
    # Directory arguments
    parser.add_argument(
        "--base-dir",
        help="Base directory for jobs and engines (creates subdirectories)"
    )
    parser.add_argument(
        "--jobs-dir",
        help="Directory for job data (overrides base-dir/jobs)"
    )
    parser.add_argument(
        "--engines-dir",
        help="Directory for engine files (overrides base-dir/engines)"
    )
    
    # Worker configuration
    parser.add_argument(
        "--polling-interval",
        type=float,
        default=5.0,
        help="Time in seconds to wait between job requests"
    )
    parser.add_argument(
        "--cleanup-job-dir",
        action="store_true",
        help="Remove job directory after successful processing"
    )
    parser.add_argument(
        "--cleanup-old-engines",
        action="store_true",
        help="Remove old engine versions when downloading new ones"
    )

    
    # Logging configuration
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Validate directory arguments
    if not args.base_dir and (not args.jobs_dir or not args.engines_dir):
        parser.error("Either --base-dir or both --jobs-dir and --engines-dir must be specified")
    
    # Create connector
    connector = Connector(worker_key=args.api_key)
    
    # Create and start worker
    worker = DummyWorker(
        api_url=args.api_url,
        connector=connector,
        base_dir=args.base_dir,
        jobs_dir=args.jobs_dir,
        engines_dir=args.engines_dir,
        polling_interval=args.polling_interval,
        cleanup_job_dir=args.cleanup_job_dir,
        cleanup_old_engines=args.cleanup_old_engines
    )
    
    logger.info(f"Starting Dummy Worker connecting to {args.api_url}")
    logger.info(f"Base directory: {args.base_dir or 'N/A'}")
    logger.info(f"Jobs directory: {args.jobs_dir or (args.base_dir + '/jobs' if args.base_dir else 'N/A')}")
    logger.info(f"Engines directory: {args.engines_dir or (args.base_dir + '/engines' if args.base_dir else 'N/A')}")
    
    worker.start()


if __name__ == "__main__":
    main()
