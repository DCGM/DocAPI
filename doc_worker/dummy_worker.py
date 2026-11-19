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

from doc_worker.config import Config
from doc_worker.doc_worker_wrapper import DocWorkerWrapper, WorkerResponse
from doc_api.api.schemas.base_objects import Job


logger = logging.getLogger(__name__)


class DummyWorker(DocWorkerWrapper):
    """
    A dummy worker that copies all job data to results without processing.
    
    Useful for testing the worker pipeline and API integration.
    """
    
    def process_job(self,
                    job: Job,
                    images_dir: str,
                    result_dir: str,
                    alto_dir: Optional[str] = None,
                    page_xml_dir: Optional[str] = None,
                    meta_file: Optional[str] = None,
                    engine_dir: Optional[str] = None) -> WorkerResponse:
        """
        Copy all downloaded job data to the results directory.
        
        Args:
            job: The job object containing job metadata
            images_dir: Directory path containing the downloaded images
            result_dir: Directory path where processing results should be saved
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
                dest_images = os.path.join(result_dir, "images")
                shutil.copytree(images_dir, dest_images)
                logger.debug(f"Copied images to {dest_images}")
            
            # Copy ALTO files if provided
            if alto_dir and os.path.exists(alto_dir):
                dest_alto = os.path.join(result_dir, "alto")
                shutil.copytree(alto_dir, dest_alto)
                logger.debug(f"Copied ALTO files to {dest_alto}")
            
            # Copy PAGE files if provided
            if page_xml_dir and os.path.exists(page_xml_dir):
                dest_page = os.path.join(result_dir, "page_xml")
                shutil.copytree(page_xml_dir, dest_page)
                logger.debug(f"Copied PAGE files to {dest_page}")
            
            # Copy meta JSON if provided
            if meta_file and os.path.exists(meta_file):
                dest_meta = os.path.join(result_dir, "meta.json")
                shutil.copy2(meta_file, dest_meta)
                logger.debug(f"Copied meta.json to {dest_meta}")

            if engine_dir and os.path.exists(engine_dir):
                dest_engine = os.path.join(result_dir, "engine_files")
                shutil.copytree(engine_dir, dest_engine)
                logger.debug(f"Copied engine files to {dest_engine}")

            self.update_job_progress(
                log="Successfully copied all data to results",
                log_user="Successfully copied all data to results"
            )
            
            self.update_job_progress(
                log="Dummy processing completed successfully",
                log_user="Dummy processing completed successfully"
            )
            
            logger.info("Dummy processing completed successfully")

            return WorkerResponse.ok()
            
        except Exception as e:
            logger.exception("Dummy processing failed")
            return WorkerResponse.error(final_log="Dummy processing failed", final_log_user="Dummy processing failed", exception=e)


def parse_args(argv=None):
    """Command-line arguments (argv, or sys.argv when None); each one given overrides the same config setting."""
    parser = argparse.ArgumentParser(
        description="Dummy worker that copies job data without processing. Settings come from the "
                    "environment (see doc_worker/.env_example); arguments given here override them.",
    )
    parser.add_argument("--worker-identity", help="The worker's name in its emails and User-Agent (WORKER_IDENTITY)")
    parser.add_argument("--api-url", help="Base URL of the DocAPI server (API_URL)")
    parser.add_argument("--api-key", help="API key for authentication (WORKER_KEY); "
                                          "prefer WORKER_KEY_FILE, which keeps the key off the command line")
    parser.add_argument("--base-dir", help="Base directory for jobs and engines (BASE_DIR)")
    parser.add_argument("--jobs-dir", help="Directory for job data, overrides base-dir/jobs (JOBS_DIR)")
    parser.add_argument("--engines-dir", help="Directory for engine files, overrides base-dir/engines (ENGINES_DIR)")
    parser.add_argument("--polling-interval", type=float,
                        help="Seconds to wait between job requests (POLLING_INTERVAL)")
    parser.add_argument("--connect-timeout", type=float,
                        help="Seconds to wait for a connection to DocAPI (API_CONNECT_TIMEOUT_SECONDS)")
    parser.add_argument("--read-timeout", type=float,
                        help="Seconds to wait for the next bytes of a DocAPI response (API_READ_TIMEOUT_SECONDS)")
    parser.add_argument("--cleanup-job-dir", action=argparse.BooleanOptionalAction,
                        help="Remove the job directory after successful processing (CLEANUP_JOB_DIR)")
    parser.add_argument("--cleanup-old-engines", action=argparse.BooleanOptionalAction,
                        help="Remove old engine versions when downloading new ones (CLEANUP_OLD_ENGINES)")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO",
                        help="Logging level (default: INFO)")
    return parser.parse_args(argv)


def build_config(args) -> Config:
    """The config from the environment, with the arguments that were given as overrides."""
    def text(value):
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    overrides = {
        "WORKER_IDENTITY": args.worker_identity, "API_URL": args.api_url,
        "BASE_DIR": args.base_dir, "JOBS_DIR": args.jobs_dir,
        "ENGINES_DIR": args.engines_dir, "POLLING_INTERVAL": args.polling_interval,
        "API_CONNECT_TIMEOUT_SECONDS": args.connect_timeout, "API_READ_TIMEOUT_SECONDS": args.read_timeout,
        "CLEANUP_JOB_DIR": args.cleanup_job_dir, "CLEANUP_OLD_ENGINES": args.cleanup_old_engines,
    }
    if args.api_key:
        # A key given here replaces a key file from the environment as well.
        overrides.update(WORKER_KEY=args.api_key, WORKER_KEY_FILE="")
    # argparse leaves an argument that was not given as None; only given ones override.
    return Config({name: text(value) for name, value in overrides.items() if value is not None})


def main():
    """Main entry point for the dummy worker."""
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    config = build_config(args)
    worker = DummyWorker.from_config(config)

    logger.info(f"Starting Dummy Worker connecting to {config.API_URL}")
    logger.info(f"Jobs directory: {worker.jobs_dir}")
    logger.info(f"Engines directory: {worker.engines_dir}")
    mail = worker.worker_mail_sender
    logger.info(f"Worker mail: {'on' if mail is not None and mail.receivers else 'off'}")

    worker.start()


if __name__ == "__main__":
    main()
