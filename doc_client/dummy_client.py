"""
Dummy client implementation for testing purposes.

This client simply sends job data to the API and downloads results
without any special processing.
"""
import argparse
import logging
import os
from typing import Optional

from doc_client.doc_client_wrapper import DocClientWrapper
from doc_api.api.schemas.base_objects import Job
from doc_api.connector import Connector


logger = logging.getLogger(__name__)


class DummyClient(DocClientWrapper):
    """
    A dummy client that sends jobs and downloads results without custom processing.
    
    Useful for testing the client pipeline and API integration.
    """
    
    def process_result(self, job: Job, result_dir: str) -> None:
        """
        Log the completion of result processing.
        
        Args:
            job: The job object containing job metadata
            result_dir: Directory path containing the downloaded and extracted results
        """
        logger.info(f"Results for job {job.id} saved to {result_dir}")


def main():
    """Main entry point for the dummy client."""
    parser = argparse.ArgumentParser(
        description="Dummy client that sends jobs and downloads results",
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
        "--images-dir",
        required=True,
        help="Directory containing images to process"
    )
    parser.add_argument(
        "--result-dir",
        help="Directory where results should be saved. If omitted, the job is "
             "submitted and uploaded but not waited on or downloaded."
    )
    parser.add_argument(
        "--alto-dir",
        help="Optional directory containing ALTO XML files"
    )
    parser.add_argument(
        "--page-xml-dir",
        help="Optional directory containing PAGE XML files"
    )
    parser.add_argument(
        "--meta-file",
        help="Optional path to meta.json file"
    )
    
    # Engine configuration
    parser.add_argument(
        "--engine-name",
        help="Optional name of the engine to use for processing"
    )
    
    # Client configuration
    parser.add_argument(
        "--polling-interval",
        type=float,
        default=5.0,
        help="Time in seconds to wait between result checks"
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
    
    # Create connector
    connector = Connector(api_key=args.api_key)
    
    # Create client
    client = DummyClient(
        api_url=args.api_url,
        connector=connector,
        polling_interval=args.polling_interval
    )
    
    logger.info(f"Starting Dummy Client connecting to {args.api_url}")
    logger.info(f"Images directory: {args.images_dir}")
    if args.alto_dir:
        logger.info(f"ALTO directory: {args.alto_dir}")
    if args.page_xml_dir:
        logger.info(f"PAGE XML directory: {args.page_xml_dir}")
    if args.meta_file:
        logger.info(f"Meta file: {args.meta_file}")
    if args.engine_name:
        logger.info(f"Engine: {args.engine_name}")
    if args.result_dir:
        logger.info(f"Results directory: {args.result_dir}")
    else:
        logger.info("No result directory given: job will be submitted without waiting for completion")


    # Run the job pipeline
    job = client.run_job_pipeline(
        images_dir=args.images_dir,
        result_dir=args.result_dir,
        alto_dir=args.alto_dir,
        page_xml_dir=args.page_xml_dir,
        meta_file=args.meta_file,
        engine_name=args.engine_name
    )
    
    if job:
        return 0
    else:
        return 1


if __name__ == "__main__":
    exit(main())
