import logging
import os
import json
from http import HTTPStatus
from urllib.parse import urljoin

import cv2
import numpy as np

from doc_api.api.schemas.base_objects import JobWithImages, JobLease, ProcessingState
from doc_api.api.schemas.responses import AppCode, DocAPIResponseOK

from doc_api.connector import Connector

logger = logging.getLogger(__name__)


class Adapter:
    def __init__(self, api_url, connector: Connector, job: JobWithImages | None = None):
        self.api_url = api_url
        self.connector = connector
        self.job = job

    def get_job_id(self, job_id=None):
        if job_id is not None:
            return str(job_id)
        elif self.job is not None:
            return str(self.job.id)
        else:
            raise ValueError("Job ID must be provided either directly or via the Adapter's job attribute.")

    def compose_url(self, *args):
        args = [str(arg).strip("/") for arg in args]
        route = os.path.join(*args)
        return urljoin(self.api_url, route)

    def get_me(self, route="/v1/me"):
        url = self.compose_url(route)
        response = self.connector.get(url)

        result = None
        if response.status_code == HTTPStatus.OK:
            result = response.json()
            logger.debug("User info successfully obtained.")
        else:
            logger.warning(f"Response: {response.status_code} {response.text}")

        return result

    def get_job(self, job_id=None, set_if_successful=False, route="/v1/jobs/") -> JobWithImages | None:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route, job_id)
        response = self.connector.get(url)

        result = None
        if response.status_code == HTTPStatus.OK:
            response_model = DocAPIResponseOK.model_validate(response.json())
            result = JobWithImages.model_validate(response_model.data)

            if set_if_successful:
                self.job = result
        else:
            logger.warning(f"Response: {response.status_code} {response.text}")

        return result

    def get_image(self, name, job_id=None, route="/v1/jobs/{job_id}/images/{name}/files/image") -> bytes | None:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id, name=name))
        response = self.connector.get(url)

        result = None
        if response.status_code == HTTPStatus.OK:
            result = cv2.imdecode(np.asarray(bytearray(response.content), dtype="uint8"), cv2.IMREAD_COLOR)
            logger.info(f"Image '{name}' for job '{job_id}' successfully downloaded.")
        else:
            logger.error(f"Downloading image '{name}' failed. Response: {response.status_code} {response.text}")

        return result

    def get_alto(self, name, job_id=None, route="/v1/jobs/{job_id}/images/{name}/files/alto") -> str | None:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id, name=name))
        response = self.connector.get(url)

        result = None
        if response.status_code == HTTPStatus.OK:
            result = response.content.decode()
            logger.info(f"ALTO '{name}' for job '{job_id}' successfully downloaded.")
        else:
            logger.error(f"Downloading ALTO '{name}' failed. Response: {response.status_code} {response.text}")

        return result

    def get_meta_json(self, job_id=None, route="/v1/jobs/{job_id}/files/metadata") -> str | None:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id))
        response = self.connector.get(url)

        result = None
        if response.status_code == HTTPStatus.OK:
            result = response.content.decode()
            logger.info(f"Meta JSON for job '{job_id}' successfully downloaded.")
        else:
            logger.error(f"Downloading Meta JSON for job '{job_id}' failed. Response: {response.status_code} {response.text}")

        return result

    def get_result(self, job_id=None, route="/v1/jobs/{job_id}/result") -> bytes | None:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id))
        response = self.connector.get(url)

        result = None
        if response.status_code == HTTPStatus.OK:
            result = response.content
            logger.info(f"Result for job '{job_id}' successfully downloaded.")
        else:
            logger.error(f"Downloading result for job '{job_id}' failed. Response: {response.status_code} {response.text}")

        return result

    def post_job(self, data, set_if_successful=False, route="/v1/jobs") -> JobWithImages | None:
        url = self.compose_url(route)
        response = self.connector.post(url, json=data)

        result = None
        if response.status_code == HTTPStatus.CREATED:
            response_model = DocAPIResponseOK.model_validate(response.json())
            result = JobWithImages.model_validate(response_model.data)
            logger.info(f"Job '{result.id}' successfully created.")

            if set_if_successful:
                self.job = result
        else:
            logger.error(f"Creating job failed. Response: {response.status_code} {response.text}")

        return result

    def post_job_lease(self, route="/v1/jobs/lease") -> JobLease | None:
        url = self.compose_url(route)
        response = self.connector.post(url)

        result = None
        if response.status_code == HTTPStatus.OK:
            response_model = DocAPIResponseOK.model_validate(response.json())

            if response_model.code == AppCode.JOB_LEASED:
                result = JobLease.model_validate(response_model.data)
                logger.debug("Job successfully obtained.")
            else:
                logger.debug(f"No job found.")
        else:
            logger.warning(f"Response: {response.status_code} {response.text}")

        return result

    def post_result(self, result_path, job_id=None, route="/v1/jobs/{job_id}/result") -> bool:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id))

        with open(result_path, 'rb') as file:
            result_bytes = file.read()

        response = self.connector.post(url, files={"file": ("result.zip", result_bytes, "application/zip")})

        if response.status_code == HTTPStatus.CREATED:
            logger.info(f"Result for job '{job_id}' successfully uploaded.")
            return True
        else:
            logger.error(f"Uploading result for job '{job_id}' failed. Response: {response.status_code} {response.text}")
            return False

    def patch_job_finish(self, job_id=None, route="/v1/jobs") -> bool:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route, job_id)
        response = self.connector.patch(url, json={"state": ProcessingState.DONE.value})

        if response.status_code == HTTPStatus.OK:
            logger.info(f"Job '{job_id}' successfully marked as finished.")
            return True
        else:
            logger.error(f"Marking job '{job_id}' as finished failed. Response: {response.status_code} {response.text}")
            return False

    def patch_job_cancel(self, job_id=None, route="/v1/jobs") -> bool:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route, job_id)
        response = self.connector.patch(url, json={"state": ProcessingState.CANCELLED.value})

        if response.status_code == HTTPStatus.OK:
            logger.info(f"Job '{job_id}' successfully cancelled.")
            return True
        else:
            logger.error(f"Cancelling job '{job_id}' failed. Response: {response.status_code} {response.text}")
            return False

    def put_job_progess_update(self, progress: float, job_id=None, route="/v1/jobs") -> bool:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route, job_id)
        response = self.connector.put(url, json={"progress": progress})

        if response.status_code == HTTPStatus.OK:
            logger.info(f"Job '{job_id}' successfully updated.")
            return True
        else:
            logger.error(f"Updating job '{job_id}' failed. Response: {response.status_code} {response.text}")
            return False

    def put_file(self, file_path, file_type: str, job_id=None, route="/v1/jobs/{job_id}/images/{name}/files/{file_type}"):
        job_id = self.get_job_id(job_id)
        name = os.path.splitext(os.path.basename(file_path))[0]

        url = self.compose_url(route.format(job_id=job_id, name=name, file_type=file_type))

        with open(file_path, 'rb') as file:
            file_bytes = file.read()

        response = self.connector.put(url, files={"file": file_bytes})

        if response.status_code == HTTPStatus.CREATED:
            logger.info(f"File '{file_type}' for job '{job_id}' successfully uploaded.")
            return True
        else:
            logger.error(f"Uploading file '{file_type}' for job '{job_id}' failed. Response: {response.status_code} {response.text}")
            return False

    def put_meta_json(self, json_path, job_id=None, route="/v1/jobs/{job_id}/files/metadata"):
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id))

        with open(json_path, 'r') as file:
            data = json.load(file)

        response = self.connector.put(url, json=data)

        if response.status_code == HTTPStatus.CREATED:
            logger.info(f"Meta JSON for job '{job_id}' successfully uploaded.")
            return True
        else:
            logger.error(f"Uploading Meta JSON for job '{job_id}' failed. Response: {response.status_code} {response.text}")
            return False
