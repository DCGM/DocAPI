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

    def get_image(self, image_id, job_id=None, route="/v1/jobs/{job_id}/images/{image_id}/files/image") -> bytes | None:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id, image_id=image_id))
        response = self.connector.get(url)

        result = None
        if response.status_code == HTTPStatus.OK:
            result = cv2.imdecode(np.asarray(bytearray(response.content), dtype="uint8"), cv2.IMREAD_COLOR)
            logger.info(f"Image '{image_id}' for job '{job_id}' successfully downloaded.")
        else:
            logger.error(f"Downloading image '{image_id}' failed. Response: {response.status_code} {response.text}")

        return result

    def get_alto(self, image_id, job_id=None, route="/v1/jobs/{job_id}/images/{image_id}/files/alto") -> str | None:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id, image_id=image_id))
        response = self.connector.get(url)

        result = None
        if response.status_code == HTTPStatus.OK:
            result = response.content.decode()
            logger.info(f"ALTO '{image_id}' for job '{job_id}' successfully downloaded.")
        else:
            logger.error(f"Downloading ALTO '{image_id}' failed. Response: {response.status_code} {response.text}")

        return result

    def get_page(self, image_id, job_id=None, route="/v1/jobs/{job_id}/images/{image_id}/files/page") -> str | None:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id, image_id=image_id))
        response = self.connector.get(url)

        result = None
        if response.status_code == HTTPStatus.OK:
            result = response.content.decode()
            logger.info(f"PAGE '{image_id}' for job '{job_id}' successfully downloaded.")
        else:
            logger.error(f"Downloading PAGE '{image_id}' failed. Response: {response.status_code} {response.text}")

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

    def get_engine_files(self, engine_id, route="/v1/engines/{engine_id}/files") -> bytes | None:
        url = self.compose_url(route.format(engine_id=engine_id))
        response = self.connector.get(url)

        result = None
        if response.status_code == HTTPStatus.OK:
            result = response.content
            logger.info(f"Engine files '{engine_id}' successfully downloaded.")
        else:
            logger.error(f"Downloading engine files '{engine_id}' failed. Response: {response.status_code} {response.text}")

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

    def post_artifacts(self, artifacts_path, job_id=None, route="/v1/jobs/{job_id}/artifacts") -> bool:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id))

        with open(artifacts_path, 'rb') as file:
            artifacts_bytes = file.read()

        response = self.connector.post(url, files={"file": ("artifacts.zip", artifacts_bytes, "application/zip")})

        if response.status_code == HTTPStatus.CREATED:
            logger.info(f"Artifacts for job '{job_id}' successfully uploaded.")
            return True
        else:
            logger.error(f"Uploading artifacts for job '{job_id}' failed. Response: {response.status_code} {response.text}")
            return False

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

    def patch_job_fail(self, log: str = None, log_user: str = None, job_id=None, route="/v1/jobs") -> bool:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route, job_id)
        
        data = {"state": ProcessingState.ERROR.value}
        if log is not None:
            data["log"] = log
        if log_user is not None:
            data["log_user"] = log_user
            
        response = self.connector.patch(url, json=data)

        if response.status_code == HTTPStatus.OK:
            logger.info(f"Job '{job_id}' successfully marked as failed.")
            return True
        else:
            logger.error(f"Marking job '{job_id}' as failed failed. Response: {response.status_code} {response.text}")
            return False

    def patch_job_lease(self, job_id=None, route="/v1/jobs/{job_id}/lease") -> JobLease | None:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id))
        response = self.connector.patch(url)

        result = None
        if response.status_code == HTTPStatus.OK:
            response_model = DocAPIResponseOK.model_validate(response.json())
            result = JobLease.model_validate(response_model.data)
            logger.info(f"Job lease '{job_id}' successfully extended.")
        else:
            logger.error(f"Extending job lease '{job_id}' failed. Response: {response.status_code} {response.text}")

        return result

    def delete_job_lease(self, job_id=None, route="/v1/jobs/{job_id}/lease") -> bool:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id))
        response = self.connector.delete(url)

        if response.status_code == HTTPStatus.NO_CONTENT:
            logger.info(f"Job lease '{job_id}' successfully released.")
            return True
        else:
            logger.error(f"Releasing job lease '{job_id}' failed. Response: {response.status_code} {response.text}")
            return False

    def patch_job_progress_update(self, progress: float, log: str = None, log_user: str = None, job_id=None, route="/v1/jobs/{job_id}") -> bool:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id))
        
        data = {"progress": progress}
        if log is not None:
            data["log"] = log
        if log_user is not None:
            data["log_user"] = log_user
            
        response = self.connector.patch(url, json=data)

        if response.status_code == HTTPStatus.OK:
            logger.info(f"Job '{job_id}' progress successfully updated.")
            return True
        else:
            logger.error(f"Updating job '{job_id}' progress failed. Response: {response.status_code} {response.text}")
            return False

    def put_image(self, file_path, image_name, job_id=None, route="/v1/jobs/{job_id}/images/{image_name}/files/image") -> bool:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id, image_name=image_name))

        with open(file_path, 'rb') as file:
            file_bytes = file.read()

        response = self.connector.put(url, files={"file": file_bytes})

        if response.status_code == HTTPStatus.CREATED or response.status_code == HTTPStatus.OK:
            logger.info(f"Image '{image_name}' for job '{job_id}' successfully uploaded.")
            return True
        else:
            logger.error(f"Uploading image '{image_name}' for job '{job_id}' failed. Response: {response.status_code} {response.text}")
            return False

    def put_alto(self, file_path, image_name, job_id=None, route="/v1/jobs/{job_id}/images/{image_name}/files/alto") -> bool:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id, image_name=image_name))

        with open(file_path, 'rb') as file:
            file_bytes = file.read()

        response = self.connector.put(url, files={"file": file_bytes})

        if response.status_code == HTTPStatus.CREATED or response.status_code == HTTPStatus.OK:
            logger.info(f"ALTO '{image_name}' for job '{job_id}' successfully uploaded.")
            return True
        else:
            logger.error(f"Uploading ALTO '{image_name}' for job '{job_id}' failed. Response: {response.status_code} {response.text}")
            return False

    def put_page(self, file_path, image_name, job_id=None, route="/v1/jobs/{job_id}/images/{image_name}/files/page") -> bool:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id, image_name=image_name))

        with open(file_path, 'rb') as file:
            file_bytes = file.read()

        response = self.connector.put(url, files={"file": file_bytes})

        if response.status_code == HTTPStatus.CREATED or response.status_code == HTTPStatus.OK:
            logger.info(f"PAGE '{image_name}' for job '{job_id}' successfully uploaded.")
            return True
        else:
            logger.error(f"Uploading PAGE '{image_name}' for job '{job_id}' failed. Response: {response.status_code} {response.text}")
            return False

    def put_meta_json(self, json_path, job_id=None, route="/v1/jobs/{job_id}/files/metadata") -> bool:
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
