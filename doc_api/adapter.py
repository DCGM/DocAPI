import logging
import os
import json
from http import HTTPStatus
from urllib.parse import urljoin
from typing import Any

import cv2
import numpy as np

from doc_api.api.schemas.base_objects import Job, JobLease, ProcessingState
from doc_api.api.schemas.responses import AppCode, DocAPIResponseOK

from doc_api.connector import Connector


logger = logging.getLogger(__name__)


class AdapterResponse:
    """
    Represents a standardized response from adapter methods.    
    Attributes:
        data: The actual data returned on success (or None on failure)
        response: The HTTP response object (primarily for error handling)
        no_data_response: True if this is a successful operation that doesn't return data
    """
    def __init__(self, data: Any = None, response: Any = None, no_data_response: bool = False):
        self.data = data
        self.response = response
        self.no_data_response = no_data_response
    
    @property
    def is_success(self) -> bool:
        """Returns True if the response indicates success."""
        if hasattr(self.response, 'status_code'):
            return (self.data is not None or self.no_data_response) and self.response.status_code in [HTTPStatus.OK, HTTPStatus.CREATED, HTTPStatus.NO_CONTENT]
        return self.data is not None


class Adapter:
    def __init__(self, api_url, connector: Connector, job: Job | None = None):
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

    def get_me(self, route="/v1/me") -> AdapterResponse:
        url = self.compose_url(route)
        response = self.connector.get(url)

        result = None
        if response.status_code == HTTPStatus.OK:
            result = response.json()

        adapter_response = AdapterResponse(data=result, response=response)
        
        if adapter_response.is_success:
            logger.debug("User info successfully obtained.")
        else:
            logger.warning(f"Response: {response.status_code} {response.text}")

        return adapter_response

    def get_job(self, job_id=None, set_if_successful=False, route="/v1/jobs/") -> AdapterResponse:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route, job_id)
        response = self.connector.get(url)

        result = None
        if response.status_code == HTTPStatus.OK:
            response_model = DocAPIResponseOK.model_validate(response.json())
            result = Job.model_validate(response_model.data)

        adapter_response = AdapterResponse(data=result, response=response)
        
        if adapter_response.is_success and set_if_successful:
            self.job = result
        elif not adapter_response.is_success:
            logger.warning(f"Response: {response.status_code} {response.text}")

        return adapter_response

    def get_image(self, image_id, job_id=None, route="/v1/jobs/{job_id}/images/{image_id}/files/image") -> AdapterResponse:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id, image_id=image_id))
        response = self.connector.get(url)

        result = None
        if response.status_code == HTTPStatus.OK:
            result = cv2.imdecode(np.asarray(bytearray(response.content), dtype="uint8"), cv2.IMREAD_COLOR)

        adapter_response = AdapterResponse(data=result, response=response)
        
        if adapter_response.is_success:
            logger.info(f"Image '{image_id}' for job '{job_id}' successfully downloaded.")
        else:
            logger.error(f"Downloading image '{image_id}' failed. Response: {response.status_code} {response.text}")

        return adapter_response

    def get_alto(self, image_id, job_id=None, route="/v1/jobs/{job_id}/images/{image_id}/files/alto") -> AdapterResponse:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id, image_id=image_id))
        response = self.connector.get(url)

        result = None
        if response.status_code == HTTPStatus.OK:
            result = response.content.decode()

        adapter_response = AdapterResponse(data=result, response=response)
        
        if adapter_response.is_success:
            logger.info(f"ALTO '{image_id}' for job '{job_id}' successfully downloaded.")
        else:
            logger.error(f"Downloading ALTO '{image_id}' failed. Response: {response.status_code} {response.text}")

        return adapter_response

    def get_page(self, image_id, job_id=None, route="/v1/jobs/{job_id}/images/{image_id}/files/page") -> AdapterResponse:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id, image_id=image_id))
        response = self.connector.get(url)

        result = None
        if response.status_code == HTTPStatus.OK:
            result = response.content.decode()

        adapter_response = AdapterResponse(data=result, response=response)
        
        if adapter_response.is_success:
            logger.info(f"PAGE '{image_id}' for job '{job_id}' successfully downloaded.")
        else:
            logger.error(f"Downloading PAGE '{image_id}' failed. Response: {response.status_code} {response.text}")

        return adapter_response

    def get_meta_json(self, job_id=None, route="/v1/jobs/{job_id}/files/metadata") -> AdapterResponse:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id))
        response = self.connector.get(url)

        result = None
        if response.status_code == HTTPStatus.OK:
            result = response.content.decode()

        adapter_response = AdapterResponse(data=result, response=response)
        
        if adapter_response.is_success:
            logger.info(f"Meta JSON for job '{job_id}' successfully downloaded.")
        else:
            logger.error(f"Downloading Meta JSON for job '{job_id}' failed. Response: {response.status_code} {response.text}")

        return adapter_response

    def get_result(self, job_id=None, route="/v1/jobs/{job_id}/result") -> AdapterResponse:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id))
        response = self.connector.get(url)

        result = None
        if response.status_code == HTTPStatus.OK:
            result = response.content

        adapter_response = AdapterResponse(data=result, response=response)
        
        if adapter_response.is_success:
            logger.info(f"Result for job '{job_id}' successfully downloaded.")
        else:
            logger.error(f"Downloading result for job '{job_id}' failed. Response: {response.status_code} {response.text}")

        return adapter_response

    def get_engine_files(self, engine_id, route="/v1/engines/{engine_id}/files") -> AdapterResponse:
        url = self.compose_url(route.format(engine_id=engine_id))
        response = self.connector.get(url)

        result = None
        if response.status_code == HTTPStatus.OK:
            result = response.content

        adapter_response = AdapterResponse(data=result, response=response)
        
        if adapter_response.is_success:
            logger.info(f"Engine files '{engine_id}' successfully downloaded.")
        else:
            logger.error(f"Downloading engine files '{engine_id}' failed. Response: {response.status_code} {response.text}")

        return adapter_response

    def post_job(self, data, set_if_successful=False, route="/v1/jobs") -> AdapterResponse:
        url = self.compose_url(route)
        response = self.connector.post(url, json=data)

        result = None
        if response.status_code == HTTPStatus.CREATED:
            response_model = DocAPIResponseOK.model_validate(response.json())
            result = Job.model_validate(response_model.data)

        adapter_response = AdapterResponse(data=result, response=response)
        
        if adapter_response.is_success:
            logger.info(f"Job '{result.id}' successfully created.")
            if set_if_successful:
                self.job = result
        else:
            logger.error(f"Creating job failed. Response: {response.status_code} {response.text}")

        return adapter_response

    def post_job_lease(self, route="/v1/jobs/lease") -> AdapterResponse:
        url = self.compose_url(route)
        response = self.connector.post(url)

        result = None
        job_leased = False
        
        if response.status_code == HTTPStatus.OK:
            response_model = DocAPIResponseOK.model_validate(response.json())

            if response_model.code == AppCode.JOB_LEASED:
                result = JobLease.model_validate(response_model.data)
                job_leased = True
            else:
                logger.debug(f"No job found.")

        adapter_response = AdapterResponse(data=result, response=response, no_data_response=not job_leased)
        
        if adapter_response.is_success and job_leased:
            logger.debug("Job successfully obtained.")
        elif not adapter_response.is_success:
            logger.warning(f"Response: {response.status_code} {response.text}")

        return adapter_response

    def post_artifacts(self, job_id, artifacts_bytes, route="/v1/jobs/{job_id}/artifacts") -> AdapterResponse:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id))

        response = self.connector.post(url, files={"file": ("artifacts.zip", artifacts_bytes, "application/zip")})

        adapter_response = AdapterResponse(data=None, response=response, no_data_response=True)
        
        if adapter_response.is_success:
            logger.info(f"Artifacts for job '{job_id}' successfully uploaded.")
        else:
            logger.error(f"Uploading artifacts for job '{job_id}' failed. Response: {response.status_code} {response.text}")

        return adapter_response

    def post_result(self, result_path, job_id=None, route="/v1/jobs/{job_id}/result") -> AdapterResponse:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id))

        with open(result_path, 'rb') as file:
            result_bytes = file.read()

        response = self.connector.post(url, files={"file": ("result.zip", result_bytes, "application/zip")})
        
        adapter_response = AdapterResponse(data=None, response=response, no_data_response=True)
        
        if adapter_response.is_success:
            logger.info(f"Result for job '{job_id}' successfully uploaded.")
        else:
            logger.error(f"Uploading result for job '{job_id}' failed. Response: {response.status_code} {response.text}")

        return adapter_response

    def patch_job_finish(self, job_id=None, route="/v1/jobs") -> AdapterResponse:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route, job_id)
        response = self.connector.patch(url, json={"state": ProcessingState.DONE.value})

        adapter_response = AdapterResponse(data=None, response=response, no_data_response=True)
        
        if adapter_response.is_success:
            logger.info(f"Job '{job_id}' successfully marked as finished.")
        else:
            logger.error(f"Marking job '{job_id}' as finished failed. Response: {response.status_code} {response.text}")

        return adapter_response

    def patch_job_cancel(self, job_id=None, route="/v1/jobs") -> AdapterResponse:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route, job_id)
        response = self.connector.patch(url, json={"state": ProcessingState.CANCELLED.value})

        adapter_response = AdapterResponse(data=None, response=response, no_data_response=True)
        
        if adapter_response.is_success:
            logger.info(f"Job '{job_id}' successfully cancelled.")
        else:
            logger.error(f"Cancelling job '{job_id}' failed. Response: {response.status_code} {response.text}")

        return adapter_response

    def patch_job_fail(self, log: str = None, log_user: str = None, job_id=None, route="/v1/jobs") -> AdapterResponse:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route, job_id)
        
        data = {"state": ProcessingState.ERROR.value}
        if log is not None:
            data["log"] = log
        if log_user is not None:
            data["log_user"] = log_user
            
        response = self.connector.patch(url, json=data)

        adapter_response = AdapterResponse(data=None, response=response, no_data_response=True)
        
        if adapter_response.is_success:
            logger.info(f"Job '{job_id}' successfully marked as failed.")
        else:
            logger.error(f"Marking job '{job_id}' as failed failed. Response: {response.status_code} {response.text}")

        return adapter_response

    def patch_job_lease(self, job_id=None, route="/v1/jobs/{job_id}/lease") -> AdapterResponse:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id))
        response = self.connector.patch(url)

        result = None
        if response.status_code == HTTPStatus.OK:
            response_model = DocAPIResponseOK.model_validate(response.json())
            result = JobLease.model_validate(response_model.data)

        adapter_response = AdapterResponse(data=result, response=response)
        
        if adapter_response.is_success:
            logger.info(f"Job lease '{job_id}' successfully extended.")
        else:
            logger.error(f"Extending job lease '{job_id}' failed. Response: {response.status_code} {response.text}")

        return adapter_response

    def delete_job_lease(self, job_id=None, route="/v1/jobs/{job_id}/lease") -> AdapterResponse:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id))
        response = self.connector.delete(url)

        adapter_response = AdapterResponse(data=None, response=response, no_data_response=True)
        
        if adapter_response.is_success:
            logger.info(f"Job lease '{job_id}' successfully released.")
        else:
            logger.error(f"Releasing job lease '{job_id}' failed. Response: {response.status_code} {response.text}")

        return adapter_response

    def patch_job_progress_update(self, progress: float, log: str = None, log_user: str = None, job_id=None, route="/v1/jobs/{job_id}") -> AdapterResponse:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id))
        
        data = {"progress": progress}
        if log is not None:
            data["log"] = log
        if log_user is not None:
            data["log_user"] = log_user
            
        response = self.connector.patch(url, json=data)

        result = None
        adapter_response = None

        if response.status_code == HTTPStatus.OK:
            response_model = DocAPIResponseOK.model_validate(response.json())
            if response_model.code == AppCode.JOB_UPDATED:
                result = JobLease.model_validate(response_model.data)
                adapter_response = AdapterResponse(data=result, response=response)
        
        if adapter_response is None:
            adapter_response = AdapterResponse(data=None, response=response, no_data_response=True)
        
        if adapter_response.is_success:
            logger.info(f"Job '{job_id}' progress successfully updated.")
        else:
            logger.error(f"Updating job '{job_id}' progress failed. Response: {response.status_code} {response.text}")

        return adapter_response

    def put_image(self, file_path, image_name, job_id=None, route="/v1/jobs/{job_id}/images/{image_name}/files/image") -> AdapterResponse:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id, image_name=image_name))

        with open(file_path, 'rb') as file:
            file_bytes = file.read()

        response = self.connector.put(url, files={"file": file_bytes})

        adapter_response = AdapterResponse(data=None, response=response, no_data_response=True)
        
        if adapter_response.is_success:
            logger.info(f"Image '{image_name}' for job '{job_id}' successfully uploaded.")
        else:
            logger.error(f"Uploading image '{image_name}' for job '{job_id}' failed. Response: {response.status_code} {response.text}")

        return adapter_response

    def put_alto(self, file_path, image_name, job_id=None, route="/v1/jobs/{job_id}/images/{image_name}/files/alto") -> AdapterResponse:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id, image_name=image_name))

        with open(file_path, 'rb') as file:
            file_bytes = file.read()

        response = self.connector.put(url, files={"file": file_bytes})

        adapter_response = AdapterResponse(data=None, response=response, no_data_response=True)
        
        if adapter_response.is_success:
            logger.info(f"ALTO '{image_name}' for job '{job_id}' successfully uploaded.")
        else:
            logger.error(f"Uploading ALTO '{image_name}' for job '{job_id}' failed. Response: {response.status_code} {response.text}")

        return adapter_response

    def put_page(self, file_path, image_name, job_id=None, route="/v1/jobs/{job_id}/images/{image_name}/files/page") -> AdapterResponse:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id, image_name=image_name))

        with open(file_path, 'rb') as file:
            file_bytes = file.read()

        response = self.connector.put(url, files={"file": file_bytes})

        adapter_response = AdapterResponse(data=None, response=response, no_data_response=True)
        
        if adapter_response.is_success:
            logger.info(f"PAGE '{image_name}' for job '{job_id}' successfully uploaded.")
        else:
            logger.error(f"Uploading PAGE '{image_name}' for job '{job_id}' failed. Response: {response.status_code} {response.text}")

        return adapter_response

    def put_meta_json(self, json_path, job_id=None, route="/v1/jobs/{job_id}/files/metadata") -> AdapterResponse:
        job_id = self.get_job_id(job_id)

        url = self.compose_url(route.format(job_id=job_id))

        with open(json_path, 'r') as file:
            data = json.load(file)

        response = self.connector.put(url, json=data)

        adapter_response = AdapterResponse(data=None, response=response, no_data_response=True)
        
        if adapter_response.is_success:
            logger.info(f"Meta JSON for job '{job_id}' successfully uploaded.")
        else:
            logger.error(f"Uploading Meta JSON for job '{job_id}' failed. Response: {response.status_code} {response.text}")

        return adapter_response
