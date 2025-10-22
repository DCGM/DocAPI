import logging
import os
import zipfile
from types import NoneType

import aiofiles
import fastapi
from fastapi import Depends, UploadFile, File, Request
from fastapi.responses import FileResponse

from aiofiles import os as aiofiles_os

from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.authentication import require_api_key
from doc_api.api.cruds import worker_cruds, general_cruds
from doc_api.api.database import get_async_session
from doc_api.api.routes import root_router
from doc_api.api.routes.helper import RouteInvariantError
from doc_api.api.routes.worker_guards import challenge_worker_access_to_processing_job
from doc_api.api.schemas import base_objects
from doc_api.api.schemas.responses import AppCode, DocAPIResponseOK, \
    DocAPIResponseClientError, DocAPIClientErrorException, make_responses, GENERAL_RESPONSES, validate_ok_response
from doc_api.db import model
from doc_api.api.config import config

from typing import List
from uuid import UUID

logger = logging.getLogger(__name__)


POST_LEASE_RESPONSES = {
    AppCode.JOB_ASSIGNED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Job has been assigned to the worker and the lease has been established (UTC time).",
        "model": DocAPIResponseOK,
        "model_data": base_objects.JobLease,
        "detail": "Job has been assigned to the worker and the lease has been established (UTC time).",
    },
    AppCode.JOB_QUEUE_EMPTY: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "No jobs are currently available in the queue.",
        "model": DocAPIResponseOK,
        "detail": "Queue is empty right now, try again shortly.",
    }
}
@root_router.post(
    "/v1/jobs/lease",
    summary="Request Job Lease",
    response_model=DocAPIResponseOK[base_objects.JobLease],
    tags=["Worker"],
    description=f"Request a job lease for processing. If a job is available, it will be assigned to the worker along with a lease time. "
                f"If no jobs are available, a response indicating an empty queue will be returned.",
    responses=make_responses(POST_LEASE_RESPONSES))
async def post_lease(
        request: Request,
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):

    db_job, code = await worker_cruds.assign_job_to_worker(db=db, worker_key_id=key.id)

    if code == AppCode.JOB_ASSIGNED and db_job is not None:
        return DocAPIResponseOK[base_objects.Job](
            status=fastapi.status.HTTP_200_OK,
            code=AppCode.JOB_ASSIGNED,
            detail=POST_LEASE_RESPONSES[AppCode.JOB_ASSIGNED]["detail"],
            data=db_job
        )
    elif code == AppCode.JOB_QUEUE_EMPTY:
        return validate_ok_response(
            DocAPIResponseOK[NoneType](
                status=fastapi.status.HTTP_200_OK,
                code=AppCode.JOB_QUEUE_EMPTY,
                detail=POST_LEASE_RESPONSES[AppCode.JOB_QUEUE_EMPTY]["detail"]
            )
        )

    raise RouteInvariantError(code=code, request=request)


PATCH_LEASE_RESPONSES = {
    AppCode.JOB_LEASE_EXTENDED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Job lease has been successfully extended (UTC time).",
        "model": DocAPIResponseOK,
        "model_data": base_objects.JobLease,
        "detail": "Job lease has been successfully extended (UTC time)."
    }
}
@root_router.patch(
    "/v1/jobs/{job_id}/lease",
    response_model=base_objects.JobLease,
    summary="Extend Job Lease",
    tags=["Worker"],
    description="Extend the lease time for a specific job that is currently being processed by the worker.",
    responses=make_responses(PATCH_LEASE_RESPONSES))
@challenge_worker_access_to_processing_job
async def patch_lease(
        request: Request,
        job_id: UUID,
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):

    code, lease_expire_at, server_time = await worker_cruds.update_processing_job_lease(db=db, job_id=job_id)

    if code == AppCode.JOB_LEASE_EXTENDED:
        return DocAPIResponseOK[base_objects.JobLease](
            status=fastapi.status.HTTP_200_OK,
            code=AppCode.JOB_LEASE_EXTENDED,
            detail=PATCH_LEASE_RESPONSES[AppCode.JOB_LEASE_EXTENDED]["detail"],
            data=base_objects.JobLease(id=job_id, lease_expire_at=lease_expire_at, server_time=server_time),
        )

    raise RouteInvariantError(code=code, request=request)


DELETE_LEASE_RESPONSES = {
    AppCode.JOB_LEASE_RELEASED: {
        "status": fastapi.status.HTTP_204_NO_CONTENT,
        "description": "The job lease has been successfully released.",
        "example_value": "NO BODY"
    }
}
@root_router.delete(
    "/v1/jobs/{job_id}/lease",
    status_code=fastapi.status.HTTP_204_NO_CONTENT,
    summary="Release Job Lease",
    tags=["Worker"],
    description="Release the lease for a specific job that is currently being processed by the worker.",
    responses=make_responses(DELETE_LEASE_RESPONSES))
@challenge_worker_access_to_processing_job
async def delete_lease(
        request: Request,
        job_id: UUID,
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):

    code = await worker_cruds.release_job_lease(db=db, job_id=job_id)

    if code == AppCode.JOB_LEASE_RELEASED:
        return validate_ok_response(DocAPIResponseOK[NoneType](
            status=fastapi.status.HTTP_204_NO_CONTENT,
            code="",
            detail=""
        ))

    raise RouteInvariantError(code=code, request=request)


GET_METADATA = {
    AppCode.META_JSON_DOWNLOADED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "JSON data of the requested Meta JSON file.",
        "content_type": "application/json",
        "example_value": {"meta_key": "meta_value"},
    },
    AppCode.META_JSON_NOT_UPLOADED: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": "The requested Meta JSON file has not been uploaded yet.",
        "model": DocAPIResponseClientError,
        "detail": "Meta JSON file for the job has not been uploaded yet.",
    }
}
@root_router.get(
    "/v1/jobs/{job_id}/images/{image_id}/files/metadata",
    response_class=FileResponse,
    summary="Download Meta JSON",
    tags=["Worker"],
    description="Download the Meta JSON file associated with a specific job.",
    responses=make_responses(GET_METADATA))
@challenge_worker_access_to_processing_job
async def get_metadata(
        request: Request,
        job_id: UUID,
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):

    db_job, code = await general_cruds.get_job(db=db, job_id=job_id)

    if code == AppCode.JOB_RETRIEVED and db_job.meta_json_uploaded:
        meta_json_path = os.path.join(config.JOBS_DIR, str(job_id), "meta.json")
        if not await aiofiles_os.path.exists(meta_json_path):
            raise DocAPIClientErrorException(
                status=fastapi.status.HTTP_404_NOT_FOUND,
                code=AppCode.META_JSON_NOT_UPLOADED,
                detail=GET_METADATA[AppCode.META_JSON_NOT_UPLOADED]["detail"],
            )
        return FileResponse(meta_json_path, media_type="application/json", filename="meta.json")
    elif code == AppCode.JOB_RETRIEVED and db_job is not None and not db_job.meta_json_uploaded:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_409_CONFLICT,
            code=AppCode.META_JSON_NOT_UPLOADED,
            detail=GET_METADATA[AppCode.META_JSON_NOT_UPLOADED]["detail"],
        )

    raise RouteInvariantError(code=code, request=request)


GET_IMAGE_FOR_JOB_RESPONSES = {
    AppCode.IMAGE_DOWNLOADED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Binary data of the requested IMAGE file, format not guaranteed but usually JPEG/PNG.",
        "content_type": "image/jpeg",
        "example_value": "(binary IMAGE file content)"
    },
    AppCode.IMAGE_NOT_UPLOADED: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": "The requested IMAGE file has not been uploaded yet.",
        "model": DocAPIResponseClientError,
        "detail": "IMAGE file has not been uploaded yet.",
    },
    AppCode.IMAGE_NOT_FOUND_FOR_JOB: GENERAL_RESPONSES[AppCode.IMAGE_NOT_FOUND_FOR_JOB],
}
@root_router.get(
    "/image/{job_id}/{image_id}",
    response_class=FileResponse,
    summary="Download IMAGE",
    tags=["Worker"],
    description="Download the IMAGE file associated with a specific image of a job.",
    responses=make_responses(GET_IMAGE_FOR_JOB_RESPONSES))
@challenge_worker_access_to_processing_job
async def get_image_for_job(
        request: Request,
        job_id: UUID, image_id: UUID,
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_worker_access_to_processing_job(db=db, key=key, job_id=job_id)

    db_image, code = await general_cruds.get_image_for_job(db=db, job_id=job_id, image_id=image_id)

    if code == AppCode.IMAGE_RETRIEVED and db_image is not None and db_image.image_uploaded:
        image_path = os.path.join(config.JOBS_DIR, str(db_image.job_id), f"{db_image.id}.jpg")
        return FileResponse(image_path, media_type="image/jpeg", filename=db_image.name)
    elif code == AppCode.IMAGE_RETRIEVED and db_image is not None and not db_image.image_uploaded:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_409_CONFLICT,
            code=AppCode.IMAGE_NOT_UPLOADED,
            detail=GET_IMAGE_FOR_JOB_RESPONSES[AppCode.IMAGE_NOT_UPLOADED]["detail"],
        )
    elif code == AppCode.IMAGE_NOT_FOUND_FOR_JOB:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_404_NOT_FOUND,
            code=AppCode.IMAGE_NOT_FOUND_FOR_JOB,
            detail=GET_IMAGE_FOR_JOB_RESPONSES[AppCode.IMAGE_NOT_FOUND_FOR_JOB]["detail"],
        )

    raise RouteInvariantError(code=code, request=request)


GET_ALTO_FOR_JOB_RESPONSES = {
    AppCode.ALTO_DOWNLOADED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "XML data of the requested ALTO file.",
        "content_type": "application/xml",
        "example_value": "<alto>...</alto>",
    },
    AppCode.ALTO_NOT_UPLOADED: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": "The requested ALTO file has not been uploaded yet.",
        "model": DocAPIResponseClientError,
        "detail": "The requested ALTO file has not been uploaded yet.",
    },
    AppCode.IMAGE_NOT_FOUND_FOR_JOB: GENERAL_RESPONSES[AppCode.IMAGE_NOT_FOUND_FOR_JOB],
}
@root_router.get(
    "/alto/{job_id}/{image_id}",
    response_class=FileResponse,
    summary="Download ALTO",
    tags=["Worker"],
    description="Download the ALTO file associated with a specific image of a job.",
    responses=make_responses(GET_ALTO_FOR_JOB_RESPONSES))
@challenge_worker_access_to_processing_job
async def get_alto_for_job(
        request: Request,
        job_id: UUID,
        image_id: UUID,
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_worker_access_to_processing_job(db=db, key=key, job_id=job_id)

    db_image, code = await general_cruds.get_image_for_job(db=db, job_id=job_id, image_id=image_id)

    if code == AppCode.IMAGE_RETRIEVED and db_image is not None and db_image.alto_uploaded:
        alto_path = os.path.join(config.JOBS_DIR, str(db_image.job_id), f"{db_image.id}.alto.xml")
        return FileResponse(alto_path, media_type="application/xml", filename=f"{os.path.splitext(db_image.name)[0]}.xml")
    elif code == AppCode.IMAGE_RETRIEVED and db_image is not None and not db_image.alto_uploaded:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_404_NOT_FOUND,
            code=AppCode.ALTO_NOT_UPLOADED,
            detail=GET_ALTO_FOR_JOB_RESPONSES[AppCode.ALTO_NOT_UPLOADED]["detail"]
        )
    elif code == AppCode.IMAGE_NOT_FOUND_FOR_JOB:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_404_NOT_FOUND,
            code=AppCode.IMAGE_NOT_FOUND_FOR_JOB,
            detail=GET_ALTO_FOR_JOB_RESPONSES[AppCode.IMAGE_NOT_FOUND_FOR_JOB]["detail"]
        )

    raise RouteInvariantError(code=code, request=request)




'''
UPDATE_JOB_RESPONSES = {
    AppCode.JOB_UPDATED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "The job has been updated successfully and the lease has been extended.",
        "model": DocAPIResponseOK,
        "model_data": base_objects.JobLease,
        "detail": "Job has been updated successfully, lease extended (UTC time).",
    },
}
@root_router.patch(
    "/job/{job_id}",
    response_model=base_objects.JobLease,
    summary="Update Job Progress",
    tags=["Worker"],
    description="Update the job's progress and extend its lease time.",
    responses=make_responses(UPDATE_JOB_RESPONSES))
@challenge_worker_access_to_processing_job
async def patch_job(
        request: Request,
        job_id: UUID,
        job_progress_update: base_objects.JobProgressUpdate,
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_worker_access_to_processing_job(db=db, key=key, job_id=job_id)

    # job already challenged above, so here we are sure it exists and in PROCESSING state
    db_job, lease_expire_at, server_time, code = await worker_cruds.update_processing_job_progress(db=db, job_id=job_id, job_progress_update=job_progress_update)
    if code == AppCode.JOB_UPDATED:
        return DocAPIResponseOK[base_objects.JobLease](
            status=fastapi.status.HTTP_200_OK,
            code=AppCode.JOB_UPDATED,
            detail=UPDATE_JOB_RESPONSES[AppCode.JOB_UPDATED]["detail"],
            data=base_objects.JobLease(id=job_id, lease_expire_at=lease_expire_at, server_time=server_time)
        )

    raise RouteInvariantError(code=code, request=request)
'''


POST_RESULT_FOR_JOB_RESPONSES = {
    AppCode.JOB_RESULT_UPLOADED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "The result ZIP archive for the job has been uploaded successfully.",
        "model": DocAPIResponseOK,
        "detail": "Result ZIP archive for Job uploaded successfully.",
    },
    AppCode.JOB_RESULT_INVALID: {
        "status": fastapi.status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
        "description": "The uploaded file is not a valid ZIP archive.",
        "model": DocAPIResponseClientError,
        "detail": "The uploaded file is not a valid ZIP archive.",
    },
}
@root_router.post(
    "/result/{job_id}",
    response_model=DocAPIResponseOK,
    summary="Upload Job Result",
    tags=["Worker"],
    description="Upload the result ZIP archive for a specific job.",
    responses=make_responses(POST_RESULT_FOR_JOB_RESPONSES))
@challenge_worker_access_to_processing_job
async def post_result_for_job(
    job_id: UUID,
    result: UploadFile = File(...),
    key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
    db: AsyncSession = Depends(get_async_session),
):
    await challenge_worker_access_to_processing_job(db=db, key=key, job_id=job_id)

    await aiofiles_os.makedirs(config.RESULT_DIR, exist_ok=True)
    final_path = os.path.join(config.RESULT_DIR, f"{job_id}.zip")
    tmp_path = final_path + ".validating"

    # Stream upload to a temp file
    async with aiofiles.open(tmp_path, "wb") as f:
        while chunk := await result.read(1024 * 1024):
            await f.write(chunk)

    # Validate ZIP (central directory)
    try:
        with zipfile.ZipFile(tmp_path):
            pass
    except zipfile.BadZipFile:
        await aiofiles_os.remove(tmp_path)
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            code=AppCode.JOB_RESULT_INVALID,
            detail=POST_RESULT_FOR_JOB_RESPONSES[AppCode.JOB_RESULT_INVALID]["detail"],
        )

    # Atomically move the validated file into place
    os.replace(tmp_path, final_path)

    return DocAPIResponseOK[NoneType](
        status=fastapi.status.HTTP_200_OK,
        code=AppCode.JOB_RESULT_UPLOADED,
        detail=POST_RESULT_FOR_JOB_RESPONSES[AppCode.JOB_RESULT_UPLOADED]["detail"]
    )


POST_JOB_COMPLETE_RESPONSES = {
    AppCode.JOB_COMPLETED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "The job has been marked as completed.",
        "model": DocAPIResponseOK,
        "detail": "Job has been marked as completed.",
    },
    AppCode.JOB_ALREADY_COMPLETED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "The job was already marked as completed.",
        "model": DocAPIResponseOK,
        "detail": "Job was already marked as completed.",
    },
    AppCode.JOB_RESULT_MISSING: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": "The result ZIP for the job has not been uploaded yet.",
        "model": DocAPIResponseClientError,
        "detail": "Result ZIP for Job has not been uploaded yet.",
    },
}

'''
@root_router.post(
    "/jobs/{job_id}/complete",
    response_model=DocAPIResponseOK,
    summary="Complete Job",
    tags=["Worker"],
    description="Mark a specific job as completed after all results have been uploaded.",
    responses= make_responses(POST_JOB_COMPLETE_RESPONSES))
@uses_challenge_worker_access_to_finalizing_job
async def post_job_complete(
        request: Request,
        job_id: UUID,
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_worker_access_to_finalizing_job(db=db, key=key, job_id=job_id)

    result_path = os.path.join(config.RESULT_DIR, f"{job_id}.zip")
    if not await aiofiles_os.path.exists(result_path):
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_409_CONFLICT,
            code=AppCode.JOB_RESULT_MISSING,
            detail=POST_JOB_COMPLETE_RESPONSES[AppCode.JOB_RESULT_MISSING]["detail"],
        )
    code = await worker_cruds.complete_job(db=db, job_id=job_id)

    if code == AppCode.JOB_COMPLETED:
        return DocAPIResponseOK[NoneType](
            status=fastapi.status.HTTP_200_OK,
            code=AppCode.JOB_COMPLETED,
            detail=POST_JOB_COMPLETE_RESPONSES[AppCode.JOB_COMPLETED]["detail"]
        )
    elif code == AppCode.JOB_ALREADY_COMPLETED:
        return DocAPIResponseOK[NoneType](
            status=fastapi.status.HTTP_200_OK,
            code=AppCode.JOB_ALREADY_COMPLETED,
            detail=POST_JOB_COMPLETE_RESPONSES[AppCode.JOB_ALREADY_COMPLETED]["detail"]
        )

    raise RouteInvariantError(code=code, request=request)


POST_JOB_FAIL_RESPONSES = {
    AppCode.JOB_FAILED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "The job has been marked as failed.",
        "model": DocAPIResponseOK,
        "detail": "Job has been marked as failed.",
    },
    AppCode.JOB_ALREADY_FAILED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "The job was already marked as failed.",
        "model": DocAPIResponseOK,
        "detail": "Job was already marked as failed.",
    },
}
@root_router.post(
    "/jobs/{job_id}/fail",
    response_model=DocAPIResponseOK[NoneType],
    summary="Fail Job",
    tags=["Worker"],
    description="Mark a specific job as failed.",
    responses=make_responses(POST_JOB_FAIL_RESPONSES))
@uses_challenge_worker_access_to_finalizing_job
async def post_job_fail(
    request: Request,
    job_id: UUID,
    key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
    db: AsyncSession = Depends(get_async_session),
):
    await challenge_worker_access_to_finalizing_job(db=db, key=key, job_id=job_id)

    code = await worker_cruds.fail_job(db=db, job_id=job_id)

    if code == AppCode.JOB_FAILED:
        return DocAPIResponseOK[NoneType](
            status=fastapi.status.HTTP_200_OK,
            code=AppCode.JOB_FAILED,
            detail=POST_JOB_FAIL_RESPONSES[AppCode.JOB_FAILED]["detail"]
        )
    elif code == AppCode.JOB_ALREADY_FAILED:
        return DocAPIResponseOK[NoneType](
            status=fastapi.status.HTTP_200_OK,
            code=AppCode.JOB_ALREADY_FAILED,
            detail=POST_JOB_FAIL_RESPONSES[AppCode.JOB_ALREADY_FAILED]["detail"]
        )

    raise RouteInvariantError(code=code, request=request)
'''

