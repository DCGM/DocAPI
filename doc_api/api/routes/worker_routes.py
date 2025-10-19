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
from doc_api.api.cruds import worker_cruds, job_cruds
from doc_api.api.database import get_async_session
from doc_api.api.routes.helper import RouteInvariantError
from doc_api.api.routes.route_guards import challenge_worker_access_to_job, uses_challenge_worker_access_to_job
from doc_api.api.schemas import base_objects
from doc_api.api.schemas.responses import AppCode, DocAPIResponseOK, validate_no_data_ok_response, \
    DocAPIResponseClientError, DocAPIClientErrorException, DETAILS_GENERAL, make_responses
from doc_api.db import model
from doc_api.api.routes import worker_router
from doc_api.config import config

from typing import List
from uuid import UUID

logger = logging.getLogger(__name__)


GET_JOB_RESPONSES = {
    AppCode.JOB_ASSIGNED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "A job has been successfully assigned to the worker.",
        "model": DocAPIResponseOK[base_objects.JobLease],
        "detail": "Job has been assigned to the worker, lease established (UTC time).",
    },

    AppCode.JOB_QUEUE_EMPTY: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "No jobs are currently available in the queue.",
        "model": DocAPIResponseOK[NoneType],
        "detail": "Queue is empty right now, try again shortly.",
    },
}

@worker_router.get(
    "/job",
    summary="Assign Job",
    response_model=DocAPIResponseOK[base_objects.JobLease],
    tags=["Worker"],
    description=f"Assign a job to the requesting worker. "
                f"If a job is available in the queue, it will be assigned to the worker and a lease will be established. "
                f"If no jobs are available, a response indicating the empty queue will be returned.",
    responses=make_responses(GET_JOB_RESPONSES))
async def get_job(
        request: Request,
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):
    db_job, code = await worker_cruds.assign_job_to_worker(db=db, worker_key_id=key.id)
    if code == AppCode.JOB_ASSIGNED and db_job is not None:
        return DocAPIResponseOK[base_objects.Job](
            status=fastapi.status.HTTP_200_OK,
            code=AppCode.JOB_ASSIGNED,
            detail=GET_JOB_RESPONSES[AppCode.JOB_ASSIGNED]["detail"],
            data=db_job
        )
    elif code == AppCode.JOB_QUEUE_EMPTY:
        return validate_no_data_ok_response(
            DocAPIResponseOK[NoneType](
                status=fastapi.status.HTTP_200_OK,
                code=AppCode.JOB_QUEUE_EMPTY,
                detail=GET_JOB_RESPONSES[AppCode.JOB_QUEUE_EMPTY]["detail"]
            )
        )

    raise RouteInvariantError(code=code, request=request)


GET_IMAGES_FOR_JOB_RESPONSES = {
    AppCode.IMAGES_RETRIEVED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Images for the specified job have been retrieved successfully.",
        "model": DocAPIResponseOK[List[base_objects.Image]],
        "detail": "Images for Job retrieved successfully.",
    },
}
@worker_router.get(
    "/images/{job_id}",
    summary="Get Images for Job",
    response_model=List[base_objects.Image],
    tags=["Worker"],
    description="Retrieve all images associated with a specific job.",
    responses=make_responses(GET_IMAGES_FOR_JOB_RESPONSES))
@uses_challenge_worker_access_to_job
async def get_images_for_job(
        request: Request,
        job_id: UUID,
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_worker_access_to_job(db=db, key=key, job_id=job_id)

    # job already challenged above, so here we are sure it exists and in PROCESSING state
    db_images, code = await worker_cruds.get_job_images(db=db, job_id=job_id)
    if code == AppCode.IMAGES_RETRIEVED and db_images is not None:
        return DocAPIResponseOK[List[base_objects.Image]](
            status=fastapi.status.HTTP_200_OK,
            code=AppCode.IMAGES_RETRIEVED,
            detail=GET_IMAGES_FOR_JOB_RESPONSES[AppCode.IMAGES_RETRIEVED]["detail"],
            data=db_images
        )

    raise RouteInvariantError(code=code, request=request)


GET_META_JSON_FOR_JOB_RESPONSES = {
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
@worker_router.get(
    "/meta_json/{job_id}",
    response_class=FileResponse,
    summary="Download Meta JSON",
    tags=["Worker"],
    description="Download the Meta JSON file associated with a specific job.",
    responses=make_responses(GET_META_JSON_FOR_JOB_RESPONSES))
@uses_challenge_worker_access_to_job
async def get_meta_json_for_job(
        request: Request,
        job_id: UUID,
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_worker_access_to_job(db=db, key=key, job_id=job_id)

    db_job, code = await job_cruds.get_job(db=db, job_id=job_id)

    if code == AppCode.JOB_RETRIEVED and db_job is not None and db_job.meta_json_uploaded:
        meta_json_path = os.path.join(config.JOBS_DIR, str(job_id), "meta.json")
        return FileResponse(meta_json_path, media_type="application/json", filename="meta.json")
    elif code == AppCode.JOB_RETRIEVED and db_job is not None and not db_job.meta_json_uploaded:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_409_CONFLICT,
            code=AppCode.META_JSON_NOT_UPLOADED,
            detail=GET_META_JSON_FOR_JOB_RESPONSES[AppCode.META_JSON_NOT_UPLOADED]["detail"],
        )

    raise RouteInvariantError(code=code, request=request)


GET_IMAGE_FOR_JOB_RESPONSES = {
    AppCode.IMAGE_DOWNLOADED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Binary data of the requested IMAGE file, format not guaranteed but usually JPEG/PNG.",
        "content_type": "image/jpeg",
        "example_value": "(binary image data)",
    },
    AppCode.IMAGE_NOT_UPLOADED: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": "The requested IMAGE file has not been uploaded yet.",
        "model": DocAPIResponseClientError,
        "detail": "IMAGE file has not been uploaded yet.",
    },
    AppCode.IMAGE_NOT_FOUND_FOR_JOB: {
        "status": fastapi.status.HTTP_404_NOT_FOUND,
        "description": "The specified image does not exist for the given job.",
        "model": DocAPIResponseClientError,
        "detail": DETAILS_GENERAL[AppCode.IMAGE_NOT_FOUND_FOR_JOB],
    }
}
@worker_router.get(
    "/image/{job_id}/{image_id}",
    response_class=FileResponse,
    summary="Download IMAGE",
    tags=["Worker"],
    description="Download the IMAGE file associated with a specific image of a job.",
    responses=make_responses(GET_IMAGE_FOR_JOB_RESPONSES))
@uses_challenge_worker_access_to_job
async def get_image_for_worker(
        request: Request,
        job_id: UUID, image_id: UUID,
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_worker_access_to_job(db=db, key=key, job_id=job_id)

    db_image, code = await worker_cruds.get_image_for_job(db=db, job_id=job_id, image_id=image_id)

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
    AppCode.IMAGE_NOT_FOUND_FOR_JOB: {
        "status": fastapi.status.HTTP_404_NOT_FOUND,
        "description": "The specified Image does not exist for the given job.",
        "model": DocAPIResponseClientError,
        "detail": DETAILS_GENERAL[AppCode.IMAGE_NOT_FOUND_FOR_JOB],
    }
}
@worker_router.get(
    "/alto/{job_id}/{image_id}",
    response_class=FileResponse,
    summary="Download ALTO",
    tags=["Worker"],
    description="Download the ALTO file associated with a specific image of a job.",
    responses=make_responses(GET_ALTO_FOR_JOB_RESPONSES))
@uses_challenge_worker_access_to_job
async def get_alto_for_job(
        request: Request,
        job_id: UUID,
        image_id: UUID,
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_worker_access_to_job(db=db, key=key, job_id=job_id)

    db_image, code = await worker_cruds.get_image_for_job(db=db, job_id=job_id, image_id=image_id)

    if code == AppCode.IMAGE_RETRIEVED and db_image is not None and db_image.alto_uploaded:
        alto_path = os.path.join(config.JOBS_DIR, str(db_image.job_id), f"{db_image.id}.xml")
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


POST_JOB_HEARTBEAT_RESPONSES = {
    AppCode.JOB_HEARTBEAT_ACCEPTED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "The job heartbeat has been accepted and the lease has been extended.",
        "model": DocAPIResponseOK[base_objects.JobLease],
        "detail": "Heartbeat for Job accepted, lease extended (UTC time).",
    }
}
@worker_router.post(
    "/job/{job_id}/heartbeat",
    response_model=base_objects.JobLease,
    summary="Send Job Heartbeat",
    tags=["Worker"],
    description="Confirm the worker is still processing the job and extend its lease time.",
    responses=make_responses(POST_JOB_HEARTBEAT_RESPONSES))
@uses_challenge_worker_access_to_job
async def post_job_heartbeat(
    request: Request,
    job_id: UUID,
    key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
    db: AsyncSession = Depends(get_async_session),
):
    await challenge_worker_access_to_job(db=db, key=key, job_id=job_id)

    code, lease_expire_at, server_time = await worker_cruds.update_processing_job_lease(db=db, job_id=job_id)

    if code == AppCode.JOB_HEARTBEAT_ACCEPTED:
        return DocAPIResponseOK[base_objects.JobLease](
            status=fastapi.status.HTTP_200_OK,
            code=AppCode.JOB_HEARTBEAT_ACCEPTED,
            detail=POST_JOB_HEARTBEAT_RESPONSES[AppCode.JOB_HEARTBEAT_ACCEPTED]["detail"],
            data=base_objects.JobLease(id=job_id, lease_expire_at=lease_expire_at, server_time=server_time),
        )

    raise RouteInvariantError(code=code, request=request)


UPDATE_JOB_RESPONSES = {
    AppCode.JOB_UPDATED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "The job has been updated successfully and the lease has been extended.",
        "model": DocAPIResponseOK[base_objects.JobLease],
        "detail": "Job has been updated successfully, lease extended (UTC time).",
    },
}
@worker_router.patch(
    "/job/{job_id}",
    response_model=base_objects.JobLease,
    summary="Update Job Progress",
    tags=["Worker"],
    description="Update the job's progress and extend its lease time.",
    responses=make_responses(UPDATE_JOB_RESPONSES))
@uses_challenge_worker_access_to_job
async def patch_job(
        request: Request,
        job_id: UUID,
        job_update: base_objects.JobUpdate,
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_worker_access_to_job(db=db, key=key, job_id=job_id)

    # job already challenged above, so here we are sure it exists and in PROCESSING state
    db_job, lease_expire_at, server_time, code = await worker_cruds.update_processing_job_progress(db=db, job_id=job_id, job_update=job_update)
    if code == AppCode.JOB_UPDATED:
        return DocAPIResponseOK[base_objects.JobLease](
            status=fastapi.status.HTTP_200_OK,
            code=AppCode.JOB_UPDATED,
            detail=UPDATE_JOB_RESPONSES[AppCode.JOB_UPDATED]["detail"],
            data=base_objects.JobLease(id=job_id, lease_expire_at=lease_expire_at, server_time=server_time)
        )

    raise RouteInvariantError(code=code, request=request)


POST_RESULT_FOR_JOB_RESPONSES = {
    AppCode.RESULT_ZIP_UPLOADED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "The result ZIP archive for the job has been uploaded successfully.",
        "model": DocAPIResponseOK[NoneType],
        "detail": "Result ZIP archive for Job uploaded successfully.",
    },
    AppCode.RESULT_ZIP_INVALID: {
        "status": fastapi.status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
        "description": "The uploaded file is not a valid ZIP archive.",
        "model": DocAPIResponseClientError,
        "detail": "The uploaded file is not a valid ZIP archive.",
    },
}
@worker_router.post(
    "/result/{job_id}",
    response_model=DocAPIResponseOK[NoneType],
    summary="Upload Job Result",
    tags=["Worker"],
    description="Upload the result ZIP archive for a specific job.",
    responses=make_responses(POST_RESULT_FOR_JOB_RESPONSES))
@uses_challenge_worker_access_to_job
async def post_result_for_job(
    job_id: UUID,
    result: UploadFile = File(...),
    key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
    db: AsyncSession = Depends(get_async_session),
):
    await challenge_worker_access_to_job(db=db, key=key, job_id=job_id)

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
            code=AppCode.RESULT_ZIP_INVALID,
            detail=POST_RESULT_FOR_JOB_RESPONSES[AppCode.RESULT_ZIP_INVALID]["detail"],
        )

    # Atomically move the validated file into place
    os.replace(tmp_path, final_path)

    return DocAPIResponseOK[NoneType](
        status=fastapi.status.HTTP_200_OK,
        code=AppCode.RESULT_ZIP_UPLOADED,
        detail=POST_RESULT_FOR_JOB_RESPONSES[AppCode.RESULT_ZIP_UPLOADED]["detail"]
    )


POST_JOB_COMPLETE_RESPONSES = {
    AppCode.JOB_COMPLETED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "The job has been marked as completed.",
        "model": DocAPIResponseOK[NoneType],
        "detail": "Job has been marked as completed.",
    },
    AppCode.JOB_ALREADY_COMPLETED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "The job was already marked as completed.",
        "model": DocAPIResponseOK[NoneType],
        "detail": "Job was already marked as completed.",
    },
    AppCode.RESULT_ZIP_MISSING: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": "The result ZIP for the job has not been uploaded yet.",
        "model": DocAPIResponseClientError,
        "detail": "Result ZIP for Job has not been uploaded yet.",
    },
}

@worker_router.post(
    "/jobs/{job_id}/complete",
    response_model=DocAPIResponseOK[NoneType],
    summary="Complete Job",
    tags=["Worker"],
    description="Mark a specific job as completed after all results have been uploaded.",
    responses= make_responses(POST_JOB_COMPLETE_RESPONSES))
@uses_challenge_worker_access_to_job
async def post_job_complete(
        request: Request,
        job_id: UUID,
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):
    # ensure idempotency by allowing FAILED state too
    await challenge_worker_access_to_job(db=db, key=key, job_id=job_id,
        allowed_states={base_objects.ProcessingState.PROCESSING, base_objects.ProcessingState.DONE}
    )

    result_path = os.path.join(config.RESULT_DIR, f"{job_id}.zip")
    if not await aiofiles_os.path.exists(result_path):
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_409_CONFLICT,
            code=AppCode.RESULT_ZIP_MISSING,
            detail=POST_JOB_COMPLETE_RESPONSES[AppCode.RESULT_ZIP_MISSING]["detail"],
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
        "model": DocAPIResponseOK[NoneType],
        "detail": "Job has been marked as failed.",
    },
    AppCode.JOB_ALREADY_FAILED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "The job was already marked as failed.",
        "model": DocAPIResponseOK[NoneType],
        "detail": "Job was already marked as failed.",
    },
}
@worker_router.post(
    "/jobs/{job_id}/fail",
    response_model=DocAPIResponseOK[NoneType],
    summary="Fail Job",
    tags=["Worker"],
    description="Mark a specific job as failed.",
    responses=make_responses(POST_JOB_FAIL_RESPONSES))
@uses_challenge_worker_access_to_job
async def post_job_fail(
    request: Request,
    job_id: UUID,
    key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
    db: AsyncSession = Depends(get_async_session),
):
    # ensure idempotency by allowing FAILED state too
    await challenge_worker_access_to_job(db=db, key=key, job_id=job_id,
        allowed_states={base_objects.ProcessingState.PROCESSING, base_objects.ProcessingState.FAILED}
    )

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



