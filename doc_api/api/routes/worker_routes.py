import logging
import os
import zipfile
from types import NoneType

import aiofiles
import fastapi
from fastapi import Depends, HTTPException, UploadFile, File, Request
from fastapi.responses import FileResponse

from aiofiles import os as aiofiles_os

from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.authentication import require_api_key
from doc_api.api.cruds import worker_cruds, job_cruds
from doc_api.api.database import get_async_session
from doc_api.api.routes.helper import RouteInvariantError
from doc_api.api.routes.route_guards import challenge_worker_access_to_job, WORKER_ACCESS_TO_JOB_GUARD_EXAMPLES
from doc_api.api.schemas import base_objects
from doc_api.api.schemas.base_objects import model_example
from doc_api.api.schemas.responses import AppCode, DocAPIResponseOK, validate_no_data_ok_response, \
    DocAPIResponseClientError, DocAPIClientErrorException, DETAILS_GENERAL
from doc_api.db import model
from doc_api.api.routes import worker_router
from doc_api.config import config

from typing import List, Literal
from uuid import UUID

logger = logging.getLogger(__name__)


DETAILS_GET_JOB = {
    AppCode.JOB_ASSIGNED: "Job (id={job_id}) has been assigned to the worker, lease established (UTC time).",
    AppCode.JOB_QUEUE_EMPTY: "Queue is empty right now, try again shortly.",
}
@worker_router.get(
    "/job",
    summary="Assign Job",
    response_model=DocAPIResponseOK[base_objects.JobLease],
    tags=["Worker"],
    description=f"Assign a job to the requesting worker. "
                f"If a job is available in the queue, it will be assigned to the worker and a lease will be established. "
                f"If no jobs are available, a response indicating the empty queue will be returned.",
    responses={
        fastapi.status.HTTP_200_OK: {
            "content": {
                "application/json": {
                    "examples": {
                        "example_0": {
                            "summary": AppCode.JOB_ASSIGNED.value,
                            "description": "A job has been successfully assigned to the worker.",
                            "value": DocAPIResponseOK[base_objects.JobLease](
                                status=fastapi.status.HTTP_200_OK,
                                code=AppCode.JOB_ASSIGNED,
                                detail=DETAILS_GET_JOB[AppCode.JOB_ASSIGNED],
                                data=model_example(base_objects.JobLease)
                            ).model_dump(mode="json")},
                        "example_1": {
                            "summary": AppCode.JOB_QUEUE_EMPTY.value,
                            "description": "No jobs are currently available in the queue.",
                            "value": DocAPIResponseOK[NoneType](
                                status=fastapi.status.HTTP_200_OK,
                                code=AppCode.JOB_QUEUE_EMPTY,
                                detail=DETAILS_GET_JOB[AppCode.JOB_QUEUE_EMPTY]
                            ).model_dump(mode="json")}}}}}})
async def get_job(
        request: Request,
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):
    db_job, code = await worker_cruds.assign_job_to_worker(db=db, worker_key_id=key.id)
    if code == AppCode.JOB_ASSIGNED and db_job is not None:
        return DocAPIResponseOK[base_objects.Job](
            status=fastapi.status.HTTP_200_OK,
            code=AppCode.JOB_ASSIGNED,
            detail=DETAILS_GET_JOB[AppCode.JOB_ASSIGNED].format(job_id=db_job.id),
            data=db_job
        )
    elif code == AppCode.JOB_QUEUE_EMPTY:
        return validate_no_data_ok_response(
            DocAPIResponseOK[NoneType](
                status=fastapi.status.HTTP_200_OK,
                code=AppCode.JOB_QUEUE_EMPTY,
                detail=DETAILS_GET_JOB[AppCode.JOB_QUEUE_EMPTY])
        )

    raise RouteInvariantError(code=code, request=request)


DETAILS_GET_IMAGES_FOR_JOB = {
    AppCode.IMAGES_RETRIEVED: "Images for Job (id={job_id}) retrieved successfully.",
}
@worker_router.get(
    "/images/{job_id}",
    summary="Get Images for Job",
    response_model=List[base_objects.Image],
    tags=["Worker"],
    description="Retrieve all images associated with a specific job.",
    responses={
        **WORKER_ACCESS_TO_JOB_GUARD_EXAMPLES,
        fastapi.status.HTTP_200_OK: {
            "content": {
                "application/json": {
                    "examples": {
                        AppCode.IMAGES_RETRIEVED.value: {
                            "summary": AppCode.IMAGES_RETRIEVED.value,
                            "description": "Images for the specified job have been retrieved successfully.",
                            "value": DocAPIResponseOK[List[base_objects.Image]](
                                status=fastapi.status.HTTP_200_OK,
                                code=AppCode.IMAGES_RETRIEVED,
                                detail=DETAILS_GET_IMAGES_FOR_JOB[AppCode.IMAGES_RETRIEVED],
                                data=model_example(List[base_objects.Image])
                            ).model_dump(mode="json")},
                    }}}}})
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
            detail=DETAILS_GET_IMAGES_FOR_JOB[AppCode.IMAGES_RETRIEVED].format(job_id=job_id),
            data=db_images
        )

    raise RouteInvariantError(code=code, request=request)


DETAILS_GET_META_JSON_FOR_JOB = {
    AppCode.META_JSON_NOT_UPLOADED: "Meta JSON file for Job (id={job_id}) has not been uploaded yet."
}
@worker_router.get(
    "/meta_json/{job_id}",
    response_class=FileResponse,
    summary="Download Meta JSON",
    tags=["Worker"],
    description="Download the Meta JSON file associated with a specific job.",
    responses={
        **WORKER_ACCESS_TO_JOB_GUARD_EXAMPLES,
        fastapi.status.HTTP_200_OK: {
            "content": {
                "application/json": {
                    "examples": {
                        AppCode.META_JSON_DOWNLOADED.value: {
                            "summary": AppCode.META_JSON_DOWNLOADED.value,
                            "description": "JSON data of the requested Meta JSON file.",
                            "value": {"meta_key": "meta_value"}
                        }
                    }
                }
            }
        },
        fastapi.status.HTTP_409_CONFLICT: {
            "model": DocAPIResponseClientError,
            "content": {
                "application/json": {
                    "examples": {
                        AppCode.META_JSON_NOT_UPLOADED.value: {
                            "summary": AppCode.META_JSON_NOT_UPLOADED.value,
                            "description": "The requested Meta JSON file has not been uploaded yet.",
                            "value": DocAPIResponseClientError(
                                status=fastapi.status.HTTP_409_CONFLICT,
                                code=AppCode.META_JSON_NOT_UPLOADED,
                                detail=DETAILS_GET_META_JSON_FOR_JOB[AppCode.META_JSON_NOT_UPLOADED]
                            ).model_dump(mode="json")}}}}}})
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
            detail=DETAILS_GENERAL[AppCode.META_JSON_NOT_UPLOADED].format(job_id=job_id),
        )

    raise RouteInvariantError(code=code, request=request)


DETAILS_GET_IMAGE_FOR_JOB = {
    AppCode.IMAGE_NOT_UPLOADED: "IMAGE file for Image (id={image_id}, name={image_name}) and Job (id={job_id}) has not been uploaded yet."
}
@worker_router.get(
    "/image/{job_id}/{image_id}",
    response_class=FileResponse,
    summary="Download IMAGE",
    tags=["Worker"],
    description="Download the IMAGE file associated with a specific image of a job.",
    responses={
        **WORKER_ACCESS_TO_JOB_GUARD_EXAMPLES,
        fastapi.status.HTTP_200_OK: {
            "content": {
                "image/jpeg": {
                    "examples": {
                        AppCode.IMAGE_DOWNLOADED.value: {
                            "summary": AppCode.IMAGE_DOWNLOADED.value,
                            "description": "Binary data of the requested IMAGE file, format not guaranteed but usually JPEG/PNG.",
                            "value": "(binary image data)"}}}}},
        fastapi.status.HTTP_409_CONFLICT: {
            "model": DocAPIResponseClientError,
            "content": {
                "application/json": {
                    "examples": {
                        AppCode.IMAGE_NOT_UPLOADED.value: {
                            "summary": AppCode.IMAGE_NOT_UPLOADED.value,
                            "description": "The requested IMAGE file has not been uploaded yet.",
                            "value": DocAPIResponseClientError(
                                status=fastapi.status.HTTP_409_CONFLICT,
                                code=AppCode.IMAGE_NOT_UPLOADED,
                                detail=DETAILS_GET_IMAGE_FOR_JOB[AppCode.IMAGE_NOT_UPLOADED]
                            ).model_dump(mode="json")}}}}},
        fastapi.status.HTTP_404_NOT_FOUND: {
            "model": DocAPIResponseClientError,
            "content": {
                "application/json": {
                    "examples": {
                        AppCode.IMAGE_NOT_FOUND_FOR_JOB.value: {
                            "summary": AppCode.IMAGE_NOT_FOUND_FOR_JOB.value,
                            "description": "The specified Image does not exist for the given job.",
                            "value": DocAPIResponseClientError(
                                status=fastapi.status.HTTP_404_NOT_FOUND,
                                code=AppCode.IMAGE_NOT_FOUND_FOR_JOB,
                                detail=DETAILS_GENERAL[AppCode.IMAGE_NOT_FOUND_FOR_JOB]
                            ).model_dump(mode="json")}}}}}})
async def get_image_for_job(
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
            detail=DETAILS_GET_IMAGE_FOR_JOB[AppCode.IMAGE_NOT_UPLOADED].format(image_id=db_image.id,
                                                                                image_name=db_image.name,
                                                                                job_id=job_id),
        )
    elif code == AppCode.IMAGE_NOT_FOUND_FOR_JOB:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_404_NOT_FOUND,
            code=AppCode.IMAGE_NOT_FOUND_FOR_JOB,
            detail=DETAILS_GENERAL[AppCode.IMAGE_NOT_FOUND_FOR_JOB].format(image_id=image_id, job_id=job_id),
        )

    raise RouteInvariantError(code=code, request=request)


DETAILS_GET_ALTO_FOR_JOB = {
    AppCode.ALTO_NOT_UPLOADED: "ALTO file for Image (id={image_id}, name={image_name}) and Job (id={job_id}) has not been uploaded yet."
}
@worker_router.get(
    "/alto/{job_id}/{image_id}",
    response_class=FileResponse,
    summary="Download ALTO",
    tags=["Worker"],
    description="Download the ALTO file associated with a specific image of a job.",
    responses={
        **WORKER_ACCESS_TO_JOB_GUARD_EXAMPLES,
        fastapi.status.HTTP_200_OK: {
            "content": {
                "application/xml": {
                    "examples": {
                        AppCode.ALTO_DOWNLOADED.value: {
                            "summary": AppCode.ALTO_DOWNLOADED.value,
                            "description": "XML data of the requested ALTO file.",
                            "value": "<alto>...</alto>"
                        }
                    }
                }
            }
        },
        fastapi.status.HTTP_409_CONFLICT: {
            "model": DocAPIResponseClientError,
            "content": {
                "application/json": {
                    "examples": {
                        AppCode.ALTO_NOT_UPLOADED.value: {
                            "summary": AppCode.ALTO_NOT_UPLOADED.value,
                            "description": "The requested ALTO file has not been uploaded yet.",
                            "value": DocAPIResponseClientError(
                                status=fastapi.status.HTTP_409_CONFLICT,
                                code=AppCode.ALTO_NOT_UPLOADED,
                                detail=DETAILS_GET_ALTO_FOR_JOB[AppCode.ALTO_NOT_UPLOADED]
                            ).model_dump(mode="json")}}}}},
        fastapi.status.HTTP_404_NOT_FOUND: {
            "model": DocAPIResponseClientError,
            "content": {
                "application/json": {
                    "examples": {
                        AppCode.IMAGE_NOT_FOUND_FOR_JOB.value: {
                            "summary": AppCode.IMAGE_NOT_FOUND_FOR_JOB.value,
                            "description": "The specified Image does not exist for the given job.",
                            "value": DocAPIResponseClientError(
                                status=fastapi.status.HTTP_404_NOT_FOUND,
                                code=AppCode.IMAGE_NOT_FOUND_FOR_JOB,
                                detail=DETAILS_GENERAL[AppCode.IMAGE_NOT_FOUND_FOR_JOB]
                            ).model_dump(mode="json")}}}}}})
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
            detail=DETAILS_GET_IMAGE_FOR_JOB[AppCode.ALTO_NOT_UPLOADED].format(image_id=db_image.id,
                                                                               image_name=db_image.name,
                                                                               job_id=job_id),
        )
    elif code == AppCode.IMAGE_NOT_FOUND_FOR_JOB:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_404_NOT_FOUND,
            code=AppCode.IMAGE_NOT_FOUND_FOR_JOB,
            detail=DETAILS_GENERAL[AppCode.IMAGE_NOT_FOUND_FOR_JOB].format(image_id=image_id, job_id=job_id),
        )

    raise RouteInvariantError(code=code, request=request)


DETAILS_HEARTBEAT_JOB = {
    AppCode.JOB_HEARTBEAT_ACCEPTED: "Heartbeat for Job (id={job_id}) accepted, lease extended (UTC time)."
}
@worker_router.post(
    "/job/{job_id}/heartbeat",
    response_model=base_objects.JobLease,
    summary="Send Job Heartbeat",
    tags=["Worker"],
    description="Confirm the worker is still processing the job and extend its lease time.",
    responses={
        **WORKER_ACCESS_TO_JOB_GUARD_EXAMPLES,
        fastapi.status.HTTP_200_OK: {
            "content": {
                "application/json": {
                    "examples": {
                        AppCode.JOB_HEARTBEAT_ACCEPTED.value: {
                            "summary": AppCode.JOB_HEARTBEAT_ACCEPTED.value,
                            "description": "The job heartbeat has been accepted and the lease has been extended.",
                            "value": DocAPIResponseOK[base_objects.JobLease](
                                status=fastapi.status.HTTP_200_OK,
                                code=AppCode.JOB_HEARTBEAT_ACCEPTED,
                                detail=DETAILS_HEARTBEAT_JOB[AppCode.JOB_HEARTBEAT_ACCEPTED],
                                data=model_example(base_objects.JobLease)
                            ).model_dump(mode="json")}}}}}})
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
            detail=DETAILS_HEARTBEAT_JOB[AppCode.JOB_HEARTBEAT_ACCEPTED].format(job_id=job_id),
            data=base_objects.JobLease(id=job_id, lease_expire_at=lease_expire_at, server_time=server_time),
        )

    raise RouteInvariantError(code=code, request=request)


DETAILS_UPDATE_JOB = {
    AppCode.JOB_UPDATED: "Job (id={job_id}) has been updated successfully, lease extended (UTC time)."
}
@worker_router.patch(
    "/job/{job_id}",
    response_model=base_objects.JobLease,
    summary="Update Job Progress",
    tags=["Worker"],
    description="Update the job's progress and extend its lease time.",
    responses={
        **WORKER_ACCESS_TO_JOB_GUARD_EXAMPLES,
        fastapi.status.HTTP_200_OK: {
            "content": {
                "application/json": {
                    "examples": {
                        AppCode.JOB_UPDATED.value: {
                            "summary": AppCode.JOB_UPDATED.value,
                            "description": "The job has been updated successfully and the lease has been extended.",
                            "value": DocAPIResponseOK[base_objects.JobLease](
                                status=fastapi.status.HTTP_200_OK,
                                code=AppCode.JOB_UPDATED,
                                detail=DETAILS_UPDATE_JOB[AppCode.JOB_UPDATED],
                                data=model_example(base_objects.JobLease)
                            ).model_dump(mode="json")},
                    }}}}})
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
            detail=DETAILS_UPDATE_JOB[AppCode.JOB_UPDATED].format(job_id=job_id),
            data=base_objects.JobLease(id=job_id, lease_expire_at=lease_expire_at, server_time=server_time)
        )

    raise RouteInvariantError(code=code, request=request)


DETAILS_POST_RESULT = {
    AppCode.RESULT_ZIP_INVALID: "The uploaded file is not a valid ZIP archive.",
    AppCode.RESULT_ZIP_UPLOADED: "Result ZIP archive for Job (id={job_id}) uploaded successfully.",
}
@worker_router.post(
    "/result/{job_id}",
    response_model=DocAPIResponseOK[NoneType],
    summary="Upload Job Result",
    tags=["Worker"],
    description="Upload the result ZIP archive for a specific job.",
    responses={
        **WORKER_ACCESS_TO_JOB_GUARD_EXAMPLES,
        fastapi.status.HTTP_200_OK: {
            "content": {
                "application/json": {
                    "examples": {
                        AppCode.RESULT_ZIP_UPLOADED.value: {
                            "summary": AppCode.RESULT_ZIP_UPLOADED.value,
                            "description": "The result ZIP archive for the job has been uploaded successfully.",
                            "value": DocAPIResponseOK[NoneType](
                                status=fastapi.status.HTTP_200_OK,
                                code=AppCode.RESULT_ZIP_UPLOADED,
                                detail=DETAILS_POST_RESULT[AppCode.RESULT_ZIP_UPLOADED],
                                data=None
                            ).model_dump(mode="json")}}}}},
        fastapi.status.HTTP_415_UNSUPPORTED_MEDIA_TYPE: {
            "model": DocAPIResponseClientError,
            "content": {
                "application/json": {
                    "examples": {
                        AppCode.RESULT_ZIP_INVALID.value: {
                            "summary": AppCode.RESULT_ZIP_INVALID.value,
                            "description": "The uploaded file is not a valid ZIP archive.",
                            "value": DocAPIResponseClientError(
                                status=fastapi.status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                                code=AppCode.RESULT_ZIP_INVALID,
                                detail=DETAILS_POST_RESULT[AppCode.RESULT_ZIP_INVALID]
                            ).model_dump(mode="json")},
                    }}}}})
async def post_result(
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
            detail=DETAILS_POST_RESULT[AppCode.RESULT_ZIP_INVALID],
        )

    # Atomically move the validated file into place
    os.replace(tmp_path, final_path)

    return DocAPIResponseOK[NoneType](
        status=fastapi.status.HTTP_200_OK,
        code=AppCode.RESULT_ZIP_UPLOADED,
        detail=DETAILS_POST_RESULT[AppCode.RESULT_ZIP_UPLOADED].format(job_id=job_id),
    )


DETAILS_POST_JOB_COMPLETE = {
    AppCode.RESULT_ZIP_MISSING: "Result ZIP for Job (id={job_id}) has not been uploaded yet.",
    AppCode.JOB_COMPLETED: "Job (id={job_id}) has been marked as completed.",
    AppCode.JOB_ALREADY_COMPLETED: "Job (id={job_id}) was already marked as completed.",
}
@worker_router.post(
    "/jobs/{job_id}/complete",
    response_model=DocAPIResponseOK[NoneType],
    summary="Complete Job",
    tags=["Worker"],
    description="Mark a specific job as completed after all results have been uploaded.",
    responses={
        **WORKER_ACCESS_TO_JOB_GUARD_EXAMPLES,
        fastapi.status.HTTP_200_OK: {
            "content": {
                "application/json": {
                    "examples": {
                        AppCode.JOB_COMPLETED.value: {
                            "summary": AppCode.JOB_COMPLETED.value,
                            "description": "The job has been marked as completed.",
                            "value": DocAPIResponseOK[NoneType](
                                status=fastapi.status.HTTP_200_OK,
                                code=AppCode.JOB_COMPLETED,
                                detail=DETAILS_POST_JOB_COMPLETE[AppCode.JOB_COMPLETED]
                            ).model_dump(mode="json")},
                        AppCode.JOB_ALREADY_COMPLETED.value: {
                            "summary": AppCode.JOB_ALREADY_COMPLETED.value,
                            "description": "The job was already marked as completed.",
                            "value": DocAPIResponseOK[NoneType](
                                status=fastapi.status.HTTP_200_OK,
                                code=AppCode.JOB_ALREADY_COMPLETED,
                                detail=DETAILS_POST_JOB_COMPLETE[AppCode.JOB_ALREADY_COMPLETED]
                            ).model_dump(mode="json")
                        }}}}},
        fastapi.status.HTTP_409_CONFLICT: {
            "model": DocAPIResponseClientError,
            "content": {
                "application/json": {
                    "examples": {
                        AppCode.RESULT_ZIP_MISSING.value: {
                            "summary": AppCode.RESULT_ZIP_MISSING.value,
                            "description": "The result ZIP for the job has not been uploaded yet.",
                            "value": DocAPIResponseClientError(
                                status=fastapi.status.HTTP_409_CONFLICT,
                                code=AppCode.RESULT_ZIP_MISSING,
                                detail=DETAILS_POST_JOB_COMPLETE[AppCode.RESULT_ZIP_MISSING]
                            ).model_dump(mode="json")}}}}}
    })
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
            detail=DETAILS_POST_JOB_COMPLETE[AppCode.RESULT_ZIP_MISSING].format(job_id=job_id),
        )
    code = await worker_cruds.complete_job(db=db, job_id=job_id)

    if code == AppCode.JOB_COMPLETED:
        return DocAPIResponseOK[NoneType](
            status=fastapi.status.HTTP_200_OK,
            code=AppCode.JOB_COMPLETED,
            detail=DETAILS_POST_JOB_COMPLETE[AppCode.JOB_COMPLETED].format(job_id=job_id)
        )
    elif code == AppCode.JOB_ALREADY_COMPLETED:
        return DocAPIResponseOK[NoneType](
            status=fastapi.status.HTTP_200_OK,
            code=AppCode.JOB_ALREADY_COMPLETED,
            detail=DETAILS_POST_JOB_COMPLETE[AppCode.JOB_ALREADY_COMPLETED].format(job_id=job_id)
        )

    raise RouteInvariantError(code=code, request=request)


DETAILS_POST_JOB_FAIL = {
    AppCode.JOB_FAILED: "Job (id={job_id}) has been marked as failed.",
    AppCode.JOB_ALREADY_FAILED: "Job (id={job_id}) was already marked as failed.",
}

@worker_router.post(
    "/jobs/{job_id}/fail",
    response_model=DocAPIResponseOK[NoneType],
    summary="Fail Job",
    tags=["Worker"],
    description="Mark a specific job as failed.",
    responses={
        **WORKER_ACCESS_TO_JOB_GUARD_EXAMPLES,
        fastapi.status.HTTP_200_OK: {
            "content": {
                "application/json": {
                    "examples": {
                        AppCode.JOB_FAILED.value: {
                            "summary": AppCode.JOB_FAILED.value,
                            "description": "The job has been marked as failed.",
                            "value": DocAPIResponseOK[NoneType](
                                status=fastapi.status.HTTP_200_OK,
                                code=AppCode.JOB_FAILED,
                                detail=DETAILS_POST_JOB_FAIL[AppCode.JOB_FAILED],
                            ).model_dump(mode="json")
                        },
                        AppCode.JOB_ALREADY_FAILED.value: {
                            "summary": AppCode.JOB_ALREADY_FAILED.value,
                            "description": "The job was already marked as failed.",
                            "value": DocAPIResponseOK[NoneType](
                                status=fastapi.status.HTTP_200_OK,
                                code=AppCode.JOB_ALREADY_FAILED,
                                detail=DETAILS_POST_JOB_FAIL[AppCode.JOB_ALREADY_FAILED],
                            ).model_dump(mode="json")}}}}}})
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
            detail=DETAILS_POST_JOB_FAIL[AppCode.JOB_FAILED].format(job_id=job_id),
        )
    elif code == AppCode.JOB_ALREADY_FAILED:
        return DocAPIResponseOK[NoneType](
            status=fastapi.status.HTTP_200_OK,
            code=AppCode.JOB_ALREADY_FAILED,
            detail=DETAILS_POST_JOB_FAIL[AppCode.JOB_ALREADY_FAILED].format(job_id=job_id),
        )

    raise RouteInvariantError(code=code, request=request)



