import logging
import os
from types import NoneType

import aiofiles
import fastapi
from fastapi import Depends, HTTPException, UploadFile, File, Request
from fastapi.responses import FileResponse

from aiofiles import os as aiofiles_os

from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.authentication import require_api_key
from doc_api.api.cruds import cruds
from doc_api.api.database import get_async_session
from doc_api.api.routes.helper import RouteInvariantError
from doc_api.api.routes.route_guards import challenge_worker_access_to_job
from doc_api.api.schemas import base_objects
from doc_api.api.schemas.base_objects import model_example, ProcessingState
from doc_api.api.schemas.responses import AppCode, DocAPIResponseOK, validate_no_data_ok_response, \
    DocAPIResponseClientError, DocAPIClientErrorException, DETAILS_GENERAL
from doc_api.db import model
from doc_api.api.routes import worker_router
from doc_api.config import config

from typing import List, Literal, Tuple, Optional, Any
from uuid import UUID

logger = logging.getLogger(__name__)


require_worker_key = require_api_key(key_role=base_objects.KeyRole.WORKER)

DETAILS_GET_JOB = {
    AppCode.JOB_ASSIGNED: "Job (id={job_id}) has been assigned to the worker.",
    AppCode.JOB_QUEUE_EMPTY: "Queue is empty right now, try again shortly.",
}
@worker_router.get(
    "/job",
    summary="Get Job",
    response_model=DocAPIResponseOK[base_objects.Job],
    tags=["Worker"],
    description=f"Assign a job the requesting worker.",
    responses={
        fastapi.status.HTTP_200_OK: {
            "content": {
                "application/json": {
                    "examples": {
                        "example_0": {
                            "summary": AppCode.JOB_ASSIGNED.value,
                            "description": "A job has been assigned to the worker.",
                            "value": DocAPIResponseOK[base_objects.Job](
                                status=fastapi.status.HTTP_200_OK,
                                code=AppCode.JOB_ASSIGNED,
                                detail=DETAILS_GET_JOB[AppCode.JOB_ASSIGNED],
                                data=model_example(base_objects.Job)
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
        key: model.Key = Depends(require_worker_key),
        db: AsyncSession = Depends(get_async_session)):
    db_job, code = await cruds.assign_job_to_worker(db=db, worker_key_id=key.id)
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
    AppCode.JOB_IMAGES_RETRIEVED: "Images for Job (id={job_id}) retrieved successfully.",
}
@worker_router.get(
    "/images/{job_id}",
    summary="Get Images for Job",
    response_model=List[base_objects.Image],
    tags=["Worker"],
    description="Retrieve all images associated with a specific job.",
    responses={
        fastapi.status.HTTP_200_OK: {
            "content": {
                "application/json": {
                    "examples": {
                        "example_0": {
                            "summary": AppCode.JOB_IMAGES_RETRIEVED.value,
                            "description": "Images for the specified job have been retrieved successfully.",
                            "value": DocAPIResponseOK[List[base_objects.Image]](
                                status=fastapi.status.HTTP_200_OK,
                                code=AppCode.JOB_IMAGES_RETRIEVED,
                                detail=DETAILS_GET_IMAGES_FOR_JOB[AppCode.JOB_IMAGES_RETRIEVED],
                                data=model_example(List[base_objects.Image])
                            ).model_dump(mode="json")},
                    }}}},
        fastapi.status.HTTP_404_NOT_FOUND: {
            "model": DocAPIResponseClientError,
            "content": {
                "application/json": {
                    "examples": {
                        "example_0": {
                            "summary": AppCode.JOB_NOT_FOUND.value,
                            "description": "The specified job does not exist.",
                            "value": DocAPIResponseClientError(
                                status=fastapi.status.HTTP_404_NOT_FOUND,
                                code=AppCode.JOB_NOT_FOUND,
                                detail=DETAILS_GENERAL[AppCode.JOB_NOT_FOUND]
                            ).model_dump(mode="json")}}}}}})
async def get_images_for_job(
        request: Request,
        job_id: UUID,
        key: model.Key = Depends(require_worker_key),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_worker_access_to_job(db, key, job_id)

    db_images, code = await cruds.get_job_images(db=db, job_id=job_id)
    if code == AppCode.JOB_IMAGES_RETRIEVED and db_images is not None:
        return DocAPIResponseOK[List[base_objects.Image]](
            status=fastapi.status.HTTP_200_OK,
            code=AppCode.JOB_IMAGES_RETRIEVED,
            detail=DETAILS_GET_IMAGES_FOR_JOB[AppCode.JOB_IMAGES_RETRIEVED].format(job_id=job_id),
            data=db_images
        )
    elif code == AppCode.JOB_NOT_FOUND:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_404_NOT_FOUND,
            code=AppCode.JOB_NOT_FOUND,
            detail=DETAILS_GENERAL[AppCode.JOB_NOT_FOUND].format(job_id=job_id),
        )

    raise RouteInvariantError(code=code, request=request)

DETAILS_GET_IMAGE_FOR_JOB = {
    AppCode.JOB_IMAGE_RETRIEVED: "Image (id={image_id}) for Job (id={job_id}) retrieved successfully",
    AppCode.JOB_IMAGE_NOT_UPLOADED: "Image (id={image_id}, name='{image_name}') for Job (id={job_id}) has not been uploaded yet."
}
@worker_router.get(
    "/image/{job_id}/{image_id}",
    response_class=FileResponse,
    summary="Get Image for Job",
    tags=["Worker"],
    description="Retrieve a specific image associated with a job.",
    responses={
        fastapi.status.HTTP_200_OK: {
            "content": {
                "image/jpeg": {
                    "examples": {
                        "example_0": {
                            "summary": AppCode.JOB_IMAGE_RETRIEVED.value,
                            "description": "The requested image has been retrieved successfully.",
                            "value": "Binary image data here"
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
                        "example_0": {
                            "summary": AppCode.JOB_IMAGE_NOT_UPLOADED.value,
                            "description": "The requested image has not been uploaded yet.",
                            "value": DocAPIResponseClientError(
                                status=fastapi.status.HTTP_409_CONFLICT,
                                code=AppCode.JOB_IMAGE_NOT_UPLOADED,
                                detail=DETAILS_GET_IMAGE_FOR_JOB[AppCode.JOB_IMAGE_NOT_UPLOADED]
                            ).model_dump(mode="json")}}}}},
        fastapi.status.HTTP_404_NOT_FOUND: {
            "model": DocAPIResponseClientError,
            "content": {
                "application/json": {
                    "examples": {
                        "example_0": {
                            "summary": AppCode.JOB_IMAGE_NOT_FOUND.value,
                            "description": "The specified image does not exist for the given job.",
                            "value": DocAPIResponseClientError(
                                status=fastapi.status.HTTP_404_NOT_FOUND,
                                code=AppCode.JOB_IMAGE_NOT_FOUND,
                                detail=DETAILS_GENERAL[AppCode.JOB_IMAGE_NOT_FOUND]
                            ).model_dump(mode="json")}}}}}})
async def get_image_for_job(
        request: Request,
        job_id: UUID, image_id: UUID,
        key: model.Key = Depends(require_worker_key),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_worker_access_to_job(db, key, job_id)

    db_image, code = await cruds.get_image_for_job(db=db, job_id=job_id, image_id=image_id)

    if code == AppCode.JOB_IMAGE_RETRIEVED and db_image is not None and db_image.image_uploaded:
        image_path = os.path.join(config.JOBS_DIR, str(db_image.job_id), f"{db_image.id}.jpg")
        return FileResponse(image_path, media_type="image/jpeg", filename=db_image.name)
    elif code == AppCode.JOB_IMAGE_RETRIEVED and db_image is not None and not db_image.image_uploaded:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_409_CONFLICT,
            code=AppCode.JOB_IMAGE_NOT_UPLOADED,
            detail=DETAILS_GET_IMAGE_FOR_JOB[AppCode.JOB_IMAGE_NOT_UPLOADED].format(image_id=db_image.id,
                                                                                    image_name=db_image.name,
                                                                                    job_id=job_id),
        )
    elif code == AppCode.JOB_IMAGE_NOT_FOUND:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_404_NOT_FOUND,
            code=AppCode.JOB_IMAGE_NOT_FOUND,
            detail=DETAILS_GENERAL[AppCode.JOB_IMAGE_NOT_FOUND].format(image_id=image_id, job_id=job_id),
        )

    raise RouteInvariantError(code=code, request=request)

@worker_router.get("/alto/{job_id}/{image_id}", response_class=FileResponse, tags=["Worker"])
async def get_alto(job_id: UUID, image_id: UUID,
        key: model.Key = Depends(require_worker_key),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_worker_access_to_job(db, key, job_id)

    db_image = await cruds.get_image_for_job(db, job_id, image_id)
    if not db_image.alto_uploaded:
        raise HTTPException(
            status_code=fastapi.status.HTTP_404_NOT_FOUND,
            detail={"code": "ALTO_NOT_UPLOADED", "message": f"ALTO for image '{db_image.name}' (ID: {image_id}) is not uploaded"},
        )
    alto_path = os.path.join(config.JOBS_DIR, str(db_image.job_id), f"{db_image.id}.xml")
    return FileResponse(alto_path, media_type="application/xml", filename=f"{os.path.splitext(db_image.name)[0]}.xml")


@worker_router.get("/meta_json/{job_id}", response_class=FileResponse, tags=["Worker"])
async def get_meta_json(job_id: UUID,
        key: model.Key = Depends(require_worker_key),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_worker_access_to_job(db, key, job_id)

    db_job = await cruds.get_job(db, job_id)
    if not db_job.meta_json_uploaded:
        raise HTTPException(
            status_code=fastapi.status.HTTP_404_NOT_FOUND,
            detail={"code": "META_JSON_NOT_UPLOADED", "message": f"Meta JSON for job '{job_id}' is not uploaded"},
        )
    meta_json_path = os.path.join(config.JOBS_DIR, str(job_id), "meta.json")
    return FileResponse(meta_json_path, media_type="application/json", filename="meta.json")


@worker_router.put("/update_job/{job_id}", tags=["Worker"])
async def update_job(job_id: UUID,
        job_update: base_objects.JobUpdate,
        key: model.Key = Depends(require_worker_key),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_worker_access_to_job(db, key, job_id)

    db_job = await cruds.get_job(db, job_id)
    if db_job.state not in {base_objects.ProcessingState.QUEUED, base_objects.ProcessingState.PROCESSING}:
        raise HTTPException(
            status_code=fastapi.status.HTTP_409_CONFLICT,
            detail={"code": "JOB_NOT_UPDATABLE", "message": f"Job '{job_id}' must be in '{base_objects.ProcessingState.QUEUED.value}' or '{base_objects.ProcessingState.PROCESSING.value}' state to be updated, current state: '{db_job.state.value}'"},
        )
    await cruds.update_job(db, job_update, key.id)
    return {"code": "JOB_UPDATED", "message": f"Job '{job_id}' updated successfully"}


@worker_router.post("/result/{job_id}", tags=["Worker"])
async def upload_result(
    job_id: UUID,
    result: UploadFile = File(...),
    key: model.Key = Depends(require_worker_key),
    db: AsyncSession = Depends(get_async_session)):
    await challenge_worker_access_to_job(db, key, job_id)

    db_job = await cruds.get_job(db, job_id)
    if db_job.state != base_objects.ProcessingState.PROCESSING:
        raise HTTPException(
            status_code=fastapi.status.HTTP_409_CONFLICT,
            detail={
                "code": "JOB_NOT_PROCESSING",
                "message": (
                    f"Job '{job_id}' must be in "
                    f"'{base_objects.ProcessingState.PROCESSING.value}' state to upload result, "
                    f"current state: '{db_job.state.value}'"
                ),
            },
        )

    await aiofiles_os.makedirs(config.RESULT_DIR, exist_ok=True)
    result_file_path = os.path.join(config.RESULT_DIR, f"{job_id}.zip")

    async with aiofiles.open(result_file_path, "wb") as f:
        while chunk := await result.read(1024 * 1024):  # 1MB chunks
            await f.write(chunk)

    return {"code": "RESULT_UPLOADED", "message": f"Result for job '{job_id}' uploaded successfully"}


@worker_router.post("/finish_job/{job_id}/{state}", tags=["Worker"])
async def finish_job(job_id: UUID,
        state: Literal[base_objects.ProcessingState.DONE, base_objects.ProcessingState.ERROR],
        key: model.Key = Depends(require_worker_key),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_worker_access_to_job(db, key, job_id)

    db_job = await cruds.get_job(db, job_id)
    if db_job.state != base_objects.ProcessingState.PROCESSING:
        raise HTTPException(
            status_code=fastapi.status.HTTP_409_CONFLICT,
            detail={"code": "JOB_NOT_FINISHABLE", "message": f"Job '{job_id}' must be in '{base_objects.ProcessingState.PROCESSING.value}' state, current state: '{db_job.state.value}'"},
        )
    if state == base_objects.ProcessingState.DONE:
        result_path = os.path.join(config.RESULT_DIR, f"{job_id}.zip")
        if not await aiofiles_os.path.exists(result_path):
            raise HTTPException(
                status_code=fastapi.status.HTTP_404_NOT_FOUND,
                detail={"code": "RESULT_NOT_FOUND", "message": f"Result file for job '{job_id}' not found at expected location: '{result_path}'"},
            )
    await cruds.finish_job(db, job_id, base_objects.ProcessingState.DONE)
    return {"code": "JOB_FINISHED", "message": f"Job '{job_id}' finished successfully"}