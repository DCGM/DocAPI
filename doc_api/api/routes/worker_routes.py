import http
import logging
import os

import aiofiles
from fastapi import Depends, HTTPException, status, UploadFile, File
from fastapi.responses import FileResponse

from aiofiles import os as aiofiles_os

from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.authentication import require_api_key
from doc_api.api.cruds import cruds
from doc_api.api.database import get_async_session
from doc_api.api.routes.helper import render_msg, render_example, RouteInvariantError
from doc_api.api.routes.route_guards import challenge_worker_access_to_job
from doc_api.api.schemas import base_objects
from doc_api.api.schemas.responses import AppCode, DocAPIResponseOK, make_validated_ok
from doc_api.db import model
from doc_api.api.routes import worker_router
from doc_api.config import config

from typing import List, Literal, Tuple
from uuid import UUID

logger = logging.getLogger(__name__)


require_worker_key = require_api_key(key_role=base_objects.KeyRole.WORKER)

MESSAGES_GET_JOB = {
    AppCode.JOB_ASSIGNED:    "Job '{job_id}' assigned.",
    AppCode.JOB_QUEUE_EMPTY: "Queue is empty right now, try again shortly.",
}
@worker_router.get(
    "/job",
    response_model=DocAPIResponseOK[base_objects.Job],
    responses={
        200: {
            "description": f"Job assignment for workers.",
            "content": {
                "application/json": {
                    "examples": {
                        "example_0": {
                            "summary": AppCode.JOB_ASSIGNED.value,
                            "description": "A job has been assigned to the worker.",
                            "value": {
                                "status_code": 200,
                                "app_code": AppCode.JOB_ASSIGNED.value,
                                "message": render_example(MESSAGES_GET_JOB, AppCode.JOB_ASSIGNED),
                                "data": base_objects.model_example(base_objects.Job),
                            }},
                        "example_1": {
                            "summary": AppCode.JOB_QUEUE_EMPTY.value,
                            "description": "No jobs are currently available in the queue.",
                            "value": {
                                "status_code": 200,
                                "app_code": AppCode.JOB_QUEUE_EMPTY.value,
                                "message": render_example(MESSAGES_GET_JOB, AppCode.JOB_QUEUE_EMPTY)
                            }}
                    }}}}},
    tags=["Worker"])
async def get_job(
        key: model.Key = Depends(require_worker_key),
        db: AsyncSession = Depends(get_async_session)):
    db_job, app_code = await cruds.assign_job_to_worker(db, key.id)
    if app_code == AppCode.JOB_ASSIGNED:
        return DocAPIResponseOK[base_objects.Job](status_code=http.HTTPStatus.OK,
                                                  app_code=app_code,
                                                  message=render_msg(MESSAGES_GET_JOB, AppCode.JOB_ASSIGNED, job_id=str(db_job.id)),
                                                  data=db_job)
    elif app_code == AppCode.JOB_QUEUE_EMPTY:
        return make_validated_ok(status_code=http.HTTPStatus.OK,
                                 app_code=app_code,
                                 message=render_msg(MESSAGES_GET_JOB, AppCode.JOB_QUEUE_EMPTY))

    raise RouteInvariantError(f"Unexpected app_code '{app_code}' from assign_job_to_worker")


@worker_router.get("/images/{job_id}", response_model=List[base_objects.Image], tags=["Worker"])
async def get_images_for_job(job_id: UUID,
        key: model.Key = Depends(require_worker_key),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_worker_access_to_job(db, key, job_id)

    db_images = await cruds.get_images(db, job_id)
    return [base_objects.Image.model_validate(db_image) for db_image in db_images]


@worker_router.get("/image/{job_id}/{image_id}", response_class=FileResponse, tags=["Worker"])
async def get_image(job_id: UUID, image_id: UUID,
        key: model.Key = Depends(require_worker_key),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_worker_access_to_job(db, key, job_id)

    db_image = await cruds.get_image(db, image_id)
    if not db_image.image_uploaded:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "IMAGE_NOT_UPLOADED", "message": f"Image '{db_image.name}' (ID: {image_id}) is not uploaded"},
        )
    image_path = os.path.join(config.JOBS_DIR, str(db_image.job_id), f"{db_image.id}.jpg")
    return FileResponse(image_path, media_type="image/jpeg", filename=db_image.name)


@worker_router.get("/alto/{job_id}/{image_id}", response_class=FileResponse, tags=["Worker"])
async def get_alto(job_id: UUID, image_id: UUID,
        key: model.Key = Depends(require_worker_key),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_worker_access_to_job(db, key, job_id)

    db_image = await cruds.get_image(db, image_id)
    if not db_image.alto_uploaded:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
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
            status_code=status.HTTP_404_NOT_FOUND,
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
            status_code=status.HTTP_409_CONFLICT,
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
            status_code=status.HTTP_409_CONFLICT,
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
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "JOB_NOT_FINISHABLE", "message": f"Job '{job_id}' must be in '{base_objects.ProcessingState.PROCESSING.value}' state, current state: '{db_job.state.value}'"},
        )
    if state == base_objects.ProcessingState.DONE:
        result_path = os.path.join(config.RESULT_DIR, f"{job_id}.zip")
        if not await aiofiles_os.path.exists(result_path):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"code": "RESULT_NOT_FOUND", "message": f"Result file for job '{job_id}' not found at expected location: '{result_path}'"},
            )
    await cruds.finish_job(db, job_id, base_objects.ProcessingState.DONE)
    return {"code": "JOB_FINISHED", "message": f"Job '{job_id}' finished successfully"}