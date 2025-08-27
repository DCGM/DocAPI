import hashlib
import json
import logging
import os

from fastapi import Depends, HTTPException, status
from fastapi.responses import FileResponse

from aiofiles import os as aiofiles_os
from pydantic import ValidationError

from sqlalchemy.ext.asyncio import AsyncSession

from metakat.app.api.authentication import require_api_key
from metakat.app.api.cruds import cruds
from metakat.app.api.database import get_async_session
from metakat.app.api.schemas import base_objects
from metakat.app.db import model
from metakat.app.api.routes import worker_router
from metakat.app.config import config

from typing import List
from uuid import UUID

from schemas.base_objects import MetakatIO


logger = logging.getLogger(__name__)


require_worker_key = require_api_key(key_role=base_objects.KeyRole.WORKER)

@worker_router.get("/queued_jobs", response_model=List[base_objects.Job], tags=["Worker"])
async def get_queued_jobs(
        key: model.Key = Depends(require_worker_key),
        db: AsyncSession = Depends(get_async_session)):
    db_jobs = await cruds.get_queued_jobs(db)
    return [base_objects.Job.model_validate(db_job) for db_job in db_jobs]


@worker_router.get("/images/{job_id}", response_model=List[base_objects.Image], tags=["Worker"])
async def get_images_for_job(job_id: UUID,
        key: model.Key = Depends(require_worker_key),
        db: AsyncSession = Depends(get_async_session)):
    db_images = await cruds.get_images(db, job_id)
    return [base_objects.Image.model_validate(db_image) for db_image in db_images]


@worker_router.get("/image/{image_id}", response_class=FileResponse, tags=["Worker"])
async def get_image(image_id: UUID,
        key: model.Key = Depends(require_worker_key),
        db: AsyncSession = Depends(get_async_session)):
    db_image = await cruds.get_image(db, image_id)
    if not db_image.image_uploaded:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "IMAGE_NOT_UPLOADED", "message": f"Image '{db_image.name}' (ID: {image_id}) is not uploaded"},
        )
    image_path = os.path.join(config.BATCH_UPLOADED_DIR, str(db_image.job_id), f"{db_image.id}.jpg")
    return FileResponse(image_path, media_type="image/jpeg", filename=db_image.name)


@worker_router.get("/alto/{image_id}", response_class=FileResponse, tags=["Worker"])
async def get_alto(image_id: UUID,
        key: model.Key = Depends(require_worker_key),
        db: AsyncSession = Depends(get_async_session)):
    db_image = await cruds.get_image(db, image_id)
    if not db_image.alto_uploaded:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "ALTO_NOT_UPLOADED", "message": f"ALTO for image '{db_image.name}' (ID: {image_id}) is not uploaded"},
        )
    alto_path = os.path.join(config.BATCH_UPLOADED_DIR, str(db_image.job_id), f"{db_image.id}.xml")
    return FileResponse(alto_path, media_type="application/xml", filename=f"{os.path.splitext(db_image.name)[0]}.xml")


@worker_router.get("/proarc_json/{job_id}", response_class=FileResponse, tags=["Worker"])
async def get_proarc_json(job_id: UUID,
        key: model.Key = Depends(require_worker_key),
        db: AsyncSession = Depends(get_async_session)):
    db_job = await cruds.get_job(db, job_id)
    if not db_job.proarc_json_uploaded:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "PROARC_NOT_UPLOADED", "message": f"Proarc JSON for job '{job_id}' is not uploaded"},
        )
    proarc_json_path = os.path.join(config.BATCH_UPLOADED_DIR, str(job_id), "proarc.json")
    return FileResponse(proarc_json_path, media_type="application/json", filename="proarc.json")


@worker_router.put("/update_job/{job_id}", tags=["Worker"])
async def update_job(job_id: UUID,
        job_update: base_objects.JobUpdate,
        key: model.Key = Depends(require_worker_key),
        db: AsyncSession = Depends(get_async_session)):
    db_job = await cruds.get_job(db, job_id)
    if db_job.state not in {base_objects.ProcessingState.QUEUED, base_objects.ProcessingState.PROCESSING}:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "JOB_NOT_UPDATABLE", "message": f"Job '{job_id}' must be in '{base_objects.ProcessingState.QUEUED.value}' or '{base_objects.ProcessingState.PROCESSING.value}' state to be updated, current state: '{db_job.state.value}'"},
        )
    await cruds.update_job(db, job_update)
    return {"code": "JOB_UPDATED", "message": f"Job '{job_id}' updated successfully"}


@worker_router.post("/result/{job_id}", response_model=MetakatIO, tags=["Worker"])
async def upload_result(job_id: UUID,
        result: MetakatIO,
        key: model.Key = Depends(require_worker_key),
        db: AsyncSession = Depends(get_async_session)):
    db_job = await cruds.get_job(db, job_id)
    if db_job.state != base_objects.ProcessingState.PROCESSING:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "JOB_NOT_PROCESSING", "message": f"Job '{job_id}' must be in '{base_objects.ProcessingState.PROCESSING.value}' state to upload result, current state: '{db_job.state.value}'"},
        )
    result_path = os.path.join(config.RESULT_DIR, str(job_id))
    await aiofiles_os.makedirs(result_path, exist_ok=True)
    result_file_path = os.path.join(result_path, f"{job_id}.json")
    with open(result_file_path, "w", encoding="utf-8") as f:
        json.dump(result.model_dump(mode="json"), f, ensure_ascii=False, indent=4)
    return {"code": "RESULT_UPLOADED", "message": f"Result for job '{job_id}' uploaded successfully"}


@worker_router.post("/finish_job/{job_id}", tags=["Worker"])
async def finish_job(job_id: UUID,
        job_finish: base_objects.JobFinish,
        key: model.Key = Depends(require_worker_key),
        db: AsyncSession = Depends(get_async_session)):
    db_job = await cruds.get_job(db, job_id)
    if db_job.state != base_objects.ProcessingState.PROCESSING:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "JOB_NOT_FINISHABLE", "message": f"Job '{job_id}' must be in '{base_objects.ProcessingState.PROCESSING.value}' state, current state: '{db_job.state.value}'"},
        )
    result_path = os.path.join(config.RESULT_DIR, str(job_id), f"{job_id}.json")
    if not await aiofiles_os.path.exists(result_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "RESULT_NOT_FOUND", "message": f"Result file for job '{job_id}' not found at expected location: '{result_path}'"},
        )
    with open(result_path, "r", encoding="utf-8") as f:
        try:
            result_json = json.load(f)
            MetakatIO.model_validate(result_json)
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"code": "RESULT_INVALID_JSON", "message": f"Result file for job '{job_id}' is not valid JSON"},
            )
        except ValidationError:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"code": "RESULT_INVALID_SCHEMA", "message": f"Result file for job '{job_id}' does not conform to Metakat schema"},
            )
    await cruds.finish_job(db, job_finish)
    return {"code": "JOB_FINISHED", "message": f"Job '{job_id}' finished successfully"}