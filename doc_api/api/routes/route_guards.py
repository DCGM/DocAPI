from uuid import UUID

import fastapi
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException

from doc_api.api.schemas.base_objects import KeyRole
from doc_api.db import model


async def challenge_owner_access_to_job(db: AsyncSession, key: model.Key, job_id: UUID):
    if key.role == KeyRole.ADMIN:
        return
    job = await db.get(model.Job, job_id)
    if job is None:
        raise HTTPException(
            status_code=fastapi.status.HTTP_404_NOT_FOUND,
            detail={"code": "JOB_NOT_FOUND", "message": f"Job '{job_id}' does not exist"}
        )
    if job.owner_key_id != key.id:
        raise HTTPException(
            status_code=fastapi.status.HTTP_403_FORBIDDEN,
            detail={"code": "KEY_FORBIDDEN_FOR_JOB", "message": f"Key '{key.id}' does not have access to the job"}
        )

async def challenge_worker_access_to_job(db: AsyncSession, key: model.Key, job_id: UUID):
    if key.role == KeyRole.ADMIN:
        return
    job = await db.get(model.Job, job_id)
    if job is None:
        raise HTTPException(
            status_code=fastapi.status.HTTP_404_NOT_FOUND,
            detail={"code": "JOB_NOT_FOUND", "message": f"Job '{job_id}' does not exist"}
        )
    if job.worker_key_id != key.id:
        raise HTTPException(
            status_code=fastapi.status.HTTP_403_FORBIDDEN,
            detail={"code": "KEY_FORBIDDEN_FOR_JOB", "message": f"Key '{key.id}' does not have access to the job"}
        )

