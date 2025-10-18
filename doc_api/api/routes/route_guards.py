from pyexpat.errors import messages
from typing import Set, Optional, Iterable, Tuple
from uuid import UUID

import fastapi
from sqlalchemy import select, exc
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException

from doc_api.api.database import DBError
from doc_api.api.schemas.base_objects import KeyRole, ProcessingState
from doc_api.api.schemas.responses import DocAPIClientErrorException, AppCode, DETAILS_GENERAL, \
    DocAPIResponseClientError
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

WORKER_ACCESS_TO_JOB_GUARD_DETAILS = {
    AppCode.JOB_NOT_FOUND: "Job with id={job_id} does not exist.",
    AppCode.API_KEY_FORBIDDEN_FOR_JOB: "Worker's Key (label={key_label}) does not have access to the Job (id={job_id}).",
    AppCode.JOB_NOT_IN_PROCESSING: "Job (id={job_id}, state={job_state}), only Job in PROCESSING state can be accessed by workers."
}
WORKER_ACCESS_TO_JOB_GUARD_EXAMPLES = {
    fastapi.status.HTTP_404_NOT_FOUND: {
        "model": DocAPIResponseClientError,
        "content": {
            "application/json": {
                "examples": {
                    "JobNotFound": {
                        "summary": AppCode.JOB_NOT_FOUND.value,
                        "description": "The specified job does not exist.",
                        "value": DocAPIResponseClientError(
                            status=fastapi.status.HTTP_404_NOT_FOUND,
                            code=AppCode.JOB_NOT_FOUND,
                            detail=WORKER_ACCESS_TO_JOB_GUARD_DETAILS[AppCode.JOB_NOT_FOUND]).model_dump(mode="json")}}}}
    },
    fastapi.status.HTTP_403_FORBIDDEN: {
        "model": DocAPIResponseClientError,
        "content": {
            "application/json": {
                "examples": {
                    "APIKeyForbiddenForJob": {
                        "summary": AppCode.API_KEY_FORBIDDEN_FOR_JOB.value,
                        "description": "The worker's API key does not have access to the specified job.",
                        "value": DocAPIResponseClientError(
                            status=fastapi.status.HTTP_403_FORBIDDEN,
                            code=AppCode.API_KEY_FORBIDDEN_FOR_JOB,
                            detail=WORKER_ACCESS_TO_JOB_GUARD_DETAILS[AppCode.API_KEY_FORBIDDEN_FOR_JOB]).model_dump(mode="json")}}}}
        },
    fastapi.status.HTTP_409_CONFLICT: {
        "model": DocAPIResponseClientError,
        "content": {
            "application/json": {
                "examples": {
                    "JobNotInProcessing": {
                        "summary": AppCode.JOB_NOT_IN_PROCESSING.value,
                        "description": "The worker's job is not in the PROCESSING state.",
                        "value": DocAPIResponseClientError(
                            status=fastapi.status.HTTP_409_CONFLICT,
                            code=AppCode.JOB_NOT_IN_PROCESSING,
                            detail=WORKER_ACCESS_TO_JOB_GUARD_DETAILS[AppCode.JOB_NOT_IN_PROCESSING]).model_dump(mode="json")}}}}
    }
}
async def challenge_worker_access_to_job(
    *,
    db: AsyncSession,
    key: model.Key,
    job_id: UUID,
    allowed_states: Optional[Iterable[ProcessingState]] = None
):
    """
    Authorize worker access to a job and enforce allowed states.
    - ADMIN bypasses worker ownership and state checks, still returns 404 while job is not found.
    - `allowed_states` controls which states are acceptable for this route.
      Defaults to {PROCESSING}.
    - If `lock=True`, acquires FOR UPDATE (useful before state transitions).
    Returns the job row so callers don't re-query.
    """
    try:
        async with db.begin_nested():
            states: Set[ProcessingState] = set(allowed_states) if allowed_states is not None else {ProcessingState.PROCESSING}

            result = await db.execute(select(model.Job).where(model.Job.id == job_id))
            db_job = result.scalar_one_or_none()
            if db_job is None:
                raise DocAPIClientErrorException(
                    status=fastapi.status.HTTP_404_NOT_FOUND,
                    code=AppCode.JOB_NOT_FOUND,
                    detail=WORKER_ACCESS_TO_JOB_GUARD_DETAILS[AppCode.JOB_NOT_FOUND].format(job_id=job_id)
                )

            if key.role == KeyRole.ADMIN:
                return

            if db_job.worker_key_id != key.id:
                raise DocAPIClientErrorException(
                    status=fastapi.status.HTTP_403_FORBIDDEN,
                    code=AppCode.API_KEY_FORBIDDEN_FOR_JOB,
                    detail=WORKER_ACCESS_TO_JOB_GUARD_DETAILS[AppCode.API_KEY_FORBIDDEN_FOR_JOB].format(key_label=key.label, job_id=job_id)
                )

            if states and db_job.state not in states:
                raise DocAPIClientErrorException(
                    status=fastapi.status.HTTP_409_CONFLICT,
                    code=AppCode.JOB_NOT_IN_PROCESSING,
                    detail=WORKER_ACCESS_TO_JOB_GUARD_DETAILS[AppCode.JOB_NOT_IN_PROCESSING].format(job_id=job_id, job_state=db_job.state)
                )

            return

    except exc.SQLAlchemyError as e:
        raise DBError("Failed authorizing worker access to job in database") from e