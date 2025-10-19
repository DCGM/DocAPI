from pyexpat.errors import messages
from typing import Set, Optional, Iterable, Tuple
from uuid import UUID

import fastapi
from sqlalchemy import select, exc
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException

from doc_api.api.database import DBError
from doc_api.api.schemas.base_objects import KeyRole, ProcessingState
from doc_api.api.schemas.responses import DocAPIClientErrorException, AppCode, \
    DocAPIResponseClientError
from doc_api.db import model


async def challenge_user_access_to_job(db: AsyncSession, key: model.Key, job_id: UUID):
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


WORKER_ACCESS_TO_JOB_GUARD_RESPONSES = {
    AppCode.JOB_NOT_FOUND: {
        "status": fastapi.status.HTTP_404_NOT_FOUND,
        "description": "The specified job does not exist.",
        "model": DocAPIResponseClientError,
        "detail": "Job does not exist.",
    },
    AppCode.API_KEY_FORBIDDEN_FOR_JOB: {
        "status": fastapi.status.HTTP_403_FORBIDDEN,
        "description": "The worker's API key does not have access to the specified job.",
        "model": DocAPIResponseClientError,
        "detail": "The worker's API key does not have access to the job.",
    },
    AppCode.JOB_NOT_IN_PROCESSING: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": "The worker's job is not in the PROCESSING state.",
        "model": DocAPIResponseClientError,
        "detail": "Only jobs in PROCESSING state can be accessed by workers.",
    }
}

# this decorator must be used on route handlers that use challenge_worker_access_to_job for the documentation to be correct
def uses_challenge_worker_access_to_job(fn):
    setattr(fn, "__challenge_worker_access_to_job__", True)
    return fn

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
                    detail=WORKER_ACCESS_TO_JOB_GUARD_RESPONSES[AppCode.JOB_NOT_FOUND]["detail"]
                )

            if key.role == KeyRole.ADMIN:
                return

            if db_job.worker_key_id != key.id:
                raise DocAPIClientErrorException(
                    status=fastapi.status.HTTP_403_FORBIDDEN,
                    code=AppCode.API_KEY_FORBIDDEN_FOR_JOB,
                    detail=WORKER_ACCESS_TO_JOB_GUARD_RESPONSES[AppCode.API_KEY_FORBIDDEN_FOR_JOB]["detail"]
                )

            if states and db_job.state not in states:
                raise DocAPIClientErrorException(
                    status=fastapi.status.HTTP_409_CONFLICT,
                    code=AppCode.JOB_NOT_IN_PROCESSING,
                    detail=WORKER_ACCESS_TO_JOB_GUARD_RESPONSES[AppCode.JOB_NOT_IN_PROCESSING]["detail"]
                )

            return

    except exc.SQLAlchemyError as e:
        raise DBError("Failed authorizing worker access to job in database") from e