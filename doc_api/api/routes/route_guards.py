from typing import Set, Optional, Iterable
from uuid import UUID

import fastapi
from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.cruds import job_cruds
from doc_api.api.schemas.base_objects import KeyRole, ProcessingState
from doc_api.api.schemas.responses import DocAPIClientErrorException, AppCode, \
    DocAPIResponseClientError, GENERAL_RESPONSES
from doc_api.db import model


USER_ACCESS_TO_JOB_GUARD_RESPONSES = {
    AppCode.JOB_NOT_FOUND: GENERAL_RESPONSES[AppCode.JOB_NOT_FOUND],
    AppCode.API_KEY_FORBIDDEN_FOR_JOB: GENERAL_RESPONSES[AppCode.API_KEY_FORBIDDEN_FOR_JOB]
}

# this decorator must be used on route handlers that use challenge_user_access_to_job for the documentation to be correct
def uses_challenge_user_access_to_job(fn):
    setattr(fn, "__challenge_user_access_to_job__", True)
    return fn

async def challenge_user_access_to_job(db: AsyncSession, key: model.Key, job_id: UUID):
    db_job, code = await job_cruds.get_job(db=db, job_id=job_id)

    if code == AppCode.JOB_NOT_FOUND:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_404_NOT_FOUND,
            code=AppCode.JOB_NOT_FOUND,
            detail="Job does not exist."
        )

    if key.role == KeyRole.ADMIN:
        return

    if db_job.owner_key_id != key.id:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_403_FORBIDDEN,
            code=AppCode.API_KEY_FORBIDDEN_FOR_JOB,
            detail="The API key does not have access to the job."
        )


WORKER_ACCESS_TO_JOB_GUARD_RESPONSES = {
    AppCode.JOB_NOT_FOUND: GENERAL_RESPONSES[AppCode.JOB_NOT_FOUND],
    AppCode.API_KEY_FORBIDDEN_FOR_JOB: GENERAL_RESPONSES[AppCode.API_KEY_FORBIDDEN_FOR_JOB],
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
    db_job, code = await job_cruds.get_job(db=db, job_id=job_id)

    if code == AppCode.JOB_NOT_FOUND:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_404_NOT_FOUND,
            code=AppCode.JOB_NOT_FOUND,
            detail=WORKER_ACCESS_TO_JOB_GUARD_RESPONSES[AppCode.JOB_NOT_FOUND]["detail"]
        )

    if key.role == KeyRole.ADMIN:
        return

    states: Set[ProcessingState] = set(allowed_states) if allowed_states is not None else {ProcessingState.PROCESSING}

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
