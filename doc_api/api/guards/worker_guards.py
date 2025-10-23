from functools import wraps
from uuid import UUID

import fastapi
from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.cruds import general_cruds
from doc_api.api.guards.user_guards import _get_job_access_params
from doc_api.api.schemas import base_objects
from doc_api.api.schemas.base_objects import KeyRole, ProcessingState
from doc_api.api.schemas.responses import DocAPIClientErrorException, AppCode, \
    DocAPIResponseClientError, GENERAL_RESPONSES
from doc_api.db import model


WORKER_ACCESS_TO_JOB_GUARD_RESPONSES = {
    AppCode.JOB_NOT_FOUND: GENERAL_RESPONSES[AppCode.JOB_NOT_FOUND],
    AppCode.API_KEY_FORBIDDEN_FOR_JOB: GENERAL_RESPONSES[AppCode.API_KEY_FORBIDDEN_FOR_JOB]
}
def challenge_worker_access_to_job(fn):
    @wraps(fn)
    async def wrapper(*args, **kwargs):
        job_id, key, db = _get_job_access_params(kwargs)

        if key.role != KeyRole.WORKER:
            return await fn(*args, **kwargs)

        await _challenge_worker_access_to_job(db=db, key=key, job_id=job_id)

        return await fn(*args, **kwargs)

    wrapper.__challenge_worker_access_to_job__ = True
    return wrapper

async def _challenge_worker_access_to_job(
        *,
        db: AsyncSession,
        key: model.Key,
        job_id: UUID) -> base_objects.Job:

    db_job, code = await general_cruds.get_job(db=db, job_id=job_id)

    if code == AppCode.JOB_NOT_FOUND:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_404_NOT_FOUND,
            code=AppCode.JOB_NOT_FOUND,
            detail=WORKER_ACCESS_TO_JOB_GUARD_RESPONSES[AppCode.JOB_NOT_FOUND]["detail"]
        )

    if db_job.worker_key_id != key.id:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_403_FORBIDDEN,
            code=AppCode.API_KEY_FORBIDDEN_FOR_JOB,
            detail=WORKER_ACCESS_TO_JOB_GUARD_RESPONSES[AppCode.API_KEY_FORBIDDEN_FOR_JOB]["detail"]
        )

    return db_job


WORKER_ACCESS_TO_PROCESSING_JOB_GUARD_RESPONSES = {
    **WORKER_ACCESS_TO_JOB_GUARD_RESPONSES,
    AppCode.JOB_NOT_IN_PROCESSING: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": f"Only jobs in `state: {base_objects.ProcessingState.PROCESSING}` can be accessed by workers for this operation. "
                       f"Job is in one of the following `state: "
                       f"{base_objects.ProcessingState.CANCELLED}|"
                       f"{base_objects.ProcessingState.DONE}|"
                       f"{base_objects.ProcessingState.ERROR}`.",
        "model": DocAPIResponseClientError,
        "detail": f"Only jobs in {base_objects.ProcessingState.PROCESSING} state can be accessed by workers for this operation.",
        "details": {
            "state": f"{base_objects.ProcessingState.CANCELLED}"
        }
    }
}
def challenge_worker_access_to_processing_job(fn):
    @wraps(fn)
    async def wrapper(*args, **kwargs):
        job_id, key, db = _get_job_access_params(kwargs)

        if key.role != KeyRole.WORKER:
            return await fn(*args, **kwargs)

        await _challenge_worker_access_to_processing_job(db=db, key=key, job_id=job_id)

        return await fn(*args, **kwargs)

    wrapper.__challenge_worker_access_to_processing_job__ = True
    return wrapper

async def _challenge_worker_access_to_processing_job(
        *,
        db: AsyncSession,
        key: model.Key,
        job_id: UUID):

    db_job = await _challenge_worker_access_to_job(db=db, key=key, job_id=job_id)

    if db_job.state != ProcessingState.PROCESSING:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_409_CONFLICT,
            code=AppCode.JOB_NOT_IN_PROCESSING,
            detail=WORKER_ACCESS_TO_PROCESSING_JOB_GUARD_RESPONSES[AppCode.JOB_NOT_IN_PROCESSING]["detail"],
            details={"state": db_job.state.value}
        )
