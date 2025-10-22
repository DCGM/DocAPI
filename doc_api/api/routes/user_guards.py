from functools import wraps
from typing import Tuple, Any
from uuid import UUID

import fastapi
from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.cruds import general_cruds
from doc_api.api.schemas import base_objects
from doc_api.api.schemas.base_objects import KeyRole, ProcessingState
from doc_api.api.schemas.responses import DocAPIClientErrorException, AppCode, \
    DocAPIResponseClientError, GENERAL_RESPONSES
from doc_api.db import model


USER_ACCESS_TO_JOB_GUARD_RESPONSES = {
    AppCode.JOB_NOT_FOUND: GENERAL_RESPONSES[AppCode.JOB_NOT_FOUND],
    AppCode.API_KEY_FORBIDDEN_FOR_JOB: GENERAL_RESPONSES[AppCode.API_KEY_FORBIDDEN_FOR_JOB],
}
def challenge_user_access_to_job(fn):
    @wraps(fn)
    async def wrapper(*args, **kwargs):
        job_id, key, db = _get_job_access_params(kwargs)

        if key.role != KeyRole.USER:
            return await fn(*args, **kwargs)

        await _challenge_user_access_to_job(db=db, key=key, job_id=job_id)

        return await fn(*args, **kwargs)

    wrapper.__challenge_user_access_to_job__ = True
    return wrapper

async def _challenge_user_access_to_job(
        *,
        db: AsyncSession,
        key: model.Key,
        job_id: UUID) -> model.Job:
    db_job, code = await general_cruds.get_job(db=db, job_id=job_id)

    if code == AppCode.JOB_NOT_FOUND:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_404_NOT_FOUND,
            code=AppCode.JOB_NOT_FOUND,
            detail=USER_ACCESS_TO_JOB_GUARD_RESPONSES[AppCode.JOB_NOT_FOUND]["detail"],
        )

    if db_job.owner_key_id != key.id:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_403_FORBIDDEN,
            code=AppCode.API_KEY_FORBIDDEN_FOR_JOB,
            detail=USER_ACCESS_TO_JOB_GUARD_RESPONSES[AppCode.API_KEY_FORBIDDEN_FOR_JOB]["detail"],
        )

    return db_job


USER_ACCESS_TO_NEW_JOB_GUARD_RESPONSES = {
    **USER_ACCESS_TO_JOB_GUARD_RESPONSES,
    AppCode.JOB_NOT_IN_NEW: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": f"Only jobs in `state: {base_objects.ProcessingState.NEW}` can be accessed by users for this operation. "
                       f"Job is in one of the following `state: "
                       f"{base_objects.ProcessingState.QUEUED}|"
                       f"{base_objects.ProcessingState.PROCESSING}|"
                       f"{base_objects.ProcessingState.CANCELLED}|"
                       f"{base_objects.ProcessingState.DONE}|"
                       f"{base_objects.ProcessingState.ERROR}`.",
        "model": DocAPIResponseClientError,
        "detail": f"Only jobs in {base_objects.ProcessingState.NEW} state can be accessed by users for this operation.",
        "details": {
            "state": f"{base_objects.ProcessingState.QUEUED}"
        }
    }
}
def challenge_user_access_to_new_job(fn):
    @wraps(fn)
    async def wrapper(*args, **kwargs):
        job_id, key, db = _get_job_access_params(kwargs)

        if key.role != KeyRole.USER:
            return await fn(*args, **kwargs)

        await _challenge_user_access_to_new_job(db=db, key=key, job_id=job_id)

        return await fn(*args, **kwargs)

    wrapper.__challenge_user_access_to_new_job__ = True
    return wrapper

async def _challenge_user_access_to_new_job(
        *,
        db: AsyncSession,
        key: model.Key,
        job_id: UUID):

    db_job = await _challenge_user_access_to_job(db=db, key=key, job_id=job_id)

    if db_job.state != ProcessingState.NEW:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_409_CONFLICT,
            code=AppCode.JOB_NOT_IN_NEW,
            detail=USER_ACCESS_TO_NEW_JOB_GUARD_RESPONSES[AppCode.JOB_NOT_IN_NEW]["detail"],
            details={"state": db_job.state.value}
        )

def _get_job_access_params(kwargs: dict) -> Tuple[UUID, Any, AsyncSession]:
    """
    Extracts `job_id`, `key`, and `db` from kwargs.
    Raises RuntimeError if any are missing or None.
    """
    required = ("job_id", "key", "db")
    missing = [p for p in required if p not in kwargs or kwargs[p] is None]
    if missing:
        raise RuntimeError(
            f"[get_job_access_params] Missing required params: {', '.join(missing)}. "
            "Ensure your handler defines them and they are provided by Depends()."
        )

    job_id: UUID = kwargs["job_id"]
    key = kwargs["key"]
    db: AsyncSession = kwargs["db"]

    return job_id, key, db