from uuid import UUID

import fastapi
from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.cruds import general_cruds
from doc_api.api.schemas.base_objects import KeyRole, ProcessingState
from doc_api.api.schemas.responses import DocAPIClientErrorException, AppCode, \
    DocAPIResponseClientError, GENERAL_RESPONSES
from doc_api.db import model


WORKER_ACCESS_TO_PROCESSING_JOB_GUARD_RESPONSES = {
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
def uses_challenge_worker_access_to_processing_job(fn):
    setattr(fn, "__challenge_worker_access_to_processing_job__", True)
    return fn

async def challenge_worker_access_to_processing_job(
    *,
    db: AsyncSession,
    key: model.Key,
    job_id: UUID
):
    db_job, code = await general_cruds.get_job(db=db, job_id=job_id)

    if code == AppCode.JOB_NOT_FOUND:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_404_NOT_FOUND,
            code=AppCode.JOB_NOT_FOUND,
            detail=WORKER_ACCESS_TO_PROCESSING_JOB_GUARD_RESPONSES[AppCode.JOB_NOT_FOUND]["detail"]
        )

    if key.role == KeyRole.ADMIN:
        return

    if db_job.worker_key_id != key.id:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_403_FORBIDDEN,
            code=AppCode.API_KEY_FORBIDDEN_FOR_JOB,
            detail=WORKER_ACCESS_TO_PROCESSING_JOB_GUARD_RESPONSES[AppCode.API_KEY_FORBIDDEN_FOR_JOB]["detail"]
        )

    if db_job.state != ProcessingState.PROCESSING:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_409_CONFLICT,
            code=AppCode.JOB_NOT_IN_PROCESSING,
            detail=WORKER_ACCESS_TO_PROCESSING_JOB_GUARD_RESPONSES[AppCode.JOB_NOT_IN_PROCESSING]["detail"]
        )


WORKER_ACCESS_TO_FINALIZING_JOB_GUARD_RESPONSES = {
    AppCode.JOB_NOT_FOUND: GENERAL_RESPONSES[AppCode.JOB_NOT_FOUND],
    AppCode.API_KEY_FORBIDDEN_FOR_JOB: GENERAL_RESPONSES[AppCode.API_KEY_FORBIDDEN_FOR_JOB],
    AppCode.JOB_INVALID_STATE: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": "The worker's job is not in the PROCESSING, DONE, or ERROR state.",
        "model": DocAPIResponseClientError,
        "detail": "Only jobs in PROCESSING, DONE, or ERROR state can be accessed by workers for finalization.",
    }
}

# this decorator must be used on route handlers that use challenge_worker_access_to_finalizing_job for the documentation to be correct
def uses_challenge_worker_access_to_finalizing_job(fn):
    setattr(fn, "__challenge_worker_access_to_finalizing_job__", True)
    return fn

async def challenge_worker_access_to_finalizing_job(
    *,
    db: AsyncSession,
    key: model.Key,
    job_id: UUID
):
    db_job, code = await general_cruds.get_job(db=db, job_id=job_id)

    if code == AppCode.JOB_NOT_FOUND:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_404_NOT_FOUND,
            code=AppCode.JOB_NOT_FOUND,
            detail=WORKER_ACCESS_TO_FINALIZING_JOB_GUARD_RESPONSES[AppCode.JOB_NOT_FOUND]["detail"]
        )

    if key.role == KeyRole.ADMIN:
        return

    if db_job.worker_key_id != key.id:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_403_FORBIDDEN,
            code=AppCode.API_KEY_FORBIDDEN_FOR_JOB,
            detail=WORKER_ACCESS_TO_FINALIZING_JOB_GUARD_RESPONSES[AppCode.API_KEY_FORBIDDEN_FOR_JOB]["detail"]
        )

    if db_job.state not in {ProcessingState.PROCESSING, ProcessingState.DONE, ProcessingState.ERROR}:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_409_CONFLICT,
            code=AppCode.JOB_INVALID_STATE,
            detail=WORKER_ACCESS_TO_FINALIZING_JOB_GUARD_RESPONSES[AppCode.JOB_INVALID_STATE]["detail"]
        )