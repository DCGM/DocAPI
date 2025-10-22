import logging
from types import NoneType

import fastapi
from fastapi import Depends, status, HTTPException

from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.routes import root_router, debug_router
from doc_api.api.routes.user_guards import challenge_user_access_to_job, challenge_user_access_to_job
from doc_api.api.authentication import require_api_key
from doc_api.api.cruds import user_cruds, general_cruds
from doc_api.api.database import get_async_session
from doc_api.api.schemas import base_objects
from doc_api.api.schemas.responses import DocAPIResponseClientError, AppCode, DocAPIResponseOK, make_responses, \
     DocAPIClientErrorException
from doc_api.db import model

from typing import List
from uuid import UUID


logger = logging.getLogger(__name__)


POST_JOB_START_RESPONSES = {
    AppCode.JOB_STARTED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Job started successfully.",
        "model": DocAPIResponseOK,
        "detail": "The job has been started successfully.",
    },
    AppCode.JOB_NOT_READY: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": "Job is not ready to be started.",
        "model": DocAPIResponseClientError,
        "detail": "The job cannot be started because not all required files have been uploaded.",
    },
    AppCode.JOB_ALREADY_STARTED: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": "Job has already been started.",
        "model": DocAPIResponseClientError,
        "detail": "The job is not in NEW state and cannot be started manually.",
    }
}
@debug_router.put(
    "/jobs/{job_id}/start",
    summary="Start Job",
    response_model=DocAPIResponseOK[NoneType],
    tags=["Debug"],
    description="Start processing a job. There should be no need to call this endpoint under normal circumstances, "
                "as jobs are started automatically when all required files have been uploaded.",
    responses=make_responses(POST_JOB_START_RESPONSES))
@challenge_user_access_to_job
async def start_job(
        job_id: UUID,
        key: model.Key = Depends(require_api_key()),
        db: AsyncSession = Depends(get_async_session)):

    db_job, code = await general_cruds.get_job(db=db, job_id=job_id)

    if db_job.state == base_objects.ProcessingState.NEW:
        job_started = await user_cruds.start_job(db=db, job_id=job_id)
        if job_started:
            return DocAPIResponseOK[NoneType](
                status=status.HTTP_200_OK,
                code=AppCode.JOB_STARTED,
                detail=POST_JOB_START_RESPONSES[AppCode.JOB_STARTED]["detail"]
            )
        else:
            raise DocAPIClientErrorException(
                status=status.HTTP_409_CONFLICT,
                code=AppCode.JOB_NOT_READY,
                detail=POST_JOB_START_RESPONSES[AppCode.JOB_NOT_READY]["detail"]
            )
    else:
        raise DocAPIClientErrorException(
            status=status.HTTP_409_CONFLICT,
            code=AppCode.JOB_ALREADY_STARTED,
            detail=POST_JOB_START_RESPONSES[AppCode.JOB_ALREADY_STARTED]["detail"]
        )


# @debug_router.get("/http_exception", tags=["Debug"])
# async def http_exception(
#         key: model.Key = Depends(require_api_key()),
#         db: AsyncSession = Depends(get_async_session)):
#     raise HTTPException(status_code=418, detail="This is a debug HTTP exception.")