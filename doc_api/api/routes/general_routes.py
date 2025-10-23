import logging
import os
from types import NoneType

import fastapi
from fastapi import Depends, status, Request, Body

from aiofiles import os as aiofiles_os

from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.routes import root_router
from doc_api.api.routes.helper import RouteInvariantError
from doc_api.api.guards.user_guards import challenge_user_access_to_job
from doc_api.api.authentication import require_api_key
from doc_api.api.cruds import user_cruds, general_cruds, worker_cruds
from doc_api.api.database import get_async_session
from doc_api.api.guards.worker_guards import challenge_worker_access_to_job
from doc_api.api.schemas import base_objects
from doc_api.api.schemas.responses import DocAPIResponseClientError, AppCode, DocAPIResponseOK, make_responses, \
    DocAPIClientErrorException

from doc_api.db import model
from doc_api.config import config
from uuid import UUID


logger = logging.getLogger(__name__)


GET_JOB_RESPONSES = {
    AppCode.JOB_RETRIEVED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Job details retrieved successfully.",
        "model": DocAPIResponseOK,
        "model_data": base_objects.JobWithImages,
        "detail": "The job details have been retrieved successfully.",
    }
}
@root_router.get(
    "/v1/jobs/{job_id}",
    summary="Get Job",
    response_model=DocAPIResponseOK[base_objects.JobWithImages],
    tags=["User", "Worker"],
    description="Retrieve the details of a specific job by its ID.",
    responses=make_responses(GET_JOB_RESPONSES))
@challenge_user_access_to_job
@challenge_worker_access_to_job
async def get_job(
        request: Request,
        job_id: UUID,
        key: model.Key = Depends(require_api_key(model.KeyRole.USER, model.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):

    db_job, job_code = await general_cruds.get_job(db=db, job_id=job_id)
    db_images, images_code = await general_cruds.get_job_images(db=db, job_id=job_id)

    if job_code == AppCode.JOB_RETRIEVED and images_code == AppCode.IMAGE_RETRIEVED:
        job = base_objects.Job.model_validate(db_job).model_dump()
        images = [base_objects.Image.model_validate(img).model_dump() for img in db_images]
        data = base_objects.JobWithImages(**job, images=images)
        return DocAPIResponseOK[base_objects.JobWithImages](
            status=status.HTTP_200_OK,
            code=AppCode.JOB_RETRIEVED,
            detail=GET_JOB_RESPONSES[AppCode.JOB_RETRIEVED]["detail"],
            data=data
        )

    raise RouteInvariantError(code=job_code if job_code != AppCode.JOB_RETRIEVED else images_code, request=request)


PATCH_JOB_RESPONSES = {
    # user cancels job
    AppCode.API_KEY_USER_FORBIDDEN: {
        "status": fastapi.status.HTTP_403_FORBIDDEN,
        "description": "User can only cancel jobs.",
        "model": DocAPIResponseClientError,
        "detail": "User can only cancel jobs.",
    },
    AppCode.JOB_UNCANCELLABLE: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": f"Job is already finished and cannot be cancelled. Job is in one of the following `state: {base_objects.ProcessingState.CANCELLED}|{base_objects.ProcessingState.DONE}|{base_objects.ProcessingState.ERROR}`",
        "model": DocAPIResponseClientError,
        "detail": "Job is already finished and cannot be cancelled.",
        "details" : {"state": f"{base_objects.ProcessingState.DONE.value}"}
    },
    AppCode.JOB_CANCELLED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Job cancelled successfully.",
        "model": DocAPIResponseOK,
        "detail": "Job cancelled successfully.",
    },

    # worker updating progress
    AppCode.JOB_UPDATE_NO_FIELDS: {
        "status": fastapi.status.HTTP_400_BAD_REQUEST,
        "description": "No progress update fields provided.",
        "model": DocAPIResponseClientError,
        "detail": "At least one of `progress`, `log`, or `log_user` must be provided to update job progress.",
    },
    AppCode.JOB_UPDATED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Job has been updated successfully and the lease has been extended (UTC time).",
        "model": DocAPIResponseOK,
        "model_data": base_objects.JobLease,
        "detail": "Job has been updated successfully and the lease has been extended (UTC time).",
    },

    # worker finishing job
    AppCode.API_KEY_WORKER_FORBIDDEN: {
        "status": fastapi.status.HTTP_403_FORBIDDEN,
        "description": "Worker can only mark job as done or error, and update progress.",
        "model": DocAPIResponseClientError,
        "detail": "Worker can only mark job as done or error, and update progress.",
    },
    AppCode.JOB_UNFINISHABLE: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": f"Job cannot be finished. Job is in one of the following `state: {base_objects.ProcessingState.CANCELLED}`",
        "model": DocAPIResponseClientError,
        "detail": "Job cannot be finished.",
        "details" : {"state": f"{base_objects.ProcessingState.CANCELLED.value}"}
    },
    AppCode.JOB_RESULT_MISSING: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": "Job cannot be marked as done. Result ZIP file has not been uploaded yet.",
        "model": DocAPIResponseClientError,
        "detail": "Job cannot be marked as done. Result ZIP file has not been uploaded yet.",
    },
    AppCode.JOB_COMPLETED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Job has been marked as done.",
        "model": DocAPIResponseOK,
        "detail": "Job has been marked as done.",
    },
    AppCode.JOB_FAILED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Job has been marked as error.",
        "model": DocAPIResponseOK,
        "detail": "Job has been marked as error.",
    },
    AppCode.JOB_ALREADY_COMPLETED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Job was already marked as done.",
        "model": DocAPIResponseOK,
        "detail": "Job was already marked as done.",
    },
    AppCode.JOB_ALREADY_FAILED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Job was already marked as error.",
        "model": DocAPIResponseOK,
        "detail": "Job was already marked as error.",
    }
}
@root_router.patch(
    "/v1/jobs/{job_id}",
    summary="Update Job",
    response_model=DocAPIResponseOK[NoneType],
    tags=["User", "Worker"],
    description="Update the status of a specific job. "
                "Users can cancel jobs, while workers can mark jobs as done or error, and update progress.",
    responses=make_responses(PATCH_JOB_RESPONSES))
@challenge_user_access_to_job
@challenge_worker_access_to_job
async def patch_job(
        request: Request,
        *,
        job_id: UUID,
        job_progress_update: base_objects.JobProgressUpdate =
        Body(
            openapi_examples={
                "user_cancelled": {
                    "summary": f"User cancels job",
                    "description": "User marks job as cancelled."
                                   "\n\nOnly `state: cancelled` will be accepted, other fields will be ignored."
                                   "\n\nJob must be in one of the following "
                                   f"`state: {base_objects.ProcessingState.NEW}|{base_objects.ProcessingState.QUEUED}|{base_objects.ProcessingState.PROCESSING}`.",
                    "value": {"state": base_objects.ProcessingState.CANCELLED.value},
                },
                "worker_done": {
                    "summary": "Worker finalizes job as done",
                    "description": "Worker marks job as done, `progress: 1.0` is automatically set. If given, logs are appended to existing logs."
                                   f"\n\n`state: {base_objects.ProcessingState.DONE}` must be provided, other fields are optional."
                                   "\n\nJob must be in one of the following "
                                   f"`state: {base_objects.ProcessingState.PROCESSING}|{base_objects.ProcessingState.DONE}`.",
                    "value": {
                        "state": base_objects.ProcessingState.DONE.value,
                        "log": "Processed 1000/1000. Job complete.",
                        "log_user": "Proceessed 1000 pages. Job complete.",
                    },
                },
                "worker_error": {
                    "summary": "Worker finalizes job as error",
                    "description": "Worker marks job as error. If given, logs are appended to existing logs."
                                   f"\n\n`state: {base_objects.ProcessingState.ERROR}` must be provided, other fields are optional."
                                   f"\n\nJob must be in one of the following "
                                   f"`state: {base_objects.ProcessingState.PROCESSING}|{base_objects.ProcessingState.ERROR}`.",
                    "value": {
                        "state": base_objects.ProcessingState.ERROR.value,
                        "progress": 0.7,
                        "log": "Processed 700/1000. Encountered an error.",
                        "log_user": "Processing page 700 of 1000. Encountered an error.",
                    },
                },
                "worker_progress": {
                    "summary": "Worker updates job progress",
                    "description": (
                        "Worker updates job progress and log messages. If given, logs are appended to existing logs."
                        "\n\nAt least one of `progress|log|log_user` must be provided."
                        f"\n\nJob must be in `state: {base_objects.ProcessingState.PROCESSING}`."
                        f"\n\n`progress` is clipped to range `[0.0, 1.0]`."
                        "\n\nLease is renewed automatically when updating progress. "
                        "If you only want to renew the lease without updating progress "
                        f"use [`PATCH /v1/jobs/{{job_id}}/lease`]({config.APP_URL_ROOT}/docs#/Worker/patch_lease_v1_jobs__job_id__lease_patch). "
                    ),
                    "value": {
                        "progress": 0.7,
                        "log": "Processed 700/1000.",
                        "log_user": "Processing page 700 of 1000.",
                    },
                }
            }
        ),
        key: model.Key = Depends(require_api_key(model.KeyRole.USER, model.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):

    db_job, code_get_job = await general_cruds.get_job(db=db, job_id=job_id)
    code_update_job = None

    if key.role == model.KeyRole.USER:
        if job_progress_update.state != base_objects.ProcessingState.CANCELLED:
            raise DocAPIClientErrorException(
                status=status.HTTP_403_FORBIDDEN,
                code=AppCode.API_KEY_USER_FORBIDDEN,
                detail=PATCH_JOB_RESPONSES[AppCode.API_KEY_USER_FORBIDDEN]["detail"]
            )
        if db_job.state in {base_objects.ProcessingState.CANCELLED,
                            base_objects.ProcessingState.DONE,
                            base_objects.ProcessingState.ERROR}:
            raise DocAPIClientErrorException(
                status=status.HTTP_409_CONFLICT,
                code=AppCode.JOB_UNCANCELLABLE,
                detail=PATCH_JOB_RESPONSES[AppCode.JOB_UNCANCELLABLE]["detail"],
                details={"state": f"{db_job.state.value}"}
            )

        # user cancels job
        code_update_job = await user_cruds.cancel_job(db, job_id)
        if code_update_job == AppCode.JOB_CANCELLED:
            return DocAPIResponseOK[NoneType](
                status=status.HTTP_200_OK,
                code=AppCode.JOB_CANCELLED,
                detail=PATCH_JOB_RESPONSES[AppCode.JOB_CANCELLED]["detail"]
            )

    elif key.role == model.KeyRole.WORKER:
        if job_progress_update.state in {base_objects.ProcessingState.DONE, base_objects.ProcessingState.ERROR} \
            and db_job.state not in {base_objects.ProcessingState.PROCESSING, base_objects.ProcessingState.DONE, base_objects.ProcessingState.ERROR}:
            raise DocAPIClientErrorException(
                status=status.HTTP_409_CONFLICT,
                code=AppCode.JOB_UNFINISHABLE,
                detail=PATCH_JOB_RESPONSES[AppCode.JOB_UNFINISHABLE]["detail"],
                details={"state": f"{db_job.state.value}"}
            )

        # at this point, the job must be finishable because the worker can access only jobs in processing|done|error state
        # new and queued jobs are not assigned to workers

        # worker marks job as done
        if job_progress_update.state == base_objects.ProcessingState.DONE:
            result_path = os.path.join(config.RESULT_DIR, f"{job_id}.zip")
            if not await aiofiles_os.path.exists(result_path):
                raise DocAPIClientErrorException(
                    status=fastapi.status.HTTP_409_CONFLICT,
                    code=AppCode.JOB_RESULT_MISSING,
                    detail=PATCH_JOB_RESPONSES[AppCode.JOB_RESULT_MISSING]["detail"],
                )

            code_update_job = await worker_cruds.update_job_progress(
                db=db,
                job_id=job_id,
                job_progress_update=job_progress_update
            )

            if code_update_job == AppCode.JOB_COMPLETED:
                return DocAPIResponseOK[NoneType](
                    status=fastapi.status.HTTP_200_OK,
                    code=AppCode.JOB_COMPLETED,
                    detail=PATCH_JOB_RESPONSES[AppCode.JOB_COMPLETED]["detail"]
                )
            elif code_update_job == AppCode.JOB_ALREADY_COMPLETED:
                return DocAPIResponseOK[NoneType](
                    status=fastapi.status.HTTP_200_OK,
                    code=AppCode.JOB_ALREADY_COMPLETED,
                    detail=PATCH_JOB_RESPONSES[AppCode.JOB_ALREADY_COMPLETED]["detail"]
                )

        # worker marks job as error
        elif job_progress_update.state == base_objects.ProcessingState.ERROR:

            code_update_job = await worker_cruds.update_job_progress(
                db=db,
                job_id=job_id,
                job_progress_update=job_progress_update
            )

            if code_update_job == AppCode.JOB_FAILED:
                return DocAPIResponseOK[NoneType](
                    status=fastapi.status.HTTP_200_OK,
                    code=AppCode.JOB_FAILED,
                    detail=PATCH_JOB_RESPONSES[AppCode.JOB_FAILED]["detail"]
                )
            elif code_update_job == AppCode.JOB_ALREADY_FAILED:
                return DocAPIResponseOK[NoneType](
                    status=fastapi.status.HTTP_200_OK,
                    code=AppCode.JOB_ALREADY_FAILED,
                    detail=PATCH_JOB_RESPONSES[AppCode.JOB_ALREADY_FAILED]["detail"]
                )

        # worker updates job progress
        elif job_progress_update.state is None:
            if job_progress_update.progress is None and \
                job_progress_update.log is None and \
                job_progress_update.log_user is None:
                raise DocAPIClientErrorException(
                    status=status.HTTP_400_BAD_REQUEST,
                    code=AppCode.JOB_UPDATE_NO_FIELDS,
                    detail=PATCH_JOB_RESPONSES[AppCode.JOB_UPDATE_NO_FIELDS]["detail"]
                )
            db_job, lease_expire_at, server_time, code_update_job = await worker_cruds.update_job_progress(
                db=db,
               job_id=job_id,
               job_progress_update=job_progress_update
            )
            if code_update_job == AppCode.JOB_UPDATED:
                return DocAPIResponseOK[base_objects.JobLease](
                    status=fastapi.status.HTTP_200_OK,
                    code=AppCode.JOB_UPDATED,
                    detail=PATCH_JOB_RESPONSES[AppCode.JOB_UPDATED]["detail"],
                    data=base_objects.JobLease(id=job_id, lease_expire_at=lease_expire_at, server_time=server_time)
                )

        else:
            raise DocAPIClientErrorException(
                status=status.HTTP_403_FORBIDDEN,
                code=AppCode.API_KEY_WORKER_FORBIDDEN,
                detail=PATCH_JOB_RESPONSES[AppCode.API_KEY_WORKER_FORBIDDEN]["detail"]
            )

    raise RouteInvariantError(code=code_get_job if code_update_job is None else code_update_job, request=request)


ME_RESPONSES = {
    AppCode.API_KEY_VALID: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "API key is valid.",
        "model": DocAPIResponseOK,
        "model_data": base_objects.Key,
        "detail": "API key is valid.",
    }
}
@root_router.get(
    "/v1/me",
    summary="Who am I?",
    response_model=DocAPIResponseOK[base_objects.Key],
    tags=["General"],
    description="Validate your API key and get information about it.",
    responses=make_responses(ME_RESPONSES)
)
async def me(key: model.Key = Depends(require_api_key(model.KeyRole.USER, model.KeyRole.WORKER))):
    return DocAPIResponseOK[base_objects.Key](
        status=status.HTTP_200_OK,
        code=AppCode.API_KEY_VALID,
        detail=ME_RESPONSES[AppCode.API_KEY_VALID]["detail"],
        data=key)