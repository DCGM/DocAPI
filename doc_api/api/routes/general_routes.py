import logging
import os
from types import NoneType
from typing import Optional, List

import fastapi
from fastapi import Depends, status, Request, Body
from fastapi.responses import RedirectResponse

from aiofiles import os as aiofiles_os
from natsort import natsort_keygen, natsorted

from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.guards.general_guards import challenge_job_exists
from doc_api.api.routes import root_router
from doc_api.api.routes.helper import RouteInvariantError
from doc_api.api.guards.user_guards import challenge_user_access_to_job
from doc_api.api.authentication import require_api_key
from doc_api.api.cruds import user_cruds, general_cruds, worker_cruds
from doc_api.api.database import get_async_session
from doc_api.api.guards.worker_guards import challenge_worker_access_to_job
from doc_api.api.schemas import base_objects
from doc_api.api.schemas.responses import DocAPIResponseClientError, AppCode, DocAPIResponseOK, make_responses, \
    DocAPIClientErrorException, validate_ok_response

from doc_api.db import model
from doc_api.config import config
from uuid import UUID


logger = logging.getLogger(__name__)


@root_router.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/docs")


ME_RESPONSES = {
    AppCode.API_KEY_VALID: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "API key is valid.",
        "model": DocAPIResponseOK[base_objects.Key],
        "model_data": base_objects.Key,
        "detail": "API key is valid.",
    }
}
@root_router.get(
    "/v1/me",
    summary="Who am I?",
    response_model=DocAPIResponseOK[base_objects.Key],
    tags=["User"],
    openapi_extra={"x-order": 1},
    description="Validate your API key and get information about it.",
    responses=make_responses(ME_RESPONSES)
)
async def me(key: model.Key = Depends(require_api_key(base_objects.KeyRole.READONLY, base_objects.KeyRole.USER, base_objects.KeyRole.WORKER))):
    return DocAPIResponseOK[base_objects.Key](
        status=status.HTTP_200_OK,
        code=AppCode.API_KEY_VALID,
        detail=ME_RESPONSES[AppCode.API_KEY_VALID]["detail"],
        data=key)


GET_ENGINES_RESPONSES = {
    AppCode.ENGINE_RETRIEVED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Engines retrieved successfully.",
        "model": DocAPIResponseOK[List[base_objects.Engine]],
        "model_data": List[base_objects.Engine],
        "detail": "Engines retrieved successfully.",
    },
    AppCode.API_KEY_FORBIDDEN_FOR_ENGINE_ACTIVE: {
        "status": fastapi.status.HTTP_403_FORBIDDEN,
        "description": "Regular API keys are not allowed to filter by 'active'.",
        "model": DocAPIResponseClientError,
        "detail": "Regular API keys are not allowed to filter by 'active'."
    },
    AppCode.API_KEY_FORBIDDEN_FOR_ENGINE_DEFINITION: {
        "status": fastapi.status.HTTP_403_FORBIDDEN,
        "description": "Regular API keys are not allowed to use 'show_definition'.",
        "model": DocAPIResponseClientError,
        "detail": "Regular API keys are not allowed to use 'show_definition'."
    }
}
@root_router.get(
    "/v1/engines",
    summary="Get Engines",
    response_model=DocAPIResponseOK[List[base_objects.Engine]],
    tags=["User"],
    openapi_extra={"x-order": 100},
    description="Retrieve a list of all available engines.\n\n"
                "You can filter engines by name, version, and default status.\n\n"
                "`active` and `show_definition` are meant only for admins.",
    responses=make_responses(GET_ENGINES_RESPONSES))
async def list_engines(
        request: Request,
        name: Optional[str] = None,
        version: Optional[str] = None,
        default: Optional[bool] = None,
        active: Optional[bool] = None,
        show_definition: bool = False,
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.READONLY, base_objects.KeyRole.USER, base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):

    if key.role == base_objects.KeyRole.ADMIN:
        db_engines, code = await general_cruds.get_engines(db=db,
                                                           engine_name=name,
                                                           engine_version=version,
                                                           default=default,
                                                           active=active)
    else:
        if active is not None:
            raise DocAPIClientErrorException(
                status=status.HTTP_403_FORBIDDEN,
                code=AppCode.API_KEY_FORBIDDEN_FOR_ENGINE_ACTIVE,
                detail=GET_ENGINES_RESPONSES[AppCode.API_KEY_FORBIDDEN_FOR_ENGINE_ACTIVE]["detail"]
            )
        if show_definition is not None:
            raise DocAPIClientErrorException(
                status=status.HTTP_403_FORBIDDEN,
                code=AppCode.API_KEY_FORBIDDEN_FOR_ENGINE_DEFINITION,
                detail=GET_ENGINES_RESPONSES[AppCode.API_KEY_FORBIDDEN_FOR_ENGINE_DEFINITION]["detail"]
            )
        db_engines, code = await general_cruds.get_engines(db=db,
                                                           engine_name=name,
                                                           engine_version=version,
                                                           default=default,
                                                           active=True)

    if code == AppCode.ENGINES_RETRIEVED:
        engines: List[base_objects.Engine] = []
        for db_engine in db_engines:
            engine = base_objects.Engine.model_validate(db_engine).model_dump()
            if key.role != base_objects.KeyRole.ADMIN:
                engine.pop("id")
                engine.pop("active", None)
                engine.pop("definition", None)
                engine.pop("created_date", None)
                engine.pop("last_used", None)
                engine.pop("files_updated", None)
            else:
                if not show_definition:
                    engine.pop("definition", None)
            engines.append(base_objects.Engine(**engine))

        # Natural sort by name (primary) then version (secondary)
        k_name = natsort_keygen(key=lambda e: e.name)
        k_version = natsort_keygen(key=lambda e: e.version)
        engines = natsorted(engines, key=lambda e: (k_name(e), k_version(e)))

        return validate_ok_response(DocAPIResponseOK[List[base_objects.Engine]](
            status=status.HTTP_200_OK,
            code=AppCode.ENGINES_RETRIEVED,
            detail=GET_ENGINES_RESPONSES[AppCode.ENGINE_RETRIEVED]["detail"],
            data=engines,
        ), exclude_none=key.role in {base_objects.KeyRole.USER, base_objects.KeyRole.READONLY})

    raise RouteInvariantError(code=code, request=request)


GET_JOB_RESPONSES = {
    AppCode.JOB_RETRIEVED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Job details retrieved successfully.",
        "model": DocAPIResponseOK[base_objects.Job],
        "model_data": base_objects.Job,
        "detail": "The job details have been retrieved successfully.",
    }
}
@root_router.get(
    "/v1/jobs/{job_id}",
    summary="Get Job",
    response_model=DocAPIResponseOK[base_objects.Job],
    tags=["User"],
    openapi_extra={"x-order": 103},
    description="Retrieve the details of a specific job by its ID.",
    responses=make_responses(GET_JOB_RESPONSES))
@challenge_job_exists
@challenge_user_access_to_job
@challenge_worker_access_to_job
async def get_job(
        request: Request,
        job_id: UUID,
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.READONLY, base_objects.KeyRole.USER, base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):

    db_job, job_code = await general_cruds.get_job(db=db, job_id=job_id)
    db_images, images_code = await general_cruds.get_job_images(db=db, job_id=job_id)
    db_engine = None
    engine_code = None
    if db_job.engine_id is not None:
        db_engine, engine_code = await general_cruds.get_engine(db=db, engine_id=db_job.engine_id)

    if job_code == AppCode.JOB_RETRIEVED and images_code == AppCode.IMAGES_RETRIEVED:
        data = prepare_job_data(db_job=db_job, db_images=db_images, key=key, db_engine=db_engine)
        return validate_ok_response(DocAPIResponseOK[base_objects.Job](
            status=status.HTTP_200_OK,
            code=AppCode.JOB_RETRIEVED,
            detail=GET_JOB_RESPONSES[AppCode.JOB_RETRIEVED]["detail"],
            data=data
        ), exclude_none=key.role in {base_objects.KeyRole.USER, base_objects.KeyRole.READONLY})

    if job_code != AppCode.JOB_RETRIEVED:
        raise RouteInvariantError(code=job_code, request=request)
    if images_code != AppCode.IMAGES_RETRIEVED:
        raise RouteInvariantError(code=images_code, request=request)
    raise RouteInvariantError(code=engine_code, request=request)


def prepare_job_data(*, db_job: model.Job, db_images: List[model.Image], key: model.Key, db_engine: Optional[model.Engine] = None):
    job = base_objects.JobProper.model_validate(db_job).model_dump()
    images = [base_objects.Image.model_validate(img).model_dump() for img in db_images]

    # drop everything that is only for admins
    if key.role not in {base_objects.KeyRole.ADMIN, base_objects.KeyRole.WORKER}:
        job.pop("log", None)
        for img in images:
            img.pop("id", None)

    if db_engine is None:
        data = base_objects.Job(**job, images=images)
    else:
        engine_id = db_engine.id
        engine_files_updated = db_engine.files_updated
        engine_definition = db_engine.definition
        if key.role not in {base_objects.KeyRole.ADMIN, base_objects.KeyRole.WORKER}:
            engine_id = None
            engine_definition = None
            engine_files_updated = None
        data = base_objects.Job(**job,
                                images=images,
                                engine_name=db_engine.name,
                                engine_version=db_engine.version,
                                engine_id=engine_id,
                                engine_files_updated=engine_files_updated,
                                engine_definition=engine_definition)

    return data


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
        "model": DocAPIResponseOK[base_objects.JobLease],
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
    AppCode.JOB_MARKED_ERROR: {
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
    AppCode.JOB_ALREADY_MARKED_ERROR: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Job was already marked as error.",
        "model": DocAPIResponseOK,
        "detail": "Job was already marked as error.",
    }
}
@root_router.patch(
    "/v1/jobs/{job_id}",
    summary="Update Job",
    response_model=DocAPIResponseOK[NoneType] | DocAPIResponseOK[base_objects.JobLease],
    tags=["User"],
    openapi_extra={"x-order": 104},
    description="Update the status of a specific job. "
                "Users can cancel jobs, while workers can mark jobs as done or error, and update progress.",
    responses=make_responses(PATCH_JOB_RESPONSES))
@challenge_job_exists
@challenge_user_access_to_job
@challenge_worker_access_to_job
async def patch_job(
        request: Request,
        *,
        job_id: UUID,
        job_progress_update: base_objects.JobProgressUpdate =
        Body(...,
            openapi_examples={
                "user_cancelled": {
                    "summary": f"User cancels job",
                    "description": "User marks job as cancelled."
                                   "\n\nOnly `state: cancelled` will be accepted, other fields will be ignored."
                                   "\n\nJob must be in one of the following "
                                   f"`state: {base_objects.ProcessingState.NEW}|{base_objects.ProcessingState.QUEUED}|{base_objects.ProcessingState.PROCESSING}|{base_objects.ProcessingState.ERROR}`.",
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
                        f"use [`PATCH /v1/jobs/{{job_id}}/lease`]({config.APP_BASE_URL}/docs#/Worker/9999-patch_job_v1_jobs__job_id__patch). "
                    ),
                    "value": {
                        "progress": 0.7,
                        "log": "Processed 700/1000.",
                        "log_user": "Processing page 700 of 1000.",
                    },
                }
            }
        ),
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.USER, base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):

    db_job, code_get_job = await general_cruds.get_job(db=db, job_id=job_id)
    code_update_job = None

    # user guards
    if key.role == base_objects.KeyRole.USER:
        if job_progress_update.state != base_objects.ProcessingState.CANCELLED:
            raise DocAPIClientErrorException(
                status=status.HTTP_403_FORBIDDEN,
                code=AppCode.API_KEY_USER_FORBIDDEN,
                detail=PATCH_JOB_RESPONSES[AppCode.API_KEY_USER_FORBIDDEN]["detail"]
            )

        if db_job.state in {base_objects.ProcessingState.CANCELLED,
                            base_objects.ProcessingState.DONE,
                            base_objects.ProcessingState.FAILED}:
            raise DocAPIClientErrorException(
                status=status.HTTP_409_CONFLICT,
                code=AppCode.JOB_UNCANCELLABLE,
                detail=PATCH_JOB_RESPONSES[AppCode.JOB_UNCANCELLABLE]["detail"],
                details={"state": f"{db_job.state.value}"}
            )

    # worker guards
    if key.role == base_objects.KeyRole.WORKER:
        if job_progress_update.state not in {
            base_objects.ProcessingState.DONE,
            base_objects.ProcessingState.ERROR,
            None
        }:
            raise DocAPIClientErrorException(
                status=status.HTTP_403_FORBIDDEN,
                code=AppCode.API_KEY_WORKER_FORBIDDEN,
                detail=PATCH_JOB_RESPONSES[AppCode.API_KEY_WORKER_FORBIDDEN]["detail"]
            )

        if job_progress_update.state in {base_objects.ProcessingState.DONE, base_objects.ProcessingState.ERROR} and \
                db_job.state not in {base_objects.ProcessingState.PROCESSING, base_objects.ProcessingState.DONE, base_objects.ProcessingState.ERROR}:
            raise DocAPIClientErrorException(
                status=status.HTTP_409_CONFLICT,
                code=AppCode.JOB_UNFINISHABLE,
                detail=PATCH_JOB_RESPONSES[AppCode.JOB_UNFINISHABLE]["detail"],
                details={"state": f"{db_job.state.value}"}
            )

        if job_progress_update.state == base_objects.ProcessingState.DONE:
            result_path = os.path.join(config.RESULTS_DIR, f"{job_id}.zip")
            if not await aiofiles_os.path.exists(result_path):
                raise DocAPIClientErrorException(
                    status=fastapi.status.HTTP_409_CONFLICT,
                    code=AppCode.JOB_RESULT_MISSING,
                    detail=PATCH_JOB_RESPONSES[AppCode.JOB_RESULT_MISSING]["detail"],
                )

        if job_progress_update.state is None:
            if job_progress_update.progress is None and \
                    job_progress_update.log is None and \
                    job_progress_update.log_user is None:
                raise DocAPIClientErrorException(
                    status=status.HTTP_400_BAD_REQUEST,
                    code=AppCode.JOB_UPDATE_NO_FIELDS,
                    detail=PATCH_JOB_RESPONSES[AppCode.JOB_UPDATE_NO_FIELDS]["detail"]
                )

    # at this point we know the request is valid

    if key.role in {base_objects.KeyRole.USER, base_objects.KeyRole.ADMIN}:
        code_update_job = await user_cruds.cancel_job(db, job_id)
        if code_update_job == AppCode.JOB_CANCELLED:
            return DocAPIResponseOK[NoneType](
                status=status.HTTP_200_OK,
                code=AppCode.JOB_CANCELLED,
                detail=PATCH_JOB_RESPONSES[AppCode.JOB_CANCELLED]["detail"]
            )

    if key.role in {base_objects.KeyRole.WORKER, base_objects.KeyRole.ADMIN}:
        db_job, lease_expire_at, server_time, code_update_job = await worker_cruds.update_job_progress(
            db=db,
            job_id=job_id,
            job_progress_update=job_progress_update
        )
        if job_progress_update.state == base_objects.ProcessingState.DONE:
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

        elif job_progress_update.state == base_objects.ProcessingState.ERROR:
            if code_update_job == AppCode.JOB_MARKED_ERROR:
                return DocAPIResponseOK[NoneType](
                    status=fastapi.status.HTTP_200_OK,
                    code=AppCode.JOB_MARKED_ERROR,
                    detail=PATCH_JOB_RESPONSES[AppCode.JOB_MARKED_ERROR]["detail"]
                )
            elif code_update_job == AppCode.JOB_ALREADY_MARKED_ERROR:
                return DocAPIResponseOK[NoneType](
                    status=fastapi.status.HTTP_200_OK,
                    code=AppCode.JOB_ALREADY_MARKED_ERROR,
                    detail=PATCH_JOB_RESPONSES[AppCode.JOB_ALREADY_MARKED_ERROR]["detail"]
                )

        elif job_progress_update.state is None:
            if code_update_job == AppCode.JOB_UPDATED:
                return DocAPIResponseOK[base_objects.JobLease](
                    status=fastapi.status.HTTP_200_OK,
                    code=AppCode.JOB_UPDATED,
                    detail=PATCH_JOB_RESPONSES[AppCode.JOB_UPDATED]["detail"],
                    data=base_objects.JobLease(id=job_id, lease_expire_at=lease_expire_at, server_time=server_time)
                )

    raise RouteInvariantError(code=code_get_job if code_update_job is None else code_update_job, request=request)