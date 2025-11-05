import logging
import os.path
import zipfile
from types import NoneType
from uuid import UUID

import aiofiles
import fastapi
from fastapi import Depends, Request, UploadFile, File
from fastapi.responses import FileResponse


from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.authentication import require_admin_key
from doc_api.api.cruds import admin_cruds, general_cruds
from doc_api.api.database import get_async_session
from doc_api.api.guards.general_guards import challenge_job_exists
from doc_api.api.routes.helper import RouteInvariantError
from doc_api.api.schemas import base_objects
from doc_api.api.schemas.responses import make_responses, DocAPIResponseOK, AppCode, DocAPIClientErrorException, \
    validate_ok_response, DocAPIResponseClientError, GENERAL_RESPONSES
from doc_api.config import config
from doc_api.db import model
from doc_api.api.routes import admin_router
from aiofiles import os as aiofiles_os

from typing import List


logger = logging.getLogger(__name__)


POST_KEY_RESPONSES = {
    AppCode.KEY_CREATED: {
        "status": fastapi.status.HTTP_201_CREATED,
        "description": "API key created successfully.",
        "model": DocAPIResponseOK[base_objects.KeySecret],
        "model_data": base_objects.KeySecret,
        "detail": "API key created successfully.",
    },
    AppCode.KEY_ALREADY_EXISTS: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": "API key with the specified label already exists.",
        "model": DocAPIResponseClientError,
        "detail": "API key with the specified label already exists.",
    },
    AppCode.KEY_CREATION_FAILED: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": "Failed to create a new API key due to hash collision after multiple attempts.",
        "model": DocAPIResponseClientError,
        "detail": "Failed to create a new API key due to hash collision after multiple attempts.",
    }
}
@admin_router.post(
    "/keys",
    summary="Create Key",
    response_model=DocAPIResponseOK[base_objects.KeySecret],
    tags=["Admin"],
    openapi_extra={"x-order": 300},
    description="Create a new API key with the specified label and role.",
    status_code=fastapi.status.HTTP_201_CREATED,
    responses=make_responses(POST_KEY_RESPONSES))
async def post_key(
        request: Request,
        key_new: base_objects.KeyNew,
        key: model.Key = Depends(require_admin_key),
        db: AsyncSession = Depends(get_async_session)):

    secret, code = await admin_cruds.new_key(db=db, key_new=key_new)

    if code == AppCode.KEY_CREATED:
        return validate_ok_response(DocAPIResponseOK[base_objects.KeySecret](
            status=fastapi.status.HTTP_201_CREATED,
            code=AppCode.KEY_CREATED,
            detail=POST_KEY_RESPONSES[AppCode.KEY_CREATED]["detail"],
            data=base_objects.KeySecret(
                secret=secret
            )
        ))
    elif code == AppCode.KEY_ALREADY_EXISTS:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_409_CONFLICT,
            code=AppCode.KEY_ALREADY_EXISTS,
            detail=POST_KEY_RESPONSES[AppCode.KEY_ALREADY_EXISTS]["detail"]
        )
    elif code == AppCode.KEY_CREATION_FAILED:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_409_CONFLICT,
            code=AppCode.KEY_CREATION_FAILED,
            detail=POST_KEY_RESPONSES[AppCode.KEY_CREATION_FAILED]["detail"]
        )

    raise RouteInvariantError(request=request, code=code)


POST_KEY_SECRET_RESPONSES = {
    AppCode.KEY_SECRET_CREATED: {
        "status": fastapi.status.HTTP_201_CREATED,
        "description": "New secret for the API key were created successfully.",
        "model": DocAPIResponseOK[base_objects.KeySecret],
        "model_data": base_objects.KeySecret,
        "detail": "New API key secret created successfully.",
    },
    AppCode.KEY_NOT_FOUND: {
        "status": fastapi.status.HTTP_404_NOT_FOUND,
        "description": "The specified API key was not found.",
        "model": DocAPIResponseClientError,
        "detail": "The specified API key was not found.",
    },
    AppCode.KEY_SECRET_CREATION_FAILED: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": "Failed to create a new secret for the API key due to hash collision after multiple attempts.",
        "model": DocAPIResponseClientError,
        "detail": "Failed to create a new secret for the API key due to hash collision after multiple attempts.",
    }
}
@admin_router.post(
    "/keys/{label}/secret",
    summary="Create Key Secret",
    response_model=DocAPIResponseOK[base_objects.KeySecret],
    tags=["Admin"],
    openapi_extra={"x-order": 301},
    description="Create new secrets for an existing API key. The old secrets will be invalidated.",
    status_code=fastapi.status.HTTP_201_CREATED,
    responses=make_responses(POST_KEY_SECRET_RESPONSES))
async def post_key_secret(
        request: Request,
        label: str,
        key: model.Key = Depends(require_admin_key),
        db: AsyncSession = Depends(get_async_session)):

    secret, code = await admin_cruds.new_secret(db=db, label=label)

    if code == AppCode.KEY_SECRET_CREATED:
        return validate_ok_response(DocAPIResponseOK[base_objects.KeySecret](
            status=fastapi.status.HTTP_201_CREATED,
            code=AppCode.KEY_SECRET_CREATED,
            detail=POST_KEY_SECRET_RESPONSES[AppCode.KEY_SECRET_CREATED]["detail"],
            data=base_objects.KeySecret(
                secret=secret
            )
        ))
    elif code == AppCode.KEY_SECRET_CREATION_FAILED:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_409_CONFLICT,
            code=AppCode.KEY_SECRET_CREATION_FAILED,
            detail=POST_KEY_SECRET_RESPONSES[AppCode.KEY_SECRET_CREATION_FAILED]["detail"]
        )
    elif code == AppCode.KEY_NOT_FOUND:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_404_NOT_FOUND,
            code=AppCode.KEY_NOT_FOUND,
            detail=POST_KEY_SECRET_RESPONSES[AppCode.KEY_NOT_FOUND]["detail"]
        )

    raise RouteInvariantError(request=request, code=code)


PATCH_KEY_RESPONSES = {
    AppCode.KEY_UPDATED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "API key was updated successfully.",
        "model": DocAPIResponseOK,
        "detail": "API key was updated successfully.",
    },
    AppCode.KEY_UPDATE_NO_FIELDS: {
        "status": fastapi.status.HTTP_400_BAD_REQUEST,
        "description": "No fields were provided to update the API key.",
        "model": DocAPIResponseClientError,
        "detail": "At least one field must be provided to update the API key.",
    },
    AppCode.KEY_NOT_FOUND: {
        "status": fastapi.status.HTTP_404_NOT_FOUND,
        "description": "The specified API key was not found.",
        "model": DocAPIResponseClientError,
        "detail": "The specified API key was not found.",
    },
    AppCode.KEY_ALREADY_EXISTS: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": "An API key with the specified label already exists.",
        "model": DocAPIResponseClientError,
        "detail": "An API key with the specified label already exists.",
    }
}
@admin_router.patch(
    "/keys/{label}",
    summary="Update Key",
    response_model=DocAPIResponseOK[NoneType],
    tags=["Admin"],
    openapi_extra={"x-order": 302},
    description="Update the label, role, or active status of an existing API key.",
    responses=make_responses(PATCH_KEY_RESPONSES))
async def patch_key(
        request: Request,
        label: str,
        key_update: base_objects.KeyUpdate,
        key: model.Key = Depends(require_admin_key),
        db: AsyncSession = Depends(get_async_session)):

    if key_update.label is None and \
         key_update.role is None and \
            key_update.active is None:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_400_BAD_REQUEST,
            code=AppCode.KEY_UPDATE_NO_FIELDS,
            detail=PATCH_KEY_RESPONSES[AppCode.KEY_UPDATE_NO_FIELDS]["detail"]
        )

    code = await admin_cruds.update_key(db=db, label=label, key_update=key_update)

    if code == AppCode.KEY_UPDATED:
        return validate_ok_response(DocAPIResponseOK[NoneType](
            status=fastapi.status.HTTP_200_OK,
            code=AppCode.KEY_UPDATED,
            detail=PATCH_KEY_RESPONSES[AppCode.KEY_UPDATED]["detail"],
        ))
    elif code == AppCode.KEY_ALREADY_EXISTS:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_409_CONFLICT,
            code=AppCode.KEY_ALREADY_EXISTS,
            detail=PATCH_KEY_RESPONSES[AppCode.KEY_ALREADY_EXISTS]["detail"]
        )
    elif code == AppCode.KEY_NOT_FOUND:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_404_NOT_FOUND,
            code=AppCode.KEY_NOT_FOUND,
            detail=PATCH_KEY_RESPONSES[AppCode.KEY_NOT_FOUND]["detail"]
        )

    raise RouteInvariantError(request=request, code=code)


GET_KEYS_RESPONSES = {
    AppCode.KEYS_RETRIEVED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "The list of API keys was retrieved successfully.",
        "model": DocAPIResponseOK[List[base_objects.Key]],
        "model_data": List[base_objects.Key],
        "detail": "API keys retrieved successfully.",
    }
}
@admin_router.get(
    "/keys",
    summary="Get Keys",
    response_model=DocAPIResponseOK[List[base_objects.Key]],
    tags=["Admin"],
    openapi_extra={"x-order": 303},
    description="Retrieve a list of all API keys in the system.",
    responses=make_responses(GET_KEYS_RESPONSES))
async def get_keys(
        key: model.Key = Depends(require_admin_key),
        db: AsyncSession = Depends(get_async_session)):
    db_keys, code = await admin_cruds.get_keys(db=db)
    return DocAPIResponseOK[List[base_objects.Key]](
        status=fastapi.status.HTTP_200_OK,
        code=AppCode.KEYS_RETRIEVED,
        detail="API keys retrieved successfully.",
        data=db_keys
    )


POST_ENGINE_RESPONSES = {
    AppCode.ENGINE_CREATED: {
        "status": fastapi.status.HTTP_201_CREATED,
        "description": "Engine created successfully.",
        "model": DocAPIResponseOK,
        "detail": "Engine created successfully.",
    },
    AppCode.ENGINE_ALREADY_EXISTS: GENERAL_RESPONSES[AppCode.ENGINE_ALREADY_EXISTS]
}
@admin_router.post(
    "/engines",
    summary="Create Engine",
    response_model=DocAPIResponseOK[base_objects.Engine],
    tags=["Admin"],
    openapi_extra={"x-order": 304},
    description="Create a new engine.",
    status_code=fastapi.status.HTTP_201_CREATED)
async def post_engine(
        request: Request,
        engine_new: base_objects.EngineNew,
        key: model.Key = Depends(require_admin_key),
        db: AsyncSession = Depends(get_async_session)):

    db_engine, code = await admin_cruds.new_engine(db=db, engine_new=engine_new)

    if code == AppCode.ENGINE_CREATED:
        return validate_ok_response(DocAPIResponseOK[base_objects.Engine](
            status=fastapi.status.HTTP_201_CREATED,
            code=AppCode.ENGINE_CREATED,
            detail=POST_ENGINE_RESPONSES[AppCode.ENGINE_CREATED]["detail"]
        ))
    elif code == AppCode.ENGINE_ALREADY_EXISTS:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_409_CONFLICT,
            code=AppCode.ENGINE_ALREADY_EXISTS,
            detail=POST_ENGINE_RESPONSES[AppCode.ENGINE_ALREADY_EXISTS]["detail"]
        )

    raise RouteInvariantError(request=request, code=code)


PATCH_ENGINE_RESPONSES = {
    AppCode.ENGINE_UPDATED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Engine was updated successfully.",
        "model": DocAPIResponseOK,
        "detail": "Engine was updated successfully.",
    },
    AppCode.ENGINE_UPDATE_NO_FIELDS: {
        "status": fastapi.status.HTTP_400_BAD_REQUEST,
        "description": "No fields were provided to update the engine.",
        "model": DocAPIResponseClientError,
        "detail": "At least one field must be provided to update the engine.",
    },
    AppCode.ENGINE_NOT_FOUND: GENERAL_RESPONSES[AppCode.ENGINE_NOT_FOUND],
    AppCode.ENGINE_ALREADY_EXISTS: GENERAL_RESPONSES[AppCode.ENGINE_ALREADY_EXISTS]
}
@admin_router.patch(
    "/engines/{name}/{version}",
    summary="Update Engine",
    response_model=DocAPIResponseOK[NoneType],
    tags=["Admin"],
    openapi_extra={"x-order": 305},
    description="Update an existing engine.",
    responses=make_responses(PATCH_ENGINE_RESPONSES))
async def patch_engine(
        request: Request,
        name: str,
        version: str,
        engine_update: base_objects.EngineUpdate,
        key: model.Key = Depends(require_admin_key),
        db: AsyncSession = Depends(get_async_session)):

    if engine_update.name is None and \
       engine_update.version is None and \
       engine_update.description is None and \
       engine_update.definition is None and \
       engine_update.default is None and \
       engine_update.active is None:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_400_BAD_REQUEST,
            code=AppCode.ENGINE_UPDATE_NO_FIELDS,
            detail=PATCH_ENGINE_RESPONSES[AppCode.ENGINE_UPDATE_NO_FIELDS]["detail"]
        )

    code = await admin_cruds.update_engine(db=db, engine_name=name, engine_version=version, engine_update=engine_update)

    if code == AppCode.ENGINE_UPDATED:
        return validate_ok_response(DocAPIResponseOK[NoneType](
            status=fastapi.status.HTTP_200_OK,
            code=AppCode.ENGINE_UPDATED,
            detail=PATCH_ENGINE_RESPONSES[AppCode.ENGINE_UPDATED]["detail"],
        ))
    elif code == AppCode.ENGINE_NOT_FOUND:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_404_NOT_FOUND,
            code=AppCode.ENGINE_NOT_FOUND,
            detail=PATCH_ENGINE_RESPONSES[AppCode.ENGINE_NOT_FOUND]["detail"]
        )
    elif code == AppCode.ENGINE_ALREADY_EXISTS:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_409_CONFLICT,
            code=AppCode.ENGINE_ALREADY_EXISTS,
            detail=PATCH_ENGINE_RESPONSES[AppCode.ENGINE_ALREADY_EXISTS]["detail"]
        )

    raise RouteInvariantError(request=request, code=code)


POST_ENGINE_FILES_RESPONSES = {
    AppCode.ENGINE_FILES_UPLOADED: {
        "status": fastapi.status.HTTP_201_CREATED,
        "description": "Engine files ZIP archive uploaded successfully.",
        "model": DocAPIResponseOK,
        "detail": "Engine files ZIP archive uploaded successfully.",
    },
    AppCode.ENGINE_FILES_REUPLOADED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Engine files ZIP archive re-uploaded successfully.",
        "model": DocAPIResponseOK,
        "detail": "Engine files ZIP archive re-uploaded successfully.",
    },
    AppCode.ENGINE_FILES_INVALID: {
        "status": fastapi.status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
        "description": "The uploaded file is not a valid ZIP archive.",
        "model": DocAPIResponseClientError,
        "detail": "The uploaded file is not a valid ZIP archive.",
    },
    AppCode.ENGINE_NOT_FOUND: GENERAL_RESPONSES[AppCode.ENGINE_NOT_FOUND]
}
@admin_router.put(
    "/engines/{name}/{version}/files",
    response_model=DocAPIResponseOK,
    summary="Upload Engine Files",
    tags=["Admin"],
    openapi_extra={"x-order": 306},
    description="Upload the engine files ZIP archive. "
                "The uploaded file must be a `.zip`.",
    status_code=fastapi.status.HTTP_201_CREATED,
    responses=make_responses(POST_ENGINE_FILES_RESPONSES))
async def post_engine_files(
        request: Request,
        name: str,
        version: str,
        file: UploadFile = File(..., description="ZIP archive containing the engine files."),
        key: model.Key = Depends(require_admin_key),
        db: AsyncSession = Depends(get_async_session),
):
    db_engine, code = await general_cruds.get_engine_by_name_and_version(
        db=db, engine_name=name, engine_version=version)

    if code == AppCode.ENGINE_RETRIEVED:
        await aiofiles_os.makedirs(config.ENGINES_DIR, exist_ok=True)
        final_path = os.path.join(config.ENGINES_DIR, f"{db_engine.id}.zip")
        tmp_path = final_path + ".validating"

        async with aiofiles.open(tmp_path, "wb") as f:
            while chunk := await file.read(1024 * 1024):
                await f.write(chunk)

        if config.ENGINE_FILES_ZIP_VALIDATION:
            try:
                with zipfile.ZipFile(tmp_path):
                    pass
            except zipfile.BadZipFile as e:
                await aiofiles_os.remove(tmp_path)
                raise DocAPIClientErrorException(
                    status=fastapi.status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                    code=AppCode.ENGINE_FILES_INVALID,
                    detail=POST_ENGINE_FILES_RESPONSES[AppCode.ENGINE_FILES_INVALID]["detail"],
                    details={"reason": str(e)}
                )

        already_exists = await aiofiles_os.path.exists(final_path)
        os.replace(tmp_path, final_path)

        code = await admin_cruds.update_engine_files_updated(db=db, engine_id=db_engine.id)
        if code == AppCode.ENGINE_UPDATED:
            if already_exists:
                return validate_ok_response(DocAPIResponseOK[NoneType](
                    status=fastapi.status.HTTP_200_OK,
                    code=AppCode.ENGINE_FILES_REUPLOADED,
                    detail=POST_ENGINE_FILES_RESPONSES[AppCode.ENGINE_FILES_REUPLOADED]["detail"]
                ))
            else:
                return validate_ok_response(DocAPIResponseOK[NoneType](
                    status=fastapi.status.HTTP_201_CREATED,
                    code=AppCode.ENGINE_FILES_UPLOADED,
                    detail=POST_ENGINE_FILES_RESPONSES[AppCode.ENGINE_FILES_UPLOADED]["detail"]
                ))

    elif code == AppCode.ENGINE_NOT_FOUND:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_404_NOT_FOUND,
            code=AppCode.ENGINE_NOT_FOUND,
            detail=POST_ENGINE_FILES_RESPONSES[AppCode.ENGINE_NOT_FOUND]["detail"]
        )

    raise RouteInvariantError(request=request, code=code)


GET_ARTIFACTS_RESPONSES = {
    AppCode.JOB_ARTIFACTS_RETRIEVED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Job artifacts retrieved successfully.",
        "content_type": "application/zip",
        "example_value": "(binary zip file content)",
    },
    AppCode.JOB_ARTIFACTS_NOT_FOUND: {
        "status": fastapi.status.HTTP_404_NOT_FOUND,
        "description": "The specified job artifacts were not found.",
        "model": DocAPIResponseClientError,
        "detail": "The specified job artifacts were not found.",
    }
}
@admin_router.get(
    "/jobs/{job_id}/artifacts",
    summary="Download Artifacts",
    response_class=FileResponse,
    tags=["Admin"],
    openapi_extra={"x-order": 307},
    description="Retrieve artifacts for a job.",
    responses=make_responses(GET_ARTIFACTS_RESPONSES))
@challenge_job_exists
async def get_artifacts(
        route_request: fastapi.Request,
        job_id: UUID,
        key: model.Key = Depends(require_admin_key),
        db: AsyncSession = Depends(get_async_session)):

    artifacts_file_path = os.path.join(config.ARTIFACTS_DIR, f"{job_id}.zip")
    if not await aiofiles_os.path.exists(artifacts_file_path):
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_404_NOT_FOUND,
            code=AppCode.JOB_ARTIFACTS_NOT_FOUND,
            detail=GET_ARTIFACTS_RESPONSES[AppCode.JOB_ARTIFACTS_NOT_FOUND]["detail"],
        )

    return FileResponse(
            artifacts_file_path,
            media_type="application/zip",
            filename=f"{job_id}.zip",
        )


PATCH_JOB_RESPONSES = {
    AppCode.JOB_UPDATED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Job was updated successfully.",
        "model": DocAPIResponseOK,
        "detail": "Job was updated successfully.",
    }
}
@admin_router.patch(
    "/jobs/{job_id}",
    summary="Update Job",
    response_model=DocAPIResponseOK[NoneType],
    tags=["Admin"],
    openapi_extra={"x-order": 308},
    description="Force update fields of an existing job. Use with caution, can interfere with normal job processing, mainly for debugging purposes.",
    responses=make_responses(PATCH_JOB_RESPONSES))
async def patch_job(
        request: Request,
        job_id: UUID,
        job_update: base_objects.JobUpdate,
        append_logs: bool = True,
        key: model.Key = Depends(require_admin_key),
        db: AsyncSession = Depends(get_async_session)):

    db_job, code = await general_cruds.get_job(db=db, job_id=job_id)

    if code == AppCode.JOB_RETRIEVED:
        code = await admin_cruds.update_job(db=db, job_id=job_id, job_update=job_update, append_logs=append_logs)
        if code == AppCode.JOB_UPDATED:
            return validate_ok_response(DocAPIResponseOK[NoneType](
                status=fastapi.status.HTTP_200_OK,
                code=AppCode.JOB_UPDATED,
                detail=PATCH_JOB_RESPONSES[AppCode.JOB_UPDATED]["detail"],
            ))

    raise RouteInvariantError(request=request, code=code)