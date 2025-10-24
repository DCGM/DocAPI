import logging
from types import NoneType

import fastapi
from fastapi import Depends, Request

from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.authentication import require_api_key
from doc_api.api.cruds import admin_cruds
from doc_api.api.database import get_async_session
from doc_api.api.routes.helper import RouteInvariantError
from doc_api.api.schemas import base_objects
from doc_api.api.schemas.responses import make_responses, DocAPIResponseOK, AppCode, DocAPIClientErrorException, \
    validate_ok_response, DocAPIResponseClientError
from doc_api.db import model
from doc_api.api.routes import admin_router

from typing import List


logger = logging.getLogger(__name__)


GET_KEYS_RESPONSES = {
    AppCode.KEYS_RETRIEVED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "The list of API keys was retrieved successfully.",
        "model": DocAPIResponseOK,
        "model_data": List[base_objects.Key],
        "detail": "API keys retrieved successfully.",
    }
}
@admin_router.get(
    "/keys",
    summary="Get Keys",
    response_model=DocAPIResponseOK[List[base_objects.Key]],
    tags=["Admin"],
    description="Retrieve a list of all API keys in the system.",
    responses=make_responses(GET_KEYS_RESPONSES))
async def get_keys(
        key: model.Key = Depends(require_api_key()),
        db: AsyncSession = Depends(get_async_session)):
    db_keys, code = await admin_cruds.get_keys(db=db)
    return DocAPIResponseOK[List[base_objects.Key]](
        status=fastapi.status.HTTP_200_OK,
        code=AppCode.KEYS_RETRIEVED,
        detail="API keys retrieved successfully.",
        data=db_keys
    )


POST_KEY_RESPONSES = {
    AppCode.KEY_CREATED: {
        "status": fastapi.status.HTTP_201_CREATED,
        "description": "A new API key was created successfully.",
        "model": DocAPIResponseOK,
        "detail": "API key created successfully, secret: {key_str}",
    },
    AppCode.KEY_ALREADY_EXISTS: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": "An API key with the specified label already exists.",
        "model": DocAPIResponseClientError,
        "detail": "An API key with the specified label already exists.",
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
    tags=["Admin"],
    description="Create a new API key with the specified label and role.",
    status_code=fastapi.status.HTTP_201_CREATED,
    responses=make_responses(POST_KEY_RESPONSES))
async def post_key(
        request: Request,
        key_new: base_objects.KeyNew,
        key: model.Key = Depends(require_api_key()),
        db: AsyncSession = Depends(get_async_session)):

    key_str, code = await admin_cruds.new_key(db=db, key_new=key_new)

    if code == AppCode.KEY_CREATED:
        return validate_ok_response(DocAPIResponseOK[NoneType](
            status=fastapi.status.HTTP_201_CREATED,
            code=AppCode.KEY_CREATED,
            detail=POST_KEY_RESPONSES[AppCode.KEY_CREATED]["detail"].format(key_str=key_str),
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
        "model": DocAPIResponseOK,
        "detail": "New API key secret created successfully: {key_str}",
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
    tags=["Admin"],
    description="Create new secrets for an existing API key. The old secrets will be invalidated.",
    status_code=fastapi.status.HTTP_201_CREATED,
    responses=make_responses(POST_KEY_SECRET_RESPONSES))
async def post_key_secrets(
        request: Request,
        label: str,
        key: model.Key = Depends(require_api_key()),
        db: AsyncSession = Depends(get_async_session)):

    key_str, code = await admin_cruds.new_secret(db=db, label=label)

    if code == AppCode.KEY_SECRET_CREATED:
        return validate_ok_response(DocAPIResponseOK[NoneType](
            status=fastapi.status.HTTP_201_CREATED,
            code=AppCode.KEY_SECRET_CREATED,
            detail=POST_KEY_SECRET_RESPONSES[AppCode.KEY_SECRET_CREATED]["detail"].format(key_str=key_str),
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
    AppCode.KEY_ALREADY_EXISTS: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": "An API key with the specified label already exists.",
        "model": DocAPIResponseClientError,
        "detail": "An API key with the specified label already exists.",
    },
    AppCode.KEY_NOT_FOUND: {
        "status": fastapi.status.HTTP_404_NOT_FOUND,
        "description": "The specified API key was not found.",
        "model": DocAPIResponseClientError,
        "detail": "The specified API key was not found.",
    }
}
@admin_router.patch(
    "/keys/{label}",
    summary="Update Key",
    response_model=DocAPIResponseOK[NoneType],
    tags=["Admin"],
    description="Update the label, role, or active status of an existing API key.",
    responses=make_responses(PATCH_KEY_RESPONSES))
async def patch_key(
        request: Request,
        key_update: base_objects.KeyUpdate,
        key: model.Key = Depends(require_api_key()),
        db: AsyncSession = Depends(get_async_session)):

    if key_update.label is None and \
         key_update.role is None and \
            key_update.active is None:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_400_BAD_REQUEST,
            code=AppCode.KEY_UPDATE_NO_FIELDS,
            detail=PATCH_KEY_RESPONSES[AppCode.KEY_UPDATE_NO_FIELDS]["detail"]
        )

    code = await admin_cruds.update_key(db=db, key_update=key_update)

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