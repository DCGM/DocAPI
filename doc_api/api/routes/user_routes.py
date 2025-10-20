import hashlib
import json
import logging
import os
from types import NoneType

import fastapi

import cv2
import numpy as np
from fastapi import Depends, UploadFile, HTTPException, status, Request
from fastapi.responses import FileResponse

from aiofiles import os as aiofiles_os

from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.routes.helper import RouteInvariantError
from doc_api.api.routes.user_guards import challenge_user_access_to_new_job, uses_challenge_user_access_to_new_job, \
    uses_challenge_user_access_to_job, challenge_user_access_to_job
from doc_api.api.authentication import require_api_key
from doc_api.api.cruds import user_cruds, general_cruds
from doc_api.api.database import get_async_session
from doc_api.api.schemas import base_objects
from doc_api.api.schemas.responses import DocAPIResponseClientError, AppCode, DocAPIResponseOK, make_responses, \
    DETAILS_GENERAL, validate_ok_response, DocAPIClientErrorException, GENERAL_RESPONSES
from doc_api.api.validators.alto_validator import validate_alto_basic
from doc_api.api.validators.xml_validator import is_well_formed_xml
from doc_api.db import model
from doc_api.api.routes import user_router
from doc_api.config import config

from typing import List, Optional, Tuple
from uuid import UUID


logger = logging.getLogger(__name__)


ME_RESPONSES = {
    AppCode.API_KEY_VALID: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "The API key is valid.",
        "model": DocAPIResponseOK,
        "model_data": base_objects.Key,
        "detail": "The API key is valid.",
    }
}
@user_router.get(
    "/me",
    summary="Who am I?",
    response_model=DocAPIResponseOK[base_objects.Key],
    tags=["User"],
    description="Validate your API key and get information about it.",
    responses=make_responses(ME_RESPONSES)
)
async def me(key: model.Key = Depends(require_api_key(model.KeyRole.USER, model.KeyRole.WORKER))):
    return DocAPIResponseOK[base_objects.Key](
        status=status.HTTP_200_OK,
        code=AppCode.API_KEY_VALID,
        detail=ME_RESPONSES[AppCode.API_KEY_VALID]["detail"],
        data=key)


POST_JOB_RESPONSES = {
    AppCode.JOB_CREATED: {
        "status": fastapi.status.HTTP_201_CREATED,
        "description": "Job created successfully",
        "model": DocAPIResponseOK,
        "model_data": base_objects.Job,
        "detail": "The job has been created successfully.",
    },
    AppCode.REQUEST_VALIDATION_ERROR: {
        "status": fastapi.status.HTTP_422_UNPROCESSABLE_ENTITY,
        "description": "Input validation failed",
        "model": DocAPIResponseClientError,
        "detail": "Validation of the job definition failed. Please see the documentation for the correct format.",
        "details": [
                        {
                            "loc": ["body", "images", 0, "name"],
                            "msg": "field required",
                            "type": "value_error.missing",
                        },
                        {
                            "loc": ["body", "images", 1, "order"],
                            "msg": "value is not a valid integer",
                            "type": "type_error.integer",
                        },
                        {
                            "loc": ["body", "alto_required"],
                            "msg": "value could not be parsed to a boolean",
                            "type": "type_error.bool",
                        },
                    ]
    }
}
@user_router.post(
    "/job",
    summary="Create Job",
    tags=["User"],
    description="Create a new job with the specified images and options.",
    responses=make_responses(POST_JOB_RESPONSES))
async def create_job(
        request: Request,
        job_definition: user_cruds.JobDefinition,
        key: model.Key = Depends(require_api_key(model.KeyRole.USER)),
        db: AsyncSession = Depends(get_async_session)):
    #TODO check if there are duplicates in image names?
    db_job, code = await user_cruds.create_job(db=db, key_id=key.id, job_definition=job_definition)
    if code == AppCode.JOB_CREATED:
        # FastAPI automatically validates only for 200, so we need to do it manually for 201 here
        return validate_ok_response(DocAPIResponseOK[base_objects.Job](
            status=status.HTTP_201_CREATED,
            code=AppCode.JOB_CREATED,
            detail=POST_JOB_RESPONSES[AppCode.JOB_CREATED]["detail"],
            data=base_objects.Job.model_validate(db_job)))

    raise RouteInvariantError(code=code, request=request)


POST_META_JSON_RESPONSES = {
    AppCode.META_JSON_UPLOADED: {
        "status": fastapi.status.HTTP_201_CREATED,
        "description": "Meta JSON uploaded successfully.",
        "model": DocAPIResponseOK,
        "detail": "The Meta JSON file has been uploaded successfully.",
    },
    AppCode.META_JSON_REUPLOADED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Meta JSON re-uploaded successfully.",
        "model": DocAPIResponseOK,
        "detail": "The Meta JSON file has been re-uploaded successfully.",
    },
    AppCode.META_JSON_NOT_REQUIRED: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": "Meta JSON not required for this job.",
        "model": DocAPIResponseClientError,
        "detail": "Job does not require Meta JSON.",
    }
}
@user_router.post(
    "/meta_json/{job_id}",
    response_model=DocAPIResponseOK[NoneType],
    summary="Upload Meta JSON",
    tags=["User"],
    description="Upload the Meta JSON file for a job.",
    responses=make_responses(POST_META_JSON_RESPONSES)
)
@uses_challenge_user_access_to_new_job
async def upload_meta_json(job_id: UUID, meta_json,
                           key: model.Key = Depends(require_api_key(model.KeyRole.USER)),
                           db: AsyncSession = Depends(get_async_session)):
    await challenge_user_access_to_new_job(db, key, job_id)

    db_job, _ = await general_cruds.get_job(db=db, job_id=job_id)

    if db_job.meta_json_required:
        batch_path = os.path.join(config.JOBS_DIR, str(job_id))
        await aiofiles_os.makedirs(batch_path, exist_ok=True)
        meta_json_path = os.path.join(batch_path, "meta.json")
        # the json should be checked/validated by FastAPI already, open and write it without extra validation
        with open(meta_json_path, "w", encoding="utf-8") as f:
            meta_json_dict = json.loads(meta_json)
            json.dump(meta_json_dict, f, ensure_ascii=False, indent=4)

        # TODO this can potentially lead to inconsistent state if the job start fails after Meta JSON file upload
        job_started = await user_cruds.start_job(db=db, job_id=job_id)

        if not db_job.meta_json_uploaded:
            update_job = base_objects.JobUpdate(meta_json_uploaded=True)
            await general_cruds.update_job(db=db, job_id=job_id, job_update=update_job)
            return validate_ok_response(DocAPIResponseOK[NoneType](
                status=status.HTTP_201_CREATED,
                code=AppCode.META_JSON_UPLOADED,
                detail=POST_META_JSON_RESPONSES[AppCode.META_JSON_UPLOADED]["detail"]
            ))
        else:
            return validate_ok_response(DocAPIResponseOK[NoneType](
                status=status.HTTP_200_OK,
                code=AppCode.META_JSON_REUPLOADED,
                detail=POST_META_JSON_RESPONSES[AppCode.META_JSON_REUPLOADED]["detail"]
            ))
    else:
        raise DocAPIClientErrorException(
            status=status.HTTP_409_CONFLICT,
            code=AppCode.META_JSON_NOT_REQUIRED,
            detail=POST_META_JSON_RESPONSES[AppCode.META_JSON_NOT_REQUIRED]["detail"],
        )


POST_IMAGE_RESPONSES = {
    AppCode.IMAGE_UPLOADED: {
        "status": fastapi.status.HTTP_201_CREATED,
        "description": "Image uploaded successfully.",
        "model": DocAPIResponseOK,
        "detail": "The image file has been uploaded successfully.",
    },
    AppCode.IMAGE_REUPLOADED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Image re-uploaded successfully.",
        "model": DocAPIResponseOK,
        "detail": "The image file has been re-uploaded successfully.",
    },
    AppCode.IMAGE_INVALID: {
        "status": fastapi.status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
        "description": "Invalid image file.",
        "model": DocAPIResponseClientError,
        "detail": "Failed to decode the image file, probably not a valid image.",
    },
    AppCode.IMAGE_NOT_FOUND_FOR_JOB: GENERAL_RESPONSES[AppCode.IMAGE_NOT_FOUND_FOR_JOB]
}
@user_router.post(
    "/image/{job_id}/{image_name}",
    response_model=DocAPIResponseOK[NoneType],
    summary="Upload IMAGE",
    tags=["User"],
    description="Upload an IMAGE file for a specific job and image name.",
    responses=make_responses(POST_IMAGE_RESPONSES))
@uses_challenge_user_access_to_new_job
async def upload_image(
        request: Request,
        job_id: UUID,
        image_name: str,
        file: UploadFile,
        key: model.Key = Depends(require_api_key(model.KeyRole.USER)),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_user_access_to_new_job(db, key, job_id)

    db_image, code = await user_cruds.get_image_by_job_and_name(db=db, job_id=job_id, image_name=image_name)

    if code == AppCode.IMAGE_RETRIEVED:
        raw_input = file.file.read()
        contents = np.asarray(bytearray(raw_input), dtype="uint8")
        image = cv2.imdecode(contents, cv2.IMREAD_COLOR)
        if image is None:
            raise DocAPIClientErrorException(
                status=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                code=AppCode.IMAGE_INVALID,
                detail=POST_IMAGE_RESPONSES[AppCode.IMAGE_INVALID]["detail"]
            )

        imagehash = hashlib.md5(raw_input).hexdigest()

        batch_path = os.path.join(config.JOBS_DIR, str(job_id))
        await aiofiles_os.makedirs(batch_path, exist_ok=True)
        image_path = os.path.join(batch_path, f'{db_image.id}.jpg')
        cv2.imwrite(image_path, image)

        image_already_uploaded = db_image.image_uploaded
        image_update = base_objects.ImageUpdate(image_uploaded=True, imagehash=imagehash)
        await general_cruds.update_image(db=db, image_id=db_image.id, image_update=image_update)

        # TODO this can potentially lead to inconsistent state if the job start fails after image upload
        job_started = await user_cruds.start_job(db=db, job_id=job_id)

        if not image_already_uploaded:
            return DocAPIResponseOK[NoneType](
                status=status.HTTP_201_CREATED,
                code=AppCode.IMAGE_UPLOADED,
                detail=POST_IMAGE_RESPONSES[AppCode.IMAGE_UPLOADED]["detail"]
            )
        else:
            return DocAPIResponseOK[NoneType](
                status=status.HTTP_200_OK,
                code=AppCode.IMAGE_REUPLOADED,
                detail=POST_IMAGE_RESPONSES[AppCode.IMAGE_REUPLOADED]["detail"]
            )

    elif code == AppCode.IMAGE_NOT_FOUND_FOR_JOB:
        raise DocAPIClientErrorException(
            status=status.HTTP_404_NOT_FOUND,
            code=AppCode.IMAGE_NOT_FOUND_FOR_JOB,
            detail=POST_IMAGE_RESPONSES[AppCode.IMAGE_NOT_FOUND_FOR_JOB]["detail"]
        )

    raise RouteInvariantError(code=code, request=request)


POST_ALTO_RESPONSES = {
    AppCode.ALTO_UPLOADED: {
        "status": fastapi.status.HTTP_201_CREATED,
        "description": "ALTO XML uploaded successfully.",
        "model": DocAPIResponseOK,
        "detail": "The ALTO XML file has been uploaded successfully.",
    },
    AppCode.ALTO_REUPLOADED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "ALTO XML re-uploaded successfully.",
        "model": DocAPIResponseOK,
        "detail": "The ALTO XML file has been re-uploaded successfully.",
    },
    AppCode.ALTO_NOT_REQUIRED: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": "ALTO XML not required for this job.",
        "model": DocAPIResponseClientError,
        "detail": "Job does not require ALTO XML.",
    },
    AppCode.XML_PARSE_ERROR: GENERAL_RESPONSES[AppCode.XML_PARSE_ERROR],
    AppCode.ALTO_SCHEMA_INVALID: {
        "status": fastapi.status.HTTP_422_UNPROCESSABLE_CONTENT,
        "description": "ALTO XML does not conform to the required schema.",
        "model": DocAPIResponseClientError,
        "detail": "The ALTO XML file does not conform to the required schema.",
    },
}
@user_router.post(
    "/alto/{job_id}/{name}",
    summary="Upload ALTO XML",
    response_model=DocAPIResponseOK[NoneType],
    description="Upload an ALTO XML file for a specific job and image name.",
    tags=["User"],
responses=make_responses(POST_ALTO_RESPONSES))
@uses_challenge_user_access_to_new_job
async def upload_alto(
        request: Request,
        job_id: UUID,
        image_name: str,
        file: UploadFile,
        key: model.Key = Depends(require_api_key(model.KeyRole.USER)),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_user_access_to_new_job(db, key, job_id)

    db_job, _ = await general_cruds.get_job(db=db, job_id=job_id)
    if not db_job.alto_required:
        raise DocAPIClientErrorException(
            status=status.HTTP_409_CONFLICT,
            code=AppCode.ALTO_NOT_REQUIRED,
            detail=POST_ALTO_RESPONSES[AppCode.ALTO_NOT_REQUIRED]["detail"],
        )

    db_image, code = await user_cruds.get_image_by_job_and_name(db=db, job_id=job_id, image_name=image_name)
    if code != AppCode.IMAGE_RETRIEVED:
        data = await file.read()
        if not is_well_formed_xml(data):
            raise DocAPIClientErrorException(
                status=status.HTTP_400_BAD_REQUEST,
                code=AppCode.XML_PARSE_ERROR,
                detail=POST_ALTO_RESPONSES[AppCode.XML_PARSE_ERROR]["detail"],
            )
        alto_checks = validate_alto_basic(data)
        for check_type, check_val in alto_checks.items():
            if config.ALTO_VALIDATION[check_type] and not check_val:
                raise DocAPIClientErrorException(
                    status=status.HTTP_422_UNPROCESSABLE_CONTENT,
                    code=AppCode.ALTO_SCHEMA_INVALID,
                    detail=POST_ALTO_RESPONSES[AppCode.ALTO_SCHEMA_INVALID]["detail"],
                )

        batch_path = os.path.join(config.JOBS_DIR, str(job_id))
        await aiofiles_os.makedirs(batch_path, exist_ok=True)
        alto_path = os.path.join(batch_path, f"{db_image.id}.xml")

        with open(alto_path, "wb") as f:
            f.write(data)

        # TODO this can potentially lead to inconsistent state if the job start fails after ALTO upload
        job_started = await user_cruds.start_job(db=db, job_id=job_id)

        if not db_image.alto_uploaded:
            image_update = base_objects.ImageUpdate(alto_uploaded=True)
            await general_cruds.update_image(db=db, image_id=db_image.id, image_update=image_update)
            return validate_ok_response(DocAPIResponseOK[NoneType](
                status=status.HTTP_201_CREATED,
                code=AppCode.ALTO_UPLOADED,
                detail=POST_ALTO_RESPONSES[AppCode.ALTO_UPLOADED]["detail"]
            ))
        else:
            return validate_ok_response(DocAPIResponseOK[NoneType](
                status=status.HTTP_200_OK,
                code=AppCode.ALTO_REUPLOADED,
                detail=POST_ALTO_RESPONSES[AppCode.ALTO_REUPLOADED]["detail"]
            ))

    elif code == AppCode.IMAGE_NOT_FOUND_FOR_JOB:
        raise DocAPIClientErrorException(
            status=status.HTTP_404_NOT_FOUND,
            code=AppCode.IMAGE_NOT_FOUND_FOR_JOB,
            detail=POST_IMAGE_RESPONSES[AppCode.IMAGE_NOT_FOUND_FOR_JOB]["detail"]
        )

    raise RouteInvariantError(code=code, request=request)

GET_JOB_RESPONSES = {
    AppCode.JOB_RETRIEVED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Job details retrieved successfully.",
        "model": DocAPIResponseOK,
        "model_data": base_objects.Job,
        "detail": "The job details have been retrieved successfully.",
    }
}
@user_router.get(
    "/job/{job_id}",
    summary="Get Job",
    response_model=DocAPIResponseOK[base_objects.Job],
    tags=["User"],
    description="Retrieve the details of a specific job by its ID.",
    responses=make_responses({}))
@uses_challenge_user_access_to_job
async def get_job(job_id: UUID,
                  key: model.Key = Depends(require_api_key(model.KeyRole.USER)),
                  db: AsyncSession = Depends(get_async_session)):
    await challenge_user_access_to_job(db, key, job_id)

    db_job = await general_cruds.get_job(db=db, job_id=job_id)

    return DocAPIResponseOK[base_objects.Job](
        status=status.HTTP_200_OK,
        code=AppCode.JOB_RETRIEVED,
        detail=GET_JOB_RESPONSES[AppCode.JOB_RETRIEVED]["detail"],
        data=db_job
    )


GET_IMAGES_FOR_JOB_RESPONSES = {
    AppCode.IMAGES_RETRIEVED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Job images retrieved successfully.",
        "model": DocAPIResponseOK,
        "model_data": List[base_objects.Image],
        "detail": "The images for the job have been retrieved successfully.",
    }
}
@user_router.get(
    "/images/{job_id}",
    summary="Get Job Images",
    response_model=DocAPIResponseOK[List[base_objects.Image]],
    tags=["User"],
    description="Retrieve all images associated for a specific job.",
    responses=make_responses({}))
@uses_challenge_user_access_to_job
async def get_images(
        job_id: UUID,
        key: model.Key = Depends(require_api_key(model.KeyRole.USER)),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_user_access_to_job(db, key, job_id)

    db_images, code = await general_cruds.get_job_images(db=db, job_id=job_id)

    return DocAPIResponseOK[List[base_objects.Image]](
        status=status.HTTP_200_OK,
        code=AppCode.IMAGES_RETRIEVED,
        detail=GET_IMAGES_FOR_JOB_RESPONSES[AppCode.IMAGES_RETRIEVED]["detail"],
        data=db_images,
    )

GET_JOBS_RESPONSES = {
    AppCode.JOBS_RETRIEVED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Jobs retrieved successfully.",
        "model": DocAPIResponseOK,
        "model_data": List[base_objects.Job],
        "detail": "The jobs have been retrieved successfully.",
    }
}
@user_router.get(
    "/jobs",
    summary="Get Jobs",
    response_model=DocAPIResponseOK[List[base_objects.Job]],
    tags=["User"],
    description="Retrieve all jobs associated with the authenticated API key.",
    responses=make_responses(GET_JOBS_RESPONSES)
)
async def get_jobs(
        key: model.Key = Depends(require_api_key(model.KeyRole.USER)),
        db: AsyncSession = Depends(get_async_session)):

    db_jobs, code = await general_cruds.get_jobs(db=db, key_id=key.id)

    return DocAPIResponseOK[List[base_objects.Job]](
        status=status.HTTP_200_OK,
        code=AppCode.JOBS_RETRIEVED,
        detail=GET_JOBS_RESPONSES[AppCode.JOBS_RETRIEVED]["detail"],
        data=db_jobs
    )


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
@user_router.post(
    "/job/{job_id}/start",
    summary="Start Job",
    response_model=DocAPIResponseOK[NoneType],
    tags=["User"],
    description="Start processing a job.",
    responses=make_responses(POST_JOB_START_RESPONSES))
@uses_challenge_user_access_to_job
async def start_job(
        job_id: UUID,
        key: model.Key = Depends(require_api_key(model.KeyRole.USER)),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_user_access_to_job(db, key, job_id)

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


POST_JOB_CANCEL_RESPONSES = {
    AppCode.JOB_CANCELLED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Job cancelled successfully.",
        "model": DocAPIResponseOK,
        "detail": "The job has been cancelled successfully.",
    },
    AppCode.JOB_FINISHED: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": "Job is already finished and cannot be cancelled.",
        "model": DocAPIResponseClientError,
        "detail": "The job is already finished and cannot be cancelled.",
    }
}
@user_router.post(
    "/job/{job_id}/cancel",
    summary="Cancel Job",
    response_model=DocAPIResponseOK[NoneType],
    tags=["User"],
    description="Cancel a job.",
    responses=make_responses(POST_JOB_CANCEL_RESPONSES))
@uses_challenge_user_access_to_job
async def cancel_job(
        job_id: UUID,
        key: model.Key = Depends(require_api_key(model.KeyRole.USER)),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_user_access_to_job(db, key, job_id)

    db_job, code = await general_cruds.get_job(db=db, job_id=job_id)

    if db_job.state not in {base_objects.ProcessingState.CANCELLED,
                            base_objects.ProcessingState.DONE,
                            base_objects.ProcessingState.ERROR}:
        await user_cruds.cancel_job(db, job_id)
        return DocAPIResponseOK[NoneType](
            status=status.HTTP_200_OK,
            code=AppCode.JOB_CANCELLED,
            detail=POST_JOB_CANCEL_RESPONSES[AppCode.JOB_CANCELLED]["detail"]
        )
    else:
        raise DocAPIClientErrorException(
            status=status.HTTP_409_CONFLICT,
            code=AppCode.JOB_FINISHED,
            detail=POST_JOB_CANCEL_RESPONSES[AppCode.JOB_FINISHED]["detail"]
        )


GET_RESULT_RESPONSES = {
    AppCode.JOB_RESULT_RETRIEVED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Job result retrieved successfully.",
        "content_type": "application/zip",
        "example_value": "(binary ZIP file content)"
    },
    AppCode.JOB_RESULT_NOT_READY: {
        "status": fastapi.status.HTTP_425_TOO_EARLY,
        "description": "Job result is not ready yet.",
        "model": DocAPIResponseClientError,
        "detail": "The job result is not ready yet.",
    },
    AppCode.JOB_RESULT_GONE: {
        "status": fastapi.status.HTTP_410_GONE,
        "description": "Job result is no longer available.",
        "model": DocAPIResponseClientError,
        "detail": "The job result is no longer available.",
    }
}
@user_router.get(
    "/result/{job_id}",
    summary="Download Job Result",
    response_class=FileResponse,
    tags=["User"],
    description="Download the result ZIP file for a completed job.",
    responses=make_responses(GET_RESULT_RESPONSES))
@uses_challenge_user_access_to_job
async def get_result(
        route_request: fastapi.Request,
        job_id: UUID,
        key: model.Key = Depends(require_api_key(model.KeyRole.USER)),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_user_access_to_new_job(db, key, job_id)

    db_job, code = await general_cruds.get_job(db=db, job_id=job_id)

    if db_job.state in {base_objects.ProcessingState.ERROR, base_objects.ProcessingState.CANCELLED}:
        raise DocAPIClientErrorException(
            status=status.HTTP_410_GONE,
            code=AppCode.JOB_RESULT_GONE,
            detail=GET_RESULT_RESPONSES[AppCode.JOB_RESULT_GONE]["detail"]
        )

    if db_job.state in {base_objects.ProcessingState.NEW, base_objects.ProcessingState.PROCESSING}:
        raise DocAPIClientErrorException(
            status=status.HTTP_425_TOO_EARLY,
            code=AppCode.JOB_RESULT_NOT_READY,
            detail=GET_RESULT_RESPONSES[AppCode.JOB_RESULT_NOT_READY]["detail"]
        )

    if db_job.state == base_objects.ProcessingState.DONE:
        result_file_path = os.path.join(config.RESULT_DIR, f"{job_id}.zip")
        if not os.path.exists(result_file_path):
             raise DocAPIClientErrorException(
                status=status.HTTP_425_TOO_EARLY,
                code=AppCode.JOB_RESULT_NOT_READY,
                detail=GET_RESULT_RESPONSES[AppCode.JOB_RESULT_NOT_READY]["detail"]
            )

        return FileResponse(
            result_file_path,
            media_type="application/zip",
            filename=f"{job_id}.zip",
        )

    raise RouteInvariantError(code=code, request=route_request)



