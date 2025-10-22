import hashlib
import json
import logging
import os
from types import NoneType

import fastapi

import cv2
import numpy as np
from fastapi import Depends, UploadFile, status, Request, Body
from fastapi.responses import FileResponse

from aiofiles import os as aiofiles_os

from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.routes import root_router
from doc_api.api.routes.helper import RouteInvariantError
from doc_api.api.routes.user_guards import challenge_user_access_to_new_job, challenge_user_access_to_job
from doc_api.api.authentication import require_api_key
from doc_api.api.cruds import user_cruds, general_cruds, worker_cruds
from doc_api.api.database import get_async_session
from doc_api.api.routes.worker_guards import challenge_worker_access_to_job
from doc_api.api.schemas import base_objects
from doc_api.api.schemas.responses import DocAPIResponseClientError, AppCode, DocAPIResponseOK, make_responses, \
    validate_ok_response, DocAPIClientErrorException, GENERAL_RESPONSES
from doc_api.api.validators.alto_validator import validate_alto_basic
from doc_api.api.validators.page_validator import validate_page_basic
from doc_api.api.validators.xml_validator import is_well_formed_xml
from doc_api.db import model
from doc_api.api.config import config

from typing import List
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
@root_router.get(
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


GET_JOBS_RESPONSES = {
    AppCode.JOBS_RETRIEVED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Jobs retrieved successfully.",
        "model": DocAPIResponseOK,
        "model_data": List[base_objects.Job],
        "detail": "The jobs have been retrieved successfully.",
    }
}
@root_router.get(
    "/v1/jobs",
    summary="Get Jobs",
    response_model=DocAPIResponseOK[List[base_objects.Job]],
    tags=["User"],
    description="Retrieve all jobs associated with the authenticated API key.",
    responses=make_responses(GET_JOBS_RESPONSES))
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


POST_JOB_RESPONSES = {
    AppCode.JOB_CREATED: {
        "status": fastapi.status.HTTP_201_CREATED,
        "description": "Job created successfully",
        "model": DocAPIResponseOK,
        "model_data": base_objects.JobWithImages,
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
@root_router.post(
    "/v1/jobs",
    summary="Create Job",
    tags=["User"],
    description="Create a new job with the specified images and options.",
    responses=make_responses(POST_JOB_RESPONSES))
async def post_job(
        request: Request,
        job_definition: user_cruds.JobDefinition,
        key: model.Key = Depends(require_api_key(model.KeyRole.USER)),
        db: AsyncSession = Depends(get_async_session)):
    #TODO check if there are duplicates in image names?

    db_job, job_code = await user_cruds.create_job(db=db, key_id=key.id, job_definition=job_definition)
    db_images, images_code = await general_cruds.get_job_images(db=db, job_id=db_job.id)

    if job_code == AppCode.JOB_CREATED and images_code == AppCode.IMAGE_RETRIEVED:
        job = base_objects.Job.model_validate(db_job).model_dump()
        images = [base_objects.Image.model_validate(img).model_dump() for img in db_images]
        data = base_objects.JobWithImages(**job, images=images)
        # FastAPI automatically validates only for 200, so we need to do it manually for 201 here
        return validate_ok_response(DocAPIResponseOK[base_objects.Job](
            status=status.HTTP_201_CREATED,
            code=AppCode.JOB_CREATED,
            detail=POST_JOB_RESPONSES[AppCode.JOB_CREATED]["detail"],
            data=data))

    raise RouteInvariantError(code=job_code if job_code != AppCode.JOB_CREATED else images_code, request=request)


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
    tags=["User"],
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


PUT_IMAGE_RESPONSES = {
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
@root_router.put(
    " /v1/jobs/{job_id}/images/{image_name}/files/image",
    response_model=DocAPIResponseOK[NoneType],
    summary="Upload IMAGE",
    tags=["User"],
    description="Upload an IMAGE file for a specific job and image name.",
    responses=make_responses(PUT_IMAGE_RESPONSES))
@challenge_user_access_to_new_job
async def put_image(
        request: Request,
        job_id: UUID,
        image_name: str,
        file: UploadFile,
        key: model.Key = Depends(require_api_key(model.KeyRole.USER)),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_user_access_to_new_job(db=db, key=key, job_id=job_id)

    db_image, code = await user_cruds.get_image_by_job_and_name(db=db, job_id=job_id, image_name=image_name)

    if code == AppCode.IMAGE_RETRIEVED:
        raw_input = file.file.read()
        contents = np.asarray(bytearray(raw_input), dtype="uint8")
        image = cv2.imdecode(contents, cv2.IMREAD_COLOR)
        if image is None:
            raise DocAPIClientErrorException(
                status=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                code=AppCode.IMAGE_INVALID,
                detail=PUT_IMAGE_RESPONSES[AppCode.IMAGE_INVALID]["detail"]
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
                detail=PUT_IMAGE_RESPONSES[AppCode.IMAGE_UPLOADED]["detail"]
            )
        else:
            return DocAPIResponseOK[NoneType](
                status=status.HTTP_200_OK,
                code=AppCode.IMAGE_REUPLOADED,
                detail=PUT_IMAGE_RESPONSES[AppCode.IMAGE_REUPLOADED]["detail"]
            )

    elif code == AppCode.IMAGE_NOT_FOUND_FOR_JOB:
        raise DocAPIClientErrorException(
            status=status.HTTP_404_NOT_FOUND,
            code=AppCode.IMAGE_NOT_FOUND_FOR_JOB,
            detail=PUT_IMAGE_RESPONSES[AppCode.IMAGE_NOT_FOUND_FOR_JOB]["detail"]
        )

    raise RouteInvariantError(code=code, request=request)


PUT_ALTO_RESPONSES = {
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
@root_router.put(
    "/v1/jobs/{job_id}/images/{image_name}/files/alto",
    summary="Upload ALTO XML",
    response_model=DocAPIResponseOK[NoneType],
    description="Upload an ALTO XML file for a specific job and image name.",
    tags=["User"],
responses=make_responses(PUT_ALTO_RESPONSES))
@challenge_user_access_to_new_job
async def put_alto(
        request: Request,
        job_id: UUID,
        image_name: str,
        file: UploadFile,
        key: model.Key = Depends(require_api_key(model.KeyRole.USER)),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_user_access_to_new_job(db=db, key=key, job_id=job_id)

    db_job, _ = await general_cruds.get_job(db=db, job_id=job_id)
    if not db_job.alto_required:
        raise DocAPIClientErrorException(
            status=status.HTTP_409_CONFLICT,
            code=AppCode.ALTO_NOT_REQUIRED,
            detail=PUT_ALTO_RESPONSES[AppCode.ALTO_NOT_REQUIRED]["detail"],
        )

    db_image, code = await user_cruds.get_image_by_job_and_name(db=db, job_id=job_id, image_name=image_name)
    if code != AppCode.IMAGE_RETRIEVED:
        data = await file.read()
        if not is_well_formed_xml(data):
            raise DocAPIClientErrorException(
                status=status.HTTP_400_BAD_REQUEST,
                code=AppCode.XML_PARSE_ERROR,
                detail=PUT_ALTO_RESPONSES[AppCode.XML_PARSE_ERROR]["detail"],
            )
        alto_checks = validate_alto_basic(data)
        for check_type, check_val in alto_checks.items():
            if config.ALTO_VALIDATION[check_type] and not check_val:
                raise DocAPIClientErrorException(
                    status=status.HTTP_422_UNPROCESSABLE_CONTENT,
                    code=AppCode.ALTO_SCHEMA_INVALID,
                    detail=PUT_ALTO_RESPONSES[AppCode.ALTO_SCHEMA_INVALID]["detail"],
                )

        batch_path = os.path.join(config.JOBS_DIR, str(job_id))
        await aiofiles_os.makedirs(batch_path, exist_ok=True)
        alto_path = os.path.join(batch_path, f"{db_image.id}.alto.xml")

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
                detail=PUT_ALTO_RESPONSES[AppCode.ALTO_UPLOADED]["detail"]
            ))
        else:
            return validate_ok_response(DocAPIResponseOK[NoneType](
                status=status.HTTP_200_OK,
                code=AppCode.ALTO_REUPLOADED,
                detail=PUT_ALTO_RESPONSES[AppCode.ALTO_REUPLOADED]["detail"]
            ))

    elif code == AppCode.IMAGE_NOT_FOUND_FOR_JOB:
        raise DocAPIClientErrorException(
            status=status.HTTP_404_NOT_FOUND,
            code=AppCode.IMAGE_NOT_FOUND_FOR_JOB,
            detail=PUT_IMAGE_RESPONSES[AppCode.IMAGE_NOT_FOUND_FOR_JOB]["detail"]
        )

    raise RouteInvariantError(code=code, request=request)

# this should mirror the ALTO route above
PUT_PAGE_RESPONSES = {
    AppCode.PAGE_UPLOADED: {
        "status": fastapi.status.HTTP_201_CREATED,
        "description": "PAGE XML uploaded successfully.",
        "model": DocAPIResponseOK,
        "detail": "The PAGE XML file has been uploaded successfully.",
    },
    AppCode.PAGE_REUPLOADED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "PAGE XML re-uploaded successfully.",
        "model": DocAPIResponseOK,
        "detail": "The PAGE XML file has been re-uploaded successfully.",
    },
    AppCode.PAGE_NOT_REQUIRED: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": "PAGE XML not required for this job.",
        "model": DocAPIResponseClientError,
        "detail": "Job does not require PAGE XML.",
    },
    AppCode.XML_PARSE_ERROR: GENERAL_RESPONSES[AppCode.XML_PARSE_ERROR],
    AppCode.PAGE_SCHEMA_INVALID: {
        "status": fastapi.status.HTTP_422_UNPROCESSABLE_CONTENT,
        "description": "PAGE XML does not conform to the required schema.",
        "model": DocAPIResponseClientError,
        "detail": "The PAGE XML file does not conform to the required schema.",
    },
}
@root_router.put(
    "/v1/jobs/{job_id}/images/{image_name}/files/page",
    summary="Upload PAGE XML",
    response_model=DocAPIResponseOK[NoneType],
    description="Upload an PAGE XML file for a specific job and image name.",
    tags=["User"],
responses=make_responses(PUT_PAGE_RESPONSES))
@challenge_user_access_to_new_job
async def put_page(
        request: Request,
        job_id: UUID,
        image_name: str,
        file: UploadFile,
        key: model.Key = Depends(require_api_key(model.KeyRole.USER)),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_user_access_to_new_job(db=db, key=key, job_id=job_id)

    db_job, _ = await general_cruds.get_job(db=db, job_id=job_id)
    if not db_job.page_required:
        raise DocAPIClientErrorException(
            status=status.HTTP_409_CONFLICT,
            code=AppCode.PAGE_NOT_REQUIRED,
            detail=PUT_PAGE_RESPONSES[AppCode.PAGE_NOT_REQUIRED]["detail"],
        )

    db_image, code = await user_cruds.get_image_by_job_and_name(db=db, job_id=job_id, image_name=image_name)
    if code != AppCode.IMAGE_RETRIEVED:
        data = await file.read()
        if not is_well_formed_xml(data):
            raise DocAPIClientErrorException(
                status=status.HTTP_400_BAD_REQUEST,
                code=AppCode.XML_PARSE_ERROR,
                detail=PUT_PAGE_RESPONSES[AppCode.XML_PARSE_ERROR]["detail"],
            )
        page_checks = validate_page_basic(data)
        for check_type, check_val in page_checks.items():
            if config.PAGE_VALIDATION[check_type] and not check_val:
                raise DocAPIClientErrorException(
                    status=status.HTTP_422_UNPROCESSABLE_CONTENT,
                    code=AppCode.PAGE_SCHEMA_INVALID,
                    detail=PUT_PAGE_RESPONSES[AppCode.PAGE_SCHEMA_INVALID]["detail"],
                )

        batch_path = os.path.join(config.JOBS_DIR, str(job_id))
        await aiofiles_os.makedirs(batch_path, exist_ok=True)
        page_path = os.path.join(batch_path, f"{db_image.id}.page.xml")

        with open(page_path, "wb") as f:
            f.write(data)

        # TODO this can potentially lead to inconsistent state if the job start fails after PAGE upload
        job_started = await user_cruds.start_job(db=db, job_id=job_id)

        if not db_image.alto_uploaded:
            image_update = base_objects.ImageUpdate(alto_uploaded=True)
            await general_cruds.update_image(db=db, image_id=db_image.id, image_update=image_update)
            return validate_ok_response(DocAPIResponseOK[NoneType](
                status=status.HTTP_201_CREATED,
                code=AppCode.PAGE_UPLOADED,
                detail=PUT_PAGE_RESPONSES[AppCode.PAGE_UPLOADED]["detail"]
            ))
        else:
            return validate_ok_response(DocAPIResponseOK[NoneType](
                status=status.HTTP_200_OK,
                code=AppCode.PAGE_REUPLOADED,
                detail=PUT_PAGE_RESPONSES[AppCode.PAGE_REUPLOADED]["detail"]
            ))

    elif code == AppCode.IMAGE_NOT_FOUND_FOR_JOB:
        raise DocAPIClientErrorException(
            status=status.HTTP_404_NOT_FOUND,
            code=AppCode.IMAGE_NOT_FOUND_FOR_JOB,
            detail=PUT_IMAGE_RESPONSES[AppCode.IMAGE_NOT_FOUND_FOR_JOB]["detail"]
        )

    raise RouteInvariantError(code=code, request=request)


PUT_META_JSON_RESPONSES = {
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
@root_router.put(
    "/v1/jobs/{job_id}/images/{image_name}/files/metadata",
    response_model=DocAPIResponseOK[NoneType],
    summary="Upload Meta JSON",
    tags=["User"],
    description="Upload the Meta JSON file for a job.",
    responses=make_responses(PUT_META_JSON_RESPONSES)
)
@challenge_user_access_to_new_job
async def put_meta_json(
        job_id: UUID, meta_json,
        key: model.Key = Depends(require_api_key(model.KeyRole.USER)),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_user_access_to_new_job(db=db, key=key, job_id=job_id)

    db_job, _ = await general_cruds.get_job(db=db, job_id=job_id)

    if not db_job.meta_json_required:
        raise DocAPIClientErrorException(
            status=status.HTTP_409_CONFLICT,
            code=AppCode.META_JSON_NOT_REQUIRED,
            detail=PUT_META_JSON_RESPONSES[AppCode.META_JSON_NOT_REQUIRED]["detail"],
        )

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
            detail=PUT_META_JSON_RESPONSES[AppCode.META_JSON_UPLOADED]["detail"]
        ))
    else:
        return validate_ok_response(DocAPIResponseOK[NoneType](
            status=status.HTTP_200_OK,
            code=AppCode.META_JSON_REUPLOADED,
            detail=PUT_META_JSON_RESPONSES[AppCode.META_JSON_REUPLOADED]["detail"]
        ))


PATCH_JOB_RESPONSES = {
    # user cancels job
    AppCode.API_KEY_USER_FORBIDDEN: {
        "status": fastapi.status.HTTP_403_FORBIDDEN,
        "description": "User can only cancel jobs.",
        "model": DocAPIResponseClientError,
        "detail": "User can only cancel jobs.",
    },
    AppCode.JOB_FINISHED: {
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

    # worker marking job done
    AppCode.JOB_RESULT_MISSING: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": "Result ZIP file has not been uploaded yet.",
        "model": DocAPIResponseClientError,
        "detail": "Result ZIP file has not been uploaded yet.",
    },
    AppCode.JOB_COMPLETED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Job has been marked as done.",
        "model": DocAPIResponseOK,
        "detail": "Job has been marked as done.",
    },
    AppCode.JOB_ALREADY_COMPLETED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Job was already marked as done.",
        "model": DocAPIResponseOK,
        "detail": "Job was already marked as done.",
    },

    # worker marking job failed
    AppCode.JOB_FAILED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Job has been marked as error.",
        "model": DocAPIResponseOK,
        "detail": "Job has been marked as error.",
    },
    AppCode.JOB_ALREADY_FAILED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Job was already marked as error.",
        "model": DocAPIResponseOK,
        "detail": "Job was already marked as error.",
    },

    # worker updating progress
    AppCode.JOB_PROGRESS_UPDATED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Job has been updated successfully and the lease has been extended (UTC time).",
        "model": DocAPIResponseOK,
        "model_data": base_objects.JobLease,
        "detail": "Job has been updated successfully and the lease has been extended (UTC time).",
    },
}
@root_router.patch(
    "/v1/jobs/{job_id}",
    summary="Update Job",
    response_model=DocAPIResponseOK[NoneType],
    tags=["User"],
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
                    "description": "Worker marks job as done, `progress: 1.0` is automatically set."
                                   "\n\nOnly `state: done` will be accepted, other fields will be ignored."
                                   "\n\nJob must be in one of the following "
                                   f"`state: {base_objects.ProcessingState.PROCESSING}|{base_objects.ProcessingState.DONE}`.",
                    "value": {"state": base_objects.ProcessingState.DONE.value},
                },
                "worker_error": {
                    "summary": "Worker finalizes job as error",
                    "description": "Worker marks job as error."
                                   "\n\nOnly `state: error` will be accepted, other fields will be ignored."
                                   f"\n\nJob must be in one of the following "
                                   f"`state: {base_objects.ProcessingState.PROCESSING}|{base_objects.ProcessingState.ERROR}`.",
                    "value": {"state": base_objects.ProcessingState.ERROR.value},
                },
                "worker_progress": {
                    "summary": "Worker updates job progress",
                    "description": (
                        "Worker updates job progress and log messages. Logs are appended to existing logs."
                        f"\n\nJob must be in `state: {base_objects.ProcessingState.PROCESSING}`."
                        "\n\nAt least one of `progress|log|log_user` must be provided."
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
                code=AppCode.JOB_FINISHED,
                detail=PATCH_JOB_RESPONSES[AppCode.JOB_FINISHED]["detail"],
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

            code_update_job = await worker_cruds.complete_job(db=db, job_id=job_id)

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

            code_update_job = await worker_cruds.fail_job(db=db, job_id=job_id)

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
            db_job, lease_expire_at, server_time, code_update_job = await worker_cruds.update_processing_job_progress(db=db,
                                                                                                           job_id=job_id,
                                                                                                           job_progress_update=job_progress_update)
            if code_update_job == AppCode.JOB_PROGRESS_UPDATED:
                return DocAPIResponseOK[base_objects.JobLease](
                    status=fastapi.status.HTTP_200_OK,
                    code=AppCode.JOB_PROGRESS_UPDATED,
                    detail=PATCH_JOB_RESPONSES[AppCode.JOB_PROGRESS_UPDATED]["detail"],
                    data=base_objects.JobLease(id=job_id, lease_expire_at=lease_expire_at, server_time=server_time)
                )

        else:
            raise DocAPIClientErrorException(
                status=status.HTTP_403_FORBIDDEN,
                code=AppCode.API_KEY_WORKER_FORBIDDEN,
                detail=PATCH_JOB_RESPONSES[AppCode.API_KEY_WORKER_FORBIDDEN]["detail"]
            )

    raise RouteInvariantError(code=code_get_job if code_update_job is None else code_update_job, request=request)


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
@root_router.get(
    "/v1/jobs/{job_id}/result",
    summary="Download Job Result",
    response_class=FileResponse,
    tags=["User"],
    description="Download the result ZIP file for a completed job.",
    responses=make_responses(GET_RESULT_RESPONSES))
@challenge_user_access_to_job
async def get_result(
        route_request: fastapi.Request,
        job_id: UUID,
        key: model.Key = Depends(require_api_key(model.KeyRole.USER)),
        db: AsyncSession = Depends(get_async_session)):
    await challenge_user_access_to_new_job(db=db, key=key, job_id=job_id)

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



