import logging
import os
import zipfile
from types import NoneType

import aiofiles
import fastapi
from fastapi import Depends, UploadFile, File, Request
from fastapi.responses import FileResponse

from aiofiles import os as aiofiles_os

from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.authentication import require_api_key
from doc_api.api.cruds import worker_cruds, general_cruds
from doc_api.api.database import get_async_session
from doc_api.api.routes import root_router
from doc_api.api.routes.helper import RouteInvariantError
from doc_api.api.guards.worker_guards import challenge_worker_access_to_processing_job
from doc_api.api.schemas import base_objects
from doc_api.api.schemas.responses import AppCode, DocAPIResponseOK, \
    DocAPIResponseClientError, DocAPIClientErrorException, make_responses, GENERAL_RESPONSES, validate_ok_response
from doc_api.db import model
from doc_api.api.config import config

from uuid import UUID


logger = logging.getLogger(__name__)


POST_LEASE_RESPONSES = {
    AppCode.JOB_LEASED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Job has been assigned to the worker and the lease has been established (UTC time).",
        "model": DocAPIResponseOK,
        "model_data": base_objects.JobLease,
        "detail": "Job has been assigned to the worker and the lease has been established (UTC time).",
    },
    AppCode.JOB_QUEUE_EMPTY: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "No jobs are currently available in the queue.",
        "model": DocAPIResponseOK,
        "detail": "Queue is empty right now, try again shortly.",
    }
}
@root_router.post(
    "/v1/jobs/lease",
    summary="Request Lease",
    response_model=DocAPIResponseOK[base_objects.JobLease],
    tags=["Worker"],
    description=f"Request a job lease for processing. If a job is available, it will be assigned to the worker along with a lease time. "
                f"If no jobs are available, a response indicating an empty queue will be returned.",
    responses=make_responses(POST_LEASE_RESPONSES))
async def post_lease(
        request: Request,
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):

    db_job, lease_expire_at, server_time, code = await worker_cruds.lease_job_to_worker(db=db, worker_key_id=key.id)

    if code == AppCode.JOB_LEASED:
        return DocAPIResponseOK[base_objects.Job](
            status=fastapi.status.HTTP_200_OK,
            code=AppCode.JOB_LEASED,
            detail=POST_LEASE_RESPONSES[AppCode.JOB_LEASED]["detail"],
            data=base_objects.JobLease(
                id=db_job.id,
                lease_expire_at=lease_expire_at,
                server_time=server_time),
        )
    elif code == AppCode.JOB_QUEUE_EMPTY:
        return validate_ok_response(
            DocAPIResponseOK[NoneType](
                status=fastapi.status.HTTP_200_OK,
                code=AppCode.JOB_QUEUE_EMPTY,
                detail=POST_LEASE_RESPONSES[AppCode.JOB_QUEUE_EMPTY]["detail"]
            )
        )

    raise RouteInvariantError(code=code, request=request)


PATCH_LEASE_RESPONSES = {
    AppCode.JOB_LEASE_EXTENDED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Job lease has been successfully extended (UTC time).",
        "model": DocAPIResponseOK,
        "model_data": base_objects.JobLease,
        "detail": "Job lease has been successfully extended (UTC time)."
    }
}
@root_router.patch(
    "/v1/jobs/{job_id}/lease",
    response_model=base_objects.JobLease,
    summary="Extend Lease",
    tags=["Worker"],
    description="Extend the lease time for a specific job that is currently being processed by the worker.",
    responses=make_responses(PATCH_LEASE_RESPONSES))
@challenge_worker_access_to_processing_job
async def patch_lease(
        request: Request,
        job_id: UUID,
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):

    lease_expire_at, server_time, code = await worker_cruds.update_processing_job_lease(db=db, job_id=job_id)

    if code == AppCode.JOB_LEASE_EXTENDED:
        return DocAPIResponseOK[base_objects.JobLease](
            status=fastapi.status.HTTP_200_OK,
            code=AppCode.JOB_LEASE_EXTENDED,
            detail=PATCH_LEASE_RESPONSES[AppCode.JOB_LEASE_EXTENDED]["detail"],
            data=base_objects.JobLease(id=job_id, lease_expire_at=lease_expire_at, server_time=server_time),
        )

    raise RouteInvariantError(code=code, request=request)


DELETE_LEASE_RESPONSES = {
    AppCode.JOB_LEASE_RELEASED: {
        "status": fastapi.status.HTTP_204_NO_CONTENT,
        "description": "The job lease has been successfully released.",
        "example_value": "NO BODY"
    }
}
@root_router.delete(
    "/v1/jobs/{job_id}/lease",
    status_code=fastapi.status.HTTP_204_NO_CONTENT,
    summary="Release Lease",
    tags=["Worker"],
    description="Release the lease for a specific job that is currently being processed by the worker.",
    responses=make_responses(DELETE_LEASE_RESPONSES))
@challenge_worker_access_to_processing_job
async def delete_lease(
        request: Request,
        job_id: UUID,
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):

    code = await worker_cruds.release_job_lease(db=db, job_id=job_id)

    if code == AppCode.JOB_LEASE_RELEASED:
        return validate_ok_response(DocAPIResponseOK[NoneType](
            status=fastapi.status.HTTP_204_NO_CONTENT,
            code="",
            detail=""
        ))

    raise RouteInvariantError(code=code, request=request)


GET_IMAGE_RESPONSES = {
    AppCode.IMAGE_DOWNLOADED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Binary data of the requested IMAGE file, format not guaranteed but usually JPEG/PNG.",
        "content_type": "image/jpeg",
        "example_value": "(binary IMAGE file content)"
    },
    AppCode.IMAGE_GONE: {
        "status": fastapi.status.HTTP_410_GONE,
        "description": "IMAGE file was probably deleted from the server.",
        "model": DocAPIResponseClientError,
        "detail": "IMAGE file was probably deleted from the server. Consider setting the job state to error.",
    },
    AppCode.IMAGE_NOT_FOUND_FOR_JOB: GENERAL_RESPONSES[AppCode.IMAGE_NOT_FOUND_FOR_JOB],
}
@root_router.get(
    "/v1/jobs/{job_id}/images/{image_id}/files/image",
    response_class=FileResponse,
    summary="Download IMAGE",
    tags=["Worker"],
    description="Download the IMAGE file associated with a specific image of a job.",
    responses=make_responses(GET_IMAGE_RESPONSES))
@challenge_worker_access_to_processing_job
async def get_image(
        request: Request,
        job_id: UUID, image_id: UUID,
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):

    db_image, code = await general_cruds.get_image_for_job(db=db, job_id=job_id, image_id=image_id)

    if code == AppCode.IMAGE_RETRIEVED:
        image_path = os.path.join(config.JOBS_DIR, str(db_image.job_id), f"{db_image.id}.jpg")
        if not await aiofiles_os.path.exists(image_path) or not db_image.image_uploaded:
            raise DocAPIClientErrorException(
                status=fastapi.status.HTTP_410_GONE,
                code=AppCode.IMAGE_GONE,
                detail=GET_IMAGE_RESPONSES[AppCode.IMAGE_GONE]["detail"],
            )
        return FileResponse(image_path, media_type="image/jpeg", filename=db_image.name)

    elif code == AppCode.IMAGE_NOT_FOUND_FOR_JOB:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_404_NOT_FOUND,
            code=AppCode.IMAGE_NOT_FOUND_FOR_JOB,
            detail=GET_IMAGE_RESPONSES[AppCode.IMAGE_NOT_FOUND_FOR_JOB]["detail"],
        )

    raise RouteInvariantError(code=code, request=request)


GET_ALTO_RESPONSES = {
    AppCode.ALTO_DOWNLOADED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "XML data of the requested ALTO file.",
        "content_type": "application/xml",
        "example_value": "<alto>...</alto>",
    },
    AppCode.ALTO_NOT_REQUIRED: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": "ALTO XML file is not required for this job.",
        "model": DocAPIResponseClientError,
        "detail": "ALTO XML file is not required for this job.",
    },
    AppCode.ALTO_GONE: {
        "status": fastapi.status.HTTP_410_GONE,
        "description": "ALTO XML file was probably deleted from the server.",
        "model": DocAPIResponseClientError,
        "detail": "ALTO XML file was probably deleted from the server. Consider setting the job state to error.",
    },
    AppCode.IMAGE_NOT_FOUND_FOR_JOB: GENERAL_RESPONSES[AppCode.IMAGE_NOT_FOUND_FOR_JOB],
}
@root_router.get(
    "/v1/jobs/{job_id}/images/{image_id}/files/alto",
    response_class=FileResponse,
    summary="Download ALTO XML",
    tags=["Worker"],
    description="Download the ALTO XML file associated with a specific image of a job.",
    responses=make_responses(GET_ALTO_RESPONSES))
@challenge_worker_access_to_processing_job
async def get_alto(
        request: Request,
        job_id: UUID,
        image_id: UUID,
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):

    db_job, _ = await general_cruds.get_job(db=db, job_id=job_id)
    db_image, code = await general_cruds.get_image_for_job(db=db, job_id=job_id, image_id=image_id)

    if code == AppCode.IMAGE_RETRIEVED:
        if not db_job.alto_required:
            raise DocAPIClientErrorException(
                status=fastapi.status.HTTP_409_CONFLICT,
                code=AppCode.ALTO_NOT_REQUIRED,
                detail=GET_ALTO_RESPONSES[AppCode.ALTO_NOT_REQUIRED]["detail"]
            )

        alto_path = os.path.join(config.JOBS_DIR, str(db_image.job_id), f"{db_image.id}.alto.xml")
        if not await aiofiles_os.path.exists(alto_path) or not db_image.alto_uploaded:
            raise DocAPIClientErrorException(
                status=fastapi.status.HTTP_410_GONE,
                code=AppCode.ALTO_GONE,
                detail=GET_ALTO_RESPONSES[AppCode.ALTO_GONE]["detail"],
            )
        return FileResponse(alto_path, media_type="application/xml", filename=f"{os.path.splitext(db_image.name)[0]}.xml")

    elif code == AppCode.IMAGE_NOT_FOUND_FOR_JOB:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_404_NOT_FOUND,
            code=AppCode.IMAGE_NOT_FOUND_FOR_JOB,
            detail=GET_ALTO_RESPONSES[AppCode.IMAGE_NOT_FOUND_FOR_JOB]["detail"]
        )

    raise RouteInvariantError(code=code, request=request)


GET_PAGE_RESPONSES = {
    AppCode.PAGE_DOWNLOADED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "XML data of the requested PAGE file.",
        "content_type": "application/xml",
        "example_value": "<PcGts>...</PcGts>",
    },
    AppCode.PAGE_NOT_REQUIRED: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": "PAGE XML file is not required for this job.",
        "model": DocAPIResponseClientError,
        "detail": "PAGE XML file is not required for this job.",
    },
    AppCode.PAGE_GONE: {
        "status": fastapi.status.HTTP_410_GONE,
        "description": "PAGE XML file was probably deleted from the server.",
        "model": DocAPIResponseClientError,
        "detail": "PAGE XML file was probably deleted from the server. Consider setting the job state to error.",
    },
    AppCode.IMAGE_NOT_FOUND_FOR_JOB: GENERAL_RESPONSES[AppCode.IMAGE_NOT_FOUND_FOR_JOB],
}
@root_router.get(
    "/v1/jobs/{job_id}/images/{image_id}/files/page",
    response_class=FileResponse,
    summary="Download PAGE XML",
    tags=["Worker"],
    description="Download the PAGE XML file associated with a specific image of a job.",
    responses=make_responses(GET_PAGE_RESPONSES))
@challenge_worker_access_to_processing_job
async def get_page(
        request: Request,
        job_id: UUID,
        image_id: UUID,
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):
    db_job, _ = await general_cruds.get_job(db=db, job_id=job_id)
    db_image, code = await general_cruds.get_image_for_job(db=db, job_id=job_id, image_id=image_id)

    if code == AppCode.IMAGE_RETRIEVED:
        if not db_job.page_required:
            raise DocAPIClientErrorException(
                status=fastapi.status.HTTP_409_CONFLICT,
                code=AppCode.PAGE_NOT_REQUIRED,
                detail=GET_PAGE_RESPONSES[AppCode.PAGE_NOT_REQUIRED]["detail"]
            )

        page_path = os.path.join(config.JOBS_DIR, str(db_image.job_id), f"{db_image.id}.page.xml")
        if not await aiofiles_os.path.exists(page_path) or not db_image.page_uploaded:
            raise DocAPIClientErrorException(
                status=fastapi.status.HTTP_410_GONE,
                code=AppCode.PAGE_GONE,
                detail=GET_PAGE_RESPONSES[AppCode.PAGE_GONE]["detail"],
            )
        return FileResponse(page_path, media_type="application/xml",
                            filename=f"{os.path.splitext(db_image.name)[0]}.xml")

    elif code == AppCode.IMAGE_NOT_FOUND_FOR_JOB:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_404_NOT_FOUND,
            code=AppCode.IMAGE_NOT_FOUND_FOR_JOB,
            detail=GET_PAGE_RESPONSES[AppCode.IMAGE_NOT_FOUND_FOR_JOB]["detail"]
        )

    raise RouteInvariantError(code=code, request=request)


GET_METADATA = {
    AppCode.META_JSON_DOWNLOADED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "JSON data of the requested Meta JSON file.",
        "content_type": "application/json",
        "example_value": {"meta_key": "meta_value"},
    },
    AppCode.META_JSON_NOT_REQUIRED: {
        "status": fastapi.status.HTTP_409_CONFLICT,
        "description": "The Meta JSON file is not required for this job.",
        "model": DocAPIResponseClientError,
        "detail": "Meta JSON file is not required for this job.",
    },
    AppCode.META_JSON_GONE: {
        "status": fastapi.status.HTTP_410_GONE,
        "description": "The Meta JSON file was probably deleted from the server.",
        "model": DocAPIResponseClientError,
        "detail": "The Meta JSON file was probably deleted from the server. Consider setting the job state to error.",
    }
}
@root_router.get(
    "/v1/jobs/{job_id}/images/{image_id}/files/metadata",
    response_class=FileResponse,
    summary="Download Meta JSON",
    tags=["Worker"],
    description="Download the Meta JSON file associated with a specific job.",
    responses=make_responses(GET_METADATA))
@challenge_worker_access_to_processing_job
async def get_metadata(
        request: Request,
        job_id: UUID,
        key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
        db: AsyncSession = Depends(get_async_session)):

    db_job, code = await general_cruds.get_job(db=db, job_id=job_id)

    if code == AppCode.JOB_RETRIEVED:
        if not db_job.meta_json_required:
            raise DocAPIClientErrorException(
                status=fastapi.status.HTTP_409_CONFLICT,
                code=AppCode.META_JSON_NOT_REQUIRED,
                detail=GET_METADATA[AppCode.META_JSON_NOT_REQUIRED]["detail"]
            )

        meta_json_path = os.path.join(config.JOBS_DIR, str(job_id), "meta.json")
        if not await aiofiles_os.path.exists(meta_json_path) or not db_job.meta_json_uploaded:
            raise DocAPIClientErrorException(
                status=fastapi.status.HTTP_410_GONE,
                code=AppCode.META_JSON_GONE,
                detail=GET_METADATA[AppCode.META_JSON_GONE]["detail"],
            )
        return FileResponse(meta_json_path, media_type="application/json", filename="meta.json")

    raise RouteInvariantError(code=code, request=request)


POST_RESULT_RESPONSES = {
    AppCode.JOB_RESULT_UPLOADED: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "Result ZIP archive uploaded successfully.",
        "model": DocAPIResponseOK,
        "detail": "Result ZIP archive uploaded successfully.",
    },
    AppCode.JOB_RESULT_INVALID: {
        "status": fastapi.status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
        "description": "The uploaded file is not a valid ZIP archive.",
        "model": DocAPIResponseClientError,
        "detail": "The uploaded file is not a valid ZIP archive.",
    },
}
@root_router.post(
    "/v1/jobs/{job_id}/result/",
    response_model=DocAPIResponseOK,
    summary="Upload Result",
    tags=["Worker"],
    description="Upload the result ZIP archive for a specific job.",
    responses=make_responses(POST_RESULT_RESPONSES))
@challenge_worker_access_to_processing_job
async def post_result(
    job_id: UUID,
    result: UploadFile = File(...),
    key: model.Key = Depends(require_api_key(base_objects.KeyRole.WORKER)),
    db: AsyncSession = Depends(get_async_session),
):

    await aiofiles_os.makedirs(config.RESULT_DIR, exist_ok=True)
    final_path = os.path.join(config.RESULT_DIR, f"{job_id}.zip")
    tmp_path = final_path + ".validating"

    async with aiofiles.open(tmp_path, "wb") as f:
        while chunk := await result.read(1024 * 1024):
            await f.write(chunk)

    if config.RESULT_ZIP_VALIDATION:
        try:
            with zipfile.ZipFile(tmp_path):
                pass
        except zipfile.BadZipFile:
            await aiofiles_os.remove(tmp_path)
            raise DocAPIClientErrorException(
                status=fastapi.status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                code=AppCode.JOB_RESULT_INVALID,
                detail=POST_RESULT_RESPONSES[AppCode.JOB_RESULT_INVALID]["detail"],
            )

    os.replace(tmp_path, final_path)

    return DocAPIResponseOK[NoneType](
        status=fastapi.status.HTTP_200_OK,
        code=AppCode.JOB_RESULT_UPLOADED,
        detail=POST_RESULT_RESPONSES[AppCode.JOB_RESULT_UPLOADED]["detail"]
    )

