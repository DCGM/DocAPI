import logging
from typing import Tuple, Optional, List
from uuid import UUID

from sqlalchemy import select, exc
from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.database import DBError
from doc_api.api.schemas import base_objects
from doc_api.api.schemas.responses import AppCode
from doc_api.db import model


logger = logging.getLogger(__name__)


async def get_job(*, db: AsyncSession, job_id: UUID) -> Tuple[Optional[model.Job], AppCode]:
    try:
        async with db.begin():
            result = await db.execute(
                select(model.Job).where(model.Job.id == job_id)
            )
            db_job = result.scalar_one_or_none()

            if db_job is None:
                return None, AppCode.JOB_NOT_FOUND

            return db_job, AppCode.JOB_RETRIEVED

    except exc.SQLAlchemyError as e:
        raise DBError(f"Failed reading job from database") from e


async def get_jobs(*, db: AsyncSession, key_id: UUID) -> Tuple[List[model.Job], AppCode]:
    try:
        async with db.begin():
            result = await db.scalars(
                select(model.Job)
                  .where(model.Job.owner_key_id == key_id)
                  .order_by(model.Job.created_date.desc())
            )
            return list(result.all()), AppCode.JOBS_RETRIEVED

    except exc.SQLAlchemyError as e:
        raise DBError('Failed reading jobs from database') from e


async def update_job(*, db: AsyncSession, job_id: UUID, job_update: base_objects.JobUpdate) -> AppCode:
    try:
        async with db.begin():
            result = await db.execute(
                select(model.Job).where(model.Job.id == job_id).with_for_update()
            )
            db_job = result.scalar_one_or_none()

            if db_job is None:
                return AppCode.JOB_NOT_FOUND

            if job_update.meta_json_uploaded is not None:
                db_job.meta_json_uploaded = job_update.meta_json_uploaded

            return AppCode.JOB_UPDATED

    except exc.SQLAlchemyError as e:
        raise DBError(f"Failed updating job in database") from e


async def get_job_images(*, db: AsyncSession, job_id: UUID) -> Tuple[Optional[List[model.Image]], AppCode]:
    try:
        async with db.begin():
            result = await db.execute(
                select(model.Job).where(model.Job.id == job_id)
            )
            db_job = result.scalar_one_or_none()
            if db_job is None:
                return None, AppCode.JOB_NOT_FOUND

            result = await db.scalars(
                select(model.Image)
                  .where(model.Image.job_id == job_id)
                  .order_by(model.Image.order.asc())
            )
            job_images = list(result.all())
            return job_images, AppCode.IMAGES_RETRIEVED

    except exc.SQLAlchemyError as e:
        raise DBError('Failed reading images from database') from e


async def get_image_for_job(*, db: AsyncSession, job_id: UUID, image_id: UUID) -> Tuple[Optional[model.Image], AppCode]:
    try:
        async with db.begin():
            result = await db.execute(
                select(model.Image).
                where(model.Image.id == image_id).
                where(model.Image.job_id == job_id)
            )
            db_image = result.scalar_one_or_none()

            if db_image is None:
                return None, AppCode.IMAGE_NOT_FOUND_FOR_JOB

            return db_image, AppCode.IMAGE_RETRIEVED

    except exc.SQLAlchemyError as e:
        raise DBError(f"Failed reading image from database") from e


async def update_image(*, db: AsyncSession, image_id: UUID, image_update: base_objects.ImageUpdate) -> AppCode:
    try:
        async with db.begin():
            result = await db.execute(
                select(model.Image).where(model.Image.id == image_id).with_for_update()
            )
            db_image = result.scalar_one_or_none()

            if db_image is None:
                return AppCode.IMAGE_NOT_FOUND

            if image_update.image_uploaded is not None:
                db_image.image_uploaded = image_update.image_uploaded

            if image_update.alto_uploaded is not None:
                db_image.alto_uploaded = image_update.alto_uploaded

            if image_update.imagehash is not None:
                db_image.imagehash = image_update.imagehash

            return AppCode.IMAGE_UPDATED

    except exc.SQLAlchemyError as e:
        raise DBError(f"Failed updating image in database") from e


