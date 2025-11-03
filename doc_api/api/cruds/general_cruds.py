import logging
from datetime import datetime
from typing import Tuple, Optional, List
from uuid import UUID

from sqlalchemy import select, exc, Row
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
        raise DBError(f"Failed reading job.") from e


async def get_jobs_with_engines(*, db: AsyncSession,
                                key_id: Optional[UUID] = None,
                                state: Optional[base_objects.ProcessingState] = None,
                                engine_name: Optional[str] = None,
                                engine_version: Optional[str] = None,
                                from_created_date: Optional[datetime] = None,
                                from_started_date: Optional[datetime] = None,
                                from_last_change: Optional[datetime] = None,
                                from_finished_date: Optional[datetime] = None) -> Tuple[List[Tuple[model.Job, model.Engine]], AppCode]:
    try:
        async with db.begin():
            query = select(model.Job, model.Engine).join(model.Engine)

            if key_id is not None:
                query = query.where(model.Job.owner_key_id == key_id)
            if state is not None:
                query = query.where(model.Job.state == state)
            if engine_name is not None:
                query = query.where(model.Engine.name == engine_name)
            if engine_version is not None:
                query = query.where(model.Engine.version == engine_version)
            if from_created_date is not None:
                query = query.where(model.Job.created_date >= from_created_date)
            if from_started_date is not None:
                query = query.where(model.Job.started_date >= from_started_date)
            if from_last_change is not None:
                query = query.where(model.Job.last_change >= from_last_change)
            if from_finished_date is not None:
                query = query.where(model.Job.finished_date >= from_finished_date)

            query = query.order_by(model.Job.created_date.desc())

            result = await db.execute(query)
            rows = list(result.all())
            return rows, AppCode.JOBS_RETRIEVED

    except exc.SQLAlchemyError as e:
        raise DBError("Failed reading jobs.") from e


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
        raise DBError(f"Failed updating job.") from e


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
        raise DBError("Failed reading images.") from e


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
        raise DBError(f"Failed reading image.") from e


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

            if image_update.page_uploaded is not None:
                db_image.page_uploaded = image_update.page_uploaded

            if image_update.imagehash is not None:
                db_image.imagehash = image_update.imagehash

            return AppCode.IMAGE_UPDATED

    except exc.SQLAlchemyError as e:
        raise DBError(f"Failed updating image.") from e


async def get_engine(*, db: AsyncSession, engine_id: UUID) -> Tuple[Optional[model.Engine], AppCode]:
    try:
        async with db.begin():
            result = await db.execute(
                select(model.Engine).where(model.Engine.id == engine_id)
            )
            db_engine = result.scalar_one_or_none()

            if db_engine is None:
                return None, AppCode.ENGINE_NOT_FOUND

            return db_engine, AppCode.ENGINE_RETRIEVED

    except exc.SQLAlchemyError as e:
        raise DBError(f"Failed reading engine.") from e


async def get_engines(*, db: AsyncSession,
                      engine_name: Optional[str],
                      engine_version: Optional[str],
                      default: Optional[bool] = None,
                      active: Optional[bool] = None) -> Tuple[List[model.Engine], AppCode]:
    try:
        async with db.begin():
            query = select(model.Engine)
            if engine_name is not None:
                query = query.where(model.Engine.name == engine_name)
            if engine_version is not None:
                query = query.where(model.Engine.version == engine_version)
            if default is not None:
                query = query.where(model.Engine.default.is_(default))
            if active is not None:
                query = query.where(model.Engine.active.is_(active))

            result = await db.scalars(query)
            db_engines = list(result.all())

            return db_engines, AppCode.ENGINES_RETRIEVED

    except exc.SQLAlchemyError as e:
        raise DBError(f"Failed reading engines.") from e




