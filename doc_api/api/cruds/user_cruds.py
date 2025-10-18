import logging
from datetime import datetime, timezone
from typing import List, Tuple, Optional
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy import select, exc, exists, literal, or_, and_, not_, update
from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.database import DBError
from doc_api.api.schemas.responses import AppCode
from doc_api.db import model
from doc_api.api.schemas import base_objects


logger = logging.getLogger(__name__)


class ImageForJobDefinition(BaseModel):
    name: str
    order: int


class JobDefinition(BaseModel):
    images: List[ImageForJobDefinition]
    alto_required: bool = False
    meta_json_required: bool = False


async def create_job(db: AsyncSession, key_id: UUID, job_definition: JobDefinition) -> model.Job:
    try:
        result = await db.execute(
            select(model.Key).where(model.Key.id == key_id)
        )
        db_key = result.scalar_one_or_none()
        if db_key is None:
            raise DBError(f"Key '{key_id}' does not exist", code="KEY_NOT_FOUND", status_code=404)

        db_job = model.Job(
            owner_key_id=key_id,
            definition=job_definition.model_dump(mode="json"),
            alto_required=job_definition.alto_required,
            meta_json_required=job_definition.meta_json_required)

        db.add(db_job)

        for img in job_definition.images:
            db_image = model.Image(
                job=db_job,
                name=img.name,
                order=img.order
            )
            db.add(db_image)

        await db.commit()
        return db_job
    except exc.SQLAlchemyError as e:
        raise DBError("Failed creating new job in database", status_code=500) from e


async def get_image_by_job_and_name(db: AsyncSession, job_id: UUID, image_name: str) -> model.Image:
    try:
        result = await db.execute(
            select(model.Image).where(
                model.Image.job_id == job_id,
                model.Image.name == image_name
            )
        )
        db_image = result.scalar_one_or_none()
        if db_image is None:
            raise DBError(f"Image '{image_name}' for Job '{job_id}' does not exist", code="IMAGE_NOT_FOUND", status_code=404)
        return db_image
    except exc.SQLAlchemyError as e:
        raise DBError(f"Failed reading image from database", status_code=500) from e


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


async def get_jobs(db: AsyncSession, key_id: UUID) -> List[model.Job]:
    try:
        result = await db.scalars(
            select(model.Job)
              .where(model.Job.owner_key_id == key_id)
              .order_by(model.Job.created_date.desc())
        )
        return list(result.all())
    except exc.SQLAlchemyError as e:
        raise DBError('Failed reading jobs from database') from e


async def start_job(db: AsyncSession, job_id: UUID) -> bool:
    try:
        # EXISTS: is there any image not uploaded?
        img_missing = exists(
            select(literal(1))
              .select_from(model.Image)
              .where(
                  model.Image.job_id == job_id,
                  model.Image.image_uploaded.is_(False),
              )
        )

        # EXISTS: is there any ALTO not uploaded?
        alto_missing = exists(
            select(literal(1))
              .select_from(model.Image)
              .where(
                  model.Image.job_id == job_id,
                  model.Image.alto_uploaded.is_(False),
              )
        )

        # meta condition: either not required OR (required AND already uploaded)
        meta_ok = or_(
            model.Job.meta_json_required.is_(False),
            and_(
                model.Job.meta_json_required.is_(True),
                model.Job.meta_json_uploaded.is_(True),
            ),
        )

        # readiness condition:
        # - if alto not required: all images uploaded  -> NOT img_missing
        # - if alto required: all images & all alto -> NOT img_missing AND NOT alto_missing
        ready = or_(
            and_(model.Job.alto_required.is_(False), not_(img_missing)),
            and_(model.Job.alto_required.is_(True),  not_(img_missing), not_(alto_missing)),
        )

        stmt = (
            update(model.Job)
            .where(
                model.Job.id == job_id,
                model.Job.state == base_objects.ProcessingState.NEW,
                meta_ok,
                ready,
            )
            .values(
                state=base_objects.ProcessingState.QUEUED,
                last_change=datetime.now(timezone.utc),
            )
            .returning(model.Job.id)   # tells us if an update happened
        )

        res = await db.execute(stmt)
        updated = res.scalar_one_or_none() is not None
        if updated:
            await db.commit()
            return True
        return False

    except exc.SQLAlchemyError as e:
        raise DBError("Failed updating job state in database") from e


async def cancel_job(db: AsyncSession, job_id: UUID) -> None:
    try:
        result = await db.execute(
            select(model.Job).where(model.Job.id == job_id).with_for_update()
        )
        db_job = result.scalar_one_or_none()
        if db_job is None:
            raise DBError(f"Job '{job_id}' does not exist", code="JOB_NOT_FOUND", status_code=404)

        db_job.state = base_objects.ProcessingState.CANCELLED
        db_job.finished_date = datetime.now(timezone.utc)
        await db.commit()

    except exc.SQLAlchemyError as e:
        raise DBError("Failed updating job state in database", status_code=500) from e