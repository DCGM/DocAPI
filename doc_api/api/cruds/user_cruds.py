import logging
from datetime import datetime, timezone
from typing import List, Tuple, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict
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

    model_config = ConfigDict(extra="forbid")


class JobDefinition(BaseModel):
    images: List[ImageForJobDefinition]
    meta_json_required: bool = False
    alto_required: bool = False
    page_required: bool = False
    engine_name: Optional[str] = None
    engine_version: Optional[str] = None

    model_config = ConfigDict(extra="forbid")


async def create_job(*, db: AsyncSession, key_id: UUID, job_definition: JobDefinition, active_engine: bool = False) -> Tuple[Optional[model.Job], AppCode]:
    try:
        async with db.begin():
            result = await db.execute(
                select(model.Key).where(model.Key.id == key_id)
            )
            db_key = result.scalar_one_or_none()
            if db_key is None:
                return None, AppCode.API_KEY_INVALID

            db_engine_id = None
            if job_definition.engine_name is not None:
                query = select(model.Engine).where(model.Engine.name == job_definition.engine_name)
                if job_definition.engine_version is not None:
                    query = query.where(model.Engine.version == job_definition.engine_version)
                result = await db.execute(query)

                db_engines = result.scalars().all()
                if not db_engines:
                    return None, AppCode.ENGINE_NOT_FOUND

                logger.info(f"Found {len(db_engines)} engines matching name/version")

                if active_engine or len(db_engines) > 1:
                    active_engines = [e for e in db_engines if e.active]
                    if not active_engines:
                        return None, AppCode.ENGINE_INACTIVE
                    db_engines = [active_engines[0]]

                db_engine_id = db_engines[0].id
            else:
                result = await db.execute(
                    select(model.Engine).
                    where(model.Engine.default.is_(True)).
                    where(model.Engine.active.is_(True))
                )
                db_engine = result.scalar_one_or_none()
                if db_engine is not None:
                    db_engine_id = db_engine.id

            db_job = model.Job(
                owner_key_id=key_id,
                engine_id=db_engine_id,
                definition=job_definition.model_dump(mode="json"),
                alto_required=job_definition.alto_required,
                page_required=job_definition.page_required,
                meta_json_required=job_definition.meta_json_required)

            db.add(db_job)

            for img in job_definition.images:
                db_image = model.Image(
                    job=db_job,
                    name=img.name,
                    order=img.order
                )
                db.add(db_image)

            return db_job, AppCode.JOB_CREATED

    except exc.SQLAlchemyError as e:
        raise DBError("Failed creating new job.") from e


async def get_image_by_job_and_name(*, db: AsyncSession, job_id: UUID, image_name: str) -> Tuple[Optional[model.Image], AppCode]:
    try:
        async with db.begin():
            result = await db.execute(
                select(model.Image).
                where(model.Image.job_id == job_id).
                where(model.Image.name == image_name)
            )
            db_image = result.scalar_one_or_none()

            if db_image is None:
                return None, AppCode.IMAGE_NOT_FOUND_FOR_JOB

            return db_image, AppCode.IMAGE_RETRIEVED

    except exc.SQLAlchemyError as e:
        raise DBError(f"Failed reading image.") from e


async def start_job(*, db: AsyncSession, job_id: UUID) -> bool:
    try:
        async with db.begin():
            img_missing = exists(
                select(literal(1))
                .select_from(model.Image)
                .where(
                    model.Image.job_id == job_id,
                    model.Image.image_uploaded.is_(False),
                )
            )
            alto_missing = exists(
                select(literal(1))
                .select_from(model.Image)
                .where(
                    model.Image.job_id == job_id,
                    model.Image.alto_uploaded.is_(False),
                )
            )
            page_missing = exists(
                select(literal(1))
                .select_from(model.Image)
                .where(
                    model.Image.job_id == job_id,
                    model.Image.page_uploaded.is_(False),
                )
            )

            # Implication forms
            meta_ok = or_(
                model.Job.meta_json_required.is_(False),
                model.Job.meta_json_uploaded.is_(True),
            )

            images_ok = and_(
                not_(img_missing),
                or_(model.Job.alto_required.is_(False), not_(alto_missing)),
                or_(model.Job.page_required.is_(False), not_(page_missing)),
            )

            now = datetime.now(timezone.utc)

            stmt = (
                update(model.Job)
                .where(
                    model.Job.id == job_id,
                    model.Job.state == base_objects.ProcessingState.NEW,
                    meta_ok,
                    images_ok,
                )
                .values(
                    state=base_objects.ProcessingState.QUEUED,
                    last_change=now,
                )
                .returning(model.Job.id, model.Job.engine_id)
            )

            res = await db.execute(stmt)
            row = res.first()

            if not row:
                return False

            _job_id, engine_id = row

            if engine_id is not None:
                await db.execute(
                    update(model.Engine)
                    .where(model.Engine.id == engine_id)
                    .values(last_used=now)
                )

            return True

    except exc.SQLAlchemyError as e:
        raise DBError("Failed updating job state.") from e



async def cancel_job(db: AsyncSession, job_id: UUID) -> AppCode:
    try:
        async with db.begin():
            result = await db.execute(
                select(model.Job).where(model.Job.id == job_id).with_for_update()
            )
            db_job = result.scalar_one_or_none()

            if db_job is None:
                return AppCode.JOB_NOT_FOUND

            if db_job.state in (base_objects.ProcessingState.DONE,
                                base_objects.ProcessingState.FAILED,
                                base_objects.ProcessingState.CANCELLED):
                return AppCode.JOB_UNCANCELLABLE

            db_job.state = base_objects.ProcessingState.CANCELLED
            db_job.finished_date = datetime.now(timezone.utc)

            return AppCode.JOB_CANCELLED

    except exc.SQLAlchemyError as e:
        raise DBError("Failed cancelling job in database") from e