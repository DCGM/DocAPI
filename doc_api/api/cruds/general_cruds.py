import logging
from datetime import datetime, timezone
from typing import Tuple, Optional, List
from uuid import UUID

from sqlalchemy import select, exc, Row, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased, defer

from doc_api.api.database import DBError, transaction
from doc_api.api.schemas import base_objects
from doc_api.api.schemas.responses import AppCode
from doc_api.db import model


logger = logging.getLogger(__name__)


async def get_job(*, db: AsyncSession, job_id: UUID) -> Tuple[Optional[model.Job], AppCode]:
    try:
        async with transaction(db):
            result = await db.execute(
                # Sessions retain ORM objects after commit. A subsequent read
                # must refresh them if another transaction changed the job.
                select(model.Job).where(model.Job.id == job_id).execution_options(populate_existing=True)
            )
            db_job = result.scalar_one_or_none()

            if db_job is None:
                return None, AppCode.JOB_NOT_FOUND

            return db_job, AppCode.JOB_RETRIEVED

    except exc.SQLAlchemyError as e:
        raise DBError(f"Failed reading job.") from e


def jobs_query(*, key_id: Optional[UUID] = None, owner_label: Optional[str] = None,
               worker_label: Optional[str] = None, state: Optional[list[base_objects.ProcessingState]] = None,
               engine_name: Optional[str] = None, engine_version: Optional[str] = None,
               from_created_date: Optional[datetime] = None, from_started_date: Optional[datetime] = None,
               from_last_change: Optional[datetime] = None, from_finished_date: Optional[datetime] = None):
    """Common job-list selection used by listing and matching statistics."""
    owner = aliased(model.Key)
    worker = aliased(model.Key)
    query = select(model.Job, model.Engine.name, model.Engine.version, owner.label, worker.label).outerjoin(
        model.Engine, model.Engine.id == model.Job.engine_id).join(
        owner, owner.id == model.Job.owner_key_id).outerjoin(
        worker, worker.id == model.Job.worker_key_id)
    if key_id is not None:
        query = query.where(model.Job.owner_key_id == key_id)
    if owner_label is not None:
        query = query.where(owner.label == owner_label)
    if worker_label is not None:
        query = query.where(worker.label == worker_label)
    if state is not None:
        query = query.where(model.Job.state.in_(state))
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

    return query


async def get_jobs_with_engines(*, db: AsyncSession,
                                key_id: Optional[UUID] = None,
                                owner_label: Optional[str] = None,
                                worker_label: Optional[str] = None,
                                limit: Optional[int] = None,
                                offset: int = 0,
                                sort_by: base_objects.JobSort = 'created_date',
                                sort_order: base_objects.SortOrder = 'desc',
                                state: Optional[list[base_objects.ProcessingState]] = None,
                                engine_name: Optional[str] = None,
                                engine_version: Optional[str] = None,
                                result_available: Optional[bool] = None,
                                from_created_date: Optional[datetime] = None,
                                from_started_date: Optional[datetime] = None,
                                from_last_change: Optional[datetime] = None,
                                from_finished_date: Optional[datetime] = None,
                                include_logs: bool = True,
                                include_engine_settings: bool = True) -> Tuple[List[Tuple[model.Job, Optional[str], Optional[str], str, Optional[str]]], int, AppCode]:
    try:
        async with db.begin():
            query = jobs_query(
                key_id=key_id, owner_label=owner_label, worker_label=worker_label, state=state,
                engine_name=engine_name, engine_version=engine_version,
                from_created_date=from_created_date, from_started_date=from_started_date,
                from_last_change=from_last_change, from_finished_date=from_finished_date)
            if result_available is not None:
                available = ((model.Job.state == base_objects.ProcessingState.DONE)
                             & model.Job.results_deleted_at.is_(None))
                query = query.where(available if result_available else ~available)
            if not include_logs:
                query = query.options(*(defer(getattr(model.Job, field), raiseload=True)
                                        for field in base_objects.JOB_LOG_FIELDS))
            if not include_engine_settings:
                query = query.options(defer(model.Job.definition, raiseload=True))

            total = await db.scalar(select(func.count()).select_from(
                query.with_only_columns(model.Job.id).subquery()))
            timestamp = getattr(model.Job, sort_by)
            ordering = timestamp.asc() if sort_order == 'asc' else timestamp.desc()
            tie_breaker = model.Job.id.asc() if sort_order == 'asc' else model.Job.id.desc()
            query = query.order_by(ordering.nulls_last(), tie_breaker).offset(offset)
            if limit is not None:
                query = query.limit(limit)

            result = await db.execute(query)
            rows = list(result.all())
            return rows, total, AppCode.JOBS_RETRIEVED

    except exc.SQLAlchemyError as e:
        raise DBError("Failed reading jobs.") from e


async def update_job(*, db: AsyncSession, job_id: UUID, job_update: base_objects.JobUpdate,
                     append_logs: bool = True, apply_state_defaults: bool = False) -> AppCode:
    try:
        async with transaction(db):
            result = await db.execute(
                select(model.Job).where(model.Job.id == job_id)
                .execution_options(populate_existing=True).with_for_update()
            )
            db_job = result.scalar_one_or_none()

            if db_job is None:
                return AppCode.JOB_NOT_FOUND

            if apply_state_defaults:
                now = datetime.now(timezone.utc)
                db_job.last_change = now
                if job_update.state in {base_objects.ProcessingState.DONE, base_objects.ProcessingState.FAILED}:
                    db_job.progress = 1.0
                if job_update.state in {base_objects.ProcessingState.DONE, base_objects.ProcessingState.FAILED,
                                        base_objects.ProcessingState.ERROR, base_objects.ProcessingState.CANCELLED}:
                    db_job.finished_date = now

            # Explicit values take precedence over activity/finalization defaults.
            if job_update.state is not None:
                db_job.state = job_update.state

            if job_update.progress is not None:
                db_job.progress = job_update.progress

            if "previous_attempts" in job_update.model_fields_set:
                db_job.previous_attempts = job_update.previous_attempts

            if job_update.meta_json_uploaded is not None:
                db_job.meta_json_uploaded = job_update.meta_json_uploaded

            if job_update.meta_json_required is not None:
                db_job.meta_json_required = job_update.meta_json_required

            if job_update.alto_required is not None:
                db_job.alto_required = job_update.alto_required

            if job_update.page_required is not None:
                db_job.page_required = job_update.page_required

            if job_update.created_date is not None:
                db_job.created_date = job_update.created_date

            if "started_date" in job_update.model_fields_set:
                db_job.started_date = job_update.started_date

            if job_update.last_change is not None:
                db_job.last_change = job_update.last_change

            if "finished_date" in job_update.model_fields_set:
                db_job.finished_date = job_update.finished_date

            for field in ("log", "log_user"):
                if field in job_update.model_fields_set:
                    db_job.update_log(field, getattr(job_update, field), append=append_logs)

            for field in ("final_log", "final_log_user"):
                if field in job_update.model_fields_set:
                    db_job.update_log(field, getattr(job_update, field), append=False)

            return {
                base_objects.ProcessingState.CANCELLED: AppCode.JOB_CANCELLED,
                base_objects.ProcessingState.DONE: AppCode.JOB_COMPLETED,
                base_objects.ProcessingState.ERROR: AppCode.JOB_MARKED_ERROR,
                base_objects.ProcessingState.FAILED: AppCode.JOB_MARKED_FAILED,
            }.get(job_update.state, AppCode.JOB_UPDATED)

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
        async with transaction(db):
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


async def get_engine_by_name_and_version(*, db: AsyncSession,
                                         engine_name: str,
                                         engine_version: str) -> Tuple[Optional[model.Engine], AppCode]:
    try:
        async with db.begin():
            result = await db.execute(
                select(model.Engine).
                where(model.Engine.name == engine_name).
                where(model.Engine.version == engine_version)
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




