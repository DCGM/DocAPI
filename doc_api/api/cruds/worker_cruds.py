import logging
from datetime import datetime, timezone, timedelta
from typing import Tuple, Optional
from uuid import UUID

from sqlalchemy import select, exc, or_, and_, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.database import DBError
from doc_api.api.schemas.base_objects import ProcessingState
from doc_api.api.schemas.responses import AppCode
from doc_api.api.config import config
from doc_api.db import model
from doc_api.api.schemas import base_objects


logger = logging.getLogger(__name__)


async def assign_job_to_worker(*, db: AsyncSession, worker_key_id: UUID) -> Tuple[Optional[model.Job], AppCode]:
    try:
        async with db.begin():
            # 1) Retry timed-out or ERROR jobs
            now = datetime.now(timezone.utc)
            timeout_threshold = now - timedelta(seconds=config.JOB_TIMEOUT_SECONDS) - timedelta(seconds=config.JOB_TIMEOUT_GRACE_SECONDS)
            max_attempts_minus_1 = config.JOB_MAX_ATTEMPTS - 1

            previous_attempts = func.coalesce(model.Job.previous_attempts, -1)

            retryable_predicate = or_(
                and_(
                    model.Job.state == base_objects.ProcessingState.PROCESSING,
                    model.Job.last_change < timeout_threshold,
                ),
                model.Job.state == base_objects.ProcessingState.ERROR,
            )

            await db.execute(
                update(model.Job)
                .where(retryable_predicate, previous_attempts < max_attempts_minus_1)
                .values(
                    state=base_objects.ProcessingState.QUEUED,
                    worker_key_id=None,
                    last_change=now,
                    progress=0.0
                )
            )

            await db.execute(
                update(model.Job)
                .where(retryable_predicate, previous_attempts >= max_attempts_minus_1)
                .values(
                    state=base_objects.ProcessingState.FAILED,
                    last_change=now,
                    finished_date=now,
                    progress=1.0,
                )
            )

            # 2) Pick one QUEUED job atomically
            result = await db.execute(
                select(model.Job)
                .where(model.Job.state == base_objects.ProcessingState.QUEUED)
                .order_by(model.Job.created_date.asc())
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            db_job = result.scalar_one_or_none()
            if db_job is None:
                return None, AppCode.JOB_QUEUE_EMPTY

            db_job.state = base_objects.ProcessingState.PROCESSING
            db_job.started_date = now
            db_job.last_change = now
            db_job.worker_key_id = worker_key_id
            db_job.previous_attempts = (db_job.previous_attempts or 0) + 1

        return db_job, AppCode.JOB_ASSIGNED

    except exc.SQLAlchemyError as e:
        raise DBError("Failed assigning Job to worker.") from e


async def update_processing_job_lease(*, db: AsyncSession, job_id: UUID) -> Tuple[AppCode, Optional[datetime], Optional[datetime]]:
    try:
        async with db.begin():
            result = await db.execute(
                select(model.Job)
                .where(model.Job.id == job_id)
                .with_for_update()
            )
            db_job = result.scalar_one_or_none()
            if db_job is None:
                return AppCode.JOB_NOT_FOUND, None, None
            if db_job.state != base_objects.ProcessingState.PROCESSING:
                return AppCode.JOB_NOT_IN_PROCESSING, None, None

            lease_expire_at, server_time = get_new_lease()
            db_job.last_change = server_time

            return AppCode.JOB_LEASE_EXTENDED, lease_expire_at, server_time

    except exc.SQLAlchemyError as e:
        raise DBError("Failed updating Job lease.") from e


async def update_processing_job_progress(*, db: AsyncSession, job_id: UUID, job_progress_update: base_objects.JobProgressUpdate) -> Tuple[Optional[base_objects.Job], Optional[datetime], Optional[datetime], AppCode]:
    try:
        async with db.begin():
            result = await db.execute(
                select(model.Job).where(model.Job.id == job_id).with_for_update()
            )
            db_job = result.scalar_one_or_none()
            if db_job is None:
                return None, None, None, AppCode.JOB_NOT_FOUND
            if db_job.state != base_objects.ProcessingState.PROCESSING:
                return db_job, None, None, AppCode.JOB_NOT_IN_PROCESSING

            if job_progress_update.progress is not None:
                p = job_progress_update.progress
                p = max(0.0, min(1.0, p))
                db_job.progress = p

            lease_expire_at, server_time = get_new_lease()
            db_job.last_change = server_time

            if job_progress_update.log:
                if db_job.log:
                    if not db_job.log.endswith("\n"):
                        db_job.log += "\n"
                    db_job.log += job_progress_update.log
                else:
                    db_job.log = job_progress_update.log

            if job_progress_update.log_user:
                if db_job.log_user:
                    if not db_job.log_user.endswith("\n"):
                        db_job.log_user += "\n"
                    db_job.log_user += job_progress_update.log_user
                else:
                    db_job.log_user = job_progress_update.log_user

            return db_job, lease_expire_at, server_time, AppCode.JOB_PROGRESS_UPDATED

    except exc.SQLAlchemyError as e:
        raise DBError(f"Failed updating job.") from e


def get_new_lease() -> Tuple[datetime, datetime]:
    server_time = datetime.now(timezone.utc)
    lease_expire_at = server_time + timedelta(seconds=config.JOB_TIMEOUT_SECONDS)
    return lease_expire_at, server_time


async def release_job_lease(*, db: AsyncSession, job_id: UUID) -> AppCode:
    try:
        async with db.begin():
            result = await db.execute(
                select(model.Job).where(model.Job.id == job_id).with_for_update()
            )
            db_job = result.scalar_one_or_none()
            if db_job is None:
                return AppCode.JOB_NOT_FOUND

            if db_job.state != base_objects.ProcessingState.PROCESSING:
                return AppCode.JOB_NOT_IN_PROCESSING

            now = datetime.now(timezone.utc)
            db_job.last_change = now
            db_job.worker_key_id = None
            db_job.state = base_objects.ProcessingState.QUEUED

            return AppCode.JOB_LEASE_RELEASED

    except exc.SQLAlchemyError as e:
        raise DBError("Failed releasing job lease.") from e


async def complete_job(*, db: AsyncSession, job_id: UUID) -> AppCode:
    try:
        async with db.begin():
            result = await db.execute(
                select(model.Job).where(model.Job.id == job_id).with_for_update()
            )
            db_job = result.scalar_one_or_none()
            if db_job is None:
                return AppCode.JOB_NOT_FOUND

            if db_job.state == ProcessingState.DONE:
                return AppCode.JOB_ALREADY_COMPLETED

            db_job.state = ProcessingState.DONE
            db_job.progress = 1.0

            finished_date = datetime.now(timezone.utc)
            db_job.finished_date = finished_date
            db_job.last_change = finished_date

    except exc.SQLAlchemyError as e:
        raise DBError("Failed finishing job in database.") from e


async def fail_job(*, db: AsyncSession, job_id: UUID) -> AppCode:
    try:
        async with db.begin():
            result = await db.execute(
                select(model.Job).where(model.Job.id == job_id).with_for_update()
            )
            db_job = result.scalar_one_or_none()
            if db_job is None:
                return AppCode.JOB_NOT_FOUND

            db_job.state = ProcessingState.FAILED

            finished_date = datetime.now(timezone.utc)
            db_job.finished_date = finished_date
            db_job.last_change = finished_date

    except exc.SQLAlchemyError as e:
        raise DBError("Failed failing job in database.") from e


async def get_log_header_for_job(db: AsyncSession, job_id: UUID) -> str:
    result = await db.execute(
        select(model.Job).where(model.Job.id == job_id)
    )
    db_job = result.scalar_one_or_none()
    if db_job is None:
        raise DBError(f"Job '{job_id}' does not exist", code="JOB_NOT_FOUND", status_code=404)

    db_worker_key = await db.execute(
        select(model.Key).where(model.Key.id == db_job.worker_key_id)
    )
    db_worker_key = db_worker_key.scalar_one_or_none()
    db_owner_key = await db.execute(
        select(model.Key).where(model.Key.id == db_job.owner_key_id)
    )
    db_owner_key = db_owner_key.scalar_one_or_none()
    log_header = (f"\n\n"
                  f"JOB_UPDATE_STAMP - {db_job.last_change} - {config.SERVER_NAME}\n"
                  f"########################################################################\n"
                  f"OWNER_LABEL: {db_owner_key.label}\n"
                  f"WORKER_LABEL: {None if db_worker_key is None else db_worker_key.label}\n"
                  f"JOB_STATE: {db_job.state}\n"
                  f"JOB_PROGRESS: {db_job.progress}\n"
                  f"JOB_PREVIOUS_ATTEMPTS: {db_job.previous_attempts}\n"
                  f"\n"
                  f"JOB_CREATED: {db_job.created_date}\n"
                  f"JOB_STARTED: {db_job.started_date}\n"
                  f"JOB_LAST_CHANGE: {db_job.last_change}\n"
                  f"JOB_FINISHED: {db_job.finished_date}\n"
                  f"\n"
                  f"OWNER_ID: {db_owner_key.id}\n"
                  f"WORKER_ID: {None if db_worker_key is None else db_worker_key.id}\n"
                  f"JOB_ID: {db_job.id}\n"
                  f"########################################################################\n"
                  f"\n\n")
    return log_header







