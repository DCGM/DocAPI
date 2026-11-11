"""Queue jobs when each required input is uploaded or supplied by URL."""

import asyncio
import logging
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import select, exc, exists, func, literal, or_, and_, not_, update
from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.database import DBError, open_session, transaction
from doc_api.api.schemas.base_objects import ProcessingState
from doc_api.config import config
from doc_api.db import model


logger = logging.getLogger(__name__)


async def start_job(*, db: AsyncSession, job_id: UUID) -> bool:
    """Queue a NEW job when required inputs have uploads/URLs and metadata is saved."""
    try:
        async with transaction(db):
            img_missing = exists(
                select(literal(1))
                .select_from(model.Image)
                .where(
                    model.Image.job_id == job_id,
                    model.Image.image_uploaded.is_(False),
                    model.Image.image_url_provided.is_(False),
                )
            )
            alto_missing = exists(
                select(literal(1))
                .select_from(model.Image)
                .where(
                    model.Image.job_id == job_id,
                    model.Image.alto_uploaded.is_(False),
                    model.Image.alto_url_provided.is_(False),
                )
            )
            page_missing = exists(
                select(literal(1))
                .select_from(model.Image)
                .where(
                    model.Image.job_id == job_id,
                    model.Image.page_uploaded.is_(False),
                    model.Image.page_url_provided.is_(False),
                )
            )

            # Implication forms
            meta_ok = or_(
                model.Job.meta_json_required.is_(False),
                model.Job.meta_json_uploaded.is_(True),
            )

            images_ok = and_(
                # Cleanup can remove image records before an admin reopens a job.
                # Missing-input checks alone would treat deleted records as ready.
                model.Job.image_count == (
                    select(func.count()).select_from(model.Image)
                    .where(model.Image.job_id == job_id).scalar_subquery()
                ),
                not_(img_missing),
                or_(model.Job.alto_required.is_(False), not_(alto_missing)),
                or_(model.Job.page_required.is_(False), not_(page_missing)),
            )

            now = datetime.now(timezone.utc)

            stmt = (
                update(model.Job)
                .where(
                    model.Job.id == job_id,
                    model.Job.state == ProcessingState.NEW,
                    meta_ok,
                    images_ok,
                )
                .values(
                    state=ProcessingState.QUEUED,
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


async def queue_ready_jobs(db):
    last_id = None
    while True:
        async with db.begin():
            query = select(model.Job.id).where(model.Job.state == ProcessingState.NEW)
            if last_id is not None:
                query = query.where(model.Job.id > last_id)
            # Coordinate with input installation, cancellation, and other API
            # processes. Busy jobs can wait until the next check.
            job_ids = list(await db.scalars(
                query.order_by(model.Job.id).limit(100).with_for_update(skip_locked=True)))
            if not job_ids:
                return
            last_id = job_ids[-1]
            for job_id in job_ids:
                if await start_job(db=db, job_id=job_id):
                    logger.info("Queued ready job %s", job_id)


async def run_job_readiness_checks():
    while True:
        try:
            async with open_session() as db:
                await queue_ready_jobs(db)
        except Exception:
            logger.exception("Job readiness check failed; will retry")
        await asyncio.sleep(config.JOB_READINESS_CHECK_INTERVAL_SECONDS)
