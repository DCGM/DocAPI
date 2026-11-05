"""Recover expired leases and retryable errors independently of worker requests."""
import asyncio
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import and_, or_, select

from doc_api.api.database import open_session
from doc_api.api.schemas.base_objects import ProcessingState
from doc_api.config import config
from doc_api.db import model

logger = logging.getLogger(__name__)


async def check_job_leases(db):
    now = datetime.now(timezone.utc)
    recoverable = model.Job.state == ProcessingState.ERROR
    if config.JOB_TIMEOUT_SECONDS > 0:
        cutoff = now - timedelta(seconds=config.JOB_TIMEOUT_SECONDS + config.JOB_TIMEOUT_GRACE_SECONDS)
        recoverable = or_(recoverable, and_(
            model.Job.state == ProcessingState.PROCESSING, model.Job.last_change < cutoff))
    last_id = None
    while True:
        async with db.begin():
            query = select(model.Job).where(recoverable)
            if last_id is not None:
                query = query.where(model.Job.id > last_id)
            jobs = list(await db.scalars(query.order_by(model.Job.id).limit(100)
                                        .execution_options(populate_existing=True)
                                        .with_for_update(skip_locked=True)))
            if not jobs:
                return
            last_id = jobs[-1].id
            for job in jobs:
                attempts = job.previous_attempts if job.previous_attempts is not None else -1
                if attempts < config.JOB_MAX_ATTEMPTS - 1:
                    job.state = ProcessingState.QUEUED
                    job.worker_key_id = None
                    job.progress = 0.0
                else:
                    job.state = ProcessingState.FAILED
                    job.finished_date = now
                    job.progress = 1.0
                job.last_change = now
                logger.info("Lease checker changed job %s to %s", job.id, job.state.value)


async def run_job_lease_checks():
    while True:
        try:
            async with open_session() as db:
                await check_job_leases(db)
        except Exception:
            logger.exception("Job lease check failed; will retry")
        await asyncio.sleep(config.JOB_LEASE_CHECK_INTERVAL_SECONDS)
