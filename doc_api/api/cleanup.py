import asyncio
import logging
import os
import shutil
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.database import open_session
from doc_api.api.schemas.base_objects import ProcessingState
from doc_api.config import config
from doc_api.db import model


logger = logging.getLogger(__name__)


def _remove_job_data(path, directory):
    # Do not follow or remove links in place of job data.
    if os.path.islink(path):
        return False
    try:
        if directory:
            shutil.rmtree(path)
        else:
            os.remove(path)
    except FileNotFoundError:
        # Recover if a previous cleanup removed the files but its DB commit failed.
        pass
    return True


async def _delete_job_data(db, target, directory, cutoff=None):
    column = getattr(model.Job, f"{target}_deleted_at")
    last_id = None
    while True:
        async with db.begin():
            query = select(model.Job).where(
                model.Job.state.in_([ProcessingState.DONE, ProcessingState.FAILED]),
                column.is_(None),
            )
            if cutoff is not None:
                query = query.where(model.Job.finished_date <= cutoff)
            if last_id is not None:
                query = query.where(model.Job.id > last_id)
            jobs = list(await db.scalars(query.order_by(model.Job.id).limit(100).with_for_update(skip_locked=True)))
            if not jobs:
                return
            last_id = jobs[-1].id
            # Keep row locks through removal and timestamp recording to prevent requeue races.
            for job in jobs:
                is_input = target == "inputs"
                name = str(job.id) if is_input else f"{job.id}.zip"
                path = os.path.join(directory, name)
                try:
                    removed = await asyncio.to_thread(_remove_job_data, path, is_input)
                except OSError:
                    logger.exception("Could not delete %s for job %s; will retry", target, job.id)
                    continue
                if removed:
                    if is_input and config.DELETE_JOB_IMAGE_RECORDS_WITH_INPUTS:
                        await db.execute(delete(model.Image).where(model.Image.job_id == job.id))
                    setattr(job, f"{target}_deleted_at", datetime.now(timezone.utc))
                    logger.info("Cleaned up %s for job %s", target, job.id)


async def delete_completed_job_inputs(db: AsyncSession):
    if config.DELETE_JOB_INPUTS_ON_COMPLETION:
        cutoff = None
        if config.INPUT_RETENTION_SECONDS > 0:
            cutoff = datetime.now(timezone.utc) - timedelta(seconds=config.INPUT_RETENTION_SECONDS)
        await _delete_job_data(db, "inputs", config.JOBS_DIR, cutoff)


async def delete_expired_job_outputs(db: AsyncSession):
    if not config.DELETE_JOB_OUTPUTS:
        return
    cutoff = None
    if config.OUTPUT_RETENTION_SECONDS > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=config.OUTPUT_RETENTION_SECONDS)
    directories = {"results": config.RESULTS_DIR, "artifacts": config.ARTIFACTS_DIR}
    for target in sorted(config.OUTPUT_CLEANUP_TARGETS):
        await _delete_job_data(db, target, directories[target], cutoff)


async def run_cleanup():
    while True:
        for clean in (delete_completed_job_inputs, delete_expired_job_outputs):
            try:
                async with open_session() as db:
                    await clean(db)
            except Exception:
                logger.exception("%s failed; will retry", clean.__name__)
        await asyncio.sleep(config.CLEANUP_INTERVAL_SECONDS)
