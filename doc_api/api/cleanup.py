import asyncio
import logging
import os
import shutil
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.database import open_session
from doc_api.api.schemas.base_objects import ProcessingState
from doc_api.config import config
from doc_api.db import model


logger = logging.getLogger(__name__)


def _input_directories():
    """Only consider canonical job UUID directories, without following symlinks."""
    try:
        with os.scandir(config.JOBS_DIR) as entries:
            jobs = []
            for entry in entries:
                if not entry.is_dir(follow_symlinks=False):
                    continue
                try:
                    job_id = UUID(entry.name)
                except ValueError:
                    continue
                if entry.name == str(job_id):
                    jobs.append(job_id)
            return jobs
    except FileNotFoundError:
        return []


async def delete_completed_job_inputs(db: AsyncSession):
    """Delete input files for terminal jobs, preserving all database records."""
    if not config.DELETE_JOB_INPUTS_ON_COMPLETION:
        return

    candidates = await asyncio.to_thread(_input_directories)
    for offset in range(0, len(candidates), 1000):
        async with db.begin():
            completed = list(await db.scalars(
                select(model.Job.id).where(
                    model.Job.id.in_(candidates[offset:offset + 1000]),
                    model.Job.state.in_([ProcessingState.DONE, ProcessingState.FAILED]),
                ).with_for_update(skip_locked=True)
            ))

            # Hold row locks until deletion finishes so a job cannot be requeued mid-cleanup.
            for job_id in completed:
                path = os.path.join(config.JOBS_DIR, str(job_id))
                try:
                    await asyncio.to_thread(shutil.rmtree, path)
                    logger.info("Deleted input directory for completed job %s", job_id)
                except FileNotFoundError:
                    # Another cleanup process may already have removed it.
                    pass
                except OSError:
                    logger.exception("Could not delete input directory for job %s; will retry", job_id)


async def run_input_cleanup():
    while True:
        try:
            async with open_session() as db:
                await delete_completed_job_inputs(db)
        except Exception:
            logger.exception("Input cleanup failed; will retry")
        await asyncio.sleep(config.CLEANUP_INTERVAL_SECONDS)
