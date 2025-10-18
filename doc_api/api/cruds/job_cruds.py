import logging
from typing import Tuple, Optional
from uuid import UUID

from sqlalchemy import select, exc
from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.database import DBError
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