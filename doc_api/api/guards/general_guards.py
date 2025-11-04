from functools import wraps
from typing import Tuple
from uuid import UUID

import fastapi
from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.cruds import general_cruds
from doc_api.api.schemas.responses import DocAPIClientErrorException, AppCode, GENERAL_RESPONSES
from doc_api.db import model


JOB_EXISTS_GUARD_RESPONSES = {
    AppCode.JOB_NOT_FOUND: GENERAL_RESPONSES[AppCode.JOB_NOT_FOUND]
}
def challenge_job_exists(fn):
    @wraps(fn)
    async def wrapper(*args, **kwargs):
        job_id, db = _get_job_exists_params(kwargs)

        await _challenge_job_exists(db=db, job_id=job_id)

        return await fn(*args, **kwargs)

    wrapper.__challenge_job_exists__ = True
    return wrapper


async def _challenge_job_exists(
        *,
        db: AsyncSession,
        job_id: UUID) -> model.Job:
    db_job, code = await general_cruds.get_job(db=db, job_id=job_id)

    if code == AppCode.JOB_NOT_FOUND:
        raise DocAPIClientErrorException(
            status=fastapi.status.HTTP_404_NOT_FOUND,
            code=AppCode.JOB_NOT_FOUND,
            detail=JOB_EXISTS_GUARD_RESPONSES[AppCode.JOB_NOT_FOUND]["detail"],
        )

    return db_job


def _get_job_exists_params(kwargs: dict) -> Tuple[UUID, AsyncSession]:
    """
    Extracts `job_id`, `key`, and `db` from kwargs.
    Raises RuntimeError if any are missing or None.
    """
    required = ("job_id", "db")
    missing = [p for p in required if p not in kwargs or kwargs[p] is None]
    if missing:
        raise RuntimeError(
            f"[get_job_access_params] Missing required params: {', '.join(missing)}. "
            "Ensure your handler defines them and they are provided by Depends()."
        )

    job_id: UUID = kwargs["job_id"]
    db: AsyncSession = kwargs["db"]

    return job_id, db
