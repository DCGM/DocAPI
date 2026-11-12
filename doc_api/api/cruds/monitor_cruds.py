"""Read-only monitoring queries independent of authentication roles."""
from datetime import datetime
from uuid import UUID

from sqlalchemy import select, func, case, and_, or_, exc, text
from sqlalchemy.orm import aliased
from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.database import DBError
from doc_api.api.schemas.base_objects import ProcessingState, KeyRole
from doc_api.db import model


async def database_connected(*, db: AsyncSession) -> bool:
    try:
        async with db.begin():
            await db.execute(select(1))
        return True
    except exc.SQLAlchemyError:
        return False


async def get_database_stats(*, db: AsyncSession):
    # One snapshot of this database's client connections, excluding this query.
    # No SQL text or connection identities leave the database.
    query = text("""
        WITH activity AS (
            SELECT count(*) AS connections,
                   count(*) FILTER (WHERE state = 'active') AS active_connections,
                   count(*) FILTER (WHERE state = 'idle') AS idle_connections,
                   count(*) FILTER (WHERE state IN ('idle in transaction', 'idle in transaction (aborted)'))
                       AS idle_in_transaction_connections,
                   count(*) FILTER (WHERE wait_event_type = 'Lock') AS lock_waiting_connections,
                   max(GREATEST(0, EXTRACT(EPOCH FROM clock_timestamp() - query_start)))
                       FILTER (WHERE state = 'active') AS longest_active_query_seconds
            FROM pg_stat_activity
            WHERE datname = current_database() AND backend_type = 'client backend'
                  AND pid <> pg_backend_pid()
        )
        SELECT current_database() AS database_name,
               pg_database_size(current_database()) AS size_bytes,
               activity.*, stats.deadlocks, stats.stats_reset
        FROM activity CROSS JOIN pg_stat_database AS stats
        WHERE stats.datname = current_database()
    """)
    try:
        async with db.begin():
            return (await db.execute(query)).mappings().one()
    except exc.SQLAlchemyError as error:
        raise DBError('Failed reading monitoring database statistics.') from error


async def get_stats(*, db: AsyncSession, since: datetime | None,
                    owner_id: UUID | None, worker_id: UUID | None,
                    engine_name: str | None = None, engine_version: str | None = None):
    # Attempt counters exclude released leases. Individual attempt timestamps
    # and historical worker assignments are not stored.
    job = model.Job
    attempts = func.coalesce(job.previous_attempts + 1, 0)
    valid_dates = and_(job.started_date.is_not(None), job.finished_date.is_not(None),
                       job.finished_date >= job.started_date)
    elapsed = case((valid_dates, func.extract('epoch', job.finished_date - job.started_date)), else_=None)
    query = select(
        job.state, func.count().label('job_count'), func.sum(job.image_count).label('image_count'),
        func.sum(elapsed).label('runtime_seconds'), func.sum(attempts).label('attempt_count'),
        func.sum(attempts * job.image_count).label('attempt_image_count'),
    )
    if since is not None:
        activity_time = case((job.state.in_([ProcessingState.DONE, ProcessingState.FAILED, ProcessingState.ERROR]),
                              job.finished_date), else_=job.last_change)
        query = query.where(activity_time >= since)
    if owner_id is not None:
        query = query.where(job.owner_key_id == owner_id)
    if worker_id is not None:
        query = query.where(job.worker_key_id == worker_id)
    if engine_name is not None or engine_version is not None:
        query = query.join(model.Engine, model.Engine.id == job.engine_id)
        if engine_name is not None:
            query = query.where(model.Engine.name == engine_name)
        if engine_version is not None:
            query = query.where(model.Engine.version == engine_version)
    try:
        async with db.begin():
            return (await db.execute(query.group_by(job.state))).mappings().all()
    except exc.SQLAlchemyError as error:
        raise DBError('Failed reading monitoring job statistics.') from error


async def get_recent_jobs(*, db: AsyncSession, limit: int, mode: str = 'priority', order: str = 'desc',
                          owner_id: UUID | None = None, worker_id: UUID | None = None,
                          engine_name: str | None = None, engine_version: str | None = None,
                          include_all_pending: bool = False, offset: int = 0, with_total: bool = False):
    owner, worker = aliased(model.Key), aliased(model.Key)
    job = model.Job
    if mode == 'priority':
        pending = job.state.in_([ProcessingState.PROCESSING, ProcessingState.QUEUED])
        ordering = [
            case((job.state == ProcessingState.PROCESSING, 0),
                 (job.state == ProcessingState.QUEUED, 1), else_=2),
            case((pending, job.created_date), else_=None).asc().nulls_last(),
            case((~pending, job.created_date), else_=None).desc().nulls_last(),
            job.id.asc(),
        ]
    else:
        timestamp = {'created_date': job.created_date, 'started_date': job.started_date,
                     'finished_date': job.finished_date, 'last_change': job.last_change}[mode]
        ordering = [(timestamp.asc() if order == 'asc' else timestamp.desc()).nulls_last(),
                    job.id.asc() if order == 'asc' else job.id.desc()]
    # Select only the table's small summary columns, never the potentially large
    # definitions/logs or the image records (which cleanup may have removed).
    query = select(
        model.Job.id, model.Job.state, model.Job.image_count, model.Job.progress,
        model.Job.previous_attempts, model.Job.created_date, model.Job.started_date,
        model.Job.finished_date, model.Job.last_change, owner.label.label('owner_label'),
        worker.label.label('worker_label'), model.Engine.name.label('engine_name'),
        model.Engine.version.label('engine_version'),
    ).join(owner, owner.id == model.Job.owner_key_id).outerjoin(
        worker, worker.id == model.Job.worker_key_id).outerjoin(
        model.Engine, model.Engine.id == model.Job.engine_id)
    if owner_id is not None:
        query = query.where(job.owner_key_id == owner_id)
    if worker_id is not None:
        query = query.where(job.worker_key_id == worker_id)
    if engine_name is not None:
        query = query.where(model.Engine.name == engine_name)
    if engine_version is not None:
        query = query.where(model.Engine.version == engine_version)
    count_query = select(func.count()).select_from(query.subquery()) if with_total else None
    if include_all_pending:
        # One statement gives complete pending counts and a bounded recent history.
        recent = query.with_only_columns(job.id).where(~pending).order_by(
            job.created_date.desc(), job.id.asc()).limit(limit).correlate(None)
        query = query.where(or_(pending, job.id.in_(recent))).order_by(*ordering)
    else:
        query = query.order_by(*ordering).offset(offset).limit(limit)
    try:
        async with db.begin():
            total = (await db.execute(count_query)).scalar_one() if with_total else None
            rows = (await db.execute(query)).mappings().all()
            return (rows, total) if with_total else rows
    except exc.SQLAlchemyError as error:
        raise DBError('Failed reading recent monitoring jobs.') from error


async def get_workers(*, db: AsyncSession):
    processing = select(model.Job.worker_key_id.label('key_id'), func.count().label('processing_jobs')).where(
        model.Job.state == ProcessingState.PROCESSING).group_by(model.Job.worker_key_id).subquery()
    query = select(model.Key.id, model.Key.label, model.Key.active.label('enabled'), model.Key.last_used,
                   func.coalesce(processing.c.processing_jobs, 0).label('processing_jobs')).outerjoin(
        processing, processing.c.key_id == model.Key.id).where(
        model.Key.role == KeyRole.WORKER).order_by(model.Key.label)
    try:
        async with db.begin():
            return (await db.execute(query)).mappings().all()
    except exc.SQLAlchemyError as error:
        raise DBError('Failed reading worker activity.') from error


async def get_principals(*, db: AsyncSession):
    try:
        async with db.begin():
            return (await db.execute(select(model.Key.id, model.Key.label, model.Key.role).order_by(
                model.Key.label))).mappings().all()
    except exc.SQLAlchemyError as error:
        raise DBError('Failed reading monitoring filter options.') from error


async def get_engines(*, db: AsyncSession):
    try:
        async with db.begin():
            return (await db.execute(select(model.Engine.name, model.Engine.version).order_by(
                model.Engine.name, model.Engine.version))).mappings().all()
    except exc.SQLAlchemyError as error:
        raise DBError('Failed reading monitoring engine options.') from error
