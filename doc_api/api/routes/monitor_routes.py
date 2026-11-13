"""Monitoring presentation and authenticated JSON endpoints."""
import asyncio
from datetime import datetime, timezone
from html import escape
from pathlib import Path
import socket
from time import monotonic
from typing import Annotated, Literal
from uuid import UUID

from fastapi import Depends, Query, Request
from fastapi.responses import FileResponse, HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.authentication import require_management_key
from doc_api.api.cruds import monitor_cruds
from doc_api.api.database import get_async_session
from doc_api.api.presentation import commit_badge_html, version_badge_html
from doc_api.api.routes import monitor_router, monitor_page_router
from doc_api.api.schemas import monitor as schemas, base_objects
from doc_api.api.schemas.responses import AppCode, DocAPIResponseOK, make_responses, validate_ok_response
from doc_api.config import config
from doc_api.db import model

STATIC_DIR = Path(__file__).parents[1] / 'static' / 'monitor'


def _responses(payload, detail):
    return {AppCode.MONITOR_RETRIEVED: {
        'status': 200, 'description': detail, 'detail': detail,
        'model': DocAPIResponseOK[payload], 'model_data': payload,
    }}


INFO_RESPONSES = _responses(schemas.MonitorInfo, 'Server information retrieved successfully.')
STATS_RESPONSES = _responses(schemas.MonitorStats, 'Job statistics retrieved successfully.')
JOBS_RESPONSES = _responses(schemas.MonitorProcessing, 'Processing jobs retrieved successfully.')
WORKERS_RESPONSES = _responses(list[schemas.MonitorWorker], 'Worker activity retrieved successfully.')
DATABASE_RESPONSES = _responses(schemas.MonitorDatabase, 'Database statistics retrieved successfully.')


def _ok(data, responses):
    return validate_ok_response(DocAPIResponseOK(
        status=200, code=AppCode.MONITOR_RETRIEVED,
        detail=responses[AppCode.MONITOR_RETRIEVED]['detail'], data=data))


@monitor_page_router.get('/monitor', include_in_schema=False)
async def monitor_page():
    # Only the login shell is public, like Swagger. All data requests below use
    # management-key authentication; the browser remembers the key until sign-out.
    page = await asyncio.to_thread((STATIC_DIR / 'index.html').read_text, encoding='utf-8')
    return HTMLResponse(page.replace('{{VERSION_BADGE}}', version_badge_html())
                        .replace('{{COMMIT_BADGE}}', commit_badge_html())
                        .replace('{{SERVER_NAME}}', escape(config.SERVER_NAME)),
                        headers={'Cache-Control': 'no-store'})


@monitor_page_router.get('/monitor/monitor.css', include_in_schema=False)
async def monitor_css():
    return FileResponse(STATIC_DIR / 'monitor.css', headers={'Cache-Control': 'no-store'})


@monitor_page_router.get('/monitor/monitor.js', include_in_schema=False)
async def monitor_js():
    return FileResponse(STATIC_DIR / 'monitor.js', headers={'Cache-Control': 'no-store'})


def _host_addresses():
    try:
        return sorted({item[4][0] for item in socket.getaddrinfo(socket.gethostname(), None)})
    except OSError:
        return []


@monitor_router.get('/info', summary='Get Server Information', tags=['Monitor'],
                    openapi_extra={'x-order': 400},
                    description='Read application identity, uptime and database connectivity. Hostname and IP '
                                'addresses use optional monitoring-only overrides, otherwise process detection.',
                    response_model=DocAPIResponseOK[schemas.MonitorInfo], responses=make_responses(INFO_RESPONSES))
async def get_info(request: Request, key: model.Key = Depends(require_management_key),
                   db: AsyncSession = Depends(get_async_session)):
    data = schemas.MonitorInfo(
        server_name=config.SERVER_NAME, hostname=config.MONITOR_HOSTNAME or socket.gethostname(),
        ip_addresses=config.MONITOR_IP_ADDRESSES or await asyncio.to_thread(_host_addresses), version=config.APP_VERSION,
        commit=config.APP_GIT_COMMIT, started_at=request.app.state.monitor_started_at,
        uptime_seconds=max(0, monotonic() - request.app.state.monitor_started_monotonic),
        server_time=datetime.now(timezone.utc), database_connected=await monitor_cruds.database_connected(db=db),
        refresh_interval_seconds=config.MONITOR_REFRESH_INTERVAL_SECONDS,
        worker_offline_after_seconds=config.MONITOR_WORKER_OFFLINE_AFTER_SECONDS)
    return _ok(data, INFO_RESPONSES)


@monitor_router.get('/database', summary='Get Database Statistics', tags=['Monitor'],
                    openapi_extra={'x-order': 405},
                    description='Read PostgreSQL size, client connection states, longest active query duration, '
                                'lock waits and deadlocks for the application database. Excludes this monitoring '
                                'connection. Deadlocks are cumulative since stats_reset. Activity visibility '
                                'depends on the database role privileges and PostgreSQL tracking settings.',
                    response_model=DocAPIResponseOK[schemas.MonitorDatabase],
                    responses=make_responses(DATABASE_RESPONSES))
async def get_database(key: model.Key = Depends(require_management_key),
                       db: AsyncSession = Depends(get_async_session)):
    data = await monitor_cruds.get_database_stats(db=db)
    return _ok(schemas.MonitorDatabase(**data), DATABASE_RESPONSES)


@monitor_router.get('/jobs/statistics', summary='Get Job Statistics', tags=['Monitor'],
                    openapi_extra={'x-order': 401},
                    description='Aggregate every job matching the job-list filters except result availability, before pagination. '
                                'Overall totals are unlimited. Grouped owners, workers, engines and engine versions are ranked separately. '
                                'top defaults to 10 per group type; top_by ranks descending across all matching states, '
                                'with identity as a deterministic tie-breaker. Null identities represent unassigned jobs. '
                                'Workers refer to the latest assignment. Released leases do not count as attempts. '
                                'Images count per attempt. Runtime spans first start to finish, including retry waits. '
                                'Individual attempt times, runtimes and historical worker attribution are unavailable.',
                    response_model=DocAPIResponseOK[schemas.MonitorStats], responses=make_responses(STATS_RESPONSES))
async def get_stats(top: Annotated[int, Query(ge=1, description='Maximum entries per category; overall totals are unaffected.')] = 10,
                    top_by: Literal['job_count', 'image_count', 'runtime_seconds', 'attempt_count', 'attempt_image_count'] = 'job_count',
                    owner_label: str | None = None, worker_label: str | None = None,
                    state: list[base_objects.ProcessingState] | None = Query(None, description="Filter by one or more states; repeat state for multiple values."),
                    engine_name: str | None = None, engine_version: str | None = None,
                    from_created_date: base_objects.UTCDateTime | None = None,
                    from_started_date: base_objects.UTCDateTime | None = None,
                    from_finished_date: base_objects.UTCDateTime | None = None,
                    from_last_change: base_objects.UTCDateTime | None = None,
                    key: model.Key = Depends(require_management_key), db: AsyncSession = Depends(get_async_session)):
    rows = await monitor_cruds.get_stats(
        db=db, top=top, top_by=top_by, owner_label=owner_label, worker_label=worker_label, state=state,
        engine_name=engine_name, engine_version=engine_version,
        from_created_date=from_created_date, from_started_date=from_started_date,
        from_finished_date=from_finished_date, from_last_change=from_last_change)

    def totals(row):
        values = {}
        for state in ('done', 'failed'):
            count = row[state + '_job_count']
            values[state] = schemas.MonitorTotals(
                job_count=count, image_count=row[state + '_image_count'],
                runtime_seconds=row[state + '_runtime_seconds'] if count else 0)
        values['attempts'] = schemas.MonitorTotals(
            job_count=row['attempt_count'], image_count=row['attempt_image_count'], runtime_seconds=None)
        return values

    groups = {'owners': [], 'workers': [], 'engines': [], 'engine_versions': []}
    for row in rows:
        values = totals(row)
        if row['category'] == 'overall':
            overall = values
            continue
        values['total'] = schemas.MonitorTotals(
            job_count=row['job_count'], image_count=row['image_count'], runtime_seconds=row['runtime_seconds'])
        if row['category'] in ('owners', 'workers'):
            field, prefix = ('owners', 'owner') if row['category'] == 'owners' else ('workers', 'worker')
            groups[field].append(schemas.MonitorKeyStats(
                id=row[prefix + '_key_id'], label=row[prefix + '_label'], **values))
        elif row['category'] == 'engines':
            groups['engines'].append(schemas.MonitorEngineStats(name=row['engine_name'], **values))
        else:
            groups['engine_versions'].append(schemas.MonitorEngineVersionStats(
                name=row['engine_name'], version=row['engine_version'], **values))
    return _ok(schemas.MonitorStats(**overall, **groups), STATS_RESPONSES)


@monitor_router.get('/jobs/processing', summary='Get Processing Activity', tags=['Monitor'],
                    openapi_extra={'x-order': 407},
                    description='List PROCESSING and QUEUED jobs oldest first within each group, followed by NEW and '
                                'recent DONE, FAILED and ERROR jobs newest creation first. '
                                'limit and offset apply to the whole list; omit limit for all matches. '
                                'X-Total-Count and X-Has-More describe pagination. Body totals count all matching '
                                'PROCESSING, QUEUED and NEW jobs and images before pagination. '
                                'Optional state restricts results to PROCESSING, QUEUED or NEW. '
                                'finished_within_seconds defaults to 60; zero disables finished-job retention. '
                                'Owner, latest worker, engine and version filters apply before totals and pagination.',
                    response_model=DocAPIResponseOK[schemas.MonitorProcessing], responses=make_responses(JOBS_RESPONSES))
async def get_recent_jobs(limit: Annotated[int | None, Query(ge=1)] = None,
                          offset: Annotated[int, Query(ge=0)] = 0,
                          finished_within_seconds: Annotated[int, Query(ge=0, le=86400)] = 60,
                          state: Literal['processing', 'queued', 'new'] | None = None,
                          owner_id: UUID | None = None, worker_id: UUID | None = None,
                          engine_name: str | None = None, engine_version: str | None = None,
                          key: model.Key = Depends(require_management_key),
                          db: AsyncSession = Depends(get_async_session)):
    rows, total, totals, image_totals = await monitor_cruds.get_recent_jobs(db=db, limit=limit, offset=offset,
                                              finished_within_seconds=finished_within_seconds, state=state,
                                              owner_id=owner_id, worker_id=worker_id,
                                              engine_name=engine_name, engine_version=engine_version)
    response = _ok(schemas.MonitorProcessing(jobs=[schemas.MonitorJob(**row) for row in rows],
                                            totals=schemas.MonitorProcessingTotals(**totals),
                                            image_totals=schemas.MonitorProcessingTotals(**image_totals)), JOBS_RESPONSES)
    response.headers['X-Total-Count'] = str(total)
    response.headers['X-Has-More'] = str(offset + len(rows) < total).lower()
    return response


@monitor_router.get('/workers', summary='Get Worker Activity', tags=['Monitor'],
                    openapi_extra={'x-order': 403},
                    description='Read activity for enabled worker keys, ordered by last_used descending '
                                'with never-seen workers last. PROCESSING assignments always mean active. '
                                'Otherwise recent last_used means idle and older or missing last_used means off. '
                                'Multiple processes sharing one key appear as one worker.',
                    response_model=DocAPIResponseOK[list[schemas.MonitorWorker]], responses=make_responses(WORKERS_RESPONSES))
async def get_workers(key: model.Key = Depends(require_management_key), db: AsyncSession = Depends(get_async_session)):
    rows = await monitor_cruds.get_workers(db=db)
    now = datetime.now(timezone.utc)
    workers = []
    for row in rows:
        if row['processing_jobs']:
            status = 'active'
        elif row['last_used'] is None:
            status = 'off'
        elif (now - row['last_used']).total_seconds() <= config.MONITOR_WORKER_OFFLINE_AFTER_SECONDS:
            status = 'idle'
        else:
            status = 'off'
        workers.append(schemas.MonitorWorker(**row, status=status))
    return _ok(workers, WORKERS_RESPONSES)
