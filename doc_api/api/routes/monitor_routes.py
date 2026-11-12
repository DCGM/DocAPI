"""Monitoring presentation and authenticated JSON endpoints."""
import asyncio
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path
import socket
from time import monotonic
from typing import Annotated
from uuid import UUID

from fastapi import Depends, Query, Request
from fastapi.responses import FileResponse, HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.authentication import require_management_key
from doc_api.api.cruds import monitor_cruds
from doc_api.api.database import get_async_session
from doc_api.api.presentation import commit_badge_html, version_badge_html
from doc_api.api.routes import monitor_router, monitor_page_router
from doc_api.api.schemas import monitor as schemas
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
JOBS_RESPONSES = _responses(list[schemas.MonitorJob], 'Recent jobs retrieved successfully.')
WORKERS_RESPONSES = _responses(list[schemas.MonitorWorker], 'Worker activity retrieved successfully.')
PRINCIPALS_RESPONSES = _responses(list[schemas.MonitorPrincipal], 'Monitoring filter options retrieved successfully.')
DATABASE_RESPONSES = _responses(schemas.MonitorDatabase, 'Database statistics retrieved successfully.')
ENGINES_RESPONSES = _responses(list[schemas.MonitorEngine], 'Engine filter options retrieved successfully.')


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


@monitor_router.get('/stats', summary='Get Job Statistics', tags=['Monitor'],
                    openapi_extra={'x-order': 401},
                    description='Read DONE/FAILED job totals and total attempts regardless of outcome, including '
                                'in-progress attempts. Released leases do not count. Windows use finished_date for '
                                'DONE/FAILED/ERROR and last_change for other states; 1m means 30 days. '
                                'All attempts of a matching job count in that window. Images count per attempt. '
                                'Runtime spans first start to finish, including retry waits. '
                                'Individual attempt times, runtimes and historical worker attribution are unavailable.',
                    response_model=DocAPIResponseOK[schemas.MonitorStats], responses=make_responses(STATS_RESPONSES))
async def get_stats(period: schemas.MonitorPeriod = '24h', owner_id: UUID | None = None,
                    worker_id: UUID | None = None, engine_name: str | None = None,
                    engine_version: str | None = None, key: model.Key = Depends(require_management_key),
                    db: AsyncSession = Depends(get_async_session)):
    days = {'24h': 1, '7d': 7, '1m': 30}
    since = None if period == 'all' else datetime.now(timezone.utc) - timedelta(days=days[period])
    rows = await monitor_cruds.get_stats(db=db, since=since, owner_id=owner_id, worker_id=worker_id,
                                         engine_name=engine_name, engine_version=engine_version)
    totals = {state: schemas.MonitorTotals(job_count=0, image_count=0, runtime_seconds=0 if state != 'attempts' else None)
              for state in ('done', 'failed', 'attempts')}
    for row in rows:
        state = row['state'].value
        if state in ('done', 'failed'):
            totals[state] = schemas.MonitorTotals(job_count=row['job_count'], image_count=row['image_count'],
                                                  runtime_seconds=row['runtime_seconds'])
        totals['attempts'].job_count += row['attempt_count']
        totals['attempts'].image_count += row['attempt_image_count']
    return _ok(schemas.MonitorStats(period=period, **totals), STATS_RESPONSES)


@monitor_router.get('/jobs', summary='Get Recent Jobs', tags=['Monitor'],
                    openapi_extra={'x-order': 402},
                    description='Read jobs with current state and progress. Priority mode shows PROCESSING first, '
                                'then QUEUED (both oldest creation first), then other jobs newest creation first. '
                                'Order applies only to timestamp modes; missing timestamps always appear last. '
                                'Optional owner, latest assigned worker, engine and version filters apply before pagination. '
                                'Offset skips matching jobs after sorting. X-Total-Count gives the filtered total; X-Has-More indicates another page.',
                    response_model=DocAPIResponseOK[list[schemas.MonitorJob]], responses=make_responses(JOBS_RESPONSES))
async def get_jobs(limit: Annotated[int, Query(ge=1, le=20)] = 10,
                   mode: schemas.MonitorJobSort = 'priority', order: schemas.MonitorSortOrder = 'desc',
                   offset: Annotated[int, Query(ge=0)] = 0,
                   owner_id: UUID | None = None, worker_id: UUID | None = None,
                   engine_name: str | None = None, engine_version: str | None = None,
                   key: model.Key = Depends(require_management_key), db: AsyncSession = Depends(get_async_session)):
    rows, total = await monitor_cruds.get_recent_jobs(db=db, limit=limit, offset=offset, mode=mode, order=order,
                                              with_total=True,
                                              owner_id=owner_id, worker_id=worker_id,
                                              engine_name=engine_name, engine_version=engine_version)
    response = _ok([schemas.MonitorJob(**row) for row in rows], JOBS_RESPONSES)
    response.headers['X-Has-More'] = str(offset + limit < total).lower()
    response.headers['X-Total-Count'] = str(total)
    return response


@monitor_router.get('/recent-jobs', summary='Get Recent Job Activity', tags=['Monitor'],
                    openapi_extra={'x-order': 407},
                    description='Read every PROCESSING and QUEUED job, oldest creation first within each group, '
                                'followed by up to limit other jobs, newest creation first. '
                                'The limit applies only to other jobs; pending jobs are never truncated. '
                                'Optional owner, latest assigned worker, engine and version filters apply to all groups.',
                    response_model=DocAPIResponseOK[list[schemas.MonitorJob]], responses=make_responses(JOBS_RESPONSES))
async def get_recent_jobs(limit: Annotated[int, Query(ge=1, le=20)] = 10,
                          owner_id: UUID | None = None, worker_id: UUID | None = None,
                          engine_name: str | None = None, engine_version: str | None = None,
                          key: model.Key = Depends(require_management_key),
                          db: AsyncSession = Depends(get_async_session)):
    rows = await monitor_cruds.get_recent_jobs(db=db, limit=limit, include_all_pending=True,
                                              owner_id=owner_id, worker_id=worker_id,
                                              engine_name=engine_name, engine_version=engine_version)
    return _ok([schemas.MonitorJob(**row) for row in rows], JOBS_RESPONSES)


@monitor_router.get('/workers', summary='Get Worker Activity', tags=['Monitor'],
                    openapi_extra={'x-order': 403},
                    description='Read activity per worker key. PROCESSING assignments always mean active. '
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


@monitor_router.get('/principals', summary='Get Monitoring Filter Options', tags=['Monitor'],
                    openapi_extra={'x-order': 404},
                    description='Read key IDs, labels and roles for owner and worker statistics filters.',
                    response_model=DocAPIResponseOK[list[schemas.MonitorPrincipal]], responses=make_responses(PRINCIPALS_RESPONSES))
async def get_principals(key: model.Key = Depends(require_management_key), db: AsyncSession = Depends(get_async_session)):
    rows = await monitor_cruds.get_principals(db=db)
    return _ok([schemas.MonitorPrincipal(**row) for row in rows], PRINCIPALS_RESPONSES)


@monitor_router.get('/engines', summary='Get Monitoring Engine Options', tags=['Monitor'],
                    openapi_extra={'x-order': 406},
                    description='Read engine names and versions, including inactive engines, for statistics filters.',
                    response_model=DocAPIResponseOK[list[schemas.MonitorEngine]], responses=make_responses(ENGINES_RESPONSES))
async def get_engines(key: model.Key = Depends(require_management_key), db: AsyncSession = Depends(get_async_session)):
    rows = await monitor_cruds.get_engines(db=db)
    return _ok([schemas.MonitorEngine(**row) for row in rows], ENGINES_RESPONSES)
