"""Monitoring routes and PostgreSQL aggregates on disposable test databases."""
import asyncio
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio
from pydantic import TypeAdapter

from doc_api.api.main import app
from doc_api.api.cruds import worker_cruds
from doc_api.api.routes import monitor_routes
from doc_api.api.schemas.base_objects import KeyRole, ProcessingState
from doc_api.config import Config, config
from doc_api.db import model


@pytest.fixture(params=['admin', 'manager'])
def monitor_headers(request, admin_headers, manager_headers):
    return admin_headers if request.param == 'admin' else manager_headers


@pytest_asyncio.fixture
async def monitor_jobs(job_factory, db_sessions):
    now = datetime.now(timezone.utc)
    definitions = [
        (ProcessingState.DONE, 12, 2, now, 600),
        (ProcessingState.FAILED, 5, 1, now - timedelta(days=2), 120),
        (ProcessingState.ERROR, 3, 0, now, 30),
        (ProcessingState.PROCESSING, 4, 0, None, 0),
        (ProcessingState.NEW, 9, None, None, 0),
    ]
    ids = []
    for index, (state, images, attempts, finished, runtime) in enumerate(definitions):
        job_id = await job_factory(state)
        ids.append(job_id)
        async with db_sessions.begin() as db:
            job = await db.get(model.Job, job_id)
            job.image_count = images
            job.previous_attempts = attempts
            job.created_date = now - timedelta(minutes=index + 1)
            job.finished_date = finished
            job.started_date = finished - timedelta(seconds=runtime) if finished else None
            job.definition = {'secret': 'never-return-this-value'}
            job.final_log = {'0': 'never-return-this-value'}
    return ids


@pytest.mark.parametrize('endpoint', ['info', 'stats', 'jobs', 'workers', 'principals', 'database', 'engines', 'recent-jobs'])
@pytest.mark.asyncio
async def test_get_monitor_200(client, monitor_headers, endpoint):
    response = await client.get('/v1/monitor/' + endpoint, headers=monitor_headers)
    assert response.status_code == 200, response.text
    assert response.json()['code'] == 'MONITOR_RETRIEVED'
    assert response.json()['data'] is not None


@pytest.mark.asyncio
async def test_get_monitor_info_200(client, monitor_headers, monkeypatch):
    monkeypatch.setattr(config, 'MONITOR_HOSTNAME', '')
    monkeypatch.setattr(config, 'MONITOR_IP_ADDRESSES', [])
    monkeypatch.setattr(monitor_routes, '_host_addresses', lambda: ['192.0.2.1'])
    response = await client.get('/v1/monitor/info', headers=monitor_headers)
    data = response.json()['data']
    assert data['ip_addresses'] == ['192.0.2.1']
    assert data['database_connected'] is True
    assert data['uptime_seconds'] >= 0
    assert TypeAdapter(datetime).validate_python(data['started_at']).tzinfo is not None
    assert data['worker_offline_after_seconds'] == config.MONITOR_WORKER_OFFLINE_AFTER_SECONDS


@pytest.mark.parametrize('hostname,addresses', [
    ('', []), ('outer.example.org', []), ('', ['192.0.2.10', '2001:db8::10']),
    ('outer.example.org', ['192.0.2.10', '2001:db8::10']),
])
@pytest.mark.asyncio
async def test_get_monitor_info_200_identity_overrides(client, admin_headers, monkeypatch, hostname, addresses):
    monkeypatch.setattr(config, 'MONITOR_HOSTNAME', hostname)
    monkeypatch.setattr(config, 'MONITOR_IP_ADDRESSES', addresses)
    monkeypatch.setattr(monitor_routes.socket, 'gethostname', lambda: 'process-hostname')

    def detect_addresses():
        assert not addresses, 'Explicit addresses must bypass detection'
        return ['172.18.0.2']

    monkeypatch.setattr(monitor_routes, '_host_addresses', detect_addresses)
    response = await client.get('/v1/monitor/info', headers=admin_headers)
    assert response.status_code == 200, response.text
    data = response.json()['data']
    assert data['hostname'] == (hostname or 'process-hostname')
    assert data['ip_addresses'] == (addresses or ['172.18.0.2'])


@pytest.mark.asyncio
async def test_get_monitor_database_200_idle(client, monitor_headers, isolated_database):
    response = await client.get('/v1/monitor/database', headers=monitor_headers)
    assert response.status_code == 200, response.text
    data = response.json()['data']
    assert data['database_name'] == isolated_database.name
    assert data['size_bytes'] > 0
    # With no workload, the monitoring request must not count itself as active.
    assert data['active_connections'] == 0
    assert data['connections'] == data['idle_connections']
    assert data['idle_in_transaction_connections'] == 0
    assert data['lock_waiting_connections'] == 0
    assert data['longest_active_query_seconds'] is None
    assert data['deadlocks'] == 0
    if data['stats_reset'] is not None:
        assert TypeAdapter(datetime).validate_python(data['stats_reset']).tzinfo is not None


@pytest.mark.asyncio
async def test_get_monitor_database_200_lock_wait(client, monitor_headers, isolated_database):
    url = isolated_database.engine.url
    kwargs = dict(host=url.host, port=url.port or 5432, user=url.username,
                  password=url.password, database=isolated_database.name)
    blocker = await asyncpg.connect(**kwargs)
    waiter = await asyncpg.connect(**kwargs)
    waiting = None
    try:
        await blocker.execute('BEGIN')
        await blocker.execute('SELECT pg_advisory_xact_lock(819273)')
        waiting = asyncio.create_task(waiter.execute('SELECT pg_advisory_xact_lock(819273)'))

        async def wait_for_lock():
            while True:
                await blocker.execute('SELECT pg_stat_clear_snapshot()')
                if await blocker.fetchval("SELECT wait_event_type = 'Lock' FROM pg_stat_activity WHERE pid = $1",
                                          waiter.get_server_pid()):
                    return
                await asyncio.sleep(0.01)

        await asyncio.wait_for(wait_for_lock(), timeout=5)
        response = await client.get('/v1/monitor/database', headers=monitor_headers)
        assert response.status_code == 200, response.text
        data = response.json()['data']
        assert data['database_name'] == isolated_database.name
        assert data['active_connections'] == 1
        assert data['idle_in_transaction_connections'] == 1
        assert data['lock_waiting_connections'] == 1
        assert data['connections'] == data['idle_connections'] + 2
        assert data['longest_active_query_seconds'] >= 0
    finally:
        await blocker.close()
        try:
            if waiting is not None:
                await asyncio.wait_for(waiting, timeout=5)
        finally:
            await waiter.close()


@pytest.mark.parametrize('period,failed,attempts,attempt_images', [
    ('24h', 0, 5, 43), ('7d', 1, 7, 53), ('1m', 1, 7, 53), ('all', 1, 7, 53),
])
@pytest.mark.asyncio
async def test_get_monitor_stats_200(client, monitor_headers, monitor_jobs, period, failed, attempts, attempt_images):
    response = await client.get('/v1/monitor/stats', params={'period': period}, headers=monitor_headers)
    assert response.status_code == 200, response.text
    stats = response.json()['data']
    assert stats['done'] == {'job_count': 1, 'image_count': 12, 'runtime_seconds': 600}
    assert stats['failed']['job_count'] == failed
    assert stats['failed']['image_count'] == (5 if failed else 0)
    assert stats['failed']['runtime_seconds'] == (120 if failed else 0)
    assert stats['attempts'] == {'job_count': attempts, 'image_count': attempt_images, 'runtime_seconds': None}


@pytest.mark.parametrize('field', ['owner_id', 'worker_id'])
@pytest.mark.asyncio
async def test_get_monitor_stats_200_filters(client, monitor_headers, monitor_jobs, db_sessions, field):
    async with db_sessions() as db:
        job = await db.get(model.Job, monitor_jobs[0])
        key_id = job.owner_key_id if field == 'owner_id' else job.worker_key_id
    matched = await client.get('/v1/monitor/stats', params={field: str(key_id)}, headers=monitor_headers)
    assert matched.json()['data']['done']['job_count'] == 1
    empty = await client.get('/v1/monitor/stats', params={field: str(UUID(int=0))}, headers=monitor_headers)
    assert empty.json()['data']['done']['job_count'] == 0
    assert empty.json()['data']['attempts']['job_count'] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('previous_attempts', [0, 2])
async def test_monitor_attempts_exclude_released_leases(client, monitor_headers, job_factory,
                                                      worker_key, db_sessions, previous_attempts):
    job_id = await job_factory(ProcessingState.PROCESSING)
    async with db_sessions.begin() as db:
        (await db.get(model.Job, job_id)).previous_attempts = previous_attempts
    async def attempts():
        response = await client.get('/v1/monitor/stats?period=all', headers=monitor_headers)
        assert response.status_code == 200, response.text
        return response.json()['data']['attempts']['job_count']
    assert await attempts() == previous_attempts + 1
    async with db_sessions() as db:
        await worker_cruds.release_job_lease(db=db, job_id=job_id, expected_worker_key_id=worker_key.id,
                                           expected_state=ProcessingState.PROCESSING)
    assert await attempts() == previous_attempts
    async with db_sessions() as db:
        job, _, _, _ = await worker_cruds.lease_job_to_worker(db=db, worker_key_id=worker_key.id)
        assert job.id == job_id
    assert await attempts() == previous_attempts + 1


@pytest.mark.asyncio
async def test_get_monitor_engine_filters(client, monitor_headers, engine_factory, job_factory, db_sessions):
    engines = [await engine_factory(name='Alpha', version='1'),
               await engine_factory(name='Alpha', version='2', active=False),
               await engine_factory(name='Beta', version='1')]
    for index, engine in enumerate(engines):
        job_id = await job_factory(ProcessingState.DONE)
        async with db_sessions.begin() as db:
            job = await db.get(model.Job, job_id)
            job.engine_id = engine.id
            job.image_count = index + 1
            job.previous_attempts = index
    # Unassigned engines must still count when both filters are All.
    await job_factory(ProcessingState.DONE)
    response = await client.get('/v1/monitor/engines', headers=monitor_headers)
    assert response.status_code == 200, response.text
    assert response.json()['data'] == [
        {'name': 'Alpha', 'version': '1'}, {'name': 'Alpha', 'version': '2'},
        {'name': 'Beta', 'version': '1'}]
    for filters, count, images, attempts in [
        ({}, 4, None, 7),
        ({'engine_name': 'Alpha'}, 2, 3, 3),
        ({'engine_name': 'Alpha', 'engine_version': '2'}, 1, 2, 2),
        ({'engine_version': '1'}, 2, 4, 4),
        ({'engine_name': 'Beta', 'engine_version': '2'}, 0, 0, 0),
        ({'engine_name': 'Missing'}, 0, 0, 0),
    ]:
        response = await client.get('/v1/monitor/stats', params={'period': 'all', **filters}, headers=monitor_headers)
        assert response.status_code == 200, response.text
        data = response.json()['data']
        assert data['done']['job_count'] == count
        if images is not None:
            assert data['done']['image_count'] == images
        assert data['attempts']['job_count'] == attempts
        for endpoint in ['jobs', 'recent-jobs']:
            jobs_response = await client.get('/v1/monitor/' + endpoint, params=filters, headers=monitor_headers)
            assert jobs_response.status_code == 200, jobs_response.text
            rows = jobs_response.json()['data']
            assert len(rows) == count
            if endpoint == 'jobs':
                assert int(jobs_response.headers['X-Total-Count']) == count
            for field in ['engine_name', 'engine_version']:
                if field in filters:
                    assert all(row[field] == filters[field] for row in rows)


@pytest.mark.asyncio
async def test_get_monitor_jobs_200_limit_and_refresh(client, monitor_headers, monitor_jobs, db_sessions):
    response = await client.get('/v1/monitor/jobs?limit=2&mode=created_date', headers=monitor_headers)
    rows = response.json()['data']
    assert [row['id'] for row in rows] == [str(value) for value in monitor_jobs[:2]]
    assert 'never-return-this-value' not in response.text
    assert rows[0]['image_count'] == 12  # Preserved even with no image rows.
    async with db_sessions.begin() as db:
        (await db.get(model.Job, monitor_jobs[0])).state = ProcessingState.FAILED
    refreshed = await client.get('/v1/monitor/jobs?limit=2&mode=created_date', headers=monitor_headers)
    assert refreshed.json()['data'][0]['state'] == 'failed'


@pytest.mark.asyncio
async def test_get_monitor_jobs_priority(client, admin_headers, job_factory, db_sessions):
    now = datetime.now(timezone.utc)
    states = [ProcessingState.DONE, ProcessingState.QUEUED, ProcessingState.PROCESSING,
              ProcessingState.NEW, ProcessingState.QUEUED, ProcessingState.PROCESSING,
              ProcessingState.FAILED, ProcessingState.ERROR, ProcessingState.CANCELLED]
    ids = []
    for index, state in enumerate(states):
        job_id = await job_factory(state)
        ids.append(str(job_id))
        async with db_sessions.begin() as db:
            (await db.get(model.Job, job_id)).created_date = now - timedelta(minutes=index)
    expected = [ids[i] for i in [5, 2, 4, 1, 0, 3, 6, 7, 8]]
    for query in ['', '?order=asc', '?order=desc']:
        response = await client.get('/v1/monitor/jobs' + query, headers=admin_headers)
        assert response.status_code == 200, response.text
        assert [row['id'] for row in response.json()['data']] == expected
    limited = await client.get('/v1/monitor/jobs?limit=3', headers=admin_headers)
    assert [row['id'] for row in limited.json()['data']] == expected[:3]


@pytest.mark.asyncio
async def test_jobs_pagination(client, monitor_headers, admin_headers, job_factory, db_sessions, reader_owner):
    identity = await client.get('/v1/me', headers=admin_headers)
    admin_id = UUID(identity.json()['data']['id'])
    now = datetime.now(timezone.utc)
    ids = []
    for index in range(7):
        job_id = await job_factory(ProcessingState.DONE,
                                   owner_key_id=admin_id if index % 2 else reader_owner.id)
        if not index % 2:
            ids.append(str(job_id))
        async with db_sessions.begin() as db:
            (await db.get(model.Job, job_id)).created_date = now - timedelta(minutes=index)
    for order in ['asc', 'desc']:
        expected = ids if order == 'desc' else ids[::-1]
        for offset in [0, 2, 4, 100]:
            response = await client.get('/v1/monitor/jobs', headers=monitor_headers, params={
                'limit': 2, 'offset': offset, 'mode': 'created_date', 'order': order,
                'owner_id': str(reader_owner.id)})
            assert response.status_code == 200, response.text
            assert [row['id'] for row in response.json()['data']] == expected[offset:offset + 2]
            assert response.headers['X-Has-More'] == str(offset + 2 < len(expected)).lower()
            assert int(response.headers['X-Total-Count']) == len(expected)


@pytest.mark.asyncio
async def test_recent_activity_includes_all_pending(client, monitor_headers, job_factory, db_sessions,
                                                    admin_headers, reader_owner):
    now = datetime.now(timezone.utc)
    states = [ProcessingState.DONE, ProcessingState.QUEUED, ProcessingState.PROCESSING,
              ProcessingState.NEW, ProcessingState.QUEUED, ProcessingState.PROCESSING,
              ProcessingState.FAILED, ProcessingState.ERROR, ProcessingState.CANCELLED]
    ids = []
    for index, state in enumerate(states):
        job_id = await job_factory(state)
        ids.append(str(job_id))
        async with db_sessions.begin() as db:
            (await db.get(model.Job, job_id)).created_date = now - timedelta(minutes=index)
    for limit in [1, 2, 20]:
        response = await client.get(f'/v1/monitor/recent-jobs?limit={limit}', headers=monitor_headers)
        assert response.status_code == 200, response.text
        rows = response.json()['data']
        history = [ids[i] for i in [0, 3, 6, 7, 8]][:limit]
        assert [row['id'] for row in rows] == [ids[i] for i in [5, 2, 4, 1]] + history
        assert sum(row['state'] == 'processing' for row in rows) == 2
        assert sum(row['state'] == 'queued' for row in rows) == 2


    identity = await client.get('/v1/me', headers=admin_headers)
    async with db_sessions.begin() as db:
        for index in [0, 2, 4]:
            (await db.get(model.Job, UUID(ids[index]))).owner_key_id = UUID(identity.json()['data']['id'])
    response = await client.get('/v1/monitor/recent-jobs', headers=monitor_headers,
                                params={'limit': 1, 'owner_id': str(reader_owner.id)})
    assert response.status_code == 200, response.text
    assert [row['id'] for row in response.json()['data']] == [ids[i] for i in [5, 1, 3]]


@pytest.mark.parametrize('mode', ['created_date', 'started_date', 'finished_date', 'last_change'])
@pytest.mark.asyncio
async def test_get_monitor_jobs_timestamp_order(client, admin_headers, job_factory, db_sessions, mode):
    now = datetime.now(timezone.utc)
    ids = []
    nullable = mode in ('started_date', 'finished_date')
    for index in range(3):
        job_id = await job_factory(ProcessingState.DONE)
        ids.append(str(job_id))
        async with db_sessions.begin() as db:
            job = await db.get(model.Job, job_id)
            setattr(job, mode, None if nullable and index == 2 else now - timedelta(minutes=index))
    for order in ['desc', 'asc']:
        response = await client.get(f'/v1/monitor/jobs?mode={mode}&order={order}', headers=admin_headers)
        assert response.status_code == 200, response.text
        expected = ids if order == 'desc' else ([ids[1], ids[0], ids[2]] if nullable else ids[::-1])
        assert [row['id'] for row in response.json()['data']] == expected


@pytest.mark.asyncio
@pytest.mark.parametrize('endpoint', ['jobs', 'recent-jobs'])
async def test_get_monitor_jobs_owner_worker_filters(endpoint, client, monitor_headers, admin_headers,
                                                   reader_owner, worker_key, job_factory, db_sessions):
    identity = await client.get('/v1/me', headers=admin_headers)
    admin_id = identity.json()['data']['id']
    older = await job_factory(ProcessingState.DONE)
    newer = await job_factory(ProcessingState.DONE, owner_key_id=UUID(admin_id))
    now = datetime.now(timezone.utc)
    async with db_sessions.begin() as db:
        (await db.get(model.Job, older)).created_date = now - timedelta(days=1)
        job = await db.get(model.Job, newer)
        job.created_date = now
        job.worker_key_id = None
    for filters, expected in [
        ({}, [newer]),
        ({'owner_id': str(reader_owner.id)}, [older]),
        ({'worker_id': str(worker_key.id)}, [older]),
        ({'owner_id': str(reader_owner.id), 'worker_id': str(worker_key.id)}, [older]),
        ({'owner_id': admin_id, 'worker_id': str(worker_key.id)}, []),
        ({'owner_id': str(UUID(int=0))}, []),
        ({'worker_id': str(UUID(int=0))}, []),
    ]:
        response = await client.get('/v1/monitor/' + endpoint, params={'limit': 1, **filters}, headers=monitor_headers)
        assert response.status_code == 200, response.text
        assert [row['id'] for row in response.json()['data']] == [str(value) for value in expected]


@pytest.mark.asyncio
async def test_get_monitor_workers_200_activity(client, monitor_headers, worker_key, db_sessions, monkeypatch):
    now = datetime.now(timezone.utc)
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now
    monkeypatch.setattr(monitor_routes, 'datetime', Clock)
    monkeypatch.setattr(config, 'MONITOR_WORKER_OFFLINE_AFTER_SECONDS', 60)
    async def status(last_used):
        async with db_sessions.begin() as db:
            (await db.get(model.Key, worker_key.id)).last_used = last_used
        response = await client.get('/v1/monitor/workers', headers=monitor_headers)
        assert response.status_code == 200, response.text
        return next(row for row in response.json()['data'] if row['id'] == str(worker_key.id))
    assert (await status(None))['status'] == 'off'
    assert (await status(now - timedelta(seconds=61)))['status'] == 'off'
    assert (await status(now - timedelta(seconds=60)))['status'] == 'idle'
    assert (await status(now))['status'] == 'idle'


@pytest.mark.asyncio
@pytest.mark.parametrize('role', [KeyRole.ADMIN, KeyRole.MANAGER])
async def test_get_monitor_workers_excludes_management_assignments(client, monitor_headers, monitor_jobs,
                                                                 worker_key, db_sessions, role):
    async with db_sessions.begin() as db:
        (await db.get(model.Key, worker_key.id)).role = role
    response = await client.get('/v1/monitor/workers', headers=monitor_headers)
    assert response.status_code == 200, response.text
    assert str(worker_key.id) not in {row['id'] for row in response.json()['data']}


@pytest.mark.asyncio
async def test_get_monitor_workers_200_processing_always_active(client, monitor_headers, monitor_jobs,
                                                               worker_key, db_sessions):
    async with db_sessions.begin() as db:
        key = await db.get(model.Key, worker_key.id)
        key.last_used = None
        key.active = False
    response = await client.get('/v1/monitor/workers', headers=monitor_headers)
    worker = next(row for row in response.json()['data'] if row['id'] == str(worker_key.id))
    assert worker['status'] == 'active'
    assert worker['processing_jobs'] == 1
    assert worker['enabled'] is False
    assert worker['last_used'] is None
    assert not {'kid', 'key_hash', 'salt'} & worker.keys()


@pytest.mark.parametrize('path', [
    '/stats?period=invalid', '/stats?owner_id=invalid', '/stats?worker_id=invalid',
    '/recent-jobs?owner_id=invalid', '/recent-jobs?worker_id=invalid',
    '/recent-jobs?limit=0', '/recent-jobs?limit=21',
    '/jobs?offset=-1', '/jobs?offset=1.5', '/jobs?limit=0', '/jobs?limit=21', '/jobs?limit=1.5',
    '/jobs?mode=invalid', '/jobs?order=invalid',
    '/jobs?owner_id=invalid', '/jobs?worker_id=invalid',
])
@pytest.mark.asyncio
async def test_get_monitor_422(client, monitor_headers, path):
    response = await client.get('/v1/monitor' + path, headers=monitor_headers)
    assert response.status_code == 422, response.text
    assert response.json()['code'] == 'REQUEST_VALIDATION_ERROR'


@pytest.mark.asyncio
async def test_get_monitor_page_200(client):
    response = await client.get('/monitor')
    assert response.status_code == 200
    assert 'text/html' in response.headers['content-type']
    assert 'Sign in' in response.text
    for name, mime in [('monitor.js', 'javascript'), ('monitor.css', 'text/css')]:
        asset = await client.get('/monitor/' + name)
        assert asset.status_code == 200
        assert mime in asset.headers['content-type']
    assert (await client.get('/monitor/../config.py')).status_code == 404


@pytest.mark.asyncio
async def test_get_monitor_openapi_200(client, monkeypatch):
    monkeypatch.setattr(config, 'SHOW_SECTIONS', {'Monitor'})
    monkeypatch.setattr(app, 'openapi_schema', None)
    response = await client.get('/openapi.json')
    paths = response.json()['paths']
    assert set(paths) == {'/v1/monitor/' + name for name in ('info', 'stats', 'jobs', 'workers', 'principals', 'database', 'engines', 'recent-jobs')}
    for path in paths.values():
        operation = path['get']
        assert operation['tags'] == ['Monitor']
        assert {'200', '401', '403', '422'} <= operation['responses'].keys()
        assert operation['security']


@pytest.mark.parametrize('name', ['MONITOR_REFRESH_INTERVAL_SECONDS', 'MONITOR_WORKER_OFFLINE_AFTER_SECONDS'])
@pytest.mark.parametrize('value', ['0', '-1', '1.5', 'nan', 'inf', ''])
def test_monitor_configuration_invalid(monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match=name):
        Config()


def test_monitor_configuration_matches_env(monkeypatch):
    from pathlib import Path
    for name in ('MONITOR_REFRESH_INTERVAL_SECONDS', 'MONITOR_WORKER_OFFLINE_AFTER_SECONDS',
                 'MONITOR_HOSTNAME', 'MONITOR_IP_ADDRESSES'):
        monkeypatch.delenv(name, raising=False)
    defaults = Config()
    example = (Path(__file__).parents[1] / '.env_example').read_text()
    assert defaults.MONITOR_REFRESH_INTERVAL_SECONDS == 5
    assert defaults.MONITOR_WORKER_OFFLINE_AFTER_SECONDS == 60
    assert defaults.MONITOR_HOSTNAME == ''
    assert defaults.MONITOR_IP_ADDRESSES == []
    for name in vars(defaults):
        if name.startswith('MONITOR_'):
            value = getattr(defaults, name)
            if isinstance(value, list):
                value = ','.join(value)
            assert f'{name}={value}' in example


@pytest.mark.parametrize('hostname,addresses,expected', [
    ('  outer.example.org  ', ' 192.0.2.10, 2001:db8::10 ', ['192.0.2.10', '2001:db8::10']),
    ('', '', []), ('  ', ' , ', []),
])
def test_monitor_identity_configuration(monkeypatch, hostname, addresses, expected):
    monkeypatch.setenv('MONITOR_HOSTNAME', hostname)
    monkeypatch.setenv('MONITOR_IP_ADDRESSES', addresses)
    settings = Config()
    assert settings.MONITOR_HOSTNAME == hostname.strip()
    assert settings.MONITOR_IP_ADDRESSES == expected


@pytest.mark.parametrize('value', ['outer.example.org', '999.0.0.1', '192.0.2.1/24'])
def test_monitor_ip_configuration_invalid(monkeypatch, value):
    monkeypatch.setenv('MONITOR_IP_ADDRESSES', value)
    with pytest.raises(ValueError, match='MONITOR_IP_ADDRESSES'):
        Config()


@pytest.mark.parametrize('age,period,expected', [(20, '7d', 0), (20, '1m', 1), (40, '1m', 0), (40, 'all', 1)])
@pytest.mark.asyncio
async def test_get_monitor_stats_200_month_window(client, admin_headers, job_factory, db_sessions, age, period, expected):
    job_id = await job_factory(ProcessingState.DONE)
    async with db_sessions.begin() as db:
        job = await db.get(model.Job, job_id)
        job.finished_date = datetime.now(timezone.utc) - timedelta(days=age)
        job.started_date = job.finished_date - timedelta(seconds=10)
    response = await client.get('/v1/monitor/stats', params={'period': period}, headers=admin_headers)
    assert response.status_code == 200, response.text
    assert response.json()['data']['done']['job_count'] == expected


@pytest.mark.parametrize('missing', [True, False])
@pytest.mark.asyncio
async def test_get_monitor_stats_200_unavailable_runtime(client, admin_headers, job_factory, db_sessions, missing):
    job_id = await job_factory(ProcessingState.DONE)
    async with db_sessions.begin() as db:
        job = await db.get(model.Job, job_id)
        job.finished_date = datetime.now(timezone.utc)
        job.started_date = None if missing else job.finished_date + timedelta(seconds=1)
    response = await client.get('/v1/monitor/stats', headers=admin_headers)
    assert response.status_code == 200, response.text
    assert response.json()['data']['done']['job_count'] == 1
    assert response.json()['data']['done']['runtime_seconds'] is None
