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


@pytest.mark.parametrize('endpoint', ['info', 'jobs/statistics', 'workers', 'database', 'jobs/processing'])
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


@pytest.mark.asyncio
async def test_get_monitor_stats_matches_jobs(client, monitor_headers, job_factory, db_sessions,
                                             reader_owner, worker_key, engine_factory):
    engines = [await engine_factory(name='Alpha', version='1'),
               await engine_factory(name='Alpha', version='2'),
               await engine_factory(name='Beta', version='1')]
    states = [ProcessingState.DONE, ProcessingState.DONE, ProcessingState.FAILED,
              ProcessingState.PROCESSING, ProcessingState.NEW, ProcessingState.ERROR]
    ids = [await job_factory(state) for state in states]
    now = datetime.now(timezone.utc).replace(microsecond=0)
    async with db_sessions.begin() as db:
        for index, job_id in enumerate(ids):
            job = await db.get(model.Job, job_id)
            job.created_date = now - timedelta(days=index)
            job.last_change = now - timedelta(hours=index)
            job.started_date = None if index == 4 else now - timedelta(hours=index, minutes=2)
            job.finished_date = None if index in {3, 4} else now - timedelta(hours=index)
            job.image_count = index + 1
            job.previous_attempts = None if index == 4 else index % 3
            job.engine_id = engines[index % 3].id if index != 4 else None
            if index == 5:
                job.owner_key_id = worker_key.id
                job.worker_key_id = None
    filters = [
        {}, {'owner_label': reader_owner.label}, {'worker_label': worker_key.label},
        {'owner_label': 'missing'}, {'worker_label': 'missing'},
        {'engine_name': 'Alpha'}, {'engine_version': '1'},
        {'engine_name': 'Alpha', 'engine_version': '2'},
        {'state': ['done', 'failed']}, {'state': ['processing', 'new']},
        {'engine_name': 'Alpha', 'state': ['done', 'failed']},
        *({'state': state.value} for state in states),
        *({f'from_{field}': (now - timedelta(hours=2)).isoformat()}
          for field in ['created_date', 'started_date', 'finished_date', 'last_change']),
        {'owner_label': reader_owner.label, 'worker_label': worker_key.label,
         'engine_name': 'Alpha', 'engine_version': '1', 'state': 'done',
         'from_finished_date': (now - timedelta(days=1)).isoformat()},
    ]
    for params in filters:
        listing = await client.get('/v1/jobs', headers=monitor_headers, params=params)
        assert listing.status_code == 200, listing.text
        jobs = listing.json()['data']
        response = await client.get('/v1/monitor/jobs/statistics', headers=monitor_headers,
                                    params={**params, 'limit': 1, 'offset': 999, 'sort_order': 'asc'})
        assert response.status_code == 200, response.text
        stats = response.json()['data']
        for state in ['done', 'failed']:
            matching = [job for job in jobs if job['state'] == state]
            assert stats[state]['job_count'] == len(matching), params
            assert stats[state]['image_count'] == sum(job['image_count'] for job in matching), params
        attempts = lambda job: 0 if job['previous_attempts'] is None else job['previous_attempts'] + 1
        assert stats['attempts']['job_count'] == sum(attempts(job) for job in jobs), params
        assert stats['attempts']['image_count'] == sum(attempts(job) * job['image_count'] for job in jobs), params
        paginated = await client.get('/v1/jobs', headers=monitor_headers, params={**params, 'limit': 1})
        assert int(paginated.headers['X-Total-Count']) == len(jobs), params


@pytest.mark.asyncio
async def test_get_monitor_stats_filter_contract(client, monitor_headers):
    schema = app.openapi()['paths']
    parameters = lambda path: {p['name']: p['schema'] for p in schema[path]['get']['parameters']
                              if p['in'] == 'query'}
    jobs = parameters('/v1/jobs')
    stats = parameters('/v1/monitor/jobs/statistics')
    filters = {'owner_label', 'worker_label', 'state', 'engine_name', 'engine_version',
               'from_created_date', 'from_started_date', 'from_finished_date', 'from_last_change'}
    assert filters <= stats.keys()
    assert not {'limit', 'offset', 'sort_by', 'sort_order', 'period', 'owner_id', 'worker_id', 'result_available'} & stats.keys()
    for name in filters:
        # Descriptions can differ, but types, enums and defaults must agree.
        assert {k: v for k, v in jobs[name].items() if k not in {'title', 'description'}} == {
            k: v for k, v in stats[name].items() if k not in {'title', 'description'}}, name


@pytest.mark.asyncio
async def test_get_monitor_stats_200(client, monitor_headers, monitor_jobs):
    response = await client.get('/v1/monitor/jobs/statistics', headers=monitor_headers)
    assert response.status_code == 200, response.text
    stats = response.json()['data']
    assert 'period' not in stats
    assert stats['done'] == {'job_count': 1, 'image_count': 12, 'runtime_seconds': 600}
    assert stats['failed'] == {'job_count': 1, 'image_count': 5, 'runtime_seconds': 120}
    assert stats['attempts'] == {'job_count': 7, 'image_count': 53, 'runtime_seconds': None}


@pytest.mark.parametrize('field', ['owner_label', 'worker_label'])
@pytest.mark.asyncio
async def test_get_monitor_stats_200_filters(client, monitor_headers, monitor_jobs, db_sessions, field):
    async with db_sessions() as db:
        job = await db.get(model.Job, monitor_jobs[0])
        key_id = job.owner_key_id if field == 'owner_label' else job.worker_key_id
        label = (await db.get(model.Key, key_id)).label
    matched = await client.get('/v1/monitor/jobs/statistics', params={field: label}, headers=monitor_headers)
    assert matched.json()['data']['done']['job_count'] == 1
    empty = await client.get('/v1/monitor/jobs/statistics', params={field: 'nonexistent-label'}, headers=monitor_headers)
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
        response = await client.get('/v1/monitor/jobs/statistics', headers=monitor_headers)
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
        new_id = await job_factory(ProcessingState.NEW)
        async with db_sessions.begin() as db:
            new_job = await db.get(model.Job, new_id)
            new_job.engine_id = engine.id
            new_job.previous_attempts = None
    new_id = await job_factory(ProcessingState.NEW)
    async with db_sessions.begin() as db:
        (await db.get(model.Job, new_id)).previous_attempts = None
    # Unassigned engines must still count when both filters are All.
    await job_factory(ProcessingState.DONE)
    response = await client.get('/v1/engines', headers=monitor_headers)
    assert response.status_code == 200, response.text
    assert [{'name': engine['name'], 'version': engine['version']} for engine in response.json()['data']] == [
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
        response = await client.get('/v1/monitor/jobs/statistics', params=filters, headers=monitor_headers)
        assert response.status_code == 200, response.text
        data = response.json()['data']
        assert data['done']['job_count'] == count
        if images is not None:
            assert data['done']['image_count'] == images
        assert data['attempts']['job_count'] == attempts
        jobs_response = await client.get('/v1/monitor/jobs/processing', params=filters, headers=monitor_headers)
        assert jobs_response.status_code == 200, jobs_response.text
        rows = jobs_response.json()['data']['jobs']
        assert len(rows) == count
        for field in ['engine_name', 'engine_version']:
            if field in filters:
                assert all(row[field] == filters[field] for row in rows)


@pytest.mark.asyncio
async def test_get_monitor_jobs_removed(client):
    for path in ['/v1/monitor/jobs', '/v1/monitor/stats', '/v1/monitor/recent-jobs',
                 '/v1/monitor/principals', '/v1/monitor/engines']:
        response = await client.get(path)
        assert response.status_code == 404
        assert path not in app.openapi()['paths']


@pytest.mark.asyncio
async def test_jobs_list_200_limit_and_refresh(client, monitor_headers, monitor_jobs, db_sessions):
    response = await client.get('/v1/jobs?limit=2&sort_by=created_date', headers=monitor_headers)
    rows = response.json()['data']
    assert [row['id'] for row in rows] == [str(value) for value in monitor_jobs[:2]]
    assert 'never-return-this-value' not in response.text
    assert rows[0]['image_count'] == 12  # Preserved even with no image rows.
    async with db_sessions.begin() as db:
        (await db.get(model.Job, monitor_jobs[0])).state = ProcessingState.FAILED
    refreshed = await client.get('/v1/jobs?limit=2&sort_by=created_date', headers=monitor_headers)
    assert refreshed.json()['data'][0]['state'] == 'failed'


@pytest.mark.asyncio
async def test_recent_activity_includes_all_pending(client, monitor_headers, job_factory, db_sessions,
                                                    admin_headers, reader_owner):
    now = datetime.now(timezone.utc)
    states = [ProcessingState.DONE, ProcessingState.QUEUED, ProcessingState.PROCESSING,
              ProcessingState.NEW, ProcessingState.QUEUED, ProcessingState.PROCESSING,
              ProcessingState.FAILED, ProcessingState.ERROR, ProcessingState.CANCELLED,
              ProcessingState.NEW, ProcessingState.NEW]
    ids = []
    for index, state in enumerate(states):
        job_id = await job_factory(state)
        ids.append(str(job_id))
        async with db_sessions.begin() as db:
            job = await db.get(model.Job, job_id)
            job.created_date = now - timedelta(minutes=index)
            job.image_count = index + 1
    expected = [ids[i] for i in [5, 2, 4, 1, 3, 9, 10]]
    for limit in [None, 1, 2, 20, 21]:
        for offset in [0, 1, 4, 7, 100]:
            params = {'offset': offset}
            if limit is not None:
                params['limit'] = limit
            response = await client.get('/v1/monitor/jobs/processing', params=params, headers=monitor_headers)
            assert response.status_code == 200, response.text
            data = response.json()['data']
            rows = data['jobs']
            assert [row['id'] for row in rows] == expected[offset:None if limit is None else offset + limit]
            assert data['totals'] == {'processing': 2, 'queued': 2, 'new': 3}
            assert data['image_totals'] == {'processing': 9, 'queued': 7, 'new': 25}
            assert response.headers['X-Total-Count'] == '7'
            assert response.headers['X-Has-More'] == str(offset + len(rows) < 7).lower()

    for state, indices in [('processing', [5, 2]), ('queued', [4, 1]), ('new', [3, 9, 10])]:
        response = await client.get('/v1/monitor/jobs/processing', headers=monitor_headers,
                                    params={'state': state, 'limit': 1, 'offset': 1})
        assert response.status_code == 200, response.text
        data = response.json()['data']
        assert [job['id'] for job in data['jobs']] == [ids[indices[1]]]
        assert response.headers['X-Total-Count'] == str(len(indices))
        assert data['totals'] == {name: len(indices) if name == state else 0
                                  for name in ['processing', 'queued', 'new']}
        assert data['image_totals'] == {name: sum(i + 1 for i in indices) if name == state else 0
                                        for name in ['processing', 'queued', 'new']}
    for state in ['done', 'failed', 'error', 'cancelled', 'invalid']:
        response = await client.get('/v1/monitor/jobs/processing', headers=monitor_headers, params={'state': state})
        assert response.status_code == 422, response.text

    identity = await client.get('/v1/me', headers=admin_headers)
    async with db_sessions.begin() as db:
        for index in [0, 2, 4]:
            (await db.get(model.Job, UUID(ids[index]))).owner_key_id = UUID(identity.json()['data']['id'])
    response = await client.get('/v1/monitor/jobs/processing', headers=monitor_headers,
                                params={'limit': 1, 'owner_id': str(reader_owner.id)})
    assert response.status_code == 200, response.text
    assert [row['id'] for row in response.json()['data']['jobs']] == [ids[5]]
    assert response.json()['data']['totals'] == {'processing': 1, 'queued': 1, 'new': 3}
    assert response.headers['X-Total-Count'] == '5'


@pytest.mark.asyncio
async def test_processing_finished_grace_period(client, monitor_headers, job_factory, db_sessions, reader_owner):
    now = datetime.now(timezone.utc)
    recent, older = [], []
    for state in [ProcessingState.DONE, ProcessingState.FAILED, ProcessingState.ERROR]:
        for seconds, target in [(20, recent), (120, older)]:
            job_id = await job_factory(state)
            target.append(str(job_id))
            async with db_sessions.begin() as db:
                job = await db.get(model.Job, job_id)
                job.created_date = now - timedelta(days=1)
                job.finished_date = now - timedelta(seconds=seconds)
                job.last_change = now  # Retention uses finish time, not last change.
    missing_finish = await job_factory(ProcessingState.ERROR)
    cancelled = await job_factory(ProcessingState.CANCELLED)
    async with db_sessions.begin() as db:
        (await db.get(model.Job, cancelled)).finished_date = now
    new_jobs = [str(await job_factory(ProcessingState.NEW)) for _ in range(3)]
    for seconds, expected in [(None, recent), (0, []), (300, recent + older)]:
        params = {}
        if seconds is not None:
            params['finished_within_seconds'] = seconds
        response = await client.get('/v1/monitor/jobs/processing', params=params, headers=monitor_headers)
        assert response.status_code == 200, response.text
        rows = response.json()['data']['jobs']
        assert response.json()['data']['totals'] == {'processing': 0, 'queued': 0, 'new': 3}
        assert int(response.headers['X-Total-Count']) == len(expected) + 3
        assert {row['id'] for row in rows if row['state'] != 'new'} == set(expected)
        assert sum(row['id'] in new_jobs for row in rows) == 3
        assert str(missing_finish) not in {row['id'] for row in rows}
        assert str(cancelled) not in {row['id'] for row in rows}
    response = await client.get('/v1/monitor/jobs/processing',
                                params={'owner_id': str(UUID(int=0))}, headers=monitor_headers)
    assert response.json()['data'] == {'jobs': [], 'totals': {'processing': 0, 'queued': 0, 'new': 0},
                                         'image_totals': {'processing': 0, 'queued': 0, 'new': 0}}
    assert response.headers['X-Total-Count'] == '0'
    for seconds in [-1, 86401, 'invalid']:
        response = await client.get('/v1/monitor/jobs/processing',
                                    params={'finished_within_seconds': seconds}, headers=monitor_headers)
        assert response.status_code == 422


@pytest.mark.asyncio
async def test_get_monitor_jobs_owner_worker_filters(client, monitor_headers, admin_headers,
                                                   reader_owner, worker_key, job_factory, db_sessions):
    identity = await client.get('/v1/me', headers=admin_headers)
    admin_id = identity.json()['data']['id']
    older = await job_factory(ProcessingState.NEW)
    newer = await job_factory(ProcessingState.NEW, owner_key_id=UUID(admin_id))
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
        response = await client.get('/v1/monitor/jobs/processing', params={'limit': 1, **filters}, headers=monitor_headers)
        assert response.status_code == 200, response.text
        assert [row['id'] for row in response.json()['data']['jobs']] == [str(value) for value in expected]


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
async def test_get_monitor_workers_enabled_last_seen_order(client, monitor_headers, worker_key, db_sessions):
    now = datetime.now(timezone.utc)
    async with db_sessions.begin() as db:
        (await db.get(model.Key, worker_key.id)).last_used = now - timedelta(minutes=1)
        for label, last_used, active in [
            ('a-never-seen', None, True),
            ('z-recent', now, True),
            ('disabled-recent', now + timedelta(seconds=1), False),
        ]:
            db.add(model.Key(label=label, role=KeyRole.WORKER, active=active,
                             last_used=last_used, kid=label, salt=label, key_hash=label))
    response = await client.get('/v1/monitor/workers', headers=monitor_headers)
    assert response.status_code == 200, response.text
    workers = response.json()['data']
    assert [row['label'] for row in workers] == ['z-recent', worker_key.label, 'a-never-seen']
    assert all('enabled' not in row for row in workers)


@pytest.mark.asyncio
async def test_get_monitor_workers_200_processing_always_active(client, monitor_headers, monitor_jobs,
                                                               worker_key, db_sessions):
    async with db_sessions.begin() as db:
        key = await db.get(model.Key, worker_key.id)
        key.last_used = None
    response = await client.get('/v1/monitor/workers', headers=monitor_headers)
    worker = next(row for row in response.json()['data'] if row['id'] == str(worker_key.id))
    assert worker['status'] == 'active'
    assert worker['processing_jobs'] == 1
    assert 'enabled' not in worker
    assert worker['last_used'] is None
    assert not {'kid', 'key_hash', 'salt'} & worker.keys()

    async with db_sessions.begin() as db:
        (await db.get(model.Key, worker_key.id)).active = False
    response = await client.get('/v1/monitor/workers', headers=monitor_headers)
    assert response.status_code == 200, response.text
    assert str(worker_key.id) not in {row['id'] for row in response.json()['data']}


@pytest.mark.parametrize('path', [
    '/jobs/statistics?from_created_date=invalid', '/jobs/statistics?state=invalid',
    '/jobs/processing?owner_id=invalid', '/jobs/processing?worker_id=invalid',
    '/jobs/processing?limit=0', '/jobs/processing?offset=-1', '/jobs/processing?offset=invalid',
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
    assert set(paths) == {'/v1/monitor/' + name for name in ('info', 'jobs/statistics', 'workers', 'database', 'jobs/processing')}
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


@pytest.mark.parametrize('age,days,expected', [(20, 7, 0), (20, 30, 1), (40, 30, 0), (40, None, 1)])
@pytest.mark.asyncio
async def test_get_monitor_stats_200_finished_date_filter(client, admin_headers, job_factory, db_sessions, age, days, expected):
    job_id = await job_factory(ProcessingState.DONE)
    async with db_sessions.begin() as db:
        job = await db.get(model.Job, job_id)
        job.finished_date = datetime.now(timezone.utc) - timedelta(days=age)
        job.started_date = job.finished_date - timedelta(seconds=10)
    response = await client.get('/v1/monitor/jobs/statistics', params={} if days is None else {'from_finished_date': (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()}, headers=admin_headers)
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
    response = await client.get('/v1/monitor/jobs/statistics', headers=admin_headers)
    assert response.status_code == 200, response.text
    assert response.json()['data']['done']['job_count'] == 1
    assert response.json()['data']['done']['runtime_seconds'] is None


@pytest.mark.asyncio
async def test_get_monitor_stats_200_group_partitions(client, monitor_headers, monitor_jobs, db_sessions):
    from sqlalchemy import event

    statements = []
    async with db_sessions() as db:
        engine = db.bind.sync_engine
    def record(connection, cursor, statement, parameters, context, executemany):
        if 'GROUPING SETS' in statement:
            statements.append(statement)
    event.listen(engine, 'before_cursor_execute', record)
    try:
        response = await client.get('/v1/monitor/jobs/statistics', headers=monitor_headers,
                                    params=[('state', 'done'), ('state', 'failed')])
    finally:
        event.remove(engine, 'before_cursor_execute', record)
    assert response.status_code == 200, response.text
    assert len(statements) == 1
    data = response.json()['data']
    for category in ('owners', 'workers', 'engines', 'engine_versions'):
        groups = data[category]
        assert len(groups) == 1
        assert groups[0]['total'] == {'job_count': 2, 'image_count': 17, 'runtime_seconds': 720}
        for state in ('done', 'failed', 'attempts'):
            assert groups[0][state] == data[state]
    assert data['owners'][0]['id'] is not None
    assert data['workers'][0]['id'] is not None
    assert data['engines'][0]['name'] is None
    assert data['engine_versions'][0]['version'] is None


@pytest.mark.asyncio
async def test_get_monitor_stats_200_group_ranking(client, admin_headers, job_factory, engine_factory, db_sessions):
    engines = [await engine_factory(name='Alpha', version='1'),
               await engine_factory(name='Alpha', version='2'),
               await engine_factory(name='Beta', version='1')]
    identities = []
    for label in ('rank-a', 'rank-b'):
        response = await client.post('/v1/admin/keys', headers=admin_headers,
                                     json={'label': label, 'role': 'user'})
        assert response.status_code == 201, response.text
        identity = await client.get('/v1/me', headers={'X-API-Key': response.json()['data']['secret']})
        assert identity.status_code == 200, identity.text
        identities.append(UUID(identity.json()['data']['id']))
    now = datetime.now(timezone.utc)
    for index, (images, runtime, previous) in enumerate([(2, 10, 1), (5, 20, 0), (9, 40, 2), (1, None, None)]):
        job_id = await job_factory(ProcessingState.FAILED if index == 1 else ProcessingState.DONE,
                                   owner_key_id=identities[index // 2])
        async with db_sessions.begin() as db:
            job = await db.get(model.Job, job_id)
            job.engine_id = engines[index].id if index < 3 else None
            job.worker_key_id = identities[index // 2] if index < 3 else None
            job.image_count = images
            job.previous_attempts = previous
            job.finished_date = now
            job.started_date = now - timedelta(seconds=runtime) if runtime is not None else None
    async def stats(**params):
        response = await client.get('/v1/monitor/jobs/statistics', headers=admin_headers, params=params)
        assert response.status_code == 200, response.text
        return response.json()['data']
    all_groups = await stats()
    assert await stats(top=0) == all_groups
    assert [row['name'] for row in all_groups['engines']] == ['Alpha', 'Beta', None]
    assert [(row['name'], row['version']) for row in all_groups['engine_versions']] == [
        ('Alpha', '1'), ('Alpha', '2'), ('Beta', '1'), (None, None)]
    assert [(row['label'], row['total']['job_count']) for row in all_groups['workers']] == [
        ('rank-a', 2), ('rank-b', 1), (None, 1)]
    for top_by, expected in [('job_count', 'Alpha'), ('image_count', 'Beta'),
                             ('runtime_seconds', 'Beta'), ('attempt_count', 'Alpha'),
                             ('attempt_image_count', 'Beta')]:
        limited = await stats(top=1, top_by=top_by)
        assert all(len(limited[category]) == 1 for category in ('owners', 'workers', 'engines', 'engine_versions'))
        assert limited['engines'][0]['name'] == expected
        for state in ('done', 'failed', 'attempts'):
            assert limited[state] == all_groups[state]
    filtered = await stats(engine_name='Alpha', engine_version='2', state='failed', top=1)
    assert filtered['engines'][0]['total']['job_count'] == 1
    assert filtered['engines'][0]['failed'] == filtered['failed']
    empty = await stats(owner_label='nonexistent')
    assert all(empty[category] == [] for category in ('owners', 'workers', 'engines', 'engine_versions'))
    assert empty['done']['job_count'] == empty['attempts']['job_count'] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('params', [{'top': -1}, {'top_by': 'unknown'}])
async def test_get_monitor_stats_422_group_options(client, admin_headers, params):
    response = await client.get('/v1/monitor/jobs/statistics', headers=admin_headers, params=params)
    assert response.status_code == 422, response.text


@pytest.mark.parametrize('reader_role', ['user', 'readonly'])
@pytest.mark.asyncio
async def test_monitor_owner_scope(client, reader_headers, reader_owner, job_factory, db_sessions, worker_key, reader_role):
    """Totals, pagination, grouped identities and filters cannot cross ownership."""
    own_done = await job_factory(ProcessingState.DONE)
    own_pending = await job_factory(ProcessingState.PROCESSING)
    other_done = await job_factory(ProcessingState.DONE, owner_key_id=worker_key.id)
    other_pending = await job_factory(ProcessingState.PROCESSING, owner_key_id=worker_key.id)
    async with db_sessions.begin() as db:
        for job_id in (own_done, other_done):
            job = await db.get(model.Job, job_id)
            job.finished_date = datetime.now(timezone.utc)
        for job_id, name in [(own_done, 'own-engine'), (other_done, 'other-engine')]:
            engine = model.Engine(name=name, version='1', description=name)
            db.add(engine)
            await db.flush()
            (await db.get(model.Job, job_id)).engine_id = engine.id
        own = await db.get(model.Job, own_done)
        own.final_log_user = {'0': 'User final message'}
        own.final_log = {'0': 'Private final message'}

    stats = await client.get('/v1/monitor/jobs/statistics?top=0&state=done', headers=reader_headers)
    assert stats.status_code == 200, stats.text
    data = stats.json()['data']
    assert data['done']['job_count'] == 1
    assert data['owners'] == data['workers'] == []
    assert [group['name'] for group in data['engines']] == ['own-engine']
    assert [group['name'] for group in data['engine_versions']] == ['own-engine']
    assert sum(group['total']['job_count'] for group in data['engines']) == 1
    for extra, count in [('', 2), ('&state=processing', 1), ('&engine_name=missing', 0), ('&offset=10', 2)]:
        response = await client.get('/v1/monitor/jobs/processing?limit=1' + extra, headers=reader_headers)
        assert response.status_code == 200, response.text
        assert response.headers['X-Total-Count'] == str(count)
        assert response.json()['data']['totals']['processing'] == (0 if 'missing' in extra else 1)
        for job in response.json()['data']['jobs']:
            assert job['id'] in {str(own_done), str(own_pending)}
            assert job['owner_label'] is None and job['worker_label'] is None
        if 'offset' in extra:
            assert response.json()['data']['jobs'] == []
            assert response.headers['X-Has-More'] == 'false'
    for path in [f'jobs/processing?owner_id={worker_key.id}', f'jobs/processing?worker_id={worker_key.id}',
                 'jobs/statistics?owner_label=other', 'jobs/statistics?worker_label=other',
                 'info', 'workers', 'database']:
        response = await client.get('/v1/monitor/' + path, headers=reader_headers)
        assert response.status_code == 403, (path, response.text)
    response = await client.get('/v1/jobs?state=done&limit=1', headers=reader_headers)
    assert response.status_code == 200, response.text
    assert response.headers['X-Total-Count'] == '1'
    assert [job['id'] for job in response.json()['data']] == [str(own_done)]
    response = await client.get(f'/v1/jobs/{own_done}', headers=reader_headers)
    assert response.status_code == 200, response.text
    data = response.json()['data']
    assert data['log_user'] == 'User log'
    assert data['final_log_user'] == 'User final message'
    assert not data.get('log') and not data.get('final_log')
    for job_id in (other_done, other_pending):
        # Read-only detail access is intentionally broader in the existing API.
        assert (await client.get(f'/v1/jobs/{job_id}', headers=reader_headers)).status_code == (
            403 if reader_role == 'user' else 200)


@pytest.mark.parametrize('reader_role', ['user', 'readonly'])
@pytest.mark.asyncio
async def test_monitor_user_empty(client, reader_headers):
    response = await client.get('/v1/monitor/jobs/statistics?top=0', headers=reader_headers)
    assert response.status_code == 200, response.text
    data = response.json()['data']
    assert data['done']['job_count'] == data['failed']['job_count'] == 0
    assert all(data[group] == [] for group in ('owners', 'workers', 'engines', 'engine_versions'))
    response = await client.get('/v1/monitor/jobs/processing?limit=5&offset=20', headers=reader_headers)
    assert response.status_code == 200, response.text
    assert response.headers['X-Total-Count'] == '0'
    assert response.headers['X-Has-More'] == 'false'
    assert response.json()['data']['jobs'] == []


@pytest.mark.parametrize('role', ['worker'])
@pytest.mark.parametrize('endpoint', ['jobs/statistics', 'jobs/processing'])
@pytest.mark.asyncio
async def test_monitor_job_role_forbidden(client, role, endpoint):
    response = await client.get('/v1/monitor/' + endpoint,
                                headers={'X-API-Key': getattr(config, f'TEST_{role.upper()}_KEY')})
    assert response.status_code == 403, response.text
