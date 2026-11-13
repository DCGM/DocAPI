"""Exercise the dummy database filler against a disposable PostgreSQL database."""
from sqlalchemy import select, func
import pytest

from doc_api.api.schemas.base_objects import ProcessingState
from doc_api.db import model
from scripts.seed_dummy_database import DUMMY_UUID_PREFIX, clear_database, seed_database


@pytest.mark.asyncio
async def test_seed_dummy_database_finished_dataset(client, admin_headers, isolated_database, db_sessions, job_factory):
    original = await job_factory(ProcessingState.NEW)
    result = await seed_database(isolated_database.engine.url.render_as_string(hide_password=False),
                                 jobs=1200, owners=12, workers=6, engines=4, versions=3,
                                 prefix='dummy-db-test')
    assert result['done'] + result['failed'] == 1200
    assert result['done'] > result['failed'] > 0
    async with db_sessions() as db:
        jobs = (await db.scalars(select(model.Job).where(model.Job.id != original))).all()
        assert len(jobs) == 1200
        assert all(str(job.id).startswith(DUMMY_UUID_PREFIX + '-') for job in jobs)
        assert (await db.get(model.Job, original)).state == ProcessingState.NEW
        assert {job.state for job in jobs} == {ProcessingState.DONE, ProcessingState.FAILED}
        assert all(job.created_date < job.started_date < job.finished_date <= job.last_change for job in jobs)
        assert all(job.results_deleted_at is not None for job in jobs)
        assert all(job.definition == {'dummy_records': result['prefix']} for job in jobs)
        assert await db.scalar(select(func.count()).select_from(model.Engine).where(model.Engine.active)) == 0
    response = await client.get('/v1/monitor/jobs/statistics', headers=admin_headers,
                                params=[('state', 'done'), ('state', 'failed'), ('top', 20)])
    assert response.status_code == 200, response.text
    data = response.json()['data']
    assert data['done']['job_count'] == result['done']
    assert data['failed']['job_count'] == result['failed']
    assert len(data['owners']) == 12
    assert len(data['workers']) == 7  # Six workers and unassigned jobs.
    assert len(data['engines']) == 5  # Four engines and unassigned jobs.
    assert len(data['engine_versions']) == 13
    for category in ('owners', 'workers', 'engines', 'engine_versions'):
        assert sum(row['total']['job_count'] for row in data[category]) == 1200
        assert sum(row['total']['image_count'] for row in data[category]) == sum(job.image_count for job in jobs)

    removed = await clear_database(isolated_database.engine.url.render_as_string(hide_password=False))
    assert removed == {'jobs': 1200, 'keys': 18, 'engines': 12}
    async with db_sessions() as db:
        assert (await db.get(model.Job, original)).state == ProcessingState.NEW
        assert await db.scalar(select(func.count()).select_from(model.Job)) == 1
        assert await db.scalar(select(func.count()).select_from(model.Engine)) == 0
        assert await db.get(model.Key, jobs[0].owner_key_id) is None
    assert await clear_database(isolated_database.engine.url.render_as_string(hide_password=False)) == {
        'jobs': 0, 'keys': 0, 'engines': 0}
