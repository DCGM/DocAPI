import os
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import asyncpg
import httpx
import pytest
import pytest_asyncio
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from doc_api.adapter import Adapter
from doc_api.api.cruds import worker_cruds
from doc_api.api.database import get_async_session
from doc_api.api.main import app
from doc_api.api.schemas.base_objects import KeyRole, ProcessingState, JobProgressUpdate
from doc_api.api.schemas.responses import AppCode
from doc_api.db import model


@pytest_asyncio.fixture
async def failure_api():
    """Use a temporary database so running workers cannot lease test jobs."""
    database_url = os.getenv("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL required for isolated PostgreSQL tests")
    url = make_url(database_url)
    database_name = "docapi_test_" + uuid4().hex
    admin = await asyncpg.connect(
        host=url.host, port=url.port or 5432, user=url.username,
        password=url.password, database=url.database,
    )
    engine = None
    previous_overrides = dict(app.dependency_overrides)
    try:
        await admin.execute(f'CREATE DATABASE "{database_name}"')
        engine = create_async_engine(url.set(database=database_name))
        async with engine.begin() as conn:
            await conn.run_sync(model.Base.metadata.create_all)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        keys = []
        async with sessions.begin() as db:
            for role in [KeyRole.USER, KeyRole.WORKER, KeyRole.WORKER]:
                token = uuid4().hex
                key = model.Key(id=uuid4(), kid=token, salt=token, key_hash=token,
                                label=token, role=role)
                db.add(key)
                keys.append(key)
        owner, worker, other_worker = keys
        identity = SimpleNamespace(key=worker)

        async def current_key():
            return identity.key

        async def session():
            async with sessions() as db:
                yield db

        route = next(r for r in app.routes if getattr(r, "path", None) == "/v1/jobs/{job_id}"
                     and "PATCH" in r.methods)
        auth = next(d.call for d in route.dependant.dependencies if d.name == "key")
        app.dependency_overrides[auth] = current_key
        app.dependency_overrides[get_async_session] = session
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            yield SimpleNamespace(client=client, sessions=sessions, identity=identity,
                                  owner=owner, worker=worker, other_worker=other_worker)
    finally:
        app.dependency_overrides.clear()
        app.dependency_overrides.update(previous_overrides)
        if engine is not None:
            await engine.dispose()
        await admin.execute(f'DROP DATABASE IF EXISTS "{database_name}"')
        await admin.close()


async def create_job(api, state):
    job = model.Job(id=uuid4(), owner_key_id=api.owner.id, worker_key_id=api.worker.id,
                    definition={"images": []}, state=state, progress=0.4,
                    previous_attempts=0, log="Technical log", log_user="User log")
    async with api.sessions.begin() as db:
        db.add(job)
    return job.id


@pytest.mark.asyncio
@pytest.mark.parametrize("initial_state", [ProcessingState.PROCESSING, ProcessingState.ERROR])
async def test_worker_permanent_failure(failure_api, initial_state):
    api = failure_api
    job_id = await create_job(api, initial_state)
    before = datetime.now(timezone.utc)
    payload = {"state": "failed", "log": "Permanent error", "log_user": "Cannot process input"}
    response = await api.client.patch(f"/v1/jobs/{job_id}", json=payload)
    assert response.status_code == 200, response.text
    assert response.json()["code"] == AppCode.JOB_MARKED_FAILED.value

    async with api.sessions() as db:
        job = await db.get(model.Job, job_id)
        assert job.state == ProcessingState.FAILED
        assert job.progress == 1.0
        assert job.finished_date >= before
        assert job.last_change == job.finished_date
        assert job.log == "Technical log\nPermanent error"
        assert job.log_user == "User log\nCannot process input"
        assert job.previous_attempts == 0
        finished_date = job.finished_date

    response = await api.client.patch(f"/v1/jobs/{job_id}", json=payload)
    assert response.status_code == 200, response.text
    assert response.json()["code"] == AppCode.JOB_ALREADY_MARKED_FAILED.value
    for state in ["done", "error"]:
        response = await api.client.patch(f"/v1/jobs/{job_id}", json={"state": state})
        assert response.status_code == 409, response.text
    async with api.sessions() as db:
        _, _, _, code = await worker_cruds.lease_job_to_worker(db=db, worker_key_id=api.worker.id)
        assert code == AppCode.JOB_QUEUE_EMPTY
        job = await db.get(model.Job, job_id)
        assert job.state == ProcessingState.FAILED
        assert job.finished_date == finished_date
        assert job.log == "Technical log\nPermanent error"


@pytest.mark.asyncio
@pytest.mark.parametrize("state", [ProcessingState.NEW, ProcessingState.QUEUED,
                                   ProcessingState.CANCELLED, ProcessingState.DONE])
async def test_failure_rejects_invalid_source_state(failure_api, state):
    api = failure_api
    job_id = await create_job(api, state)
    response = await api.client.patch(f"/v1/jobs/{job_id}", json={"state": "failed"})
    assert response.status_code == 409, response.text
    assert response.json()["code"] == AppCode.JOB_UNFINISHABLE.value
    async with api.sessions() as db:
        assert (await db.get(model.Job, job_id)).state == state
        await db.rollback()
        # The transaction-level check also rejects a stale request after a state change.
        _, _, _, code = await worker_cruds.update_job_progress(
            db=db, job_id=job_id, job_progress_update=JobProgressUpdate(state="failed"))
        assert code == AppCode.JOB_UNFINISHABLE


@pytest.mark.asyncio
@pytest.mark.parametrize("identity", ["owner", "other_worker"])
async def test_failure_requires_assigned_worker(failure_api, identity):
    api = failure_api
    job_id = await create_job(api, ProcessingState.PROCESSING)
    api.identity.key = getattr(api, identity)
    response = await api.client.patch(f"/v1/jobs/{job_id}", json={"state": "failed"})
    assert response.status_code == 403, response.text
    async with api.sessions() as db:
        assert (await db.get(model.Job, job_id)).state == ProcessingState.PROCESSING


@pytest.mark.asyncio
async def test_error_remains_retryable(failure_api):
    api = failure_api
    job_id = await create_job(api, ProcessingState.PROCESSING)
    response = await api.client.patch(f"/v1/jobs/{job_id}", json={"state": "error"})
    assert response.status_code == 200, response.text
    assert response.json()["code"] == AppCode.JOB_MARKED_ERROR.value
    async with api.sessions() as db:
        job, _, _, code = await worker_cruds.lease_job_to_worker(db=db, worker_key_id=api.worker.id)
        assert code == AppCode.JOB_LEASED
        assert job.id == job_id
        assert job.previous_attempts == 1


@pytest.mark.parametrize("method,expected", [("patch_job_fail", "failed"), ("patch_job_error", "error")])
def test_adapter_failure_state(method, expected):
    connector = Mock()
    connector.patch.return_value.status_code = 200
    connector.patch.return_value.json.return_value = {"code": "OK"}
    adapter = Adapter("http://test", connector)
    response = getattr(adapter, method)(job_id="job", log="technical", log_user="public")
    assert response.is_success
    connector.patch.assert_called_once_with(
        "http://test/v1/jobs/job", json={"state": expected, "log": "technical", "log_user": "public"})
