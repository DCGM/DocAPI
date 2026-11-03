import logging
import os
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import asyncpg
from sqlalchemy import select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import httpx

import os.path

import pytest
import pytest_asyncio

from doc_api.api import cleanup
from doc_api.api import database as db_module
from doc_api.api.main import app
from doc_api.api.schemas.base_objects import ProcessingState
from doc_api.db import model
from doc_api.tests.helpers import migrate, _put_file, _ename
from doc_api.api.schemas import base_objects
from doc_api.tests.dummy_data import make_white_image_bytes, VALID_ALTO_XML, VALID_PAGE_XML, VALID_ZIP

from doc_api.api.schemas.responses import AppCode
from doc_api.config import config

logger = logging.getLogger(__name__)

# Database-backed tests require an explicit PostgreSQL maintenance connection.
# Only uniquely named databases created here are migrated or dropped.
@pytest_asyncio.fixture
async def isolated_database():
    database_url = os.getenv("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("Set TEST_DATABASE_URL to run isolated PostgreSQL tests (see tests/README.md)")
    url = make_url(database_url).set(drivername="postgresql+asyncpg")
    database_name = "docapi_test_" + uuid4().hex
    admin = await asyncpg.connect(
        host=url.host, port=url.port or 5432, user=url.username,
        password=url.password, database=url.database,
    )
    engine = None
    created = False
    try:
        await admin.execute(f'CREATE DATABASE "{database_name}"')
        created = True
        engine = create_async_engine(url.set(database=database_name))
        yield SimpleNamespace(engine=engine, name=database_name,
                              sessions=async_sessionmaker(engine, expire_on_commit=False))
    finally:
        try:
            if engine is not None:
                await engine.dispose()
            if created:
                await admin.execute(f'DROP DATABASE "{database_name}" WITH (FORCE)')
        finally:
            await admin.close()


@pytest.fixture
def test_settings(monkeypatch, tmp_path):
    # Disregard deployment settings: startup must only create test credentials,
    # and uploads/cleanup must never reach the application's data directories.
    monkeypatch.setattr(config, "PRODUCTION", False)
    monkeypatch.setattr(config, "ADMIN_KEY", None)
    monkeypatch.setattr(config, "INTERNAL_MAIL_SERVER", None)
    monkeypatch.setattr(config, "KEY_PREFIX", "doc-api")
    monkeypatch.setattr(config, "HMAC_SECRET", "isolated-test-hmac-secret")
    monkeypatch.setattr(config, "DELETE_JOB_INPUTS_ON_COMPLETION", False)
    monkeypatch.setattr(config, "DELETE_JOB_OUTPUTS", False)
    monkeypatch.setattr(config, "REQUIRE_IMAGE_EXTENSION", False)
    monkeypatch.setattr(config, "ALLOWED_IMAGE_EXTENSIONS", set())
    for role in ("ADMIN", "READONLY", "USER", "WORKER"):
        monkeypatch.setattr(config, f"TEST_{role}_KEY", f"doc-api.test{role.lower()}kid.test{role.lower()}secret")
        monkeypatch.setattr(config, f"TEST_{role}_KEY_LABEL", f"test-{role.lower()}")
    monkeypatch.setattr(config, "BASE_DIR", str(tmp_path))
    for name in ("JOBS", "RESULTS", "ARTIFACTS", "ENGINES", "LOGGING"):
        monkeypatch.setattr(config, f"{name}_DIR", str(tmp_path / name.lower()))
    config.create_dirs()


@pytest_asyncio.fixture
async def prepared_database(isolated_database, test_settings, monkeypatch):
    """Migrated, isolated DB before application startup creates credentials."""
    await migrate(isolated_database.engine, "head")
    monkeypatch.setattr(db_module, "global_engine", isolated_database.engine)
    monkeypatch.setattr(db_module, "global_async_session_maker", isolated_database.sessions)
    return isolated_database


@pytest_asyncio.fixture
async def client(prepared_database):
    previous_overrides = dict(app.dependency_overrides)
    try:
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app, raise_app_exceptions=False), base_url="http://test",
                follow_redirects=True,
            ) as http_client:
                yield http_client
    finally:
        app.dependency_overrides.clear()
        app.dependency_overrides.update(previous_overrides)


@pytest_asyncio.fixture
async def db_sessions(client, isolated_database):
    """Session factory for setup/assertions against the same DB as client."""
    return isolated_database.sessions


@pytest.fixture
def readonly_headers(test_settings):
    return {"X-API-Key": config.TEST_READONLY_KEY}


@pytest.fixture
def user_role():
    return "user"


@pytest.fixture
def worker_role():
    return "worker"


@pytest.fixture
def reader_role():
    return "user"


@pytest.fixture
def reader_headers(test_settings, reader_role):
    return {"X-API-Key": getattr(config, f"TEST_{reader_role.upper()}_KEY")}


@pytest_asyncio.fixture
async def reader_owner(db_sessions, reader_role):
    """Readonly listing uses its own jobs; admins exercise user-owned jobs."""
    label = config.TEST_READONLY_KEY_LABEL if reader_role == "readonly" else config.TEST_USER_KEY_LABEL
    async with db_sessions() as db:
        return (await db.execute(select(model.Key).where(model.Key.label == label))).scalar_one()


@pytest.fixture
def user_headers(test_settings, user_role):
    return {"X-API-Key": getattr(config, f"TEST_{user_role.upper()}_KEY")}


@pytest.fixture
def worker_headers(test_settings, worker_role):
    return {"X-API-Key": getattr(config, f"TEST_{worker_role.upper()}_KEY")}


@pytest.fixture
def admin_headers(test_settings):
    return {"X-API-Key": config.TEST_ADMIN_KEY}


@pytest_asyncio.fixture
async def worker_key(db_sessions):
    async with db_sessions() as db:
        return (await db.execute(select(model.Key).where(model.Key.label == config.TEST_WORKER_KEY_LABEL))).scalar_one()


@pytest_asyncio.fixture
async def other_worker_headers(client, admin_headers):
    response = await client.post("/v1/admin/keys", headers=admin_headers,
                                 json={"label": "other-worker", "role": "worker"})
    assert response.status_code == 201, response.text
    return {"X-API-Key": response.json()["data"]["secret"]}


@pytest_asyncio.fixture
async def job_factory(db_sessions, worker_key, reader_owner):
    async def create(state, *, owner_key_id=None):
        job = model.Job(id=uuid4(), owner_key_id=owner_key_id or reader_owner.id,
                        worker_key_id=worker_key.id, definition={"images": []},
                        state=state, progress=0.4, previous_attempts=0,
                        log="Technical log", log_user="User log")
        async with db_sessions.begin() as db:
            db.add(job)
        return job.id

    return create


@pytest_asyncio.fixture
async def output_factory(db_sessions, job_factory):
    async def create(state, finished):
        job_id = await job_factory(state)
        async with db_sessions.begin() as db:
            (await db.get(model.Job, job_id)).finished_date = finished
        outputs = {}
        for target, directory in [("results", config.RESULTS_DIR), ("artifacts", config.ARTIFACTS_DIR)]:
            path = Path(directory) / f"{job_id}.zip"
            path.write_bytes(b"output")
            outputs[target] = path
        return job_id, outputs

    return create


@pytest.fixture
def payload(request):
    return request.param


@pytest_asyncio.fixture
async def created_job(client, user_headers, payload):
    r = await client.post("/v1/jobs", json=payload, headers=user_headers)

    assert r.status_code == 201, r.text

    body = r.json()
    assert body["code"] == AppCode.JOB_CREATED.value
    assert body["status"] == 201

    job = body["data"]

    return {"created_job": job, "payload": payload}

@pytest_asyncio.fixture
async def cancelled_new_job(client, user_headers, created_job):
    job = created_job["created_job"]
    job_id = job["id"]

    r = await client.patch(
        f"/v1/jobs/{job_id}",
        headers=user_headers,
        json={"state": base_objects.ProcessingState.CANCELLED.value},
    )
    assert r.status_code == 200, r.text

    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_CANCELLED.value

    return created_job


@pytest_asyncio.fixture
async def job_with_required_uploads_by_payload_name(client, user_headers, created_job):
    return await _job_with_required_uploads_by_payload_name(client, user_headers, created_job)

@pytest_asyncio.fixture
async def job_with_engine_with_required_uploads_by_payload_name(client, user_headers, created_job_with_engine):
    return await _job_with_required_uploads_by_payload_name(client, user_headers, created_job_with_engine)

@pytest_asyncio.fixture
async def job_with_uploaded_engine_with_required_uploads_by_payload_name(client, user_headers, created_job_with_uploaded_engine):
    return await _job_with_required_uploads_by_payload_name(client, user_headers, created_job_with_uploaded_engine)


async def _job_with_required_uploads_by_payload_name(client, user_headers, created_job):
    job = created_job["created_job"]
    payload = created_job["payload"]
    job_id = job["id"]

    if payload["meta_json_required"]:
        r = await client.put(
            f"/v1/jobs/{job_id}/files/metadata",
            headers=user_headers,
            json={"meta": "dummy"},
        )
        assert r.status_code == 201, r.text
        r = await client.put(
            f"/v1/jobs/{job_id}/files/metadata",
            headers=user_headers,
            json={"meta": "dummy"},
        )
        assert r.status_code == 200, r.text

    for i, pimg in enumerate(payload["images"]):
        name = pimg["name"]
        enc = _ename(name)

        img_bytes, ctype = make_white_image_bytes(os.path.splitext(name)[1])
        r = await _put_file(
            client,
            f"/v1/jobs/{job_id}/images/{enc}/files/image",
            "file",
            name,
            img_bytes,
            ctype,
            user_headers,
        )
        assert r.status_code == 201, r.text
        if i < len(payload["images"]) - 1:
            r = await _put_file(
                client,
                f"/v1/jobs/{job_id}/images/{enc}/files/image",
                "file",
                name,
                img_bytes,
                ctype,
                user_headers,
            )
            assert r.status_code == 200, r.text

        if payload["alto_required"]:
            r = await _put_file(
                client,
                f"/v1/jobs/{job_id}/images/{enc}/files/alto",
                "file",
                f"{name.rsplit('.', 1)[0]}.xml",
                VALID_ALTO_XML,
                "application/xml",
                user_headers,
            )
            assert r.status_code == 201, r.text
            if i < len(payload["images"]) - 1:
                r = await _put_file(
                    client,
                    f"/v1/jobs/{job_id}/images/{enc}/files/alto",
                    "file",
                    f"{name.rsplit('.', 1)[0]}.xml",
                    VALID_ALTO_XML,
                    "application/xml",
                    user_headers,
                )
                assert r.status_code == 200, r.text

        if payload["page_required"]:
            r = await _put_file(
                client,
                f"/v1/jobs/{job_id}/images/{enc}/files/page",
                "file",
                f"{name.rsplit('.', 1)[0]}.xml",
                VALID_PAGE_XML,
                "application/xml",
                user_headers,
            )
            assert r.status_code == 201, r.text
            if i < len(payload["images"]) - 1:
                r = await _put_file(
                    client,
                    f"/v1/jobs/{job_id}/images/{enc}/files/page",
                    "file",
                    f"{name.rsplit('.', 1)[0]}.xml",
                    VALID_PAGE_XML,
                    "application/xml",
                    user_headers,
                )
                assert r.status_code == 200, r.text

    return created_job


@pytest_asyncio.fixture
async def lease_job(client, worker_headers, job_with_required_uploads_by_payload_name):
    return await _lease_job(client, worker_headers, job_with_required_uploads_by_payload_name)

@pytest_asyncio.fixture
async def lease_job_with_engine(client, worker_headers, job_with_engine_with_required_uploads_by_payload_name):
    return await _lease_job(client, worker_headers, job_with_engine_with_required_uploads_by_payload_name)

@pytest_asyncio.fixture
async def lease_job_with_uploaded_engine(client, worker_headers, job_with_uploaded_engine_with_required_uploads_by_payload_name):
    return await _lease_job(client, worker_headers, job_with_uploaded_engine_with_required_uploads_by_payload_name)

async def _lease_job(client, worker_headers, job_with_required_uploads_by_payload_name):
    r = await client.post(
        "/v1/jobs/lease",
        headers=worker_headers
    )
    assert r.status_code == 200, r.text

    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_LEASED.value

    lease = body["data"]

    return {**job_with_required_uploads_by_payload_name, "lease": lease}


@pytest_asyncio.fixture
async def failed_job(client, worker_headers, job_with_required_uploads_by_payload_name):
    job = job_with_required_uploads_by_payload_name["created_job"]
    job_id = job["id"]

    for i in range(config.JOB_MAX_ATTEMPTS):
        r = await client.post(
            "/v1/jobs/lease",
            headers=worker_headers
        )
        assert r.status_code == 200, r.text

        body = r.json()
        assert body["status"] == 200
        assert body["code"] == AppCode.JOB_LEASED.value

        lease = body["data"]

        r = await client.patch(
            f"/v1/jobs/{job_id}",
            headers=worker_headers,
            json={"state": base_objects.ProcessingState.ERROR.value,
                  "log": "technical error log",
                  "log_user": "user-friendly error log"}
        )

        assert r.status_code == 200, r.text
        body = r.json()
        assert body["status"] == 200
        assert body["code"] == AppCode.JOB_MARKED_ERROR.value

    r = await client.post(
        "/v1/jobs/lease",
        headers=worker_headers
    )
    assert r.status_code == 200, r.text

    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_QUEUE_EMPTY.value

    return job_with_required_uploads_by_payload_name


@pytest_asyncio.fixture
async def cancelled_processing_job(client, user_headers, lease_job):
    job = lease_job["created_job"]
    job_id = job["id"]

    r = await client.patch(
        f"/v1/jobs/{job_id}",
        headers=user_headers,
        json={"state": base_objects.ProcessingState.CANCELLED.value},
    )
    assert r.status_code == 200, r.text

    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_CANCELLED.value

    return lease_job


@pytest_asyncio.fixture
async def job_with_result(client, worker_headers, lease_job):
    job_id = lease_job["created_job"]["id"]

    r = await client.post(
        f"/v1/jobs/{job_id}/result",
        headers=worker_headers,
        files={"file": ("result.zip", VALID_ZIP, "application/zip")},
    )

    assert r.status_code == 201, r.text
    body = r.json()
    assert body["status"] == 201
    assert body["code"] == AppCode.JOB_RESULT_UPLOADED.value

    return lease_job


@pytest_asyncio.fixture
async def job_with_artifacts(client, worker_headers, lease_job):
    job_id = lease_job["created_job"]["id"]

    r = await client.post(
        f"/v1/jobs/{job_id}/artifacts",
        headers=worker_headers,
        files={"file": ("result.zip", VALID_ZIP, "application/zip")},
    )

    assert r.status_code == 201, r.text
    body = r.json()
    assert body["status"] == 201
    assert body["code"] == AppCode.JOB_ARTIFACTS_UPLOADED.value

    return lease_job


@pytest_asyncio.fixture
async def job_marked_done(client, worker_headers, job_with_result):
    job_id = job_with_result["lease"]["id"]

    update_payload = {"state": base_objects.ProcessingState.DONE.value,
                      "log": "technical log",
                      "log_user": "user-friendly log"}

    r = await client.patch(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
        json=update_payload
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_COMPLETED.value

    return {**job_with_result, "update_payload": update_payload}


@pytest_asyncio.fixture
async def job_marked_error(client, worker_headers, lease_job):
    job_id = lease_job["lease"]["id"]

    update_payload = {"state": base_objects.ProcessingState.ERROR.value,
                      "log": "technical error log",
                      "log_user": "user-friendly error log"}

    r = await client.patch(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
        json=update_payload
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_MARKED_ERROR.value

    return {**lease_job, "update_payload": update_payload}

@pytest.fixture
def key_role(request):
    return request.param

@pytest_asyncio.fixture
async def new_key(client, admin_headers, key_role):
    label = f"test-{key_role}-key-{os.urandom(4).hex()}"
    r = await client.post(
        "/v1/admin/keys",
        headers=admin_headers,
        json={
            "label": label,
            "role": key_role
        }
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["code"] == AppCode.KEY_CREATED.value
    data = body["data"]
    assert "secret" in data
    assert len(data["secret"]) > 0

    return {"role": key_role, "label": label, "secret": data["secret"]}


@pytest_asyncio.fixture
async def inactive_key(client, admin_headers, new_key):
    label = new_key["label"]

    r = await client.patch(
        f"/v1/admin/keys/{label}",
        headers=admin_headers,
        json={
            "active": False
        }
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.KEY_UPDATED.value

    return new_key


@pytest_asyncio.fixture
async def created_engine(client, admin_headers):
    name = f"test-engine-{os.urandom(4).hex()}"
    version = "1.0.0"
    definition = {
                "type": "test engine",
                "config": {
                    "param1": "value1",
                    "param2": 2
                }
            }

    engine = {
            "name": name,
            "version": version,
            "description": "Test engine",
            "definition": definition,
            "default": True,
            "active": True
        }

    r = await client.post(
        "/v1/admin/engines",
        headers=admin_headers,
        json=engine
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINE_CREATED.value

    return engine

@pytest_asyncio.fixture
async def created_job_with_engine(client, user_headers, created_engine, payload):
    r = await client.post("/v1/jobs", json=payload, headers=user_headers)

    assert r.status_code == 201, r.text

    body = r.json()
    assert body["code"] == AppCode.JOB_CREATED.value
    assert body["status"] == 201

    job = body["data"]

    return {"created_job": job, "engine": created_engine, "payload": payload}

@pytest_asyncio.fixture
async def created_job_with_uploaded_engine(client, user_headers, admin_headers, created_job_with_engine, payload):
    engine = created_job_with_engine["engine"]
    name = engine["name"]
    version = engine["version"]

    r = await client.put(
        f"/v1/admin/engines/{name}/{version}/files",
        headers=admin_headers,
        files={"file": ("engine.zip", VALID_ZIP, "application/zip")},
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINE_FILES_UPLOADED.value

    return created_job_with_engine


@pytest.fixture
def output_storage(monkeypatch, tmp_path):
    for setting in ["JOBS_DIR", "RESULTS_DIR", "ARTIFACTS_DIR", "ENGINES_DIR"]:
        path = tmp_path / setting
        path.mkdir()
        monkeypatch.setattr(config, setting, str(path))
    monkeypatch.setattr(config, "DELETE_JOB_INPUTS_ON_COMPLETION", False)
    monkeypatch.setattr(config, "DELETE_JOB_OUTPUTS", True)
    monkeypatch.setattr(config, "OUTPUT_RETENTION_SECONDS", 604800)
    monkeypatch.setattr(config, "OUTPUT_CLEANUP_TARGETS", {"results", "artifacts"})
    now = datetime(2026, 11, 2, tzinfo=timezone.utc)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    monkeypatch.setattr(cleanup, "datetime", FixedDatetime)
    return now


@pytest_asyncio.fixture
async def jobs_with_output_availability(db_sessions, job_factory, output_factory, other_worker_headers, output_storage, monkeypatch):
    old = output_storage - timedelta(days=8)
    deleted_id, _ = await output_factory(ProcessingState.DONE, old)
    active_id, _ = await output_factory(ProcessingState.DONE, output_storage)
    processing_id, _ = await output_factory(ProcessingState.PROCESSING, old)
    missing_id = await job_factory(ProcessingState.DONE)
    other_id, _ = await output_factory(ProcessingState.DONE, output_storage)
    async with db_sessions.begin() as db:
        other_worker = (await db.execute(select(model.Key).where(model.Key.label == "other-worker"))).scalar_one()
        (await db.get(model.Job, other_id)).owner_key_id = other_worker.id
    monkeypatch.setattr(config, "DELETE_JOB_INPUTS_ON_COMPLETION", True)
    async with db_sessions() as db:
        await cleanup.delete_completed_job_inputs(db)
        await cleanup.delete_expired_job_outputs(db)
    return SimpleNamespace(deleted=deleted_id, active=active_id, processing=processing_id,
                           missing=missing_id, other=other_id)


@pytest_asyncio.fixture
async def job_with_deletion_timestamps(db_sessions, job_factory, output_storage):
    async def create(state):
        job_id = await job_factory(state)
        before = output_storage - timedelta(days=8)
        async with db_sessions.begin() as db:
            job = await db.get(model.Job, job_id)
            for target in ("inputs", "results", "artifacts"):
                setattr(job, f"{target}_deleted_at", before)
            job.meta_json_required = True
            job.alto_required = True
            job.page_required = True
            db.add(model.Image(id=uuid4(), job_id=job_id, name="page.jpg", order=0))
        return job_id, before
    return create


@pytest_asyncio.fixture
async def engine_factory(db_sessions):
    async def create(name="Engine", version="1", **fields):
        engine = model.Engine(id=uuid4(), name=name, version=version,
                              description="Test engine", definition={"type": "test"}, **fields)
        async with db_sessions.begin() as db:
            db.add(engine)
        return engine
    return create
