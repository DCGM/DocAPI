"""Application lifespan and credentials, before any API request is served."""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from doc_api.api import main
from doc_api.api.authentication import issue_key_components, lookup_key, salted_hmac_sha256_hex
from doc_api.api.schemas.base_objects import KeyRole
from doc_api.config import config
from doc_api.db import model


ADMIN_SECRET = "doc-api.startupadminkid.startupadminsecret"


@pytest_asyncio.fixture
async def startup_sessions(prepared_database):
    return prepared_database.sessions


async def key_snapshot(sessions):
    async with sessions() as db:
        keys = (await db.scalars(select(model.Key))).all()
        return {key.label: model.orm2dict(key) for key in keys}


@pytest.mark.asyncio
@pytest.mark.parametrize("production", [False, True], ids=["development", "production"])
@pytest.mark.parametrize("configured_admin", [False, True], ids=["no-admin-setting", "configured-admin"])
async def test_startup_creates_credentials(startup_sessions, monkeypatch, production, configured_admin):
    monkeypatch.setattr(config, "PRODUCTION", production)
    monkeypatch.setattr(config, "ADMIN_KEY", ADMIN_SECRET if configured_admin else None)
    credentials = {} if production else {
        getattr(config, f"TEST_{role.upper()}_KEY_LABEL"): (getattr(config, f"TEST_{role.upper()}_KEY"), KeyRole(role))
        for role in ("admin", "readonly", "user", "worker")
    }
    if configured_admin:
        credentials["admin"] = (ADMIN_SECRET, KeyRole.ADMIN)
    async with main.lifespan(main.app):
        snapshot = await key_snapshot(startup_sessions)
        assert set(snapshot) == set(credentials)
        for label, (secret, role) in credentials.items():
            async with startup_sessions() as db:
                key = await lookup_key(db, secret)
                assert key is not None
                assert key.id == snapshot[label]["id"]
                assert key.role == role
                assert key.active
                assert key.key_hash != secret.split(".")[-1]


@pytest.mark.asyncio
@pytest.mark.parametrize("production", [False, True], ids=["development", "production"])
async def test_startup_preserves_existing_credentials(startup_sessions, monkeypatch, production):
    monkeypatch.setattr(config, "ADMIN_KEY", ADMIN_SECRET)
    async with main.lifespan(main.app):
        pass
    kid, secret, salt = issue_key_components()
    async with startup_sessions.begin() as db:
        db.add(model.Key(kid=kid, salt=salt, key_hash=salted_hmac_sha256_hex(secret, salt),
                         label="custom-user", role=KeyRole.USER, active=True))
        user = await db.scalar(select(model.Key).where(model.Key.label == config.TEST_USER_KEY_LABEL))
        user.role = KeyRole.READONLY
        user.active = False
        user.last_used = datetime(2026, 1, 1, tzinfo=timezone.utc)
        admin = await db.scalar(select(model.Key).where(model.Key.label == "admin"))
        admin.active = False
    before = await key_snapshot(startup_sessions)
    monkeypatch.setattr(config, "PRODUCTION", production)
    # Repeated startup must not regenerate salts, reset roles, reactivate keys,
    # delete development credentials, or add duplicate rows.
    for _ in range(2):
        async with main.lifespan(main.app):
            assert await key_snapshot(startup_sessions) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("production", [False, True], ids=["development", "production"])
@pytest.mark.parametrize("change", ["kid", "secret", "both"])
async def test_startup_rotates_configured_admin(startup_sessions, monkeypatch, production, change):
    monkeypatch.setattr(config, "PRODUCTION", production)
    monkeypatch.setattr(config, "ADMIN_KEY", ADMIN_SECRET)
    async with main.lifespan(main.app):
        pass
    before = await key_snapshot(startup_sessions)
    new_kid = "replacementkid" if change in {"kid", "both"} else "startupadminkid"
    new_secret = "replacementsecret" if change in {"secret", "both"} else "startupadminsecret"
    replacement = f"doc-api.{new_kid}.{new_secret}"
    monkeypatch.setattr(config, "ADMIN_KEY", replacement)
    async with main.lifespan(main.app):
        after = await key_snapshot(startup_sessions)
        assert set(after) == set(before)
        admin = after.pop("admin")
        previous = before.pop("admin")
        assert after == before
        for field in ("id", "created_date", "role", "active"):
            assert admin[field] == previous[field]
        assert admin["kid"] == new_kid
        for field in ("salt", "key_hash"):
            assert (admin[field] != previous[field]) == (change != "kid")
        async with startup_sessions() as db:
            assert await lookup_key(db, ADMIN_SECRET) is None
            key = await lookup_key(db, replacement)
            assert key is not None
            assert key.id == admin["id"]
            assert key.role == KeyRole.ADMIN


@pytest.mark.asyncio
@pytest.mark.parametrize("production", [False, True], ids=["development", "production"])
async def test_startup_without_admin_setting_preserves_admin(startup_sessions, monkeypatch, production):
    monkeypatch.setattr(config, "PRODUCTION", production)
    monkeypatch.setattr(config, "ADMIN_KEY", ADMIN_SECRET)
    async with main.lifespan(main.app):
        pass
    before = await key_snapshot(startup_sessions)
    monkeypatch.setattr(config, "ADMIN_KEY", None)
    async with main.lifespan(main.app):
        assert await key_snapshot(startup_sessions) == before
        async with startup_sessions() as db:
            assert await lookup_key(db, ADMIN_SECRET) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("production", [False, True], ids=["development", "production"])
@pytest.mark.parametrize("setting", ["ADMIN_KEY", "TEST_ADMIN_KEY", "TEST_READONLY_KEY", "TEST_USER_KEY", "TEST_WORKER_KEY"])
async def test_startup_invalid_credentials(startup_sessions, monkeypatch, production, setting):
    monkeypatch.setattr(config, "PRODUCTION", production)
    monkeypatch.setattr(config, setting, "malformed-key")
    ignored = production and setting != "ADMIN_KEY"
    monkeypatch.setattr(config, "DELETE_JOB_INPUTS_ON_COMPLETION", not ignored)
    cleanup = AsyncMock()
    monkeypatch.setattr(main, "run_cleanup", cleanup)
    if ignored:
        async with main.lifespan(main.app):
            assert await key_snapshot(startup_sessions) == {}
    else:
        with pytest.raises(ValueError, match="Invalid API key format"):
            async with main.lifespan(main.app):
                pytest.fail("Startup failure must prevent entering the serving lifespan")
        assert await key_snapshot(startup_sessions) == {}
    cleanup.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["admin", "test-keys"])
@pytest.mark.parametrize("operation", ["execute", "commit"])
async def test_startup_database_failure_rolls_back(startup_sessions, monkeypatch, phase, operation):
    monkeypatch.setattr(config, "PRODUCTION", phase == "admin")
    monkeypatch.setattr(config, "ADMIN_KEY", ADMIN_SECRET if phase == "admin" else None)
    monkeypatch.setattr(config, "DELETE_JOB_INPUTS_ON_COMPLETION", True)
    cleanup = AsyncMock()
    monkeypatch.setattr(main, "run_cleanup", cleanup)

    @asynccontextmanager
    async def failing_session():
        async with startup_sessions() as db:
            monkeypatch.setattr(db, operation, AsyncMock(side_effect=SQLAlchemyError("injected startup DB failure")))
            yield db

    monkeypatch.setattr(main, "open_session", failing_session)
    with pytest.raises(SQLAlchemyError, match="injected startup DB failure"):
        async with main.lifespan(main.app):
            pytest.fail("Startup failure must prevent entering the serving lifespan")
    assert await key_snapshot(startup_sessions) == {}
    cleanup.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_role", ["admin", "readonly", "user", "worker"])
async def test_startup_creates_only_missing_test_credentials(startup_sessions, missing_role):
    async with main.lifespan(main.app):
        pass
    before = await key_snapshot(startup_sessions)
    label = getattr(config, f"TEST_{missing_role.upper()}_KEY_LABEL")
    async with startup_sessions.begin() as db:
        key = await db.scalar(select(model.Key).where(model.Key.label == label))
        await db.delete(key)
    async with main.lifespan(main.app):
        after = await key_snapshot(startup_sessions)
        assert set(after) == set(before)
        assert after[label]["id"] != before[label]["id"]
        assert after[label]["role"] == KeyRole(missing_role)
        assert after[label]["active"]
        async with startup_sessions() as db:
            assert await lookup_key(db, getattr(config, f"TEST_{missing_role.upper()}_KEY")) is not None
        after.pop(label)
        before.pop(label)
        assert after == before


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["admin", "test-keys"])
async def test_startup_key_conflict_rolls_back(startup_sessions, monkeypatch, phase):
    monkeypatch.setattr(config, "PRODUCTION", phase == "admin")
    monkeypatch.setattr(config, "ADMIN_KEY", ADMIN_SECRET if phase == "admin" else None)
    monkeypatch.setattr(config, "DELETE_JOB_OUTPUTS", True)
    kid, secret, salt = issue_key_components()
    # Configured admin collides by kid; development credentials collide by label.
    existing = model.Key(kid="startupadminkid" if phase == "admin" else kid,
                         label="custom-key" if phase == "admin" else config.TEST_USER_KEY_LABEL,
                         salt=salt, key_hash=salted_hmac_sha256_hex(secret, salt),
                         role=KeyRole.USER, active=True)
    async with startup_sessions.begin() as db:
        db.add(existing)
    before = await key_snapshot(startup_sessions)
    cleanup = AsyncMock()
    monkeypatch.setattr(main, "run_cleanup", cleanup)
    with pytest.raises(IntegrityError):
        async with main.lifespan(main.app):
            pytest.fail("Conflicting credentials must prevent serving")
    assert await key_snapshot(startup_sessions) == before
    cleanup.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("inputs,outputs", [(False, False), (True, False), (False, True), (True, True)])
async def test_cleanup_task_lifecycle(monkeypatch, test_settings, inputs, outputs):
    enabled = inputs or outputs
    monkeypatch.setattr(config, "ADMIN_KEY", None)
    monkeypatch.setattr(config, "PRODUCTION", True)
    monkeypatch.setattr(config, "DELETE_JOB_INPUTS_ON_COMPLETION", inputs)
    monkeypatch.setattr(config, "DELETE_JOB_OUTPUTS", outputs)
    started = asyncio.Event()
    stopped = asyncio.Event()

    async def cleanup_task():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    monkeypatch.setattr(main, "run_cleanup", cleanup_task)
    async with main.lifespan(main.app):
        if enabled:
            await asyncio.wait_for(started.wait(), timeout=5)
        else:
            await asyncio.sleep(0)
            assert not started.is_set()
    assert stopped.is_set() is enabled


@pytest.mark.asyncio
@pytest.mark.parametrize("checker", ["run_job_readiness_checks", "run_job_lease_checks", "run_job_mail_checks"])
async def test_background_checker_lifecycle(monkeypatch, startup_sessions, checker):
    started = asyncio.Event()
    stopped = asyncio.Event()

    async def readiness_task():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    monkeypatch.setattr(main, checker, readiness_task)
    async with main.lifespan(main.app):
        await asyncio.wait_for(started.wait(), timeout=5)
    assert stopped.is_set()


@pytest.mark.asyncio
async def test_readiness_recovers_jobs_at_startup(monkeypatch, startup_sessions):
    from doc_api.api.checkers import job_readiness
    from doc_api.api.schemas.base_objects import ProcessingState

    async with main.lifespan(main.app):
        async with startup_sessions.begin() as db:
            owner = await db.scalar(select(model.Key).where(model.Key.label == config.TEST_USER_KEY_LABEL))
            job = model.Job(owner_key_id=owner.id, definition={"images": []}, state=ProcessingState.NEW)
            db.add(job)
            await db.flush()
            job_id = job.id

    monkeypatch.setattr(main, "run_job_readiness_checks", job_readiness.run_job_readiness_checks)
    monkeypatch.setattr(config, "JOB_READINESS_CHECK_INTERVAL_SECONDS", 3600)
    async def wait_for_queue():
        while True:
            async with startup_sessions() as db:
                if (await db.get(model.Job, job_id)).state == ProcessingState.QUEUED:
                    return
            await asyncio.sleep(0.01)
    async with main.lifespan(main.app):
        await asyncio.wait_for(wait_for_queue(), timeout=5)


@pytest.mark.asyncio
@pytest.mark.parametrize("overflow", [0, 1])
async def test_database_pool_limits_and_recovers_after_exhaustion(isolated_database, monkeypatch, overflow):
    from sqlalchemy import text
    from sqlalchemy.exc import TimeoutError as PoolTimeoutError
    from sqlalchemy.ext.asyncio import create_async_engine
    from doc_api.api import database

    monkeypatch.setattr(config, "DATABASE_POOL_SIZE", 1)
    monkeypatch.setattr(config, "DATABASE_MAX_OVERFLOW", overflow)
    monkeypatch.setattr(config, "DATABASE_POOL_TIMEOUT_SECONDS", 0.05)
    engine = create_async_engine(isolated_database.engine.url, **database._engine_kwargs())
    connections = []
    try:
        for _ in range(1 + overflow):
            connections.append(await engine.connect())
        with pytest.raises(PoolTimeoutError):
            async with engine.connect():
                pytest.fail("An exhausted pool must wait and time out")
        await connections.pop().close()
        async with engine.connect() as connection:
            assert await connection.scalar(text("SELECT 1")) == 1
    finally:
        for connection in connections:
            await connection.close()
        await engine.dispose()
