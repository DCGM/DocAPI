"""Periodic readiness checks, independent of individual input requests."""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from doc_api.api import job_readiness
from doc_api.api.schemas.base_objects import ProcessingState
from doc_api.config import Config, config
from doc_api.db import model


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [None, "image", "alto", "page", "metadata"])
async def test_queue_ready_jobs_required_inputs(db_sessions, job_factory, missing):
    job_id = await job_factory(ProcessingState.NEW)
    old = datetime(2020, 1, 1, tzinfo=timezone.utc)
    async with db_sessions.begin() as db:
        job = await db.get(model.Job, job_id)
        engine = model.Engine(id=uuid4(), name="readiness", version="1", description="Test engine", last_used=old)
        db.add(engine)
        await db.flush()
        job.engine_id = engine.id
        job.last_change = old
        job.alto_required = job.page_required = job.meta_json_required = True
        job.meta_json_uploaded = missing != "metadata"
        db.add(model.Image(job_id=job_id, name="page.jpg", order=0,
                           image_uploaded=missing != "image", alto_uploaded=missing != "alto",
                           page_uploaded=missing != "page"))
        engine_id = engine.id
    async with db_sessions() as db:
        await job_readiness.queue_ready_jobs(db)
    async with db_sessions() as db:
        job = await db.get(model.Job, job_id)
        engine = await db.get(model.Engine, engine_id)
        assert job.state == (ProcessingState.NEW if missing else ProcessingState.QUEUED)
        assert (job.last_change == old) == bool(missing)
        assert (engine.last_used == old) == bool(missing)
        changed = job.last_change
    async with db_sessions() as db:
        await job_readiness.queue_ready_jobs(db)
    async with db_sessions() as db:
        assert (await db.get(model.Job, job_id)).last_change == changed


@pytest.mark.asyncio
@pytest.mark.parametrize("state", list(ProcessingState))
async def test_queue_ready_jobs_only_new(db_sessions, job_factory, state):
    job_id = await job_factory(state)
    async with db_sessions() as db:
        await job_readiness.queue_ready_jobs(db)
    async with db_sessions() as db:
        assert (await db.get(model.Job, job_id)).state == (
            ProcessingState.QUEUED if state == ProcessingState.NEW else state)


@pytest.mark.asyncio
async def test_queue_ready_jobs_skips_locked(db_sessions, job_factory):
    from sqlalchemy import select
    locked_id = await job_factory(ProcessingState.NEW)
    ready_id = await job_factory(ProcessingState.NEW)
    async with db_sessions.begin() as locked:
        await locked.execute(select(model.Job).where(model.Job.id == locked_id).with_for_update())
        async with db_sessions() as db:
            await asyncio.wait_for(job_readiness.queue_ready_jobs(db), timeout=5)
        async with db_sessions() as db:
            assert (await db.get(model.Job, locked_id)).state == ProcessingState.NEW
            assert (await db.get(model.Job, ready_id)).state == ProcessingState.QUEUED
    async with db_sessions() as db:
        await job_readiness.queue_ready_jobs(db)
    async with db_sessions() as db:
        assert (await db.get(model.Job, locked_id)).state == ProcessingState.QUEUED


@pytest.mark.asyncio
async def test_queue_ready_jobs_batches(db_sessions, reader_owner):
    from sqlalchemy import func, select
    async with db_sessions.begin() as db:
        db.add_all([model.Job(owner_key_id=reader_owner.id, definition={"images": []},
                              state=ProcessingState.NEW) for _ in range(105)])
    async with db_sessions() as db:
        await job_readiness.queue_ready_jobs(db)
    async with db_sessions() as db:
        assert await db.scalar(select(func.count()).select_from(model.Job).where(
            model.Job.state == ProcessingState.QUEUED)) == 105


@pytest.mark.asyncio
async def test_readiness_loop_interval_and_retry(monkeypatch):
    calls = []
    @asynccontextmanager
    async def session():
        yield object()

    check = AsyncMock(side_effect=[RuntimeError("temporary failure"), None])
    async def sleep(interval):
        calls.append(interval)
        if len(calls) == 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(job_readiness, "open_session", session)
    monkeypatch.setattr(job_readiness, "queue_ready_jobs", check)
    monkeypatch.setattr(job_readiness.asyncio, "sleep", sleep)
    monkeypatch.setattr(config, "JOB_READINESS_CHECK_INTERVAL_SECONDS", 7)
    with pytest.raises(asyncio.CancelledError):
        await job_readiness.run_job_readiness_checks()
    assert check.await_count == 2
    assert calls == [7, 7]


@pytest.mark.parametrize("value,expected", [(None, 5), ("2", 2), ("0", None), ("-1", None), ("bad", None)])
def test_readiness_interval_configuration(monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv("JOB_READINESS_CHECK_INTERVAL_SECONDS", raising=False)
    else:
        monkeypatch.setenv("JOB_READINESS_CHECK_INTERVAL_SECONDS", value)
    if expected is None:
        with pytest.raises(ValueError):
            Config()
    else:
        assert Config().JOB_READINESS_CHECK_INTERVAL_SECONDS == expected
