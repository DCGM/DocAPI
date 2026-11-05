"""Periodic lease recovery independent of worker lease requests."""
import asyncio
from contextlib import suppress, asynccontextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from doc_api.api.checkers import job_leases
from doc_api.api.schemas.base_objects import ProcessingState
from doc_api.config import Config, config
from doc_api.db import model
from doc_api.tests.helpers import run_lease_check


@pytest.mark.asyncio
@pytest.mark.parametrize('state,exhausted,expected', [
    (ProcessingState.ERROR, False, ProcessingState.QUEUED),
    (ProcessingState.ERROR, True, ProcessingState.FAILED),
    (ProcessingState.PROCESSING, False, ProcessingState.QUEUED),
    (ProcessingState.PROCESSING, True, ProcessingState.FAILED),
    (ProcessingState.DONE, False, ProcessingState.DONE),
    (ProcessingState.FAILED, False, ProcessingState.FAILED),
    (ProcessingState.CANCELLED, False, ProcessingState.CANCELLED),
])
async def test_lease_recovery_without_requests(job_factory, db_sessions, state, exhausted, expected):
    job_id = await job_factory(state)
    old = datetime.now(timezone.utc) - timedelta(days=1)
    async with db_sessions.begin() as db:
        job = await db.get(model.Job, job_id)
        job.previous_attempts = config.JOB_MAX_ATTEMPTS - 1 if exhausted else 0
        job.last_change = old
        original_logs = job.log, job.log_user
    await run_lease_check(db_sessions)
    async with db_sessions() as db:
        job = await db.get(model.Job, job_id)
        assert job.state == expected
        assert (job.log, job.log_user) == original_logs
        assert job.previous_attempts == (config.JOB_MAX_ATTEMPTS - 1 if exhausted else 0)
        if expected == ProcessingState.QUEUED:
            assert job.worker_key_id is None and job.progress == 0
        if exhausted:
            assert job.finished_date is not None and job.progress == 1
    await run_lease_check(db_sessions)


@pytest.mark.asyncio
async def test_lease_recovery_skips_locked_jobs(job_factory, db_sessions):
    job_id = await job_factory(ProcessingState.ERROR)
    async with db_sessions() as locking:
        async with locking.begin():
            await locking.get(model.Job, job_id, with_for_update=True)
            await asyncio.wait_for(run_lease_check(db_sessions), timeout=2)
            assert (await locking.get(model.Job, job_id)).state == ProcessingState.ERROR
    await run_lease_check(db_sessions)
    async with db_sessions() as db:
        assert (await db.get(model.Job, job_id)).state == ProcessingState.QUEUED


@pytest.mark.asyncio
async def test_lease_checker_runs_periodically(job_factory, db_sessions, monkeypatch):
    job_id = await job_factory(ProcessingState.PROCESSING)
    monkeypatch.setattr(config, 'JOB_LEASE_CHECK_INTERVAL_SECONDS', 0.01)
    task = asyncio.create_task(job_leases.run_job_lease_checks())
    try:
        await asyncio.sleep(0.02)
        async with db_sessions.begin() as db:
            (await db.get(model.Job, job_id)).last_change = datetime.now(timezone.utc) - timedelta(days=1)
        async def wait_for_queue():
            while True:
                async with db_sessions() as db:
                    if (await db.get(model.Job, job_id)).state == ProcessingState.QUEUED:
                        return
                await asyncio.sleep(0.01)
        await asyncio.wait_for(wait_for_queue(), timeout=5)
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_lease_checker_recovers_after_failure(monkeypatch, caplog):
    @asynccontextmanager
    async def session():
        yield None
    check = AsyncMock(side_effect=[OSError('temporary database failure'), None])
    sleep = AsyncMock(side_effect=[None, asyncio.CancelledError()])
    monkeypatch.setattr(job_leases, 'open_session', session)
    monkeypatch.setattr(job_leases, 'check_job_leases', check)
    monkeypatch.setattr(job_leases, 'asyncio', SimpleNamespace(sleep=sleep))
    monkeypatch.setattr(config, 'JOB_LEASE_CHECK_INTERVAL_SECONDS', 7)
    with pytest.raises(asyncio.CancelledError):
        await job_leases.run_job_lease_checks()
    assert check.await_count == 2
    assert [call.args for call in sleep.await_args_list] == [(7,), (7,)]
    assert 'Job lease check failed' in caplog.text


@pytest.mark.parametrize('value,expected', [(None, 0), ('0', 0), ('300', 300), ('-1', None), ('inf', None)])
def test_lease_timeout_configuration(monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv('JOB_TIMEOUT_SECONDS', raising=False)
    else:
        monkeypatch.setenv('JOB_TIMEOUT_SECONDS', value)
    if expected is None:
        with pytest.raises(ValueError):
            Config()
    else:
        assert Config().JOB_TIMEOUT_SECONDS == expected


@pytest.mark.asyncio
@pytest.mark.parametrize('state,exhausted', [
    (ProcessingState.PROCESSING, False), (ProcessingState.PROCESSING, True),
    (ProcessingState.ERROR, False), (ProcessingState.ERROR, True),
])
async def test_non_expiring_leases_recover_only_errors(job_factory, db_sessions, monkeypatch, state, exhausted):
    monkeypatch.setattr(config, 'JOB_TIMEOUT_SECONDS', 0)
    job_id = await job_factory(state)
    old = datetime(2000, 1, 1, tzinfo=timezone.utc)
    async with db_sessions.begin() as db:
        job = await db.get(model.Job, job_id)
        job.last_change = old
        job.previous_attempts = config.JOB_MAX_ATTEMPTS - 1 if exhausted else 0
        worker_id = job.worker_key_id
    await run_lease_check(db_sessions)
    async with db_sessions() as db:
        job = await db.get(model.Job, job_id)
        if state == ProcessingState.PROCESSING:
            assert job.state == state and job.last_change == old
            assert job.worker_key_id == worker_id
        else:
            assert job.state == (ProcessingState.FAILED if exhausted else ProcessingState.QUEUED)
