from doc_api.api.checkers.job_mail import check_job_notifications
from doc_api.tests.helpers import run_lease_check
from doc_api.api.checkers import job_leases
from unittest.mock import AsyncMock
from doc_api.tests.helpers import assert_route_invariant, delete_job_after_exists, delete_before_file_install, delete_job_before_crud
from doc_api.api import file_saving as upload_helpers
from starlette.datastructures import UploadFile
import asyncio
import logging
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone

from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSessionTransaction

from doc_api.api.routes import worker_routes
from doc_api.api import main
from doc_api.tests.helpers import (assert_generated_zip_save, assert_multipart_rejected, assert_worker_update_race,
                                   interrupted_upload_body, interrupted_upload_headers,
                                   assert_internal_transfer_error, assert_records_unchanged,
                                   interrupt_download, inject_zip_upload_failure)

from doc_api.config import config
from doc_api.api.cruds import worker_cruds
from doc_api.api.schemas.base_objects import ProcessingState, JobProgressUpdate
from doc_api.db import model
from doc_api.tests.helpers import assert_deletion_timestamps
from doc_api.api.schemas import base_objects
from doc_api.api.schemas.responses import AppCode
from doc_api.tests.dummy_data import JOB_DEFINITION_PAYLOADS, VALID_ZIP


logger = logging.getLogger(__name__)


#
# POST /v1/jobs/lease - 200
#

@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_LEASED], indirect=True)
async def test_post_job_lease_200_job_leased(client, worker_headers, lease_job):
    job = lease_job["created_job"]
    lease = lease_job["lease"]

    assert lease["id"] == job["id"]
    assert "lease_expire_at" in lease
    assert "server_time" in lease

    assert job["id"] is not None

    r = await client.get(
        f"/v1/jobs/{job['id']}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    job_after_lease = body["data"]
    assert job_after_lease["state"] == base_objects.ProcessingState.PROCESSING.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_LEASED], indirect=True)
async def test_post_job_lease_200_job_leased_with_engine(client, worker_headers, admin_headers, lease_job_with_engine):
    job = lease_job_with_engine["created_job"]
    engine = lease_job_with_engine["engine"]
    lease = lease_job_with_engine["lease"]

    assert lease["id"] == job["id"]
    assert "lease_expire_at" in lease
    assert "server_time" in lease

    assert job["id"] is not None

    r = await client.get(
        f"/v1/jobs/{job['id']}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    job_after_lease = body["data"]
    assert job_after_lease["state"] == base_objects.ProcessingState.PROCESSING.value

    r = await client.get(
        f"/v1/engines",
        headers=admin_headers,
        params={"name": engine["name"],
                "version": engine["version"]}
    )

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.ENGINES_RETRIEVED.value
    engines = body["data"]
    assert len(engines) == 1
    assert engines[0]["last_used"] is not None


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[AppCode.JOB_QUEUE_EMPTY])
async def test_post_job_lease_200_queue_empty(client, worker_headers, dummy):
    r = await client.post(
        "/v1/jobs/lease",
        headers=worker_headers
    )
    assert r.status_code == 200, r.text

    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_QUEUE_EMPTY.value
    assert body["data"] is None


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("offset", [-1, 0, 1])
@pytest.mark.parametrize("exhausted", [False, True])
async def test_post_job_lease_200_timeout_boundaries(client, worker_headers, job_factory, db_sessions, monkeypatch, offset, exhausted):
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now
    monkeypatch.setattr(worker_cruds, "datetime", FixedDatetime)
    monkeypatch.setattr(job_leases, "datetime", FixedDatetime)
    job_id = await job_factory(ProcessingState.PROCESSING)
    async with db_sessions.begin() as db:
        job = await db.get(model.Job, job_id)
        job.last_change = now - timedelta(seconds=config.JOB_TIMEOUT_SECONDS + config.JOB_TIMEOUT_GRACE_SECONDS) + timedelta(seconds=offset)
        job.previous_attempts = config.JOB_MAX_ATTEMPTS - 1 if exhausted else 0
    await run_lease_check(db_sessions)
    response = await client.post("/v1/jobs/lease", headers=worker_headers)
    assert response.status_code == 200, response.text
    leased = offset < 0 and not exhausted
    assert response.json()["code"] == (AppCode.JOB_LEASED if leased else AppCode.JOB_QUEUE_EMPTY).value
    async with db_sessions() as db:
        job = await db.get(model.Job, job_id)
        assert job.state == (ProcessingState.FAILED if offset < 0 and exhausted else ProcessingState.PROCESSING)
        if offset < 0 and exhausted:
            assert job.finished_date == now
            assert job.progress == 1.0


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_post_job_lease_200_concurrent_assignment(client, worker_headers, job_factory, db_sessions):
    job_id = await job_factory(ProcessingState.QUEUED)
    async with db_sessions.begin() as db:
        (await db.get(model.Job, job_id)).previous_attempts = None
    responses = await asyncio.gather(*(client.post("/v1/jobs/lease", headers=worker_headers) for _ in range(2)))
    assert all(r.status_code == 200 for r in responses)
    assert sorted(r.json()["code"] for r in responses) == sorted([AppCode.JOB_LEASED.value, AppCode.JOB_QUEUE_EMPTY.value])
    leased = next(r.json()["data"] for r in responses if r.json()["code"] == AppCode.JOB_LEASED.value)
    assert leased["id"] == str(job_id)
    async with db_sessions() as db:
        assert (await db.get(model.Job, job_id)).previous_attempts == 0



@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_post_job_lease_200_many_workers_and_retries(client, worker_pool, job_factory, db_sessions, monkeypatch):
    monkeypatch.setattr(config, "JOB_MAX_ATTEMPTS", 3)
    old = datetime.now(timezone.utc) - timedelta(days=1)
    expected = {}
    for state, count, attempts in [(ProcessingState.QUEUED, 16, None), (ProcessingState.ERROR, 8, 0),
                                    (ProcessingState.PROCESSING, 8, 1), (ProcessingState.ERROR, 4, 2),
                                    (ProcessingState.PROCESSING, 4, 2), (ProcessingState.DONE, 2, 0),
                                    (ProcessingState.FAILED, 2, 0), (ProcessingState.CANCELLED, 2, 0)]:
        for _ in range(count):
            job_id = await job_factory(state)
            expected[job_id] = (state, attempts)
    async with db_sessions.begin() as db:
        for job in await db.scalars(select(model.Job)):
            job.previous_attempts = expected[job.id][1]
            job.last_change = old
            if job.state != ProcessingState.QUEUED:
                job.started_date = old
    await run_lease_check(db_sessions)
    gate = asyncio.Event()

    async def lease(worker):
        await gate.wait()
        response = await client.post("/v1/jobs/lease", headers=worker.headers)
        assert response.status_code == 200, response.text
        return worker, response.json()

    # More requests than available connections/jobs exercise pool queuing and
    # concurrent claims after recovery without assuming a particular scheduling order.
    tasks = [asyncio.create_task(lease(worker)) for _ in range(12) for worker in worker_pool]
    try:
        gate.set()
        results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=60)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    # SKIP LOCKED can report an empty queue while another claim holds queued
    # rows. Poll after each concurrent wave has finished as workers do.
    for _ in range(8):
        if sum(body["code"] == AppCode.JOB_LEASED.value for _, body in results) == 32:
            break
        results.extend(await asyncio.wait_for(asyncio.gather(*(lease(worker) for worker in worker_pool)), timeout=30))
    leased = [(worker, UUID(body["data"]["id"])) for worker, body in results
              if body["code"] == AppCode.JOB_LEASED.value]
    assert len(leased) == 32
    assert len({job_id for _, job_id in leased}) == 32, "A job was assigned more than once"
    assert sum(body["code"] == AppCode.JOB_QUEUE_EMPTY.value for _, body in results) >= 16
    assignments = {job_id: worker.id for worker, job_id in leased}
    async with db_sessions() as db:
        for job in await db.scalars(select(model.Job)):
            state, attempts = expected[job.id]
            if job.id in assignments:
                assert job.state == ProcessingState.PROCESSING
                assert job.worker_key_id == assignments[job.id]
                assert job.previous_attempts == (0 if attempts is None else attempts + 1)
                assert job.last_change > old
                if state != ProcessingState.QUEUED:
                    assert job.started_date == old
                    assert job.progress == 0.0
                    assert "".join(entry or "" for entry in (job.log or [])).count("ATTEMPT ") == 1
                    assert job.log_user == ["User log"] + [None] * job.previous_attempts
            elif state in {ProcessingState.ERROR, ProcessingState.PROCESSING}:
                assert job.state == ProcessingState.FAILED
                assert job.previous_attempts == 2
                assert job.finished_date > old and job.progress == 1.0
            else:
                assert job.state == state
                assert job.previous_attempts == attempts
    response = await client.post("/v1/jobs/lease", headers=worker_pool[0].headers)
    assert response.json()["code"] == AppCode.JOB_QUEUE_EMPTY.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_post_job_lease_cancelled_before_commit(client, worker_headers, job_factory, db_sessions, monkeypatch):
    job_id = await job_factory(ProcessingState.QUEUED)
    async with db_sessions.begin() as db:
        (await db.get(model.Job, job_id)).previous_attempts = None
    entered, release = asyncio.Event(), asyncio.Event()
    original_exit = AsyncSessionTransaction.__aexit__

    async def exit_transaction(transaction, exc_type, exc, traceback):
        if exc_type is None and any(isinstance(row, model.Job) and row.id == job_id
                                    and row.state == ProcessingState.PROCESSING
                                    for row in transaction.session.dirty):
            entered.set()
            try:
                await release.wait()
            except BaseException as error:
                await original_exit(transaction, type(error), error, error.__traceback__)
                raise
        return await original_exit(transaction, exc_type, exc, traceback)

    monkeypatch.setattr(AsyncSessionTransaction, "__aexit__", exit_transaction)
    task = asyncio.create_task(client.post("/v1/jobs/lease", headers=worker_headers))
    try:
        await asyncio.wait_for(entered.wait(), timeout=10)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    async with db_sessions() as db:
        job = await db.get(model.Job, job_id)
        assert job.state == ProcessingState.QUEUED
        assert job.previous_attempts is None
    # A new request uses the same credentials after the cancelled connection.
    retry = await client.post("/v1/jobs/lease", headers=worker_headers)
    assert retry.status_code == 200, retry.text
    assert retry.json()["data"]["id"] == str(job_id)
    async with db_sessions() as db:
        assert (await db.get(model.Job, job_id)).previous_attempts == 0


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("interruption", ["cancellation", "disconnect"])
@pytest.mark.asyncio
async def test_post_job_lease_200_lost_response(client, worker_headers, worker_pool, job_factory,
                                              db_sessions, monkeypatch, interruption):
    jobs = {await job_factory(ProcessingState.QUEUED) for _ in range(2)}
    async with db_sessions.begin() as db:
        for job in await db.scalars(select(model.Job)):
            job.previous_attempts = None
    committed, release = asyncio.Event(), asyncio.Event()
    original_lease = worker_cruds.lease_job_to_worker
    lost_id = None

    async def lease(**kwargs):
        nonlocal lost_id
        result = await original_lease(**kwargs)
        if not committed.is_set():
            lost_id = result[0].id
            committed.set()
            if interruption == "cancellation":
                await release.wait()
        return result

    monkeypatch.setattr(worker_cruds, "lease_job_to_worker", lease)
    if interruption == "cancellation":
        task = asyncio.create_task(client.post("/v1/jobs/lease", headers=worker_headers))
        try:
            await asyncio.wait_for(committed.wait(), timeout=10)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            release.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    else:
        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(message):
            if message["type"] == "http.response.start":
                assert message["status"] == 200
                raise OSError("client disconnected before receiving lease")

        url = "/v1/jobs/lease"
        scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"},
                 "http_version": "1.1", "method": "POST", "scheme": "http", "path": url,
                 "raw_path": url.encode(), "root_path": "", "query_string": b"",
                 "headers": [(key.lower().encode(), value.encode()) for key, value in worker_headers.items()],
                 "server": ("test", 80), "client": ("127.0.0.1", 1234)}
        with pytest.raises(OSError, match="client disconnected"):
            await main.app(scope, receive, send)
    assert lost_id in jobs
    async with db_sessions() as db:
        job = await db.get(model.Job, lost_id)
        assert job.state == ProcessingState.PROCESSING and job.previous_attempts == 0
    # Retrying POST creates another lease; it cannot undo a committed assignment
    # whose response was lost. The first lease is recoverable by release/timeout.
    retry = await client.post("/v1/jobs/lease", headers=worker_headers)
    assert retry.status_code == 200, retry.text
    assert UUID(retry.json()["data"]["id"]) == (jobs - {lost_id}).pop()
    empty = await client.post("/v1/jobs/lease", headers=worker_pool[0].headers)
    assert empty.json()["code"] == AppCode.JOB_QUEUE_EMPTY.value
    released = await client.delete(f"/v1/jobs/{lost_id}/lease", headers=worker_headers)
    assert released.status_code == 204, released.text
    recovered = await client.post("/v1/jobs/lease", headers=worker_pool[0].headers)
    assert recovered.json()["data"]["id"] == str(lost_id)
    async with db_sessions() as db:
        job = await db.get(model.Job, lost_id)
        assert job.worker_key_id == worker_pool[0].id
        assert job.previous_attempts == 0




@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_post_job_lease_500_unexpected_crud_result(client, worker_headers, job_factory, monkeypatch, invariant_errors):
    job_id = await job_factory(ProcessingState.PROCESSING)
    code = AppCode.API_KEY_VALID
    monkeypatch.setattr(worker_cruds, "lease_job_to_worker", AsyncMock(return_value=(None, None, None, code)))
    response = await client.request("POST", f"/v1/jobs/lease", headers=worker_headers)
    assert_route_invariant(response, invariant_errors, code)


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_post_job_lease_200_non_expiring(client, worker_headers, job_factory, db_sessions, monkeypatch):
    from doc_api.api.schemas.base_objects import JobLease
    monkeypatch.setattr(config, "JOB_TIMEOUT_SECONDS", 0)
    job_id = await job_factory(ProcessingState.QUEUED)
    requests = [
        ("POST", "/v1/jobs/lease", {}, AppCode.JOB_LEASED),
        ("PATCH", f"/v1/jobs/{job_id}/lease", {}, AppCode.JOB_LEASE_EXTENDED),
        ("PATCH", f"/v1/jobs/{job_id}", {"json": {"progress": 0.5, "log": "still working"}}, AppCode.JOB_UPDATED),
    ]
    for method, url, kwargs, code in requests:
        response = await client.request(method, url, headers=worker_headers, **kwargs)
        assert response.status_code == 200, response.text
        assert response.json()["code"] == code.value
        lease = JobLease.model_validate(response.json()["data"])
        assert lease.id == job_id and lease.lease_expire_at is None
        assert lease.server_time.tzinfo is not None
    async with db_sessions() as db:
        job = await db.get(model.Job, job_id)
        assert job.progress == 0.5
        assert job.log[job.previous_attempts].endswith("still working")


#
# PATCH /v1/jobs/{job_id}/lease - 200, 403, 404, 409, 422
#

@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_LEASE_EXTENDED], indirect=True)
async def test_patch_job_lease_extend_200(client, worker_headers, lease_job, payload):
    job = lease_job["created_job"]
    lease = lease_job["lease"]

    r = await client.patch(
        f"/v1/jobs/{job['id']}/lease",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text

    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_LEASE_EXTENDED.value

    extended_lease = body["data"]
    assert extended_lease["id"] == lease["id"]
    assert extended_lease["lease_expire_at"] > lease["lease_expire_at"], "Lease expiration time should be extended"
    assert extended_lease["server_time"] > lease["server_time"], "Server time should be updated"


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_patch_job_lease_404_job_not_found(client, worker_headers):
    job_id = uuid4()
    response = await client.request('PATCH', f'/v1/jobs/{job_id}/lease', headers=worker_headers)
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


@pytest.mark.asyncio
async def test_patch_job_lease_403_job_access_forbidden(client, worker_headers, job_factory, other_worker_headers):
    job_id = await job_factory(base_objects.ProcessingState.PROCESSING)
    response = await client.request('PATCH', f'/v1/jobs/{job_id}/lease', headers=other_worker_headers)
    assert response.status_code == 403, response.text
    assert response.json()["code"] == AppCode.API_KEY_FORBIDDEN_FOR_JOB.value


@pytest.mark.asyncio
@pytest.mark.parametrize("state", [state for state in base_objects.ProcessingState if state != base_objects.ProcessingState.PROCESSING])
async def test_patch_job_lease_409_job_not_processing(client, worker_headers, job_factory, state):
    job_id = await job_factory(state)
    response = await client.request('PATCH', f'/v1/jobs/{job_id}/lease', headers=worker_headers)
    assert response.status_code == 409, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_IN_PROCESSING.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_patch_job_lease_422_invalid_request(client, worker_headers):
    response = await client.request('PATCH', '/v1/jobs/not-a-uuid/lease', headers=worker_headers)
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value



@pytest.mark.asyncio
async def test_patch_job_lease_403_reassignment_during_request(client, worker_headers, other_worker_headers,
                                                        user_headers, job_factory, db_sessions, monkeypatch):
    await assert_worker_update_race(client, worker_headers, other_worker_headers, user_headers,
                                    job_factory, db_sessions, monkeypatch,
                                    operation='extend', concurrent_change="reassignment")



@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_patch_job_lease_409_cancellation_during_request(client, worker_headers, other_worker_headers,
                                                        user_headers, job_factory, db_sessions, monkeypatch):
    await assert_worker_update_race(client, worker_headers, other_worker_headers, user_headers,
                                    job_factory, db_sessions, monkeypatch,
                                    operation='extend', concurrent_change="cancellation")




@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_patch_job_lease_500_unexpected_crud_result(client, worker_headers, job_factory, monkeypatch, invariant_errors):
    job_id = await job_factory(ProcessingState.PROCESSING)
    code = AppCode.API_KEY_VALID
    monkeypatch.setattr(worker_cruds, "update_processing_job_lease", AsyncMock(return_value=(None, None, code)))
    response = await client.request("PATCH", f"/v1/jobs/{job_id}/lease", headers=worker_headers)
    assert_route_invariant(response, invariant_errors, code)




@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_patch_job_lease_404_disappeared_after_check(client, worker_headers, job_factory, db_sessions, monkeypatch):
    job_id = await job_factory(ProcessingState.PROCESSING)
    delete_job_after_exists(monkeypatch, db_sessions, job_id)
    response = await client.request("PATCH", f"/v1/jobs/{job_id}/lease", headers=worker_headers)
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value



@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_patch_job_lease_404_disappeared_before_update(client, worker_headers, job_factory, db_sessions, monkeypatch):
    job_id = await job_factory(ProcessingState.PROCESSING)
    delete_job_before_crud(monkeypatch, db_sessions, job_id, worker_cruds, "update_processing_job_lease")
    response = await client.request("PATCH", f"/v1/jobs/{job_id}/lease", headers=worker_headers)
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


#
# DELETE /v1/jobs/{job_id}/lease - 204, 403, 404, 409, 422
#

@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_LEASE_RELEASED], indirect=True)
async def test_delete_job_lease_204(client, worker_headers, worker_role, user_headers, lease_job, payload):
    job = lease_job["created_job"]

    r = await client.delete(
        f"/v1/jobs/{job['id']}/lease",
        headers=worker_headers,
    )
    assert r.status_code == 204, r.text

    assert r.text == ""

    r = await client.get(
        f"/v1/jobs/{job['id']}",
        headers=user_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    job_after_release = body["data"]
    assert job_after_release["state"] == base_objects.ProcessingState.QUEUED.value

    # Releasing a lease revokes worker access; admins retain access.
    r = await client.get(
        f"/v1/jobs/{job['id']}",
        headers=worker_headers,
    )
    body = r.json()
    if worker_role == "admin":
        assert r.status_code == 200, r.text
        assert body["code"] == AppCode.JOB_RETRIEVED.value
        assert body["data"]["state"] == base_objects.ProcessingState.QUEUED.value
    else:
        assert r.status_code == 403, r.text
        assert body["status"] == 403
        assert body["code"] == AppCode.API_KEY_FORBIDDEN_FOR_JOB.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_delete_job_lease_404_job_not_found(client, worker_headers):
    job_id = uuid4()
    response = await client.request('DELETE', f'/v1/jobs/{job_id}/lease', headers=worker_headers)
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


@pytest.mark.asyncio
async def test_delete_job_lease_403_job_access_forbidden(client, worker_headers, job_factory, other_worker_headers):
    job_id = await job_factory(base_objects.ProcessingState.PROCESSING)
    response = await client.request('DELETE', f'/v1/jobs/{job_id}/lease', headers=other_worker_headers)
    assert response.status_code == 403, response.text
    assert response.json()["code"] == AppCode.API_KEY_FORBIDDEN_FOR_JOB.value


@pytest.mark.asyncio
@pytest.mark.parametrize("state", [state for state in base_objects.ProcessingState if state != base_objects.ProcessingState.PROCESSING])
async def test_delete_job_lease_409_job_not_processing(client, worker_headers, job_factory, state):
    job_id = await job_factory(state)
    response = await client.request('DELETE', f'/v1/jobs/{job_id}/lease', headers=worker_headers)
    assert response.status_code == 409, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_IN_PROCESSING.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_delete_job_lease_422_invalid_request(client, worker_headers):
    response = await client.request('DELETE', '/v1/jobs/not-a-uuid/lease', headers=worker_headers)
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("attempts", [0, 1, 2])
async def test_delete_job_lease_204_attempts_restored(client, worker_headers, job_factory, db_sessions, attempts):
    job_id = await job_factory(ProcessingState.PROCESSING)
    async with db_sessions.begin() as db:
        (await db.get(model.Job, job_id)).previous_attempts = attempts
    response = await client.delete(f"/v1/jobs/{job_id}/lease", headers=worker_headers)
    assert response.status_code == 204
    assert not response.content
    async with db_sessions() as db:
        job = await db.get(model.Job, job_id)
        assert job.previous_attempts == (attempts - 1 if attempts else None)
        assert job.worker_key_id is None
        assert job.state == ProcessingState.QUEUED



@pytest.mark.asyncio
async def test_delete_job_lease_403_reassignment_during_request(client, worker_headers, other_worker_headers,
                                                        user_headers, job_factory, db_sessions, monkeypatch):
    await assert_worker_update_race(client, worker_headers, other_worker_headers, user_headers,
                                    job_factory, db_sessions, monkeypatch,
                                    operation='release', concurrent_change="reassignment")



@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_delete_job_lease_409_cancellation_during_request(client, worker_headers, other_worker_headers,
                                                        user_headers, job_factory, db_sessions, monkeypatch):
    await assert_worker_update_race(client, worker_headers, other_worker_headers, user_headers,
                                    job_factory, db_sessions, monkeypatch,
                                    operation='release', concurrent_change="cancellation")




@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_delete_job_lease_500_unexpected_crud_result(client, worker_headers, job_factory, monkeypatch, invariant_errors):
    job_id = await job_factory(ProcessingState.PROCESSING)
    code = AppCode.API_KEY_VALID
    monkeypatch.setattr(worker_cruds, "release_job_lease", AsyncMock(return_value=code))
    response = await client.request("DELETE", f"/v1/jobs/{job_id}/lease", headers=worker_headers)
    assert_route_invariant(response, invariant_errors, code)




@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_delete_job_lease_404_disappeared_after_check(client, worker_headers, job_factory, db_sessions, monkeypatch):
    job_id = await job_factory(ProcessingState.PROCESSING)
    delete_job_after_exists(monkeypatch, db_sessions, job_id)
    response = await client.request("DELETE", f"/v1/jobs/{job_id}/lease", headers=worker_headers)
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value



@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_delete_job_lease_404_disappeared_before_update(client, worker_headers, job_factory, db_sessions, monkeypatch):
    job_id = await job_factory(ProcessingState.PROCESSING)
    delete_job_before_crud(monkeypatch, db_sessions, job_id, worker_cruds, "release_job_lease")
    response = await client.request("DELETE", f"/v1/jobs/{job_id}/lease", headers=worker_headers)
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


#
# GET /v1/jobs/{job_id} - 200, 403, 404
#

@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_RETRIEVED], indirect=True)
async def test_get_job_200(client, worker_headers, lease_job, payload):
    job_id = lease_job["created_job"]["id"]

    r = await client.get(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    job = body["data"]
    assert job["id"] == job_id


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_RETRIEVED], indirect=True)
async def test_get_job_200_with_engine(client, worker_headers, lease_job_with_engine, payload):
    job_id = lease_job_with_engine["created_job"]["id"]
    engine = lease_job_with_engine["engine"]

    r = await client.get(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    job = body["data"]
    assert job["id"] == job_id
    assert job["engine_name"] == engine["name"]
    assert job["engine_version"] == engine["version"]
    assert job["engine_definition"] == engine["definition"]
    assert "engine_id" in job
    assert "engine_files_updated" in job


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_get_job_404_job_not_found(client, worker_headers):
    job_id = uuid4()
    response = await client.request('GET', f'/v1/jobs/{job_id}', headers=worker_headers)
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


@pytest.mark.asyncio
async def test_get_job_403_job_access_forbidden(client, worker_headers, job_factory, other_worker_headers):
    job_id = await job_factory(base_objects.ProcessingState.PROCESSING)
    response = await client.request('GET', f'/v1/jobs/{job_id}', headers=other_worker_headers)
    assert response.status_code == 403, response.text
    assert response.json()["code"] == AppCode.API_KEY_FORBIDDEN_FOR_JOB.value


#
# GET /v1/jobs/{job_id}/images/{image_id}/files/image - 200, 403, 404, 409, 410, 422
#

@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.IMAGE_DOWNLOADED], indirect=True)
async def test_get_image_200(client, worker_headers, lease_job, payload):
    job_id = lease_job["created_job"]["id"]

    r = await client.get(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    job = body["data"]
    image_id = job["images"][0]["id"]
    image_name = job["images"][0]["name"]
    r = await client.get(
        f"/v1/jobs/{job_id}/images/{image_id}/files/image",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    assert r.headers["Content-Disposition"] == f'attachment; filename="{image_name}"'
    assert r.headers["Content-Type"].startswith("image/")


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.IMAGE_NOT_FOUND_FOR_JOB], indirect=True)
async def test_get_image_404(client, worker_headers, lease_job, payload):
    job_id = lease_job["created_job"]["id"]

    r = await client.get(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    # Attempt to get a non-existent image file as worker
    fake_image_id = "00000000-0000-0000-0000-000000000000"
    r = await client.get(
        f"/v1/jobs/{job_id}/images/{fake_image_id}/files/image",
        headers=worker_headers,
    )
    assert r.status_code == 404, r.text
    body = r.json()
    assert body["status"] == 404
    assert body["code"] == AppCode.IMAGE_NOT_FOUND_FOR_JOB.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.IMAGE_GONE], indirect=True)
async def test_get_image_410(client, worker_headers, lease_job, payload, tmp_path):
    job_id = lease_job["created_job"]["id"]

    r = await client.get(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    job = body["data"]
    for image in job["images"]:
        image_id = image["id"]
        image_name = image["name"]

        image_path = tmp_path / "jobs" / str(job_id) / f"{image_id}.jpg"
        assert os.path.exists(image_path), f"Image file should exist at {image_path} in the test directory"
        os.remove(image_path)

        # Attempt to get the image file as worker
        r = await client.get(
            f"/v1/jobs/{job_id}/images/{image_id}/files/image",
            headers=worker_headers,
        )
        assert r.status_code == 410, r.text
        body = r.json()
        assert body["status"] == 410
        assert body["code"] == AppCode.IMAGE_GONE.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_get_image_404_job_not_found(client, worker_headers):
    job_id = uuid4()
    response = await client.request('GET', f'/v1/jobs/{job_id}/images/00000000-0000-0000-0000-000000000001/files/image', headers=worker_headers)
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


@pytest.mark.asyncio
async def test_get_image_403_job_access_forbidden(client, worker_headers, job_factory, other_worker_headers):
    job_id = await job_factory(base_objects.ProcessingState.PROCESSING)
    response = await client.request('GET', f'/v1/jobs/{job_id}/images/00000000-0000-0000-0000-000000000001/files/image', headers=other_worker_headers)
    assert response.status_code == 403, response.text
    assert response.json()["code"] == AppCode.API_KEY_FORBIDDEN_FOR_JOB.value


@pytest.mark.asyncio
@pytest.mark.parametrize("state", [state for state in base_objects.ProcessingState if state != base_objects.ProcessingState.PROCESSING])
async def test_get_image_409_job_not_processing(client, worker_headers, job_factory, state):
    job_id = await job_factory(state)
    response = await client.request('GET', f'/v1/jobs/{job_id}/images/00000000-0000-0000-0000-000000000001/files/image', headers=worker_headers)
    assert response.status_code == 409, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_IN_PROCESSING.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_get_image_422_invalid_request(client, worker_headers):
    response = await client.request('GET', '/v1/jobs/not-a-uuid/images/00000000-0000-0000-0000-000000000001/files/image', headers=worker_headers)
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("failure", ["open", "read", "disconnect"])
@pytest.mark.asyncio
async def test_get_image_200_stream_failure(client, worker_headers, download_case, db_sessions, monkeypatch, failure):
    case = await download_case("image")
    await interrupt_download(main.app, case.url, worker_headers, case.path, monkeypatch, failure)
    assert case.path.read_bytes() == case.content
    await assert_records_unchanged(db_sessions, case.records)
    retry = await client.get(case.url, headers=worker_headers)
    assert retry.status_code == 200, retry.text
    assert retry.content == case.content




@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_get_image_500_unexpected_crud_result(client, worker_headers, download_case, monkeypatch, invariant_errors):
    case = await download_case("image")
    code = AppCode.API_KEY_VALID
    monkeypatch.setattr(worker_routes.general_cruds, "get_image_for_job", AsyncMock(return_value=(None, code)))
    response = await client.get(case.url, headers=worker_headers)
    assert_route_invariant(response, invariant_errors, code)



@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_get_image_410_link_is_not_upload(client, user_headers, worker_headers, db_sessions):
    from doc_api.api.checkers.job_readiness import queue_ready_jobs
    response = await client.post("/v1/jobs", headers=user_headers,
                                  json={"images": [{"name": "page.jpg", "order": 0,
                                                      **{f"{kind}_url": f"https://files.invalid/{kind}" for kind in ["image", "alto", "page"]}}],
                                        "alto_required": True, "page_required": True})
    assert response.status_code == 201, response.text
    job_id = response.json()["data"]["id"]
    async with db_sessions() as db:
        await queue_ready_jobs(db)
    lease = await client.post("/v1/jobs/lease", headers=worker_headers)
    assert lease.json()["code"] == AppCode.JOB_LEASED.value
    job = await client.get(f"/v1/jobs/{job_id}", headers=worker_headers)
    image = job.json()["data"]["images"][0]
    assert image["image_url"] == "https://files.invalid/image"
    result = await client.get(f"/v1/jobs/{job_id}/images/{image['id']}/files/image", headers=worker_headers)
    assert result.status_code == 410, result.text
    assert result.json()["code"] == AppCode.IMAGE_GONE.value


#
# GET /v1/jobs/{job_id}/images/{image_id}/files/alto - 200, 403, 404, 409, 410, 422
#

@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[2]], ids=[AppCode.ALTO_DOWNLOADED], indirect=True)
async def test_get_alto_200(client, worker_headers, lease_job, payload):
    job_id = lease_job["created_job"]["id"]

    r = await client.get(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    job = body["data"]
    image_id = job["images"][0]["id"]
    image_name = job["images"][0]["name"]
    r = await client.get(
        f"/v1/jobs/{job_id}/images/{image_id}/files/alto",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    assert r.headers["Content-Disposition"] == f'attachment; filename="{os.path.splitext(image_name)[0]}.xml"'
    assert r.headers["Content-Type"] == "application/xml"


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[2]], ids=[AppCode.IMAGE_NOT_FOUND_FOR_JOB], indirect=True)
async def test_get_alto_404(client, worker_headers, lease_job, payload):
    job_id = lease_job["created_job"]["id"]

    r = await client.get(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    # Attempt to get a non-existent ALTO file as worker
    fake_image_id = "00000000-0000-0000-0000-000000000000"
    r = await client.get(
        f"/v1/jobs/{job_id}/images/{fake_image_id}/files/alto",
        headers=worker_headers,
    )
    assert r.status_code == 404, r.text
    body = r.json()
    assert body["status"] == 404
    assert body["code"] == AppCode.IMAGE_NOT_FOUND_FOR_JOB.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.ALTO_NOT_REQUIRED], indirect=True)
async def test_get_alto_409(client, worker_headers, lease_job, payload):
    job_id = lease_job["created_job"]["id"]

    r = await client.get(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    job = body["data"]
    for image in job["images"]:
        image_id = image["id"]

        # Attempt to get the ALTO file as worker when not required
        r = await client.get(
            f"/v1/jobs/{job_id}/images/{image_id}/files/alto",
            headers=worker_headers,
        )
        assert r.status_code == 409, r.text
        body = r.json()
        assert body["status"] == 409
        assert body["code"] == AppCode.ALTO_NOT_REQUIRED.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[2]], ids=[AppCode.ALTO_GONE], indirect=True)
async def test_get_alto_410(client, worker_headers, lease_job, payload, tmp_path):
    job_id = lease_job["created_job"]["id"]

    r = await client.get(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    job = body["data"]
    for image in job["images"]:
        image_id = image["id"]
        image_name = image["name"]

        alto_path = tmp_path / "jobs" / str(job_id) / f"{image_id}.alto.xml"
        assert os.path.exists(alto_path), f"ALTO file should exist at {alto_path} in the test directory"
        os.remove(alto_path)

        # Attempt to get the ALTO file as worker
        r = await client.get(
            f"/v1/jobs/{job_id}/images/{image_id}/files/alto",
            headers=worker_headers,
        )
        assert r.status_code == 410, r.text
        body = r.json()
        assert body["status"] == 410
        assert body["code"] == AppCode.ALTO_GONE.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_get_alto_404_job_not_found(client, worker_headers):
    job_id = uuid4()
    response = await client.request('GET', f'/v1/jobs/{job_id}/images/00000000-0000-0000-0000-000000000001/files/alto', headers=worker_headers)
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


@pytest.mark.asyncio
async def test_get_alto_403_job_access_forbidden(client, worker_headers, job_factory, other_worker_headers):
    job_id = await job_factory(base_objects.ProcessingState.PROCESSING)
    response = await client.request('GET', f'/v1/jobs/{job_id}/images/00000000-0000-0000-0000-000000000001/files/alto', headers=other_worker_headers)
    assert response.status_code == 403, response.text
    assert response.json()["code"] == AppCode.API_KEY_FORBIDDEN_FOR_JOB.value


@pytest.mark.asyncio
@pytest.mark.parametrize("state", [state for state in base_objects.ProcessingState if state != base_objects.ProcessingState.PROCESSING])
async def test_get_alto_409_job_not_processing(client, worker_headers, job_factory, state):
    job_id = await job_factory(state)
    response = await client.request('GET', f'/v1/jobs/{job_id}/images/00000000-0000-0000-0000-000000000001/files/alto', headers=worker_headers)
    assert response.status_code == 409, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_IN_PROCESSING.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_get_alto_422_invalid_request(client, worker_headers):
    response = await client.request('GET', '/v1/jobs/not-a-uuid/images/00000000-0000-0000-0000-000000000001/files/alto', headers=worker_headers)
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("failure", ["open", "read", "disconnect"])
@pytest.mark.asyncio
async def test_get_alto_200_stream_failure(client, worker_headers, download_case, db_sessions, monkeypatch, failure):
    case = await download_case("alto")
    await interrupt_download(main.app, case.url, worker_headers, case.path, monkeypatch, failure)
    assert case.path.read_bytes() == case.content
    await assert_records_unchanged(db_sessions, case.records)
    retry = await client.get(case.url, headers=worker_headers)
    assert retry.status_code == 200, retry.text
    assert retry.content == case.content




@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_get_alto_500_unexpected_crud_result(client, worker_headers, download_case, monkeypatch, invariant_errors):
    case = await download_case("alto")
    code = AppCode.API_KEY_VALID
    monkeypatch.setattr(worker_routes.general_cruds, "get_image_for_job", AsyncMock(return_value=(None, code)))
    response = await client.get(case.url, headers=worker_headers)
    assert_route_invariant(response, invariant_errors, code)



@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_get_alto_410_link_is_not_upload(client, user_headers, worker_headers, db_sessions):
    from doc_api.api.checkers.job_readiness import queue_ready_jobs
    response = await client.post("/v1/jobs", headers=user_headers,
                                  json={"images": [{"name": "page.jpg", "order": 0,
                                                      **{f"{kind}_url": f"https://files.invalid/{kind}" for kind in ["image", "alto", "page"]}}],
                                        "alto_required": True, "page_required": True})
    assert response.status_code == 201, response.text
    job_id = response.json()["data"]["id"]
    async with db_sessions() as db:
        await queue_ready_jobs(db)
    lease = await client.post("/v1/jobs/lease", headers=worker_headers)
    assert lease.json()["code"] == AppCode.JOB_LEASED.value
    job = await client.get(f"/v1/jobs/{job_id}", headers=worker_headers)
    image = job.json()["data"]["images"][0]
    assert image["alto_url"] == "https://files.invalid/alto"
    result = await client.get(f"/v1/jobs/{job_id}/images/{image['id']}/files/alto", headers=worker_headers)
    assert result.status_code == 410, result.text
    assert result.json()["code"] == AppCode.ALTO_GONE.value


#
# GET /v1/jobs/{job_id}/images/{image_id}/files/page - 200, 403, 404, 409, 410, 422
#

@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[3]], ids=[AppCode.PAGE_DOWNLOADED], indirect=True)
async def test_get_page_200(client, worker_headers, lease_job, payload):
    job_id = lease_job["created_job"]["id"]

    r = await client.get(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    job = body["data"]
    image_id = job["images"][0]["id"]
    image_name = job["images"][0]["name"]
    r = await client.get(
        f"/v1/jobs/{job_id}/images/{image_id}/files/page",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    assert r.headers["Content-Disposition"] == f'attachment; filename="{os.path.splitext(image_name)[0]}.xml"'
    assert r.headers["Content-Type"] == "application/xml"


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[3]], ids=[AppCode.IMAGE_NOT_FOUND_FOR_JOB], indirect=True)
async def test_get_page_404(client, worker_headers, lease_job, payload):
    job_id = lease_job["created_job"]["id"]

    r = await client.get(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    # Attempt to get a non-existent PAGE file as worker
    fake_image_id = "00000000-0000-0000-0000-000000000000"
    r = await client.get(
        f"/v1/jobs/{job_id}/images/{fake_image_id}/files/page",
        headers=worker_headers,
    )
    assert r.status_code == 404, r.text
    body = r.json()
    assert body["status"] == 404
    assert body["code"] == AppCode.IMAGE_NOT_FOUND_FOR_JOB.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.PAGE_NOT_REQUIRED], indirect=True)
async def test_get_page_409(client, worker_headers, lease_job, payload):
    job_id = lease_job["created_job"]["id"]

    r = await client.get(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    job = body["data"]
    for image in job["images"]:
        image_id = image["id"]

        # Attempt to get the PAGE file as worker when not required
        r = await client.get(
            f"/v1/jobs/{job_id}/images/{image_id}/files/page",
            headers=worker_headers,
        )
        assert r.status_code == 409, r.text
        body = r.json()
        assert body["status"] == 409
        assert body["code"] == AppCode.PAGE_NOT_REQUIRED.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[3]], ids=[AppCode.PAGE_GONE], indirect=True)
async def test_get_page_410(client, worker_headers, lease_job, payload, tmp_path):
    job_id = lease_job["created_job"]["id"]

    r = await client.get(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    job = body["data"]
    for image in job["images"]:
        image_id = image["id"]
        image_name = image["name"]

        page_path = tmp_path / "jobs" / str(job_id) / f"{image_id}.page.xml"
        assert os.path.exists(page_path), f"PAGE file should exist at {page_path} in the test directory"
        os.remove(page_path)

        # Attempt to get the PAGE file as worker
        r = await client.get(
            f"/v1/jobs/{job_id}/images/{image_id}/files/page",
            headers=worker_headers,
        )
        assert r.status_code == 410, r.text
        body = r.json()
        assert body["status"] == 410
        assert body["code"] == AppCode.PAGE_GONE.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_get_page_404_job_not_found(client, worker_headers):
    job_id = uuid4()
    response = await client.request('GET', f'/v1/jobs/{job_id}/images/00000000-0000-0000-0000-000000000001/files/page', headers=worker_headers)
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


@pytest.mark.asyncio
async def test_get_page_403_job_access_forbidden(client, worker_headers, job_factory, other_worker_headers):
    job_id = await job_factory(base_objects.ProcessingState.PROCESSING)
    response = await client.request('GET', f'/v1/jobs/{job_id}/images/00000000-0000-0000-0000-000000000001/files/page', headers=other_worker_headers)
    assert response.status_code == 403, response.text
    assert response.json()["code"] == AppCode.API_KEY_FORBIDDEN_FOR_JOB.value


@pytest.mark.asyncio
@pytest.mark.parametrize("state", [state for state in base_objects.ProcessingState if state != base_objects.ProcessingState.PROCESSING])
async def test_get_page_409_job_not_processing(client, worker_headers, job_factory, state):
    job_id = await job_factory(state)
    response = await client.request('GET', f'/v1/jobs/{job_id}/images/00000000-0000-0000-0000-000000000001/files/page', headers=worker_headers)
    assert response.status_code == 409, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_IN_PROCESSING.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_get_page_422_invalid_request(client, worker_headers):
    response = await client.request('GET', '/v1/jobs/not-a-uuid/images/00000000-0000-0000-0000-000000000001/files/page', headers=worker_headers)
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("failure", ["open", "read", "disconnect"])
@pytest.mark.asyncio
async def test_get_page_200_stream_failure(client, worker_headers, download_case, db_sessions, monkeypatch, failure):
    case = await download_case("page")
    await interrupt_download(main.app, case.url, worker_headers, case.path, monkeypatch, failure)
    assert case.path.read_bytes() == case.content
    await assert_records_unchanged(db_sessions, case.records)
    retry = await client.get(case.url, headers=worker_headers)
    assert retry.status_code == 200, retry.text
    assert retry.content == case.content




@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_get_page_500_unexpected_crud_result(client, worker_headers, download_case, monkeypatch, invariant_errors):
    case = await download_case("page")
    code = AppCode.API_KEY_VALID
    monkeypatch.setattr(worker_routes.general_cruds, "get_image_for_job", AsyncMock(return_value=(None, code)))
    response = await client.get(case.url, headers=worker_headers)
    assert_route_invariant(response, invariant_errors, code)



@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_get_page_410_link_is_not_upload(client, user_headers, worker_headers, db_sessions):
    from doc_api.api.checkers.job_readiness import queue_ready_jobs
    response = await client.post("/v1/jobs", headers=user_headers,
                                  json={"images": [{"name": "page.jpg", "order": 0,
                                                      **{f"{kind}_url": f"https://files.invalid/{kind}" for kind in ["image", "alto", "page"]}}],
                                        "alto_required": True, "page_required": True})
    assert response.status_code == 201, response.text
    job_id = response.json()["data"]["id"]
    async with db_sessions() as db:
        await queue_ready_jobs(db)
    lease = await client.post("/v1/jobs/lease", headers=worker_headers)
    assert lease.json()["code"] == AppCode.JOB_LEASED.value
    job = await client.get(f"/v1/jobs/{job_id}", headers=worker_headers)
    image = job.json()["data"]["images"][0]
    assert image["page_url"] == "https://files.invalid/page"
    result = await client.get(f"/v1/jobs/{job_id}/images/{image['id']}/files/page", headers=worker_headers)
    assert result.status_code == 410, result.text
    assert result.json()["code"] == AppCode.PAGE_GONE.value


#
# GET /v1/jobs/{job_id}/files/metadata - 200, 403, 404, 409, 410, 422
#

@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[-1]], ids=[AppCode.META_JSON_DOWNLOADED], indirect=True)
async def test_get_metadata_200(client, worker_headers, lease_job, payload):
    job_id = lease_job["created_job"]["id"]

    r = await client.get(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    # Attempt to get the metadata file as worker
    r = await client.get(
        f"/v1/jobs/{job_id}/files/metadata",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    assert r.headers["Content-Disposition"] == f'attachment; filename="meta.json"'
    assert r.headers["Content-Type"] == "application/json"


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.META_JSON_NOT_REQUIRED], indirect=True)
async def test_get_metadata_409(client, worker_headers, lease_job, payload):
    job_id = lease_job["created_job"]["id"]

    r = await client.get(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    # Attempt to get the metadata file as worker when not required
    r = await client.get(
        f"/v1/jobs/{job_id}/files/metadata",
        headers=worker_headers,
    )
    assert r.status_code == 409, r.text
    body = r.json()
    assert body["status"] == 409
    assert body["code"] == AppCode.META_JSON_NOT_REQUIRED.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[-1]], ids=[AppCode.META_JSON_GONE], indirect=True)
async def test_get_metadata_410(client, worker_headers, lease_job, payload, tmp_path):
    job_id = lease_job["created_job"]["id"]

    r = await client.get(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    metadata_path = tmp_path / "jobs" / str(job_id) / "meta.json"
    assert os.path.exists(metadata_path), f"Metadata file should exist at {metadata_path} in the test directory"
    os.remove(metadata_path)

    # Attempt to get the metadata file as worker
    r = await client.get(
        f"/v1/jobs/{job_id}/files/metadata",
        headers=worker_headers,
    )
    assert r.status_code == 410, r.text
    body = r.json()
    assert body["status"] == 410
    assert body["code"] == AppCode.META_JSON_GONE.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_get_metadata_404_job_not_found(client, worker_headers):
    job_id = uuid4()
    response = await client.request('GET', f'/v1/jobs/{job_id}/files/metadata', headers=worker_headers)
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


@pytest.mark.asyncio
async def test_get_metadata_403_job_access_forbidden(client, worker_headers, job_factory, other_worker_headers):
    job_id = await job_factory(base_objects.ProcessingState.PROCESSING)
    response = await client.request('GET', f'/v1/jobs/{job_id}/files/metadata', headers=other_worker_headers)
    assert response.status_code == 403, response.text
    assert response.json()["code"] == AppCode.API_KEY_FORBIDDEN_FOR_JOB.value


@pytest.mark.asyncio
@pytest.mark.parametrize("state", [state for state in base_objects.ProcessingState if state != base_objects.ProcessingState.PROCESSING])
async def test_get_metadata_409_job_not_processing(client, worker_headers, job_factory, state):
    job_id = await job_factory(state)
    response = await client.request('GET', f'/v1/jobs/{job_id}/files/metadata', headers=worker_headers)
    assert response.status_code == 409, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_IN_PROCESSING.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_get_metadata_422_invalid_request(client, worker_headers):
    response = await client.request('GET', '/v1/jobs/not-a-uuid/files/metadata', headers=worker_headers)
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("failure", ["open", "read", "disconnect"])
@pytest.mark.asyncio
async def test_get_metadata_200_stream_failure(client, worker_headers, download_case, db_sessions, monkeypatch, failure):
    case = await download_case("metadata")
    await interrupt_download(main.app, case.url, worker_headers, case.path, monkeypatch, failure)
    assert case.path.read_bytes() == case.content
    await assert_records_unchanged(db_sessions, case.records)
    retry = await client.get(case.url, headers=worker_headers)
    assert retry.status_code == 200, retry.text
    assert retry.content == case.content




@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_get_metadata_404_disappeared_after_check(client, worker_headers, download_case, db_sessions, monkeypatch):
    case = await download_case("metadata")
    delete_job_after_exists(monkeypatch, db_sessions, case.records[0][1])
    response = await client.get(case.url, headers=worker_headers)
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


#
# GET /v1/engines/{engine_id}/files - 200, 404, 410, 422
#

@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.ENGINE_FILES_RETRIEVED], indirect=True)
async def test_get_engine_files_200(client, worker_headers, lease_job_with_uploaded_engine, payload):
    engine = lease_job_with_uploaded_engine["engine"]
    job = lease_job_with_uploaded_engine["created_job"]

    r = await client.get(
        f"/v1/jobs/{job['id']}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value
    job = body["data"]

    r = await client.get(
        f"/v1/engines/{job['engine_id']}/files",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text

    # check for valid zip content
    assert r.headers["Content-Disposition"] == f'attachment; filename="{job["engine_id"]}.zip"'
    assert r.headers["Content-Type"] == "application/zip"
    assert r.content == VALID_ZIP


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.ENGINE_FILES_NOT_FOUND], indirect=True)
async def test_get_engine_files_404(client, worker_headers, lease_job_with_engine, payload):
    job_id = lease_job_with_engine["created_job"]["id"]

    r = await client.get(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    job = body["data"]

    r = await client.get(
        f"/v1/engines/{job['engine_id']}/files",
        headers=worker_headers,
    )
    assert r.status_code == 404, r.text
    body = r.json()
    assert body["status"] == 404
    assert body["code"] == AppCode.ENGINE_FILES_NOT_FOUND.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.ENGINE_FILES_GONE], indirect=True)
async def test_get_engine_files_410(client, worker_headers, lease_job_with_uploaded_engine, payload, tmp_path):
    engine = lease_job_with_uploaded_engine["engine"]
    job = lease_job_with_uploaded_engine["created_job"]

    r = await client.get(
        f"/v1/jobs/{job['id']}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value
    job = body["data"]

    # Delete engine files to simulate gone engine
    engine_path = tmp_path / "engines" / f"{job['engine_id']}.zip"

    assert os.path.exists(engine_path), f"Engine files should exist at {engine_path} in the test directory"

    os.remove(engine_path)

    r = await client.get(
        f"/v1/engines/{job['engine_id']}/files",
        headers=worker_headers,
    )
    assert r.status_code == 410, r.text
    body = r.json()
    assert body["status"] == 410
    assert body["code"] == AppCode.ENGINE_FILES_GONE.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_get_engine_files_422_invalid_request(client, worker_headers):
    response = await client.request('GET', '/v1/engines/not-a-uuid/files', headers=worker_headers)
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_get_engine_files_404_engine_not_found(client, worker_headers):
    response = await client.get(f"/v1/engines/{uuid4()}/files", headers=worker_headers)
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.ENGINE_NOT_FOUND.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("failure", ["open", "read", "disconnect"])
@pytest.mark.asyncio
async def test_get_engine_files_200_stream_failure(client, worker_headers, download_case, db_sessions, monkeypatch, failure):
    case = await download_case("engine")
    await interrupt_download(main.app, case.url, worker_headers, case.path, monkeypatch, failure)
    assert case.path.read_bytes() == case.content
    await assert_records_unchanged(db_sessions, case.records)
    retry = await client.get(case.url, headers=worker_headers)
    assert retry.status_code == 200, retry.text
    assert retry.content == case.content




@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_get_engine_files_500_unexpected_crud_result(client, worker_headers, download_case, monkeypatch, invariant_errors):
    case = await download_case("engine")
    code = AppCode.API_KEY_VALID
    monkeypatch.setattr(worker_routes.general_cruds, "get_engine", AsyncMock(return_value=(None, code)))
    response = await client.get(case.url, headers=worker_headers)
    assert_route_invariant(response, invariant_errors, code)


#
# POST /v1/jobs/{job_id}/result - 201, 200, 403, 404, 409, 415, 422, 400, 500
#

@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_RESULT_UPLOADED.value], indirect=True)
async def test_post_job_result_201(client, worker_headers, job_with_result, payload):
    pass

@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_post_job_result_201_preserves_deletion_timestamps(client, worker_headers, db_sessions, job_with_deletion_timestamps):
    job_id, before = await job_with_deletion_timestamps(base_objects.ProcessingState.PROCESSING)
    response = await client.post(f"/v1/jobs/{job_id}/result", headers=worker_headers,
                                 files={"file": ("upload.zip", VALID_ZIP)})
    assert response.status_code == 201, response.text
    await assert_deletion_timestamps(db_sessions, job_id, before)


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_RESULT_REUPLOADED.value], indirect=True)
async def test_post_job_result_200(client, worker_headers, job_with_result, payload):
    job_id = job_with_result["lease"]["id"]

    r = await client.post(
        f"/v1/jobs/{job_id}/result",
        headers=worker_headers,
        files={"file": ("result.zip", VALID_ZIP, "application/zip")},
    )

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RESULT_REUPLOADED.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_RESULT_INVALID], indirect=True)
async def test_post_job_result_415(client, worker_headers, lease_job, payload):
    job_id = lease_job["lease"]["id"]

    r = await client.post(
        f"/v1/jobs/{job_id}/result",
        headers=worker_headers,
        files={"file": ("result.txt", b"This is not a zip file.", "text/plain")},
    )

    assert r.status_code == 415, r.text
    body = r.json()
    assert body["status"] == 415
    assert body["code"] == AppCode.JOB_RESULT_INVALID.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_post_job_result_404_job_not_found(client, worker_headers):
    job_id = uuid4()
    response = await client.request('POST', f'/v1/jobs/{job_id}/result', headers=worker_headers, files={'file': ('output.zip', b'data')})
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


@pytest.mark.asyncio
async def test_post_job_result_403_job_access_forbidden(client, worker_headers, job_factory, other_worker_headers):
    job_id = await job_factory(base_objects.ProcessingState.PROCESSING)
    response = await client.request('POST', f'/v1/jobs/{job_id}/result', headers=other_worker_headers, files={'file': ('output.zip', b'data')})
    assert response.status_code == 403, response.text
    assert response.json()["code"] == AppCode.API_KEY_FORBIDDEN_FOR_JOB.value


@pytest.mark.asyncio
@pytest.mark.parametrize("state", [state for state in base_objects.ProcessingState if state != base_objects.ProcessingState.PROCESSING])
async def test_post_job_result_409_job_not_processing(client, worker_headers, job_factory, state):
    job_id = await job_factory(state)
    response = await client.request('POST', f'/v1/jobs/{job_id}/result', headers=worker_headers, files={'file': ('output.zip', b'data')})
    assert response.status_code == 409, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_IN_PROCESSING.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_post_job_result_422_invalid_request(client, worker_headers):
    response = await client.request('POST', '/v1/jobs/00000000-0000-0000-0000-000000000001/result', headers=worker_headers)
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_post_job_result_201_validation_disabled(client, worker_headers, job_factory, monkeypatch):
    monkeypatch.setattr(config, 'RESULT_ZIP_VALIDATION', False)
    job_id = await job_factory(ProcessingState.PROCESSING)
    response = await client.post(f"/v1/jobs/{job_id}/result", headers=worker_headers,
                                 files={"file": ("output.zip", b"not a ZIP")})
    assert response.status_code == 201, response.text
    assert response.json()["code"] == AppCode.JOB_RESULT_UPLOADED.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("existing", [False, True], ids=["first-upload", "replacement"])
@pytest.mark.parametrize("failure", ["read", "write", "replace"])
@pytest.mark.asyncio
async def test_post_job_result_500_upload_failure(client, worker_headers, zip_upload_case, db_sessions, monkeypatch, existing, failure):
    case = await zip_upload_case("result", existing=existing)
    with monkeypatch.context() as patch:
        inject_zip_upload_failure(case.path, failure, patch)
        response = await client.post(case.url, headers=worker_headers, files={"file": ("replacement.zip", VALID_ZIP)})
    assert_internal_transfer_error(response)
    assert case.path.exists() == existing
    if existing:
        assert case.path.read_bytes() == case.old
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-upload-staging-*"))
    await assert_records_unchanged(db_sessions, case.records)
    retry = await client.post(case.url, headers=worker_headers, files={"file": ("replacement.zip", VALID_ZIP)})
    assert retry.status_code == (200 if existing else 201), retry.text
    assert case.path.read_bytes() == VALID_ZIP


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_post_job_result_415_preserves_existing_file(client, worker_headers, zip_upload_case, db_sessions):
    case = await zip_upload_case("result")
    response = await client.post(case.url, headers=worker_headers, files={"file": ("invalid.zip", b"invalid")})
    assert response.status_code == 415, response.text
    assert case.path.read_bytes() == case.old
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-upload-staging-*"))
    await assert_records_unchanged(db_sessions, case.records)


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("existing", [False, True], ids=["first-upload", "replacement"])
@pytest.mark.asyncio
async def test_post_job_result_400_interrupted_upload(client, worker_headers, zip_upload_case, db_sessions, existing):
    case = await zip_upload_case("result", existing=existing)
    response = await client.post(case.url, headers=interrupted_upload_headers(worker_headers),
                                    content=interrupted_upload_body())
    assert response.status_code == 400, response.text
    assert response.json()["code"] == AppCode.HTTP_ERROR.value
    assert case.path.exists() == existing
    if existing:
        assert case.path.read_bytes() == case.old
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-upload-staging-*"))
    await assert_records_unchanged(db_sessions, case.records)


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("existing", [False, True], ids=["first-upload", "replacement"])
@pytest.mark.asyncio
async def test_post_job_result_cancelled_upload(client, worker_headers, zip_upload_case, db_sessions, monkeypatch, existing):
    case = await zip_upload_case("result", existing=existing)
    started = asyncio.Event()
    block = asyncio.Event()
    reads = 0
    original_read = UploadFile.read
    async def interrupted_read(upload, size=-1):
        nonlocal reads
        if reads:
            started.set()
            await block.wait()
        reads += 1
        return await original_read(upload, 16)
    with monkeypatch.context() as patch:
        patch.setattr(UploadFile, "read", interrupted_read)
        request = asyncio.create_task(client.post(case.url, headers=worker_headers, files={"file": ("upload.zip", VALID_ZIP)}))
        try:
            await asyncio.wait_for(started.wait(), timeout=10)
            request.cancel()
            with pytest.raises(asyncio.CancelledError):
                await request
        finally:
            block.set()
            if not request.done():
                request.cancel()
                try:
                    await request
                except asyncio.CancelledError:
                    pass
    assert case.path.exists() == existing
    if existing:
        assert case.path.read_bytes() == case.old
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-upload-staging-*"))
    await assert_records_unchanged(db_sessions, case.records)
    retry = await client.post(case.url, headers=worker_headers, files={"file": ("upload.zip", VALID_ZIP)})
    assert retry.status_code == (200 if existing else 201), retry.text


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_post_job_result_409_file_save_in_progress(client, worker_headers, zip_upload_case, monkeypatch, db_sessions):
    from doc_api.tests.helpers import assert_file_save_conflict
    case = await zip_upload_case("result")
    await assert_file_save_conflict(client, method="POST", url=case.url, headers=worker_headers,
                                   kwargs={"files": {"file": ("replacement.zip", VALID_ZIP)}}, path=case.path,
                                   monkeypatch=monkeypatch, db_sessions=db_sessions, first_status=200)
    assert case.path.read_bytes() == VALID_ZIP


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_post_job_result_200_progress_during_staging(client, worker_headers, zip_upload_case, db_sessions, monkeypatch):
    case = await zip_upload_case("result")
    started, release = asyncio.Event(), asyncio.Event()
    original_read = UploadFile.read
    reads = 0
    async def slow_read(upload, size=-1):
        nonlocal reads
        if reads:
            started.set()
            await release.wait()
        reads += 1
        return await original_read(upload, 16)
    with monkeypatch.context() as patch:
        patch.setattr(UploadFile, "read", slow_read)
        request = asyncio.create_task(client.post(case.url, headers=worker_headers, files={"file": ("new.zip", VALID_ZIP)}))
        try:
            await asyncio.wait_for(started.wait(), timeout=10)
            job_id = case.records[0][1]
            response = await asyncio.wait_for(client.patch(f"/v1/jobs/{job_id}", headers=worker_headers,
                                                           json={"progress": 0.7}), timeout=5)
            assert response.status_code == 200, response.text
            assert response.json()["code"] == AppCode.JOB_UPDATED.value
            assert not request.done()
            release.set()
            uploaded = await asyncio.wait_for(request, timeout=10)
            assert uploaded.status_code == 200, uploaded.text
        finally:
            release.set()
            if not request.done():
                request.cancel()
            await asyncio.gather(request, return_exceptions=True)
    assert case.path.read_bytes() == VALID_ZIP
    async with db_sessions() as db:
        assert (await db.get(model.Job, job_id)).progress == 0.7


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_post_job_result_200_responsive_during_preparation(client, worker_headers, zip_upload_case, db_sessions, blocked_upload_preparation):
    case = await zip_upload_case("result")
    blocked = blocked_upload_preparation
    request = asyncio.create_task(client.post(case.url, headers=worker_headers, files={"file": ("new.zip", VALID_ZIP)}))
    try:
        assert await asyncio.to_thread(blocked.started.wait, 10)
        assert not blocked.release.is_set(), "Filesystem preparation blocked the event loop"
        job_id = case.records[0][1]
        response = await asyncio.wait_for(client.patch(f"/v1/jobs/{job_id}", headers=worker_headers,
                                                       json={"progress": 0.6}), timeout=10)
        assert response.status_code == 200, response.text
        assert not blocked.release.is_set()
        blocked.release.set()
        uploaded = await asyncio.wait_for(request, timeout=10)
        assert uploaded.status_code == 200, uploaded.text
    finally:
        blocked.release.set()
        if not request.done():
            request.cancel()
        await asyncio.gather(request, return_exceptions=True)
    async with db_sessions() as db:
        assert (await db.get(model.Job, job_id)).progress == 0.6


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_post_job_result_cancelled_during_preparation(client, worker_headers, zip_upload_case, blocked_upload_preparation, db_sessions):
    case = await zip_upload_case("result")
    blocked = blocked_upload_preparation
    first = asyncio.create_task(client.post(case.url, headers=worker_headers, files={"file": ("first.zip", VALID_ZIP)}))
    try:
        assert await asyncio.to_thread(blocked.started.wait, 10)
        assert db_sessions.kw["bind"].pool.checkedout() == 0
        first.cancel()
        second = await asyncio.wait_for(client.post(case.url, headers=worker_headers,
            files={"file": ("second.zip", VALID_ZIP + b"second")}), timeout=5)
        assert second.status_code == 409, second.text
        assert second.json()["code"] == AppCode.FILE_SAVE_IN_PROGRESS.value
        assert not first.done()
        blocked.release.set()
        with pytest.raises(asyncio.CancelledError):
            await first
        response = await client.post(case.url, headers=worker_headers,
            files={"file": ("second.zip", VALID_ZIP + b"second")})
        assert response.status_code == 200, response.text
    finally:
        blocked.release.set()
        if not first.done():
            first.cancel()
        await asyncio.gather(first, return_exceptions=True)
    assert case.path.read_bytes() == VALID_ZIP + b"second"
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-*"))








@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("variant", ["empty", "many_entries", "unicode", "large", "compressed"])
@pytest.mark.asyncio
async def test_post_job_result_200_generated_zip(client, worker_headers, admin_headers, zip_upload_case, db_sessions, variant):
    case = await zip_upload_case("result")
    await assert_generated_zip_save(client, worker_headers, admin_headers, case, db_sessions,
                                    kind="result", variant=variant, code=AppCode.JOB_RESULT_REUPLOADED, invalid=False)


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("variant", ["truncated", "invalid_directory"])
@pytest.mark.asyncio
async def test_post_job_result_415_generated_zip(client, worker_headers, admin_headers, zip_upload_case, db_sessions, variant):
    case = await zip_upload_case("result")
    await assert_generated_zip_save(client, worker_headers, admin_headers, case, db_sessions,
                                    kind="result", variant=variant, code=AppCode.JOB_RESULT_INVALID, invalid=True)



@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("variant", ["missing_boundary", "invalid_body"])
@pytest.mark.asyncio
async def test_post_job_result_400_multipart(client, worker_headers, zip_upload_case, db_sessions, variant):
    case = await zip_upload_case("result")
    await assert_multipart_rejected(client, worker_headers, case, db_sessions,
                                    method="POST", variant=variant)


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("variant", ["wrong_field", "empty_filename", "text_field"])
@pytest.mark.asyncio
async def test_post_job_result_422_multipart(client, worker_headers, zip_upload_case, db_sessions, variant):
    case = await zip_upload_case("result")
    await assert_multipart_rejected(client, worker_headers, case, db_sessions,
                                    method="POST", variant=variant)



@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_post_job_result_404_disappeared_before_install(client, worker_headers, zip_upload_case, db_sessions, monkeypatch):
    case = await zip_upload_case("result")
    entity, record_id, _ = case.records[0]
    delete_before_file_install(monkeypatch, db_sessions, entity, record_id)
    response = await client.request("POST", case.url, headers=worker_headers,
                                    files={"file": ("new.zip", VALID_ZIP)})
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value
    assert case.path.read_bytes() == case.old
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-*"))


#
# POST /v1/jobs/{job_id}/artifacts - 201, 200, 403, 404, 409, 415, 422, 400, 500
#

@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_ARTIFACTS_UPLOADED.value], indirect=True)
async def test_post_job_artifacts_201(client, worker_headers, job_with_artifacts, payload):
    pass

@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_post_job_artifacts_201_preserves_deletion_timestamps(client, worker_headers, db_sessions, job_with_deletion_timestamps):
    job_id, before = await job_with_deletion_timestamps(base_objects.ProcessingState.PROCESSING)
    response = await client.post(f"/v1/jobs/{job_id}/artifacts", headers=worker_headers,
                                 files={"file": ("upload.zip", VALID_ZIP)})
    assert response.status_code == 201, response.text
    await assert_deletion_timestamps(db_sessions, job_id, before)


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_ARTIFACTS_REUPLOADED.value], indirect=True)
async def test_post_job_artifacts_200(client, worker_headers, job_with_artifacts, payload):
    job_id = job_with_artifacts["lease"]["id"]

    r = await client.post(
        f"/v1/jobs/{job_id}/artifacts",
        headers=worker_headers,
        files={"file": ("artifacts.zip", VALID_ZIP, "application/zip")},
    )

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_ARTIFACTS_REUPLOADED.value

@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_ARTIFACTS_INVALID], indirect=True)
async def test_post_job_artifacts_415(client, worker_headers, lease_job, payload):
    job_id = lease_job["lease"]["id"]

    r = await client.post(
        f"/v1/jobs/{job_id}/artifacts",
        headers=worker_headers,
        files={"file": ("artifacts.txt", b"This is not a zip file.", "text/plain")},
    )

    assert r.status_code == 415, r.text
    body = r.json()
    assert body["status"] == 415
    assert body["code"] == AppCode.JOB_ARTIFACTS_INVALID.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_post_job_artifacts_404_job_not_found(client, worker_headers):
    job_id = uuid4()
    response = await client.request('POST', f'/v1/jobs/{job_id}/artifacts', headers=worker_headers, files={'file': ('output.zip', b'data')})
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


@pytest.mark.asyncio
async def test_post_job_artifacts_403_job_access_forbidden(client, worker_headers, job_factory, other_worker_headers):
    job_id = await job_factory(base_objects.ProcessingState.PROCESSING)
    response = await client.request('POST', f'/v1/jobs/{job_id}/artifacts', headers=other_worker_headers, files={'file': ('output.zip', b'data')})
    assert response.status_code == 403, response.text
    assert response.json()["code"] == AppCode.API_KEY_FORBIDDEN_FOR_JOB.value


@pytest.mark.asyncio
@pytest.mark.parametrize("state", [state for state in base_objects.ProcessingState if state != base_objects.ProcessingState.PROCESSING])
async def test_post_job_artifacts_409_job_not_processing(client, worker_headers, job_factory, state):
    job_id = await job_factory(state)
    response = await client.request('POST', f'/v1/jobs/{job_id}/artifacts', headers=worker_headers, files={'file': ('output.zip', b'data')})
    assert response.status_code == 409, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_IN_PROCESSING.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_post_job_artifacts_422_invalid_request(client, worker_headers):
    response = await client.request('POST', '/v1/jobs/00000000-0000-0000-0000-000000000001/artifacts', headers=worker_headers)
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_post_job_artifacts_201_validation_disabled(client, worker_headers, job_factory, monkeypatch):
    monkeypatch.setattr(config, 'ARTIFACTS_ZIP_VALIDATION', False)
    job_id = await job_factory(ProcessingState.PROCESSING)
    response = await client.post(f"/v1/jobs/{job_id}/artifacts", headers=worker_headers,
                                 files={"file": ("output.zip", b"not a ZIP")})
    assert response.status_code == 201, response.text
    assert response.json()["code"] == AppCode.JOB_ARTIFACTS_UPLOADED.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("existing", [False, True], ids=["first-upload", "replacement"])
@pytest.mark.parametrize("failure", ["read", "write", "replace"])
@pytest.mark.asyncio
async def test_post_job_artifacts_500_upload_failure(client, worker_headers, zip_upload_case, db_sessions, monkeypatch, existing, failure):
    case = await zip_upload_case("artifacts", existing=existing)
    with monkeypatch.context() as patch:
        inject_zip_upload_failure(case.path, failure, patch)
        response = await client.post(case.url, headers=worker_headers, files={"file": ("replacement.zip", VALID_ZIP)})
    assert_internal_transfer_error(response)
    assert case.path.exists() == existing
    if existing:
        assert case.path.read_bytes() == case.old
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-upload-staging-*"))
    await assert_records_unchanged(db_sessions, case.records)
    retry = await client.post(case.url, headers=worker_headers, files={"file": ("replacement.zip", VALID_ZIP)})
    assert retry.status_code == (200 if existing else 201), retry.text
    assert case.path.read_bytes() == VALID_ZIP


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_post_job_artifacts_415_preserves_existing_file(client, worker_headers, zip_upload_case, db_sessions):
    case = await zip_upload_case("artifacts")
    response = await client.post(case.url, headers=worker_headers, files={"file": ("invalid.zip", b"invalid")})
    assert response.status_code == 415, response.text
    assert case.path.read_bytes() == case.old
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-upload-staging-*"))
    await assert_records_unchanged(db_sessions, case.records)


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("existing", [False, True], ids=["first-upload", "replacement"])
@pytest.mark.asyncio
async def test_post_job_artifacts_400_interrupted_upload(client, worker_headers, zip_upload_case, db_sessions, existing):
    case = await zip_upload_case("artifacts", existing=existing)
    response = await client.post(case.url, headers=interrupted_upload_headers(worker_headers),
                                    content=interrupted_upload_body())
    assert response.status_code == 400, response.text
    assert response.json()["code"] == AppCode.HTTP_ERROR.value
    assert case.path.exists() == existing
    if existing:
        assert case.path.read_bytes() == case.old
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-upload-staging-*"))
    await assert_records_unchanged(db_sessions, case.records)


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("existing", [False, True], ids=["first-upload", "replacement"])
@pytest.mark.asyncio
async def test_post_job_artifacts_cancelled_upload(client, worker_headers, zip_upload_case, db_sessions, monkeypatch, existing):
    case = await zip_upload_case("artifacts", existing=existing)
    started = asyncio.Event()
    block = asyncio.Event()
    reads = 0
    original_read = UploadFile.read
    async def interrupted_read(upload, size=-1):
        nonlocal reads
        if reads:
            started.set()
            await block.wait()
        reads += 1
        return await original_read(upload, 16)
    with monkeypatch.context() as patch:
        patch.setattr(UploadFile, "read", interrupted_read)
        request = asyncio.create_task(client.post(case.url, headers=worker_headers, files={"file": ("upload.zip", VALID_ZIP)}))
        try:
            await asyncio.wait_for(started.wait(), timeout=10)
            request.cancel()
            with pytest.raises(asyncio.CancelledError):
                await request
        finally:
            block.set()
            if not request.done():
                request.cancel()
                try:
                    await request
                except asyncio.CancelledError:
                    pass
    assert case.path.exists() == existing
    if existing:
        assert case.path.read_bytes() == case.old
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-upload-staging-*"))
    await assert_records_unchanged(db_sessions, case.records)
    retry = await client.post(case.url, headers=worker_headers, files={"file": ("upload.zip", VALID_ZIP)})
    assert retry.status_code == (200 if existing else 201), retry.text


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_post_job_artifacts_409_file_save_in_progress(client, worker_headers, zip_upload_case, monkeypatch, db_sessions):
    from doc_api.tests.helpers import assert_file_save_conflict
    case = await zip_upload_case("artifacts")
    await assert_file_save_conflict(client, method="POST", url=case.url, headers=worker_headers,
                                   kwargs={"files": {"file": ("replacement.zip", VALID_ZIP)}}, path=case.path,
                                   monkeypatch=monkeypatch, db_sessions=db_sessions, first_status=200)
    assert case.path.read_bytes() == VALID_ZIP


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_post_job_artifacts_200_progress_during_staging(client, worker_headers, zip_upload_case, db_sessions, monkeypatch):
    case = await zip_upload_case("artifacts")
    started, release = asyncio.Event(), asyncio.Event()
    original_read = UploadFile.read
    reads = 0
    async def slow_read(upload, size=-1):
        nonlocal reads
        if reads:
            started.set()
            await release.wait()
        reads += 1
        return await original_read(upload, 16)
    with monkeypatch.context() as patch:
        patch.setattr(UploadFile, "read", slow_read)
        request = asyncio.create_task(client.post(case.url, headers=worker_headers, files={"file": ("new.zip", VALID_ZIP)}))
        try:
            await asyncio.wait_for(started.wait(), timeout=10)
            job_id = case.records[0][1]
            response = await asyncio.wait_for(client.patch(f"/v1/jobs/{job_id}", headers=worker_headers,
                                                           json={"progress": 0.7}), timeout=5)
            assert response.status_code == 200, response.text
            assert response.json()["code"] == AppCode.JOB_UPDATED.value
            assert not request.done()
            release.set()
            uploaded = await asyncio.wait_for(request, timeout=10)
            assert uploaded.status_code == 200, uploaded.text
        finally:
            release.set()
            if not request.done():
                request.cancel()
            await asyncio.gather(request, return_exceptions=True)
    assert case.path.read_bytes() == VALID_ZIP
    async with db_sessions() as db:
        assert (await db.get(model.Job, job_id)).progress == 0.7






@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("variant", ["empty", "many_entries", "unicode", "large", "compressed"])
@pytest.mark.asyncio
async def test_post_job_artifacts_200_generated_zip(client, worker_headers, admin_headers, zip_upload_case, db_sessions, variant):
    case = await zip_upload_case("artifacts")
    await assert_generated_zip_save(client, worker_headers, admin_headers, case, db_sessions,
                                    kind="artifacts", variant=variant, code=AppCode.JOB_ARTIFACTS_REUPLOADED, invalid=False)


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("variant", ["truncated", "invalid_directory"])
@pytest.mark.asyncio
async def test_post_job_artifacts_415_generated_zip(client, worker_headers, admin_headers, zip_upload_case, db_sessions, variant):
    case = await zip_upload_case("artifacts")
    await assert_generated_zip_save(client, worker_headers, admin_headers, case, db_sessions,
                                    kind="artifacts", variant=variant, code=AppCode.JOB_ARTIFACTS_INVALID, invalid=True)



@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("variant", ["missing_boundary", "invalid_body"])
@pytest.mark.asyncio
async def test_post_job_artifacts_400_multipart(client, worker_headers, zip_upload_case, db_sessions, variant):
    case = await zip_upload_case("artifacts")
    await assert_multipart_rejected(client, worker_headers, case, db_sessions,
                                    method="POST", variant=variant)


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("variant", ["wrong_field", "empty_filename", "text_field"])
@pytest.mark.asyncio
async def test_post_job_artifacts_422_multipart(client, worker_headers, zip_upload_case, db_sessions, variant):
    case = await zip_upload_case("artifacts")
    await assert_multipart_rejected(client, worker_headers, case, db_sessions,
                                    method="POST", variant=variant)



@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_post_job_artifacts_404_disappeared_before_install(client, worker_headers, zip_upload_case, db_sessions, monkeypatch):
    case = await zip_upload_case("artifacts")
    entity, record_id, _ = case.records[0]
    delete_before_file_install(monkeypatch, db_sessions, entity, record_id)
    response = await client.request("POST", case.url, headers=worker_headers,
                                    files={"file": ("new.zip", VALID_ZIP)})
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value
    assert case.path.read_bytes() == case.old
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-*"))


#
# PATCH /v1/jobs/{job_id} - 200, 400, 403, 404, 409
#

@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_UPDATED], indirect=True)
async def test_patch_job_200_update_progress(client, worker_headers, lease_job, payload):
    job_id = lease_job["created_job"]["id"]
    lease = lease_job["lease"]

    r = await client.patch(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
        json={"log": "technical log",
              "log_user": "user-friendly log",
              "progress": 0.7}
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_UPDATED.value
    extended_lease = body["data"]
    assert extended_lease["id"] == lease["id"]
    assert extended_lease["lease_expire_at"] > lease["lease_expire_at"], "Lease expiration time should be extended"
    assert extended_lease["server_time"] > lease["server_time"], "Server time should be updated"

    r = await client.get(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    job = body["data"]
    assert job["log_user"] == "user-friendly log"
    assert job["progress"] == 0.7


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_COMPLETED], indirect=True)
async def test_patch_job_200_job_completed(client, worker_headers, job_marked_done, payload):
    job_id = job_marked_done["lease"]["id"]
    update_payload = job_marked_done["update_payload"]

    r = await client.get(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    job = body["data"]
    assert job["state"] == base_objects.ProcessingState.DONE.value
    assert job["log_user"] == update_payload["log_user"]
    assert job["progress"] == 1.0

@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_ALREADY_COMPLETED], indirect=True)
async def test_patch_job_200_job_already_completed(client, worker_headers, job_marked_done, payload):
    job_id = job_marked_done["lease"]["id"]
    update_payload = job_marked_done["update_payload"]

    r = await client.patch(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
        json=update_payload
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_ALREADY_COMPLETED.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_MARKED_ERROR], indirect=True)
async def test_patch_job_200_job_marked_error(client, worker_headers, job_marked_error, payload):
    job_id = job_marked_error["lease"]["id"]
    update_payload = job_marked_error["update_payload"]

    r = await client.get(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    job = body["data"]
    assert job["state"] == base_objects.ProcessingState.ERROR.value
    assert job["log_user"] == update_payload["log_user"]


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_ALREADY_MARKED_ERROR], indirect=True)
async def test_patch_job_200_job_already_marked_error(client, worker_headers, job_marked_error, payload):
    job_id = job_marked_error["lease"]["id"]
    update_payload = job_marked_error["update_payload"]

    r = await client.patch(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
        json=update_payload
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_ALREADY_MARKED_ERROR.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("initial_state", [ProcessingState.PROCESSING, ProcessingState.ERROR],
                         ids=[f"{AppCode.JOB_MARKED_FAILED}:{state.name}" for state in [ProcessingState.PROCESSING, ProcessingState.ERROR]])
async def test_patch_job_200_job_marked_failed(client, worker_headers, db_sessions, initial_state, job_factory, worker_key):
    job_id = await job_factory(initial_state)
    before = datetime.now(timezone.utc)
    payload = {"state": "failed", "log": "Permanent error", "log_user": "Cannot process input"}
    response = await client.patch(f"/v1/jobs/{job_id}", headers=worker_headers, json=payload)
    assert response.status_code == 200, response.text
    assert response.json()["code"] == AppCode.JOB_MARKED_FAILED.value

    async with db_sessions() as db:
        job = await db.get(model.Job, job_id)
        assert job.state == ProcessingState.FAILED
        assert job.progress == 1.0
        assert job.finished_date >= before
        assert job.last_change == job.finished_date
        assert job.log == ["Technical log\nPermanent error"]
        assert job.log_user == ["User log\nCannot process input"]
        assert job.previous_attempts == 0
        finished_date = job.finished_date

    async with db_sessions() as db:
        _, _, _, code = await worker_cruds.lease_job_to_worker(db=db, worker_key_id=worker_key.id)
        assert code == AppCode.JOB_QUEUE_EMPTY
        job = await db.get(model.Job, job_id)
        assert job.state == ProcessingState.FAILED
        assert job.finished_date == finished_date
        assert job.log == ["Technical log\nPermanent error"]


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_patch_job_200_job_already_marked_failed(client, worker_headers, db_sessions, job_factory):
    job_id = await job_factory(ProcessingState.FAILED)
    async with db_sessions() as db:
        before = model.orm2dict(await db.get(model.Job, job_id))
    response = await client.patch(f"/v1/jobs/{job_id}", headers=worker_headers,
                                  json={"state": "failed", "log": "Must not append again"})
    assert response.status_code == 200, response.text
    assert response.json()["code"] == AppCode.JOB_ALREADY_MARKED_FAILED.value
    async with db_sessions() as db:
        assert model.orm2dict(await db.get(model.Job, job_id)) == before


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_patch_job_200_error_remains_retryable(client, worker_headers, db_sessions, job_factory, worker_key):
    job_id = await job_factory(ProcessingState.PROCESSING)
    response = await client.patch(f"/v1/jobs/{job_id}", headers=worker_headers, json={"state": "error"})
    assert response.status_code == 200, response.text
    assert response.json()["code"] == AppCode.JOB_MARKED_ERROR.value
    await run_lease_check(db_sessions)
    async with db_sessions() as db:
        job, _, _, code = await worker_cruds.lease_job_to_worker(db=db, worker_key_id=worker_key.id)
        assert code == AppCode.JOB_LEASED
        assert job.id == job_id
        assert job.previous_attempts == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_UPDATE_NO_FIELDS.value], indirect=True)
async def test_patch_job_400_no_fields(client, worker_headers, lease_job, payload):
    job_id = lease_job["created_job"]["id"]

    r = await client.patch(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
        json={}
    )
    assert r.status_code == 400, r.text
    body = r.json()
    assert body["status"] == 400
    assert body["code"] == AppCode.JOB_UPDATE_NO_FIELDS.value


@pytest.mark.asyncio
@pytest.mark.parametrize("identity", ["owner", "other_worker"])
async def test_patch_job_403_failure_unassigned_worker(client, user_headers, other_worker_headers, db_sessions, identity, job_factory):
    job_id = await job_factory(ProcessingState.PROCESSING)
    headers = user_headers if identity == "owner" else other_worker_headers
    response = await client.patch(f"/v1/jobs/{job_id}", headers=headers, json={"state": "failed"})
    assert response.status_code == 403, response.text
    async with db_sessions() as db:
        assert (await db.get(model.Job, job_id)).state == ProcessingState.PROCESSING


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_RESULT_MISSING.value], indirect=True)
async def test_patch_job_409_job_result_missing(client, worker_headers, lease_job, payload):
    job_id = lease_job["created_job"]["id"]

    r = await client.patch(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
        json={"state": base_objects.ProcessingState.DONE.value,
              "log_user": "user-friendly log"}
    )
    assert r.status_code == 409, r.text
    body = r.json()
    assert body["status"] == 409
    assert body["code"] == AppCode.JOB_RESULT_MISSING.value


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_UNFINISHABLE.value], indirect=True)
async def test_patch_job_409_job_unfinishable(client, worker_headers, cancelled_processing_job, payload):
    job_id = cancelled_processing_job["lease"]["id"]

    r = await client.patch(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
        json={"state": base_objects.ProcessingState.DONE.value,
              "log_user": "user-friendly log"}
    )

    assert r.status_code == 409, r.text
    body = r.json()
    assert body["status"] == 409
    assert body["code"] == AppCode.JOB_UNFINISHABLE.value
    assert "state" in body["details"]
    assert body["details"]["state"] == base_objects.ProcessingState.CANCELLED.value


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["done", "error"], ids=["done", "error"])
async def test_patch_job_409_update_failed_job(client, worker_headers, db_sessions, job_factory, state):
    job_id = await job_factory(ProcessingState.FAILED)
    async with db_sessions() as db:
        before = model.orm2dict(await db.get(model.Job, job_id))
    response = await client.patch(f"/v1/jobs/{job_id}", headers=worker_headers, json={"state": state})
    assert response.status_code == 409, response.text
    async with db_sessions() as db:
        assert model.orm2dict(await db.get(model.Job, job_id)) == before


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("state", [ProcessingState.NEW, ProcessingState.QUEUED,
                                   ProcessingState.CANCELLED, ProcessingState.DONE])
async def test_patch_job_409_failure_invalid_source_state(client, worker_headers, db_sessions, state, job_factory):
    job_id = await job_factory(state)
    response = await client.patch(f"/v1/jobs/{job_id}", headers=worker_headers, json={"state": "failed"})
    assert response.status_code == 409, response.text
    assert response.json()["code"] == AppCode.JOB_UNFINISHABLE.value
    async with db_sessions() as db:
        assert (await db.get(model.Job, job_id)).state == state
        await db.rollback()
        # The transaction-level check also rejects a stale request after a state change.
        _, _, _, code = await worker_cruds.update_job_progress(
            worker_key_id=None,
            db=db, job_id=job_id, job_progress_update=JobProgressUpdate(state="failed"))
        assert code == AppCode.JOB_UNFINISHABLE


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_patch_job_404_job_not_found(client, worker_headers):
    job_id = uuid4()
    response = await client.request('PATCH', f'/v1/jobs/{job_id}', headers=worker_headers, json={'progress': 0.5})
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


@pytest.mark.asyncio
async def test_patch_job_403_job_access_forbidden(client, worker_headers, job_factory, other_worker_headers):
    job_id = await job_factory(base_objects.ProcessingState.PROCESSING)
    response = await client.request('PATCH', f'/v1/jobs/{job_id}', headers=other_worker_headers, json={'progress': 0.5})
    assert response.status_code == 403, response.text
    assert response.json()["code"] == AppCode.API_KEY_FORBIDDEN_FOR_JOB.value



@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["new", "queued", "cancelled", "processing"])
async def test_patch_job_403_unsupported_worker_state(client, worker_headers, job_factory, state):
    job_id = await job_factory(ProcessingState.PROCESSING)
    response = await client.patch(f"/v1/jobs/{job_id}", headers=worker_headers, json={"state": state})
    assert response.status_code == 403, response.text
    assert response.json()["code"] == AppCode.API_KEY_WORKER_FORBIDDEN.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("progress,expected", [(-1, 0), (0, 0), (1, 1), (2, 1)])
async def test_patch_job_200_progress_clamped(client, worker_headers, job_factory, db_sessions, progress, expected):
    job_id = await job_factory(ProcessingState.PROCESSING)
    response = await client.patch(f"/v1/jobs/{job_id}", headers=worker_headers, json={"progress": progress})
    assert response.status_code == 200, response.text
    assert response.json()["code"] == AppCode.JOB_UPDATED.value
    async with db_sessions() as db:
        assert (await db.get(model.Job, job_id)).progress == expected


@pytest.mark.parametrize("operation", ["progress", "done", "error", "failed"])
@pytest.mark.asyncio
async def test_patch_job_403_reassignment_during_request(client, worker_headers, other_worker_headers,
                                                        user_headers, job_factory, db_sessions, monkeypatch, operation):
    await assert_worker_update_race(client, worker_headers, other_worker_headers, user_headers,
                                    job_factory, db_sessions, monkeypatch,
                                    operation=operation, concurrent_change="reassignment")



@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("operation", ["progress", "done", "error", "failed"])
@pytest.mark.asyncio
async def test_patch_job_409_cancellation_during_request(client, worker_headers, other_worker_headers,
                                                        user_headers, job_factory, db_sessions, monkeypatch, operation):
    await assert_worker_update_race(client, worker_headers, other_worker_headers, user_headers,
                                    job_factory, db_sessions, monkeypatch,
                                    operation=operation, concurrent_change="cancellation")



@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("exhausted", [False, True])
@pytest.mark.asyncio
async def test_patch_job_200_error_during_lease_poll(client, worker_headers, other_worker_headers,
                                                   job_factory, db_sessions, monkeypatch, exhausted):
    monkeypatch.setattr(config, "JOB_MAX_ATTEMPTS", 3)
    job_id = await job_factory(ProcessingState.PROCESSING)
    async with db_sessions.begin() as db:
        (await db.get(model.Job, job_id)).previous_attempts = 2 if exhausted else 0
    entered, release = asyncio.Event(), asyncio.Event()
    original_exit = AsyncSessionTransaction.__aexit__

    async def exit_transaction(transaction, exc_type, exc, traceback):
        if exc_type is None and any(isinstance(row, model.Job) and row.id == job_id
                                    and row.state == ProcessingState.ERROR
                                    for row in transaction.session.dirty):
            entered.set()
            try:
                await release.wait()
            except BaseException as error:
                await original_exit(transaction, type(error), error, error.__traceback__)
                raise
        return await original_exit(transaction, exc_type, exc, traceback)

    monkeypatch.setattr(AsyncSessionTransaction, "__aexit__", exit_transaction)
    report = asyncio.create_task(client.patch(f"/v1/jobs/{job_id}", headers=worker_headers,
                                              json={"state": "error", "log": "retry race"}))
    try:
        await asyncio.wait_for(entered.wait(), timeout=10)
        # The pending ERROR is uncommitted; another worker must not lease it.
        before = await asyncio.wait_for(client.post("/v1/jobs/lease", headers=other_worker_headers), timeout=10)
        assert before.status_code == 200, before.text
        assert before.json()["code"] == AppCode.JOB_QUEUE_EMPTY.value
        release.set()
        response = await asyncio.wait_for(report, timeout=10)
        assert response.json()["code"] == AppCode.JOB_MARKED_ERROR.value
    finally:
        release.set()
        report.cancel()
        await asyncio.gather(report, return_exceptions=True)
    await run_lease_check(db_sessions)
    after = await client.post("/v1/jobs/lease", headers=other_worker_headers)
    assert after.status_code == 200, after.text
    assert after.json()["code"] == (AppCode.JOB_QUEUE_EMPTY if exhausted else AppCode.JOB_LEASED).value
    async with db_sessions() as db:
        job = await db.get(model.Job, job_id)
        assert job.state == (ProcessingState.FAILED if exhausted else ProcessingState.PROCESSING)
        assert job.previous_attempts == (2 if exhausted else 1)
        assert "".join(entry or "" for entry in (job.log or [])).count("retry race") == 1
        assert "".join(entry or "" for entry in (job.log or [])).count("ATTEMPT ") == (0 if exhausted else 1)


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("first_state", ["error", "failed"])
@pytest.mark.asyncio
async def test_patch_job_200_409_concurrent_error_and_failure(client, worker_headers, job_factory,
                                                            db_sessions, monkeypatch, first_state):
    job_id = await job_factory(ProcessingState.PROCESSING)
    entered = asyncio.Event()
    release = {state: asyncio.Event() for state in ["error", "failed"]}
    original = worker_cruds.update_job_progress
    arrivals = 0

    async def update(**kwargs):
        nonlocal arrivals
        arrivals += 1
        if arrivals == 2:
            entered.set()
        await release[kwargs["job_progress_update"].state.value].wait()
        return await original(**kwargs)

    monkeypatch.setattr(worker_cruds, "update_job_progress", update)
    tasks = [asyncio.create_task(client.patch(f"/v1/jobs/{job_id}", headers=worker_headers,
                                              json={"state": state, "log": f"report-{state}"}))
             for state in ["error", "failed"]]
    try:
        await asyncio.wait_for(entered.wait(), timeout=10)
        # Both requests passed their initial guards. Exercise both commit
        # orders explicitly so the stale ERROR-after-FAILED path cannot hide.
        release[first_state].set()
        await asyncio.wait_for(tasks[0 if first_state == "error" else 1], timeout=10)
        release["failed" if first_state == "error" else "error"].set()
        error, failure = await asyncio.wait_for(asyncio.gather(*tasks), timeout=10)
        assert failure.status_code == 200, failure.text
        assert failure.json()["code"] == AppCode.JOB_MARKED_FAILED.value
        assert error.status_code == (200 if first_state == "error" else 409), error.text
        expected_code = AppCode.JOB_MARKED_ERROR if error.status_code == 200 else AppCode.JOB_UNFINISHABLE
        assert error.json()["code"] == expected_code.value
        async with db_sessions() as db:
            job = await db.get(model.Job, job_id)
            assert job.state == ProcessingState.FAILED and job.progress == 1.0
            assert job.finished_date is not None
            assert "".join(entry or "" for entry in (job.log or [])).count("report-failed") == 1
            assert "".join(entry or "" for entry in (job.log or [])).count("report-error") == (1 if error.status_code == 200 else 0)
        retry = await client.post("/v1/jobs/lease", headers=worker_headers)
        assert retry.json()["code"] == AppCode.JOB_QUEUE_EMPTY.value
    finally:
        for event in release.values():
            event.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("state", [None, "done", "error", "failed"])
@pytest.mark.asyncio
async def test_patch_job_500_unexpected_crud_result(client, worker_headers, job_factory, monkeypatch, invariant_errors, state):
    job_id = await job_factory(ProcessingState.PROCESSING)
    (Path(config.RESULTS_DIR) / f"{job_id}.zip").write_bytes(VALID_ZIP)
    code = AppCode.API_KEY_VALID
    monkeypatch.setattr(worker_cruds, "update_job_progress", AsyncMock(return_value=(None, None, None, code)))
    payload = {"progress": 0.5} if state is None else {"state": state}
    response = await client.patch(f"/v1/jobs/{job_id}", headers=worker_headers, json=payload)
    assert_route_invariant(response, invariant_errors, code)


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("state", [None, "done", "error", "failed"])
@pytest.mark.asyncio
async def test_patch_job_404_disappeared_before_update(client, worker_headers, job_factory, db_sessions, monkeypatch, state):
    job_id = await job_factory(ProcessingState.PROCESSING)
    (Path(config.RESULTS_DIR) / f"{job_id}.zip").write_bytes(VALID_ZIP)
    delete_job_before_crud(monkeypatch, db_sessions, job_id, worker_cruds, "update_job_progress")
    response = await client.patch(f"/v1/jobs/{job_id}", headers=worker_headers,
                                   json={"progress": 0.5} if state is None else {"state": state})
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("state", ["done", "error", "failed"])
@pytest.mark.asyncio
async def test_patch_job_200_attempt_logs(client, worker_headers, admin_headers, user_headers, readonly_headers,
                                         job_factory, db_sessions, state):
    job_id = await job_factory(ProcessingState.QUEUED)
    async with db_sessions.begin() as db:
        (await db.get(model.Job, job_id)).previous_attempts = None
    await run_lease_check(db_sessions)
    lease = await client.post("/v1/jobs/lease", headers=worker_headers)
    assert lease.json()["code"] == AppCode.JOB_LEASED.value
    async with db_sessions() as db:
        job = await db.get(model.Job, job_id)
        assert job.log is None and job.log_user is None
    for message in ["first", "second\n"]:
        response = await client.patch(f"/v1/jobs/{job_id}", headers=worker_headers,
                                      json={"log": message, "log_user": message})
        assert response.status_code == 200, response.text
    response = await client.patch(f"/v1/jobs/{job_id}", headers=worker_headers, json={"state": "error"})
    assert response.status_code == 200, response.text
    await run_lease_check(db_sessions)
    lease = await client.post("/v1/jobs/lease", headers=worker_headers)
    assert lease.json()["code"] == AppCode.JOB_LEASED.value
    async with db_sessions() as db:
        job = await db.get(model.Job, job_id)
        assert job.log[0] == "first\nsecond\n"
        assert job.log[1].startswith("ATTEMPT 2 STARTED AT ")
        assert job.log_user == ["first\nsecond\n", None]
    # The previous attempt's public messages must not appear while this one is quiet.
    for headers in [user_headers, readonly_headers]:
        response = await client.get(f"/v1/jobs/{job_id}", headers=headers)
        assert response.status_code == 200, response.text
        assert response.json()["data"].get("log_user") is None
    if state == "done":
        (Path(config.RESULTS_DIR) / f"{job_id}.zip").write_bytes(b"result")
    response = await client.patch(f"/v1/jobs/{job_id}", headers=worker_headers,
                                  json={"log": "latest technical", "log_user": "latest public", "state": state})
    assert response.status_code == 200, response.text
    for headers in [user_headers, readonly_headers, worker_headers, admin_headers]:
        response = await client.get(f"/v1/jobs/{job_id}", headers=headers)
        assert response.status_code == 200, response.text
        job = response.json()["data"]
        is_admin = headers == admin_headers
        assert job["log_user"] == ("first\nsecond\n\n\nlatest public" if is_admin else "latest public")
        if is_admin:
            assert "ATTEMPT 2 STARTED AT " in job["log"]
            assert "latest technical" in job["log"] and "first\nsecond\n" in job["log"]
        listing = await client.get("/v1/jobs", headers=headers)
        # Workers cannot list jobs; user/readonly/admin must use the same log projection as GET.
        if headers == user_headers or is_admin:
            assert listing.status_code == 200, listing.text
            listed = next(item for item in listing.json()["data"] if item["id"] == str(job_id))
            assert listed["log_user"] == job["log_user"]


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.parametrize("attempt", [0, 1])
@pytest.mark.asyncio
async def test_post_job_lease_200_reused_attempt_logs(client, worker_headers, job_factory, db_sessions, attempt):
    job_id = await job_factory(ProcessingState.PROCESSING)
    async with db_sessions.begin() as db:
        job = await db.get(model.Job, job_id)
        job.previous_attempts = attempt
        job.log = ["previous technical"] * attempt + ["discard technical"]
        job.log_user = ["previous public"] * attempt + ["discard public"]
        job.final_log = ["previous final technical"] * attempt + ["discard final technical"]
        job.final_log_user = ["previous final public"] * attempt + ["discard final public"]
    response = await client.delete(f"/v1/jobs/{job_id}/lease", headers=worker_headers)
    assert response.status_code == 204, response.text
    response = await client.post("/v1/jobs/lease", headers=worker_headers)
    assert response.json()["code"] == AppCode.JOB_LEASED.value
    async with db_sessions() as db:
        job = await db.get(model.Job, job_id)
        assert job.previous_attempts == attempt
        assert job.final_log == (["previous final technical", None] if attempt else None)
        assert job.final_log_user == (["previous final public", None] if attempt else None)
        if attempt == 0:
            assert job.log is None and job.log_user is None
        else:
            assert job.log[0] == "previous technical"
            assert job.log[1].startswith("ATTEMPT 2 STARTED AT ")
            assert "discard" not in job.log[1]
            assert job.log_user == ["previous public", None]


@pytest.mark.parametrize("worker_role", ["worker", "admin"])
@pytest.mark.asyncio
async def test_patch_job_200_quiet_first_attempt_logs(client, worker_headers, job_factory, db_sessions):
    job_id = await job_factory(ProcessingState.QUEUED)
    async with db_sessions.begin() as db:
        (await db.get(model.Job, job_id)).previous_attempts = None
    assert (await client.post("/v1/jobs/lease", headers=worker_headers)).json()["code"] == AppCode.JOB_LEASED.value
    (Path(config.RESULTS_DIR) / f"{job_id}.zip").write_bytes(b"result")
    response = await client.patch(f"/v1/jobs/{job_id}", headers=worker_headers, json={"state": "done"})
    assert response.status_code == 200, response.text
    async with db_sessions() as db:
        job = await db.get(model.Job, job_id)
        assert job.log is None and job.log_user is None


@pytest.mark.parametrize('worker_role', ['worker', 'admin'])
@pytest.mark.parametrize('state', ['done', 'error', 'failed'])
@pytest.mark.asyncio
async def test_patch_job_200_final_reports(client, worker_headers, job_factory, db_sessions, job_mail_outbox, state):
    job_id = await job_factory(ProcessingState.PROCESSING)
    async with db_sessions.begin() as db:
        job = await db.get(model.Job, job_id)
        owner = await db.get(model.Key, job.owner_key_id)
        owner.email = 'owner@example.com'
        owner.email_notifications = True
    Path(config.RESULTS_DIR, f'{job_id}.zip').write_bytes(b'results')
    response = await client.patch(f'/v1/jobs/{job_id}', headers=worker_headers,
        json={'state': state, 'final_log': 'technical final report', 'final_log_user': 'user final report'})
    assert response.status_code == 200, response.text
    async with db_sessions() as db:
        job = await db.get(model.Job, job_id)
        assert job.log == ['Technical log']
        assert job.log_user == ['User log']
        assert job.final_log == ['technical final report']
        assert job.final_log_user == ['user final report']
    await check_job_notifications()
    if state == 'error':
        assert job_mail_outbox == []
    else:
        assert len(job_mail_outbox) == 2
        assert {tuple(receivers): message.get_content().strip() for _, receivers, message in job_mail_outbox} == {
            ('owner@example.com',): 'user final report', ('admin@example.com',): 'technical final report'}
        # Repeating a finalization must not send duplicate notifications.
        response = await client.patch(f'/v1/jobs/{job_id}', headers=worker_headers, json={'state': state})
        assert response.status_code == 200, response.text
        assert len(job_mail_outbox) == 2


@pytest.mark.parametrize('worker_role', ['worker', 'admin'])
@pytest.mark.parametrize('email,enabled,report,sends', [
    (None, True, 'report', False), ('owner@example.com', False, 'report', False),
    ('owner@example.com', True, '', False), ('owner@example.com', True, '  ', False),
    ('owner@example.com', True, None, False), ('owner@example.com', True, 'report', True)])
@pytest.mark.asyncio
async def test_patch_job_200_owner_mail_conditions(client, worker_headers, job_factory, db_sessions,
                                                   job_mail_outbox, email, enabled, report, sends):
    job_id = await job_factory(ProcessingState.PROCESSING)
    async with db_sessions.begin() as db:
        job = await db.get(model.Job, job_id)
        owner = await db.get(model.Key, job.owner_key_id)
        owner.email, owner.email_notifications = email, enabled
    response = await client.patch(f'/v1/jobs/{job_id}', headers=worker_headers,
        json={'state': 'failed', 'final_log_user': report})
    assert response.status_code == 200, response.text
    await check_job_notifications()
    assert len(job_mail_outbox) == int(sends)


@pytest.mark.parametrize('worker_role', ['worker', 'admin'])
@pytest.mark.asyncio
async def test_patch_job_422_final_report_without_final_state(client, worker_headers, job_factory):
    job_id = await job_factory(ProcessingState.PROCESSING)
    response = await client.patch(f'/v1/jobs/{job_id}', headers=worker_headers, json={'final_log_user': 'report'})
    assert response.status_code == 422, response.text


@pytest.mark.parametrize('worker_role', ['worker', 'admin'])
@pytest.mark.parametrize('state', [ProcessingState.ERROR, ProcessingState.PROCESSING])
@pytest.mark.asyncio
async def test_post_job_lease_200_exhausted_attempt_mail(client, worker_headers, job_factory, db_sessions,
                                                       job_mail_outbox, state):
    job_id = await job_factory(state)
    async with db_sessions.begin() as db:
        job = await db.get(model.Job, job_id)
        job.previous_attempts = config.JOB_MAX_ATTEMPTS - 1
        job.last_change = datetime.now(timezone.utc) - timedelta(days=1)
        job.final_log = [None] * (config.JOB_MAX_ATTEMPTS - 1) + ['last report']
    await run_lease_check(db_sessions)
    await check_job_notifications()
    response = await client.post('/v1/jobs/lease', headers=worker_headers)
    assert response.status_code == 200, response.text
    async with db_sessions() as db:
        assert (await db.get(model.Job, job_id)).state == ProcessingState.FAILED
    assert len(job_mail_outbox) == 1
    response = await client.post('/v1/jobs/lease', headers=worker_headers)
    assert response.status_code == 200, response.text
    assert len(job_mail_outbox) == 1


@pytest.mark.parametrize('worker_role', ['worker', 'admin'])
@pytest.mark.asyncio
async def test_patch_job_200_smtp_failure(client, worker_headers, job_factory, db_sessions, job_mail_outbox, monkeypatch):
    from unittest.mock import Mock
    monkeypatch.setattr('doc_api.tools.mail.mail_sender.smtplib.SMTP', Mock(side_effect=OSError('SMTP unavailable')))
    job_id = await job_factory(ProcessingState.PROCESSING)
    response = await client.patch(f'/v1/jobs/{job_id}', headers=worker_headers,
        json={'state': 'failed', 'final_log': 'report'})
    assert response.status_code == 200, response.text
    await check_job_notifications()
    async with db_sessions() as db:
        job = await db.get(model.Job, job_id)
        assert job.state == ProcessingState.FAILED
        assert job.final_log == ['report']


@pytest.mark.parametrize('worker_role', ['worker', 'admin'])
@pytest.mark.asyncio
async def test_patch_job_500_no_mail_on_failed_commit(client, worker_headers, job_factory, db_sessions, job_mail_outbox):
    from sqlalchemy import event, exc
    from sqlalchemy.orm import Session
    job_id = await job_factory(ProcessingState.PROCESSING)

    def fail_commit(session):
        if any(isinstance(row, model.Job) and row.id == job_id and row.state == ProcessingState.FAILED
               for row in session.identity_map.values()):
            raise exc.SQLAlchemyError('commit rejected')

    event.listen(Session, 'before_commit', fail_commit)
    try:
        response = await client.patch(f'/v1/jobs/{job_id}', headers=worker_headers,
            json={'state': 'failed', 'final_log': 'report'})
    finally:
        event.remove(Session, 'before_commit', fail_commit)
    assert response.status_code == 500, response.text
    await check_job_notifications()
    assert job_mail_outbox == []
    async with db_sessions() as db:
        job = await db.get(model.Job, job_id)
        assert job.state == ProcessingState.PROCESSING
        assert job.final_log is None


@pytest.mark.parametrize('worker_role', ['worker', 'admin'])
@pytest.mark.asyncio
async def test_patch_job_200_api_responsive_during_mail(client, worker_headers, admin_headers, job_factory,
                                                       job_mail_outbox, monkeypatch, isolated_database):
    from threading import Event
    from doc_api.tools.mail.mail_sender import MailSender
    started, release = Event(), Event()

    def blocked_send(*args, **kwargs):
        started.set()
        assert release.wait(10), 'SMTP test did not release the sender'
        return True

    monkeypatch.setattr(MailSender, 'send', blocked_send)
    job_id = await job_factory(ProcessingState.PROCESSING)
    response = await client.patch(f'/v1/jobs/{job_id}', headers=worker_headers,
        json={'state': 'failed', 'final_log': 'report'})
    assert response.status_code == 200, response.text
    request = asyncio.create_task(check_job_notifications())
    try:
        assert await asyncio.to_thread(started.wait, 5)
        assert isolated_database.engine.pool.checkedout() == 0
        response = await asyncio.wait_for(client.get(f'/v1/jobs/{job_id}', headers=admin_headers), timeout=2)
        assert response.status_code == 200, response.text
        assert response.json()['data']['state'] == 'failed'
    finally:
        release.set()
        await request
    assert response.status_code == 200, response.text


@pytest.mark.parametrize('worker_role', ['worker', 'admin'])
@pytest.mark.asyncio
async def test_patch_job_200_logs_pad_missing_attempts(client, worker_headers, job_factory, db_sessions):
    job_id = await job_factory(ProcessingState.PROCESSING)
    async with db_sessions.begin() as db:
        job = await db.get(model.Job, job_id)
        job.previous_attempts = 2
        job.log = job.log_user = job.final_log = job.final_log_user = None
    response = await client.patch(f'/v1/jobs/{job_id}', headers=worker_headers,
        json={'log': 'technical progress', 'log_user': 'user progress'})
    assert response.status_code == 200, response.text
    response = await client.patch(f'/v1/jobs/{job_id}', headers=worker_headers,
        json={'state': 'failed', 'final_log': 'technical final', 'final_log_user': 'user final'})
    assert response.status_code == 200, response.text
    async with db_sessions() as db:
        job = await db.get(model.Job, job_id)
        assert job.log == [None, None, 'technical progress']
        assert job.log_user == [None, None, 'user progress']
        assert job.final_log == [None, None, 'technical final']
        assert job.final_log_user == [None, None, 'user final']


@pytest.mark.parametrize('worker_role', ['worker', 'admin'])
@pytest.mark.asyncio
async def test_patch_job_200_mail_preparation_failure(client, worker_headers, job_factory, db_sessions,
                                                      job_mail_outbox, monkeypatch, caplog):
    from sqlalchemy import exc
    from sqlalchemy.ext.asyncio import AsyncSession
    job_id = await job_factory(ProcessingState.PROCESSING)
    original = AsyncSession.get

    async def fail_recipient_read(session, entity, *args, **kwargs):
        if entity is model.Key:
            raise exc.SQLAlchemyError('recipient lookup failed')
        return await original(session, entity, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, 'get', fail_recipient_read)
    response = await client.patch(f'/v1/jobs/{job_id}', headers=worker_headers,
        json={'state': 'failed', 'final_log': 'final report'})
    assert response.status_code == 200, response.text
    await check_job_notifications()
    assert job_mail_outbox == []
    assert 'Failed preparing final reports for job' in caplog.text
    async with db_sessions() as db:
        job = await db.get(model.Job, job_id)
        assert job.state == ProcessingState.FAILED
        assert job.final_log == ['final report']
