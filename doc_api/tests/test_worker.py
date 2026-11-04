from doc_api.api import file_saving as upload_helpers
from starlette.datastructures import UploadFile
import asyncio
import logging
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone

from uuid import uuid4

import pytest

from doc_api.api.routes import worker_routes
from doc_api.api import main
from doc_api.tests.helpers import (assert_generated_zip_save, assert_multipart_rejected,
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
    job_id = await job_factory(ProcessingState.PROCESSING)
    async with db_sessions.begin() as db:
        job = await db.get(model.Job, job_id)
        job.last_change = now - timedelta(seconds=config.JOB_TIMEOUT_SECONDS + config.JOB_TIMEOUT_GRACE_SECONDS) + timedelta(seconds=offset)
        job.previous_attempts = config.JOB_MAX_ATTEMPTS - 1 if exhausted else 0
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
        assert job.log == "Technical log\nPermanent error"
        assert job.log_user == "User log\nCannot process input"
        assert job.previous_attempts == 0
        finished_date = job.finished_date

    async with db_sessions() as db:
        _, _, _, code = await worker_cruds.lease_job_to_worker(db=db, worker_key_id=worker_key.id)
        assert code == AppCode.JOB_QUEUE_EMPTY
        job = await db.get(model.Job, job_id)
        assert job.state == ProcessingState.FAILED
        assert job.finished_date == finished_date
        assert job.log == "Technical log\nPermanent error"


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
