#
# after job is created by USER and processed by WORKER, USER is changed to READONLY and can still READ results and jobs, but cannot create new jobs
#
import os
from uuid import uuid4

import pytest

from doc_api.api.schemas import base_objects
from doc_api.api.schemas.responses import AppCode
from doc_api.tests.conftest import _put_file, _ename
from doc_api.tests.dummy_data import JOB_DEFINITION_PAYLOADS, VALID_ALTO_XML, VALID_PAGE_XML, make_white_image_bytes, \
    VALID_ZIP, job_definition_payload_id

#
# Upload all required job files according to payload and verify flags
#

@pytest.mark.asyncio
@pytest.mark.parametrize("payload", JOB_DEFINITION_PAYLOADS, ids=job_definition_payload_id, indirect=True)
async def test_upload_job_files(client, user_headers, job_with_required_uploads_by_payload_name):
    job = job_with_required_uploads_by_payload_name["created_job"]
    payload = job_with_required_uploads_by_payload_name["payload"]
    job_id = job["id"]

    # Fetch fresh state
    r = await client.get(f"/v1/jobs/{job_id}", headers=user_headers)
    assert r.status_code == 200, r.text
    data = r.json()["data"]

    # Job-level flags
    assert data["meta_json_required"] == payload["meta_json_required"]
    assert data["alto_required"] == payload["alto_required"]
    assert data["page_required"] == payload["page_required"]
    assert data["meta_json_uploaded"] == payload["meta_json_required"]

    # Build lookup by name from server response
    by_name = {img["name"]: img for img in data["images"]}

    # Ensure every payload image is present and flags match
    for pimg in payload["images"]:
        name = pimg["name"]
        assert name in by_name, f"Server response missing image named {name!r}"
        got = by_name[name]
        assert got["image_uploaded"] is True
        assert got["alto_uploaded"] is payload["alto_required"]
        assert got["page_uploaded"] is payload["page_required"]

    assert data["state"] == base_objects.ProcessingState.QUEUED.value


@pytest.mark.asyncio
async def test_user_to_readonly_job_access(client, admin_headers, worker_headers):
    # create random API key with USER role as ADMIN
    custom_key_label = f"test_user_to_readonly_job_access-{uuid4().hex}"
    r = await client.post(
        "/v1/admin/keys",
        headers=admin_headers,
        json={
            "label": custom_key_label,
            "role": base_objects.KeyRole.USER.value
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["code"] == AppCode.KEY_CREATED.value
    custom_key = body["data"]
    assert custom_key["secret"] is not None
    assert len(custom_key["secret"]) > 0


    custom_headers = {"X-API-Key": custom_key["secret"]}

    # Create job as USER
    job_payload = JOB_DEFINITION_PAYLOADS[0]
    r = await client.post("/v1/jobs", json=job_payload, headers=custom_headers)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["code"] == AppCode.JOB_CREATED.value
    job = body["data"]
    job_id = job["id"]

    # Upload required files
    for img in job_payload["images"]:
        name = img["name"]
        enc = _ename(name)
        img_bytes, ctype = make_white_image_bytes(os.path.splitext(name)[1])
        r = await _put_file(
            client,
            f"/v1/jobs/{job_id}/images/{enc}/files/image",
            "file",
            name,
            img_bytes,
            ctype,
            custom_headers,
        )
        assert r.status_code == 201, r.text

        if job_payload["alto_required"]:
            r = await _put_file(
                client,
                f"/v1/jobs/{job_id}/images/{enc}/files/alto",
                "file",
                f"{name.rsplit('.', 1)[0]}.xml",
                VALID_ALTO_XML,
                "application/xml",
                custom_headers,
            )
            assert r.status_code == 201, r.text

        if job_payload["page_required"]:
            r = await _put_file(
                client,
                f"/v1/jobs/{job_id}/images/{enc}/files/page",
                "file",
                f"{name.rsplit('.', 1)[0]}.xml",
                VALID_PAGE_XML,
                "application/xml",
                custom_headers,
            )
            assert r.status_code == 201, r.text

    if job_payload["meta_json_required"]:
        r = await client.put(
            f"/v1/jobs/{job_id}/files/metadata",
            headers=custom_headers,
            json={"meta": "dummy"},
        )
        assert r.status_code == 201, r.text

    # Lease job as WORKER
    r = await client.post(
        "/v1/jobs/lease",
        headers=worker_headers
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.JOB_LEASED.value
    leased_job = body["data"]
    leased_job_id = leased_job["id"]
    assert leased_job_id == job_id

    # Upload result
    r = await client.post(
        f"/v1/jobs/{job_id}/result/",
        headers=worker_headers,
        files={"file": ("result.zip", VALID_ZIP, "application/zip")},
    )

    assert r.status_code == 201, r.text
    body = r.json()
    assert body["code"] == AppCode.JOB_RESULT_UPLOADED.value

    # Mark job as done
    r = await client.patch(
        f"/v1/jobs/{leased_job_id}",
        headers=worker_headers,
        json={"state": base_objects.ProcessingState.DONE.value}
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.JOB_COMPLETED.value

    # Read with USER key
    r = await client.get(f"/v1/jobs/{job_id}", headers=custom_headers)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    # Download result with USER key
    r = await client.get(f"/v1/jobs/{job_id}/result", headers=custom_headers)
    assert r.status_code == 200, r.text
    assert r.headers["Content-Type"] == "application/zip"

    # Change key role to READONLY as ADMIN
    r = await client.patch(
        f"/v1/admin/keys/{custom_key_label}",
        headers=admin_headers,
        json={"role": base_objects.KeyRole.READONLY.value}
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.KEY_UPDATED.value

    # Read job with READONLY key
    r = await client.get(f"/v1/jobs/{job_id}", headers=custom_headers)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    # Download result with READONLY key
    r = await client.get(f"/v1/jobs/{job_id}/result", headers=custom_headers)
    assert r.status_code == 200, r.text
    assert r.headers["Content-Type"] == "application/zip"


    # Attempt to create new job with READONLY key
    r = await client.post("/v1/jobs", json=job_payload, headers=custom_headers)
    assert r.status_code == 403, r.text
    body = r.json()
    assert body["code"] == AppCode.API_KEY_ROLE_FORBIDDEN.value


#
# after JOB_MAX_ATTEMPTS, the job is automatically set to FAILED
#

@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=["JOB_FAILED_AFTER_MAX_ATTEMPTS"], indirect=True)
async def test_job_failed_after_max_attempts(client, worker_headers, failed_job, payload):
    job_id = failed_job["created_job"]["id"]

    r = await client.get(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    job = body["data"]
    assert job["state"] == base_objects.ProcessingState.FAILED.value