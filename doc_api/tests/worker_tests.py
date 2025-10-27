import os
import pytest

from doc_api.api.schemas import base_objects
from doc_api.api.schemas.responses import AppCode
from doc_api.config import config
from doc_api.tests.conftest import user_headers
from doc_api.tests.dummy_data import JOB_DEFINITION_PAYLOADS, job_definition_payload_id, VALID_ZIP


#
# POST /v1/jobs/lease - 200
#

@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_LEASED], indirect=True)
async def test_post_job_lease_200_job_leased(client, worker_headers, lease_job):
    job = lease_job["created_job"]
    lease = lease_job["lease"]

    assert lease["id"] == job["id"], "This will only pass if there are not other jobs in QUEUED state apart from the one just created by this test."
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


#
# PATCH /v1/jobs/{job_id}/lease - 200
#

@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_LEASED], indirect=True)
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

#
# DELETE /v1/jobs/{job_id}/lease - 200
#

@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_LEASE_RELEASED], indirect=True)
async def test_delete_job_lease_200(client, worker_headers, user_headers, lease_job, payload):
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

    # worker should no longer have access
    r = await client.get(
        f"/v1/jobs/{job['id']}",
        headers=worker_headers,
    )
    assert r.status_code == 403, r.text
    body = r.json()
    assert body["status"] == 403
    assert body["code"] == AppCode.API_KEY_FORBIDDEN_FOR_JOB.value

#
# GET /v1/jobs/{job_id}/images/{image_id}/files/image - 200, 404, 410
# GET /v1/jobs/{job_id}/images/{image_id}/files/alto - 200, 404, 409, 410
# GET /v1/jobs/{job_id}/images/{image_id}/files/page - 200, 404, 409, 410
# GET /v1/jobs/{job_id}/files/metadata - 200
#

@pytest.mark.asyncio
@pytest.mark.parametrize("payload", JOB_DEFINITION_PAYLOADS, ids=job_definition_payload_id, indirect=True)
async def test_worker_get_uploaded_files_200(client, worker_headers, lease_job, payload):
    job_id = lease_job["created_job"]["id"]

    r = await client.get(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    # Metadata file if required
    if payload["meta_json_required"]:
        r = await client.get(
            f"/v1/jobs/{job_id}/files/metadata",
            headers=worker_headers,
        )
        assert r.status_code == 200, r.text
        assert r.headers["content-type"] == "application/json", "Expected JSON content type for metadata"
        assert len(r.content) > 0, "Metadata file content should not be empty"

    job = body["data"]
    for image in job["images"]:
        image_id = image["id"]

        # Image file
        r = await client.get(
            f"/v1/jobs/{job_id}/images/{image_id}/files/image",
            headers=worker_headers,
        )
        assert r.status_code == 200, r.text
        assert r.headers["content-type"].startswith("image/"), "Expected image content type"
        assert len(r.content) > 0, "Image file content should not be empty"

        # ALTO file if required
        if payload["alto_required"]:
            r = await client.get(
                f"/v1/jobs/{job_id}/images/{image_id}/files/alto",
                headers=worker_headers,
            )
            assert r.status_code == 200, r.text
            assert r.headers["content-type"] == "application/xml", "Expected XML content type for ALTO"
            assert len(r.content) > 0, "ALTO file content should not be empty"

        # PAGE file if required
        if payload["page_required"]:
            r = await client.get(
                f"/v1/jobs/{job_id}/images/{image_id}/files/page",
                headers=worker_headers,
            )
            assert r.status_code == 200, r.text
            assert r.headers["content-type"] == "application/xml", "Expected XML content type for PAGE"
            assert len(r.content) > 0, "PAGE file content should not be empty"


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


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.IMAGE_GONE], indirect=True)
async def test_get_image_410(client, worker_headers, lease_job, payload):
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

        image_path = os.path.join(config.JOBS_DIR, str(job_id), f"{image_id}.jpg")
        assert os.path.exists(image_path), (f"Image file should exist at {image_path}, "
                                            f"this will only pass if testing locally with BASE_DIR setup.")
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


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[2]], ids=[AppCode.ALTO_GONE], indirect=True)
async def test_get_alto_410(client, worker_headers, lease_job, payload):
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

        alto_path = os.path.join(config.JOBS_DIR, str(job_id), f"{image_id}.alto.xml")
        assert os.path.exists(alto_path), (f"ALTO file should exist at {alto_path}, "
                                           f"this will only pass if testing locally with BASE_DIR setup.")
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


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[3]], ids=[AppCode.PAGE_GONE], indirect=True)
async def test_get_page_410(client, worker_headers, lease_job, payload):
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

        page_path = os.path.join(config.JOBS_DIR, str(job_id), f"{image_id}.page.xml")
        assert os.path.exists(page_path), (f"PAGE file should exist at {page_path}, "
                                           f"this will only pass if testing locally with BASE_DIR setup.")
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


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[-1]], ids=[AppCode.META_JSON_GONE], indirect=True)
async def test_get_metadata_410(client, worker_headers, lease_job, payload):
    job_id = lease_job["created_job"]["id"]

    r = await client.get(
        f"/v1/jobs/{job_id}",
        headers=worker_headers,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    metadata_path = os.path.join(config.JOBS_DIR, str(job_id), "meta.json")
    assert os.path.exists(metadata_path), (f"Metadata file should exist at {metadata_path}, "
                                           f"this will only pass if testing locally with BASE_DIR setup.")
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


#
# POST /v1/jobs/{job_id}/result/ - 201, 200, 415
#

@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_RESULT_UPLOADED.value], indirect=True)
async def test_post_job_result_201(client, worker_headers, job_with_result, payload):
    pass

@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_RESULT_REUPLOADED.value], indirect=True)
async def test_post_job_result_200(client, worker_headers, job_with_result, payload):
    job_id = job_with_result["lease"]["id"]

    r = await client.post(
        f"/v1/jobs/{job_id}/result/",
        headers=worker_headers,
        files={"file": ("result.zip", VALID_ZIP, "application/zip")},
    )

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RESULT_REUPLOADED.value


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_RESULT_INVALID], indirect=True)
async def test_post_job_result_415(client, worker_headers, lease_job, payload):
    job_id = lease_job["lease"]["id"]

    r = await client.post(
        f"/v1/jobs/{job_id}/result/",
        headers=worker_headers,
        files={"result": ("result.txt", b"This is not a zip file.", "text/plain")},
    )

    assert r.status_code == 415, r.text
    body = r.json()
    assert body["status"] == 415
    assert body["code"] == AppCode.JOB_RESULT_INVALID.value


#
# PATCH /v1/jobs/{job_id} - 200, 400, 409
#

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


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_COMPLETED], indirect=True)
async def test_patch_job_200_job_marked_done(client, worker_headers, job_marked_done, payload):
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

@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_ALREADY_COMPLETED], indirect=True)
async def test_patch_job_200_job_already_marked_done(client, worker_headers, job_marked_done, payload):
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
