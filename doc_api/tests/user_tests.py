import io
import os.path
import urllib.parse

import logging

import pytest
import pytest_asyncio

from doc_api.api.schemas import base_objects
from doc_api.api.schemas.responses import AppCode
from doc_api.tests.conftest import _ename, _put_file
from doc_api.tests.dummy_data import make_white_image_bytes, VALID_ALTO_XML, VALID_PAGE_XML, JOB_DEFINITION_PAYLOADS, \
    job_definition_payload_id

logger = logging.getLogger(__name__)


#
# POST /v1/jobs - 201, 422
#

@pytest.mark.asyncio
@pytest.mark.parametrize("payload", JOB_DEFINITION_PAYLOADS, ids=job_definition_payload_id, indirect=True)
async def test_post_job_201(created_job):
    job = created_job["created_job"]
    payload = created_job["payload"]
    assert job["state"] == base_objects.ProcessingState.NEW.value
    assert job["meta_json_required"] == payload["meta_json_required"]
    assert job["alto_required"] == payload["alto_required"]
    assert job["page_required"] == payload["page_required"]
    assert len(job["images"]) == len(payload["images"])
    for img_payload, img_body in zip(payload["images"], job["images"]):
        assert img_payload["name"] == img_body["name"]
        assert img_payload["order"] == img_body["order"]
        assert img_body["image_uploaded"] is False
        assert img_body["alto_uploaded"] is False
        assert img_body["page_uploaded"] is False


@pytest.mark.asyncio
async def test_post_job_422(client, user_headers):
    invalid_payload = {
        "images": [
            {"order": 0}, # name missing
            {"name": "b.png", "order": "one"},  # order wrong type
        ],
        "meta_json_required": False,
        "alto_required": "ffff",   # wrong type
        "page_required": False,
    }
    r = await client.post("/v1/jobs", json=invalid_payload, headers=user_headers)
    assert r.status_code == 422, r.text
    body = r.json()
    assert body["code"] == AppCode.REQUEST_VALIDATION_ERROR.value
    details = body.get("details")
    assert isinstance(details, list)

    paths = [err.get("loc") for err in details if isinstance(err, dict)]
    assert ["body", "images", 0, "name"] in paths
    assert ["body", "images", 1, "order"] in paths
    assert ["body", "alto_required"] in paths


#
# GET /v1/jobs/{job_id} - 200
#

@pytest.mark.asyncio
@pytest.mark.parametrize("payload", JOB_DEFINITION_PAYLOADS, ids=job_definition_payload_id, indirect=True)
async def test_retrieving_job(client, user_headers, created_job):
    job = created_job["created_job"]
    job_id = job["id"]

    r = await client.get(f"/v1/jobs/{job_id}", headers=user_headers)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.JOB_RETRIEVED.value
    data = body["data"]

    assert data["id"] == job_id
    assert data["state"] == base_objects.ProcessingState.NEW.value
    assert len(data["images"]) == len(job["images"])
    for img_post, img_get in zip(job["images"], data["images"]):
        assert img_post == img_get


#
# PUT /v1/jobs/{job_id}/images/{image_id}/files/image - 201, 200, 404, 409, 415
# PUT /v1/jobs/{job_id}/images/{image_id}/files/alto - 201, 200, 400, 404, 409, 422
# PUT /v1/jobs/{job_id}/images/{image_id}/files/page - 201, 200, 400, 404, 409, 422
# PUT /v1/jobs/{job_id}/files/metadata - 201, 200, 409, 422
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
    assert data["meta_json_uploaded"] is bool(payload["meta_json_required"])

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
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.IMAGE_NOT_FOUND_FOR_JOB.value], indirect=True)
async def test_put_image_404(client, user_headers, created_job):
    job = created_job["created_job"]
    job_id = job["id"]

    # Pick the first image name from the payload to make the test deterministic
    invalid_name = 'this_is_invalid_name.jpg'
    enc = _ename(invalid_name)

    img_bytes, ctype = make_white_image_bytes(os.path.splitext(invalid_name)[1])
    r = await _put_file(
        client,
        f"/v1/jobs/{job_id}/images/{enc}/files/image",
        "file",
        invalid_name,
        img_bytes,
        ctype,
        user_headers,
    )

    assert r.status_code == 404, r.text

    body = r.json()
    assert body["code"] == AppCode.IMAGE_NOT_FOUND_FOR_JOB.value


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_NOT_IN_NEW.value], indirect=True)
async def test_put_image_409(client, user_headers, job_with_required_uploads_by_payload_name):
    job = job_with_required_uploads_by_payload_name["created_job"]
    payload = job_with_required_uploads_by_payload_name["payload"]
    job_id = job["id"]

    name = payload["images"][0]["name"]
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

    assert r.status_code == 409, r.text

    body = r.json()
    assert body["code"] == AppCode.JOB_NOT_IN_NEW.value


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.IMAGE_INVALID.value], indirect=True)
async def test_put_image_415(client, user_headers, created_job):
    job = created_job["created_job"]
    job_id = job["id"]

    # Pick the first image name from the payload to make the test deterministic
    bad_name = created_job["payload"]["images"][0]["name"]
    enc = _ename(bad_name)

    # Hardcoded invalid file contents and content type
    invalid_bytes = b"This is not a valid image file at all!"
    invalid_ctype = "application/octet-stream"

    # Upload invalid image to endpoint
    r = await _put_file(
        client,
        f"/v1/jobs/{job_id}/images/{enc}/files/image",
        "file",
        bad_name,
        invalid_bytes,
        invalid_ctype,
        user_headers,
    )

    assert r.status_code == 415, r.text

    body = r.json()
    assert body["code"] == AppCode.IMAGE_INVALID.value


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[2]], ids=[AppCode.XML_PARSE_ERROR.value], indirect=True)
async def test_put_alto_400(client, user_headers, created_job):
    job = created_job["created_job"]
    job_id = job["id"]

    name = created_job["payload"]["images"][0]["name"]
    enc = _ename(name)

    invalid_xml = b"<this is not valid xml>"

    r = await _put_file(
        client,
        f"/v1/jobs/{job_id}/images/{enc}/files/alto",
        "file",
        f"{name.rsplit('.', 1)[0]}.xml",
        invalid_xml,
        "application/xml",
        user_headers,
    )

    assert r.status_code == 400, r.text

    body = r.json()
    assert body["code"] == AppCode.XML_PARSE_ERROR.value


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[2]], ids=[AppCode.IMAGE_NOT_FOUND_FOR_JOB.value], indirect=True)
async def test_put_alto_404(client, user_headers, created_job):
    job = created_job["created_job"]
    job_id = job["id"]

    invalid_name = 'this_is_invalid_name.jpg'
    enc = _ename(invalid_name)

    r = await _put_file(
        client,
        f"/v1/jobs/{job_id}/images/{enc}/files/alto",
        "file",
        f"{invalid_name.rsplit('.', 1)[0]}.xml",
        VALID_ALTO_XML,
        "application/xml",
        user_headers,
    )

    assert r.status_code == 404, r.text

    body = r.json()
    assert body["code"] == AppCode.IMAGE_NOT_FOUND_FOR_JOB.value


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.ALTO_NOT_REQUIRED.value], indirect=True)
async def test_put_alto_409_alto_not_required(client, user_headers, created_job):
    job = created_job["created_job"]
    job_id = job["id"]

    name = created_job["payload"]["images"][0]["name"]
    enc = _ename(name)

    r = await _put_file(
        client,
        f"/v1/jobs/{job_id}/images/{enc}/files/alto",
        "file",
        f"{name.rsplit('.', 1)[0]}.xml",
        VALID_ALTO_XML,
        "application/xml",
        user_headers,
    )

    assert r.status_code == 409, r.text

    body = r.json()
    assert body["code"] == AppCode.ALTO_NOT_REQUIRED.value


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[2]], ids=[AppCode.JOB_NOT_IN_NEW.value], indirect=True)
async def test_put_alto_409_job_not_in_new(client, user_headers, job_with_required_uploads_by_payload_name):
    job = job_with_required_uploads_by_payload_name["created_job"]
    payload = job_with_required_uploads_by_payload_name["payload"]
    job_id = job["id"]

    name = payload["images"][0]["name"]
    enc = _ename(name)

    r = await _put_file(
        client,
        f"/v1/jobs/{job_id}/images/{enc}/files/alto",
        "file",
        f"{name.rsplit('.', 1)[0]}.xml",
        VALID_ALTO_XML,
        "application/xml",
        user_headers,
    )

    assert r.status_code == 409, r.text

    body = r.json()
    assert body["code"] == AppCode.JOB_NOT_IN_NEW.value


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[2]], ids=[AppCode.ALTO_SCHEMA_INVALID.value], indirect=True)
async def test_put_alto_422(client, user_headers, created_job):
    job = created_job["created_job"]
    job_id = job["id"]

    name = created_job["payload"]["images"][0]["name"]
    enc = _ename(name)

    invalid_alto_xml = b"""<?xml version="1.0" encoding="UTF-8"?>
                            <halto xmlns="http://www.loc.gov/standards/alto/ns-v4#">
                              <Layout></Layout>
                            </halto>"""
    r = await _put_file(
        client,
        f"/v1/jobs/{job_id}/images/{enc}/files/alto",
        "file",
        f"{name.rsplit('.', 1)[0]}.xml",
        invalid_alto_xml,
        "application/xml",
        user_headers,
    )

    assert r.status_code == 422, r.text

    body = r.json()
    assert body["code"] == AppCode.ALTO_SCHEMA_INVALID.value


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[3]], ids=[AppCode.XML_PARSE_ERROR], indirect=True)
async def test_put_page_400(client, user_headers, created_job):
    job = created_job["created_job"]
    job_id = job["id"]

    name = created_job["payload"]["images"][0]["name"]
    enc = _ename(name)

    invalid_xml = b"<this is not valid xml>"

    r = await _put_file(
        client,
        f"/v1/jobs/{job_id}/images/{enc}/files/page",
        "file",
        f"{name.rsplit('.', 1)[0]}.xml",
        invalid_xml,
        "application/xml",
        user_headers,
    )

    assert r.status_code == 400, r.text

    body = r.json()
    assert body["code"] == AppCode.XML_PARSE_ERROR.value


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[3]], ids=[AppCode.IMAGE_NOT_FOUND_FOR_JOB.value], indirect=True)
async def test_put_page_404(client, user_headers, created_job):
    job = created_job["created_job"]
    job_id = job["id"]

    invalid_name = 'this_is_invalid_name.jpg'
    enc = _ename(invalid_name)

    r = await _put_file(
        client,
        f"/v1/jobs/{job_id}/images/{enc}/files/page",
        "file",
        f"{invalid_name.rsplit('.', 1)[0]}.xml",
        VALID_PAGE_XML,
        "application/xml",
        user_headers,
    )

    assert r.status_code == 404, r.text

    body = r.json()
    assert body["code"] == AppCode.IMAGE_NOT_FOUND_FOR_JOB.value


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.PAGE_NOT_REQUIRED.value], indirect=True)
async def test_put_page_409_page_not_required(client, user_headers, created_job):
    job = created_job["created_job"]
    job_id = job["id"]

    name = created_job["payload"]["images"][0]["name"]
    enc = _ename(name)

    r = await _put_file(
        client,
        f"/v1/jobs/{job_id}/images/{enc}/files/page",
        "file",
        f"{name.rsplit('.', 1)[0]}.xml",
        VALID_PAGE_XML,
        "application/xml",
        user_headers,
    )

    assert r.status_code == 409, r.text

    body = r.json()
    assert body["code"] == AppCode.PAGE_NOT_REQUIRED.value


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[3]], ids=[AppCode.JOB_NOT_IN_NEW.value], indirect=True)
async def test_put_page_409_job_not_in_new(client, user_headers, job_with_required_uploads_by_payload_name):
    job = job_with_required_uploads_by_payload_name["created_job"]
    payload = job_with_required_uploads_by_payload_name["payload"]
    job_id = job["id"]

    name = payload["images"][0]["name"]
    enc = _ename(name)

    r = await _put_file(
        client,
        f"/v1/jobs/{job_id}/images/{enc}/files/page",
        "file",
        f"{name.rsplit('.', 1)[0]}.xml",
        VALID_PAGE_XML,
        "application/xml",
        user_headers,
    )

    assert r.status_code == 409, r.text

    body = r.json()
    assert body["code"] == AppCode.JOB_NOT_IN_NEW.value


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[3]], ids=[AppCode.PAGE_SCHEMA_INVALID.value], indirect=True)
async def test_put_page_422(client, user_headers, created_job):
    job = created_job["created_job"]
    job_id = job["id"]

    name = created_job["payload"]["images"][0]["name"]
    enc = _ename(name)

    invalid_page_xml = b"""<?xml version="1.0" encoding="UTF-8"?>
                            <hpage xmlns="http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15">
                              <Layout></Layout>
                            </hpage>"""
    r = await _put_file(
        client,
        f"/v1/jobs/{job_id}/images/{enc}/files/page",
        "file",
        f"{name.rsplit('.', 1)[0]}.xml",
        invalid_page_xml,
        "application/xml",
        user_headers,
    )

    assert r.status_code == 422, r.text

    body = r.json()
    assert body["code"] == AppCode.PAGE_SCHEMA_INVALID.value


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.META_JSON_NOT_REQUIRED.value], indirect=True)
async def test_put_metadata_409_meta_json_not_required(client, user_headers, created_job):
    job = created_job["created_job"]
    job_id = job["id"]

    r = await client.put(
        f"/v1/jobs/{job_id}/files/metadata",
        headers=user_headers,
        json={"meta": "dummy"},
    )

    assert r.status_code == 409, r.text

    body = r.json()
    assert body["code"] == AppCode.META_JSON_NOT_REQUIRED.value


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[1]], ids=[AppCode.JOB_NOT_IN_NEW.value], indirect=True)
async def test_put_metadata_409_job_not_in_new(client, user_headers, job_with_required_uploads_by_payload_name):
    job = job_with_required_uploads_by_payload_name["created_job"]
    job_id = job["id"]

    r = await client.put(
        f"/v1/jobs/{job_id}/files/metadata",
        headers=user_headers,
        json={"meta": "dummy"},
    )

    assert r.status_code == 409, r.text

    body = r.json()
    assert body["code"] == AppCode.JOB_NOT_IN_NEW.value


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[-1]], ids=[AppCode.REQUEST_VALIDATION_ERROR.value], indirect=True)
async def test_put_metadata_422(client, user_headers, created_job):
    job = created_job["created_job"]
    job_id = job["id"]

    invalid_payload = "this is not json"

    r = await client.put(
        f"/v1/jobs/{job_id}/files/metadata",
        headers=user_headers,
        content=invalid_payload
    )

    assert r.status_code == 422, r.text

    body = r.json()
    assert body["code"] == AppCode.REQUEST_VALIDATION_ERROR.value
    details = body.get("details")
    assert isinstance(details, list)


#
# PATCH /v1/jobs/{job_id} - 200, 409
#

@pytest_asyncio.fixture
async def cancel_new_job(client, user_headers, created_job):
    job_id = created_job["created_job"]["id"]

    r = await client.patch(
        f"/v1/jobs/{job_id}",
        headers=user_headers,
        json={"state": base_objects.ProcessingState.CANCELLED.value},
    )
    assert r.status_code == 200, r.text

    body = r.json()
    assert body["code"] == AppCode.JOB_CANCELLED.value
    assert body["status"] == 200

    return {"job": created_job["created_job"], "payload": created_job["payload"]}

@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_CANCELLED], indirect=True)
async def test_patch_job_200_cancel_new_job(client, user_headers, cancel_new_job):
    job = cancel_new_job["job"]
    job_id = job["id"]

    r = await client.get(f"/v1/jobs/{job_id}", headers=user_headers)
    assert r.status_code == 200, r.text
    data = r.json()["data"]
    assert data["state"] == base_objects.ProcessingState.CANCELLED.value


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_CANCELLED], indirect=True)
async def test_patch_job_200_cancel_queued_job(client, user_headers, job_with_required_uploads_by_payload_name):
    job = job_with_required_uploads_by_payload_name["created_job"]
    job_id = job["id"]

    r = await client.patch(
        f"/v1/jobs/{job_id}",
        headers=user_headers,
        json={"state": base_objects.ProcessingState.CANCELLED.value},
    )
    assert r.status_code == 200, r.text

    body = r.json()
    assert body["code"] == AppCode.JOB_CANCELLED.value

    r = await client.get(f"/v1/jobs/{job_id}", headers=user_headers)
    assert r.status_code == 200, r.text
    data = r.json()["data"]
    assert data["state"] == base_objects.ProcessingState.CANCELLED.value


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_CANCELLED], indirect=True)
async def test_patch_job_200_cancel_processing_job(client, user_headers, lease_job):
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

    r = await client.get(f"/v1/jobs/{job_id}", headers=user_headers)

    assert r.status_code == 200, r.text

    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_RETRIEVED.value

    data = body["data"]
    assert data["state"] == base_objects.ProcessingState.CANCELLED.value


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_UNCANCELLABLE], indirect=True)
async def test_patch_job_409_cancel_cancelled_job(client, user_headers, cancel_new_job):
    job = cancel_new_job["job"]
    job_id = job["id"]

    r = await client.patch(
        f"/v1/jobs/{job_id}",
        headers=user_headers,
        json={"state": base_objects.ProcessingState.CANCELLED.value},
    )
    assert r.status_code == 409, r.text

    body = r.json()
    assert body["code"] == AppCode.JOB_UNCANCELLABLE.value



