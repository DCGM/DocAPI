import io
import itertools
import os.path
import urllib.parse

import logging

import pytest
import pytest_asyncio

from doc_api.api.schemas import base_objects
from doc_api.api.schemas.responses import AppCode
from doc_api.tests.dummy_data import make_white_image_bytes, VALID_ALTO_XML, VALID_PAGE_XML

logger = logging.getLogger(__name__)

#
# Generate combinations of job definition payloads for testing
#

def generate_job_definition_payloads():
    base_images = [
        {"name": "img1.png", "order": 0},
        {"name": "img2.jpg", "order": 1},
        {"name": "img3.tif", "order": 2},
    ]

    payloads = []
    for combo in itertools.product([False, True], repeat=3):
        flags = dict(zip(["meta_json_required", "alto_required", "page_required"], combo))
        payloads.append({
            "images": base_images[:2] if any(combo) else base_images[:3],
            **flags,
        })
    return payloads

def job_definition_payload_id(payload):
    """Generate a readable ID for pytest logs."""
    image_count = len(payload["images"])
    # collect unique extensions
    exts = {img["name"].split(".")[-1] for img in payload["images"]}
    flags = [
        name.replace("_required", "")
        for name, val in payload.items()
        if name.endswith("_required") and val
    ]
    flags_str = "+".join(flags) if flags else "none"
    return f"{image_count}-imgs:{'+'.join(sorted(exts))}:{flags_str}"

JOB_DEFINITION_PAYLOADS = generate_job_definition_payloads()

@pytest.fixture
def payload(request):
    return request.param

#
# POST /v1/jobs - 201, 422
#

@pytest_asyncio.fixture
async def created_job(client, user_headers, payload):
    r = await client.post("/v1/jobs", json=payload, headers=user_headers)
    assert r.status_code == 201, r.text
    body = r.json()
    job = body["data"]
    return {"job": job, "payload": payload}


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", JOB_DEFINITION_PAYLOADS, ids=job_definition_payload_id, indirect=True)
async def test_creating_job(created_job):
    job = created_job["job"]
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
    job = created_job["job"]
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
# PUT /v1/jobs/{job_id}/images/{image_id}/files/image - 201, 200, 415, 404
# PUT /v1/jobs/{job_id}/images/{image_id}/files/alto - 201, 200
# PUT /v1/jobs/{job_id}/images/{image_id}/files/page - 201, 200
# PUT /v1/jobs/{job_id}/files/metadata - 201, 200
#


async def _put_file(client, url: str, field: str, filename: str, data: bytes, content_type: str, headers):
    files = {field: (filename, io.BytesIO(data), content_type)}
    r = await client.put(url, files=files, headers=headers)
    return r

def _ename(name: str) -> str:
    return urllib.parse.quote(name, safe="._-()[]")

@pytest_asyncio.fixture
async def job_with_required_uploads_by_payload_name(client, user_headers, created_job):
    job = created_job["job"]
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



    return {"job": job, "payload": payload}

@pytest.mark.asyncio
@pytest.mark.parametrize("payload", JOB_DEFINITION_PAYLOADS, ids=job_definition_payload_id, indirect=True)
async def test_upload_job_files(client, user_headers, job_with_required_uploads_by_payload_name):
    job = job_with_required_uploads_by_payload_name["job"]
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
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=["invalid-image"], indirect=True)
async def test_put_image_415(client, user_headers, created_job):
    job = created_job["job"]
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

    # Expect 415 Unsupported Media Type
    assert r.status_code == 415, r.text

    body = r.json()
    assert body["code"] == AppCode.IMAGE_INVALID.value


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=["invalid-image"], indirect=True)
async def test_put_image_404(client, user_headers, created_job):
    job = created_job["job"]
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
