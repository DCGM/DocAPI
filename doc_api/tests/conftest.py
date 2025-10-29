import os
import asyncio
from typing import Optional

import httpx

import io
import os.path
import urllib.parse

import pytest
import pytest_asyncio

from doc_api.api.schemas import base_objects
from doc_api.tests.dummy_data import make_white_image_bytes, VALID_ALTO_XML, VALID_PAGE_XML, VALID_ZIP

from doc_api.api.schemas.responses import AppCode
from doc_api.config import config

# -----------------------------------------------------------------------------
# CLI options & env fallbacks
# -----------------------------------------------------------------------------
def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--base-url",
        action="store",
        default=os.getenv("APP_BASE_URL", config.APP_BASE_URL),
        help="REQUIRED: Base URL of a running DocAPI instance (e.g., http://localhost:9999).",
    )
    parser.addoption("--api-key-readonly", action="store", default=os.getenv("TEST_READONLY_KEY", config.TEST_READONLY_KEY))
    parser.addoption("--api-key-user", action="store", default=os.getenv("TEST_USER_KEY", config.TEST_USER_KEY))
    parser.addoption("--api-key-worker", action="store", default=os.getenv("TEST_WORKER_KEY", config.TEST_WORKER_KEY))
    parser.addoption("--api-key-admin", action="store", default=os.getenv("TEST_ADMIN_KEY", config.TEST_ADMIN_KEY))
    parser.addoption("--http-timeout", action="store",
                     type=float,
                     default=float(os.getenv("TEST_HTTP_TIMEOUT", config.TEST_HTTP_TIMEOUT)),
                     help="HTTP client timeout in seconds.")


@pytest.fixture(scope="session")
def _opts(request):
    return {
        "APP_BASE_URL": request.config.getoption("--base-url"),
        "TEST_READONLY_KEY": request.config.getoption("--api-key-readonly"),
        "TEST_USER_KEY": request.config.getoption("--api-key-user"),
        "TEST_WORKER_KEY": request.config.getoption("--api-key-worker"),
        "TEST_ADMIN_KEY": request.config.getoption("--api-key-admin"),
        "TEST_HTTP_TIMEOUT": request.config.getoption("--http-timeout"),
    }


# -----------------------------------------------------------------------------
# Single event loop for the entire test session
# -----------------------------------------------------------------------------
@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


# -----------------------------------------------------------------------------
# Validate required APP_BASE_URL once per session
# -----------------------------------------------------------------------------
@pytest.fixture(scope="session", autouse=True)
def _require_base_url(_opts):
    if not _opts["APP_BASE_URL"]:
        pytest.exit(
            "Remote-only test run requires --base-url (or APP_BASE_URL env). "
            "Example: pytest --base-url http://localhost:9999",
            returncode=2,
        )


# -----------------------------------------------------------------------------
# httpx client pointed at the running instance
# -----------------------------------------------------------------------------
@pytest_asyncio.fixture()
async def client(_opts):
    timeout = httpx.Timeout(_opts["TEST_HTTP_TIMEOUT"])
    async with httpx.AsyncClient(base_url=_opts["APP_BASE_URL"], timeout=timeout, follow_redirects=True) as ac:
        yield ac


# -----------------------------------------------------------------------------
# Header fixtures (skip tests if a needed key isn't supplied)
# -----------------------------------------------------------------------------
def _headers_or_skip(key: Optional[str], which: str):
    if not key:
        pytest.skip(
            f"{which} API key not provided for remote target. "
            f"Pass --api-key-{which.lower()} or set TEST_{which.upper()}_KEY."
        )
    return {"X-API-Key": key}

@pytest.fixture()
def readonly_headers(_opts):
    return _headers_or_skip(_opts["TEST_READONLY_KEY"], "READONLY")

@pytest.fixture()
def user_headers(_opts):
    return _headers_or_skip(_opts["TEST_USER_KEY"], "USER")

@pytest.fixture()
def worker_headers(_opts):
    return _headers_or_skip(_opts["TEST_WORKER_KEY"], "WORKER")

@pytest.fixture()
def admin_headers(_opts):
    return _headers_or_skip(_opts["TEST_ADMIN_KEY"], "ADMIN")

# -----------------------------------------------------------------------------

@pytest.fixture
def payload(request):
    return request.param


@pytest_asyncio.fixture
async def created_job(client, user_headers, admin_headers, payload):
    r = await client.post("/v1/jobs", json=payload, headers=user_headers)

    assert r.status_code == 201, r.text

    body = r.json()
    assert body["code"] == AppCode.JOB_CREATED.value
    assert body["status"] == 201

    job = body["data"]

    yield {"created_job": job, "payload": payload}

    job_id = job["id"]
    r = await client.patch(f"/v1/admin/jobs/{job_id}",
                           headers=admin_headers,
                           json={"state": base_objects.ProcessingState.DONE.value})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == 200
    assert body["code"] == AppCode.JOB_UPDATED.value


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


async def _put_file(client, url: str, field: str, filename: str, data: bytes, content_type: str, headers):
    files = {field: (filename, io.BytesIO(data), content_type)}
    r = await client.put(url, files=files, headers=headers)
    return r

def _ename(name: str) -> str:
    return urllib.parse.quote(name, safe="._-()[]")

@pytest_asyncio.fixture
async def job_with_required_uploads_by_payload_name(client, user_headers, created_job):
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

    return {"created_job": job, "payload": payload}


@pytest_asyncio.fixture
async def lease_job(client, worker_headers, job_with_required_uploads_by_payload_name):
    job = job_with_required_uploads_by_payload_name["created_job"]
    payload = job_with_required_uploads_by_payload_name["payload"]

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
        f"/v1/jobs/{job_id}/result/",
        headers=worker_headers,
        files={"file": ("result.zip", VALID_ZIP, "application/zip")},
    )

    assert r.status_code == 201, r.text
    body = r.json()
    assert body["status"] == 201
    assert body["code"] == AppCode.JOB_RESULT_UPLOADED.value

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