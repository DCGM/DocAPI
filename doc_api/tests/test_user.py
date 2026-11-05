from doc_api.tests.helpers import assert_route_invariant, delete_job_after_exists, delete_before_file_install, delete_job_before_crud
import asyncio
import io
import builtins
import tempfile
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
import subprocess
import sys
from itertools import combinations, product
import os.path
import logging
from datetime import datetime, timedelta, timezone
from functools import partial
from pathlib import Path

from uuid import UUID, uuid4
from doc_api.config import config
from doc_api.db import model
from doc_api.api import main
from doc_api.api.routes import general_routes, user_routes
from doc_api.api.database import DBError

import pytest

from doc_api.api import cleanup
from doc_api.tests.helpers import (assert_generated_xml_save, xml_validation_combinations,
                                   assert_multipart_rejected, assert_deletion_timestamps, _ename, _put_file,
                                   OPENAPI_SECTION_OPERATIONS, OPENAPI_VISIBILITY_ROUTES,
                                   interrupted_upload_body, interrupted_upload_headers,
                                   assert_internal_transfer_error, assert_records_unchanged, interrupt_download)
from doc_api.api.schemas.base_objects import ProcessingState
from doc_api.api.schemas import base_objects
from doc_api.api.schemas.responses import AppCode
from doc_api.tests.dummy_data import make_white_image_bytes, VALID_ALTO_XML, VALID_PAGE_XML, JOB_DEFINITION_PAYLOADS, \
    job_definition_payload_id, VALID_ZIP, make_xml_payload




logger = logging.getLogger(__name__)


#
# POST /v1/jobs - 201, 403, 404, 409, 422
#

@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", JOB_DEFINITION_PAYLOADS, ids=partial(job_definition_payload_id, app_code=AppCode.JOB_CREATED.value), indirect=True)
async def test_post_job_201(created_job):
    job = created_job["created_job"]
    payload = created_job["payload"]
    assert job["state"] == base_objects.ProcessingState.NEW.value
    assert job["meta_json_required"] == payload["meta_json_required"]
    assert job["alto_required"] == payload["alto_required"]
    assert job["page_required"] == payload["page_required"]
    assert len(job["images"]) == len(payload["images"])
    assert job["image_count"] == len(payload["images"])
    for img_payload, img_body in zip(payload["images"], job["images"]):
        assert img_payload["name"] == img_body["name"]
        assert img_payload["order"] == img_body["order"]
        assert img_body["image_uploaded"] is False
        assert img_body["alto_uploaded"] is False
        assert img_body["page_uploaded"] is False


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", JOB_DEFINITION_PAYLOADS, ids=partial(job_definition_payload_id, app_code=AppCode.JOB_CREATED.value), indirect=True)
async def test_post_job_201_with_engine(created_job_with_engine):
    job = created_job_with_engine["created_job"]
    engine = created_job_with_engine["engine"]
    payload = created_job_with_engine["payload"]
    assert job["engine_name"] == engine["name"]
    assert job["engine_version"] == engine["version"]
    assert job["state"] == base_objects.ProcessingState.NEW.value
    assert job["meta_json_required"] == payload["meta_json_required"]
    assert job["alto_required"] == payload["alto_required"]
    assert job["page_required"] == payload["page_required"]
    assert len(job["images"]) == len(payload["images"])
    assert job["image_count"] == len(payload["images"])
    for img_payload, img_body in zip(payload["images"], job["images"]):
        assert img_payload["name"] == img_body["name"]
        assert img_payload["order"] == img_body["order"]
        assert img_body["image_uploaded"] is False
        assert img_body["alto_uploaded"] is False
        assert img_body["page_uploaded"] is False


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[AppCode.REQUEST_VALIDATION_ERROR.value])
async def test_post_job_422_invalid_key_values(client, user_headers, dummy):
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


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[AppCode.REQUEST_VALIDATION_ERROR.value])
async def test_post_job_422_extra_keys(client, user_headers, dummy):
    invalid_payload = {
        "images": [
            {"name": "a.png", "order": 0, "extra_key": "extra_value"},
        ],
        "meta_json_required": False,
        "alto_required": False,
        "page_required": False,
        "unexpected_key": 123,
    }
    r = await client.post("/v1/jobs", json=invalid_payload, headers=user_headers)
    assert r.status_code == 422, r.text
    body = r.json()
    assert body["code"] == AppCode.REQUEST_VALIDATION_ERROR.value
    details = body.get("details")
    assert isinstance(details, list)

    paths = [err.get("loc") for err in details if isinstance(err, dict)]
    assert ["body", "images", 0, "extra_key"] in paths
    assert ["body", "unexpected_key"] in paths


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[AppCode.REQUEST_VALIDATION_ERROR.value])
async def test_post_job_422_duplicate_image_names(client, user_headers, dummy):
    invalid_payload = {
        "images": [
            {"name": "a.png", "order": 0},
            {"name": "a.png", "order": 1},
            {"name": "b.png", "order": 2},
            {"name": "b.png", "order": 3},
            {"name": "c.png", "order": 4},
            {"name": "C.PNG", "order": 5},
            {"name": "d.png", "order": 6},
            {"name": " d.png ", "order": 7},
        ],
        "meta_json_required": False,
        "alto_required": False,
        "page_required": False,
    }
    r = await client.post("/v1/jobs", json=invalid_payload, headers=user_headers)
    assert r.status_code == 422, r.text
    body = r.json()
    assert body["code"] == AppCode.REQUEST_VALIDATION_ERROR.value
    details = body.get("details")
    assert isinstance(details, list)

    duplicate_names = set()
    for item in details:
        if item["loc"] == ["body", "images", "name"]:
            if item["msg"].startswith("Duplicate image names found:"):
                duplicate_names = duplicate_names.union(set([x.strip() for x in item["msg"].split(":")[-1].strip().split(",")]))

    assert duplicate_names == {"a.png", "b.png"}

    paths = [err.get("loc") for err in details if isinstance(err, dict)]
    assert ["body", "images", "name"] in paths


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_post_job_422_invalid_request(client, user_headers):
    response = await client.request('POST', '/v1/jobs', headers=user_headers, json={})
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_post_job_404_engine_not_found(client, user_headers):
    response = await client.post("/v1/jobs", headers=user_headers, json={"images": [], "engine_name": "missing"})
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.ENGINE_NOT_FOUND.value


@pytest.mark.asyncio
async def test_post_job_409_engine_inactive(client, user_headers, engine_factory):
    await engine_factory(active=False)
    response = await client.post("/v1/jobs", headers=user_headers, json={"images": [], "engine_name": "Engine"})
    assert response.status_code == 409, response.text
    assert response.json()["code"] == AppCode.ENGINE_INACTIVE.value


@pytest.mark.asyncio
async def test_post_job_403_explicit_engine_version(client, user_headers):
    response = await client.post("/v1/jobs", headers=user_headers,
                                 json={"images": [], "engine_name": "Engine", "engine_version": "1"})
    assert response.status_code == 403, response.text
    assert response.json()["code"] == AppCode.API_KEY_FORBIDDEN_FOR_ENGINE_VERSION.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("required,allowed,name", [(True, set(), "page"), (False, {".jpg"}, "page.png")])
async def test_post_job_422_extension_policy(client, user_headers, monkeypatch, required, allowed, name):
    monkeypatch.setattr(config, "REQUIRE_IMAGE_EXTENSION", required)
    monkeypatch.setattr(config, "ALLOWED_IMAGE_EXTENSIONS", allowed)
    response = await client.post("/v1/jobs", headers=user_headers, json={"images": [{"name": name, "order": 0}]})
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value
    assert response.json()["details"][0]["loc"] == ["body", "images", 0, "name"]




@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("stage", ["create", "images", "engine"])
@pytest.mark.asyncio
async def test_post_job_500_unexpected_crud_result(client, user_headers, engine_factory, monkeypatch, invariant_errors, stage):
    payload = {"images": []}
    code = AppCode.API_KEY_VALID
    if stage == "create":
        monkeypatch.setattr(user_routes.user_cruds, "create_job", AsyncMock(return_value=(None, code)))
    elif stage == "images":
        monkeypatch.setattr(general_routes.general_cruds, "get_job_images", AsyncMock(return_value=([], code)))
    else:
        engine = await engine_factory(active=True)
        payload["engine_name"] = engine.name
        monkeypatch.setattr(general_routes.general_cruds, "get_engine", AsyncMock(return_value=(None, code)))
    response = await client.post("/v1/jobs", headers=user_headers, json=payload)
    assert_route_invariant(response, invariant_errors, code)



@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("sources", list(product(["url", "upload", "missing"], repeat=3)),
                         ids=lambda values: "-".join(values))
@pytest.mark.asyncio
async def test_post_job_201_mixed_input_links(client, user_headers, worker_headers, db_sessions, sources):
    from doc_api.api.job_readiness import queue_ready_jobs
    kinds = ["image", "alto", "page"]
    linked = {"name": "linked.jpg", "order": 0,
              **{f"{kind}_url": f"https://source.invalid/linked.{kind}" for kind in kinds}}
    mixed = {"name": "mixed.jpg", "order": 1,
             **{f"{kind}_url": f"https://source.invalid/mixed.{kind}" for kind, source in zip(kinds, sources) if source == "url"}}
    payload = {"images": [linked, mixed], "alto_required": True, "page_required": True}
    response = await client.post("/v1/jobs", headers=user_headers, json=payload)
    assert response.status_code == 201, response.text
    job_id = UUID(response.json()["data"]["id"])
    assert not (Path(config.JOBS_DIR) / str(job_id)).exists()
    async with db_sessions() as db:
        images, _ = await general_routes.general_cruds.get_job_images(db=db, job_id=job_id)
        assert len(images) == 2
        for image in images:
            assert not image.image_uploaded and not image.alto_uploaded and not image.page_uploaded
        assert images[0].image_url_provided and images[0].alto_url_provided and images[0].page_url_provided
        await queue_ready_jobs(db)
        job = await db.get(model.Job, job_id)
        assert job.definition == {**payload, "meta_json_required": False, "engine_name": None,
                                  "engine_version": None, "engine_settings": None,
                                  "images": [dict(linked), {**mixed, **{f"{kind}_url": None for kind, source in zip(kinds, sources) if source != "url"}}]}
    for kind, source in zip(kinds, sources):
        if source == "upload":
            data = {"image": make_white_image_bytes()[0], "alto": VALID_ALTO_XML, "page": VALID_PAGE_XML}[kind]
            uploaded = await client.put(f"/v1/jobs/{job_id}/images/mixed.jpg/files/{kind}", headers=user_headers,
                                         files={"file": (f"input.{kind}", data)})
            assert uploaded.status_code == 201, uploaded.text
    async with db_sessions() as db:
        await queue_ready_jobs(db)
        job = await db.get(model.Job, job_id)
        ready = "missing" not in sources
        assert job.state == (ProcessingState.QUEUED if ready else ProcessingState.NEW)
    if ready:
        lease = await client.post("/v1/jobs/lease", headers=worker_headers)
        assert lease.json()["code"] == AppCode.JOB_LEASED.value
        fetched = await client.get(f"/v1/jobs/{job_id}", headers=worker_headers)
        assert fetched.status_code == 200, fetched.text
        assert fetched.json()["data"]["images"][0]["alto_url"] == linked["alto_url"]
    # Fill every missing slot using normal uploads; URLs never set upload flags.
    for kind, source in zip(kinds, sources):
        if source == "missing":
            data = {"image": make_white_image_bytes()[0], "alto": VALID_ALTO_XML, "page": VALID_PAGE_XML}[kind]
            uploaded = await client.put(f"/v1/jobs/{job_id}/images/mixed.jpg/files/{kind}", headers=user_headers,
                                         files={"file": (f"input.{kind}", data)})
            assert uploaded.status_code == 201, uploaded.text
    async with db_sessions() as db:
        await queue_ready_jobs(db)
        assert (await db.get(model.Job, job_id)).state in {ProcessingState.QUEUED, ProcessingState.PROCESSING}


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("kind", ["image", "alto", "page"])
@pytest.mark.parametrize("url", ["", "/relative/path", "ftp://source.invalid/file", "file:///tmp/input"])
@pytest.mark.asyncio
async def test_post_job_422_invalid_input_link(client, user_headers, kind, url):
    response = await client.post("/v1/jobs", headers=user_headers,
                                  json={"images": [{"name": "page.jpg", "order": 0, f"{kind}_url": url}]})
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("meta_required", [False, True])
@pytest.mark.asyncio
async def test_post_job_201_input_links_metadata_requirement(client, user_headers, db_sessions, meta_required):
    from doc_api.api.job_readiness import queue_ready_jobs
    payload = {"images": [{"name": "page.jpg", "order": 0, "image_url": "https://source.invalid/page.jpg",
                           "alto_url": "https://source.invalid/unused.xml"}], "meta_json_required": meta_required}
    response = await client.post("/v1/jobs", headers=user_headers, json=payload)
    assert response.status_code == 201, response.text
    job_id = UUID(response.json()["data"]["id"])
    async with db_sessions() as db:
        await queue_ready_jobs(db)
        assert (await db.get(model.Job, job_id)).state == (ProcessingState.NEW if meta_required else ProcessingState.QUEUED)
    if meta_required:
        uploaded = await client.put(f"/v1/jobs/{job_id}/files/metadata", headers=user_headers, json={"source": "linked"})
        assert uploaded.status_code == 201, uploaded.text
        async with db_sessions() as db:
            await queue_ready_jobs(db)
            assert (await db.get(model.Job, job_id)).state == ProcessingState.QUEUED



@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_post_job_201_preserves_input_link(client, user_headers):
    url = "https://files.invalid/a%2Fb.jpg?signature=a%2Fb%3D&token=" + "x" * 5000
    response = await client.post("/v1/jobs", headers=user_headers,
                                  json={"images": [{"name": "page.jpg", "order": 0, "image_url": url}]})
    assert response.status_code == 201, response.text
    assert response.json()["data"]["images"][0]["image_url"] == url


#
# GET /v1/jobs - 200, 422
#

@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[AppCode.JOBS_RETRIEVED.value])
@pytest.mark.parametrize("has_owned_jobs", [True, False], ids=["owned-jobs", "no-owned-jobs"])
async def test_get_jobs_200(client, user_headers, reader_headers, reader_role, reader_owner,
                            db_sessions, job_factory, worker_key, has_owned_jobs, dummy):
    created_ids = set()
    for p in (JOB_DEFINITION_PAYLOADS if has_owned_jobs else []):
        r = await client.post("/v1/jobs", json=p, headers=user_headers)
        assert r.status_code == 201, r.text
        job_id = r.json()["data"]["id"]
        created_ids.add(job_id)
        async with db_sessions.begin() as db:
            (await db.get(model.Job, UUID(job_id))).owner_key_id = reader_owner.id

    other_id = await job_factory(ProcessingState.NEW, owner_key_id=worker_key.id)
    if reader_role == "admin":
        created_ids.add(str(other_id))

    r = await client.get("/v1/jobs", headers=reader_headers)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.JOBS_RETRIEVED.value
    data = body["data"]
    assert isinstance(data, list)
    assert {job["id"] for job in data} == created_ids

    for job in data:
        assert ("log" in job) == (reader_role == "admin")
        assert "artifacts_deleted_at" not in job or reader_role == "admin"


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[AppCode.JOBS_RETRIEVED.value])
async def test_get_jobs_200_with_engines(client, user_headers, reader_headers, reader_owner, db_sessions, created_engine, dummy):
    now = datetime.now(timezone.utc)
    created_ids = set()
    for p in JOB_DEFINITION_PAYLOADS:
        r = await client.post("/v1/jobs", json={**p, "engine_name": created_engine["name"]} , headers=user_headers)
        assert r.status_code == 201, r.text
        job_id = r.json()["data"]["id"]
        created_ids.add(job_id)
        async with db_sessions.begin() as db:
            (await db.get(model.Job, UUID(job_id))).owner_key_id = reader_owner.id

    r = await client.get("/v1/jobs", params={"from_created_date": now}, headers=reader_headers)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.JOBS_RETRIEVED.value
    data = body["data"]
    assert isinstance(data, list)
    assert {job["id"] for job in data} == created_ids
    for job in data:
        assert job["engine_name"] == created_engine["name"]
        assert job["engine_version"] == created_engine["version"]


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
async def test_get_jobs_200_result_availability(client, reader_headers, reader_role, jobs_with_output_availability):
    jobs = jobs_with_output_availability
    response = await client.get("/v1/jobs", params={"result_available": True}, headers=reader_headers)
    assert response.status_code == 200, response.text
    assert {job["id"] for job in response.json()["data"]} == ({str(jobs.active), str(jobs.other)} if reader_role == "admin" else {str(jobs.active)})
    response = await client.get("/v1/jobs", params={"result_available": False}, headers=reader_headers)
    assert response.status_code == 200, response.text
    data = {job["id"]: job for job in response.json()["data"]}
    assert set(data) == {str(jobs.deleted), str(jobs.processing), str(jobs.missing)}
    deleted = data[str(jobs.deleted)]
    assert deleted["state"] == "done"
    assert deleted["results_deleted_at"] is not None
    assert deleted["inputs_deleted_at"] is not None
    assert ("artifacts_deleted_at" in deleted) == (reader_role == "admin")
    # Missing legacy files are unavailable even without a deletion timestamp.
    if reader_role == "admin":
        assert data[str(jobs.missing)]["results_deleted_at"] is None
    else:
        assert "results_deleted_at" not in data[str(jobs.missing)]


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
async def test_get_jobs_422_invalid_request(client, reader_headers):
    response = await client.request('GET', '/v1/jobs', headers=reader_headers, params={'state': 'invalid'})
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["created_date", "started_date", "last_change", "finished_date"])
@pytest.mark.parametrize("offset", [-1, 0, 1])
@pytest.mark.parametrize("timezone_minutes", [0, 330, -210, None], ids=["UTC-Z", "UTC+05:30", "UTC-03:30", "naive-UTC"])
async def test_get_jobs_200_date_filter_boundaries(client, reader_headers, job_factory, db_sessions, field, offset, timezone_minutes):
    job_id = await job_factory(ProcessingState.DONE)
    when = datetime(2026, 1, 1, tzinfo=timezone.utc)
    async with db_sessions.begin() as db:
        setattr(await db.get(model.Job, job_id), field, when)
    threshold = when + timedelta(seconds=offset)
    if timezone_minutes is None:
        encoded = threshold.replace(tzinfo=None).isoformat()
    else:
        encoded = threshold.astimezone(timezone(timedelta(minutes=timezone_minutes))).isoformat()
        if timezone_minutes == 0:
            encoded = encoded.replace("+00:00", "Z")
    response = await client.get("/v1/jobs", headers=reader_headers, params={f"from_{field}": encoded})
    assert response.status_code == 200, response.text
    assert {job["id"] for job in response.json()["data"]} == ({str(job_id)} if offset <= 0 else set())


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["started_date", "finished_date"])
async def test_get_jobs_200_date_filter_null_dates(client, reader_headers, job_factory, db_sessions, field):
    missing = await job_factory(ProcessingState.NEW)
    dated = await job_factory(ProcessingState.DONE)
    when = datetime(2026, 1, 1, tzinfo=timezone.utc)
    async with db_sessions.begin() as db:
        setattr(await db.get(model.Job, missing), field, None)
        setattr(await db.get(model.Job, dated), field, when)
    response = await client.get("/v1/jobs", headers=reader_headers)
    assert response.status_code == 200, response.text
    assert {job["id"] for job in response.json()["data"]} == {str(missing), str(dated)}
    response = await client.get("/v1/jobs", headers=reader_headers, params={f"from_{field}": when.isoformat()})
    assert response.status_code == 200, response.text
    assert response.json()["code"] == AppCode.JOBS_RETRIEVED.value
    assert [job["id"] for job in response.json()["data"]] == [str(dated)]


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
async def test_get_jobs_200_combined_date_filters(client, reader_headers, job_factory, engine_factory, db_sessions):
    when = datetime(2026, 1, 1, tzinfo=timezone.utc)
    fields = ["created_date", "started_date", "last_change", "finished_date"]
    engine = await engine_factory(name="A", version="1")
    other_version = await engine_factory(name="A", version="2")
    other_engine = await engine_factory(name="B", version="1")
    cases = [
        ("boundary", {}),
        ("later", {field: when + timedelta(seconds=1) for field in fields}),
        *[(field, {field: when - timedelta(seconds=1)}) for field in fields],
        ("not_started", {"started_date": None}),
        ("not_finished", {"finished_date": None}),
        ("state", {"state": ProcessingState.QUEUED}),
        ("version", {"engine_id": other_version.id}),
        ("engine", {"engine_id": other_engine.id}),
        ("no_engine", {"engine_id": None}),
        ("no_result", {}),
    ]
    ids = {}
    for name, changes in cases:
        job_id = await job_factory(ProcessingState.DONE)
        ids[name] = str(job_id)
        async with db_sessions.begin() as db:
            job = await db.get(model.Job, job_id)
            for field, value in {**dict.fromkeys(fields, when), "engine_id": engine.id, **changes}.items():
                setattr(job, field, value)
        if name != "no_result":
            (Path(config.RESULTS_DIR) / f"{job_id}.zip").write_bytes(VALID_ZIP)

    response = await client.get("/v1/jobs", headers=reader_headers, params={
        **{f"from_{field}": when.isoformat() for field in fields},
        "engine_name": "A", "engine_version": "1", "state": "done", "result_available": True,
    })
    assert response.status_code == 200, response.text
    assert response.json()["code"] == AppCode.JOBS_RETRIEVED.value
    assert [job["id"] for job in response.json()["data"]] == [ids["later"], ids["boundary"]]


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("field", [None, "created_date", "started_date", "last_change", "finished_date"],
                         ids=["unfiltered", "created_date", "started_date", "last_change", "finished_date"])
async def test_get_jobs_200_date_filter_ordering(client, reader_headers, job_factory, db_sessions, field):
    when = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ids = {}
    # Insert out of order; other timestamps run in the opposite direction.
    for rank in [1, 4, 2, 0, 3]:
        job_id = await job_factory(ProcessingState.DONE)
        ids[rank] = str(job_id)
        async with db_sessions.begin() as db:
            job = await db.get(model.Job, job_id)
            job.created_date = when + timedelta(seconds=rank)
            for other in ["started_date", "last_change", "finished_date"]:
                setattr(job, other, when + timedelta(seconds=10 - rank))
    threshold = when + timedelta(seconds=2 if field == "created_date" else 8)
    params = {f"from_{field}": threshold.isoformat()} if field else {}
    response = await client.get("/v1/jobs", headers=reader_headers, params=params)
    assert response.status_code == 200, response.text
    assert response.json()["code"] == AppCode.JOBS_RETRIEVED.value
    expected = [4, 3, 2, 1, 0] if field is None else ([4, 3, 2] if field == "created_date" else [2, 1, 0])
    assert [job["id"] for job in response.json()["data"]] == [ids[rank] for rank in expected]


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("state", list(ProcessingState))
async def test_get_jobs_200_state_filter(client, reader_headers, job_factory, state):
    ids = {current: await job_factory(current) for current in ProcessingState}
    response = await client.get("/v1/jobs", headers=reader_headers, params={"state": state.value})
    assert response.status_code == 200, response.text
    assert {job["id"] for job in response.json()["data"]} == {str(ids[state])}


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
async def test_get_jobs_200_engine_filter_combination(client, reader_headers, job_factory, engine_factory, db_sessions):
    first = await engine_factory(name="A", version="1")
    second = await engine_factory(name="A", version="2")
    job_id = await job_factory(ProcessingState.DONE)
    other_id = await job_factory(ProcessingState.DONE)
    await job_factory(ProcessingState.DONE)  # A job without an engine is still listable.
    async with db_sessions.begin() as db:
        (await db.get(model.Job, job_id)).engine_id = first.id
        (await db.get(model.Job, other_id)).engine_id = second.id
    response = await client.get("/v1/jobs", headers=reader_headers,
                                params={"engine_name": "A", "engine_version": "1", "state": "done"})
    assert response.status_code == 200, response.text
    assert {job["id"] for job in response.json()["data"]} == {str(job_id)}




@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
async def test_get_jobs_500_unexpected_crud_result(client, reader_headers, monkeypatch, invariant_errors):
    code = AppCode.API_KEY_VALID
    monkeypatch.setattr(user_routes.general_cruds, "get_jobs_with_engines", AsyncMock(return_value=([], code)))
    response = await client.get("/v1/jobs", headers=reader_headers)
    assert_route_invariant(response, invariant_errors, code)


#
# GET /v1/jobs/{job_id} - 200, 403, 404, 422, 500
#

@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", JOB_DEFINITION_PAYLOADS, ids=partial(job_definition_payload_id, app_code=AppCode.JOB_RETRIEVED.value), indirect=True)
async def test_get_job_200(client, reader_headers, reader_role, created_job):
    job = created_job["created_job"]
    job_id = job["id"]

    r = await client.get(f"/v1/jobs/{job_id}", headers=reader_headers)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.JOB_RETRIEVED.value
    data = body["data"]

    assert ("log" in data) == (reader_role == "admin")
    assert data["id"] == job_id
    assert data["state"] == base_objects.ProcessingState.NEW.value
    assert len(data["images"]) == len(job["images"])
    for img_post, img_get in zip(job["images"], data["images"]):
        if reader_role == "admin":
            assert img_get["id"] is not None
            assert all(img_get[field] == value for field, value in img_post.items())
        else:
            assert img_post == img_get
            assert "id" not in img_get


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", JOB_DEFINITION_PAYLOADS, ids=partial(job_definition_payload_id, app_code=AppCode.JOB_RETRIEVED.value), indirect=True)
async def test_get_job_200_with_engine(client, reader_headers, reader_role, created_job_with_engine):
    job = created_job_with_engine["created_job"]
    engine = created_job_with_engine["engine"]
    job_id = job["id"]

    r = await client.get(f"/v1/jobs/{job_id}", headers=reader_headers)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.JOB_RETRIEVED.value
    data = body["data"]

    assert ("log" in data) == (reader_role == "admin")
    assert data["id"] == job_id
    assert data["state"] == base_objects.ProcessingState.NEW.value
    assert len(data["images"]) == len(job["images"])
    for img_post, img_get in zip(job["images"], data["images"]):
        if reader_role == "admin":
            assert img_get["id"] is not None
            assert all(img_get[field] == value for field, value in img_post.items())
        else:
            assert img_post == img_get
            assert "id" not in img_get

    assert data["engine_name"] == engine["name"]
    assert data["engine_version"] == engine["version"]
    if reader_role == "admin":
        assert data["engine_definition"] == engine["definition"]
    else:
        assert "engine_definition" not in data


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
async def test_get_job_200_deletion_visibility(client, reader_headers, reader_role, jobs_with_output_availability):
    job_id = jobs_with_output_availability.deleted
    response = await client.get(f"/v1/jobs/{job_id}", headers=reader_headers)
    assert response.status_code == 200, response.text
    job = response.json()["data"]
    assert job["result_available"] is False
    assert job["results_deleted_at"] is not None
    assert job["inputs_deleted_at"] is not None
    assert ("artifacts_deleted_at" in job) == (reader_role == "admin")


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
async def test_get_job_404_job_not_found(client, reader_headers):
    job_id = uuid4()
    response = await client.request('GET', f'/v1/jobs/{job_id}', headers=reader_headers)
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


@pytest.mark.asyncio
async def test_get_job_403_job_access_forbidden(client, user_headers, job_factory, worker_key):
    job_id = await job_factory(base_objects.ProcessingState.NEW, owner_key_id=worker_key.id)
    response = await client.request('GET', f'/v1/jobs/{job_id}', headers=user_headers)
    assert response.status_code == 403, response.text
    assert response.json()["code"] == AppCode.API_KEY_FORBIDDEN_FOR_JOB.value


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
async def test_get_job_422_invalid_request(client, reader_headers):
    response = await client.request('GET', '/v1/jobs/not-a-uuid', headers=reader_headers)
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
async def test_get_job_500_database_failure(client, reader_headers, monkeypatch):
    async def fail(**kwargs):
        raise RuntimeError("injected database failure")
    monkeypatch.setattr(general_routes.general_cruds, "get_job", fail)
    response = await client.get(f"/v1/jobs/{uuid4()}", headers=reader_headers)
    assert response.status_code == 500, response.text
    assert response.json()["code"] == AppCode.INTERNAL_ERROR.value
    assert "injected database failure" not in response.text




@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.parametrize("stage", ["job", "images", "engine"])
@pytest.mark.asyncio
async def test_get_job_500_unexpected_crud_result(client, reader_headers, job_factory, engine_factory,
                                                db_sessions, monkeypatch, invariant_errors, stage):
    job_id = await job_factory(ProcessingState.DONE)
    code = AppCode.API_KEY_VALID
    if stage == "job":
        original = general_routes.general_cruds.get_job
        async def unexpected(**kwargs):
            job, _ = await original(**kwargs)
            return job, code
        monkeypatch.setattr(general_routes.general_cruds, "get_job", unexpected)
    elif stage == "images":
        monkeypatch.setattr(general_routes.general_cruds, "get_job_images", AsyncMock(return_value=([], code)))
    else:
        engine = await engine_factory()
        async with db_sessions.begin() as db:
            (await db.get(model.Job, job_id)).engine_id = engine.id
        monkeypatch.setattr(general_routes.general_cruds, "get_engine", AsyncMock(return_value=(None, code)))
    response = await client.get(f"/v1/jobs/{job_id}", headers=reader_headers)
    assert_route_invariant(response, invariant_errors, code)


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
async def test_get_job_404_disappeared_after_check(client, reader_headers, job_factory, db_sessions, monkeypatch):
    job_id = await job_factory(ProcessingState.DONE)
    delete_job_after_exists(monkeypatch, db_sessions, job_id)
    response = await client.get(f"/v1/jobs/{job_id}", headers=reader_headers)
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value



@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
async def test_get_job_404_disappeared_before_images(client, reader_headers, job_factory, db_sessions, monkeypatch):
    job_id = await job_factory(ProcessingState.DONE)
    delete_job_before_crud(monkeypatch, db_sessions, job_id, general_routes.general_cruds, "get_job_images")
    response = await client.get(f"/v1/jobs/{job_id}", headers=reader_headers)
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value



@pytest.mark.parametrize("role", ["user", "readonly", "worker", "admin"])
@pytest.mark.parametrize("provided", list(product([False, True], repeat=3)))
@pytest.mark.asyncio
async def test_get_job_200_symmetric_input_links(client, user_headers, readonly_headers, worker_headers,
                                               admin_headers, db_sessions, role, provided):
    from doc_api.api.job_readiness import queue_ready_jobs
    fields = [f"{kind}_url" for kind in ["image", "alto", "page"]]
    first = {"name": "first.jpg", "order": 0,
             **{field: f"https://files.invalid/{field}" for field, present in zip(fields, provided) if present}}
    response = await client.post("/v1/jobs", headers=user_headers,
                                  json={"images": [first, {"name": "second.jpg", "order": 1}]})
    assert response.status_code == 201, response.text
    created = response.json()["data"]
    for image in created["images"]:
        for field, present in zip(fields, provided):
            assert (field in image) == present
            if present:
                assert image[field] == (first[field] if image["name"] == "first.jpg" else None)
    job_id = created["id"]
    for name in ["first.jpg", "second.jpg"]:
        result = await client.put(f"/v1/jobs/{job_id}/images/{name}/files/image", headers=user_headers,
                                   files={"file": (name, make_white_image_bytes()[0])})
        assert result.status_code == 201, result.text
    if role == "worker":
        async with db_sessions() as db:
            await queue_ready_jobs(db)
        lease = await client.post("/v1/jobs/lease", headers=worker_headers)
        assert lease.json()["code"] == AppCode.JOB_LEASED.value
    headers = {"user": user_headers, "readonly": readonly_headers, "worker": worker_headers, "admin": admin_headers}[role]
    fetched = await client.get(f"/v1/jobs/{job_id}", headers=headers)
    assert fetched.status_code == 200, fetched.text
    for image in fetched.json()["data"]["images"]:
        for field, present in zip(fields, provided):
            assert (field in image) == present
            assert f"{field}_provided" not in image
            if present:
                assert image[field] == (first[field] if image["name"] == "first.jpg" else None)


#
# PUT /v1/jobs/{job_id}/images/{image_id}/files/image - 201, 200, 403, 404, 409, 415, 422, 400, 500
#

@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.IMAGE_UPLOADED.value], indirect=True)
async def test_put_image_201(client, user_headers, created_job):
    job = created_job["created_job"]
    job_id = job["id"]

    name = created_job["payload"]["images"][0]["name"]
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

    body = r.json()
    assert body["code"] == AppCode.IMAGE_UPLOADED.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_image_201_preserves_deletion_timestamps(client, user_headers, db_sessions, job_with_deletion_timestamps):
    job_id, before = await job_with_deletion_timestamps(base_objects.ProcessingState.NEW)
    response = await client.put(f"/v1/jobs/{job_id}/images/page.jpg/files/image", headers=user_headers, files={"file": ("upload", make_white_image_bytes()[0])})
    assert response.status_code == 201, response.text
    await assert_deletion_timestamps(db_sessions, job_id, before)


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.IMAGE_REUPLOADED.value], indirect=True)
async def test_put_image_200(client, user_headers, created_job):
    job = created_job["created_job"]
    job_id = job["id"]

    name = created_job["payload"]["images"][0]["name"]
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

    assert r.status_code == 200, r.text

    body = r.json()
    assert body["code"] == AppCode.IMAGE_REUPLOADED.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
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


@pytest.mark.parametrize("user_role", ["user", "admin"])
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


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_image_404_job_not_found(client, user_headers):
    job_id = uuid4()
    response = await client.request('PUT', f'/v1/jobs/{job_id}/images/page.jpg/files/image', headers=user_headers, files={'file': ('page', b'data')})
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


@pytest.mark.asyncio
async def test_put_image_403_job_access_forbidden(client, user_headers, job_factory, worker_key):
    job_id = await job_factory(base_objects.ProcessingState.NEW, owner_key_id=worker_key.id)
    response = await client.request('PUT', f'/v1/jobs/{job_id}/images/page.jpg/files/image', headers=user_headers, files={'file': ('page', b'data')})
    assert response.status_code == 403, response.text
    assert response.json()["code"] == AppCode.API_KEY_FORBIDDEN_FOR_JOB.value


@pytest.mark.asyncio
@pytest.mark.parametrize("state", [state for state in base_objects.ProcessingState if state != base_objects.ProcessingState.NEW])
async def test_put_image_409_job_not_new(client, user_headers, job_factory, state):
    job_id = await job_factory(state)
    response = await client.request('PUT', f'/v1/jobs/{job_id}/images/page.jpg/files/image', headers=user_headers, files={'file': ('page', b'data')})
    assert response.status_code == 409, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_IN_NEW.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_image_422_invalid_request(client, user_headers):
    response = await client.request('PUT', '/v1/jobs/00000000-0000-0000-0000-000000000001/images/page.jpg/files/image', headers=user_headers)
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_image_500_write_failure(client, user_headers, input_upload_case, db_sessions, monkeypatch):
    case = await input_upload_case("image")
    with monkeypatch.context() as patch:
        patch.setattr(user_routes.cv2, "imwrite", lambda *args: False)
        response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert_internal_transfer_error(response)
    assert not case.path.exists()
    async with db_sessions() as db:
        assert not getattr(await db.get(case.entity, case.record_id), case.flag)
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW
    retry = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert retry.status_code == 201, retry.text
    async with db_sessions() as db:
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_image_500_database_failure(client, user_headers, input_upload_case, db_sessions, monkeypatch):
    case = await input_upload_case("image")
    with monkeypatch.context() as patch:
        method = 'update_image'
        patch.setattr(user_routes.general_cruds, method, AsyncMock(side_effect=DBError("injected transfer failure")))
        response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert_internal_transfer_error(response)
    # The file and upload metadata roll back together.
    assert not case.path.exists()
    async with db_sessions() as db:
        assert not getattr(await db.get(case.entity, case.record_id), case.flag)
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW
    retry = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert retry.status_code == 201, retry.text
    async with db_sessions() as db:
        assert getattr(await db.get(case.entity, case.record_id), case.flag)
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_image_400_interrupted_upload(client, user_headers, input_upload_case, db_sessions):
    case = await input_upload_case("image")
    metadata = False
    response = await client.put(case.url, headers=interrupted_upload_headers(user_headers, metadata=metadata),
                                content=interrupted_upload_body(metadata))
    assert response.status_code == 400, response.text
    assert response.json()["code"] == AppCode.HTTP_ERROR.value
    assert "injected transfer failure" not in response.text
    assert not case.path.exists()
    async with db_sessions() as db:
        assert not getattr(await db.get(case.entity, case.record_id), case.flag)
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW
    retry = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert retry.status_code == 201, retry.text


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("failure", ["write", "replace"])
@pytest.mark.asyncio
async def test_put_image_500_reupload_preserves_file(client, user_headers, input_upload_case, db_sessions, monkeypatch, failure):
    case = await input_upload_case("image")
    case.path.parent.mkdir(exist_ok=True)
    old = b"previous uploaded bytes"
    case.path.write_bytes(old)
    case.path.chmod(0o640)
    async with db_sessions.begin() as db:
        setattr(await db.get(case.entity, case.record_id), case.flag, True)
    async with db_sessions() as db:
        before = model.orm2dict(await db.get(case.entity, case.record_id))
    with monkeypatch.context() as patch:
        if failure == "replace":
            real_replace = user_routes.os.replace
            def broken_replace(source, destination):
                if Path(destination) == case.path and ".docapi-tmp-upload-staging-" in str(source):
                    raise OSError("injected transfer failure")
                return real_replace(source, destination)
            patch.setattr(user_routes.os, "replace", broken_replace)
        else:
            def partial_image(filename, image):
                Path(filename).write_bytes(b"partial")
                return False
            patch.setattr(user_routes.cv2, "imwrite", partial_image)
        response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert_internal_transfer_error(response)
    assert case.path.read_bytes() == old
    assert set(case.path.parent.iterdir()) == {case.path, case.path.with_name(f".{case.path.name}.docapi-save.lock")}
    async with db_sessions() as db:
        assert model.orm2dict(await db.get(case.entity, case.record_id)) == before
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW
    retry = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert retry.status_code == 200, retry.text
    assert case.path.read_bytes() != old
    assert case.path.stat().st_mode & 0o777 == 0o640
    async with db_sessions() as db:
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_image_500_read_failure(client, user_headers, input_upload_case, db_sessions, monkeypatch):
    case = await input_upload_case("image")
    with monkeypatch.context() as patch:
        def broken_read(*args, **kwargs):
            raise OSError("injected transfer failure")
        patch.setattr(tempfile.SpooledTemporaryFile, "read", broken_read)
        response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert_internal_transfer_error(response)
    assert not case.path.exists()
    async with db_sessions() as db:
        assert not getattr(await db.get(case.entity, case.record_id), case.flag)
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW
    retry = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert retry.status_code == 201, retry.text


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("existing", [False, True], ids=["first-upload", "replacement"])
@pytest.mark.asyncio
async def test_put_image_500_commit_failure(client, user_headers, input_upload_case, db_sessions, reject_upload_commit, existing):
    case = await input_upload_case("image")
    old = b"original input bytes"
    if existing:
        case.path.parent.mkdir(exist_ok=True)
        case.path.write_bytes(old)
        async with db_sessions.begin() as db:
            setattr(await db.get(case.entity, case.record_id), case.flag, True)
    async with db_sessions() as db:
        before = [(case.entity, case.record_id, model.orm2dict(await db.get(case.entity, case.record_id))),
                  (model.Job, case.job_id, model.orm2dict(await db.get(model.Job, case.job_id)))]
    with reject_upload_commit(case.path, old=old if existing else None) as observed:
        response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert_internal_transfer_error(response)
    assert observed == [True]
    assert case.path.exists() == existing
    if existing:
        assert case.path.read_bytes() == old
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-*"))
    await assert_records_unchanged(db_sessions, before)
    retry = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert retry.status_code == (200 if existing else 201), retry.text
    async with db_sessions() as db:
        assert getattr(await db.get(case.entity, case.record_id), case.flag)
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-*"))


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_image_201_concurrent_inputs_queue_job(client, user_headers, db_sessions, monkeypatch):
    from doc_api.api import job_readiness
    from doc_api.api import file_saving as file_helpers

    response = await client.post("/v1/jobs", headers=user_headers,
        json={"images": [{"name": "first.jpg", "order": 0}, {"name": "second.jpg", "order": 1}]})
    assert response.status_code == 201, response.text
    job_id = UUID(response.json()["data"]["id"])
    both_staged = asyncio.Event()
    release = asyncio.Event()
    original = file_helpers.FileSaveTransaction.install
    staged = []

    async def install(save):
        staged.append(Path(save.staged))
        if len(staged) == 2:
            both_staged.set()
        await release.wait()
        await original(save)

    tasks = []
    with monkeypatch.context() as patch:
        patch.setattr(file_helpers.FileSaveTransaction, "install", install)
        try:
            for name in ("first.jpg", "second.jpg"):
                tasks.append(asyncio.create_task(client.put(f"/v1/jobs/{job_id}/images/{name}/files/image",
                    headers=user_headers, files={"file": (name, make_white_image_bytes()[0])})))
            await asyncio.wait_for(both_staged.wait(), timeout=10)
            assert all(path.stat().st_size > 0 for path in staged)
            async with db_sessions() as db:
                await job_readiness.queue_ready_jobs(db)
            async with db_sessions() as db:
                assert (await db.get(model.Job, job_id)).state == ProcessingState.NEW
            release.set()
            responses = await asyncio.wait_for(asyncio.gather(*tasks), timeout=10)
        finally:
            release.set()
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    assert all(response.status_code == 201 for response in responses)
    async with db_sessions() as db:
        assert (await db.get(model.Job, job_id)).state == ProcessingState.NEW
    async with db_sessions() as db:
        await job_readiness.queue_ready_jobs(db)
    async with db_sessions() as db:
        assert (await db.get(model.Job, job_id)).state == ProcessingState.QUEUED
    assert not list((Path(config.JOBS_DIR) / str(job_id)).glob("*.docapi-tmp-*"))

@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_image_201_parallel_input_formats(client, user_headers, db_sessions, monkeypatch):
    from doc_api.api import job_readiness
    from doc_api.api import file_saving as file_helpers
    from doc_api.tests.dummy_data import VALID_ALTO_XML, VALID_PAGE_XML

    response = await client.post("/v1/jobs", headers=user_headers, json={
        "images": [{"name": "page.jpg", "order": 0}],
        "alto_required": True, "page_required": True, "meta_json_required": True})
    assert response.status_code == 201, response.text
    job_id = UUID(response.json()["data"]["id"])
    staged = []
    ready = asyncio.Event()
    release = asyncio.Event()
    original = file_helpers.FileSaveTransaction.install

    async def install(save):
        staged.append(Path(save.staged))
        if len(staged) == 4:
            ready.set()
        await release.wait()
        await original(save)

    tasks = []
    with monkeypatch.context() as patch:
        patch.setattr(file_helpers.FileSaveTransaction, "install", install)
        try:
            for kind, data in [("image", make_white_image_bytes()[0]),
                               ("alto", VALID_ALTO_XML), ("page", VALID_PAGE_XML)]:
                tasks.append(asyncio.create_task(client.put(
                    f"/v1/jobs/{job_id}/images/page.jpg/files/{kind}", headers=user_headers,
                    files={"file": ("page", data)})))
            tasks.append(asyncio.create_task(client.put(f"/v1/jobs/{job_id}/files/metadata",
                headers=user_headers, json={"source": "parallel"})))
            await asyncio.wait_for(ready.wait(), timeout=10)
            assert len(set(staged)) == 4
            assert all(path.stat().st_size > 0 for path in staged)
            release.set()
            responses = await asyncio.wait_for(asyncio.gather(*tasks), timeout=10)
        finally:
            release.set()
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    assert [response.status_code for response in responses] == [201] * 4, [r.text for r in responses]
    async with db_sessions() as db:
        assert (await db.get(model.Job, job_id)).state == ProcessingState.NEW
    async with db_sessions() as db:
        await job_readiness.queue_ready_jobs(db)
    async with db_sessions() as db:
        assert (await db.get(model.Job, job_id)).state == ProcessingState.QUEUED
    assert not list((Path(config.JOBS_DIR) / str(job_id)).glob("*.docapi-tmp-*"))


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("kind", ["image", "alto", "page", "metadata"])
@pytest.mark.asyncio
async def test_put_input_409_file_save_in_progress(client, user_headers, input_upload_case, monkeypatch, db_sessions, kind):
    from doc_api.tests.helpers import assert_file_save_conflict
    case = await input_upload_case(kind)
    await assert_file_save_conflict(client, method="PUT", url=case.url, headers=user_headers,
                                   kwargs=case.kwargs, path=case.path,
                                   monkeypatch=monkeypatch, db_sessions=db_sessions)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["queue", "cancel"])
async def test_put_image_409_state_changed_during_staging(client, user_headers, input_upload_case, db_sessions, monkeypatch, change):
    from doc_api.api import job_readiness
    from doc_api.api import file_saving as file_helpers
    case = await input_upload_case("image")
    old = b"original image"
    case.path.parent.mkdir(exist_ok=True)
    case.path.write_bytes(old)
    async with db_sessions.begin() as db:
        (await db.get(case.entity, case.record_id)).image_uploaded = True
    original = file_helpers.FileSaveTransaction.install

    async def install(save):
        if change == "queue":
            async with db_sessions() as db:
                await job_readiness.queue_ready_jobs(db)
        else:
            response = await client.patch(f"/v1/jobs/{case.job_id}", headers=user_headers,
                                          json={"state": ProcessingState.CANCELLED.value})
            assert response.status_code == 200, response.text
        await original(save)

    monkeypatch.setattr(file_helpers.FileSaveTransaction, "install", install)
    response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert response.status_code == 409, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_IN_NEW.value
    assert case.path.read_bytes() == old
    assert not list(case.path.parent.glob("*.docapi-tmp-*"))

@pytest.mark.asyncio
async def test_put_image_500_restore_before_readiness_unlock(client, user_headers, input_upload_case, db_sessions, monkeypatch):
    from sqlalchemy.ext.asyncio import AsyncSession
    from doc_api.api import job_readiness
    case = await input_upload_case("image")
    old = b"previous image"
    case.path.parent.mkdir(exist_ok=True)
    case.path.write_bytes(old)
    async with db_sessions.begin() as db:
        (await db.get(case.entity, case.record_id)).image_uploaded = True
    original_rollback = AsyncSession.rollback
    checked = False

    async def rollback(db):
        nonlocal checked
        if db.info.get("file_save_transaction"):
            assert case.path.read_bytes() == old
            # The job remains locked until its file and metadata agree again.
            async with db_sessions() as checker:
                await asyncio.wait_for(job_readiness.queue_ready_jobs(checker), timeout=5)
            async with db_sessions() as reader:
                assert (await reader.get(model.Job, case.job_id)).state == ProcessingState.NEW
            checked = True
        await original_rollback(db)

    monkeypatch.setattr(AsyncSession, "rollback", rollback)
    monkeypatch.setattr(user_routes.general_cruds, "update_image",
                        AsyncMock(side_effect=DBError("injected transfer failure")))
    response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert_internal_transfer_error(response)
    assert checked
    assert case.path.read_bytes() == old
    async with db_sessions() as db:
        await job_readiness.queue_ready_jobs(db)
    async with db_sessions() as db:
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.QUEUED










@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("variant", ["missing_boundary", "invalid_body"])
@pytest.mark.asyncio
async def test_put_image_400_multipart(client, user_headers, input_upload_case, db_sessions, variant):
    case = await input_upload_case("image")
    await assert_multipart_rejected(client, user_headers, case, db_sessions,
                                    method="PUT", variant=variant)


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("variant", ["wrong_field", "empty_filename", "text_field"])
@pytest.mark.asyncio
async def test_put_image_422_multipart(client, user_headers, input_upload_case, db_sessions, variant):
    case = await input_upload_case("image")
    await assert_multipart_rejected(client, user_headers, case, db_sessions,
                                    method="PUT", variant=variant)




@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_image_500_unexpected_crud_result(client, user_headers, input_upload_case, db_sessions,
                                                    monkeypatch, invariant_errors):
    case = await input_upload_case("image")
    code = AppCode.API_KEY_VALID
    monkeypatch.setattr(user_routes.general_cruds, "update_image", AsyncMock(return_value=code))
    response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert_route_invariant(response, invariant_errors, code)
    assert not case.path.exists()
    assert not list(case.path.parent.glob("*.docapi-tmp-*"))
    async with db_sessions() as db:
        assert not getattr(await db.get(case.entity, case.record_id), case.flag)



@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("deleted", ["job", "image"])
@pytest.mark.asyncio
async def test_put_image_404_disappeared_before_install(client, user_headers, input_upload_case,
                                                        db_sessions, monkeypatch, deleted):
    case = await input_upload_case("image")
    case.path.parent.mkdir(exist_ok=True)
    case.path.write_bytes(b"previous file")
    entity, record_id = (model.Job, case.job_id) if deleted == "job" else (case.entity, case.record_id)
    delete_before_file_install(monkeypatch, db_sessions, entity, record_id)
    response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert response.status_code == 404, response.text
    expected = AppCode.JOB_NOT_FOUND if deleted == "job" else AppCode.IMAGE_NOT_FOUND_FOR_JOB
    assert response.json()["code"] == expected.value
    assert case.path.read_bytes() == b"previous file"
    assert not list(case.path.parent.glob("*.docapi-tmp-*"))


#
# PUT /v1/jobs/{job_id}/images/{image_id}/files/alto - 201, 200, 400, 403, 404, 409, 422, 500
#

@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[2]], ids=[AppCode.ALTO_UPLOADED.value], indirect=True)
async def test_put_alto_201(client, user_headers, created_job):
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

    assert r.status_code == 201, r.text

    body = r.json()
    assert body["code"] == AppCode.ALTO_UPLOADED.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_alto_201_preserves_deletion_timestamps(client, user_headers, db_sessions, job_with_deletion_timestamps):
    job_id, before = await job_with_deletion_timestamps(base_objects.ProcessingState.NEW)
    response = await client.put(f"/v1/jobs/{job_id}/images/page.jpg/files/alto", headers=user_headers, files={"file": ("upload", VALID_ALTO_XML)})
    assert response.status_code == 201, response.text
    await assert_deletion_timestamps(db_sessions, job_id, before)


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[2]], ids=[AppCode.ALTO_REUPLOADED.value], indirect=True)
async def test_put_alto_200(client, user_headers, created_job):
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

    assert r.status_code == 201, r.text

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

    body = r.json()
    assert body["code"] == AppCode.ALTO_REUPLOADED.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
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


@pytest.mark.parametrize("user_role", ["user", "admin"])
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


@pytest.mark.parametrize("user_role", ["user", "admin"])
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


@pytest.mark.parametrize("user_role", ["user", "admin"])
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


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_alto_404_job_not_found(client, user_headers):
    job_id = uuid4()
    response = await client.request('PUT', f'/v1/jobs/{job_id}/images/page.jpg/files/alto', headers=user_headers, files={'file': ('page', b'data')})
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


@pytest.mark.asyncio
async def test_put_alto_403_job_access_forbidden(client, user_headers, job_factory, worker_key):
    job_id = await job_factory(base_objects.ProcessingState.NEW, owner_key_id=worker_key.id)
    response = await client.request('PUT', f'/v1/jobs/{job_id}/images/page.jpg/files/alto', headers=user_headers, files={'file': ('page', b'data')})
    assert response.status_code == 403, response.text
    assert response.json()["code"] == AppCode.API_KEY_FORBIDDEN_FOR_JOB.value


@pytest.mark.asyncio
@pytest.mark.parametrize("state", [state for state in base_objects.ProcessingState if state != base_objects.ProcessingState.NEW])
async def test_put_alto_409_job_not_new(client, user_headers, job_factory, state):
    job_id = await job_factory(state)
    response = await client.request('PUT', f'/v1/jobs/{job_id}/images/page.jpg/files/alto', headers=user_headers, files={'file': ('page', b'data')})
    assert response.status_code == 409, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_IN_NEW.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_alto_422_invalid_request(client, user_headers):
    response = await client.request('PUT', '/v1/jobs/00000000-0000-0000-0000-000000000001/images/page.jpg/files/alto', headers=user_headers)
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("option", ['namespace', 'has_layout', 'has_page', 'has_text'])
async def test_put_alto_422_optional_validation(client, user_headers, job_with_deletion_timestamps, monkeypatch, option):
    monkeypatch.setattr(config, "ALTO_VALIDATION", {**{key: False for key in config.ALTO_VALIDATION}, "root": True, option: True})
    job_id, _ = await job_with_deletion_timestamps(ProcessingState.NEW)
    response = await client.put(f"/v1/jobs/{job_id}/images/page.jpg/files/alto", headers=user_headers,
                                files={"file": ("input.xml", b"<alto/>")})
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.ALTO_SCHEMA_INVALID.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_alto_201_root_validation_disabled(client, user_headers, job_with_deletion_timestamps, monkeypatch):
    monkeypatch.setattr(config, "ALTO_VALIDATION", {key: False for key in config.ALTO_VALIDATION})
    job_id, _ = await job_with_deletion_timestamps(ProcessingState.NEW)
    response = await client.put(f"/v1/jobs/{job_id}/images/page.jpg/files/alto", headers=user_headers,
                                files={"file": ("input.xml", b"<other/>")})
    assert response.status_code == 201, response.text


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_alto_500_write_failure(client, user_headers, input_upload_case, db_sessions, monkeypatch):
    case = await input_upload_case("alto")
    with monkeypatch.context() as patch:
        real_open = builtins.open
        def broken_open(filename, *args, **kwargs):
            if Path(filename).parent == case.path.parent:
                raise OSError("injected transfer failure")
            return real_open(filename, *args, **kwargs)
        patch.setattr(user_routes, "open", broken_open, raising=False)
        response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert_internal_transfer_error(response)
    assert not case.path.exists()
    async with db_sessions() as db:
        assert not getattr(await db.get(case.entity, case.record_id), case.flag)
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW
    retry = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert retry.status_code == 201, retry.text
    async with db_sessions() as db:
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_alto_500_database_failure(client, user_headers, input_upload_case, db_sessions, monkeypatch):
    case = await input_upload_case("alto")
    with monkeypatch.context() as patch:
        method = 'update_image'
        patch.setattr(user_routes.general_cruds, method, AsyncMock(side_effect=DBError("injected transfer failure")))
        response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert_internal_transfer_error(response)
    # The file and upload metadata roll back together.
    assert not case.path.exists()
    async with db_sessions() as db:
        assert not getattr(await db.get(case.entity, case.record_id), case.flag)
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW
    retry = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert retry.status_code == 201, retry.text
    async with db_sessions() as db:
        assert getattr(await db.get(case.entity, case.record_id), case.flag)
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_alto_400_interrupted_upload(client, user_headers, input_upload_case, db_sessions):
    case = await input_upload_case("alto")
    metadata = False
    response = await client.put(case.url, headers=interrupted_upload_headers(user_headers, metadata=metadata),
                                content=interrupted_upload_body(metadata))
    assert response.status_code == 400, response.text
    assert response.json()["code"] == AppCode.HTTP_ERROR.value
    assert "injected transfer failure" not in response.text
    assert not case.path.exists()
    async with db_sessions() as db:
        assert not getattr(await db.get(case.entity, case.record_id), case.flag)
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW
    retry = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert retry.status_code == 201, retry.text


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("failure", ["write", "replace"])
@pytest.mark.asyncio
async def test_put_alto_500_reupload_preserves_file(client, user_headers, input_upload_case, db_sessions, monkeypatch, failure):
    case = await input_upload_case("alto")
    case.path.parent.mkdir(exist_ok=True)
    old = b"previous uploaded bytes"
    case.path.write_bytes(old)
    case.path.chmod(0o640)
    async with db_sessions.begin() as db:
        setattr(await db.get(case.entity, case.record_id), case.flag, True)
    async with db_sessions() as db:
        before = model.orm2dict(await db.get(case.entity, case.record_id))
    with monkeypatch.context() as patch:
        if failure == "replace":
            real_replace = user_routes.os.replace
            def broken_replace(source, destination):
                if Path(destination) == case.path and ".docapi-tmp-upload-staging-" in str(source):
                    raise OSError("injected transfer failure")
                return real_replace(source, destination)
            patch.setattr(user_routes.os, "replace", broken_replace)
        else:
            real_open = builtins.open
            def broken_open(filename, *args, **kwargs):
                opened = real_open(filename, *args, **kwargs)
                if Path(filename).parent != case.path.parent:
                    return opened
                @contextmanager
                def partial_writer():
                    with opened as file:
                        def write(data):
                            file.write(data[:1])
                            raise OSError("injected transfer failure")
                        yield SimpleNamespace(write=write)
                return partial_writer()
            patch.setattr(user_routes, "open", broken_open, raising=False)
        response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert_internal_transfer_error(response)
    assert case.path.read_bytes() == old
    assert set(case.path.parent.iterdir()) == {case.path, case.path.with_name(f".{case.path.name}.docapi-save.lock")}
    async with db_sessions() as db:
        assert model.orm2dict(await db.get(case.entity, case.record_id)) == before
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW
    retry = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert retry.status_code == 200, retry.text
    assert case.path.read_bytes() != old
    assert case.path.stat().st_mode & 0o777 == 0o640
    async with db_sessions() as db:
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_alto_500_read_failure(client, user_headers, input_upload_case, db_sessions, monkeypatch):
    case = await input_upload_case("alto")
    with monkeypatch.context() as patch:
        def broken_read(*args, **kwargs):
            raise OSError("injected transfer failure")
        patch.setattr(tempfile.SpooledTemporaryFile, "read", broken_read)
        response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert_internal_transfer_error(response)
    assert not case.path.exists()
    async with db_sessions() as db:
        assert not getattr(await db.get(case.entity, case.record_id), case.flag)
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW
    retry = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert retry.status_code == 201, retry.text


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("existing", [False, True], ids=["first-upload", "replacement"])
@pytest.mark.asyncio
async def test_put_alto_500_commit_failure(client, user_headers, input_upload_case, db_sessions, reject_upload_commit, existing):
    case = await input_upload_case("alto")
    old = b"original input bytes"
    if existing:
        case.path.parent.mkdir(exist_ok=True)
        case.path.write_bytes(old)
        async with db_sessions.begin() as db:
            setattr(await db.get(case.entity, case.record_id), case.flag, True)
    async with db_sessions() as db:
        before = [(case.entity, case.record_id, model.orm2dict(await db.get(case.entity, case.record_id))),
                  (model.Job, case.job_id, model.orm2dict(await db.get(model.Job, case.job_id)))]
    with reject_upload_commit(case.path, old=old if existing else None) as observed:
        response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert_internal_transfer_error(response)
    assert observed == [True]
    assert case.path.exists() == existing
    if existing:
        assert case.path.read_bytes() == old
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-*"))
    await assert_records_unchanged(db_sessions, before)
    retry = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert retry.status_code == (200 if existing else 201), retry.text
    async with db_sessions() as db:
        assert getattr(await db.get(case.entity, case.record_id), case.flag)
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-*"))






@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("variant", ["unicode", "utf16", "deep", "large"])
@pytest.mark.asyncio
async def test_put_alto_201_generated_xml(client, user_headers, input_upload_case, db_sessions, monkeypatch, variant):
    case = await input_upload_case("alto")
    monkeypatch.setattr(config, "ALTO_VALIDATION", {key: True for key in config.ALTO_VALIDATION})
    payload = make_xml_payload("alto", variant)
    if variant == "large":
        assert len(payload) > 1024 ** 2  # Exercise multipart's disk-spooling path.
    await assert_generated_xml_save(client, user_headers, case, db_sessions, payload=payload,
                                    status=201, code=AppCode.ALTO_UPLOADED)


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("variant", ["truncated", "invalid_encoding", "entity", "empty"])
@pytest.mark.asyncio
async def test_put_alto_400_generated_xml(client, user_headers, input_upload_case, db_sessions, variant):
    case = await input_upload_case("alto")
    await assert_generated_xml_save(client, user_headers, case, db_sessions,
                                    payload=make_xml_payload("alto", variant),
                                    status=400, code=AppCode.XML_PARSE_ERROR)


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("enabled", xml_validation_combinations("alto", accepted=True),
                         ids=lambda enabled: "+".join(enabled) or "none")
@pytest.mark.asyncio
async def test_put_alto_201_validation_combinations(client, user_headers, input_upload_case,
                                                         db_sessions, monkeypatch, enabled):
    case = await input_upload_case("alto")
    monkeypatch.setattr(config, "ALTO_VALIDATION", {key: key in enabled for key in config.ALTO_VALIDATION})
    await assert_generated_xml_save(client, user_headers, case, db_sessions,
                                    payload=make_xml_payload("alto", "validation_combinations"),
                                    status=201, code=AppCode.ALTO_UPLOADED)


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("enabled", xml_validation_combinations("alto", accepted=False),
                         ids=lambda enabled: "+".join(enabled) or "none")
@pytest.mark.asyncio
async def test_put_alto_422_validation_combinations(client, user_headers, input_upload_case,
                                                         db_sessions, monkeypatch, enabled):
    case = await input_upload_case("alto")
    monkeypatch.setattr(config, "ALTO_VALIDATION", {key: key in enabled for key in config.ALTO_VALIDATION})
    await assert_generated_xml_save(client, user_headers, case, db_sessions,
                                    payload=make_xml_payload("alto", "validation_combinations"),
                                    status=422, code=AppCode.ALTO_SCHEMA_INVALID)



@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("variant", ["missing_boundary", "invalid_body"])
@pytest.mark.asyncio
async def test_put_alto_400_multipart(client, user_headers, input_upload_case, db_sessions, variant):
    case = await input_upload_case("alto")
    await assert_multipart_rejected(client, user_headers, case, db_sessions,
                                    method="PUT", variant=variant)


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("variant", ["wrong_field", "empty_filename", "text_field"])
@pytest.mark.asyncio
async def test_put_alto_422_multipart(client, user_headers, input_upload_case, db_sessions, variant):
    case = await input_upload_case("alto")
    await assert_multipart_rejected(client, user_headers, case, db_sessions,
                                    method="PUT", variant=variant)




@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_alto_500_unexpected_crud_result(client, user_headers, input_upload_case, db_sessions,
                                                    monkeypatch, invariant_errors):
    case = await input_upload_case("alto")
    code = AppCode.API_KEY_VALID
    monkeypatch.setattr(user_routes.general_cruds, "update_image", AsyncMock(return_value=code))
    response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert_route_invariant(response, invariant_errors, code)
    assert not case.path.exists()
    assert not list(case.path.parent.glob("*.docapi-tmp-*"))
    async with db_sessions() as db:
        assert not getattr(await db.get(case.entity, case.record_id), case.flag)



@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("deleted", ["job", "image"])
@pytest.mark.asyncio
async def test_put_alto_404_disappeared_before_install(client, user_headers, input_upload_case,
                                                        db_sessions, monkeypatch, deleted):
    case = await input_upload_case("alto")
    case.path.parent.mkdir(exist_ok=True)
    case.path.write_bytes(b"previous file")
    entity, record_id = (model.Job, case.job_id) if deleted == "job" else (case.entity, case.record_id)
    delete_before_file_install(monkeypatch, db_sessions, entity, record_id)
    response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert response.status_code == 404, response.text
    expected = AppCode.JOB_NOT_FOUND if deleted == "job" else AppCode.IMAGE_NOT_FOUND_FOR_JOB
    assert response.json()["code"] == expected.value
    assert case.path.read_bytes() == b"previous file"
    assert not list(case.path.parent.glob("*.docapi-tmp-*"))


#
# PUT /v1/jobs/{job_id}/images/{image_id}/files/page - 201, 200, 400, 403, 404, 409, 422, 500
#

@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[3]], ids=[AppCode.PAGE_UPLOADED.value], indirect=True)
async def test_put_page_201(client, user_headers, created_job):
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

    assert r.status_code == 201, r.text

    body = r.json()
    assert body["code"] == AppCode.PAGE_UPLOADED.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_page_201_preserves_deletion_timestamps(client, user_headers, db_sessions, job_with_deletion_timestamps):
    job_id, before = await job_with_deletion_timestamps(base_objects.ProcessingState.NEW)
    response = await client.put(f"/v1/jobs/{job_id}/images/page.jpg/files/page", headers=user_headers, files={"file": ("upload", VALID_PAGE_XML)})
    assert response.status_code == 201, response.text
    await assert_deletion_timestamps(db_sessions, job_id, before)


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[3]], ids=[AppCode.PAGE_REUPLOADED.value], indirect=True)
async def test_put_page_200(client, user_headers, created_job):
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

    assert r.status_code == 201, r.text

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

    body = r.json()
    assert body["code"] == AppCode.PAGE_REUPLOADED.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
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


@pytest.mark.parametrize("user_role", ["user", "admin"])
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


@pytest.mark.parametrize("user_role", ["user", "admin"])
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


@pytest.mark.parametrize("user_role", ["user", "admin"])
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


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_page_404_job_not_found(client, user_headers):
    job_id = uuid4()
    response = await client.request('PUT', f'/v1/jobs/{job_id}/images/page.jpg/files/page', headers=user_headers, files={'file': ('page', b'data')})
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


@pytest.mark.asyncio
async def test_put_page_403_job_access_forbidden(client, user_headers, job_factory, worker_key):
    job_id = await job_factory(base_objects.ProcessingState.NEW, owner_key_id=worker_key.id)
    response = await client.request('PUT', f'/v1/jobs/{job_id}/images/page.jpg/files/page', headers=user_headers, files={'file': ('page', b'data')})
    assert response.status_code == 403, response.text
    assert response.json()["code"] == AppCode.API_KEY_FORBIDDEN_FOR_JOB.value


@pytest.mark.asyncio
@pytest.mark.parametrize("state", [state for state in base_objects.ProcessingState if state != base_objects.ProcessingState.NEW])
async def test_put_page_409_job_not_new(client, user_headers, job_factory, state):
    job_id = await job_factory(state)
    response = await client.request('PUT', f'/v1/jobs/{job_id}/images/page.jpg/files/page', headers=user_headers, files={'file': ('page', b'data')})
    assert response.status_code == 409, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_IN_NEW.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_page_422_invalid_request(client, user_headers):
    response = await client.request('PUT', '/v1/jobs/00000000-0000-0000-0000-000000000001/images/page.jpg/files/page', headers=user_headers)
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("option", ['namespace', 'has_page', 'has_text'])
async def test_put_page_422_optional_validation(client, user_headers, job_with_deletion_timestamps, monkeypatch, option):
    monkeypatch.setattr(config, "PAGE_VALIDATION", {**{key: False for key in config.PAGE_VALIDATION}, "root": True, option: True})
    job_id, _ = await job_with_deletion_timestamps(ProcessingState.NEW)
    response = await client.put(f"/v1/jobs/{job_id}/images/page.jpg/files/page", headers=user_headers,
                                files={"file": ("input.xml", b'<PcGts xmlns="urn:invalid"/>' if option == "namespace" else b"<PcGts/>")})
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.PAGE_SCHEMA_INVALID.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_page_201_root_validation_disabled(client, user_headers, job_with_deletion_timestamps, monkeypatch):
    monkeypatch.setattr(config, "PAGE_VALIDATION", {key: False for key in config.PAGE_VALIDATION})
    job_id, _ = await job_with_deletion_timestamps(ProcessingState.NEW)
    response = await client.put(f"/v1/jobs/{job_id}/images/page.jpg/files/page", headers=user_headers,
                                files={"file": ("input.xml", b"<other/>")})
    assert response.status_code == 201, response.text


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_page_500_write_failure(client, user_headers, input_upload_case, db_sessions, monkeypatch):
    case = await input_upload_case("page")
    with monkeypatch.context() as patch:
        real_open = builtins.open
        def broken_open(filename, *args, **kwargs):
            if Path(filename).parent == case.path.parent:
                raise OSError("injected transfer failure")
            return real_open(filename, *args, **kwargs)
        patch.setattr(user_routes, "open", broken_open, raising=False)
        response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert_internal_transfer_error(response)
    assert not case.path.exists()
    async with db_sessions() as db:
        assert not getattr(await db.get(case.entity, case.record_id), case.flag)
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW
    retry = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert retry.status_code == 201, retry.text
    async with db_sessions() as db:
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_page_500_database_failure(client, user_headers, input_upload_case, db_sessions, monkeypatch):
    case = await input_upload_case("page")
    with monkeypatch.context() as patch:
        method = 'update_image'
        patch.setattr(user_routes.general_cruds, method, AsyncMock(side_effect=DBError("injected transfer failure")))
        response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert_internal_transfer_error(response)
    # The file and upload metadata roll back together.
    assert not case.path.exists()
    async with db_sessions() as db:
        assert not getattr(await db.get(case.entity, case.record_id), case.flag)
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW
    retry = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert retry.status_code == 201, retry.text
    async with db_sessions() as db:
        assert getattr(await db.get(case.entity, case.record_id), case.flag)
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_page_400_interrupted_upload(client, user_headers, input_upload_case, db_sessions):
    case = await input_upload_case("page")
    metadata = False
    response = await client.put(case.url, headers=interrupted_upload_headers(user_headers, metadata=metadata),
                                content=interrupted_upload_body(metadata))
    assert response.status_code == 400, response.text
    assert response.json()["code"] == AppCode.HTTP_ERROR.value
    assert "injected transfer failure" not in response.text
    assert not case.path.exists()
    async with db_sessions() as db:
        assert not getattr(await db.get(case.entity, case.record_id), case.flag)
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW
    retry = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert retry.status_code == 201, retry.text


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("failure", ["write", "replace"])
@pytest.mark.asyncio
async def test_put_page_500_reupload_preserves_file(client, user_headers, input_upload_case, db_sessions, monkeypatch, failure):
    case = await input_upload_case("page")
    case.path.parent.mkdir(exist_ok=True)
    old = b"previous uploaded bytes"
    case.path.write_bytes(old)
    case.path.chmod(0o640)
    async with db_sessions.begin() as db:
        setattr(await db.get(case.entity, case.record_id), case.flag, True)
    async with db_sessions() as db:
        before = model.orm2dict(await db.get(case.entity, case.record_id))
    with monkeypatch.context() as patch:
        if failure == "replace":
            real_replace = user_routes.os.replace
            def broken_replace(source, destination):
                if Path(destination) == case.path and ".docapi-tmp-upload-staging-" in str(source):
                    raise OSError("injected transfer failure")
                return real_replace(source, destination)
            patch.setattr(user_routes.os, "replace", broken_replace)
        else:
            real_open = builtins.open
            def broken_open(filename, *args, **kwargs):
                opened = real_open(filename, *args, **kwargs)
                if Path(filename).parent != case.path.parent:
                    return opened
                @contextmanager
                def partial_writer():
                    with opened as file:
                        def write(data):
                            file.write(data[:1])
                            raise OSError("injected transfer failure")
                        yield SimpleNamespace(write=write)
                return partial_writer()
            patch.setattr(user_routes, "open", broken_open, raising=False)
        response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert_internal_transfer_error(response)
    assert case.path.read_bytes() == old
    assert set(case.path.parent.iterdir()) == {case.path, case.path.with_name(f".{case.path.name}.docapi-save.lock")}
    async with db_sessions() as db:
        assert model.orm2dict(await db.get(case.entity, case.record_id)) == before
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW
    retry = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert retry.status_code == 200, retry.text
    assert case.path.read_bytes() != old
    assert case.path.stat().st_mode & 0o777 == 0o640
    async with db_sessions() as db:
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_page_500_read_failure(client, user_headers, input_upload_case, db_sessions, monkeypatch):
    case = await input_upload_case("page")
    with monkeypatch.context() as patch:
        def broken_read(*args, **kwargs):
            raise OSError("injected transfer failure")
        patch.setattr(tempfile.SpooledTemporaryFile, "read", broken_read)
        response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert_internal_transfer_error(response)
    assert not case.path.exists()
    async with db_sessions() as db:
        assert not getattr(await db.get(case.entity, case.record_id), case.flag)
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW
    retry = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert retry.status_code == 201, retry.text


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("existing", [False, True], ids=["first-upload", "replacement"])
@pytest.mark.asyncio
async def test_put_page_500_commit_failure(client, user_headers, input_upload_case, db_sessions, reject_upload_commit, existing):
    case = await input_upload_case("page")
    old = b"original input bytes"
    if existing:
        case.path.parent.mkdir(exist_ok=True)
        case.path.write_bytes(old)
        async with db_sessions.begin() as db:
            setattr(await db.get(case.entity, case.record_id), case.flag, True)
    async with db_sessions() as db:
        before = [(case.entity, case.record_id, model.orm2dict(await db.get(case.entity, case.record_id))),
                  (model.Job, case.job_id, model.orm2dict(await db.get(model.Job, case.job_id)))]
    with reject_upload_commit(case.path, old=old if existing else None) as observed:
        response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert_internal_transfer_error(response)
    assert observed == [True]
    assert case.path.exists() == existing
    if existing:
        assert case.path.read_bytes() == old
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-*"))
    await assert_records_unchanged(db_sessions, before)
    retry = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert retry.status_code == (200 if existing else 201), retry.text
    async with db_sessions() as db:
        assert getattr(await db.get(case.entity, case.record_id), case.flag)
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-*"))






@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("variant", ["unicode", "utf16", "deep", "large"])
@pytest.mark.asyncio
async def test_put_page_201_generated_xml(client, user_headers, input_upload_case, db_sessions, monkeypatch, variant):
    case = await input_upload_case("page")
    monkeypatch.setattr(config, "PAGE_VALIDATION", {key: True for key in config.PAGE_VALIDATION})
    payload = make_xml_payload("page", variant)
    if variant == "large":
        assert len(payload) > 1024 ** 2  # Exercise multipart's disk-spooling path.
    await assert_generated_xml_save(client, user_headers, case, db_sessions, payload=payload,
                                    status=201, code=AppCode.PAGE_UPLOADED)


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("variant", ["truncated", "invalid_encoding", "entity", "empty"])
@pytest.mark.asyncio
async def test_put_page_400_generated_xml(client, user_headers, input_upload_case, db_sessions, variant):
    case = await input_upload_case("page")
    await assert_generated_xml_save(client, user_headers, case, db_sessions,
                                    payload=make_xml_payload("page", variant),
                                    status=400, code=AppCode.XML_PARSE_ERROR)


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("enabled", xml_validation_combinations("page", accepted=True),
                         ids=lambda enabled: "+".join(enabled) or "none")
@pytest.mark.asyncio
async def test_put_page_201_validation_combinations(client, user_headers, input_upload_case,
                                                         db_sessions, monkeypatch, enabled):
    case = await input_upload_case("page")
    monkeypatch.setattr(config, "PAGE_VALIDATION", {key: key in enabled for key in config.PAGE_VALIDATION})
    await assert_generated_xml_save(client, user_headers, case, db_sessions,
                                    payload=make_xml_payload("page", "validation_combinations"),
                                    status=201, code=AppCode.PAGE_UPLOADED)


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("enabled", xml_validation_combinations("page", accepted=False),
                         ids=lambda enabled: "+".join(enabled) or "none")
@pytest.mark.asyncio
async def test_put_page_422_validation_combinations(client, user_headers, input_upload_case,
                                                         db_sessions, monkeypatch, enabled):
    case = await input_upload_case("page")
    monkeypatch.setattr(config, "PAGE_VALIDATION", {key: key in enabled for key in config.PAGE_VALIDATION})
    await assert_generated_xml_save(client, user_headers, case, db_sessions,
                                    payload=make_xml_payload("page", "validation_combinations"),
                                    status=422, code=AppCode.PAGE_SCHEMA_INVALID)



@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("variant", ["missing_boundary", "invalid_body"])
@pytest.mark.asyncio
async def test_put_page_400_multipart(client, user_headers, input_upload_case, db_sessions, variant):
    case = await input_upload_case("page")
    await assert_multipart_rejected(client, user_headers, case, db_sessions,
                                    method="PUT", variant=variant)


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("variant", ["wrong_field", "empty_filename", "text_field"])
@pytest.mark.asyncio
async def test_put_page_422_multipart(client, user_headers, input_upload_case, db_sessions, variant):
    case = await input_upload_case("page")
    await assert_multipart_rejected(client, user_headers, case, db_sessions,
                                    method="PUT", variant=variant)




@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_page_500_unexpected_crud_result(client, user_headers, input_upload_case, db_sessions,
                                                    monkeypatch, invariant_errors):
    case = await input_upload_case("page")
    code = AppCode.API_KEY_VALID
    monkeypatch.setattr(user_routes.general_cruds, "update_image", AsyncMock(return_value=code))
    response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert_route_invariant(response, invariant_errors, code)
    assert not case.path.exists()
    assert not list(case.path.parent.glob("*.docapi-tmp-*"))
    async with db_sessions() as db:
        assert not getattr(await db.get(case.entity, case.record_id), case.flag)



@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("deleted", ["job", "image"])
@pytest.mark.asyncio
async def test_put_page_404_disappeared_before_install(client, user_headers, input_upload_case,
                                                        db_sessions, monkeypatch, deleted):
    case = await input_upload_case("page")
    case.path.parent.mkdir(exist_ok=True)
    case.path.write_bytes(b"previous file")
    entity, record_id = (model.Job, case.job_id) if deleted == "job" else (case.entity, case.record_id)
    delete_before_file_install(monkeypatch, db_sessions, entity, record_id)
    response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert response.status_code == 404, response.text
    expected = AppCode.JOB_NOT_FOUND if deleted == "job" else AppCode.IMAGE_NOT_FOUND_FOR_JOB
    assert response.json()["code"] == expected.value
    assert case.path.read_bytes() == b"previous file"
    assert not list(case.path.parent.glob("*.docapi-tmp-*"))


#
# PUT /v1/jobs/{job_id}/files/metadata - 201, 200, 403, 404, 409, 422, 400, 500
#

@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[-1]], ids=[AppCode.META_JSON_UPLOADED.value], indirect=True)
async def test_put_metadata_201(client, user_headers, created_job):
    job = created_job["created_job"]
    job_id = job["id"]

    r = await client.put(
        f"/v1/jobs/{job_id}/files/metadata",
        headers=user_headers,
        json={"meta": "dummy"},
    )

    assert r.status_code == 201, r.text

    body = r.json()
    assert body["code"] == AppCode.META_JSON_UPLOADED.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_metadata_201_preserves_deletion_timestamps(client, user_headers, db_sessions, job_with_deletion_timestamps):
    job_id, before = await job_with_deletion_timestamps(base_objects.ProcessingState.NEW)
    response = await client.put(f"/v1/jobs/{job_id}/files/metadata", headers=user_headers, json={})
    assert response.status_code == 201, response.text
    await assert_deletion_timestamps(db_sessions, job_id, before)


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[-1]], ids=[AppCode.META_JSON_REUPLOADED.value], indirect=True)
async def test_put_metadata_200(client, user_headers, created_job):
    job = created_job["created_job"]
    job_id = job["id"]

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

    body = r.json()
    assert body["code"] == AppCode.META_JSON_REUPLOADED.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
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
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[-1]], ids=[AppCode.JOB_NOT_IN_NEW.value], indirect=True)
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


@pytest.mark.parametrize("user_role", ["user", "admin"])
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


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_metadata_404_job_not_found(client, user_headers):
    job_id = uuid4()
    response = await client.request('PUT', f'/v1/jobs/{job_id}/files/metadata', headers=user_headers, json={})
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


@pytest.mark.asyncio
async def test_put_metadata_403_job_access_forbidden(client, user_headers, job_factory, worker_key):
    job_id = await job_factory(base_objects.ProcessingState.NEW, owner_key_id=worker_key.id)
    response = await client.request('PUT', f'/v1/jobs/{job_id}/files/metadata', headers=user_headers, json={})
    assert response.status_code == 403, response.text
    assert response.json()["code"] == AppCode.API_KEY_FORBIDDEN_FOR_JOB.value


@pytest.mark.asyncio
@pytest.mark.parametrize("state", [state for state in base_objects.ProcessingState if state != base_objects.ProcessingState.NEW])
async def test_put_metadata_409_job_not_new(client, user_headers, job_factory, state):
    job_id = await job_factory(state)
    response = await client.request('PUT', f'/v1/jobs/{job_id}/files/metadata', headers=user_headers, json={})
    assert response.status_code == 409, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_IN_NEW.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_metadata_422_invalid_request(client, user_headers):
    response = await client.request('PUT', '/v1/jobs/00000000-0000-0000-0000-000000000001/files/metadata', headers=user_headers)
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_metadata_500_write_failure(client, user_headers, input_upload_case, db_sessions, monkeypatch):
    case = await input_upload_case("metadata")
    with monkeypatch.context() as patch:
        real_open = builtins.open
        def broken_open(filename, *args, **kwargs):
            if Path(filename).parent == case.path.parent:
                raise OSError("injected transfer failure")
            return real_open(filename, *args, **kwargs)
        patch.setattr(user_routes, "open", broken_open, raising=False)
        response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert_internal_transfer_error(response)
    assert not case.path.exists()
    async with db_sessions() as db:
        assert not getattr(await db.get(case.entity, case.record_id), case.flag)
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW
    retry = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert retry.status_code == 201, retry.text
    async with db_sessions() as db:
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_metadata_500_database_failure(client, user_headers, input_upload_case, db_sessions, monkeypatch):
    case = await input_upload_case("metadata")
    with monkeypatch.context() as patch:
        method = 'update_job'
        patch.setattr(user_routes.general_cruds, method, AsyncMock(side_effect=DBError("injected transfer failure")))
        response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert_internal_transfer_error(response)
    # The file and upload metadata roll back together.
    assert not case.path.exists()
    async with db_sessions() as db:
        assert not getattr(await db.get(case.entity, case.record_id), case.flag)
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW
    retry = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert retry.status_code == 201, retry.text
    async with db_sessions() as db:
        assert getattr(await db.get(case.entity, case.record_id), case.flag)
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_metadata_400_interrupted_upload(client, user_headers, input_upload_case, db_sessions):
    case = await input_upload_case("metadata")
    metadata = True
    response = await client.put(case.url, headers=interrupted_upload_headers(user_headers, metadata=metadata),
                                content=interrupted_upload_body(metadata))
    assert response.status_code == 400, response.text
    assert response.json()["code"] == AppCode.HTTP_ERROR.value
    assert "injected transfer failure" not in response.text
    assert not case.path.exists()
    async with db_sessions() as db:
        assert not getattr(await db.get(case.entity, case.record_id), case.flag)
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW
    retry = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert retry.status_code == 201, retry.text


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("failure", ["write", "replace"])
@pytest.mark.asyncio
async def test_put_metadata_500_reupload_preserves_file(client, user_headers, input_upload_case, db_sessions, monkeypatch, failure):
    case = await input_upload_case("metadata")
    case.path.parent.mkdir(exist_ok=True)
    old = b"previous uploaded bytes"
    case.path.write_bytes(old)
    case.path.chmod(0o640)
    async with db_sessions.begin() as db:
        setattr(await db.get(case.entity, case.record_id), case.flag, True)
    async with db_sessions() as db:
        before = model.orm2dict(await db.get(case.entity, case.record_id))
    with monkeypatch.context() as patch:
        if failure == "replace":
            real_replace = user_routes.os.replace
            def broken_replace(source, destination):
                if Path(destination) == case.path and ".docapi-tmp-upload-staging-" in str(source):
                    raise OSError("injected transfer failure")
                return real_replace(source, destination)
            patch.setattr(user_routes.os, "replace", broken_replace)
        else:
            real_open = builtins.open
            def broken_open(filename, *args, **kwargs):
                opened = real_open(filename, *args, **kwargs)
                if Path(filename).parent != case.path.parent:
                    return opened
                @contextmanager
                def partial_writer():
                    with opened as file:
                        def write(data):
                            file.write(data[:1])
                            raise OSError("injected transfer failure")
                        yield SimpleNamespace(write=write)
                return partial_writer()
            patch.setattr(user_routes, "open", broken_open, raising=False)
        response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert_internal_transfer_error(response)
    assert case.path.read_bytes() == old
    assert set(case.path.parent.iterdir()) == {case.path, case.path.with_name(f".{case.path.name}.docapi-save.lock")}
    async with db_sessions() as db:
        assert model.orm2dict(await db.get(case.entity, case.record_id)) == before
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW
    retry = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert retry.status_code == 200, retry.text
    assert case.path.read_bytes() != old
    assert case.path.stat().st_mode & 0o777 == 0o640
    async with db_sessions() as db:
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("existing", [False, True], ids=["first-upload", "replacement"])
@pytest.mark.asyncio
async def test_put_metadata_500_commit_failure(client, user_headers, input_upload_case, db_sessions, reject_upload_commit, existing):
    case = await input_upload_case("metadata")
    old = b"original input bytes"
    if existing:
        case.path.parent.mkdir(exist_ok=True)
        case.path.write_bytes(old)
        async with db_sessions.begin() as db:
            setattr(await db.get(case.entity, case.record_id), case.flag, True)
    async with db_sessions() as db:
        before = [(case.entity, case.record_id, model.orm2dict(await db.get(case.entity, case.record_id))),
                  (model.Job, case.job_id, model.orm2dict(await db.get(model.Job, case.job_id)))]
    with reject_upload_commit(case.path, old=old if existing else None) as observed:
        response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert_internal_transfer_error(response)
    assert observed == [True]
    assert case.path.exists() == existing
    if existing:
        assert case.path.read_bytes() == old
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-*"))
    await assert_records_unchanged(db_sessions, before)
    retry = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert retry.status_code == (200 if existing else 201), retry.text
    async with db_sessions() as db:
        assert getattr(await db.get(case.entity, case.record_id), case.flag)
        assert (await db.get(model.Job, case.job_id)).state == ProcessingState.NEW
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-*"))






@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_metadata_500_unexpected_crud_result(client, user_headers, input_upload_case, db_sessions,
                                                    monkeypatch, invariant_errors):
    case = await input_upload_case("metadata")
    code = AppCode.API_KEY_VALID
    monkeypatch.setattr(user_routes.general_cruds, "update_job", AsyncMock(return_value=code))
    response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert_route_invariant(response, invariant_errors, code)
    assert not case.path.exists()
    assert not list(case.path.parent.glob("*.docapi-tmp-*"))
    async with db_sessions() as db:
        assert not getattr(await db.get(case.entity, case.record_id), case.flag)




@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_put_metadata_404_disappeared_after_check(client, user_headers, input_upload_case, db_sessions, monkeypatch):
    case = await input_upload_case("metadata")
    delete_job_after_exists(monkeypatch, db_sessions, case.job_id)
    response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value
    assert not case.path.exists()



@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.parametrize("deleted", ["job"])
@pytest.mark.asyncio
async def test_put_metadata_404_disappeared_before_install(client, user_headers, input_upload_case,
                                                        db_sessions, monkeypatch, deleted):
    case = await input_upload_case("metadata")
    case.path.parent.mkdir(exist_ok=True)
    case.path.write_bytes(b"previous file")
    entity, record_id = (model.Job, case.job_id) if deleted == "job" else (case.entity, case.record_id)
    delete_before_file_install(monkeypatch, db_sessions, entity, record_id)
    response = await client.put(case.url, headers=user_headers, **case.kwargs)
    assert response.status_code == 404, response.text
    expected = AppCode.JOB_NOT_FOUND if deleted == "job" else AppCode.IMAGE_NOT_FOUND_FOR_JOB
    assert response.json()["code"] == expected.value
    assert case.path.read_bytes() == b"previous file"
    assert not list(case.path.parent.glob("*.docapi-tmp-*"))


#
# PATCH /v1/jobs/{job_id} - 200, 403, 404, 409, 422
#

@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_CANCELLED], indirect=True)
async def test_patch_job_200_cancel_new_job(client, user_headers, cancelled_new_job):
    job = cancelled_new_job["created_job"]
    job_id = job["id"]

    r = await client.get(f"/v1/jobs/{job_id}", headers=user_headers)
    assert r.status_code == 200, r.text
    data = r.json()["data"]
    assert data["state"] == base_objects.ProcessingState.CANCELLED.value
    assert data["last_change"] == data["finished_date"]
    assert datetime.fromisoformat(data["last_change"].replace("Z", "+00:00")) > datetime.fromisoformat(job["last_change"].replace("Z", "+00:00"))


@pytest.mark.parametrize("user_role", ["user", "admin"])
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
    assert data["last_change"] == data["finished_date"]


@pytest.mark.parametrize("user_role", ["user", "admin"])
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
    assert body["code"] == AppCode.JOB_CANCELLED.value

    r = await client.get(f"/v1/jobs/{job_id}", headers=user_headers)
    assert r.status_code == 200, r.text
    data = r.json()["data"]
    assert data["state"] == base_objects.ProcessingState.CANCELLED.value
    assert data["last_change"] == data["finished_date"]


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_CANCELLED], indirect=True)
async def test_patch_job_200_cancel_error_job(client, user_headers, job_marked_error):
    job = job_marked_error["created_job"]
    job_id = job["id"]

    r = await client.patch(
        f"/v1/jobs/{job_id}",
        headers=user_headers,
        json={"state": base_objects.ProcessingState.CANCELLED.value},
    )
    assert r.status_code == 200, r.text

    body = r.json()
    assert body["code"] == AppCode.JOB_CANCELLED.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_UNCANCELLABLE], indirect=True)
async def test_patch_job_409_cancel_cancelled_job(client, user_headers, cancelled_new_job):
    job = cancelled_new_job["created_job"]
    job_id = job["id"]

    r = await client.patch(
        f"/v1/jobs/{job_id}",
        headers=user_headers,
        json={"state": base_objects.ProcessingState.CANCELLED.value},
    )
    assert r.status_code == 409, r.text

    body = r.json()
    assert body["code"] == AppCode.JOB_UNCANCELLABLE.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_UNCANCELLABLE], indirect=True)
async def test_patch_job_409_cancel_done_job(client, user_headers, job_marked_done):
    job = job_marked_done["created_job"]
    job_id = job["id"]

    r = await client.patch(
        f"/v1/jobs/{job_id}",
        headers=user_headers,
        json={"state": base_objects.ProcessingState.CANCELLED.value},
    )
    assert r.status_code == 409, r.text

    body = r.json()
    assert body["code"] == AppCode.JOB_UNCANCELLABLE.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_UNCANCELLABLE], indirect=True)
async def test_patch_job_409_cancel_failed_job(client, user_headers, failed_job):
    job = failed_job["created_job"]
    job_id = job["id"]

    r = await client.patch(
        f"/v1/jobs/{job_id}",
        headers=user_headers,
        json={"state": base_objects.ProcessingState.CANCELLED.value},
    )
    assert r.status_code == 409, r.text

    body = r.json()
    assert body["code"] == AppCode.JOB_UNCANCELLABLE.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_patch_job_404_job_not_found(client, user_headers):
    job_id = uuid4()
    response = await client.request('PATCH', f'/v1/jobs/{job_id}', headers=user_headers, json={'state': 'cancelled'})
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


@pytest.mark.asyncio
async def test_patch_job_403_job_access_forbidden(client, user_headers, job_factory, worker_key):
    job_id = await job_factory(base_objects.ProcessingState.NEW, owner_key_id=worker_key.id)
    response = await client.request('PATCH', f'/v1/jobs/{job_id}', headers=user_headers, json={'state': 'cancelled'})
    assert response.status_code == 403, response.text
    assert response.json()["code"] == AppCode.API_KEY_FORBIDDEN_FOR_JOB.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_patch_job_422_invalid_request(client, user_headers):
    response = await client.request('PATCH', '/v1/jobs/not-a-uuid', headers=user_headers, json={})
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


@pytest.mark.asyncio
@pytest.mark.parametrize("state", [None, "new", "queued", "processing", "done", "error", "failed"])
async def test_patch_job_403_user_may_only_cancel(client, user_headers, db_sessions, job_factory, state):
    job_id = await job_factory(ProcessingState.NEW)
    response = await client.patch(f"/v1/jobs/{job_id}", headers=user_headers, json={"state": state, "progress": .5})
    assert response.status_code == 403, response.text
    assert response.json()["code"] == AppCode.API_KEY_USER_FORBIDDEN.value
    async with db_sessions() as db:
        assert (await db.get(model.Job, job_id)).state == ProcessingState.NEW




@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_patch_job_404_disappeared_after_check(client, user_headers, job_factory, db_sessions, monkeypatch):
    job_id = await job_factory(ProcessingState.NEW)
    delete_job_after_exists(monkeypatch, db_sessions, job_id)
    response = await client.patch(f"/v1/jobs/{job_id}", headers=user_headers, json={"state": "cancelled"})
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_patch_job_500_unexpected_cancel_result(client, user_headers, job_factory, monkeypatch, invariant_errors):
    job_id = await job_factory(ProcessingState.NEW)
    code = AppCode.API_KEY_VALID
    monkeypatch.setattr(general_routes.user_cruds, "cancel_job", AsyncMock(return_value=code))
    response = await client.patch(f"/v1/jobs/{job_id}", headers=user_headers, json={"state": "cancelled"})
    assert_route_invariant(response, invariant_errors, code)



@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_patch_job_404_disappeared_before_update(client, user_headers, job_factory, db_sessions, monkeypatch):
    job_id = await job_factory(ProcessingState.NEW)
    delete_job_before_crud(monkeypatch, db_sessions, job_id, general_routes.user_cruds, "cancel_job")
    response = await client.patch(f"/v1/jobs/{job_id}", headers=user_headers, json={"state": "cancelled"})
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


#
# GET /v1/jobs/{job_id}/result - 200, 403, 404, 409, 410, 422, 425
#

@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_RESULT_RETRIEVED], indirect=True)
async def test_get_job_result_200(client, reader_headers, job_marked_done):
    job = job_marked_done["created_job"]
    job_id = job["id"]

    r = await client.get(f"/v1/jobs/{job_id}/result", headers=reader_headers)
    assert r.status_code == 200, r.text

    # check for zip
    assert r.headers["Content-Type"] == "application/zip"
    body = r.content
    assert isinstance(body, bytes)
    assert len(body) > 0
    disposition = r.headers["Content-Disposition"]
    assert disposition.startswith("attachment;")
    params = disposition.split(";")
    filename_param = next((p for p in params if p.strip().startswith("filename=")), None)
    assert filename_param is not None
    filename = filename_param.split("=", 1)[1].strip().strip('"')
    assert filename.endswith(".zip")

    is_zip = False
    with io.BytesIO(body) as zip_bytes_io:
        import zipfile
        with zipfile.ZipFile(zip_bytes_io, 'r') as zip_file:
            is_zip = True

    assert is_zip


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_CANCELLED], indirect=True)
async def test_get_job_result_409_job_cancelled(client, reader_headers, cancelled_new_job):
    job = cancelled_new_job["created_job"]
    job_id = job["id"]

    r = await client.get(f"/v1/jobs/{job_id}/result", headers=reader_headers)
    assert r.status_code == 409, r.text

    body = r.json()
    assert body["code"] == AppCode.JOB_CANCELLED.value


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_FAILED], indirect=True)
async def test_get_job_result_409_job_failed(client, reader_headers, failed_job):
    job = failed_job["created_job"]
    job_id = job["id"]

    r = await client.get(f"/v1/jobs/{job_id}/result", headers=reader_headers)
    assert r.status_code == 409, r.text

    body = r.json()
    assert body["code"] == AppCode.JOB_FAILED.value


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_RESULT_GONE.value], indirect=True)
async def test_get_job_result_410(client, reader_headers, job_marked_done, tmp_path):
    job  = job_marked_done["created_job"]
    job_id = job["id"]

    result_path = tmp_path / "results" / f"{job_id}.zip"
    assert os.path.exists(result_path), f"Result file should exist at {result_path} in the test directory"
    os.remove(result_path)

    r = await client.get(f"/v1/jobs/{job_id}/result", headers=reader_headers)
    assert r.status_code == 410, r.text

    body = r.json()
    assert body["code"] == AppCode.JOB_RESULT_GONE.value


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
async def test_get_job_result_410_after_cleanup(client, reader_headers, db_sessions, output_storage, output_factory):
    job_id, _ = await output_factory(ProcessingState.DONE, output_storage - timedelta(days=8))
    response = await client.get(f"/v1/jobs/{job_id}/result", headers=reader_headers)
    assert response.status_code == 200
    assert response.content == b"output"
    async with db_sessions() as db:
        await cleanup.delete_expired_job_outputs(db)
    response = await client.get(f"/v1/jobs/{job_id}/result", headers=reader_headers)
    assert response.status_code == 410, response.text
    assert response.json()["code"] == AppCode.JOB_RESULT_GONE.value


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_RESULT_NOT_READY.value], indirect=True)
async def test_get_job_result_425_job_new(client, reader_headers, created_job):
    job = created_job["created_job"]
    job_id = job["id"]

    r = await client.get(f"/v1/jobs/{job_id}/result", headers=reader_headers)
    assert r.status_code == 425, r.text

    body = r.json()
    assert body["code"] == AppCode.JOB_RESULT_NOT_READY.value
    assert "state" in body["details"]
    assert body["details"]["state"] == base_objects.ProcessingState.NEW.value


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_RESULT_NOT_READY.value], indirect=True)
async def test_get_job_result_425_job_queued(client, reader_headers, job_with_required_uploads_by_payload_name):
    job = job_with_required_uploads_by_payload_name["created_job"]
    job_id = job["id"]

    r = await client.get(f"/v1/jobs/{job_id}/result", headers=reader_headers)
    assert r.status_code == 425, r.text

    body = r.json()
    assert body["code"] == AppCode.JOB_RESULT_NOT_READY.value
    assert "state" in body["details"]
    assert body["details"]["state"] == base_objects.ProcessingState.QUEUED.value


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_RESULT_NOT_READY.value], indirect=True)
async def test_get_job_result_425_job_processing(client, reader_headers, lease_job):
    job = lease_job["created_job"]
    job_id = job["id"]

    r = await client.get(f"/v1/jobs/{job_id}/result", headers=reader_headers)
    assert r.status_code == 425, r.text

    body = r.json()
    assert body["code"] == AppCode.JOB_RESULT_NOT_READY.value
    assert "state" in body["details"]
    assert body["details"]["state"] == base_objects.ProcessingState.PROCESSING.value


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_RESULT_NOT_READY.value], indirect=True)
async def test_get_job_result_425_job_error(client, reader_headers, job_marked_error):
    job = job_marked_error["created_job"]
    job_id = job["id"]

    r = await client.get(f"/v1/jobs/{job_id}/result", headers=reader_headers)
    assert r.status_code == 425, r.text

    body = r.json()
    assert body["code"] == AppCode.JOB_RESULT_NOT_READY.value
    assert "state" in body["details"]
    assert body["details"]["state"] == base_objects.ProcessingState.ERROR.value


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
async def test_get_job_result_404_job_not_found(client, reader_headers):
    job_id = uuid4()
    response = await client.request('GET', f'/v1/jobs/{job_id}/result', headers=reader_headers)
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


@pytest.mark.asyncio
async def test_get_job_result_403_job_access_forbidden(client, user_headers, job_factory, worker_key):
    job_id = await job_factory(base_objects.ProcessingState.NEW, owner_key_id=worker_key.id)
    response = await client.request('GET', f'/v1/jobs/{job_id}/result', headers=user_headers)
    assert response.status_code == 403, response.text
    assert response.json()["code"] == AppCode.API_KEY_FORBIDDEN_FOR_JOB.value


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
async def test_get_job_result_422_invalid_request(client, reader_headers):
    response = await client.request('GET', '/v1/jobs/not-a-uuid/result', headers=reader_headers)
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.parametrize("failure", ["open", "read", "disconnect"])
@pytest.mark.asyncio
async def test_get_job_result_200_stream_failure(client, reader_headers, download_case, db_sessions, monkeypatch, failure):
    case = await download_case("result")
    await interrupt_download(main.app, case.url, reader_headers, case.path, monkeypatch, failure)
    assert case.path.read_bytes() == case.content
    await assert_records_unchanged(db_sessions, case.records)
    retry = await client.get(case.url, headers=reader_headers)
    assert retry.status_code == 200, retry.text
    assert retry.content == case.content





@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
async def test_get_job_result_404_disappeared_after_check(client, reader_headers, job_factory, db_sessions, monkeypatch):
    job_id = await job_factory(ProcessingState.DONE)
    delete_job_after_exists(monkeypatch, db_sessions, job_id)
    response = await client.get(f"/v1/jobs/{job_id}/result", headers=reader_headers)
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


#
# GET / - 307
#

@pytest.mark.asyncio
async def test_get_root_307_docs_redirect(client):
    response = await client.get("/", follow_redirects=False)
    assert response.status_code == 307
    assert response.headers["location"] == "/docs"


#
# GET /v1/engines - 200, 403, 422
#

@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["readonly", "user", "worker"])
async def test_get_engines_200_public_fields(client, engine_factory, role):
    await engine_factory(name="Engine10")
    await engine_factory(name="Engine2")
    await engine_factory(name="Hidden", active=False)
    headers = {"X-API-Key": getattr(config, f"TEST_{role.upper()}_KEY")}
    response = await client.get("/v1/engines", headers=headers)
    assert response.status_code == 200, response.text
    assert response.json()["code"] == AppCode.ENGINES_RETRIEVED.value
    engines = response.json()["data"]
    assert [e["name"] for e in engines] == ["Engine2", "Engine10"]
    for engine in engines:
        assert all(engine.get(field) is None for field in ("id", "active", "definition", "created_date", "last_used", "files_updated"))


@pytest.mark.asyncio
@pytest.mark.parametrize("params,code", [
    ({"active": "true"}, AppCode.API_KEY_FORBIDDEN_FOR_ENGINE_ACTIVE),
    ({"active": "false"}, AppCode.API_KEY_FORBIDDEN_FOR_ENGINE_ACTIVE),
    ({"show_definition": "true"}, AppCode.API_KEY_FORBIDDEN_FOR_ENGINE_DEFINITION),
])
@pytest.mark.parametrize("role", ["readonly", "user", "worker"])
async def test_get_engines_403_admin_filters(client, params, code, role):
    response = await client.get("/v1/engines", params=params,
        headers={"X-API-Key": getattr(config, f"TEST_{role.upper()}_KEY")})
    assert response.status_code == 403, response.text
    assert response.json()["code"] == code.value


@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
async def test_get_engines_422_invalid_filter(client, user_headers):
    response = await client.get("/v1/engines", headers=user_headers, params={"default": "invalid"})
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value




@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.asyncio
async def test_get_engines_500_unexpected_crud_result(client, reader_headers, monkeypatch, invariant_errors):
    code = AppCode.API_KEY_VALID
    monkeypatch.setattr(general_routes.general_cruds, "get_engines", AsyncMock(return_value=([], code)))
    response = await client.get("/v1/engines", headers=reader_headers)
    assert_route_invariant(response, invariant_errors, code)


#
# GET /openapi.json - 200
#

@pytest.mark.parametrize("user_role", ["user", "admin"])
@pytest.mark.asyncio
@pytest.mark.parametrize("visible", [True, False], ids=["visible", "hidden"])
@pytest.mark.parametrize("flag,method,path", OPENAPI_VISIBILITY_ROUTES, ids=[row[0] for row in OPENAPI_VISIBILITY_ROUTES])
async def test_get_openapi_200_endpoint_visibility(client, user_headers, readonly_headers, monkeypatch, visible,
                                                   flag, method, path, job_with_deletion_timestamps):
    # Startup configuration binding is tested independently in a fresh process.
    for other_flag, other_method, other_path in OPENAPI_VISIBILITY_ROUTES:
        route = next(route for route in main.app.routes
                     if getattr(route, "path", None) == other_path and other_method.upper() in route.methods)
        monkeypatch.setattr(route, "include_in_schema", visible if other_flag == flag else True)
    monkeypatch.setattr(config, "SHOW_SECTIONS", {"User", "Worker", "Admin"})
    monkeypatch.setattr(main.app, "openapi_schema", None)
    schema = await client.get("/openapi.json")
    assert schema.status_code == 200, schema.text
    operations = {(verb, url) for url, methods in schema.json()["paths"].items() for verb in methods}
    assert ((method, path) in operations) == visible
    for other_flag, other_method, other_path in OPENAPI_VISIBILITY_ROUTES:
        if other_flag != flag:
            assert (other_method, other_path) in operations
    # The cached schema must preserve the same visibility.
    cached = await client.get("/openapi.json")
    assert cached.status_code == 200
    assert cached.json() == schema.json()

    kwargs = {}
    if method == "get":
        url, expected_status, expected_code = path, 200, AppCode.ENGINES_RETRIEVED
    else:
        job_id, _ = await job_with_deletion_timestamps(ProcessingState.NEW)
        url = path.format(job_id=job_id, image_name="page.jpg")
        expected_status = 201
        if flag == "SHOW_PUT_METADATA":
            kwargs = {"json": {"source": "visibility test"}}
            expected_code = AppCode.META_JSON_UPLOADED
        else:
            data, content_type, expected_code = {
                "SHOW_PUT_IMAGE": (*make_white_image_bytes(), AppCode.IMAGE_UPLOADED),
                "SHOW_PUT_ALTO": (VALID_ALTO_XML, "application/xml", AppCode.ALTO_UPLOADED),
                "SHOW_PUT_PAGE": (VALID_PAGE_XML, "application/xml", AppCode.PAGE_UPLOADED),
            }[flag]
            kwargs = {"files": {"file": ("upload", data, content_type)}}
    response = await client.request(method, url, headers=user_headers, **kwargs)
    assert response.status_code == expected_status, response.text
    assert response.json()["code"] == expected_code.value
    unauthenticated = await client.request(method, url, **kwargs)
    assert unauthenticated.status_code == 401, unauthenticated.text
    assert unauthenticated.json()["code"] == AppCode.API_KEY_MISSING.value
    if method == "put":
        forbidden = await client.request(method, url, headers=readonly_headers, **kwargs)
        assert forbidden.status_code == 403, forbidden.text
        assert forbidden.json()["code"] == AppCode.API_KEY_ROLE_FORBIDDEN.value


@pytest.mark.asyncio
@pytest.mark.parametrize("sections", [set(combo) for size in range(4)
    for combo in combinations(("User", "Worker", "Admin"), size)] + [{"General"}, {"Debug"}, {"Unknown"}],
    ids=lambda sections: ",".join(sorted(sections)) or "none")
@pytest.mark.parametrize("visibility", ["all", "none", "mixed"])
async def test_get_openapi_200_sections(client, user_headers, worker_headers, admin_headers,
                                       job_factory, monkeypatch, sections, visibility):
    hidden = set()
    for index, (flag, method, path) in enumerate(OPENAPI_VISIBILITY_ROUTES):
        visible = visibility == "all" or (visibility == "mixed" and index % 2 == 0)
        route = next(route for route in main.app.routes
                     if getattr(route, "path", None) == path and method.upper() in route.methods)
        monkeypatch.setattr(route, "include_in_schema", visible)
        if not visible:
            hidden.add((method, path))
    monkeypatch.setattr(config, "SHOW_SECTIONS", sections)
    monkeypatch.setattr(main.app, "openapi_schema", None)
    response = await client.get("/openapi.json")
    assert response.status_code == 200, response.text
    paths = response.json()["paths"]
    actual = {(method, path) for path, operations in paths.items() for method in operations}
    expected = set().union(*(operations for section, operations in OPENAPI_SECTION_OPERATIONS.items()
                             if section in sections)) - hidden
    assert actual == expected
    assert all(operations for operations in paths.values())
    # Root/debug stay undocumented even when their section names are requested.
    assert "/" not in paths
    assert "/debug/jobs/{job_id}/start" not in paths

    for method, url, headers, code in [
        ("get", "/v1/me", user_headers, AppCode.API_KEY_VALID),
        ("post", "/v1/jobs/lease", worker_headers, AppCode.JOB_QUEUE_EMPTY),
        ("get", "/v1/admin/keys", admin_headers, AppCode.KEYS_RETRIEVED),
    ]:
        result = await client.request(method, url, headers=headers)
        assert result.status_code == 200, result.text
        assert result.json()["code"] == code.value
    job_id = await job_factory(ProcessingState.NEW)
    result = await client.put(f"/debug/jobs/{job_id}/start", headers=admin_headers)
    assert result.status_code == 200, result.text
    assert result.json()["code"] == AppCode.JOB_STARTED.value


@pytest.mark.parametrize("disabled", [None, "all", *[flag for flag, _, _ in OPENAPI_VISIBILITY_ROUTES]],
                         ids=lambda disabled: disabled or "defaults")
def test_openapi_visibility_flags_from_environment(tmp_path, disabled):
    # Route decorators capture config values during import. Avoid reloading
    # shared application modules or starting a DB lifespan just to check this.
    env = os.environ.copy()
    for flag, _, _ in OPENAPI_VISIBILITY_ROUTES:
        env.pop(flag, None)
        if disabled == "all" or disabled == flag:
            env[flag] = "false"
    env.pop("SHOW_SECTIONS", None)
    if disabled == "all":
        env["SHOW_SECTIONS"] = ""
    elif disabled is not None:
        env["SHOW_SECTIONS"] = " Worker , Admin "
    for name in ("JOBS", "RESULTS", "ARTIFACTS", "ENGINES", "LOGGING"):
        env[f"{name}_DIR"] = str(tmp_path / name.lower())
    env["BASE_DIR"] = str(tmp_path)
    env["DATABASE_URL"] = "postgresql+asyncpg://unused:unused@127.0.0.1:1/unused"
    env.pop("INTERNAL_MAIL_SERVER", None)
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(Path(__file__).resolve().parents[2]), env.get("PYTHONPATH")]))
    script = """
import sys
from doc_api.api.main import app
from doc_api.config import config
from doc_api.tests.helpers import OPENAPI_SECTION_OPERATIONS, OPENAPI_VISIBILITY_ROUTES
disabled = sys.argv[1]
sections = {"User", "Worker", "Admin"} if not disabled else (set() if disabled == "all" else {"Worker", "Admin"})
assert config.SHOW_SECTIONS - {""} == sections
hidden = set()
for flag, method, path in OPENAPI_VISIBILITY_ROUTES:
    expected = disabled not in {"all", flag}
    route = next(route for route in app.routes if getattr(route, "path", None) == path and method.upper() in route.methods)
    assert getattr(config, flag) == expected, flag
    assert route.include_in_schema == expected, flag
    if not expected:
        hidden.add((method, path))
expected = set().union(*(operations for section, operations in OPENAPI_SECTION_OPERATIONS.items() if section in sections)) - hidden
actual = {(method, path) for path, operations in app.openapi()["paths"].items() for method in operations}
assert actual == expected, (actual, expected)
"""
    result = subprocess.run([sys.executable, "-c", script, disabled or ""], cwd=tmp_path,
                            env=env, capture_output=True, text=True, timeout=45)
    assert result.returncode == 0, result.stdout + result.stderr


#
# GET /docs, /docs/oauth2-redirect, /redoc, /openapi.json - 200
#

@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/docs", "/docs/oauth2-redirect", "/redoc", "/openapi.json"])
async def test_get_documentation_200_public_pages(client, path):
    response = await client.get(path)
    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith("application/json" if path == "/openapi.json" else "text/html")


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/docs", "/docs/oauth2-redirect", "/redoc", "/openapi.json"])
async def test_head_documentation_200_public_pages(client, path):
    response = await client.head(path)
    assert response.status_code == 200, response.text
    assert not response.content


@pytest.mark.parametrize("reader_role", ["user", "readonly", "admin"])
@pytest.mark.parametrize("latest", [None, "current public"])
@pytest.mark.asyncio
async def test_get_job_200_attempt_logs(client, reader_headers, reader_role, job_factory, db_sessions, latest):
    job_id = await job_factory(ProcessingState.DONE)
    async with db_sessions.begin() as db:
        job = await db.get(model.Job, job_id)
        job.previous_attempts = 1
        job.log = ["old technical", "current technical"]
        job.log_user = ["old public", latest]
    detail = await client.get(f"/v1/jobs/{job_id}", headers=reader_headers)
    listing = await client.get("/v1/jobs", headers=reader_headers)
    assert detail.status_code == listing.status_code == 200
    listed = next(item for item in listing.json()["data"] if item["id"] == str(job_id))
    expected = "old public" + ("\n\n" + latest if latest is not None else "") if reader_role == "admin" else latest
    for job in [detail.json()["data"], listed]:
        assert job.get("log_user") == expected
        if reader_role == "admin":
            assert job["log"] == "old technical\n\ncurrent technical"
        else:
            assert "log" not in job
