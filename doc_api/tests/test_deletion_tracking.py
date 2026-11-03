import importlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import event, inspect

from doc_api.api import cleanup
from doc_api.api.main import app
from doc_api.api.schemas.base_objects import KeyRole, ProcessingState
from doc_api.config import config
from doc_api.db import model
from doc_api.tests.test_worker_failure import failure_api, create_job
from doc_api.tests.test_output_cleanup import output_storage, outputs_for_job
from doc_api.tests.dummy_data import VALID_ZIP, VALID_ALTO_XML, VALID_PAGE_XML, make_white_image_bytes


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["inputs", "results"])
async def test_recovers_after_files_removed_but_commit_fails(failure_api, output_storage, monkeypatch, target):
    api = failure_api
    job_id, outputs = await outputs_for_job(api, ProcessingState.DONE, output_storage - timedelta(days=8))
    monkeypatch.setattr(config, "DELETE_JOB_INPUTS_ON_COMPLETION", True)
    inputs = Path(config.JOBS_DIR) / str(job_id)
    inputs.mkdir()
    (inputs / "image.jpg").write_bytes(b"input")
    clean = cleanup.delete_completed_job_inputs if target == "inputs" else cleanup.delete_expired_job_outputs
    path = inputs if target == "inputs" else outputs["results"]
    monkeypatch.setattr(config, "OUTPUT_CLEANUP_TARGETS", {"results"})

    def reject_commit(session):
        raise RuntimeError("Test commit failure")

    async with api.sessions() as db:
        event.listen(db.sync_session, "before_commit", reject_commit, once=True)
        with pytest.raises(RuntimeError, match="Test commit failure"):
            await clean(db)
    assert not path.exists()
    async with api.sessions() as db:
        assert getattr(await db.get(model.Job, job_id), f"{target}_deleted_at") is None
    async with api.sessions() as db:
        await clean(db)
    async with api.sessions() as db:
        recorded = getattr(await db.get(model.Job, job_id), f"{target}_deleted_at")
        assert recorded == output_storage
    async with api.sessions() as db:
        await clean(db)
    async with api.sessions() as db:
        assert getattr(await db.get(model.Job, job_id), f"{target}_deleted_at") == recorded


def authorize_job_reads(api):
    async def current_key():
        return api.identity.key

    for path in ["/v1/jobs", "/v1/jobs/{job_id}"]:
        route = next(r for r in app.routes if getattr(r, "path", None) == path and "GET" in r.methods)
        auth = next(d.call for d in route.dependant.dependencies if d.name == "key")
        app.dependency_overrides[auth] = current_key


@pytest.mark.asyncio
async def test_job_availability_and_deletion_visibility(failure_api, output_storage, monkeypatch):
    api = failure_api
    authorize_job_reads(api)
    api.identity.key = api.owner
    old = output_storage - timedelta(days=8)
    deleted_id, _ = await outputs_for_job(api, ProcessingState.DONE, old)
    active_id, _ = await outputs_for_job(api, ProcessingState.DONE, output_storage)
    processing_id, _ = await outputs_for_job(api, ProcessingState.PROCESSING, old)
    missing_id = await create_job(api, ProcessingState.DONE)
    other_id, _ = await outputs_for_job(api, ProcessingState.DONE, output_storage)
    async with api.sessions.begin() as db:
        (await db.get(model.Job, other_id)).owner_key_id = api.other_worker.id
    monkeypatch.setattr(config, "DELETE_JOB_INPUTS_ON_COMPLETION", True)
    async with api.sessions() as db:
        await cleanup.delete_completed_job_inputs(db)
        await cleanup.delete_expired_job_outputs(db)

    response = await api.client.get("/v1/jobs", params={"result_available": True})
    assert response.status_code == 200, response.text
    assert {job["id"] for job in response.json()["data"]} == {str(active_id)}
    response = await api.client.get("/v1/jobs", params={"result_available": False})
    assert response.status_code == 200, response.text
    jobs = {job["id"]: job for job in response.json()["data"]}
    assert set(jobs) == {str(deleted_id), str(processing_id), str(missing_id)}
    deleted = jobs[str(deleted_id)]
    assert deleted["state"] == "done"
    assert deleted["results_deleted_at"] is not None
    assert "inputs_deleted_at" not in deleted
    assert "artifacts_deleted_at" not in deleted
    # Missing legacy files are unavailable even without a result-deletion timestamp.
    assert "results_deleted_at" not in jobs[str(missing_id)]

    response = await api.client.get(f"/v1/jobs/{deleted_id}")
    assert response.status_code == 200, response.text
    job = response.json()["data"]
    assert job["result_available"] is False
    assert job["results_deleted_at"] is not None
    assert "inputs_deleted_at" not in job
    assert "artifacts_deleted_at" not in job

    api.identity.key = SimpleNamespace(id=api.owner.id, role=KeyRole.ADMIN)
    for path in ["/v1/jobs", f"/v1/jobs/{deleted_id}"]:
        response = await api.client.get(path)
        assert response.status_code == 200, response.text
        data = response.json()["data"]
        job = next(item for item in data if item["id"] == str(deleted_id)) if isinstance(data, list) else data
        assert all(job[name] is not None for name in ["inputs_deleted_at", "results_deleted_at", "artifacts_deleted_at"])


@pytest.mark.asyncio
async def test_deletion_timestamp_migration_preserves_jobs(failure_api):
    api = failure_api
    job_id = await create_job(api, ProcessingState.DONE)
    migration = importlib.import_module("doc_api.alembic.versions.f1c824b73a90_add_job_deletion_timestamps")
    columns = {"inputs_deleted_at", "results_deleted_at", "artifacts_deleted_at"}
    engine = api.sessions.kw["bind"]

    def migrate(connection):
        with Operations.context(MigrationContext.configure(connection)):
            migration.downgrade()
            assert not columns.intersection(c["name"] for c in inspect(connection).get_columns("jobs"))
            migration.upgrade()
        info = {c["name"]: c for c in inspect(connection).get_columns("jobs")}
        assert all(info[name]["nullable"] and info[name]["type"].timezone for name in columns)

    async with engine.begin() as connection:
        await connection.run_sync(migrate)
    async with api.sessions() as db:
        job = await db.get(model.Job, job_id)
        assert job.state == ProcessingState.DONE
        assert all(getattr(job, name) is None for name in columns)


@pytest.mark.asyncio
@pytest.mark.parametrize("target,kind", [("inputs", "image"), ("inputs", "alto"), ("inputs", "page"),
                                         ("inputs", "metadata"), ("results", "zip"), ("artifacts", "zip")])
async def test_fresh_upload_preserves_deletion_timestamp(failure_api, output_storage, target, kind):
    api = failure_api
    is_input = target == "inputs"
    state = ProcessingState.NEW if is_input else ProcessingState.PROCESSING
    job_id = await create_job(api, state)
    before = output_storage - timedelta(days=8)
    async with api.sessions.begin() as db:
        job = await db.get(model.Job, job_id)
        for name in ["inputs_deleted_at", "results_deleted_at", "artifacts_deleted_at"]:
            setattr(job, name, before)
        if is_input:
            job.meta_json_required = True
            job.alto_required = True
            job.page_required = True
            db.add(model.Image(id=uuid4(), job_id=job_id, name="page.jpg", order=0))

    async def current_key():
        return api.owner if is_input else api.worker

    if kind == "metadata":
        template = "/v1/jobs/{job_id}/files/metadata"
    elif is_input:
        template = "/v1/jobs/{job_id}/images/{image_name}/files/" + kind
    else:
        template = "/v1/jobs/{job_id}/" + ("result" if target == "results" else "artifacts")
    method = "PUT" if is_input else "POST"
    route = next(r for r in app.routes if getattr(r, "path", None) == template and method in r.methods)
    auth = next(d.call for d in route.dependant.dependencies if d.name == "key")
    app.dependency_overrides[auth] = current_key
    url = template.format(job_id=job_id, image_name="page.jpg")
    if kind == "metadata":
        response = await api.client.put(url, json={})
    else:
        files = {"image": make_white_image_bytes()[0], "alto": VALID_ALTO_XML,
                 "page": VALID_PAGE_XML, "zip": VALID_ZIP}
        response = await api.client.request(method, url, files={"file": ("upload.zip" if kind == "zip" else "upload", files[kind])})
    assert response.status_code in {200, 201}, response.text
    async with api.sessions() as db:
        job = await db.get(model.Job, job_id)
        for prefix in ["inputs", "results", "artifacts"]:
            assert getattr(job, f"{prefix}_deleted_at") == before
