from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from doc_api.api import cleanup
from doc_api.api.main import app
from doc_api.api.schemas.base_objects import ProcessingState
from doc_api.api.schemas.responses import AppCode
from doc_api.config import Config, config
from doc_api.db import model
from doc_api.tests.test_worker_failure import failure_api, create_job


@pytest.fixture
def output_storage(monkeypatch, tmp_path):
    for setting in ["JOBS_DIR", "RESULTS_DIR", "ARTIFACTS_DIR", "ENGINES_DIR"]:
        path = tmp_path / setting
        path.mkdir()
        monkeypatch.setattr(config, setting, str(path))
    monkeypatch.setattr(config, "DELETE_JOB_INPUTS_ON_COMPLETION", False)
    monkeypatch.setattr(config, "DELETE_JOB_OUTPUTS", True)
    monkeypatch.setattr(config, "OUTPUT_RETENTION_SECONDS", 604800)
    monkeypatch.setattr(config, "OUTPUT_CLEANUP_TARGETS", {"results", "artifacts"})
    now = datetime(2026, 11, 2, tzinfo=timezone.utc)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    monkeypatch.setattr(cleanup, "datetime", FixedDatetime)
    return now


async def outputs_for_job(api, state, finished):
    job_id = await create_job(api, state)
    async with api.sessions.begin() as db:
        job = await db.get(model.Job, job_id)
        job.finished_date = finished
    outputs = {}
    for target, directory in [("results", config.RESULTS_DIR), ("artifacts", config.ARTIFACTS_DIR)]:
        path = Path(directory) / f"{job_id}.zip"
        path.write_bytes(b"output")
        outputs[target] = path
    return job_id, outputs


@pytest.mark.asyncio
@pytest.mark.parametrize("state,age,expired", [
    (ProcessingState.DONE, 604801, True),
    (ProcessingState.FAILED, 604801, True),
    (ProcessingState.DONE, 604800, True),
    (ProcessingState.DONE, 604799, False),
    (ProcessingState.DONE, 0, False),
    (ProcessingState.DONE, None, False),
    (ProcessingState.FAILED, None, False),
    *[(state, 604801, False) for state in [ProcessingState.NEW, ProcessingState.QUEUED,
                                         ProcessingState.PROCESSING, ProcessingState.ERROR,
                                         ProcessingState.CANCELLED]],
])
async def test_output_retention(failure_api, output_storage, state, age, expired):
    api = failure_api
    finished = None if age is None else output_storage - timedelta(seconds=age)
    job_id, outputs = await outputs_for_job(api, state, finished)
    inputs = Path(config.JOBS_DIR) / str(job_id)
    inputs.mkdir()
    (inputs / "image.jpg").write_bytes(b"input")
    engine = Path(config.ENGINES_DIR) / f"{job_id}.zip"
    engine.write_bytes(b"engine")
    async with api.sessions() as db:
        before = model.orm2dict(await db.get(model.Job, job_id))
    async with api.sessions() as db:
        await cleanup.delete_expired_job_outputs(db)
        await cleanup.delete_expired_job_outputs(db)
    assert all(path.exists() is (not expired) for path in outputs.values())
    assert (inputs / "image.jpg").read_bytes() == b"input"
    assert engine.read_bytes() == b"engine"
    async with api.sessions() as db:
        if expired:
            before["results_deleted_at"] = output_storage
            before["artifacts_deleted_at"] = output_storage
        assert model.orm2dict(await db.get(model.Job, job_id)) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("targets", [{"results"}, {"artifacts"}, {"results", "artifacts"}])
async def test_configured_output_targets(failure_api, output_storage, monkeypatch, targets):
    monkeypatch.setattr(config, "OUTPUT_CLEANUP_TARGETS", targets)
    _, outputs = await outputs_for_job(failure_api, ProcessingState.DONE, output_storage - timedelta(days=8))
    async with failure_api.sessions() as db:
        await cleanup.delete_expired_job_outputs(db)
    assert all(path.exists() is (target not in targets) for target, path in outputs.items())


@pytest.mark.asyncio
async def test_output_cleanup_disabled(failure_api, output_storage, monkeypatch):
    monkeypatch.setattr(config, "DELETE_JOB_OUTPUTS", False)
    _, outputs = await outputs_for_job(failure_api, ProcessingState.DONE, output_storage - timedelta(days=8))
    async with failure_api.sessions() as db:
        await cleanup.delete_expired_job_outputs(db)
    assert all(path.exists() for path in outputs.values())


@pytest.mark.asyncio
async def test_deleted_result_returns_gone(failure_api, output_storage):
    api = failure_api
    job_id, _ = await outputs_for_job(api, ProcessingState.DONE, output_storage - timedelta(days=8))
    route = next(r for r in app.routes if getattr(r, "path", None) == "/v1/jobs/{job_id}/result"
                 and "GET" in r.methods)
    auth = next(d.call for d in route.dependant.dependencies if d.name == "key")

    async def current_key():
        return api.owner

    app.dependency_overrides[auth] = current_key
    response = await api.client.get(f"/v1/jobs/{job_id}/result")
    assert response.status_code == 200
    assert response.content == b"output"
    async with api.sessions() as db:
        await cleanup.delete_expired_job_outputs(db)
    response = await api.client.get(f"/v1/jobs/{job_id}/result")
    assert response.status_code == 410, response.text
    assert response.json()["code"] == AppCode.JOB_RESULT_GONE.value


@pytest.mark.asyncio
async def test_output_cleanup_retries_errors(failure_api, output_storage, monkeypatch):
    _, outputs = await outputs_for_job(failure_api, ProcessingState.DONE, output_storage - timedelta(days=8))
    remove = cleanup.os.remove

    def fail_artifacts(path):
        if str(path) == str(outputs["artifacts"]):
            raise PermissionError("Test deletion failure")
        remove(path)

    monkeypatch.setattr(cleanup.os, "remove", fail_artifacts)
    async with failure_api.sessions() as db:
        await cleanup.delete_expired_job_outputs(db)
    assert outputs["artifacts"].exists()
    assert not outputs["results"].exists()
    monkeypatch.setattr(cleanup.os, "remove", remove)
    async with failure_api.sessions() as db:
        await cleanup.delete_expired_job_outputs(db)
    assert not outputs["artifacts"].exists()


@pytest.mark.asyncio
async def test_output_cleanup_preserves_unknown_files_and_symlinks(failure_api, output_storage):
    job_id = await create_job(failure_api, ProcessingState.DONE)
    async with failure_api.sessions.begin() as db:
        (await db.get(model.Job, job_id)).finished_date = output_storage - timedelta(days=8)
    outside = Path(config.ENGINES_DIR) / "outside.zip"
    outside.write_bytes(b"retain")
    symlink = Path(config.RESULTS_DIR) / f"{job_id}.zip"
    symlink.symlink_to(outside)
    unknown = Path(config.RESULTS_DIR) / f"{uuid4()}.zip"
    unknown.write_bytes(b"retain")
    unrelated = Path(config.RESULTS_DIR) / "unrelated.zip"
    unrelated.write_bytes(b"retain")
    temporary = Path(config.RESULTS_DIR) / f"{job_id}.zip.validating"
    temporary.write_bytes(b"retain")
    async with failure_api.sessions() as db:
        await cleanup.delete_expired_job_outputs(db)
    assert symlink.is_symlink()
    assert all(path.read_bytes() == b"retain" for path in [outside, unknown, unrelated, temporary])


def test_output_cleanup_defaults(monkeypatch):
    for setting in ["DELETE_JOB_OUTPUTS", "OUTPUT_RETENTION_SECONDS", "OUTPUT_CLEANUP_TARGETS"]:
        monkeypatch.delenv(setting, raising=False)
    settings = Config()
    assert settings.DELETE_JOB_OUTPUTS is False
    assert settings.OUTPUT_RETENTION_SECONDS == 604800
    assert settings.OUTPUT_CLEANUP_TARGETS == {"results", "artifacts"}


@pytest.mark.parametrize("value,expected", [("results", {"results"}), (" Artifacts , RESULTS ", {"artifacts", "results"})])
def test_output_target_configuration(monkeypatch, value, expected):
    monkeypatch.setenv("OUTPUT_CLEANUP_TARGETS", value)
    assert Config().OUTPUT_CLEANUP_TARGETS == expected


@pytest.mark.parametrize("setting,value", [
    ("OUTPUT_RETENTION_SECONDS", "0"), ("OUTPUT_RETENTION_SECONDS", "-1"),
    ("OUTPUT_RETENTION_SECONDS", "bad"), ("OUTPUT_CLEANUP_TARGETS", ""),
    ("OUTPUT_CLEANUP_TARGETS", "results,engines"),
])
def test_invalid_output_configuration(monkeypatch, setting, value):
    monkeypatch.setenv(setting, value)
    with pytest.raises(ValueError):
        Config()
