from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from doc_api.api.checkers import cleanup
from doc_api.api.schemas.base_objects import ProcessingState
from doc_api.config import Config, config
from doc_api.db import model


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
async def test_output_retention(db_sessions, output_storage, state, age, expired, output_factory):
    finished = None if age is None else output_storage - timedelta(seconds=age)
    job_id, outputs = await output_factory(state, finished)
    inputs = Path(config.JOBS_DIR) / str(job_id)
    inputs.mkdir()
    (inputs / "image.jpg").write_bytes(b"input")
    engine = Path(config.ENGINES_DIR) / f"{job_id}.zip"
    engine.write_bytes(b"engine")
    async with db_sessions() as db:
        before = model.orm2dict(await db.get(model.Job, job_id))
    async with db_sessions() as db:
        await cleanup.delete_expired_job_outputs(db)
        await cleanup.delete_expired_job_outputs(db)
    assert all(path.exists() is (not expired) for path in outputs.values())
    assert (inputs / "image.jpg").read_bytes() == b"input"
    assert engine.read_bytes() == b"engine"
    async with db_sessions() as db:
        if expired:
            before["results_deleted_at"] = output_storage
            before["artifacts_deleted_at"] = output_storage
        assert model.orm2dict(await db.get(model.Job, job_id)) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("targets", [{"results"}, {"artifacts"}, {"results", "artifacts"}])
async def test_configured_output_targets(db_sessions, output_storage, monkeypatch, targets, output_factory):
    monkeypatch.setattr(config, "OUTPUT_CLEANUP_TARGETS", targets)
    _, outputs = await output_factory(ProcessingState.DONE, output_storage - timedelta(days=8))
    async with db_sessions() as db:
        await cleanup.delete_expired_job_outputs(db)
    assert all(path.exists() is (target not in targets) for target, path in outputs.items())


@pytest.mark.asyncio
async def test_output_cleanup_disabled(db_sessions, output_storage, monkeypatch, output_factory):
    monkeypatch.setattr(config, "DELETE_JOB_OUTPUTS", False)
    _, outputs = await output_factory(ProcessingState.DONE, output_storage - timedelta(days=8))
    async with db_sessions() as db:
        await cleanup.delete_expired_job_outputs(db)
    assert all(path.exists() for path in outputs.values())


@pytest.mark.asyncio
async def test_output_cleanup_retries_errors(db_sessions, output_storage, monkeypatch, output_factory):
    _, outputs = await output_factory(ProcessingState.DONE, output_storage - timedelta(days=8))
    remove = cleanup.os.remove

    def fail_artifacts(path):
        if str(path) == str(outputs["artifacts"]):
            raise PermissionError("Test deletion failure")
        remove(path)

    monkeypatch.setattr(cleanup.os, "remove", fail_artifacts)
    async with db_sessions() as db:
        await cleanup.delete_expired_job_outputs(db)
    assert outputs["artifacts"].exists()
    assert not outputs["results"].exists()
    monkeypatch.setattr(cleanup.os, "remove", remove)
    async with db_sessions() as db:
        await cleanup.delete_expired_job_outputs(db)
    assert not outputs["artifacts"].exists()


@pytest.mark.asyncio
async def test_output_cleanup_preserves_unknown_files_and_symlinks(db_sessions, output_storage, job_factory):
    job_id = await job_factory(ProcessingState.DONE)
    async with db_sessions.begin() as db:
        (await db.get(model.Job, job_id)).finished_date = output_storage - timedelta(days=8)
    outside = Path(config.ENGINES_DIR) / "outside.zip"
    outside.write_bytes(b"retain")
    symlink = Path(config.RESULTS_DIR) / f"{job_id}.zip"
    symlink.symlink_to(outside)
    unknown = Path(config.RESULTS_DIR) / f"{uuid4()}.zip"
    unknown.write_bytes(b"retain")
    unrelated = Path(config.RESULTS_DIR) / "unrelated.zip"
    unrelated.write_bytes(b"retain")
    temporary = Path(config.RESULTS_DIR) / f"{job_id}.zip.docapi-tmp-upload-staging-{uuid4().hex}.zip"
    temporary.write_bytes(b"retain")
    async with db_sessions() as db:
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
    ("OUTPUT_RETENTION_SECONDS", "-1"),
    ("OUTPUT_RETENTION_SECONDS", "bad"), ("OUTPUT_CLEANUP_TARGETS", ""),
    ("OUTPUT_CLEANUP_TARGETS", "results,engines"),
])
def test_invalid_output_configuration(monkeypatch, setting, value):
    monkeypatch.setenv(setting, value)
    with pytest.raises(ValueError):
        Config()


def test_zero_output_retention_configuration(monkeypatch):
    monkeypatch.setenv("OUTPUT_RETENTION_SECONDS", "0")
    assert Config().OUTPUT_RETENTION_SECONDS == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("state,future_finish", [
    *[(state, False) for state in ProcessingState],
    (ProcessingState.DONE, True), (ProcessingState.FAILED, True),
])
async def test_immediate_output_cleanup_skips_time_check(db_sessions, output_storage, monkeypatch, state, future_finish, output_factory):
    monkeypatch.setattr(config, "OUTPUT_RETENTION_SECONDS", 0)
    finished = output_storage + timedelta(hours=1) if future_finish else None
    job_id, outputs = await output_factory(state, finished)
    async with db_sessions() as db:
        await cleanup.delete_expired_job_outputs(db)
    deleted = state in {ProcessingState.DONE, ProcessingState.FAILED}
    assert all(path.exists() is (not deleted) for path in outputs.values())
    async with db_sessions() as db:
        job = await db.get(model.Job, job_id)
        assert job.results_deleted_at == (output_storage if deleted else None)
        assert job.artifacts_deleted_at == (output_storage if deleted else None)
