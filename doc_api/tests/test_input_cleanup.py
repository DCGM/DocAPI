import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from doc_api.api import cleanup
from doc_api.api.schemas.base_objects import ProcessingState
from doc_api.config import Config, config
from doc_api.db import model
from sqlalchemy import select, event


@pytest.fixture
def input_storage(monkeypatch, tmp_path):
    for setting in ["JOBS_DIR", "RESULTS_DIR", "ARTIFACTS_DIR", "ENGINES_DIR"]:
        path = tmp_path / setting
        path.mkdir()
        monkeypatch.setattr(config, setting, str(path))
    monkeypatch.setattr(config, "DELETE_JOB_INPUTS_ON_COMPLETION", True)
    monkeypatch.setattr(config, "INPUT_RETENTION_SECONDS", 0)
    monkeypatch.setattr(config, "DELETE_JOB_IMAGE_RECORDS_WITH_INPUTS", False)
    return tmp_path


def write_inputs(job_id):
    path = Path(config.JOBS_DIR) / str(job_id)
    path.mkdir()
    for name in ["image.jpg", "image.alto.xml", "image.page.xml", "meta.json"]:
        (path / name).write_text("test input")
    return path


@pytest.mark.asyncio
@pytest.mark.parametrize("state", list(ProcessingState))
async def test_cleanup_only_terminal_inputs(db_sessions, input_storage, state, job_factory):
    job_id = await job_factory(state)
    async with db_sessions.begin() as db:
        image = model.Image(id=uuid4(), job_id=job_id, name="page.jpg", order=0,
                            image_uploaded=True, alto_uploaded=True, page_uploaded=True)
        db.add(image)
    path = write_inputs(job_id)
    retained_files = []
    for setting in ["RESULTS_DIR", "ARTIFACTS_DIR", "ENGINES_DIR"]:
        retained = Path(getattr(config, setting)) / f"{job_id}.zip"
        retained.write_bytes(b"retain")
        retained_files.append(retained)
    async with db_sessions() as db:
        before_job = model.orm2dict(await db.get(model.Job, job_id))
        before_image = model.orm2dict(await db.get(model.Image, image.id))
    async with db_sessions() as db:
        await cleanup.delete_completed_job_inputs(db)
        # A repeat pass must work after the input directory has disappeared.
        await cleanup.delete_completed_job_inputs(db)
    assert path.exists() is (state not in {ProcessingState.DONE, ProcessingState.FAILED})
    assert all(file.read_bytes() == b"retain" for file in retained_files)
    async with db_sessions() as db:
        after_job = model.orm2dict(await db.get(model.Job, job_id))
        if state in {ProcessingState.DONE, ProcessingState.FAILED}:
            assert after_job["inputs_deleted_at"] is not None
            before_job["inputs_deleted_at"] = after_job["inputs_deleted_at"]
        assert after_job == before_job
        assert model.orm2dict(await db.get(model.Image, image.id)) == before_image


@pytest.mark.asyncio
async def test_disabled_cleanup_keeps_inputs(db_sessions, input_storage, monkeypatch, job_factory):
    monkeypatch.setattr(config, "DELETE_JOB_INPUTS_ON_COMPLETION", False)
    job_id = await job_factory(ProcessingState.DONE)
    path = write_inputs(job_id)
    async with db_sessions() as db:
        await cleanup.delete_completed_job_inputs(db)
    assert len(list(path.iterdir())) == 4


@pytest.mark.asyncio
async def test_cleanup_preserves_unknown_directories_and_symlink_targets(db_sessions, input_storage, job_factory):
    unknown = write_inputs(uuid4())
    unrelated = Path(config.JOBS_DIR) / "unrelated"
    unrelated.mkdir()
    target = input_storage / "outside-inputs"
    target.mkdir()
    (target / "keep").write_text("keep")
    job_id = await job_factory(ProcessingState.DONE)
    symlink = Path(config.JOBS_DIR) / str(job_id)
    symlink.symlink_to(target, target_is_directory=True)
    async with db_sessions() as db:
        await cleanup.delete_completed_job_inputs(db)
    assert unknown.is_dir()
    assert unrelated.is_dir()
    assert symlink.is_symlink()
    assert (target / "keep").read_text() == "keep"


@pytest.mark.asyncio
async def test_cleanup_retries_failed_deletion(db_sessions, input_storage, monkeypatch, job_factory):
    job_id = await job_factory(ProcessingState.FAILED)
    path = write_inputs(job_id)
    other_id = await job_factory(ProcessingState.DONE)
    other_path = write_inputs(other_id)
    rmtree = cleanup.shutil.rmtree

    def fail_one(path_to_remove):
        if str(path_to_remove) == str(path):
            raise PermissionError("Test deletion failure")
        rmtree(path_to_remove)

    monkeypatch.setattr(cleanup.shutil, "rmtree", fail_one)
    async with db_sessions() as db:
        await cleanup.delete_completed_job_inputs(db)
    assert path.exists()
    assert not other_path.exists()
    monkeypatch.setattr(cleanup.shutil, "rmtree", rmtree)
    async with db_sessions() as db:
        await cleanup.delete_completed_job_inputs(db)
    assert not path.exists()


def test_input_cleanup_disabled_by_default(monkeypatch):
    monkeypatch.delenv("DELETE_JOB_INPUTS_ON_COMPLETION", raising=False)
    assert Config().DELETE_JOB_INPUTS_ON_COMPLETION is False


def test_image_record_cleanup_disabled_by_default(monkeypatch):
    monkeypatch.delenv("DELETE_JOB_IMAGE_RECORDS_WITH_INPUTS", raising=False)
    assert Config().DELETE_JOB_IMAGE_RECORDS_WITH_INPUTS is False


@pytest.mark.parametrize("value,expected", [(None, 0), ("0", 0), ("3600", 3600)])
def test_input_retention_configuration(monkeypatch, value, expected):
    monkeypatch.delenv("INPUT_RETENTION_SECONDS", raising=False)
    if value is not None:
        monkeypatch.setenv("INPUT_RETENTION_SECONDS", value)
    assert Config().INPUT_RETENTION_SECONDS == expected


@pytest.mark.parametrize("value", ["-1", "bad", "0.5"])
def test_invalid_input_retention(monkeypatch, value):
    monkeypatch.setenv("INPUT_RETENTION_SECONDS", value)
    with pytest.raises(ValueError):
        Config()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", [ProcessingState.DONE, ProcessingState.FAILED])
@pytest.mark.parametrize("age,expired", [(599, False), (600, True), (601, True), (None, False)])
async def test_input_retention(db_sessions, input_storage, monkeypatch, state, age, expired, job_factory):
    now = datetime(2026, 11, 3, tzinfo=timezone.utc)
    monkeypatch.setattr(cleanup, "datetime", SimpleNamespace(now=lambda tz: now))
    monkeypatch.setattr(config, "INPUT_RETENTION_SECONDS", 600)
    monkeypatch.setattr(config, "DELETE_JOB_IMAGE_RECORDS_WITH_INPUTS", True)
    job_id = await job_factory(state)
    async with db_sessions.begin() as db:
        job = await db.get(model.Job, job_id)
        job.finished_date = None if age is None else now - timedelta(seconds=age)
        db.add(model.Image(job_id=job_id, name="page.jpg", order=0))
    path = write_inputs(job_id)
    async with db_sessions() as db:
        await cleanup.delete_completed_job_inputs(db)
    assert path.exists() is (not expired)
    async with db_sessions() as db:
        job = await db.get(model.Job, job_id)
        assert job.inputs_deleted_at == (now if expired else None)
        image_id = await db.scalar(select(model.Image.id).where(model.Image.job_id == job_id))
        assert (image_id is None) is expired


@pytest.mark.asyncio
@pytest.mark.parametrize("state", list(ProcessingState))
@pytest.mark.parametrize("inputs,records", [(False, False), (False, True), (True, False), (True, True)])
async def test_optional_image_record_cleanup(db_sessions, input_storage, monkeypatch, state, inputs, records, job_factory):
    monkeypatch.setattr(config, "DELETE_JOB_INPUTS_ON_COMPLETION", inputs)
    monkeypatch.setattr(config, "DELETE_JOB_IMAGE_RECORDS_WITH_INPUTS", records)
    job_id = await job_factory(state)
    other_id = await job_factory(ProcessingState.NEW)
    async with db_sessions.begin() as db:
        job = await db.get(model.Job, job_id)
        job.definition = {"images": [{"name": "page.jpg", "order": 0}]}
        job.image_count = 1
        db.add_all([model.Image(job_id=job_id, name="page.jpg", order=0),
                    model.Image(job_id=other_id, name="other.jpg", order=0)])
    path = write_inputs(job_id)
    async with db_sessions() as db:
        await cleanup.delete_completed_job_inputs(db)
    completed = inputs and state in {ProcessingState.DONE, ProcessingState.FAILED}
    async with db_sessions() as db:
        images = list(await db.scalars(select(model.Image).where(model.Image.job_id == job_id)))
        assert len(images) == (0 if completed and records else 1)
        assert await db.scalar(select(model.Image.id).where(model.Image.job_id == other_id)) is not None
        job = await db.get(model.Job, job_id)
        assert (job.inputs_deleted_at is not None) is completed
        assert job.definition == {"images": [{"name": "page.jpg", "order": 0}]}
        assert job.image_count == 1
    assert path.exists() is (not completed)


@pytest.mark.asyncio
async def test_image_records_retained_when_removal_fails(db_sessions, input_storage, monkeypatch, job_factory):
    monkeypatch.setattr(config, "DELETE_JOB_IMAGE_RECORDS_WITH_INPUTS", True)
    job_id = await job_factory(ProcessingState.DONE)
    async with db_sessions.begin() as db:
        db.add(model.Image(job_id=job_id, name="page.jpg", order=0))
    path = write_inputs(job_id)

    def fail_removal(*args):
        raise PermissionError("Test deletion failure")

    monkeypatch.setattr(cleanup.shutil, "rmtree", fail_removal)
    async with db_sessions() as db:
        await cleanup.delete_completed_job_inputs(db)
    assert path.exists()
    async with db_sessions() as db:
        assert await db.scalar(select(model.Image.id).where(model.Image.job_id == job_id)) is not None
        assert (await db.get(model.Job, job_id)).inputs_deleted_at is None


@pytest.mark.asyncio
async def test_image_record_cleanup_transaction_recovery(db_sessions, input_storage, monkeypatch, job_factory):
    monkeypatch.setattr(config, "DELETE_JOB_IMAGE_RECORDS_WITH_INPUTS", True)
    job_id = await job_factory(ProcessingState.FAILED)
    async with db_sessions.begin() as db:
        db.add(model.Image(job_id=job_id, name="page.jpg", order=0))
    path = write_inputs(job_id)

    def reject_commit(session):
        raise RuntimeError("Test commit failure")

    async with db_sessions() as db:
        event.listen(db.sync_session, "before_commit", reject_commit, once=True)
        with pytest.raises(RuntimeError, match="Test commit failure"):
            await cleanup.delete_completed_job_inputs(db)
    assert not path.exists()
    async with db_sessions() as db:
        assert await db.scalar(select(model.Image.id).where(model.Image.job_id == job_id)) is not None
        assert (await db.get(model.Job, job_id)).inputs_deleted_at is None
    async with db_sessions() as db:
        await cleanup.delete_completed_job_inputs(db)
    async with db_sessions() as db:
        assert await db.scalar(select(model.Image.id).where(model.Image.job_id == job_id)) is None
        assert (await db.get(model.Job, job_id)).inputs_deleted_at is not None


@pytest.mark.parametrize("value,expected", [(None, 60), ("1", 1), ("300", 300)])
def test_cleanup_interval(monkeypatch, value, expected):
    monkeypatch.delenv("CLEANUP_INTERVAL_SECONDS", raising=False)
    if value is not None:
        monkeypatch.setenv("CLEANUP_INTERVAL_SECONDS", value)
    assert Config().CLEANUP_INTERVAL_SECONDS == expected


@pytest.mark.parametrize("value", ["0", "-1", "invalid", "1.5"])
def test_invalid_cleanup_interval(monkeypatch, value):
    monkeypatch.setenv("CLEANUP_INTERVAL_SECONDS", value)
    with pytest.raises(ValueError):
        Config()


@pytest.mark.asyncio
async def test_cleanup_skips_locked_and_requeued_jobs(db_sessions, input_storage, job_factory):
    job_id = await job_factory(ProcessingState.DONE)
    path = write_inputs(job_id)
    async with db_sessions.begin() as admin:
        job = await admin.scalar(select(model.Job).where(model.Job.id == job_id).with_for_update())
        async with db_sessions() as db:
            await asyncio.wait_for(cleanup.delete_completed_job_inputs(db), timeout=5)
        assert path.exists()
        job.state = ProcessingState.QUEUED
    async with db_sessions() as db:
        await cleanup.delete_completed_job_inputs(db)
    assert path.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("delete_records", [False, True])
async def test_cleanup_linked_inputs(client, user_headers, db_sessions, input_storage, monkeypatch, delete_records):
    monkeypatch.setattr(config, "DELETE_JOB_IMAGE_RECORDS_WITH_INPUTS", delete_records)
    url = "https://files.invalid/linked.jpg"
    response = await client.post("/v1/jobs", headers=user_headers,
                                  json={"images": [{"name": "page.jpg", "order": 0, "image_url": url}]})
    assert response.status_code == 201, response.text
    job_id = UUID(response.json()["data"]["id"])
    async with db_sessions.begin() as db:
        (await db.get(model.Job, job_id)).state = ProcessingState.DONE
    async with db_sessions() as db:
        await cleanup.delete_completed_job_inputs(db)
    async with db_sessions() as db:
        job = await db.get(model.Job, job_id)
        assert job.inputs_deleted_at is not None
        assert job.image_count == 1
        assert job.definition["images"][0]["image_url"] == url
        images = list(await db.scalars(select(model.Image).where(model.Image.job_id == job_id)))
        assert len(images) == (0 if delete_records else 1)
        if images:
            assert images[0].image_url_provided and not images[0].image_uploaded
