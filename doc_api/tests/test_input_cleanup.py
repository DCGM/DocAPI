import asyncio
from pathlib import Path
from uuid import uuid4

import pytest

from doc_api.api import cleanup
from doc_api.api import main
from doc_api.api.schemas.base_objects import ProcessingState
from doc_api.config import Config, config
from doc_api.db import model
from doc_api.tests.test_worker_failure import failure_api, create_job
from sqlalchemy import select


@pytest.fixture
def input_storage(monkeypatch, tmp_path):
    for setting in ["JOBS_DIR", "RESULTS_DIR", "ARTIFACTS_DIR", "ENGINES_DIR"]:
        path = tmp_path / setting
        path.mkdir()
        monkeypatch.setattr(config, setting, str(path))
    monkeypatch.setattr(config, "DELETE_JOB_INPUTS_ON_COMPLETION", True)
    return tmp_path


def write_inputs(job_id):
    path = Path(config.JOBS_DIR) / str(job_id)
    path.mkdir()
    for name in ["image.jpg", "image.alto.xml", "image.page.xml", "meta.json"]:
        (path / name).write_text("test input")
    return path


@pytest.mark.asyncio
@pytest.mark.parametrize("state", list(ProcessingState))
async def test_cleanup_only_terminal_inputs(failure_api, input_storage, state):
    api = failure_api
    job_id = await create_job(api, state)
    async with api.sessions.begin() as db:
        image = model.Image(id=uuid4(), job_id=job_id, name="page.jpg", order=0,
                            image_uploaded=True, alto_uploaded=True, page_uploaded=True)
        db.add(image)
    path = write_inputs(job_id)
    retained_files = []
    for setting in ["RESULTS_DIR", "ARTIFACTS_DIR", "ENGINES_DIR"]:
        retained = Path(getattr(config, setting)) / f"{job_id}.zip"
        retained.write_bytes(b"retain")
        retained_files.append(retained)
    async with api.sessions() as db:
        before_job = model.orm2dict(await db.get(model.Job, job_id))
        before_image = model.orm2dict(await db.get(model.Image, image.id))
    async with api.sessions() as db:
        await cleanup.delete_completed_job_inputs(db)
        # A repeat pass must work after the input directory has disappeared.
        await cleanup.delete_completed_job_inputs(db)
    assert path.exists() is (state not in {ProcessingState.DONE, ProcessingState.FAILED})
    assert all(file.read_bytes() == b"retain" for file in retained_files)
    async with api.sessions() as db:
        assert model.orm2dict(await db.get(model.Job, job_id)) == before_job
        assert model.orm2dict(await db.get(model.Image, image.id)) == before_image


@pytest.mark.asyncio
async def test_disabled_cleanup_keeps_inputs(failure_api, input_storage, monkeypatch):
    monkeypatch.setattr(config, "DELETE_JOB_INPUTS_ON_COMPLETION", False)
    job_id = await create_job(failure_api, ProcessingState.DONE)
    path = write_inputs(job_id)
    async with failure_api.sessions() as db:
        await cleanup.delete_completed_job_inputs(db)
    assert len(list(path.iterdir())) == 4


@pytest.mark.asyncio
async def test_cleanup_preserves_unknown_directories_and_symlink_targets(failure_api, input_storage):
    unknown = write_inputs(uuid4())
    unrelated = Path(config.JOBS_DIR) / "unrelated"
    unrelated.mkdir()
    target = input_storage / "outside-inputs"
    target.mkdir()
    (target / "keep").write_text("keep")
    job_id = await create_job(failure_api, ProcessingState.DONE)
    symlink = Path(config.JOBS_DIR) / str(job_id)
    symlink.symlink_to(target, target_is_directory=True)
    async with failure_api.sessions() as db:
        await cleanup.delete_completed_job_inputs(db)
    assert unknown.is_dir()
    assert unrelated.is_dir()
    assert symlink.is_symlink()
    assert (target / "keep").read_text() == "keep"


@pytest.mark.asyncio
async def test_cleanup_retries_failed_deletion(failure_api, input_storage, monkeypatch):
    job_id = await create_job(failure_api, ProcessingState.FAILED)
    path = write_inputs(job_id)
    other_id = await create_job(failure_api, ProcessingState.DONE)
    other_path = write_inputs(other_id)
    rmtree = cleanup.shutil.rmtree

    def fail_one(path_to_remove):
        if str(path_to_remove) == str(path):
            raise PermissionError("Test deletion failure")
        rmtree(path_to_remove)

    monkeypatch.setattr(cleanup.shutil, "rmtree", fail_one)
    async with failure_api.sessions() as db:
        await cleanup.delete_completed_job_inputs(db)
    assert path.exists()
    assert not other_path.exists()
    monkeypatch.setattr(cleanup.shutil, "rmtree", rmtree)
    async with failure_api.sessions() as db:
        await cleanup.delete_completed_job_inputs(db)
    assert not path.exists()


def test_input_cleanup_disabled_by_default(monkeypatch):
    monkeypatch.delenv("DELETE_JOB_INPUTS_ON_COMPLETION", raising=False)
    assert Config().DELETE_JOB_INPUTS_ON_COMPLETION is False


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
async def test_cleanup_skips_locked_and_requeued_jobs(failure_api, input_storage):
    job_id = await create_job(failure_api, ProcessingState.DONE)
    path = write_inputs(job_id)
    async with failure_api.sessions.begin() as admin:
        job = await admin.scalar(select(model.Job).where(model.Job.id == job_id).with_for_update())
        async with failure_api.sessions() as db:
            await asyncio.wait_for(cleanup.delete_completed_job_inputs(db), timeout=5)
        assert path.exists()
        job.state = ProcessingState.QUEUED
    async with failure_api.sessions() as db:
        await cleanup.delete_completed_job_inputs(db)
    assert path.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
async def test_cleanup_task_lifecycle(monkeypatch, enabled):
    monkeypatch.setattr(config, "ADMIN_KEY", None)
    monkeypatch.setattr(config, "PRODUCTION", True)
    monkeypatch.setattr(config, "DELETE_JOB_INPUTS_ON_COMPLETION", enabled)
    started = asyncio.Event()
    stopped = asyncio.Event()

    async def cleanup_task():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    monkeypatch.setattr(main, "run_input_cleanup", cleanup_task)
    async with main.lifespan(main.app):
        if enabled:
            await asyncio.wait_for(started.wait(), timeout=5)
        else:
            await asyncio.sleep(0)
            assert not started.is_set()
    assert stopped.is_set() is enabled
