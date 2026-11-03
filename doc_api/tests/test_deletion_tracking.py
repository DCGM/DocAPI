from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import event

from doc_api.api import cleanup
from doc_api.api.schemas.base_objects import ProcessingState
from doc_api.config import config
from doc_api.db import model


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["inputs", "results"])
async def test_recovers_after_files_removed_but_commit_fails(db_sessions, output_storage, monkeypatch, target, output_factory):
    job_id, outputs = await output_factory(ProcessingState.DONE, output_storage - timedelta(days=8))
    monkeypatch.setattr(config, "DELETE_JOB_INPUTS_ON_COMPLETION", True)
    inputs = Path(config.JOBS_DIR) / str(job_id)
    inputs.mkdir()
    (inputs / "image.jpg").write_bytes(b"input")
    clean = cleanup.delete_completed_job_inputs if target == "inputs" else cleanup.delete_expired_job_outputs
    path = inputs if target == "inputs" else outputs["results"]
    monkeypatch.setattr(config, "OUTPUT_CLEANUP_TARGETS", {"results"})

    def reject_commit(session):
        raise RuntimeError("Test commit failure")

    async with db_sessions() as db:
        event.listen(db.sync_session, "before_commit", reject_commit, once=True)
        with pytest.raises(RuntimeError, match="Test commit failure"):
            await clean(db)
    assert not path.exists()
    async with db_sessions() as db:
        assert getattr(await db.get(model.Job, job_id), f"{target}_deleted_at") is None
    async with db_sessions() as db:
        await clean(db)
    async with db_sessions() as db:
        recorded = getattr(await db.get(model.Job, job_id), f"{target}_deleted_at")
        assert recorded == output_storage
    async with db_sessions() as db:
        await clean(db)
    async with db_sessions() as db:
        assert getattr(await db.get(model.Job, job_id), f"{target}_deleted_at") == recorded
