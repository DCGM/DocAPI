"""API responsiveness while cleanup is waiting on slow filesystem work."""
import asyncio
from contextlib import suppress
from pathlib import Path
import threading

import pytest
from sqlalchemy import text

from doc_api.api import cleanup
from doc_api.api.schemas.base_objects import ProcessingState
from doc_api.api.schemas.responses import AppCode
from doc_api.config import config
from doc_api.db import model


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["inputs", "results", "artifacts"])
async def test_api_remains_responsive_during_cleanup(client, user_headers, worker_headers, admin_headers,
                                                     db_sessions, job_factory, monkeypatch, target):
    completed = sorted([await job_factory(ProcessingState.DONE), await job_factory(ProcessingState.DONE)])
    active = await job_factory(ProcessingState.PROCESSING)
    monkeypatch.setattr(config, "DELETE_JOB_INPUTS_ON_COMPLETION", target == "inputs")
    monkeypatch.setattr(config, "DELETE_JOB_OUTPUTS", target != "inputs")
    monkeypatch.setattr(config, "INPUT_RETENTION_SECONDS", 0)
    monkeypatch.setattr(config, "OUTPUT_RETENTION_SECONDS", 0)
    monkeypatch.setattr(config, "OUTPUT_CLEANUP_TARGETS", {target} if target != "inputs" else set())
    paths = []
    for job_id in completed:
        result = Path(config.RESULTS_DIR) / f"{job_id}.zip"
        result.write_bytes(b"result")
        if target == "inputs":
            path = Path(config.JOBS_DIR) / str(job_id)
            path.mkdir()
            (path / "page.jpg").write_bytes(b"input")
        else:
            directory = config.RESULTS_DIR if target == "results" else config.ARTIFACTS_DIR
            path = Path(directory) / f"{job_id}.zip"
            path.write_bytes(b"output")
        paths.append(path)

    started = threading.Event()
    released = threading.Event()
    original_remove = cleanup._remove_job_data

    def slow_remove(path, directory):
        if Path(path) == paths[0]:
            started.set()
            # A real thread watchdog also releases an accidentally blocked event
            # loop. An asyncio timeout alone could not detect that regression.
            if not released.wait(timeout=30):
                raise RuntimeError("Cleanup responsiveness watchdog expired")
        return original_remove(path, directory)

    monkeypatch.setattr(cleanup, "_remove_job_data", slow_remove)
    watchdog = threading.Timer(30, released.set)
    watchdog.daemon = True
    watchdog.start()
    task = asyncio.create_task(cleanup.run_cleanup())
    writer = None
    try:
        assert await asyncio.to_thread(started.wait, 10), "Cleanup did not reach file deletion"
        assert not released.is_set(), "File deletion blocked the API event loop"
        # Cleanup locks the complete selected batch, including a second job whose
        # files it has not reached yet. Its update waits without blocking the loop.
        writer = asyncio.create_task(client.patch(f"/v1/admin/jobs/{completed[1]}", headers=admin_headers,
                                                  json={"log": "Updated after cleanup"}))

        async def wait_for_row_lock():
            while True:
                async with db_sessions() as db:
                    blocked = await db.scalar(text("""
                        SELECT EXISTS (
                            SELECT 1 FROM pg_stat_activity
                            WHERE datname = current_database()
                              AND cardinality(pg_blocking_pids(pid)) > 0
                        )
                    """))
                if blocked:
                    return
                await asyncio.sleep(0.01)

        await asyncio.wait_for(wait_for_row_lock(), timeout=10)
        assert not writer.done()
        download_url = (f"/v1/admin/jobs/{completed[0]}/artifacts" if target == "artifacts"
                        else f"/v1/jobs/{completed[0]}/result")
        download_headers = admin_headers if target == "artifacts" else user_headers
        responses = await asyncio.wait_for(asyncio.gather(
            client.get("/v1/me", headers=user_headers),
            client.get("/v1/jobs", headers=user_headers),
            client.get(f"/v1/jobs/{completed[0]}", headers=user_headers),
            client.patch(f"/v1/jobs/{active}", headers=worker_headers, json={"progress": 0.7}),
            client.get(download_url, headers=download_headers),
        ), timeout=10)
        assert not released.is_set(), "Requests only completed after filesystem deletion was released"
        assert not task.done()
        assert not writer.done(), "The cleanup batch should still hold its row locks"
        for response, code in zip(responses[:4], [AppCode.API_KEY_VALID, AppCode.JOBS_RETRIEVED,
                                                 AppCode.JOB_RETRIEVED, AppCode.JOB_UPDATED]):
            assert response.status_code == 200, response.text
            assert response.json()["code"] == code.value
        assert responses[4].status_code == 200, responses[4].text
        assert responses[4].content == (b"result" if target == "inputs" else b"output")
        assert all(path.exists() for path in paths)
        async with db_sessions() as db:
            assert (await db.get(model.Job, active)).progress == 0.7
            for job_id in completed:
                assert getattr(await db.get(model.Job, job_id), f"{target}_deleted_at") is None

        released.set()
        response = await asyncio.wait_for(writer, timeout=10)
        assert response.status_code == 200, response.text
        assert response.json()["code"] == AppCode.JOB_UPDATED.value
        assert not any(path.exists() for path in paths)
        async with db_sessions() as db:
            for job_id in completed:
                assert getattr(await db.get(model.Job, job_id), f"{target}_deleted_at") is not None
            assert (await db.get(model.Job, completed[1])).log.endswith("Updated after cleanup")
    finally:
        released.set()
        watchdog.cancel()
        if writer is not None:
            writer.cancel()
            with suppress(asyncio.CancelledError):
                await writer
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
