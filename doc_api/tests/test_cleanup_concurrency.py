"""API responsiveness and file requests overlapping real cleanup deletion."""
import asyncio
from contextlib import suppress
from pathlib import Path
import threading

import pytest
from sqlalchemy import select, text

from doc_api.api import file_saving, main
from doc_api.api.checkers import cleanup
from doc_api.api.schemas.base_objects import ProcessingState
from doc_api.api.schemas.responses import AppCode
from doc_api.config import config
from doc_api.db import model
from doc_api.tests.dummy_data import VALID_ZIP


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["image", "alto", "page", "metadata", "result", "artifacts"])
async def test_cleanup_skips_installing_file(client, admin_headers, input_upload_case, zip_upload_case,
                                            db_sessions, monkeypatch, kind):
    is_input = kind not in {"result", "artifacts"}
    case = await input_upload_case(kind) if is_input else await zip_upload_case(kind)
    job_id = case.job_id if is_input else case.records[0][1]
    target = "inputs" if is_input else {"result": "results", "artifacts": "artifacts"}[kind]
    async with db_sessions.begin() as db:
        (await db.get(model.Job, job_id)).state = ProcessingState.DONE
    monkeypatch.setattr(config, "DELETE_JOB_INPUTS_ON_COMPLETION", is_input)
    monkeypatch.setattr(config, "DELETE_JOB_OUTPUTS", not is_input)
    monkeypatch.setattr(config, "INPUT_RETENTION_SECONDS", 0)
    monkeypatch.setattr(config, "OUTPUT_RETENTION_SECONDS", 0)
    monkeypatch.setattr(config, "OUTPUT_CLEANUP_TARGETS", {target} if not is_input else set())
    clean = cleanup.delete_completed_job_inputs if is_input else cleanup.delete_expired_job_outputs
    original_install = file_saving.FileSaveTransaction.install
    checked = False

    async def install(file_save):
        nonlocal checked
        await original_install(file_save)
        if Path(file_save.path) != case.path:
            return
        installed_bytes = case.path.read_bytes()
        # A separate cleanup transaction must skip this job while the saver
        # holds its row lock, even though its new file is already installed.
        async with db_sessions() as db:
            await asyncio.wait_for(clean(db), timeout=5)
        assert case.path.read_bytes() == installed_bytes
        async with db_sessions() as db:
            assert getattr(await db.get(model.Job, job_id), f"{target}_deleted_at") is None
        checked = True

    monkeypatch.setattr(file_saving.FileSaveTransaction, "install", install)
    kwargs = case.kwargs if is_input else {"files": {"file": ("replacement.zip", VALID_ZIP)}}
    response = await client.request("PUT" if is_input else "POST", case.url, headers=admin_headers, **kwargs)
    assert response.status_code == (201 if is_input else 200), response.text
    assert checked and case.path.exists()
    # Once the save commits, the terminal job becomes eligible again. This pass
    # must perform actual deletion and record it, rather than skip indefinitely.
    async with db_sessions() as db:
        await clean(db)
    assert not case.path.exists()
    async with db_sessions() as db:
        assert getattr(await db.get(model.Job, job_id), f"{target}_deleted_at") is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,delete_image_records", [
    *[(kind, False) for kind in ["image", "alto", "page", "metadata", "result", "artifacts"]],
    *[(kind, True) for kind in ["image", "alto", "page", "metadata"]],
])
@pytest.mark.parametrize("deletion_point", ["before_request", "after_headers", "after_first_chunk"])
async def test_download_during_cleanup(client, admin_headers, download_case, db_sessions,
                                       monkeypatch, kind, delete_image_records, deletion_point):
    case = await download_case(kind)
    job_id = case.records[0][1]
    target = {"result": "results", "artifacts": "artifacts"}.get(kind, "inputs")
    # Admin can inspect terminal-job inputs; workers cannot download them once
    # the job has completed. Test optional image-record deletion as well.
    async with db_sessions.begin() as db:
        (await db.get(model.Job, job_id)).state = ProcessingState.DONE
    monkeypatch.setattr(config, "DELETE_JOB_INPUTS_ON_COMPLETION", target == "inputs")
    monkeypatch.setattr(config, "DELETE_JOB_IMAGE_RECORDS_WITH_INPUTS", delete_image_records)
    monkeypatch.setattr(config, "DELETE_JOB_OUTPUTS", target != "inputs")
    monkeypatch.setattr(config, "INPUT_RETENTION_SECONDS", 0)
    monkeypatch.setattr(config, "OUTPUT_RETENTION_SECONDS", 0)
    monkeypatch.setattr(config, "OUTPUT_CLEANUP_TARGETS", {target} if target != "inputs" else set())

    async def delete_data():
        # Run the real deletion and DB commit at a precise transfer boundary,
        # without a timing-dependent sleep or a background loop left running.
        async with db_sessions() as db:
            clean = (cleanup.delete_completed_job_inputs if target == "inputs"
                     else cleanup.delete_expired_job_outputs)
            await clean(db)
        assert not case.path.exists()
        async with db_sessions() as db:
            job = await db.get(model.Job, job_id)
            assert job.state == ProcessingState.DONE
            assert getattr(job, f"{target}_deleted_at") is not None
            if target == "inputs":
                image = await db.scalar(select(model.Image).where(model.Image.job_id == job_id))
                assert (image is None) is delete_image_records

    gone_code = {"image": AppCode.IMAGE_GONE, "alto": AppCode.ALTO_GONE,
                 "page": AppCode.PAGE_GONE, "metadata": AppCode.META_JSON_GONE,
                 "result": AppCode.JOB_RESULT_GONE, "artifacts": AppCode.JOB_ARTIFACTS_NOT_FOUND}[kind]
    gone_status = 404 if kind == "artifacts" else 410
    if delete_image_records and kind in {"image", "alto", "page"}:
        gone_status, gone_code = 404, AppCode.IMAGE_NOT_FOUND_FOR_JOB
    if deletion_point == "before_request":
        await delete_data()
    else:
        messages = []
        deleted = False

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(message):
            nonlocal deleted
            messages.append(message.copy())
            boundary = (message["type"] == "http.response.start" if deletion_point == "after_headers"
                        else message["type"] == "http.response.body" and bool(message.get("body")))
            if boundary and not deleted:
                deleted = True
                await delete_data()

        scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"},
                 "http_version": "1.1", "method": "GET", "scheme": "http", "path": case.url,
                 "raw_path": case.url.encode(), "root_path": "", "query_string": b"",
                 "headers": [(name.lower().encode(), value.encode()) for name, value in admin_headers.items()],
                 "server": ("test", 80), "client": ("127.0.0.1", 1234)}
        if deletion_point == "after_headers":
            # Headers are already sent: a missing file must abort the body,
            # rather than append an error JSON to a successful ZIP/XML response.
            with pytest.raises(FileNotFoundError):
                await main.app(scope, receive, send)
        else:
            await main.app(scope, receive, send)
        assert deleted, "Download did not reach the requested deletion boundary"
        starts = [message for message in messages if message["type"] == "http.response.start"]
        assert len(starts) == 1 and starts[0]["status"] == 200
        assert int(dict(starts[0]["headers"])[b"content-length"]) == len(case.content)
        bodies = [message for message in messages if message["type"] == "http.response.body"]
        if deletion_point == "after_headers":
            assert not bodies
        else:
            # Linux keeps an opened file readable after unlink/rmtree. Cleanup
            # removes the path but must not truncate an already-open download.
            assert b"".join(message.get("body", b"") for message in bodies) == case.content
            assert bodies[-1].get("more_body", False) is False

    response = await client.get(case.url, headers=admin_headers)
    assert response.status_code == gone_status, response.text
    assert response.json()["code"] == gone_code.value
    assert not case.path.exists(), "A retry must not recreate deleted data"


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
        writer = asyncio.create_task(client.patch(f"/v1/jobs/{completed[1]}", headers=admin_headers,
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
            assert (await db.get(model.Job, completed[1])).log[-1].endswith("Updated after cleanup")
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
