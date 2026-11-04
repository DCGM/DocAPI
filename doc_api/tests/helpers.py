import io
import urllib.parse
from pathlib import Path


from alembic import command
from alembic.config import Config

from doc_api.db import model


async def migrate(engine, revision, *, downgrade=False):
    """Run the actual Alembic environment on the test's existing connection."""
    cfg = Config()
    cfg.set_main_option("script_location", str(Path(__file__).resolve().parents[1] / "alembic"))

    def run(connection):
        cfg.attributes["connection"] = connection
        (command.downgrade if downgrade else command.upgrade)(cfg, revision)

    async with engine.begin() as connection:
        await connection.run_sync(run)


async def assert_deletion_timestamps(db_sessions, job_id, expected):
    async with db_sessions() as db:
        job = await db.get(model.Job, job_id)
        for target in ("inputs", "results", "artifacts"):
            assert getattr(job, f"{target}_deleted_at") == expected


async def _put_file(client, url: str, field: str, filename: str, data: bytes, content_type: str, headers):
    files = {field: (filename, io.BytesIO(data), content_type)}
    r = await client.put(url, files=files, headers=headers)
    return r


def _ename(name: str) -> str:
    return urllib.parse.quote(name, safe="._-()[]")


# Explicit contract inventory: adding a route requires assigning its test cases.
# ADMIN is permitted on every protected route.
ROUTE_CASES = [
    ("GET", "/v1/me", {}, {"readonly", "user", "worker", "admin"}),
    ("GET", "/v1/engines", {}, {"readonly", "user", "worker", "admin"}),
    ("POST", "/v1/jobs", {"json": {"images": []}}, {"user", "admin"}),
    ("GET", "/v1/jobs", {}, {"readonly", "user", "admin"}),
    ("GET", "/v1/jobs/{job_id}", {}, {"readonly", "user", "worker", "admin"}),
    ("PATCH", "/v1/jobs/{job_id}", {"json": {"progress": 0.5}}, {"user", "worker", "admin"}),
    *[("PUT", "/v1/jobs/{job_id}/images/{image_name}/files/" + kind,
       {"files": {"file": ("page", b"data")}}, {"user", "admin"}) for kind in ("image", "alto", "page")],
    ("PUT", "/v1/jobs/{job_id}/files/metadata", {"json": {}}, {"user", "admin"}),
    ("GET", "/v1/jobs/{job_id}/result", {}, {"readonly", "user", "admin"}),
    ("POST", "/v1/jobs/lease", {}, {"worker", "admin"}),
    ("PATCH", "/v1/jobs/{job_id}/lease", {}, {"worker", "admin"}),
    ("DELETE", "/v1/jobs/{job_id}/lease", {}, {"worker", "admin"}),
    ("GET", "/v1/engines/{engine_id}/files", {}, {"worker", "admin"}),
    *[("GET", "/v1/jobs/{job_id}/images/{image_id}/files/" + kind, {}, {"worker", "admin"})
      for kind in ("image", "alto", "page")],
    ("GET", "/v1/jobs/{job_id}/files/metadata", {}, {"worker", "admin"}),
    *[("POST", "/v1/jobs/{job_id}/" + kind, {"files": {"file": ("output.zip", b"data")}}, {"worker", "admin"})
      for kind in ("result", "artifacts")],
    ("GET", "/v1/admin/keys", {}, {"admin"}),
    ("POST", "/v1/admin/keys", {"json": {"label": "new-key", "role": "user"}}, {"admin"}),
    ("POST", "/v1/admin/keys/{label}/secret", {}, {"admin"}),
    ("PATCH", "/v1/admin/keys/{label}", {"json": {"active": False}}, {"admin"}),
    ("POST", "/v1/admin/engines", {"json": {"name": "engine", "version": "1"}}, {"admin"}),
    ("PATCH", "/v1/admin/engines/{name}/{version}", {"json": {"active": False}}, {"admin"}),
    ("PUT", "/v1/admin/engines/{name}/{version}/files", {"files": {"file": ("engine.zip", b"data")}}, {"admin"}),
    ("GET", "/v1/admin/jobs/{job_id}/artifacts", {}, {"admin"}),
    ("PATCH", "/v1/admin/jobs/{job_id}", {"json": {}}, {"admin"}),
    ("PUT", "/debug/jobs/{job_id}/start", {}, {"admin"}),
]


def route_url(path, **values):
    defaults = {name: "00000000-0000-0000-0000-000000000001"
                for name in ("job_id", "engine_id", "image_id")}
    defaults.update(image_name="page.jpg", label="test-user", name="missing", version="1")
    defaults.update(values)
    return path.format(**defaults)


# Swagger section membership is independent of the roles allowed to call a route.
OPENAPI_SECTION_OPERATIONS = {
    "User": {
        ("get", "/v1/me"), ("get", "/v1/engines"),
        ("get", "/v1/jobs"), ("post", "/v1/jobs"),
        ("get", "/v1/jobs/{job_id}"), ("patch", "/v1/jobs/{job_id}"),
        *[("put", "/v1/jobs/{job_id}/images/{image_name}/files/" + kind) for kind in ("image", "alto", "page")],
        ("put", "/v1/jobs/{job_id}/files/metadata"), ("get", "/v1/jobs/{job_id}/result"),
    },
    "Worker": {
        ("post", "/v1/jobs/lease"),
        ("patch", "/v1/jobs/{job_id}/lease"), ("delete", "/v1/jobs/{job_id}/lease"),
        ("get", "/v1/engines/{engine_id}/files"),
        *[("get", "/v1/jobs/{job_id}/images/{image_id}/files/" + kind) for kind in ("image", "alto", "page")],
        ("get", "/v1/jobs/{job_id}/files/metadata"),
        ("post", "/v1/jobs/{job_id}/result"), ("post", "/v1/jobs/{job_id}/artifacts"),
    },
    "Admin": {
        ("get", "/v1/admin/keys"), ("post", "/v1/admin/keys"),
        ("patch", "/v1/admin/keys/{label}"), ("post", "/v1/admin/keys/{label}/secret"),
        ("post", "/v1/admin/engines"), ("patch", "/v1/admin/engines/{name}/{version}"),
        ("put", "/v1/admin/engines/{name}/{version}/files"),
        ("get", "/v1/admin/jobs/{job_id}/artifacts"), ("patch", "/v1/admin/jobs/{job_id}"),
    },
}

OPENAPI_VISIBILITY_ROUTES = [
    ("SHOW_GET_ENGINES", "get", "/v1/engines"),
    *[(f"SHOW_PUT_{kind.upper()}", "put", "/v1/jobs/{job_id}/images/{image_name}/files/" + kind)
      for kind in ("image", "alto", "page")],
    ("SHOW_PUT_METADATA", "put", "/v1/jobs/{job_id}/files/metadata"),
]


async def interrupted_upload_body(metadata=False):
    if metadata:
        yield b'{"source": '
    else:
        yield (b'--transfer-test\r\nContent-Disposition: form-data; name="file"; filename="upload"\r\n'
               b'Content-Type: application/octet-stream\r\n\r\npartial upload')
    raise OSError("injected transfer failure")


def interrupted_upload_headers(headers, *, metadata=False):
    return {**headers, "Content-Type": "application/json" if metadata else "multipart/form-data; boundary=transfer-test"}


def assert_internal_transfer_error(response):
    from doc_api.api.schemas.responses import AppCode
    assert response.status_code == 500, response.text
    assert response.json()["code"] == AppCode.INTERNAL_ERROR.value
    assert "injected transfer failure" not in response.text


async def assert_records_unchanged(sessions, records):
    async with sessions() as db:
        for entity, record_id, before in records:
            assert model.orm2dict(await db.get(entity, record_id)) == before


async def interrupt_download(app, url, headers, path, monkeypatch, failure):
    """Observe ASGI framing: buffered HTTPX cannot detect an incomplete 200 body."""
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    import anyio
    import pytest

    real_open = anyio.open_file

    async def failing_open(filename, *args, **kwargs):
        if failure == "open" and Path(filename) == path:
            raise OSError("injected transfer failure")
        opened = await real_open(filename, *args, **kwargs)
        if Path(filename) != path or failure != "read":
            return opened

        @asynccontextmanager
        async def interrupted_reader():
            reads = 0
            async def read(size):
                nonlocal reads
                if reads:
                    raise OSError("injected transfer failure")
                reads += 1
                return await opened.read(size)
            async with opened:
                yield SimpleNamespace(read=read)
        return interrupted_reader()

    messages = []
    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        messages.append(message.copy())
        if failure == "disconnect" and message["type"] == "http.response.body":
            raise OSError("injected transfer failure")

    scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"},
             "http_version": "1.1", "method": "GET", "scheme": "http", "path": url,
             "raw_path": url.encode(), "root_path": "", "query_string": b"",
             "headers": [(name.lower().encode(), value.encode()) for name, value in headers.items()],
             "server": ("test", 80), "client": ("127.0.0.1", 1234)}
    with monkeypatch.context() as patch:
        patch.setattr(anyio, "open_file", failing_open)
        with pytest.raises(OSError, match="injected transfer failure"):
            await app(scope, receive, send)
    starts = [message for message in messages if message["type"] == "http.response.start"]
    assert len(starts) == 1 and starts[0]["status"] == 200
    declared = int(dict(starts[0]["headers"])[b"content-length"])
    bodies = [message for message in messages if message["type"] == "http.response.body"]
    assert not any(not message.get("more_body", False) for message in bodies)
    transferred = sum(len(message.get("body", b"")) for message in bodies)
    if failure == "open":
        assert transferred == 0
    else:
        assert 0 < transferred < declared


def inject_zip_upload_failure(path, failure, patch):
    """Fail after staging has begun, without affecting multipart parsing."""
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    from starlette.datastructures import UploadFile
    from doc_api.api import file_saving

    if failure == "read":
        real_read = UploadFile.read
        reads = 0
        async def broken_read(upload, size=-1):
            nonlocal reads
            if reads:
                raise OSError("injected transfer failure")
            reads += 1
            return await real_read(upload, 16)
        patch.setattr(UploadFile, "read", broken_read)
    elif failure == "write":
        real_open = file_saving.aiofiles.open
        def broken_open(filename, *args, **kwargs):
            opened = real_open(filename, *args, **kwargs)
            candidate = Path(filename)
            if candidate.parent != path.parent or not candidate.name.startswith(path.name + ".docapi-tmp-upload-staging-"):
                return opened
            @asynccontextmanager
            async def interrupted_writer():
                async with opened as file:
                    async def write(data):
                        await file.write(data[:3])
                        raise OSError("injected transfer failure")
                    yield SimpleNamespace(write=write)
            return interrupted_writer()
        patch.setattr(file_saving.aiofiles, "open", broken_open)
    else:
        real_replace = file_saving.os.replace
        def broken_replace(source, destination):
            if Path(destination) == path and ".docapi-tmp-upload-staging-" in str(source):
                raise OSError("injected transfer failure")
            return real_replace(source, destination)
        patch.setattr(file_saving.os, "replace", broken_replace)


async def assert_file_save_conflict(client, *, method, url, headers, kwargs, path,
                                    monkeypatch, db_sessions, first_status=201, fail_first=False):
    """A staged save uses no DB connection and rejects a competing request."""
    import asyncio
    from doc_api.api.database import DBError
    from doc_api.api import file_saving as file_helpers
    from doc_api.api.schemas.responses import AppCode

    ready, release = asyncio.Event(), asyncio.Event()
    original_install = file_helpers.FileSaveTransaction.install
    installs = 0

    async def install(save):
        nonlocal installs
        installs += 1
        if installs == 1:
            ready.set()
            await release.wait()
        await original_install(save)
        if installs == 1 and fail_first:
            raise DBError("Injected failure after installation")

    with monkeypatch.context() as patch:
        patch.setattr(file_helpers.FileSaveTransaction, "install", install)
        first = asyncio.create_task(client.request(method, url, headers=headers, **kwargs))
        try:
            await asyncio.wait_for(ready.wait(), timeout=10)
            assert db_sessions.kw["bind"].pool.checkedout() == 0
            conflict = await asyncio.wait_for(client.request(method, url, headers=headers, **kwargs), timeout=5)
            assert conflict.status_code == 409, conflict.text
            assert conflict.json()["code"] == AppCode.FILE_SAVE_IN_PROGRESS.value
            assert "Retry" in conflict.json()["detail"]
            assert installs == 1 and not first.done()
            assert db_sessions.kw["bind"].pool.checkedout() == 0
            assert len(list(path.parent.glob(path.name + ".docapi-tmp-upload-staging-*"))) == 1
            release.set()
            completed = await asyncio.wait_for(first, timeout=10)
            assert completed.status_code == first_status, completed.text
        finally:
            release.set()
            if not first.done():
                first.cancel()
            await asyncio.gather(first, return_exceptions=True)
    # The lock must be reusable after success or rollback.
    retry = await client.request(method, url, headers=headers, **kwargs)
    assert retry.status_code == 200, retry.text
    assert not list(path.parent.glob(path.name + ".docapi-tmp-*"))
