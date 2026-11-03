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
