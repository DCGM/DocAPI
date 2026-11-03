import json
from uuid import uuid4

import pytest
from sqlalchemy import inspect, text

from doc_api.tests.helpers import migrate


@pytest.mark.asyncio
async def test_upgrade_existing_jobs(isolated_database):
    """Start before cleanup tracking, populate jobs, then upgrade through Alembic."""
    engine = isolated_database.engine
    await migrate(engine, "dea31fd02e5b")
    owner = uuid4()
    definitions = [{"images": [{"name": "a.jpg"}, {"name": "b.jpg"}]},
                   {"images": []}, {}, {"images": None}]
    jobs = [uuid4() for _ in definitions]
    async with engine.begin() as db:
        await db.execute(text("""
            INSERT INTO keys (id, kid, salt, key_hash, label, role, active, created_date)
            VALUES (:id, 'migration-key', 'salt', 'hash', 'owner', 'USER', true, now())
        """), {"id": owner})
        for job_id, definition in zip(jobs, definitions):
            await db.execute(text("""
                INSERT INTO jobs (id, owner_key_id, definition, alto_required, page_required,
                    meta_json_required, meta_json_uploaded, state, progress, created_date, last_change)
                VALUES (:id, :owner, CAST(:definition AS jsonb), false, false, false,
                        false, 'DONE', 1.0, now(), now())
            """), {"id": job_id, "owner": owner, "definition": json.dumps(definition)})
            # Only one row remains; definitions with an image list must take precedence.
            await db.execute(text("""
                INSERT INTO images (id, job_id, name, "order", image_uploaded, alto_uploaded, page_uploaded)
                VALUES (:id, :job, 'a.jpg', 0, true, false, false)
            """), {"id": uuid4(), "job": job_id})

    await migrate(engine, "head")
    async with engine.begin() as db:
        rows = {row.id: row for row in (await db.execute(text("SELECT * FROM jobs"))).all()}
        assert [rows[job].image_count for job in jobs] == [2, 0, 1, 1]
        assert all(rows[job].state == "DONE" for job in jobs)
        assert [rows[job].definition for job in jobs] == definitions
        columns = {"inputs_deleted_at", "results_deleted_at", "artifacts_deleted_at"}
        assert all(getattr(rows[job], column) is None for job in jobs for column in columns)

        def check_schema(connection):
            info = {c["name"]: c for c in inspect(connection).get_columns("jobs")}
            assert all(info[name]["nullable"] and info[name]["type"].timezone for name in columns)
            assert not info["image_count"]["nullable"]

        await db.run_sync(check_schema)

    await migrate(engine, "dea31fd02e5b", downgrade=True)
    async with engine.begin() as db:
        names = await db.run_sync(lambda conn: {c["name"] for c in inspect(conn).get_columns("jobs")})
        assert not (columns | {"image_count"}) & names
        assert (await db.execute(text("SELECT count(*) FROM jobs"))).scalar_one() == len(jobs)
    await migrate(engine, "head")
    async with engine.begin() as db:
        assert (await db.execute(text("SELECT count(*) FROM jobs"))).scalar_one() == len(jobs)
