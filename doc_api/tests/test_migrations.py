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


@pytest.mark.asyncio
async def test_upgrade_image_input_url_flags(isolated_database):
    engine = isolated_database.engine
    await migrate(engine, "97c2e01b6a45")
    owner, job_id, image_id = uuid4(), uuid4(), uuid4()
    async with engine.begin() as db:
        await db.execute(text("""
            INSERT INTO keys (id, kid, salt, key_hash, label, role, active, created_date)
            VALUES (:id, 'url-migration', 'salt', 'hash', 'owner', 'USER', true, now())
        """), {"id": owner})
        await db.execute(text("""
            INSERT INTO jobs (id, owner_key_id, definition, image_count, alto_required, page_required,
                meta_json_required, meta_json_uploaded, state, progress, created_date, last_change)
            VALUES (:id, :owner, '{"images": []}', 1, true, true, false, false, 'NEW', 0, now(), now())
        """), {"id": job_id, "owner": owner})
        await db.execute(text("""
            INSERT INTO images (id, job_id, name, "order", image_uploaded, alto_uploaded, page_uploaded)
            VALUES (:id, :job, 'page.jpg', 0, true, false, false)
        """), {"id": image_id, "job": job_id})
    columns = {"image_url_provided", "alto_url_provided", "page_url_provided"}
    await migrate(engine, "head")
    async with engine.begin() as db:
        row = (await db.execute(text("SELECT * FROM images WHERE id = :id"), {"id": image_id})).one()
        assert row.image_uploaded and not row.alto_uploaded and not row.page_uploaded
        assert all(getattr(row, name) is False for name in columns)
        schema = await db.run_sync(lambda conn: {c["name"]: c for c in inspect(conn).get_columns("images")})
        assert all(not schema[name]["nullable"] for name in columns)
        assert not {"image_url", "alto_url", "page_url"} & schema.keys()
        url = "https://source.invalid/page.jpg?token=" + "x" * 5000
        await db.execute(text("UPDATE jobs SET definition = CAST(:definition AS jsonb) WHERE id = :id"),
                         {"id": job_id, "definition": json.dumps({"images": [{"name": "page.jpg", "image_url": url}]})})
        await db.execute(text("UPDATE images SET image_url_provided = true WHERE id = :id"), {"id": image_id})
    await migrate(engine, "97c2e01b6a45", downgrade=True)
    async with engine.begin() as db:
        names = await db.run_sync(lambda conn: {c["name"] for c in inspect(conn).get_columns("images")})
        assert not columns & names
        assert (await db.execute(text("SELECT name, image_uploaded FROM images"))).one() == ("page.jpg", True)
    await migrate(engine, "head")
    async with engine.begin() as db:
        assert (await db.execute(text("SELECT image_url_provided FROM images"))).scalar_one() is True
        assert (await db.execute(text("SELECT alto_url_provided FROM images"))).scalar_one() is False
        assert (await db.execute(text("SELECT definition -> 'images' -> 0 ->> 'image_url' FROM jobs"))).scalar_one() == url


@pytest.mark.asyncio
async def test_upgrade_job_attempt_logs(isolated_database):
    engine = isolated_database.engine
    await migrate(engine, "c42d8f1e6a90")
    owner = uuid4()
    header2 = "ATTEMPT 2 STARTED AT 2026-01-01 12:00:00+00:00 AFTER TIMEOUT/ERROR.\n\n"
    header3 = "ATTEMPT 3 STARTED AT 2026-01-02 12:00:00+00:00 AFTER TIMEOUT/ERROR.\n\n"
    cases = [
        (None, None, None, None),
        ("", "", None, None),
        ("technical", "public", ["technical"], ["public"]),
        ("first\n\n\n\n" + header2 + "second", "public\n\n\n\n" + header2,
         ["first", header2 + "second"], ["public", None]),
        ("\n\n\n\n" + header3 + "third", "\n\n\n\n" + header3 + "third public",
         [None, None, header3 + "third"], [None, None, "third public"]),
        # A reused attempt must keep its latest entry, as future leases do.
        ("first\n\n\n\n" + header2 + "discard\n\n\n\n" + header2 + "replacement", None,
         ["first", header2 + "replacement"], None),
        ("Arbitrary ATTEMPT 2 text stays intact", None, ["Arbitrary ATTEMPT 2 text stays intact"], None),
    ]
    jobs = [uuid4() for _ in cases]
    async with engine.begin() as db:
        await db.execute(text("""
            INSERT INTO keys (id, kid, salt, key_hash, label, role, active, created_date)
            VALUES (:id, 'log-migration', 'salt', 'hash', 'owner', 'USER', true, now())
        """), {"id": owner})
        for job_id, (technical, public, _, _) in zip(jobs, cases):
            await db.execute(text("""
                INSERT INTO jobs (id, owner_key_id, definition, image_count, alto_required, page_required,
                    meta_json_required, meta_json_uploaded, state, progress, created_date, last_change, log, log_user)
                VALUES (:id, :owner, '{"images": []}', 0, false, false, false, false,
                        'DONE', 1, now(), now(), :log, :log_user)
            """), {"id": job_id, "owner": owner, "log": technical, "log_user": public})
    for _ in range(2):
        await migrate(engine, "head")
        async with engine.begin() as db:
            rows = {row.id: row for row in (await db.execute(text("SELECT id, log, log_user FROM jobs"))).all()}
            for job_id, (_, _, technical, public) in zip(jobs, cases):
                assert rows[job_id].log == technical
                assert rows[job_id].log_user == public
            types = await db.run_sync(lambda conn: {c["name"]: c for c in inspect(conn).get_columns("jobs")})
            assert str(types["log"]["type"]) == "JSONB"
            assert types["log"]["nullable"] and types["log_user"]["nullable"]
        await migrate(engine, "c42d8f1e6a90", downgrade=True)
        async with engine.begin() as db:
            assert (await db.execute(text("SELECT count(*) FROM jobs"))).scalar_one() == len(cases)
