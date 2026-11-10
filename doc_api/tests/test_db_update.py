"""Startup schema upgrades, migration metadata and concurrent initialization."""
import asyncio
import os
from pathlib import Path
import subprocess
import sys

import pytest
import pytest_asyncio
from sqlalchemy import text

from doc_api.config import Config, config
from doc_api.db import db_update
from doc_api.db.migration_metadata import get_migration_metadata
from doc_api.tests.helpers import migrate

OLD_REVISION = "c43a1b93e824"
COMMIT = "b" * 40


@pytest.fixture
def migration_settings(isolated_database, monkeypatch):
    monkeypatch.setattr(config, "DATABASE_URL", isolated_database.engine.url.render_as_string(hide_password=False))
    monkeypatch.setattr(config, "DATABASE_ALLOW_SCHEMA_UPDATE", True)
    monkeypatch.setattr(config, "DATABASE_REQUIRE_MATCHING_SCHEMA", False)
    monkeypatch.setattr(config, "APP_GIT_COMMIT", COMMIT)


@pytest_asyncio.fixture
async def legacy_database(isolated_database, migration_settings):
    await migrate(isolated_database.engine, OLD_REVISION)
    async with isolated_database.engine.begin() as db:
        await db.execute(text("CREATE TABLE migration_probe (id integer PRIMARY KEY, payload text NOT NULL)"))
        await db.execute(text("INSERT INTO migration_probe VALUES (1, 'Příliš žluťoučký kůň')"))
    return isolated_database


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["empty", "current"])
async def test_initialization_and_code_update_preserve_migration_time(isolated_database, migration_settings, monkeypatch, state):
    if state == "current":
        await migrate(isolated_database.engine, "head")
    await asyncio.to_thread(db_update.init_and_update_db)
    async with isolated_database.engine.connect() as db:
        before = dict(await db.run_sync(get_migration_metadata))
        assert before["git_commit"] == COMMIT
        assert before["migrated_at"].tzinfo is not None
    monkeypatch.setattr(config, "APP_GIT_COMMIT", "c" * 40)
    monkeypatch.setattr(config, "DATABASE_ALLOW_SCHEMA_UPDATE", False)
    await asyncio.to_thread(db_update.init_and_update_db)
    async with isolated_database.engine.connect() as db:
        after = dict(await db.run_sync(get_migration_metadata))
        assert after == {**before, "git_commit": "c" * 40}


@pytest.mark.asyncio
async def test_startup_upgrade_preserves_data(legacy_database):
    await asyncio.to_thread(db_update.init_and_update_db)
    async with legacy_database.engine.connect() as db:
        assert await db.scalar(text("SELECT version_num FROM alembic_version")) == db_update.get_latest_alembic_revision()
        assert await db.scalar(text("SELECT payload FROM migration_probe")) == "Příliš žluťoučký kůň"
        assert (await db.run_sync(get_migration_metadata))["git_commit"] == COMMIT


@pytest.mark.asyncio
async def test_disallowed_upgrade_preserves_schema(legacy_database, monkeypatch):
    monkeypatch.setattr(config, "DATABASE_REQUIRE_MATCHING_SCHEMA", True)
    monkeypatch.setattr(config, "DATABASE_ALLOW_SCHEMA_UPDATE", False)
    with pytest.raises(RuntimeError, match="DATABASE_REQUIRE_MATCHING_SCHEMA=True"):
        await asyncio.to_thread(db_update.init_and_update_db)
    async with legacy_database.engine.connect() as db:
        assert await db.scalar(text("SELECT version_num FROM alembic_version")) == OLD_REVISION
        assert await db.run_sync(get_migration_metadata) is None


@pytest.mark.asyncio
async def test_failed_migration_rolls_back_metadata_and_releases_lock(legacy_database, monkeypatch):
    original = db_update.run_alembic_upgrade

    def fail_after_upgrade(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("injected failure after applying schema and metadata")

    monkeypatch.setattr(db_update, "run_alembic_upgrade", fail_after_upgrade)
    with pytest.raises(RuntimeError, match="injected failure"):
        await asyncio.to_thread(db_update.init_and_update_db)
    async with legacy_database.engine.connect() as db:
        assert await db.scalar(text("SELECT version_num FROM alembic_version")) == OLD_REVISION
        assert await db.run_sync(get_migration_metadata) is None
        assert await db.scalar(text("SELECT payload FROM migration_probe")) == "Příliš žluťoučký kůň"
    monkeypatch.setattr(db_update, "run_alembic_upgrade", original)
    await asyncio.wait_for(asyncio.to_thread(db_update.init_and_update_db), timeout=10)


@pytest.mark.asyncio
async def test_concurrent_startups_apply_one_migration(legacy_database, monkeypatch):
    original = db_update.run_alembic_upgrade
    calls = []

    def counted_upgrade(*args, **kwargs):
        calls.append(True)
        return original(*args, **kwargs)

    monkeypatch.setattr(db_update, "run_alembic_upgrade", counted_upgrade)
    await asyncio.wait_for(asyncio.gather(*(asyncio.to_thread(db_update.init_and_update_db) for _ in range(2))), timeout=20)
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["no_alembic_table", "no_alembic_version"])
async def test_unversioned_database_rejects_startup(isolated_database, migration_settings, state):
    async with isolated_database.engine.begin() as db:
        await db.execute(text("CREATE TABLE existing_data (id integer)"))
        if state == "no_alembic_version":
            await db.execute(text("CREATE TABLE alembic_version (version_num varchar(32) NOT NULL)"))
    with pytest.raises(RuntimeError, match="unversioned"):
        await asyncio.to_thread(db_update.init_and_update_db)


@pytest.mark.asyncio
async def test_online_alembic_records_migration_metadata(legacy_database):
    environment = {**os.environ, "DATABASE_URL": config.DATABASE_URL, "APP_GIT_COMMIT": COMMIT}
    result = await asyncio.to_thread(subprocess.run,
        [sys.executable, "-m", "alembic", "-c", str(Path(__file__).parents[1] / "alembic.ini"), "upgrade", "head"],
        env=environment, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    async with legacy_database.engine.connect() as db:
        assert await db.scalar(text("SELECT version_num FROM alembic_version")) == db_update.get_latest_alembic_revision()
        assert (await db.run_sync(get_migration_metadata))["git_commit"] == COMMIT


@pytest.mark.asyncio
@pytest.mark.parametrize("allow_update", [False, True])
@pytest.mark.parametrize("schema", ["unknown", "newer"])
async def test_incompatible_schema_rejects_startup(isolated_database, migration_settings, monkeypatch, allow_update, schema):
    await migrate(isolated_database.engine, "head")
    monkeypatch.setattr(config, "DATABASE_ALLOW_SCHEMA_UPDATE", allow_update)
    monkeypatch.setattr(config, "DATABASE_REQUIRE_MATCHING_SCHEMA", True)
    if schema == "unknown":
        revision = "future_revision"
        async with isolated_database.engine.begin() as db:
            await db.execute(text("UPDATE alembic_version SET version_num = :revision"), {"revision": revision})
        expected_message = "not recognized by this DocAPI build"
    else:
        revision = db_update.get_latest_alembic_revision()
        monkeypatch.setattr(db_update, "get_latest_alembic_revision", lambda: OLD_REVISION)
        expected_message = "is newer than this DocAPI build supports"
    async with isolated_database.engine.connect() as db:
        before = dict(await db.run_sync(get_migration_metadata))

    def unexpected_upgrade(*args, **kwargs):
        raise AssertionError("An incompatible schema must not reach Alembic upgrade")

    monkeypatch.setattr(db_update, "run_alembic_upgrade", unexpected_upgrade)
    with pytest.raises(RuntimeError, match=expected_message) as error:
        await asyncio.to_thread(db_update.init_and_update_db)
    assert revision in str(error.value)
    assert "DATABASE_ALLOW_SCHEMA_UPDATE" in str(error.value)
    assert "matching DocAPI image" in str(error.value)
    async with isolated_database.engine.connect() as db:
        assert await db.scalar(text("SELECT version_num FROM alembic_version")) == revision
        assert dict(await db.run_sync(get_migration_metadata)) == before


@pytest.mark.parametrize("setting,default", [
    ("DATABASE_REQUIRE_MATCHING_SCHEMA", True), ("DATABASE_ALLOW_SCHEMA_UPDATE", False),
    ("DATABASE_REQUIRE_MATCHING_COMMIT", False), ("DATABASE_ALLOW_COMMIT_UPDATE", True),
])
@pytest.mark.parametrize("value", [None, "true", "false"])
def test_database_policy_configuration(monkeypatch, setting, default, value):
    monkeypatch.delenv(setting, raising=False)
    if value is not None:
        monkeypatch.setenv(setting, value)
    assert getattr(Config(), setting) is (default if value is None else value == "true")


@pytest.mark.asyncio
@pytest.mark.parametrize("require_matching_commit", [False, True])
@pytest.mark.parametrize("allow_commit_update", [False, True])
@pytest.mark.parametrize("stored_commit", [COMMIT, None], ids=["known-db", "unknown-db"])
@pytest.mark.parametrize("running_commit", [COMMIT, "c" * 40, None], ids=["same-code", "new-code", "unknown-code"])
async def test_startup_commit_policy(isolated_database, migration_settings, monkeypatch,
                                     require_matching_commit, allow_commit_update, stored_commit, running_commit):
    await migrate(isolated_database.engine, "head")
    async with isolated_database.engine.begin() as db:
        await db.execute(text("UPDATE database_metadata SET git_commit = :commit"), {"commit": stored_commit})
        before = dict(await db.run_sync(get_migration_metadata))
    monkeypatch.setattr(config, "APP_GIT_COMMIT", running_commit)
    monkeypatch.setattr(config, "DATABASE_REQUIRE_MATCHING_COMMIT", require_matching_commit)
    monkeypatch.setattr(config, "DATABASE_ALLOW_COMMIT_UPDATE", allow_commit_update)
    accepted = not require_matching_commit or (running_commit is not None and stored_commit == running_commit)
    if accepted:
        await asyncio.to_thread(db_update.init_and_update_db)
    else:
        with pytest.raises(RuntimeError, match="DATABASE_REQUIRE_MATCHING_COMMIT=True"):
            await asyncio.to_thread(db_update.init_and_update_db)
    async with isolated_database.engine.connect() as db:
        after = dict(await db.run_sync(get_migration_metadata))
        assert after == {**before, "git_commit": running_commit if accepted and allow_commit_update else stored_commit}


@pytest.mark.asyncio
@pytest.mark.parametrize("running_commit", [COMMIT, None], ids=["known-code", "unknown-code"])
async def test_strict_commit_initialization(isolated_database, migration_settings, monkeypatch, running_commit):
    monkeypatch.setattr(config, "DATABASE_REQUIRE_MATCHING_COMMIT", True)
    monkeypatch.setattr(config, "APP_GIT_COMMIT", running_commit)
    if running_commit is None:
        with pytest.raises(RuntimeError, match="requires a known running Git commit"):
            await asyncio.to_thread(db_update.init_and_update_db)
        assert await db_update.get_db_state() == ("empty", None)
    else:
        await asyncio.to_thread(db_update.init_and_update_db)
        async with isolated_database.engine.connect() as db:
            assert (await db.run_sync(get_migration_metadata))["git_commit"] == running_commit


@pytest.mark.asyncio
@pytest.mark.parametrize("direct_alembic", [False, True], ids=["startup", "alembic"])
async def test_strict_commit_check_precedes_migration(legacy_database, monkeypatch, direct_alembic):
    monkeypatch.setattr(config, "DATABASE_REQUIRE_MATCHING_COMMIT", True)
    with pytest.raises(RuntimeError, match="Database Git commit unknown differs"):
        if direct_alembic:
            await migrate(legacy_database.engine, "head")
        else:
            await asyncio.to_thread(db_update.init_and_update_db)
    async with legacy_database.engine.connect() as db:
        assert await db.scalar(text("SELECT version_num FROM alembic_version")) == OLD_REVISION
        assert await db.run_sync(get_migration_metadata) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("require_matching_schema", [False, True])
@pytest.mark.parametrize("allow_schema_update", [False, True])
async def test_schema_guard_precedes_updates(legacy_database, monkeypatch,
                                            require_matching_schema, allow_schema_update):
    monkeypatch.setattr(config, "DATABASE_REQUIRE_MATCHING_SCHEMA", require_matching_schema)
    monkeypatch.setattr(config, "DATABASE_ALLOW_SCHEMA_UPDATE", allow_schema_update)
    if require_matching_schema:
        with pytest.raises(RuntimeError, match="DATABASE_REQUIRE_MATCHING_SCHEMA=True"):
            await asyncio.to_thread(db_update.init_and_update_db)
    else:
        await asyncio.to_thread(db_update.init_and_update_db)
    async with legacy_database.engine.connect() as db:
        expected = db_update.get_latest_alembic_revision() if allow_schema_update and not require_matching_schema else OLD_REVISION
        assert await db.scalar(text("SELECT version_num FROM alembic_version")) == expected
        assert await db.scalar(text("SELECT payload FROM migration_probe")) == "Příliš žluťoučký kůň"


@pytest.mark.asyncio
async def test_migration_preserves_commit_when_updates_disabled(legacy_database, monkeypatch):
    monkeypatch.setattr(config, "DATABASE_ALLOW_COMMIT_UPDATE", False)
    await asyncio.to_thread(db_update.init_and_update_db)
    async with legacy_database.engine.connect() as db:
        metadata = dict(await db.run_sync(get_migration_metadata))
        assert metadata["git_commit"] is None
        assert metadata["migrated_at"].tzinfo is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["unversioned", "unknown"])
async def test_disabled_schema_policy_preserves_unchecked_database(isolated_database, migration_settings, monkeypatch, state):
    if state == "unknown":
        await migrate(isolated_database.engine, "head")
        async with isolated_database.engine.begin() as db:
            await db.execute(text("UPDATE alembic_version SET version_num = 'future_revision'"))
    else:
        async with isolated_database.engine.begin() as db:
            await db.execute(text("CREATE TABLE existing_data (id integer)"))
    monkeypatch.setattr(config, "DATABASE_REQUIRE_MATCHING_SCHEMA", False)
    monkeypatch.setattr(config, "DATABASE_ALLOW_SCHEMA_UPDATE", False)
    monkeypatch.setattr(config, "DATABASE_ALLOW_COMMIT_UPDATE", False)
    await asyncio.to_thread(db_update.init_and_update_db)
    assert await db_update.get_db_state() == (("versioned", "future_revision") if state == "unknown" else ("no_alembic_table", None))


@pytest.mark.asyncio
async def test_fresh_database_initializes_without_update_permissions(isolated_database, migration_settings, monkeypatch):
    monkeypatch.setattr(config, "DATABASE_REQUIRE_MATCHING_SCHEMA", True)
    monkeypatch.setattr(config, "DATABASE_ALLOW_SCHEMA_UPDATE", False)
    monkeypatch.setattr(config, "DATABASE_REQUIRE_MATCHING_COMMIT", True)
    monkeypatch.setattr(config, "DATABASE_ALLOW_COMMIT_UPDATE", False)
    await asyncio.to_thread(db_update.init_and_update_db)
    async with isolated_database.engine.connect() as db:
        before = dict(await db.run_sync(get_migration_metadata))
        assert before["git_commit"] == COMMIT
        assert before["migrated_at"].tzinfo is not None
    # The identity seeded during initialization must also pass strict restarts.
    await asyncio.to_thread(db_update.init_and_update_db)
    async with isolated_database.engine.connect() as db:
        assert dict(await db.run_sync(get_migration_metadata)) == before
