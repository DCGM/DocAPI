import asyncio
import logging
from pathlib import Path
from typing import Optional, Tuple

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from alembic.util.exc import CommandError

from sqlalchemy import text, NullPool
from sqlalchemy.ext.asyncio import create_async_engine

from doc_api.config import config
from doc_api.db.migration_metadata import MIGRATION_LOCK, record_running_commit, validate_database_commit

logger = logging.getLogger(__name__)


def init_and_update_db():
    asyncio.run(_init_and_update_db())


async def _init_and_update_db():
    engine = create_async_engine(config.DATABASE_URL, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT pg_advisory_lock(:key)"), {"key": MIGRATION_LOCK})
            await connection.commit()
            # NullPool closes this physical connection on exit, releasing the session
            # lock even if migration fails. Keep it through the entire upgrade.
            state, revision = await get_db_state(connection)
            latest_revision = get_latest_alembic_revision()
            new_database = state == "empty"
            schema_matches = state == "versioned" and revision == latest_revision

            # Guards reject the original state. Update permissions never bypass them.
            if not new_database and config.DATABASE_REQUIRE_MATCHING_SCHEMA and not schema_matches:
                validate_schema_match(state, revision, latest_revision)
            await connection.run_sync(lambda sync_connection: validate_database_commit(
                sync_connection, config.APP_GIT_COMMIT, config.DATABASE_REQUIRE_MATCHING_COMMIT,
                new_database=new_database))

            migrate = new_database or (config.DATABASE_ALLOW_SCHEMA_UPDATE and not schema_matches)
            if migrate and not new_database:
                if state != "versioned":
                    raise RuntimeError("Database is unversioned; automatic schema updates require an Alembic revision.")
                validate_upgrade_path(revision, latest_revision)

            await connection.commit()  # Finish metadata reads before starting the update transaction.
            async with connection.begin():
                if migrate:
                    if new_database:
                        logger.info("Initializing database schema.")
                    else:
                        logger.info("Upgrading database schema from %s to %s.", revision, latest_revision)
                    # Alembic's metadata update and all schema changes share this transaction.
                    await connection.run_sync(lambda sync_connection: run_alembic_upgrade(
                        config.DATABASE_URL, connection=sync_connection))
                else:
                    logger.info("Database schema is up to date." if schema_matches else
                                "Database schema guard and automatic updates are disabled; leaving the schema unchanged.")
                    await connection.run_sync(lambda sync_connection: record_running_commit(
                        sync_connection, config.APP_GIT_COMMIT, config.DATABASE_REQUIRE_MATCHING_COMMIT,
                        config.DATABASE_ALLOW_COMMIT_UPDATE))
    finally:
        await engine.dispose()


def validate_schema_match(state: str, revision: str | None, latest_revision: str) -> None:
    """Explain why the current database cannot pass the startup schema guard."""
    if state != "versioned":
        raise RuntimeError(
            "Database exists but is unversioned (missing Alembic version information) and "
            "DATABASE_REQUIRE_MATCHING_SCHEMA=True. Restore a versioned database or disable "
            "the schema guard and automatic schema updates to use it unchanged."
        )
    validate_upgrade_path(revision, latest_revision)
    raise RuntimeError(
        f"Database schema revision {revision} differs from this DocAPI build's expected revision "
        f"{latest_revision} and DATABASE_REQUIRE_MATCHING_SCHEMA=True. "
        "The schema guard takes precedence over DATABASE_ALLOW_SCHEMA_UPDATE. "
        "Migrate manually, or disable the schema guard and enable automatic schema updates."
    )


def validate_upgrade_path(revision: str, latest_revision: str) -> None:
    """Reject incompatible schemas before considering automatic upgrades.

    Revision identifiers are opaque, so an unknown revision cannot reliably be
    distinguished from a newer schema or one created on another branch.
    """
    script = ScriptDirectory.from_config(get_alembic_cfg())
    try:
        script.get_revision(revision)
    except CommandError as exc:
        raise RuntimeError(
            f"Database schema revision {revision} is not recognized by this DocAPI build "
            f"(expected {latest_revision}). The database may have been migrated by a newer "
            "DocAPI version or a different migration branch. Run the matching DocAPI image "
            "or restore a compatible database backup. DATABASE_ALLOW_SCHEMA_UPDATE cannot fix this mismatch."
        ) from exc

    ancestors = {migration.revision for migration in script.iterate_revisions(latest_revision, "base")}
    if revision in ancestors:
        return
    database_ancestors = {migration.revision for migration in script.iterate_revisions(revision, "base")}
    reason = ("is newer than this DocAPI build supports" if latest_revision in database_ancestors
              else "belongs to an incompatible migration branch")
    raise RuntimeError(
        f"Database schema revision {revision} {reason} (expected {latest_revision}). "
        "Run the matching DocAPI image or restore a compatible database backup. "
        "DATABASE_ALLOW_SCHEMA_UPDATE only permits upgrades; automatic downgrades are not supported."
    )


async def get_db_state(connection=None) -> Tuple[str, Optional[str]]:
    if connection is not None:
        tables = list(await connection.scalars(text(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'")))
        if not tables:
            return "empty", None
        if "alembic_version" not in tables:
            return "no_alembic_table", None
        revision = await connection.scalar(text("SELECT version_num FROM alembic_version"))
        return ("versioned", revision) if revision else ("no_alembic_version", None)

    engine = create_async_engine(config.DATABASE_URL, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            return await get_db_state(connection)
    finally:
        await engine.dispose()


def run_alembic_upgrade(db_url: str, *, connection=None):
    cfg = get_alembic_cfg()
    cfg.set_main_option("sqlalchemy.url", db_url.replace("%", "%%"))
    if connection is not None:
        cfg.attributes["connection"] = connection
    command.upgrade(cfg, "head")


def get_latest_alembic_revision() -> str:
    script = ScriptDirectory.from_config(get_alembic_cfg())
    return script.get_current_head()


def get_alembic_cfg():
    root = Path(__file__).resolve().parents[1]
    return Config(str(root / "alembic.ini"))
