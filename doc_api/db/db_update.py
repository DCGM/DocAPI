import asyncio
import logging
from typing import Optional, Tuple

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory

from sqlalchemy import text, NullPool
from sqlalchemy.ext.asyncio import create_async_engine

from doc_api.config import config

logger = logging.getLogger(__name__)


def init_and_update_db():
    state, alembic_version = asyncio.run(get_db_state())
    if state == "no_alembic_table":
        raise RuntimeError("Database exists but is unversioned (missing alembic_version table). "
                           "If you want to force your database, use DATABASE_FORCE=True.")
    elif state == "no_alembic_version":
        raise RuntimeError("Database exists but is unversioned (missing version_num in alembic_version table). "
                           "If you want to force your database, use DATABASE_FORCE=True.")
    if state == "empty":
        logger.info("Database is empty, running alembic upgrade to create schema.")
        run_alembic_upgrade(config.DATABASE_URL)
    elif state == "versioned":
        latest_revision = get_latest_alembic_revision()
        if alembic_version != latest_revision:
            logger.info(
                f"Database schema is out of date -> current version: {alembic_version}, latest version: {latest_revision}.")
            if config.DATABASE_ALLOW_UPDATE and alembic_version != latest_revision:
                logger.info("Running alembic upgrade to update schema.")
                run_alembic_upgrade(config.DATABASE_URL)
            else:
                raise RuntimeError("Database schema is out of date and DATABASE_ALLOW_UPDATE=False. "
                                   "Please update the database schema manually or set DATABASE_ALLOW_UPDATE=True.")


async def get_db_state() -> Tuple[str, Optional[str]]:
    engine = create_async_engine(config.DATABASE_URL, poolclass=NullPool)
    async with engine.connect() as conn:
        # how many user tables in public? (adjust schema if needed)
        total_tables = (
            await conn.execute(text("""
                SELECT COUNT(*) 
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            """))
        ).scalar_one()

        if total_tables == 0:
            await engine.dispose()
            return "empty", None

        # does alembic_version exist?
        alembic_tbl_exists = (
            await conn.execute(text("""
                SELECT EXISTS (
                  SELECT 1 FROM information_schema.tables
                  WHERE table_schema = 'public' AND table_name = 'alembic_version'
                )
            """))
        ).scalar_one()

        if not alembic_tbl_exists:
            await engine.dispose()
            return "no_alembic_table", None

        # read version number
        row = (await conn.execute(text("SELECT version_num FROM alembic_version"))).first()
        await engine.dispose()

        if row and row[0]:
            return "versioned", row[0]
        else:
            return "no_alembic_version", None


def run_alembic_upgrade(db_url: str):
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", db_url)
    command.upgrade(cfg, "head")


def get_latest_alembic_revision() -> str:
    cfg = Config("alembic.ini")
    script = ScriptDirectory.from_config(cfg)
    return script.get_current_head()



