
if __name__ == "__main__":
    import logging.config
    from doc_api.config import config
    logging.config.dictConfig(config.LOGGING_CONFIG)

    logger = logging.getLogger(__name__)

    import asyncio
    import uvicorn

    from alembic import command
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    from doc_api.db.db_create import create_database_if_does_not_exist
    from doc_api.db.db_update import get_db_state, init_and_update_db


    def run_alembic_upgrade(db_url: str):
        cfg = Config("alembic.ini")
        cfg.set_main_option("sqlalchemy.url", db_url)
        command.upgrade(cfg, "head")

    def get_latest_alembic_revision() -> str:
        cfg = Config("alembic.ini")
        script = ScriptDirectory.from_config(cfg)
        return script.get_current_head()

    if not config.DATABASE_FORCE:
        asyncio.run(create_database_if_does_not_exist())
        init_and_update_db()
    else:
        logger.warning("Skipping creating DB and alembic upgrade due to DB_FORCE=True. "
                       "Assuming the database exist and the schema is up to date.")

    uvicorn.run("api.main:app",
                host=config.APP_HOST,
                port=config.APP_PORT,
                reload=not config.PRODUCTION,
                log_config=config.LOGGING_CONFIG)
