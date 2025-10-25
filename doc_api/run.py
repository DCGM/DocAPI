
if __name__ == "__main__":
    import logging.config
    from doc_api.config import config
    logging.config.dictConfig(config.LOGGING_CONFIG)

    logger = logging.getLogger(__name__)

    import asyncio
    import uvicorn

    from doc_api.db.db_create import create_database_if_does_not_exist
    from doc_api.db.db_update import init_and_update_db

    if not config.DATABASE_FORCE:
        asyncio.run(create_database_if_does_not_exist())
        init_and_update_db()
    else:
        logger.warning("Skipping creating DB and alembic upgrade due to DB_FORCE=True. "
                       "Assuming the database exist and the schema is up to date.")

    logger.info(f"Running DocAPI on {config.APP_HOST}:{config.APP_PORT} (production={config.PRODUCTION})")

    uvicorn.run("api.main:app",
                host=config.APP_HOST,
                port=config.APP_PORT,
                reload=not config.PRODUCTION,
                log_config=config.LOGGING_CONFIG)
