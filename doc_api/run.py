
def main():
    from doc_api.config import config
    config.create_dirs()

    import logging.config
    logging.config.dictConfig(config.LOGGING_CONFIG)

    logger = logging.getLogger(__name__)

    import asyncio
    import uvicorn

    from doc_api.db.db_create import create_database_if_does_not_exist
    from doc_api.db.db_update import init_and_update_db

    if any((config.DATABASE_REQUIRE_MATCHING_SCHEMA, config.DATABASE_ALLOW_SCHEMA_UPDATE,
            config.DATABASE_REQUIRE_MATCHING_COMMIT, config.DATABASE_ALLOW_COMMIT_UPDATE)):
        asyncio.run(create_database_if_does_not_exist())
        init_and_update_db()
    else:
        logger.warning("Database guards and automatic updates are disabled; skipping database preparation.")

    logger.info(f"Running DocAPI on {config.APP_HOST}:{config.APP_PORT} (production={config.PRODUCTION})")

    uvicorn.run("doc_api.api.main:app",
                host=config.APP_HOST,
                port=config.APP_PORT,
                reload=not config.PRODUCTION,
                log_config=config.LOGGING_CONFIG)


if __name__ == "__main__":
    exit(main())
