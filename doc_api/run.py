if __name__ == "__main__":
    import uvicorn
    from doc_api.config import config

    uvicorn.run("api.main:app",
                host=config.APP_HOST,
                port=config.APP_PORT,
                reload=not config.PRODUCTION,
                log_config=config.LOGGING_CONFIG)
