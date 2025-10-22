if __name__ == "__main__":
    import uvicorn
    from doc_api.api.config import config

    uvicorn.run("api.main:app",
                host=config.APP_HOST,
                port=config.APP_PORT,
                reload=True,
                log_config=config.LOGGING_CONFIG)
