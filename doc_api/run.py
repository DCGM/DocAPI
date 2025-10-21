if __name__ == "__main__":
    import uvicorn
    from doc_api.config import config

    uvicorn.run("api.main:app", host="0.0.0.0", port=8888, reload=True, log_config=config.LOGGING_CONFIG)
