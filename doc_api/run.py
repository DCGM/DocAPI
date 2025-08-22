if __name__ == "__main__":
    import uvicorn
    from metakat.app.config import config

    uvicorn.run("api.main:app", host="0.0.0.0", port=8088, reload=True, log_config=config.LOGGING_CONFIG)
