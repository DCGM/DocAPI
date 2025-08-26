import logging
import logging.config
import traceback

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from sqlalchemy import select

from app.api.authentication import hmac_sha256_hex
from metakat.app.api.database import DBError, get_async_session
from metakat.app.api.routes import router
from metakat.app.config import config
from metakat.app.tools.mail.mail_logger import get_internal_mail_logger
from metakat.app.db import model


exception_logger = logging.getLogger('metakat.exception_logger')
exception_logger.propagate = False


logger = logging.getLogger(__name__)
internal_mail_logger = get_internal_mail_logger().logger


tags_metadata = [
    {
        "name": "User",
        "description": "",
    },
    {
        "name": "Admin",
        "description": "",
    }
]


app = FastAPI(openapi_tags=tags_metadata,
              title=config.SERVER_NAME,
              version=config.SOFTWARE_VERSION,
              root_path=config.APP_URL_ROOT)


@app.on_event("startup")
async def startup():
    logging.config.dictConfig(config.LOGGING_CONFIG)
    if getattr(config, "ADMIN_KEY", None):
        digest = hmac_sha256_hex(config.ADMIN_KEY)
        async for db in get_async_session():
            result = await db.execute(select(model.Key).where(model.Key.key_hash == digest))
            key = result.scalar_one_or_none()
            if key is None:
                db.add(model.Key(
                    key_hash=digest,
                    label="admin",
                    active=True,
                    admin=True
                ))
                await db.commit()
    else:
        logger.warning("ADMIN_KEY is not set! No admin API key created! (this is OK if there is another admin key in the database)")

app.include_router(router, prefix="/api")

# if os.path.isdir("metakat/static"):
#     app.mount("/", StaticFiles(directory="metakat/static", html=True), name="static")


@app.exception_handler(Exception)
async def unicorn_exception_handler(request: Request, exc: Exception):
    if config.INTERNAL_MAIL_SERVER is not None:
        internal_mail_logger.critical(f'URL: {request.url}\n'
                                      f'METHOD: {request.method}\n'
                                      f'CLIENT: {request.client}\n\n'
                                      f'ERROR: {exc}\n\n'
                                      f'{traceback.format_exc()}',
                                      extra={'subject': f'{config.MODE.upper()} {config.ADMIN_SERVER_NAME} - INTERNAL SERVER ERROR'})
    exception_logger.error(f'URL: {request.url}')
    exception_logger.error(f'CLIENT: {request.client}')
    exception_logger.exception(exc)
    if isinstance(exc, DBError):
        if exc.code is not None:
            return JSONResponse(
                status_code=exc.status_code,
                content={"code": exc.code, "message": str(exc)},
            )
        else:
            return JSONResponse(
                status_code=exc.status_code,
                content={"message": str(exc)},
            )
    else:
        return Response(status_code=500)


# if config.PRODUCTION:
#     logging.warning(f'PRODUCTION')
# else:
#     logging.warning(f'DEVELOPMENT')
#     app.add_middleware(
#          CORSMiddleware,
#          allow_origins=["http://localhost:9000"],
#          allow_credentials=True,
#          allow_methods=["*"],
#          allow_headers=["*"],
#      )
