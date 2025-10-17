import logging
import logging.config
import traceback
from http import HTTPStatus

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select

from doc_api.api.authentication import hmac_sha256_hex
from doc_api.api.schemas.base_objects import KeyRole
from doc_api.api.database import get_async_session, open_session
from doc_api.api.routes import user_router, worker_router, admin_router
from doc_api.api.schemas.responses import DocAPIResponse, AppCode
from doc_api.config import config
from doc_api.tools.mail.mail_logger import get_internal_mail_logger
from doc_api.db import model


exception_logger = logging.getLogger('doc_api.exception_logger')
exception_logger.propagate = False


logger = logging.getLogger(__name__)
internal_mail_logger = get_internal_mail_logger().logger


tags_metadata = [
    {
        "name": "User",
        "description": "",
    },
    {
        "name": "Worker",
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
        async with open_session() as db:
            result = await db.execute(select(model.Key).where(model.Key.key_hash == digest))
            key = result.scalar_one_or_none()
            if key is None:
                db.add(model.Key(
                    key_hash=digest,
                    label="admin",
                    active=True,
                    role=KeyRole.ADMIN
                ))
                await db.commit()
                logger.info("Admin API key created!")
    else:
        logger.warning("ADMIN_KEY is not set! No admin API key created! (this is OK if there is another admin key in the database)")

app.include_router(user_router, prefix="/api/user")
app.include_router(worker_router, prefix="/api/worker")
app.include_router(admin_router, prefix="/api/admin")


@app.exception_handler(Exception)
async def unhandled(request: Request, exc: Exception):
    # last resort: 500 with generic app code, exact info logged (optionally emailed to admins)
    if config.INTERNAL_MAIL_SERVER is not None:
        internal_mail_logger.critical(f'URL: {request.url}\n'
                                      f'METHOD: {request.method}\n'
                                      f'CLIENT: {request.client}\n\n'
                                      f'ERROR: {exc}\n\n'
                                      f'{traceback.format_exc()}',
                                      extra={'subject': f'{config.ADMIN_SERVER_NAME} - INTERNAL SERVER ERROR'})
    exception_logger.error(f'URL: {request.url}')
    exception_logger.error(f'CLIENT: {request.client}')
    exception_logger.exception(exc)
    return JSONResponse(
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
        content=DocAPIResponse(status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                               app_code=AppCode.INTERNAL_ERROR,
                               message="Internal server error").model_dump()
    )
