import logging
import logging.config
import traceback

import fastapi
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.utils import get_openapi

from starlette.exceptions import HTTPException as StarletteHTTPException
from fastapi import HTTPException as FastAPIHTTPException

from sqlalchemy import select

from doc_api.api.authentication import hmac_sha256_hex
from doc_api.api.schemas.base_objects import KeyRole
from doc_api.api.database import open_session
from doc_api.api.routes import user_router, worker_router, admin_router, debug_router
from doc_api.api.schemas.responses import AppCode, validate_server_error_response, DocAPIResponseServerError, \
    DocAPIResponseClientError, DocAPIClientErrorException, validate_client_error_response, \
    DETAILS_GENERAL
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
    },
    {
        "name": "Debug",
        "description": "Debugging endpoints (admin only).",
    },
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
app.include_router(debug_router, prefix="/api/debug")


@app.exception_handler(DocAPIClientErrorException)
async def api_client_error_handler(_: Request, exc: DocAPIClientErrorException):
    payload = DocAPIResponseClientError(
        status=exc.status,
        code=exc.code,
        detail=exc.detail
    )
    return validate_client_error_response(payload, headers=exc.headers)

@app.exception_handler(StarletteHTTPException)
async def http_exc_handler(_: Request, exc: StarletteHTTPException):
    payload = DocAPIResponseClientError(
        status=exc.status_code,
        code=AppCode.HTTP_ERROR,
        detail=exc.detail if exc.detail else DETAILS_GENERAL[AppCode.HTTP_ERROR]
    )
    return validate_client_error_response(payload, headers=exc.headers)

@app.exception_handler(RequestValidationError)
async def validation_handler(_: Request, exc: RequestValidationError):
    payload = DocAPIResponseClientError(
        status=fastapi.status.HTTP_422_UNPROCESSABLE_CONTENT,
        code=AppCode.REQUEST_VALIDATION_ERROR,
        detail=DETAILS_GENERAL[AppCode.REQUEST_VALIDATION_ERROR],
        details=exc.errors()
    )
    return validate_client_error_response(payload)

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
    return validate_server_error_response(DocAPIResponseServerError(
        status=fastapi.status.HTTP_500_INTERNAL_SERVER_ERROR,
        code=AppCode.INTERNAL_ERROR,
        detail=DETAILS_GENERAL[AppCode.INTERNAL_ERROR]
    ))

# override OpenAPI generation to include 422 response with our error envelope
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema

    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )

    # Ensure component container exists
    components = schema.setdefault("components", {})
    comp_schemas = components.setdefault("schemas", {})

    # Register DocAPIResponseClientError + its nested defs as components
    error_schema_full = DocAPIResponseClientError.model_json_schema(
        ref_template="#/components/schemas/{model}"
    )
    for name, sub_schema in error_schema_full.get("$defs", {}).items():
        comp_schemas.setdefault(name, sub_schema)

    comp_schemas.setdefault(
        "DocAPIResponseClientError",
        {k: v for k, v in error_schema_full.items() if k != "$defs"}
    )

    # Replace ONLY the JSON schema of existing 422 responses
    for path, ops in schema.get("paths", {}).items():
        for method, op in ops.items():
            # Skip non-operations block like "parameters"
            if not isinstance(op, dict) or method.lower() not in {"get", "post", "put", "patch", "delete", "options", "head", "trace"}:
                continue

            responses = op.get("responses", {})
            resp_422 = responses.get("422")
            if not resp_422:
                continue  # do not add new 422s, only replace existing ones

            content = resp_422.setdefault("content", {})
            app_json = content.setdefault("application/json", {})

            # Preserve an existing description if present
            resp_422.setdefault("description", "Request validation failed.")

            # Preserve other content-types (e.g., text/plain) by only touching JSON
            existing_examples = app_json.get("examples", {})

            # Point to the component schema
            app_json["schema"] = {"$ref": "#/components/schemas/DocAPIResponseClientError"}

            # Merge/ensure an example without overwriting existing ones
            example_key = "default"
            if example_key not in existing_examples:
                existing_examples[example_key] = {
                    "summary": AppCode.REQUEST_VALIDATION_ERROR.value,
                    "value": DocAPIResponseClientError(
                    status=422,
                    code=AppCode.REQUEST_VALIDATION_ERROR,
                    detail="Request validation failed.",
                    details=[
                        {
                            "loc": ["body", "field_name"],
                            "msg": "field required",
                            "type": "value_error.missing",
                        },
                        {
                            "loc": ["query", "limit"],
                            "msg": "value is not a valid integer",
                            "type": "type_error.integer",
                        },
                    ],
                ).model_dump(mode="json", exclude_none=True),
                }
            app_json["examples"] = existing_examples

    app.openapi_schema = schema
    return app.openapi_schema

app.openapi = custom_openapi