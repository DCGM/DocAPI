import logging
import logging.config
import traceback
from typing import Optional, List, Set

import fastapi
from fastapi import FastAPI, Request
from fastapi.dependencies.models import Dependant
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.utils import get_openapi
from fastapi.routing import APIRoute

from starlette.exceptions import HTTPException as StarletteHTTPException

from sqlalchemy import select

from doc_api.api.authentication import hmac_sha256_hex, AUTHENTICATION_RESPONSES
from doc_api.api.routes.user_guards import USER_ACCESS_TO_NEW_JOB_GUARD_RESPONSES, USER_ACCESS_TO_JOB_GUARD_RESPONSES
from doc_api.api.routes.worker_guards import WORKER_ACCESS_TO_JOB_GUARD_RESPONSES, WORKER_ACCESS_TO_PROCESSING_JOB_GUARD_RESPONSES
from doc_api.api.schemas.base_objects import KeyRole
from doc_api.api.database import open_session
from doc_api.api.routes import admin_router, debug_router, root_router
from doc_api.api.schemas.responses import AppCode, validate_server_error_response, DocAPIResponseServerError, \
    DocAPIResponseClientError, DocAPIClientErrorException, validate_client_error_response, \
    DETAILS_GENERAL, make_responses
from doc_api.api.config import config
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
        "description": "",
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


app.include_router(root_router)
app.include_router(admin_router, prefix="/v1/admin")
app.include_router(debug_router, prefix="/debug")


@app.exception_handler(DocAPIClientErrorException)
async def api_client_error_handler(_: Request, exc: DocAPIClientErrorException):
    payload = DocAPIResponseClientError(
        status=exc.status,
        code=exc.code,
        detail=exc.detail,
        details=exc.details
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

VALIDATION_RESPONSE = {
    AppCode.REQUEST_VALIDATION_ERROR : {
        "status": fastapi.status.HTTP_422_UNPROCESSABLE_CONTENT,
        "description": "Request validation failed.",
        "model": DocAPIResponseClientError,
        "detail": "The request parameters did not pass validation.",
        "details": [
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
        ]
    }
}
@app.exception_handler(RequestValidationError)
async def validation_handler(_: Request, exc: RequestValidationError):
    payload = DocAPIResponseClientError(
        status=fastapi.status.HTTP_422_UNPROCESSABLE_CONTENT,
        code=AppCode.REQUEST_VALIDATION_ERROR,
        detail=VALIDATION_RESPONSE[AppCode.REQUEST_VALIDATION_ERROR]["detail"],
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



# --- OpenAPI customization --- inject guard docs, validation docs, roles docs ---
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema

    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes
    )

    # --- worker processing job guard ---
    def _route_challenge_worker_access_to_processing_job_job(route: APIRoute) -> bool:
        return bool(getattr(route.endpoint, "__challenge_worker_access_to_processing_job__", False))

    inject_docs(
        app=app,
        schema=schema,
        route_predicate=_route_challenge_worker_access_to_processing_job_job,
        route_responses=WORKER_ACCESS_TO_PROCESSING_JOB_GUARD_RESPONSES,
    )

    # --- worker job guard ---
    def _route_challenge_worker_access_to_job(route: APIRoute) -> bool:
        return bool(getattr(route.endpoint, "__challenge_worker_access_to_job__", False))

    inject_docs(
        app=app,
        schema=schema,
        route_predicate=_route_challenge_worker_access_to_job,
        route_responses=WORKER_ACCESS_TO_JOB_GUARD_RESPONSES,
    )

    # --- user new job guard ---
    def _route_challenge_user_access_to_new_job(route: APIRoute) -> bool:
        return bool(getattr(route.endpoint, "__challenge_user_access_to_new_job__", False))

    inject_docs(
        app=app,
        schema=schema,
        route_predicate=_route_challenge_user_access_to_new_job,
        route_responses=USER_ACCESS_TO_NEW_JOB_GUARD_RESPONSES,
    )

    # --- user job guard ---
    def _route_challenge_user_access_to_job(route: APIRoute) -> bool:
        return bool(getattr(route.endpoint, "__challenge_user_access_to_job__", False))

    inject_docs(
        app=app,
        schema=schema,
        route_predicate=_route_challenge_user_access_to_job,
        route_responses=USER_ACCESS_TO_JOB_GUARD_RESPONSES,
    )

    # --- api key authentication ---
    def _route_uses_require_api_key(route: APIRoute) -> bool:
        dep = getattr(route, "dependant", None)
        if dep is None:
            return False
        for d in _iter_dependants(dep):
            call = getattr(d, "call", None)
            if call is not None and getattr(call, "__require_api_key__", False):
                return True
        return False

    inject_docs(
        app=app,
        schema=schema,
        route_predicate=_route_uses_require_api_key,
        route_responses=AUTHENTICATION_RESPONSES,
    )

    # --- validation 422 ---
    inject_validation_422_docs(
        schema=schema,
        validation_response=VALIDATION_RESPONSE,
    )

    # --- roles ---
    inject_roles_docs(app=app, schema=schema)

    # copy x-order into operationId prefix
    for path, item in schema.get("paths", {}).items():
        for method, op in list(item.items()):
            x = op.get("x-order")
            oid = op.get("operationId")
            if oid is not None:
                prefix = f"{int(x):04d}-" if isinstance(x, int) else "9999-"
                op["operationId"] = prefix + oid

    app.openapi_schema = schema
    return app.openapi_schema



app.openapi = custom_openapi


def inject_docs(*, app, schema: dict, route_predicate, route_responses: dict):
    """
    Inject shared guard responses into Swagger (OpenAPI) for all operations
    whose routes satisfy `guard_predicate(route)`.

    - `guard_responses`: your {AppCode: {status, description, model, ...}} map.
    - Uses make_responses(..., inject_schema=True) to produce OpenAPI shards.
    - Merges without overwriting route-specific docs (schema/description/examples).
    """
    # Build fully rendered OpenAPI response fragments for the guard
    rendered = make_responses(route_responses, inject_schema=True)

    # Determine which (path, METHOD) operations are guarded
    guarded_ops = set()
    for r in app.routes:
        if isinstance(r, APIRoute) and r.include_in_schema and _route_uses_guard(r, route_predicate):
            for m in (r.methods or []):
                guarded_ops.add((r.path, m.upper()))

    # Status codes present in the rendered guard responses
    status_codes = list(rendered.keys())  # ints

    # Merge into the OpenAPI schema
    for path, ops in schema.get("paths", {}).items():
        for method, op in ops.items():
            if not isinstance(op, dict) or (path, method.upper()) not in guarded_ops:
                continue

            responses = op.setdefault("responses", {})

            for status in status_codes:
                guard_src = rendered.get(status)
                if not guard_src:
                    continue

                key = str(status)
                dest_resp = responses.setdefault(key, {})

                # Description: only set if missing
                if "description" in guard_src:
                    dest_resp.setdefault("description", guard_src["description"])

                # Content: merge per content-type; keep existing, add missing
                dest_content = dest_resp.setdefault("content", {})
                for ctype, src_payload in (guard_src.get("content") or {}).items():
                    dst_payload = dest_content.setdefault(ctype, {})

                    # Schema: only if not already provided by the route
                    if "schema" in src_payload and "schema" not in dst_payload:
                        dst_payload["schema"] = src_payload["schema"]

                    # Examples: merge without overwriting existing example keys
                    src_examples = src_payload.get("examples") or {}
                    if src_examples:
                        dst_examples = (dst_payload.get("examples") or {}).copy()
                        for ex_key, ex_val in src_examples.items():
                            dst_examples.setdefault(ex_key, ex_val)
                        if dst_examples:
                            dst_payload["examples"] = dst_examples

# --- generic helper: does this route use a given guard? ---
def _route_uses_guard(route: APIRoute, predicate) -> bool:
    try:
        return bool(predicate(route))
    except Exception:
        return False


def inject_validation_422_docs(*, schema: dict, validation_response: dict):
    """
    Inject standardized 422 Validation Error documentation into OpenAPI schema.

    - If no 422 present → add the given validation response.
    - If 422 exists with FastAPI's default schema (HTTPValidationError/ValidationError) → replace it.
    - If 422 exists and is custom (non-default) → append the given validation examples/content
      *after* existing ones, without overwriting anything.
    """
    _validation_responses = make_responses(validation_response, inject_schema=True)
    _validation_422 = _validation_responses.get(fastapi.status.HTTP_422_UNPROCESSABLE_ENTITY)
    if not _validation_422:
        return

    valid_methods = {"get", "post", "put", "patch", "delete", "options", "head", "trace"}
    default_refs = {
        "#/components/schemas/HTTPValidationError",
        "#/components/schemas/ValidationError",
    }

    for path, ops in schema.get("paths", {}).items():
        for method, op in ops.items():
            if not isinstance(op, dict) or method.lower() not in valid_methods:
                continue

            responses = op.setdefault("responses", {})

            if "422" not in responses:
                # No existing 422 → add ours
                responses["422"] = dict(_validation_422)
                continue

            existing_422 = responses["422"]
            app_json = (
                existing_422
                .get("content", {})
                .get("application/json", {})
            )
            schema_ = app_json.get("schema", {})

            # Detect FastAPI's default $ref
            ref = None
            if isinstance(schema_, dict):
                if "$ref" in schema_:
                    ref = schema_["$ref"]
                else:
                    for kw in ("oneOf", "anyOf", "allOf"):
                        seq = schema_.get(kw)
                        if isinstance(seq, list):
                            for item in seq:
                                if isinstance(item, dict) and "$ref" in item:
                                    ref = item["$ref"]
                                    break
                            if ref:
                                break

            if ref in default_refs:
                # Replace FastAPI default with our standardized response
                responses["422"] = dict(_validation_422)
                continue

            # Custom 422 present → append our examples/content
            dest_resp = responses.setdefault("422", {})
            dest_content = dest_resp.setdefault("content", {})

            src_app_json = (
                _validation_422
                .get("content", {})
                .get("application/json", {})
            )

            # Add or merge into existing application/json content
            if "application/json" not in dest_content:
                dest_content["application/json"] = dict(src_app_json)
            else:
                dst_payload = dest_content["application/json"]

                # Keep existing schema; only add ours if missing
                if "schema" not in dst_payload and "schema" in src_app_json:
                    dst_payload["schema"] = src_app_json["schema"]

                # Append examples: keep existing first, then ours (no overwrites)
                src_examples = (src_app_json.get("examples") or {})
                dst_examples = (dst_payload.get("examples") or {})

                if src_examples or dst_examples:
                    merged = {}
                    # existing first
                    for k, v in dst_examples.items():
                        merged[k] = v
                    # then ours (don't overwrite)
                    for k, v in src_examples.items():
                        if k not in merged:
                            merged[k] = v
                    dst_payload["examples"] = merged

            # Add description if missing
            if "description" in _validation_422:
                dest_resp.setdefault("description", _validation_422["description"])


def inject_roles_docs(*, app, schema: dict, ext_key: str = "x-roles-allowed"):
    """Attach machine-readable roles and append a human-readable line to descriptions."""
    route_roles = {}
    for r in app.routes:
        if isinstance(r, APIRoute) and r.include_in_schema:
            roles = _collect_roles_from_route(r)
            if roles:
                for m in (r.methods or []):
                    route_roles[(r.path, m.upper())] = roles

    for path, ops in schema.get("paths", {}).items():
        for method, op in ops.items():
            if not isinstance(op, dict):
                continue
            roles = route_roles.get((path, method.upper()))
            if not roles:
                continue
            op[ext_key] = roles
            desc = op.get("description") or ""
            line = f"**Allowed roles:** {', '.join(roles)}"
            if line not in desc:
                op["description"] = (desc + ("\n\n" if desc else "") + line)

def _collect_roles_from_route(route: APIRoute) -> Optional[List[str]]:
    """
    Collect roles from any dependency marked with:
      __require_api_key__ = True
      __require_api_key_roles__ = tuple[KeyRole | str]
    Returns a sorted list of unique role strings, or None if not guarded.
    """
    roles: Set[str] = set()

    # Endpoint-level dependency tree
    dep = getattr(route, "dependant", None)
    if dep is not None:
        for d in _iter_dependants(dep):  # uses your existing helper
            call = getattr(d, "call", None)
            if call is not None and getattr(call, "__require_api_key__", False):
                roles |= _roles_to_strings(getattr(call, "__require_api_key_roles__", ()))

    # Router-level dependencies (fallback)
    if not roles:
        for dep in getattr(route, "dependencies", []) or []:
            call = getattr(dep, "dependency", None)
            if call is not None and getattr(call, "__require_api_key__", False):
                roles |= _roles_to_strings(getattr(call, "__require_api_key_roles__", ()))

    return sorted(roles) if roles else None

def _roles_to_strings(items) -> Set[str]:
    out: Set[str] = set()
    for r in (items or ()):
        out.add(r.value if isinstance(r, KeyRole) else str(r))
    return out

def _iter_dependants(dep: Dependant):
    yield dep
    for child in dep.dependencies or ():
        yield from _iter_dependants(child)