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
from doc_api.api.routes.route_guards import WORKER_ACCESS_TO_JOB_GUARD_RESPONSES, USER_ACCESS_TO_JOB_GUARD_RESPONSES
from doc_api.api.schemas.base_objects import KeyRole
from doc_api.api.database import open_session
from doc_api.api.routes import user_router, worker_router, admin_router, debug_router
from doc_api.api.schemas.responses import AppCode, validate_server_error_response, DocAPIResponseServerError, \
    DocAPIResponseClientError, DocAPIClientErrorException, validate_client_error_response, \
    DETAILS_GENERAL, make_responses
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
        routes=app.routes,
    )

    # --- worker guard ---
    def _route_uses_challenge_worker_access_to_job(route: APIRoute) -> bool:
        return bool(getattr(route.endpoint, "__challenge_worker_access_to_job__", False))

    inject_docs(
        app=app,
        schema=schema,
        route_predicate=_route_uses_challenge_worker_access_to_job,
        route_responses=WORKER_ACCESS_TO_JOB_GUARD_RESPONSES,
    )

    # --- user guard ---
    def _route_uses_challenge_user_access_to_job(route: APIRoute) -> bool:
        return bool(getattr(route.endpoint, "__challenge_user_access_to_job__", False))

    inject_docs(
        app=app,
        schema=schema,
        route_predicate=_route_uses_challenge_user_access_to_job,
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

def _iter_dependants(dep: Dependant):
    yield dep
    for child in dep.dependencies or ():
        yield from _iter_dependants(child)

def inject_validation_422_docs(*, schema: dict, validation_response: dict):
    """
    Injects standardized 422 Validation Error documentation into OpenAPI schema.

    - Adds or replaces the default FastAPI 422 response with the given `validation_response`.
    - Replaces only if the existing schema $ref is one of FastAPI's built-in validation models:
      '#/components/schemas/HTTPValidationError' or '#/components/schemas/ValidationError'.
    - `validation_response` should be a dict like VALIDATION_RESPONSE (AppCode keyed).
    """

    _validation_responses = make_responses(validation_response, inject_schema=True)
    _validation_422 = _validation_responses.get(fastapi.status.HTTP_422_UNPROCESSABLE_ENTITY)

    if not _validation_422:
        return  # nothing to inject

    valid_methods = {"get", "post", "put", "patch", "delete", "options", "head", "trace"}

    for path, ops in schema.get("paths", {}).items():
        for method, op in ops.items():
            if not isinstance(op, dict) or method.lower() not in valid_methods:
                continue

            responses = op.setdefault("responses", {})

            if "422" in responses:
                # Replace only if current schema is FastAPI's default validation model
                app_json = (
                    responses["422"]
                    .get("content", {})
                    .get("application/json", {})
                )
                schema_ = app_json.get("schema", {})

                # Extract possible $ref (directly or inside oneOf/anyOf/allOf)
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

                if ref in {
                    "#/components/schemas/HTTPValidationError",
                    "#/components/schemas/ValidationError",
                }:
                    responses["422"] = dict(_validation_422)
            else:
                responses["422"] = dict(_validation_422)


def inject_roles_docs(*, app, schema: dict, ext_key: str = "x-roles-allowed"):
    """
    Injects role metadata into the OpenAPI schema.

    - Builds a (path, METHOD) -> roles map from the actual FastAPI routes using your
      existing `_collect_roles_from_route(route)` helper (must return Iterable[str] or None).
    - For each matching operation in `schema["paths"]`:
        * adds machine-readable roles under the `ext_key` (default: "x-roles-allowed"),
        * appends a human-readable "**Allowed roles:** ..." line to the description
          (without clobbering or duplicating existing text).
    """
    # Build a map (path, method) -> roles using the actual route objects
    route_roles_map = {}
    for r in app.routes:
        if isinstance(r, APIRoute) and r.include_in_schema:
            roles = _collect_roles_from_route(r)  # established helper
            if roles is None:
                continue
            for method in (r.methods or []):
                route_roles_map[(r.path, method.upper())] = roles

    # Inject the roles into descriptions and as x-roles-allowed (or custom ext_key)
    for path, ops in schema.get("paths", {}).items():
        for method, op in ops.items():
            if not isinstance(op, dict):
                continue

            roles = route_roles_map.get((path, method.upper()))
            if not roles:
                continue

            # Machine-readable extension
            op[ext_key] = roles

            # Human-readable note in description (keep, don't duplicate)
            desc = op.get("description") or ""
            line = f"**Allowed roles:** {', '.join(roles)}"
            if line not in desc:
                op["description"] = (desc + ("\n\n" if desc else "") + line)

def _extract_allowed_from_callable(call) -> Optional[Set[KeyRole]]:
    """Pull the captured `allowed` set out of the require_api_key closure."""
    clo = getattr(call, "__closure__", None)
    code = getattr(call, "__code__", None)
    if not clo or not code:
        return None
    names = code.co_freevars
    cells = [c.cell_contents for c in clo]
    env = {n: v for n, v in zip(names, cells)}
    allowed = env.get("allowed")
    if isinstance(allowed, set):
        return allowed
    return None

def _collect_roles_from_dependant(dependant) -> Optional[Set[KeyRole]]:
    """Walk the dependant graph to find any require_api_key closure and return its allowed set."""
    if dependant is None:
        return None
    # Check this node
    if callable(getattr(dependant, "call", None)):
        allowed = _extract_allowed_from_callable(dependant.call)
        if allowed is not None:
            return allowed
    # Recurse into dependencies
    for sub in getattr(dependant, "dependencies", []) or []:
        found = _collect_roles_from_dependant(sub)
        if found is not None:
            return found
    return None

def _collect_roles_from_route(route: APIRoute) -> Optional[List[str]]:
    """
    Return a list of role strings to document (always includes ADMIN).
    - If no allowed set is found: return None (don’t inject anything).
    - If allowed is empty: ADMIN-only endpoint.
    """
    # 1) Endpoint signature deps (covers your common case)
    allowed = _collect_roles_from_dependant(getattr(route, "dependant", None))

    # 2) Router-level dependencies as a fallback
    if allowed is None:
        for dep in getattr(route, "dependencies", []) or []:
            call = getattr(dep, "dependency", None)
            if callable(call):
                allowed = _extract_allowed_from_callable(call)
                if allowed is not None:
                    break

    if allowed is None:
        return None  # no role guard found

    # ADMIN always allowed
    roles_out = [r.value if isinstance(r, KeyRole) else str(r) for r in sorted(allowed, key=lambda x: str(x))]
    if "ADMIN" not in roles_out:
        roles_out.append("ADMIN")
    return roles_out