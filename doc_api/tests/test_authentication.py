import pytest
from starlette.routing import Route
from sqlalchemy import update

from doc_api.api.main import app
from doc_api.db import model
from doc_api.tests.helpers import ROUTE_CASES, route_url

from doc_api.api.schemas import base_objects
from doc_api.api.schemas.responses import AppCode
from doc_api.config import config


#
# GET /v1/me - 200, 401, 403
#

@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[f"{AppCode.API_KEY_VALID}:{base_objects.KeyRole.READONLY.name}"])
async def test_get_me_200_readonly(client, readonly_headers, dummy):
    r = await client.get("/v1/me", headers=readonly_headers)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.API_KEY_VALID.value
    data = body["data"]
    assert data["role"] == base_objects.KeyRole.READONLY.value
    assert data["label"] == config.TEST_READONLY_KEY_LABEL
    assert data["active"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[f"{AppCode.API_KEY_VALID}:{base_objects.KeyRole.USER.name}"])
async def test_get_me_200_user(client, user_headers, dummy):
    r = await client.get("/v1/me", headers=user_headers)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.API_KEY_VALID.value
    data = body["data"]
    assert data["role"] == base_objects.KeyRole.USER.value
    assert data["label"] == config.TEST_USER_KEY_LABEL
    assert data["active"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[f"{AppCode.API_KEY_VALID}:{base_objects.KeyRole.WORKER.name}"])
async def test_get_me_200_worker(client, worker_headers, dummy):
    r = await client.get("/v1/me", headers=worker_headers)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.API_KEY_VALID.value
    data = body["data"]
    assert data["role"] == base_objects.KeyRole.WORKER.value
    assert data["label"] == config.TEST_WORKER_KEY_LABEL
    assert data["active"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[f"{AppCode.API_KEY_VALID}:{base_objects.KeyRole.ADMIN.name}"])
async def test_get_me_200_admin(client, admin_headers, dummy):
    r = await client.get("/v1/me", headers=admin_headers)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.API_KEY_VALID.value
    data = body["data"]
    assert data["role"] == base_objects.KeyRole.ADMIN.value
    assert data["label"] == config.TEST_ADMIN_KEY_LABEL
    assert data["active"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[AppCode.API_KEY_MISSING])
async def test_get_me_401_missing(client, dummy):
    r = await client.get("/v1/me")
    assert r.status_code == 401, r.text
    body = r.json()
    assert body["code"] == AppCode.API_KEY_MISSING.value


@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[AppCode.API_KEY_INVALID])
async def test_get_me_401_invalid(client, dummy):
    r = await client.get("/v1/me", headers={"X-API-KEY": "invalidkey"})
    assert r.status_code == 401, r.text
    body = r.json()
    assert body["code"] == AppCode.API_KEY_INVALID.value


@pytest.mark.asyncio
@pytest.mark.parametrize("key_role", [x.value for x in base_objects.KeyRole], ids=[f"{AppCode.API_KEY_INACTIVE}:{x.name}" for x in base_objects.KeyRole], indirect=True)
async def test_get_me_403_inactive(client, admin_headers, inactive_key):
    r = await client.get("/v1/me", headers={"X-API-KEY": inactive_key["secret"]})
    assert r.status_code == 403, r.text
    body = r.json()
    assert body["code"] == AppCode.API_KEY_INACTIVE.value


#
# GET /v1/admin/keys - 403
#

@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[f"{AppCode.API_KEY_ROLE_FORBIDDEN}:{base_objects.KeyRole.READONLY.name}"])
async def test_get_admin_keys_403_readonly(client, readonly_headers, dummy):
    r = await client.get("/v1/admin/keys", headers=readonly_headers)
    assert r.status_code == 403, r.text
    body = r.json()
    assert body["code"] == AppCode.API_KEY_ROLE_FORBIDDEN.value


@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[f"{AppCode.API_KEY_ROLE_FORBIDDEN}:{base_objects.KeyRole.USER.name}"])
async def test_get_admin_keys_403_user(client, user_headers, dummy):
    r = await client.get("/v1/admin/keys", headers=user_headers)
    assert r.status_code == 403, r.text
    body = r.json()
    assert body["code"] == AppCode.API_KEY_ROLE_FORBIDDEN.value


@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[f"{AppCode.API_KEY_ROLE_FORBIDDEN}:{base_objects.KeyRole.WORKER.name}"])
async def test_get_admin_keys_403_worker(client, worker_headers, dummy):
    r = await client.get("/v1/admin/keys", headers=worker_headers)
    assert r.status_code == 403, r.text
    body = r.json()
    assert body["code"] == AppCode.API_KEY_ROLE_FORBIDDEN.value


#
# Authentication/role contract across every protected endpoint
#

@pytest.mark.asyncio
@pytest.mark.parametrize("method,path,kwargs,roles", ROUTE_CASES, ids=[f"{m} {p}" for m,p,_,_ in ROUTE_CASES])
@pytest.mark.parametrize("credential,code", [
    ("missing", AppCode.API_KEY_MISSING),
    ("malformed", AppCode.API_KEY_INVALID),
    ("unknown", AppCode.API_KEY_INVALID),
    ("wrong-secret", AppCode.API_KEY_INVALID),
])
async def test_route_401_authentication(client, admin_headers, method, path, kwargs, roles, credential, code):
    headers = {}
    if credential == "malformed":
        headers = {"X-API-Key": "malformed"}
    elif credential == "unknown":
        headers = {"X-API-Key": "doc-api.unknownkid.secret"}
    elif credential == "wrong-secret":
        headers = {"X-API-Key": admin_headers["X-API-Key"].rsplit(".", 1)[0] + ".wrongsecret"}
    response = await client.request(method, route_url(path), headers=headers, **kwargs)
    assert response.status_code == 401, response.text
    assert response.json()["code"] == code.value
    assert response.headers["www-authenticate"].startswith("ApiKey")


@pytest.mark.asyncio
@pytest.mark.parametrize("method,path,kwargs,roles", ROUTE_CASES, ids=[f"{m} {p}" for m,p,_,_ in ROUTE_CASES])
async def test_route_403_inactive_key(client, admin_headers, db_sessions, method, path, kwargs, roles):
    async with db_sessions.begin() as db:
        await db.execute(update(model.Key).values(active=False))
    response = await client.request(method, route_url(path), headers=admin_headers, **kwargs)
    assert response.status_code == 403, response.text
    assert response.json()["code"] == AppCode.API_KEY_INACTIVE.value


@pytest.mark.asyncio
@pytest.mark.parametrize("method,path,kwargs,role", [
    (method, path, kwargs, role)
    for method, path, kwargs, allowed in ROUTE_CASES
    for role in ("readonly", "user", "worker", "manager") if role not in allowed
], ids=lambda item: str(item) if isinstance(item, str) else None)
async def test_route_403_role_forbidden(client, manager_headers, method, path, kwargs, role):
    headers = manager_headers if role == "manager" else {"X-API-Key": getattr(config, f"TEST_{role.upper()}_KEY")}
    response = await client.request(method, route_url(path), headers=headers, **kwargs)
    assert response.status_code == 403, response.text
    assert response.json()["code"] == AppCode.API_KEY_ROLE_FORBIDDEN.value


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["query", "cookie"])
async def test_get_me_200_alternative_credentials(client, user_headers, source):
    secret = user_headers["X-API-Key"]
    if source == "query":
        response = await client.get("/v1/me", params={"api_key": secret})
    else:
        client.cookies.set("api_key", secret)
        response = await client.get("/v1/me")
    assert response.status_code == 200, response.text
    assert response.json()["code"] == AppCode.API_KEY_VALID.value


@pytest.mark.asyncio
async def test_get_me_401_header_takes_precedence(client, user_headers):
    client.cookies.set("api_key", user_headers["X-API-Key"])
    response = await client.get("/v1/me", params={"api_key": user_headers["X-API-Key"]},
                                headers={"X-API-Key": "invalid"})
    assert response.status_code == 401, response.text
    assert response.json()["code"] == AppCode.API_KEY_INVALID.value


def test_registered_routes_have_test_cases():
    registered = {(method, route.path) for route in app.routes if isinstance(route, Route)
                  for method in route.methods}
    expected = {(method, path) for method, path, _, _ in ROUTE_CASES} | {("GET", "/")}
    expected |= {(method, path) for method in ("GET", "HEAD")
                 for path in ("/docs", "/docs/oauth2-redirect", "/redoc", "/openapi.json")}
    assert registered == expected


@pytest.mark.asyncio
async def test_get_me_200_manager(client, manager_headers):
    response = await client.get("/v1/me", headers=manager_headers)
    assert response.status_code == 200, response.text
    assert response.json()["data"]["role"] == "manager"


@pytest.mark.asyncio
@pytest.mark.parametrize("method,path,kwargs,roles", [case for case in ROUTE_CASES if "manager" in case[3]], ids=[f"{m} {p}" for m,p,_,roles in ROUTE_CASES if "manager" in roles])
async def test_route_manager_authorized(client, manager_headers, method, path, kwargs, roles):
    response = await client.request(method, route_url(path), headers=manager_headers, **kwargs)
    assert response.status_code != 500, response.text
    assert response.json().get("code") not in {
        AppCode.API_KEY_ROLE_FORBIDDEN.value, AppCode.API_KEY_INVALID.value, AppCode.API_KEY_MISSING.value,
    }, response.text
