import pytest

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
