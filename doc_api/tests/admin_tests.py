import os

import pytest

from doc_api.api.schemas import base_objects
from doc_api.api.schemas.responses import AppCode


#
# POST /v1/admin/keys - 201, 409
#

@pytest.mark.asyncio
@pytest.mark.parametrize("key_role", [x.value for x in base_objects.KeyRole], ids=[f"{AppCode.KEY_CREATED}:{x.name}" for x in base_objects.KeyRole], indirect=True)
async def test_post_keys_201(client, new_key):
    role = new_key["role"]
    label = new_key["label"]
    secret = new_key["secret"]

    r = await client.get("/v1/me", headers={"X-API-KEY": secret})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.API_KEY_VALID.value
    data = body["data"]
    assert data["role"] == role
    assert data["label"] == label
    assert data["active"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("key_role", [x.value for x in base_objects.KeyRole], ids=[f"{AppCode.KEY_ALREADY_EXISTS}:{x.name}" for x in base_objects.KeyRole], indirect=True)
async def test_post_keys_409_duplicate_label(client, admin_headers, new_key):
    label = new_key["label"]

    r = await client.post(
        "/v1/admin/keys",
        headers=admin_headers,
        json={
            "label": label,
            "role": base_objects.KeyRole.USER.value
        }
    )
    assert r.status_code == 409, r.text
    body = r.json()
    assert body["code"] == AppCode.KEY_ALREADY_EXISTS.value


@pytest.mark.asyncio
async def test_post_keys_422_missing_role(client, admin_headers):
    r = await client.post(
        "/v1/admin/keys",
        headers=admin_headers,
        json={
            "label": "test-missing-role"
        }
    )
    assert r.status_code == 422, r.text
    body = r.json()
    assert body["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


@pytest.mark.asyncio
async def test_post_keys_422_extra_key(client, admin_headers):
    r = await client.post(
        "/v1/admin/keys",
        headers=admin_headers,
        json={
            "label": "test-extra-key",
            "role": base_objects.KeyRole.USER.value,
            "extra_key": "extra_value"
        }
    )
    assert r.status_code == 422, r.text
    body = r.json()
    assert body["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


#
# POST /v1/admin/keys/{label}/secret - 201, 404
#

@pytest.mark.asyncio
@pytest.mark.parametrize("key_role", [x.value for x in base_objects.KeyRole], ids=[f"{AppCode.KEY_SECRET_CREATED}:{x.name}" for x in base_objects.KeyRole], indirect=True)
async def test_post_keys_secret_201(client, admin_headers, new_key):
    label = new_key["label"]

    r = await client.post(
        f"/v1/admin/keys/{label}/secret",
        headers=admin_headers
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["code"] == AppCode.KEY_SECRET_CREATED.value
    data = body["data"]
    assert "secret" in data
    assert isinstance(data["secret"], str)
    assert len(data["secret"]) > 0


@pytest.mark.asyncio
async def test_post_keys_secret_404_nonexistent_key(client, admin_headers):
    r = await client.post(
        f"/v1/admin/keys/nonexistent-key/secret",
        headers=admin_headers
    )
    assert r.status_code == 404, r.text
    body = r.json()
    assert body["code"] == AppCode.KEY_NOT_FOUND.value


#
# PATCH /v1/admin/keys/{label} - 200, 400, 404, 409
#

@pytest.mark.asyncio
@pytest.mark.parametrize("key_role", [x.value for x in base_objects.KeyRole], ids=[f"{AppCode.KEY_UPDATED}:{x.name}" for x in base_objects.KeyRole], indirect=True)
async def test_patch_key_200_update_label(client, admin_headers, new_key):
    old_label = new_key["label"]
    new_label = f"{old_label}-updated"

    r = await client.patch(
        f"/v1/admin/keys/{old_label}",
        headers=admin_headers,
        json={
            "label": new_label
        }
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.KEY_UPDATED.value

    # Verify that the key can be accessed with the new label
    r = await client.get(
        "/v1/me",
        headers={"X-API-KEY": new_key["secret"]}
    )
    assert r.status_code == 200, r.text
    body = r.json()
    data = body["data"]
    assert data["label"] == new_label


@pytest.mark.asyncio
@pytest.mark.parametrize("key_role", [x.value for x in base_objects.KeyRole], ids=[f"{AppCode.KEY_UPDATED}:{x.name}" for x in base_objects.KeyRole], indirect=True)
async def test_patch_key_200_update_role(client, admin_headers, new_key):
    label = new_key["label"]
    new_role = base_objects.KeyRole.WORKER.value if new_key["role"] != base_objects.KeyRole.WORKER.value else base_objects.KeyRole.USER.value

    r = await client.patch(
        f"/v1/admin/keys/{label}",
        headers=admin_headers,
        json={
            "role": new_role
        }
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.KEY_UPDATED.value

    # Verify that the key role has been updated
    r = await client.get(
        "/v1/me",
        headers={"X-API-KEY": new_key["secret"]}
    )
    assert r.status_code == 200, r.text
    body = r.json()
    data = body["data"]
    assert data["role"] == new_role


@pytest.mark.asyncio
@pytest.mark.parametrize("key_role", [x.value for x in base_objects.KeyRole], ids=[f"{AppCode.KEY_UPDATED}:{x.name}" for x in base_objects.KeyRole], indirect=True)
async def test_patch_key_200_update_active(client, admin_headers, new_key):
    label = new_key["label"]

    r = await client.patch(
        f"/v1/admin/keys/{label}",
        headers=admin_headers,
        json={
            "active": False
        }
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.KEY_UPDATED.value

    # Verify that the key is now inactive
    r = await client.get(
        "/v1/me",
        headers={"X-API-KEY": new_key["secret"]}
    )
    assert r.status_code == 403, r.text
    body = r.json()
    assert body["code"] == AppCode.API_KEY_INACTIVE.value


@pytest.mark.asyncio
@pytest.mark.parametrize("key_role", [base_objects.KeyRole.USER], ids=[f"{AppCode.KEY_UPDATE_NO_FIELDS}:{base_objects.KeyRole.USER.name}"], indirect=True)
async def test_patch_key_400_no_fields(client, admin_headers, new_key):
    label = new_key["label"]

    r = await client.patch(
        f"/v1/admin/keys/{label}",
        headers=admin_headers,
        json={}
    )
    assert r.status_code == 400, r.text
    body = r.json()
    assert body["code"] == AppCode.KEY_UPDATE_NO_FIELDS.value


@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[AppCode.KEY_NOT_FOUND])
async def test_patch_key_404(client, admin_headers, dummy):
    r = await client.patch(
        f"/v1/admin/keys/nonexistent-key",
        headers=admin_headers,
        json={
            "label": "new-label"
        }
    )
    assert r.status_code == 404, r.text
    body = r.json()
    assert body["code"] == AppCode.KEY_NOT_FOUND.value


@pytest.mark.asyncio
@pytest.mark.parametrize("key_role", [base_objects.KeyRole.USER], ids=[f"{AppCode.KEY_ALREADY_EXISTS}:{base_objects.KeyRole.USER.name}"], indirect=True)
async def test_patch_key_409(client, admin_headers, new_key):
    label = new_key["label"]
    role = new_key["role"]
    random_new_label = f"test-{role}-key-{os.urandom(4).hex()}"
    r = await client.post(
        "/v1/admin/keys",
        headers=admin_headers,
        json={
            "label": random_new_label,
            "role": base_objects.KeyRole.USER.value
        }
    )
    assert r.status_code == 201, r.text

    r = await client.patch(
        f"/v1/admin/keys/{label}",
        headers=admin_headers,
        json={
            "label": "duplicate-label-key"
        }
    )
    assert r.status_code == 409, r.text
    body = r.json()
    assert body["code"] == AppCode.KEY_ALREADY_EXISTS.value



