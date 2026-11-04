from sqlalchemy.ext.asyncio import AsyncSession
from doc_api.api import file_saving as upload_helpers
from starlette.datastructures import UploadFile
import asyncio
import os
from pathlib import Path
from unittest.mock import AsyncMock

from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import select

from doc_api.api.routes import admin_routes
from doc_api.api import main
from doc_api.api.database import DBError
from doc_api.tests.helpers import (interrupted_upload_body, interrupted_upload_headers,
                                   assert_internal_transfer_error, assert_records_unchanged,
                                   interrupt_download, inject_zip_upload_failure)

from doc_api.api.cruds import admin_cruds
from doc_api.api.schemas.base_objects import ProcessingState
from doc_api.db import model
from doc_api.config import config

from doc_api.api.schemas import base_objects
from doc_api.api.schemas.responses import AppCode
from doc_api.tests.dummy_data import JOB_DEFINITION_PAYLOADS, VALID_ZIP


#
# GET /v1/admin/keys - 200
#

@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[f"{AppCode.KEYS_RETRIEVED}"])
async def test_get_keys_200(client, admin_headers, dummy):
    r = await client.get("/v1/admin/keys", headers=admin_headers)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.KEYS_RETRIEVED.value
    data = body["data"]
    assert isinstance(data, list)
    assert {item["label"] for item in data} == {"test-admin", "test-readonly", "test-user", "test-worker"}
    for item in data:
        assert "label" in item
        assert "role" in item
        assert "active" in item


#
# POST /v1/admin/keys - 201, 409, 422
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


@pytest.mark.asyncio
async def test_post_keys_409_generation_failure(client, admin_headers, monkeypatch, db_sessions):
    async def exhausted(db):
        return None, None, None, None
    monkeypatch.setattr(admin_cruds, "get_secret", exhausted)
    response = await client.post("/v1/admin/keys", headers=admin_headers, json={"label": "collision", "role": "user"})
    assert response.status_code == 409, response.text
    assert response.json()["code"] == AppCode.KEY_CREATION_FAILED.value
    async with db_sessions() as db:
        assert (await db.execute(select(model.Key).where(model.Key.label == "collision"))).scalar_one_or_none() is None


#
# POST /v1/admin/keys/{label}/secret - 201, 404, 409
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
async def test_post_keys_secret_404(client, admin_headers):
    r = await client.post(
        f"/v1/admin/keys/nonexistent-key/secret",
        headers=admin_headers
    )
    assert r.status_code == 404, r.text
    body = r.json()
    assert body["code"] == AppCode.KEY_NOT_FOUND.value


@pytest.mark.asyncio
async def test_post_keys_secret_409_generation_failure(client, admin_headers, monkeypatch):
    async def exhausted(db):
        return None, None, None, None
    monkeypatch.setattr(admin_cruds, "get_secret", exhausted)
    response = await client.post("/v1/admin/keys/test-user/secret", headers=admin_headers)
    assert response.status_code == 409, response.text
    assert response.json()["code"] == AppCode.KEY_SECRET_CREATION_FAILED.value


@pytest.mark.asyncio
async def test_post_keys_secret_201_rotates_credentials(client, admin_headers, user_headers):
    response = await client.post("/v1/admin/keys/test-user/secret", headers=admin_headers)
    assert response.status_code == 201, response.text
    assert response.json()["code"] == AppCode.KEY_SECRET_CREATED.value
    new_headers = {"X-API-Key": response.json()["data"]["secret"]}
    old_response = await client.get("/v1/me", headers=user_headers)
    assert old_response.status_code == 401, old_response.text
    assert old_response.json()["code"] == AppCode.API_KEY_INVALID.value
    new_response = await client.get("/v1/me", headers=new_headers)
    assert new_response.status_code == 200, new_response.text
    assert new_response.json()["data"]["label"] == "test-user"


#
# PATCH /v1/admin/keys/{label} - 200, 400, 404, 409, 422
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
            "label": random_new_label
        }
    )
    assert r.status_code == 409, r.text
    body = r.json()
    assert body["code"] == AppCode.KEY_ALREADY_EXISTS.value


@pytest.mark.asyncio
async def test_patch_key_422_invalid_request(client, admin_headers):
    response = await client.request('PATCH', '/v1/admin/keys/test-user', headers=admin_headers, json={'role': 'invalid'})
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


#
# POST /v1/admin/engines - 201, 409, 422
#

@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[AppCode.ENGINE_CREATED])
async def test_post_engine_201(created_engine, dummy):
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[AppCode.ENGINE_CREATED])
async def test_post_engine_201_new_default(client, admin_headers, created_engine, dummy):
    assert created_engine["default"] is True
    new_definition = {
                "type": "docker",
                "docker_image": "example/engine:latest",
                "resources": { "cpu": 2, "memory": "4G" }
            }
    r = await client.post(
        "/v1/admin/engines",
        headers=admin_headers,
        json={
            "name": f"{created_engine['name']}-new",
            "version": created_engine["version"],
            "description": created_engine["description"],
            "definition": new_definition,
            "active": True,
            "default": True
        }
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINE_CREATED.value

    r = await client.get("/v1/engines",
        params={
            "name": created_engine["name"],
            "version": created_engine["version"],
            "show_definition": True
        },
        headers=admin_headers)

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINES_RETRIEVED.value
    data = body["data"]
    assert len(data) == 1
    engine = data[0]
    assert engine["default"] is False
    assert engine["active"] is True
    assert engine["definition"] == created_engine["definition"]

    r = await client.get("/v1/engines",
                            params={
                                "name": f"{created_engine['name']}-new",
                                "version": created_engine["version"],
                                "show_definition": True

                            },
                            headers=admin_headers)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINES_RETRIEVED.value
    data = body["data"]
    assert len(data) == 1
    engine = data[0]
    assert engine["default"] is True
    assert engine["active"] is True
    assert engine["definition"] == new_definition
    
    
@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[AppCode.ENGINE_CREATED])
async def test_post_engine_201_new_active(client, admin_headers, created_engine, dummy):
    assert created_engine["default"] is True
    new_definition = {
                "type": "docker",
                "docker_image": "example/engine:latest",
                "resources": { "cpu": 2, "memory": "4G" }
            }
    r = await client.post(
        "/v1/admin/engines",
        headers=admin_headers,
        json={
            "name": created_engine['name'],
            "version": created_engine["version"] + "-new",
            "description": created_engine["description"],
            "definition": new_definition,
            "active": True
        }
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINE_CREATED.value

    r = await client.get("/v1/engines",
                         params={
                             "name": created_engine["name"],
                             "version": created_engine["version"],
                             "show_definition": True
                         },
                         headers=admin_headers)

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINES_RETRIEVED.value
    data = body["data"]
    assert len(data) == 1
    engine = data[0]
    assert engine["default"] is True
    assert engine["active"] is False
    assert engine["definition"] == created_engine["definition"]

    r = await client.get("/v1/engines",
                            params={
                                "name": created_engine["name"],
                                "version": created_engine["version"] + "-new",
                                "show_definition": True
                            },
                            headers=admin_headers)

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINES_RETRIEVED.value
    data = body["data"]
    assert len(data) == 1
    engine = data[0]
    assert engine["default"] is False
    assert engine["active"] is True
    assert engine["definition"] == new_definition


@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[AppCode.ENGINE_CREATED])
async def test_post_engine_201_new_default_and_active(client, admin_headers, created_engine, dummy):
    assert created_engine["default"] is True
    new_definition = {
        "type": "docker",
        "docker_image": "example/engine:latest",
        "resources": {"cpu": 2, "memory": "4G"}
    }
    r = await client.post(
        "/v1/admin/engines",
        headers=admin_headers,
        json={
            "name": created_engine['name'],
            "version": created_engine["version"] + "-new",
            "description": created_engine["description"],
            "definition": new_definition,
            "active": True,
            "default": True
        }
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINE_CREATED.value

    r = await client.get("/v1/engines",
                         params={
                             "name": created_engine["name"],
                             "version": created_engine["version"],
                             "show_definition": True
                         },
                         headers=admin_headers)

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINES_RETRIEVED.value
    data = body["data"]
    assert len(data) == 1
    engine = data[0]
    assert engine["default"] is False
    assert engine["active"] is False
    assert engine["definition"] == created_engine["definition"]

    r = await client.get("/v1/engines",
                            params={
                                "name": created_engine["name"],
                                "version": created_engine["version"] + "-new",
                                "show_definition": True
                            },
                            headers=admin_headers)

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINES_RETRIEVED.value
    data = body["data"]
    assert len(data) == 1
    engine = data[0]
    assert engine["default"] is True
    assert engine["active"] is True
    assert engine["definition"] == new_definition


@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[AppCode.ENGINE_ALREADY_EXISTS])
async def test_post_engine_409(client, admin_headers, created_engine, dummy):
    r = await client.post(
        "/v1/admin/engines",
        headers=admin_headers,
        json=created_engine
    )
    assert r.status_code == 409, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINE_ALREADY_EXISTS.value

@pytest.mark.asyncio
async def test_post_engine_422_invalid_request(client, admin_headers):
    response = await client.request('POST', '/v1/admin/engines', headers=admin_headers, json={})
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


#
# PATCH /v1/admin/engines/{name}/{version} - 200, 400, 404, 409, 422
#

@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[AppCode.ENGINE_UPDATED])
async def test_patch_engine_200_update_default(client, admin_headers, created_engine, dummy):
    name = created_engine["name"]
    version = created_engine["version"]

    new_version = created_engine["version"] + "-future-default"

    r = await client.post(
        f"/v1/admin/engines",
        headers=admin_headers,
        json={**created_engine,
              "version": new_version,
              "default": False}
    )

    assert r.status_code == 201, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINE_CREATED.value

    r = await client.patch(
        f"/v1/admin/engines/{name}/{new_version}",
        headers=admin_headers,
        json={
            "default": True
        }
    )

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINE_UPDATED.value
    r = await client.get(
        "/v1/engines",
        params={
            "name": name,
            "version": new_version
        },
        headers=admin_headers
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINES_RETRIEVED.value
    data = body["data"]
    assert len(data) == 1
    engine = data[0]
    assert engine["default"] is True

    f = await client.get(
        "/v1/engines",
        params={
            "name": name,
            "version": version
        },
        headers=admin_headers
    )
    assert f.status_code == 200, f.text
    body = f.json()
    assert body["code"] == AppCode.ENGINES_RETRIEVED.value
    data = body["data"]
    assert len(data) == 1
    engine = data[0]
    assert engine["default"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[AppCode.ENGINE_UPDATED])
async def test_patch_engine_200_update_active(client, admin_headers, created_engine, dummy):
    name = created_engine["name"]
    version = created_engine["version"]

    new_version = created_engine["version"] + "-future-active"

    r = await client.post(
        f"/v1/admin/engines",
        headers=admin_headers,
        json={**created_engine,
              "version": new_version,
              "active": False}
    )

    assert r.status_code == 201, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINE_CREATED.value

    r = await client.patch(
        f"/v1/admin/engines/{name}/{new_version}",
        headers=admin_headers,
        json={
            "active": True
        }
    )

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINE_UPDATED.value
    r = await client.get(
        "/v1/engines",
        params={
            "name": name,
            "version": new_version
        },
        headers=admin_headers
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINES_RETRIEVED.value
    data = body["data"]
    assert len(data) == 1
    engine = data[0]
    assert engine["active"] is True

    f = await client.get(
        "/v1/engines",
        params={
            "name": name,
            "version": version
        },
        headers=admin_headers
    )
    assert f.status_code == 200, f.text
    body = f.json()
    assert body["code"] == AppCode.ENGINES_RETRIEVED.value
    data = body["data"]
    assert len(data) == 1
    engine = data[0]
    assert engine["active"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[AppCode.ENGINE_UPDATED])
async def test_patch_engine_200_update_description(client, admin_headers, created_engine, dummy):
    name = created_engine["name"]
    version = created_engine["version"]
    new_description = f"{created_engine['description']} - updated"

    r = await client.patch(
        f"/v1/admin/engines/{name}/{version}",
        headers=admin_headers,
        json={
            "description": new_description
        }
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINE_UPDATED.value

    r = await client.get(
        "/v1/engines",
        params={
            "name": name,
            "version": version
        },
        headers=admin_headers
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINES_RETRIEVED.value
    data = body["data"]
    assert len(data) == 1
    engine = data[0]
    assert engine["description"] == new_description


@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[AppCode.ENGINE_UPDATE_NO_FIELDS])
async def test_patch_engine_400_no_fields(client, admin_headers, created_engine, dummy):
    name = created_engine["name"]
    version = created_engine["version"]

    r = await client.patch(
        f"/v1/admin/engines/{name}/{version}",
        headers=admin_headers,
        json={}
    )
    assert r.status_code == 400, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINE_UPDATE_NO_FIELDS.value


@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[AppCode.ENGINE_NOT_FOUND])
async def test_patch_engine_404(client, admin_headers, dummy):
    r = await client.patch(
        f"/v1/admin/engines/nonexistent-engine/1.0.0",
        headers=admin_headers,
        json={
            "active": True
        }
    )
    assert r.status_code == 404, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINE_NOT_FOUND.value


@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[AppCode.ENGINE_ALREADY_EXISTS])
async def test_patch_engine_409(client, admin_headers, created_engine, dummy):
    name = created_engine["name"]
    version = created_engine["version"]

    new_version = created_engine["version"] + "-duplicate"

    r = await client.post(
        f"/v1/admin/engines",
        headers=admin_headers,
        json={**created_engine,
              "version": new_version}
    )

    assert r.status_code == 201, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINE_CREATED.value

    r = await client.patch(
        f"/v1/admin/engines/{name}/{new_version}",
        headers=admin_headers,
        json={
            "version": version,
        }
    )

    assert r.status_code == 409, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINE_ALREADY_EXISTS.value


@pytest.mark.asyncio
async def test_patch_engine_422_invalid_request(client, admin_headers):
    response = await client.request('PATCH', '/v1/admin/engines/missing/1', headers=admin_headers, json={'active': 'invalid'})
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


#
# PUT /v1/admin/engines/{name}/{version}/files - 201, 200, 404, 415, 422, 400, 500
#

@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[AppCode.ENGINE_FILES_UPLOADED])
async def test_put_engine_files_201(client, admin_headers, created_engine, dummy):
    name = created_engine["name"]
    version = created_engine["version"]

    r = await client.put(
        f"/v1/admin/engines/{name}/{version}/files",
        headers=admin_headers,
        files={
            "file": ("engine-files.zip", VALID_ZIP, "application/zip")
        }
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINE_FILES_UPLOADED.value


@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[AppCode.ENGINE_FILES_REUPLOADED])
async def test_put_engine_files_200(client, admin_headers, created_engine, dummy):
    name = created_engine["name"]
    version = created_engine["version"]

    r = await client.put(
        f"/v1/admin/engines/{name}/{version}/files",
        headers=admin_headers,
        files={
            "file": ("engine-files.zip", VALID_ZIP, "application/zip")
        }
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINE_FILES_UPLOADED.value

    r = await client.get(
        f"/v1/engines",
        params={
            "name": name,
            "version": version,
            "show_definition": True
        },
        headers=admin_headers
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINES_RETRIEVED.value
    data = body["data"]
    assert len(data) == 1
    engine = data[0]
    old_files_updated = engine["files_updated"]


    r = await client.put(
        f"/v1/admin/engines/{name}/{version}/files",
        headers=admin_headers,
        files={
            "file": ("engine-files.zip", VALID_ZIP, "application/zip")
        }
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINE_FILES_REUPLOADED.value

    r = await client.get(
        f"/v1/engines",
        params={
            "name": name,
            "version": version,
            "show_definition": True
        },
        headers=admin_headers
    )

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINES_RETRIEVED.value
    data = body["data"]
    assert len(data) == 1
    engine = data[0]
    new_files_updated = engine["files_updated"]
    assert new_files_updated > old_files_updated


@pytest.mark.asyncio
@pytest.mark.parametrize("dummy", [0], ids=[AppCode.ENGINE_FILES_INVALID])
async def test_put_engine_files_415(client, admin_headers, created_engine, dummy):
    name = created_engine["name"]
    version = created_engine["version"]

    r = await client.put(
        f"/v1/admin/engines/{name}/{version}/files",
        headers=admin_headers,
        files={
            "file": ("engine-files.txt", b"this is not a zip file", "text/plain")
        }
    )
    assert r.status_code == 415, r.text
    body = r.json()
    assert body["code"] == AppCode.ENGINE_FILES_INVALID.value


@pytest.mark.asyncio
async def test_put_engine_files_404_engine_not_found(client, admin_headers):
    response = await client.put("/v1/admin/engines/missing/1/files", headers=admin_headers,
                                files={"file": ("engine.zip", VALID_ZIP)})
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.ENGINE_NOT_FOUND.value


@pytest.mark.asyncio
async def test_put_engine_files_422_missing_file(client, admin_headers):
    response = await client.put("/v1/admin/engines/missing/1/files", headers=admin_headers)
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


@pytest.mark.asyncio
async def test_put_engine_files_201_validation_disabled(client, admin_headers, engine_factory, monkeypatch):
    monkeypatch.setattr(config, "ENGINE_FILES_ZIP_VALIDATION", False)
    engine = await engine_factory()
    response = await client.put(f"/v1/admin/engines/{engine.name}/{engine.version}/files", headers=admin_headers,
                                files={"file": ("engine.zip", b"not a ZIP")})
    assert response.status_code == 201, response.text
    assert response.json()["code"] == AppCode.ENGINE_FILES_UPLOADED.value


@pytest.mark.parametrize("existing", [False, True], ids=["first-upload", "replacement"])
@pytest.mark.parametrize("failure", ["read", "write", "replace"])
@pytest.mark.asyncio
async def test_put_engine_files_500_upload_failure(client, admin_headers, zip_upload_case, db_sessions, monkeypatch, existing, failure):
    case = await zip_upload_case("engine", existing=existing)
    with monkeypatch.context() as patch:
        inject_zip_upload_failure(case.path, failure, patch)
        response = await client.put(case.url, headers=admin_headers, files={"file": ("replacement.zip", VALID_ZIP)})
    assert_internal_transfer_error(response)
    assert case.path.exists() == existing
    if existing:
        assert case.path.read_bytes() == case.old
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-upload-staging-*"))
    await assert_records_unchanged(db_sessions, case.records)
    retry = await client.put(case.url, headers=admin_headers, files={"file": ("replacement.zip", VALID_ZIP)})
    assert retry.status_code == (200 if existing else 201), retry.text
    assert case.path.read_bytes() == VALID_ZIP


@pytest.mark.asyncio
async def test_put_engine_files_415_preserves_existing_file(client, admin_headers, zip_upload_case, db_sessions):
    case = await zip_upload_case("engine")
    response = await client.put(case.url, headers=admin_headers, files={"file": ("invalid.zip", b"invalid")})
    assert response.status_code == 415, response.text
    assert case.path.read_bytes() == case.old
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-upload-staging-*"))
    await assert_records_unchanged(db_sessions, case.records)


@pytest.mark.parametrize("existing", [False, True], ids=["first-upload", "replacement"])
@pytest.mark.asyncio
async def test_put_engine_files_400_interrupted_upload(client, admin_headers, zip_upload_case, db_sessions, existing):
    case = await zip_upload_case("engine", existing=existing)
    response = await client.put(case.url, headers=interrupted_upload_headers(admin_headers),
                                    content=interrupted_upload_body())
    assert response.status_code == 400, response.text
    assert response.json()["code"] == AppCode.HTTP_ERROR.value
    assert case.path.exists() == existing
    if existing:
        assert case.path.read_bytes() == case.old
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-upload-staging-*"))
    await assert_records_unchanged(db_sessions, case.records)


@pytest.mark.asyncio
@pytest.mark.parametrize("existing", [False, True], ids=["first-upload", "replacement"])
async def test_put_engine_files_500_database_failure(client, admin_headers, zip_upload_case, db_sessions, monkeypatch, existing):
    case = await zip_upload_case("engine", existing=existing)
    with monkeypatch.context() as patch:
        patch.setattr(admin_routes.admin_cruds, "update_engine_files_updated",
                      AsyncMock(side_effect=DBError("injected transfer failure")))
        response = await client.put(case.url, headers=admin_headers, files={"file": ("replacement.zip", VALID_ZIP)})
    assert_internal_transfer_error(response)
    assert case.path.exists() == existing
    if existing:
        assert case.path.read_bytes() == case.old
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-rollback-backup-*"))
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-upload-staging-*"))
    await assert_records_unchanged(db_sessions, case.records)
    retry = await client.put(case.url, headers=admin_headers, files={"file": ("replacement.zip", VALID_ZIP)})
    assert retry.status_code == (200 if existing else 201), retry.text
    async with db_sessions() as db:
        assert (await db.get(model.Engine, case.records[0][1])).files_updated != case.records[0][2]["files_updated"]


@pytest.mark.parametrize("existing", [False, True], ids=["first-upload", "replacement"])
@pytest.mark.asyncio
async def test_put_engine_files_cancelled_upload(client, admin_headers, zip_upload_case, db_sessions, monkeypatch, existing):
    case = await zip_upload_case("engine", existing=existing)
    started = asyncio.Event()
    block = asyncio.Event()
    reads = 0
    original_read = UploadFile.read
    async def interrupted_read(upload, size=-1):
        nonlocal reads
        if reads:
            started.set()
            await block.wait()
        reads += 1
        return await original_read(upload, 16)
    with monkeypatch.context() as patch:
        patch.setattr(UploadFile, "read", interrupted_read)
        request = asyncio.create_task(client.put(case.url, headers=admin_headers, files={"file": ("upload.zip", VALID_ZIP)}))
        try:
            await asyncio.wait_for(started.wait(), timeout=10)
            request.cancel()
            with pytest.raises(asyncio.CancelledError):
                await request
        finally:
            block.set()
            if not request.done():
                request.cancel()
                try:
                    await request
                except asyncio.CancelledError:
                    pass
    assert case.path.exists() == existing
    if existing:
        assert case.path.read_bytes() == case.old
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-upload-staging-*"))
    await assert_records_unchanged(db_sessions, case.records)
    retry = await client.put(case.url, headers=admin_headers, files={"file": ("upload.zip", VALID_ZIP)})
    assert retry.status_code == (200 if existing else 201), retry.text


@pytest.mark.parametrize("existing", [False, True], ids=["first-upload", "replacement"])
@pytest.mark.asyncio
async def test_put_engine_files_500_commit_failure(client, admin_headers, zip_upload_case, db_sessions, reject_upload_commit, existing):
    case = await zip_upload_case("engine", existing=existing)
    with reject_upload_commit(case.path, old=case.old if existing else None) as observed:
        response = await client.put(case.url, headers=admin_headers, files={"file": ("replacement.zip", VALID_ZIP)})
    assert_internal_transfer_error(response)
    assert observed == [True]
    assert case.path.exists() == existing
    if existing:
        assert case.path.read_bytes() == case.old
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-*"))
    await assert_records_unchanged(db_sessions, case.records)
    retry = await client.put(case.url, headers=admin_headers, files={"file": ("replacement.zip", VALID_ZIP)})
    assert retry.status_code == (200 if existing else 201), retry.text
    assert case.path.read_bytes() == VALID_ZIP
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-*"))


@pytest.mark.asyncio
async def test_put_engine_files_409_file_save_in_progress(client, admin_headers, zip_upload_case, monkeypatch, db_sessions):
    from doc_api.tests.helpers import assert_file_save_conflict
    case = await zip_upload_case("engine")
    await assert_file_save_conflict(client, method="PUT", url=case.url, headers=admin_headers,
                                   kwargs={"files": {"file": ("replacement.zip", VALID_ZIP)}}, path=case.path,
                                   monkeypatch=monkeypatch, db_sessions=db_sessions, first_status=500, fail_first=True)
    assert case.path.read_bytes() == VALID_ZIP


@pytest.mark.asyncio
async def test_put_engine_files_409_other_process_saving(client, admin_headers, zip_upload_case, db_sessions):
    import subprocess
    import sys
    case = await zip_upload_case("engine")
    lock_path = case.path.with_name(f".{case.path.name}.docapi-save.lock")
    code = """
import fcntl, sys
with open(sys.argv[1], "a") as lock:
    fcntl.flock(lock, fcntl.LOCK_EX)
    print("locked", flush=True)
    sys.stdin.read()
"""
    process = subprocess.Popen([sys.executable, "-c", code, str(lock_path)],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert await asyncio.wait_for(asyncio.to_thread(process.stdout.readline), timeout=10) == "locked\n"
        response = await client.put(case.url, headers=admin_headers,
                                    files={"file": ("replacement.zip", VALID_ZIP)})
        assert response.status_code == 409, response.text
        assert response.json()["code"] == AppCode.FILE_SAVE_IN_PROGRESS.value
        assert case.path.read_bytes() == case.old
        assert db_sessions.kw["bind"].pool.checkedout() == 0
    finally:
        # An abrupt exit must release the OS lock without deleting its file.
        process.kill()
        await asyncio.to_thread(process.communicate, timeout=10)
    assert lock_path.exists()
    response = await client.put(case.url, headers=admin_headers,
                                files={"file": ("replacement.zip", VALID_ZIP)})
    assert response.status_code == 200, response.text
    assert case.path.read_bytes() == VALID_ZIP


@pytest.mark.parametrize("existing", [False, True], ids=["first-upload", "replacement"])
@pytest.mark.parametrize("phase", ["metadata", "commit"])
@pytest.mark.asyncio
async def test_put_engine_files_cancelled_after_install(client, admin_headers, zip_upload_case, db_sessions, monkeypatch, existing, phase):
    case = await zip_upload_case("engine", existing=existing)
    started, release = asyncio.Event(), asyncio.Event()
    original_update = admin_cruds.update_engine_files_updated
    original_commit = AsyncSession.commit
    async def update(**kwargs):
        started.set()
        await release.wait()
        return await original_update(**kwargs)
    async def commit(session):
        if session.info.get("file_save_transaction"):
            started.set()
            await release.wait()
        return await original_commit(session)
    with monkeypatch.context() as patch:
        if phase == "metadata":
            patch.setattr(admin_cruds, "update_engine_files_updated", update)
        else:
            patch.setattr(AsyncSession, "commit", commit)
        request = asyncio.create_task(client.put(case.url, headers=admin_headers, files={"file": ("new.zip", VALID_ZIP)}))
        try:
            await asyncio.wait_for(started.wait(), timeout=10)
            request.cancel()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await request
        finally:
            release.set()
            if not request.done():
                request.cancel()
            await asyncio.gather(request, return_exceptions=True)
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-*"))
    if phase == "commit":
        assert case.path.read_bytes() == VALID_ZIP
        async with db_sessions() as db:
            assert (await db.get(model.Engine, case.records[0][1])).files_updated != case.records[0][2]["files_updated"]
    else:
        assert case.path.exists() == existing
        if existing:
            assert case.path.read_bytes() == case.old
        await assert_records_unchanged(db_sessions, case.records)


@pytest.mark.asyncio
async def test_put_engine_files_500_unknown_commit_outcome(client, admin_headers, zip_upload_case, db_sessions, monkeypatch):
    case = await zip_upload_case("engine")
    original_commit = AsyncSession.commit
    async def lost_acknowledgement(session):
        await original_commit(session)
        if session.info.get("file_save_transaction"):
            raise OSError("injected lost commit acknowledgement")
    with monkeypatch.context() as patch:
        patch.setattr(AsyncSession, "commit", lost_acknowledgement)
        response = await client.put(case.url, headers=admin_headers, files={"file": ("new.zip", VALID_ZIP)})
    assert response.status_code == 500, response.text
    assert response.json()["code"] == AppCode.INTERNAL_ERROR.value
    # A lost acknowledgement must not restore old bytes over committed metadata.
    assert case.path.read_bytes() == VALID_ZIP
    backups = list(case.path.parent.glob(case.path.name + ".docapi-tmp-rollback-backup-*"))
    assert len(backups) == 1 and backups[0].read_bytes() == case.old
    async with db_sessions() as db:
        assert (await db.get(model.Engine, case.records[0][1])).files_updated != case.records[0][2]["files_updated"]
    retry = await client.put(case.url, headers=admin_headers, files={"file": ("retry.zip", VALID_ZIP)})
    assert retry.status_code == 500, retry.text
    assert len(list(case.path.parent.glob(case.path.name + "*"))) == 2
    assert backups[0].read_bytes() == case.old


@pytest.mark.asyncio
async def test_put_engine_files_500_unresolved_backup(client, admin_headers, zip_upload_case, db_sessions):
    case = await zip_upload_case("engine")
    backup = case.path.with_name(case.path.name + ".docapi-tmp-rollback-backup-leftover.zip")
    backup.write_bytes(b"retained original")
    response = await client.put(case.url, headers=admin_headers, files={"file": ("new.zip", VALID_ZIP)})
    assert_internal_transfer_error(response)
    assert case.path.read_bytes() == case.old
    assert backup.read_bytes() == b"retained original"
    assert len(list(case.path.parent.glob(case.path.name + "*"))) == 2
    await assert_records_unchanged(db_sessions, case.records)


@pytest.mark.asyncio
async def test_put_engine_files_200_abandoned_staging(client, admin_headers, zip_upload_case):
    case = await zip_upload_case("engine")
    staging = case.path.with_name(case.path.name + ".docapi-tmp-upload-staging-leftover.zip")
    staging.write_bytes(b"incomplete")
    response = await client.put(case.url, headers=admin_headers, files={"file": ("new.zip", VALID_ZIP)})
    assert response.status_code == 200, response.text
    assert case.path.read_bytes() == VALID_ZIP
    assert not list(case.path.parent.glob(case.path.name + ".docapi-tmp-*"))


@pytest.mark.asyncio
async def test_put_engine_files_200_backup_cleanup_failure(client, admin_headers, zip_upload_case, db_sessions, monkeypatch):
    case = await zip_upload_case("engine")
    original_remove = upload_helpers.os.remove
    def failed_cleanup(path):
        if ".docapi-tmp-rollback-backup-" in str(path):
            raise OSError("injected backup cleanup failure")
        return original_remove(path)
    with monkeypatch.context() as patch:
        patch.setattr(upload_helpers.os, "remove", failed_cleanup)
        response = await client.put(case.url, headers=admin_headers, files={"file": ("new.zip", VALID_ZIP)})
    assert response.status_code == 200, response.text
    assert case.path.read_bytes() == VALID_ZIP
    backups = list(case.path.parent.glob(case.path.name + ".docapi-tmp-rollback-backup-*"))
    assert len(backups) == 1 and backups[0].read_bytes() == case.old
    async with db_sessions() as db:
        assert (await db.get(model.Engine, case.records[0][1])).files_updated != case.records[0][2]["files_updated"]







#
# GET /v1/admin/jobs/{job_id}/artifacts - 200, 404, 422
#

@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_ARTIFACTS_RETRIEVED.value], indirect=True)
async def test_get_artifacts_200(client, admin_headers, job_with_artifacts, payload):
    job_id = job_with_artifacts["created_job"]["id"]
    r = await client.get(
        f"/v1/admin/jobs/{job_id}/artifacts",
        headers=admin_headers
    )
    assert r.status_code == 200, r.text

    #check for zip file response
    assert r.headers["content-type"] == "application/zip"
    assert "attachment; filename=" in r.headers["content-disposition"]
    assert r.content is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [JOB_DEFINITION_PAYLOADS[0]], ids=[AppCode.JOB_ARTIFACTS_NOT_FOUND.value], indirect=True)
async def test_get_artifacts_404(client, admin_headers, job_with_result, payload):
    job_id = job_with_result["created_job"]["id"]

    r = await client.get(
        f"/v1/admin/jobs/{job_id}/artifacts",
        headers=admin_headers
    )
    assert r.status_code == 404, r.text
    body = r.json()
    assert body["code"] == AppCode.JOB_ARTIFACTS_NOT_FOUND.value


@pytest.mark.asyncio
async def test_get_artifacts_422_invalid_request(client, admin_headers):
    response = await client.request('GET', '/v1/admin/jobs/not-a-uuid/artifacts', headers=admin_headers)
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


@pytest.mark.asyncio
async def test_get_artifacts_404_job_not_found(client, admin_headers):
    response = await client.get(f"/v1/admin/jobs/{uuid4()}/artifacts", headers=admin_headers)
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


@pytest.mark.parametrize("failure", ["open", "read", "disconnect"])
@pytest.mark.asyncio
async def test_get_artifacts_200_stream_failure(client, admin_headers, download_case, db_sessions, monkeypatch, failure):
    case = await download_case("artifacts")
    await interrupt_download(main.app, case.url, admin_headers, case.path, monkeypatch, failure)
    assert case.path.read_bytes() == case.content
    await assert_records_unchanged(db_sessions, case.records)
    retry = await client.get(case.url, headers=admin_headers)
    assert retry.status_code == 200, retry.text
    assert retry.content == case.content


#
# GET /v1/jobs as ADMIN - 200
#

@pytest.mark.asyncio
async def test_get_jobs_200_deletion_visibility(client, admin_headers, jobs_with_output_availability):
    job_id = str(jobs_with_output_availability.deleted)
    response = await client.get("/v1/jobs", headers=admin_headers)
    assert response.status_code == 200, response.text
    assert response.json()["code"] == AppCode.JOBS_RETRIEVED.value
    job = next(item for item in response.json()["data"] if item["id"] == job_id)
    assert all(job[name] is not None for name in ["inputs_deleted_at", "results_deleted_at", "artifacts_deleted_at"])


#
# GET /v1/jobs/{job_id} as ADMIN - 200
#

@pytest.mark.asyncio
async def test_get_job_200_deletion_visibility(client, admin_headers, jobs_with_output_availability):
    job_id = str(jobs_with_output_availability.deleted)
    response = await client.get(f"/v1/jobs/{job_id}", headers=admin_headers)
    assert response.status_code == 200, response.text
    assert response.json()["code"] == AppCode.JOB_RETRIEVED.value
    job = response.json()["data"]
    assert all(job[name] is not None for name in ["inputs_deleted_at", "results_deleted_at", "artifacts_deleted_at"])


#
# PATCH /v1/admin/jobs/{job_id} - 200, 404, 422
#

@pytest.mark.asyncio
@pytest.mark.parametrize("append_logs", [True, False])
async def test_patch_job_200_force_update(client, admin_headers, job_factory, db_sessions, append_logs):
    job_id = await job_factory(ProcessingState.NEW)
    now = datetime.now(timezone.utc).isoformat()
    fields = {"state": "done", "progress": 1.0, "previous_attempts": 2,
              "meta_json_uploaded": True, "meta_json_required": True,
              "alto_required": True, "page_required": True,
              "created_date": now, "started_date": now, "last_change": now, "finished_date": now,
              "log": "admin", "log_user": "user"}
    response = await client.patch(f"/v1/admin/jobs/{job_id}", headers=admin_headers,
                                  params={"append_logs": str(append_logs).lower()}, json=fields)
    assert response.status_code == 200, response.text
    assert response.json()["code"] == AppCode.JOB_UPDATED.value
    async with db_sessions() as db:
        job = await db.get(model.Job, job_id)
        assert job.state == ProcessingState.DONE
        for field in ["progress", "previous_attempts", "meta_json_uploaded", "meta_json_required", "alto_required", "page_required"]:
            assert getattr(job, field) == fields[field]
        for field in ["created_date", "started_date", "last_change", "finished_date"]:
            assert getattr(job, field).isoformat() == now
        assert job.log == ("Technical log\nadmin" if append_logs else "admin")
        assert job.log_user == ("User log\nuser" if append_logs else "user")


@pytest.mark.asyncio
async def test_patch_job_422_invalid_state(client, admin_headers, job_factory):
    job_id = await job_factory(ProcessingState.NEW)
    response = await client.patch(f"/v1/admin/jobs/{job_id}", headers=admin_headers, json={"state": "invalid"})
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


@pytest.mark.asyncio
async def test_patch_job_404_job_not_found(client, admin_headers):
    response = await client.patch(f"/v1/admin/jobs/{uuid4()}", headers=admin_headers, json={"state": "done"})
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


#
# PUT /debug/jobs/{job_id}/start - 200, 404, 409, 422
#

@pytest.mark.asyncio
async def test_put_job_start_200_ready(client, admin_headers, job_factory, db_sessions):
    job_id = await job_factory(ProcessingState.NEW)
    response = await client.put(f"/debug/jobs/{job_id}/start", headers=admin_headers)
    assert response.status_code == 200, response.text
    assert response.json()["code"] == AppCode.JOB_STARTED.value
    async with db_sessions() as db:
        assert (await db.get(model.Job, job_id)).state == ProcessingState.QUEUED


@pytest.mark.asyncio
async def test_put_job_start_409_missing_input(client, admin_headers, job_factory, db_sessions):
    job_id = await job_factory(ProcessingState.NEW)
    async with db_sessions.begin() as db:
        db.add(model.Image(id=uuid4(), job_id=job_id, name="page.jpg", order=0))
    response = await client.put(f"/debug/jobs/{job_id}/start", headers=admin_headers)
    assert response.status_code == 409, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_READY.value


@pytest.mark.asyncio
@pytest.mark.parametrize("state", [state for state in ProcessingState if state != ProcessingState.NEW])
async def test_put_job_start_409_already_started(client, admin_headers, job_factory, state):
    job_id = await job_factory(state)
    response = await client.put(f"/debug/jobs/{job_id}/start", headers=admin_headers)
    assert response.status_code == 409, response.text
    assert response.json()["code"] == AppCode.JOB_ALREADY_STARTED.value


@pytest.mark.asyncio
async def test_put_job_start_422_invalid_id(client, admin_headers):
    response = await client.put("/debug/jobs/not-a-uuid/start", headers=admin_headers)
    assert response.status_code == 422, response.text
    assert response.json()["code"] == AppCode.REQUEST_VALIDATION_ERROR.value


@pytest.mark.asyncio
async def test_put_job_start_404_job_not_found(client, admin_headers):
    response = await client.put(f"/debug/jobs/{uuid4()}/start", headers=admin_headers)
    assert response.status_code == 404, response.text
    assert response.json()["code"] == AppCode.JOB_NOT_FOUND.value


#
# GET /v1/engines as ADMIN - 200
#

@pytest.mark.asyncio
@pytest.mark.parametrize("params,names", [
    ({}, ["A", "B"]), ({"active": "false"}, ["B"]),
    ({"name": "A", "version": "1"}, ["A"]), ({"default": "true"}, ["A"]),
    ({"name": "missing"}, []),
])
async def test_get_engines_200_admin_filters(client, admin_headers, engine_factory, params, names):
    await engine_factory(name="A", default=True)
    await engine_factory(name="B", active=False)
    response = await client.get("/v1/engines", headers=admin_headers, params={**params, "show_definition": "true"})
    assert response.status_code == 200, response.text
    assert [engine["name"] for engine in response.json()["data"]] == names
    for engine in response.json()["data"]:
        assert engine["id"] is not None
        assert engine["definition"] == {"type": "test"}
