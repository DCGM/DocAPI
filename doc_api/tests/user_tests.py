import itertools
from typing import Dict, Any

import pytest

from doc_api.api.schemas import base_objects
from doc_api.api.schemas.responses import AppCode

#
# POST /v1/jobs
#

def generate_job_definition_payloads():
    base_images = [
        {"name": "img1.png", "order": 0},
        {"name": "img2.jpg", "order": 1},
        {"name": "img3.tif", "order": 2},
    ]

    payloads = []
    for combo in itertools.product([False, True], repeat=3):
        flags = dict(zip(["meta_json_required", "alto_required", "page_required"], combo))
        payloads.append({
            "images": base_images[:2] if any(combo) else base_images[:3],
            **flags,
        })
    return payloads

def job_definition_payload_id(payload):
    """Generate a readable ID for pytest logs."""
    image_count = len(payload["images"])
    # collect unique extensions
    exts = {img["name"].split(".")[-1] for img in payload["images"]}
    flags = [
        name.replace("_required", "")
        for name, val in payload.items()
        if name.endswith("_required") and val
    ]
    flags_str = "+".join(flags) if flags else "none"
    return f"{image_count}-imgs:{'+'.join(sorted(exts))}:{flags_str}"

JOB_DEFINITION_PAYLOADS = generate_job_definition_payloads()

async def assert_job_created_and_retrieved(
    client,
    headers: Dict[str, str],
    payload: Dict[str, Any],
):
    r = await client.post("/v1/jobs", json=payload, headers=headers)
    assert r.status_code == 201, r.text
    body = r.json()

    assert body["code"] == AppCode.JOB_CREATED.value
    data = body["data"]

    assert data["state"] == base_objects.ProcessingState.NEW.value
    assert data["meta_json_required"] == payload["meta_json_required"]
    assert data["alto_required"] == payload["alto_required"]
    assert data["page_required"] == payload["page_required"]

    assert data["meta_json_uploaded"] is False
    assert isinstance(data["images"], list)
    assert len(data["images"]) == len(payload["images"])

    for image_payload, image_body in zip(payload["images"], data["images"]):
        assert image_payload["name"] == image_body["name"]
        assert image_payload["order"] == image_body["order"]
        assert image_body["image_uploaded"] is False
        assert image_body["alto_uploaded"] is False
        assert image_body["page_uploaded"] is False


    r = await client.get(f"/v1/jobs/{data['id']}", headers=headers)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == AppCode.JOB_RETRIEVED.value
    data_get = body["data"]
    assert data_get == data


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", JOB_DEFINITION_PAYLOADS, ids=job_definition_payload_id)
async def test_creating_and_retrieving_job(client, user_headers, payload):
    await assert_job_created_and_retrieved(client, user_headers, payload)

@pytest.mark.asyncio
async def test_post_job_422(client, user_headers):
    invalid_payload = {
        "images": [
            {"order": 0}, # name missing
            {"name": "b.png", "order": "one"},  # order wrong type
        ],
        "meta_json_required": False,
        "alto_required": "ffff",   # wrong type
        "page_required": False,
    }
    r = await client.post("/v1/jobs", json=invalid_payload, headers=user_headers)
    assert r.status_code == 422, r.text
    body = r.json()
    assert body["code"] == AppCode.REQUEST_VALIDATION_ERROR.value
    details = body.get("details")
    assert isinstance(details, list)

    paths = [err.get("loc") for err in details if isinstance(err, dict)]
    assert ["body", "images", 0, "name"] in paths
    assert ["body", "images", 1, "order"] in paths
    assert ["body", "alto_required"] in paths
