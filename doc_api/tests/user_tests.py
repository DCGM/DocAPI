import itertools
from typing import Dict, Any

import pytest

from doc_api.api.schemas import base_objects
from doc_api.api.schemas.responses import AppCode

#
# POST /v1/jobs
#

# --- All boolean combinations of the three *_required flags ---
REQUIRED_FLAGS = ["meta_json_required", "alto_required", "page_required"]

def generate_payloads():
    base_images = [
        {"name": "img1.png", "order": 0},
        {"name": "img2.jpg", "order": 1},
        {"name": "img3.tif", "order": 2},
    ]

    payloads = []
    for combo in itertools.product([False, True], repeat=3):
        flags = dict(zip(REQUIRED_FLAGS, combo))
        payloads.append({
            "images": base_images[:2] if any(combo) else base_images[:3],
            **flags,
        })
    return payloads

def payload_id(payload):
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

PAYLOADS = generate_payloads()

@pytest.mark.asyncio
@pytest.mark.parametrize("payload", PAYLOADS, ids=payload_id)
async def test_post_job(client, user_headers, payload):
    await assert_job_created(client, user_headers, payload)

async def assert_job_created(
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



'''
@pytest.mark.asyncio
async def test_post_job_validation_422(client, user_headers):
    # Triggers the three example validation errors:
    # - images[0].name missing
    # - images[1].order not an integer
    # - alto_required not a boolean
    invalid_payload = {
        "images": [
            {"order": 0},          # name missing
            {"name": "b.png", "order": "one"},  # order wrong type
        ],
        "meta_json_required": False,
        "alto_required": "yes",   # wrong type
        "page_required": False,
    }
    r = await client.post("/v1/jobs", json=invalid_payload, headers=user_headers)
    assert r.status_code == 422, r.text
    detail = r.json().get("detail")
    assert isinstance(detail, list)

    # optional: check that at least one of the expected paths is present
    # (FastAPI’s error message format):
    paths = [err.get("loc") for err in detail if isinstance(err, dict)]
    assert ["body", "images", 0, "name"] in paths
    assert ["body", "images", 1, "order"] in paths
    assert ["body", "alto_required"] in paths
'''