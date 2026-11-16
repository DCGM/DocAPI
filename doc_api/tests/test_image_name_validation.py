import json

import pytest
from fastapi.exceptions import RequestValidationError

from doc_api.api.routes.user_routes import check_image_extensions
from doc_api.api.schemas.base_objects import JobDefinition
from doc_api.api.main import validation_handler
from doc_api.api.schemas.responses import AppCode
from doc_api.config import Config, config


@pytest.mark.parametrize(
    "required,allowed",
    [(False, ""), (True, ""), (False, " JPG, .PnG, ,jpg "), (True, ".jpg,.png")],
)
@pytest.mark.parametrize(
    "name", ["page", "page.", ".png", "", "   ", "page.   ", "page.jpg", "PAGE.PNG", " page.jpg ", "page.custom"]
)
def test_job_image_extension_options(monkeypatch, required, allowed, name):
    monkeypatch.setenv("REQUIRE_IMAGE_EXTENSION", str(required).lower())
    monkeypatch.setenv("ALLOWED_IMAGE_EXTENSIONS", allowed)
    settings = Config()
    monkeypatch.setattr(config, "REQUIRE_IMAGE_EXTENSION", settings.REQUIRE_IMAGE_EXTENSION)
    monkeypatch.setattr(config, "ALLOWED_IMAGE_EXTENSIONS", settings.ALLOWED_IMAGE_EXTENSIONS)

    if allowed:
        assert settings.ALLOWED_IMAGE_EXTENSIONS == {".jpg", ".png"}
        valid = name in {"page.jpg", "PAGE.PNG", " page.jpg "}
        error_type = "image_extension_not_allowed"
    else:
        valid = not required or name in {"page.jpg", "PAGE.PNG", " page.jpg ", "page.custom"}
        error_type = "image_extension_required"

    job_definition = JobDefinition.model_validate({"images": [{"name": name, "order": 0}]})
    if valid:
        check_image_extensions(job_definition)
    else:
        with pytest.raises(RequestValidationError) as exc:
            check_image_extensions(job_definition)
        assert exc.value.errors()[0]["loc"] == ("body", "images", 0, "name")
        assert exc.value.errors()[0]["type"] == error_type


def test_image_extension_defaults(monkeypatch):
    monkeypatch.delenv("REQUIRE_IMAGE_EXTENSION", raising=False)
    monkeypatch.delenv("ALLOWED_IMAGE_EXTENSIONS", raising=False)
    settings = Config()
    assert settings.REQUIRE_IMAGE_EXTENSION is False
    assert settings.ALLOWED_IMAGE_EXTENSIONS == set()


@pytest.mark.asyncio
@pytest.mark.parametrize("allowed", [set(), {".jpg"}])
async def test_extension_error_response(monkeypatch, allowed):
    monkeypatch.setattr(config, "REQUIRE_IMAGE_EXTENSION", True)
    monkeypatch.setattr(config, "ALLOWED_IMAGE_EXTENSIONS", allowed)
    with pytest.raises(RequestValidationError) as exc:
        check_image_extensions(JobDefinition(images=[{"name": "page", "order": 0}]))

    response = await validation_handler(None, exc.value)
    body = json.loads(response.body)
    assert response.status_code == 422
    assert body["code"] == AppCode.REQUEST_VALIDATION_ERROR.value
    assert body["details"][0]["loc"] == ["body", "images", 0, "name"]
