from unittest.mock import Mock

import pytest

from doc_api.adapter import Adapter
from doc_api.tests.dummy_data import make_white_image_bytes


@pytest.mark.parametrize("method,expected", [("patch_job_fail", "failed"), ("patch_job_error", "error")])
def test_adapter_failure_state(method, expected):
    connector = Mock()
    connector.patch.return_value.status_code = 200
    connector.patch.return_value.json.return_value = {"code": "OK"}
    adapter = Adapter("http://test", connector)
    response = getattr(adapter, method)(job_id="job", log="technical", log_user="public")
    assert response.is_success
    connector.patch.assert_called_once_with(
        "http://test/v1/jobs/job", json={"state": expected, "log": "technical", "log_user": "public"})


@pytest.mark.parametrize("kind", ["image", "alto", "page"])
def test_adapter_uploaded_input_uses_docapi(kind):
    connector = Mock()
    content = make_white_image_bytes()[0] if kind == "image" else b"<document/>"
    connector.get.return_value = Mock(status_code=200, content=content)
    connector.get.return_value.json.side_effect = ValueError("not JSON")
    result = getattr(Adapter("https://api.invalid", connector), f"get_{kind}")("image-id", job_id="job-id")
    assert result.is_success
    connector.get.assert_called_once_with(f"https://api.invalid/v1/jobs/job-id/images/image-id/files/{kind}")
