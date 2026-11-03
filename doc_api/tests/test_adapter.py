from unittest.mock import Mock

import pytest

from doc_api.adapter import Adapter


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
