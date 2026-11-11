from unittest.mock import Mock, PropertyMock

import pytest

from doc_api.adapter import Adapter, AdapterResponse
from doc_api.tests.dummy_data import make_white_image_bytes


@pytest.mark.parametrize("method,expected", [("patch_job_fail", "failed"), ("patch_job_error", "error")])
def test_adapter_failure_state(method, expected):
    connector = Mock()
    connector.patch.return_value.status_code = 200
    connector.patch.return_value.headers = {"Content-Type": "application/json"}
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
    connector.get.return_value = Mock(status_code=200, content=content, headers={"Content-Type": "application/octet-stream"})
    connector.get.return_value.json.side_effect = ValueError("not JSON")
    result = getattr(Adapter("https://api.invalid", connector), f"get_{kind}")("image-id", job_id="job-id")
    assert result.is_success
    connector.get.assert_called_once_with(f"https://api.invalid/v1/jobs/job-id/images/image-id/files/{kind}")


@pytest.mark.parametrize('method,state', [('patch_job_finish', 'done'), ('patch_job_error', 'error'), ('patch_job_fail', 'failed')])
def test_adapter_final_reports(method, state):
    connector = Mock()
    connector.patch.return_value.status_code = 200
    connector.patch.return_value.headers = {"Content-Type": "application/json"}
    connector.patch.return_value.json.return_value = {'code': 'OK'}
    response = getattr(Adapter('http://test', connector), method)(job_id='job',
        final_log='technical final', final_log_user='user final')
    assert response.is_success
    connector.patch.assert_called_once_with('http://test/v1/jobs/job',
        json={'state': state, 'final_log': 'technical final', 'final_log_user': 'user final'})


@pytest.mark.parametrize("status", [200, 404, 500])
@pytest.mark.parametrize("media_type", ["application/zip", "application/octet-stream", "image/jpeg", "application/xml", "text/plain", ""])
def test_adapter_response_skips_non_json(status, media_type):
    raw = Mock(headers={"Content-Type": media_type})
    raw.json.side_effect = AssertionError("Download bodies must not be parsed as JSON")
    response = AdapterResponse(data=b"payload" if status == 200 else None, status=status, response=raw)
    assert response.code is None
    assert response.is_success == (status == 200)
    assert response.response is (None if status == 200 else raw)
    raw.json.assert_not_called()


@pytest.mark.parametrize("media_type", ["application/json", "application/json; charset=utf-8", "Application/JSON", "application/problem+json"])
@pytest.mark.parametrize("status", [200, 400])
def test_adapter_response_extracts_json_code(media_type, status):
    raw = Mock(headers={"Content-Type": media_type})
    raw.json.return_value = {"code": "API_CODE"}
    response = AdapterResponse(status=status, response=raw, no_data_response=True)
    assert response.code == "API_CODE"
    raw.json.assert_called_once()


@pytest.mark.parametrize("payload", [[], None, "text", 42, {}])
def test_adapter_response_json_without_code(payload):
    raw = Mock(headers={"Content-Type": "application/json"})
    raw.json.return_value = payload
    assert AdapterResponse(status=400, response=raw).code is None


@pytest.mark.parametrize("error", [ValueError("malformed JSON"), RuntimeError("unexpected failure")])
def test_adapter_response_json_errors(error):
    raw = Mock(headers={"Content-Type": "application/json"})
    raw.json.side_effect = error
    if isinstance(error, ValueError):
        assert AdapterResponse(status=400, response=raw).code is None
    else:
        with pytest.raises(RuntimeError, match="unexpected failure"):
            AdapterResponse(status=400, response=raw)


def test_adapter_response_explicit_code_skips_json():
    raw = Mock(headers={"Content-Type": "application/json"})
    response = AdapterResponse(status=400, code="EXPLICIT", response=raw)
    assert response.code == "EXPLICIT"
    raw.json.assert_not_called()


@pytest.mark.parametrize("method,kwargs", [
    ("get_engine_files", {"engine_id": "engine"}), ("get_result", {"job_id": "job"}),
    ("get_meta_json", {"job_id": "job"}),
])
def test_adapter_download_does_not_parse_body(method, kwargs):
    connector = Mock()
    raw = connector.get.return_value
    raw.status_code = 200
    raw.headers = {"Content-Type": "application/octet-stream"}
    raw.content = b"file contents"
    raw.json.side_effect = AssertionError("No JSON parsing for downloads")
    result = getattr(Adapter("http://test", connector), method)(**kwargs)
    assert result.is_success
    raw.json.assert_not_called()


@pytest.mark.parametrize("kind", ["engine", "result"])
@pytest.mark.parametrize("outcome", ["success", "http_error", "read_error", "write_error"])
def test_adapter_stream_download(tmp_path, kind, outcome):
    connector = Mock()
    raw = connector.get.return_value
    raw.status_code = 404 if outcome == "http_error" else 200
    raw.headers = {"Content-Type": "application/json" if outcome == "http_error" else "application/zip"}
    raw.json.return_value = {"code": "NOT_FOUND"}
    raw.text = "not found"
    type(raw).content = PropertyMock(side_effect=AssertionError("Streaming must not buffer the body"))
    if outcome != "http_error":
        raw.json.side_effect = AssertionError("Streaming must not parse the body")
        type(raw).text = PropertyMock(side_effect=AssertionError("Streaming must not decode the body"))
    def chunks():
        yield b"first"
        yield b""
        if outcome == "read_error":
            raise ConnectionError("stream interrupted")
        yield b"second"
    raw.iter_content.side_effect = lambda **kwargs: chunks()
    target = tmp_path / "download.zip"
    if outcome in {"http_error", "write_error"}:
        target.write_bytes(b"previous")
    adapter = Adapter("http://test", connector)
    kwargs = {"engine_id": "engine"} if kind == "engine" else {"job_id": "job"}
    method = adapter.get_engine_files_stream if kind == "engine" else adapter.get_result_stream
    if outcome in {"read_error", "write_error"}:
        with pytest.raises(ConnectionError if outcome == "read_error" else IsADirectoryError):
            method(output_path=tmp_path if outcome == "write_error" else target, **kwargs)
    else:
        response = method(output_path=target, **kwargs)
        assert response.is_success == (outcome == "success")
        assert target.read_bytes() == (b"firstsecond" if outcome == "success" else b"previous")
        assert response.code == ("NOT_FOUND" if outcome == "http_error" else None)
    if outcome == "http_error":
        raw.iter_content.assert_not_called()
        assert response.response is raw
    elif outcome != "write_error":
        raw.iter_content.assert_called_once_with(chunk_size=8192)
    route = "engines/engine/files" if kind == "engine" else "jobs/job/result"
    connector.get.assert_called_once_with(f"http://test/v1/{route}", stream=True)
    raw.close.assert_called_once()


@pytest.mark.parametrize('expires_at', [None, '2026-11-10T00:05:00Z'])
def test_adapter_progress_update_admin_and_worker_payload(expires_at):
    lease = {'id': '00000000-0000-0000-0000-000000000001',
             'server_time': '2026-11-10T00:00:00Z', 'lease_expire_at': expires_at}
    connector = Mock()
    connector.patch.return_value.status_code = 200
    connector.patch.return_value.headers = {'Content-Type': 'application/json'}
    connector.patch.return_value.json.return_value = {
        'status': 200, 'code': 'JOB_UPDATED', 'detail': 'Updated', 'data': lease}
    response = Adapter('http://test', connector).patch_job_progress_update(
        job_id='00000000-0000-0000-0000-000000000001', progress=0.5)
    assert response.is_success
    assert str(response.data.id) == lease['id']
    assert (response.data.lease_expire_at is None) == (expires_at is None)
