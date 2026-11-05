"""Worker input selection and downloads, independent of the API server."""
from doc_worker.tests.helpers import input_download_response
from itertools import product
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import numpy as np
import pytest

from doc_api.adapter import Adapter, AdapterResponse
from doc_worker.doc_worker_wrapper import DocWorkerWrapper, WorkerResponse, WorkerResponseStatus


class InputWorker(DocWorkerWrapper):
    def process_job(self, *args, **kwargs):
        return WorkerResponse.ok()


@pytest.mark.parametrize("preferences", list(product([False, True], repeat=3)))
@pytest.mark.parametrize("uploaded,has_links", [(False, True), (True, True), (True, False)])
def test_worker_input_source_policy(tmp_path, preferences, uploaded, has_links):
    worker = InputWorker("https://api.invalid", Mock(), base_dir=str(tmp_path),
                         **{f"prefer_{kind}_url": prefer for kind, prefer in zip(["image", "alto", "page"], preferences) if prefer})
    image = SimpleNamespace(id=uuid4(), name="page.jpg", **{f"{kind}_uploaded": uploaded for kind in ["image", "alto", "page"]},
                            **{f"{kind}_url": f"https://files.invalid/{kind}" if has_links else None for kind in ["image", "alto", "page"]})
    worker.current_job = SimpleNamespace(id=uuid4(), images=[image], alto_required=True, page_required=True, meta_json_required=False)
    worker.get_job_data_path = Mock(return_value=str(tmp_path))
    worker.adapter = Mock()
    worker.adapter.get_image.return_value = AdapterResponse(data=np.zeros((2, 2, 3), dtype=np.uint8), status=200)
    worker.adapter.get_alto.return_value = AdapterResponse(data="<alto/>", status=200)
    worker.adapter.get_page.return_value = AdapterResponse(data="<PcGts/>", status=200)
    worker._download_external_input = Mock(side_effect=lambda url, kind: getattr(worker.adapter, f"get_{kind}").return_value.data)
    result = worker._download_job_data()
    assert result.status == WorkerResponseStatus.OK
    for kind, prefer in zip(["image", "alto", "page"], preferences):
        expected = getattr(image, f"{kind}_url") if has_links and (prefer or not uploaded) else None
        download = getattr(worker.adapter, f"get_{kind}")
        if expected is None:
            download.assert_called_once_with(str(image.id), worker.current_job.id)
        else:
            download.assert_not_called()
    assert worker._download_external_input.call_args_list == [
        ((getattr(image, f"{kind}_url"), kind), {})
        for kind, prefer in zip(["image", "alto", "page"], preferences)
        if has_links and (prefer or not uploaded)
    ]
    assert (tmp_path / "images" / "page.jpg").exists()
    assert (tmp_path / "alto" / "page.xml").read_text() == "<alto/>"
    assert (tmp_path / "page_xml" / "page.xml").read_text() == "<PcGts/>"


@pytest.mark.parametrize("failed_kind", [None, "image", "alto", "page"])
def test_worker_mixed_source_downloads(tmp_path, failed_kind, monkeypatch):
    worker = InputWorker("https://api.invalid", Mock(), base_dir=str(tmp_path))
    image = SimpleNamespace(id=uuid4(), name="page.jpg", image_uploaded=False, alto_uploaded=True, page_uploaded=False,
                            image_url="https://files.invalid/image", alto_url="https://files.invalid/unused-alto", page_url="https://files.invalid/page")
    worker.current_job = SimpleNamespace(id=uuid4(), images=[image], alto_required=True, page_required=True, meta_json_required=False)
    worker.get_job_data_path = Mock(return_value=str(tmp_path))
    connector = Mock()
    def external(url, **kwargs):
        kind = url.rsplit("/", 1)[-1]
        response = input_download_response(kind, 404 if failed_kind == kind else 200)
        response.url = url
        return response
    get = Mock(side_effect=external)
    monkeypatch.setattr("doc_worker.doc_worker_wrapper.requests.get", get)
    connector.get.return_value = input_download_response("alto", 404 if failed_kind == "alto" else 200)
    worker.adapter = Adapter("https://api.invalid", connector)
    result = worker._download_job_data()
    assert result.status == (WorkerResponseStatus.OK if failed_kind is None else WorkerResponseStatus.ERROR)
    if failed_kind is None:
        assert [call.args[0] for call in get.call_args_list] == [image.image_url, image.page_url]
        connector.get.assert_called_once_with(f"https://api.invalid/v1/jobs/{worker.current_job.id}/images/{image.id}/files/alto")
        assert (tmp_path / "images" / "page.jpg").exists()
        assert (tmp_path / "alto" / "page.xml").exists()
        assert (tmp_path / "page_xml" / "page.xml").exists()
    else:
        assert f"Failed to download {failed_kind if failed_kind == 'image' else failed_kind.upper()}" in result.final_log_user


@pytest.mark.parametrize("kind", ["image", "alto", "page"])
@pytest.mark.parametrize("status", [200, 404, 503])
def test_worker_external_input_download(tmp_path, monkeypatch, kind, status):
    worker = InputWorker("https://api.invalid", Mock(), base_dir=str(tmp_path))
    response = input_download_response(kind, status)
    response.url = "https://files.invalid/input"
    get = Mock(return_value=response)
    monkeypatch.setattr("doc_worker.doc_worker_wrapper.requests.get", get)
    if status == 200:
        result = worker._download_external_input(response.url, kind)
        if kind == "image":
            assert result.shape == (128, 128, 3)
        else:
            assert result == "<document>linked input</document>"
    else:
        import requests
        with pytest.raises(requests.HTTPError):
            worker._download_external_input(response.url, kind)
    get.assert_called_once_with(response.url, timeout=60)
    response.json.assert_not_called()


def test_worker_external_request_has_no_docapi_credentials(tmp_path, monkeypatch):
    from doc_api.connector import Connector
    connector = Connector("private-docapi-key")
    connector.session.cookies.set("docapi-cookie", "private")
    worker = InputWorker("https://api.invalid", connector, base_dir=str(tmp_path))
    get = Mock(return_value=input_download_response("alto"))
    monkeypatch.setattr("doc_worker.doc_worker_wrapper.requests.get", get)
    try:
        worker._download_external_input("https://files.invalid/input.xml", "alto")
        get.assert_called_once_with("https://files.invalid/input.xml", timeout=60)
    finally:
        connector.session.close()


@pytest.mark.parametrize("failure", ["timeout", "invalid_image", "invalid_xml"])
def test_worker_external_input_failure(tmp_path, monkeypatch, failure):
    import requests
    worker = InputWorker("https://api.invalid", Mock(), base_dir=str(tmp_path))
    kind = "alto" if failure == "invalid_xml" else "image"
    image = SimpleNamespace(name="page.jpg", **{f"{kind}_url": "https://files.invalid/input", f"{kind}_uploaded": False})
    response = input_download_response(kind)
    response._content = b"not an image" if kind == "image" else b"\xff"
    get = Mock(side_effect=requests.Timeout("source unavailable")) if failure == "timeout" else Mock(return_value=response)
    monkeypatch.setattr("doc_worker.doc_worker_wrapper.requests.get", get)
    result = worker._download_input(image, kind, str(tmp_path / "output"))
    assert result.status == WorkerResponseStatus.ERROR
    assert result.exception is not None
    assert result.error_adapter_response is None
    assert get.call_count == 1
    assert not (tmp_path / "output").exists()


@pytest.mark.parametrize("expires", [None, "2026-11-05T12:00:00+00:00"])
@pytest.mark.parametrize("updates", [{}, {"progress": 0.5}, {"log": "technical"}, {"log_user": "public"}])
def test_worker_progress_with_non_expiring_lease(tmp_path, expires, updates):
    worker = InputWorker("https://api.invalid", Mock(), base_dir=str(tmp_path))
    worker.current_job = SimpleNamespace(id=uuid4())
    worker.current_lease = SimpleNamespace(lease_expire_at=expires)
    worker.adapter = Mock()
    response = SimpleNamespace(is_success=True, data=SimpleNamespace(lease_expire_at=expires))
    worker.adapter.patch_job_progress_update.return_value = response
    worker.adapter.patch_job_lease.return_value = response
    assert worker.update_job_progress(**updates)
    if updates:
        worker.adapter.patch_job_progress_update.assert_called_once_with(
            progress=updates.get('progress'), log=updates.get('log'),
            log_user=updates.get('log_user'), job_id=worker.current_job.id)
        worker.adapter.patch_job_lease.assert_not_called()
    else:
        worker.adapter.patch_job_progress_update.assert_not_called()
        if expires is None:
            worker.adapter.patch_job_lease.assert_not_called()
        else:
            worker.adapter.patch_job_lease.assert_called_once_with(worker.current_job.id)


def test_worker_engine_download_always_streams(tmp_path):
    import io
    import zipfile
    from datetime import datetime, timezone
    worker = InputWorker("http://test", Mock(), base_dir=str(tmp_path))
    worker.current_job = SimpleNamespace(engine_id=uuid4(), engine_files_updated=datetime.now(timezone.utc))
    worker.adapter = Mock()
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w") as zipped:
        zipped.writestr("model.txt", "engine data")
    payload = archive.getvalue()
    def download(*, engine_id, output_path):
        from pathlib import Path
        Path(output_path).write_bytes(payload)
        return AdapterResponse(status=200, no_data_response=True)
    worker.adapter.get_engine_files_stream.side_effect = download
    worker.adapter.get_engine_files.side_effect = AssertionError("Worker must not buffer engine ZIPs")
    result = worker._check_and_download_engine_files()
    assert result.status == WorkerResponseStatus.OK
    engine_dir = next((tmp_path / "engines").iterdir())
    assert (engine_dir / "model.txt").read_text() == "engine data"
    assert not (engine_dir / "engine.zip").exists()
    worker.adapter.get_engine_files_stream.assert_called_once()
    worker.adapter.get_engine_files.assert_not_called()
