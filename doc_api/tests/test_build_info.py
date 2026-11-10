"""Build identity for source checkouts, installed packages and Docker."""
from subprocess import CalledProcessError, CompletedProcess, TimeoutExpired
from unittest.mock import Mock

import pytest

import doc_api.config as config_module


@pytest.fixture
def source_checkout(tmp_path, monkeypatch):
    monkeypatch.setenv("APP_GIT_COMMIT", "")
    monkeypatch.setattr(config_module, "__file__", str(tmp_path / "doc_api/config.py"))
    (tmp_path / ".git").mkdir()
    return tmp_path


def test_explicit_build_commit_is_preserved(monkeypatch):
    monkeypatch.setenv("APP_GIT_COMMIT", "a" * 40)
    run = Mock()
    monkeypatch.setattr(config_module.subprocess, "run", run)
    assert config_module.resolve_git_commit() == "a" * 40
    run.assert_not_called()


@pytest.mark.parametrize("value", ["../unsafe", "not-a-hash", "a" * 65])
def test_invalid_build_commit_is_rejected(monkeypatch, value):
    monkeypatch.setenv("APP_GIT_COMMIT", value)
    with pytest.raises(ValueError, match="APP_GIT_COMMIT"):
        config_module.resolve_git_commit()


def test_checkout_commit_is_exported_once(source_checkout, monkeypatch):
    import os
    run = Mock(return_value=CompletedProcess([], 0, "b" * 40 + "\n", ""))
    monkeypatch.setattr(config_module.subprocess, "run", run)
    assert config_module.resolve_git_commit() == "b" * 40
    assert os.environ["APP_GIT_COMMIT"] == "b" * 40
    assert config_module.resolve_git_commit() == "b" * 40
    run.assert_called_once_with(["git", "-C", str(source_checkout), "rev-parse", "HEAD"],
                                check=True, capture_output=True, text=True, timeout=2)


@pytest.mark.parametrize("error", [FileNotFoundError(), CalledProcessError(1, "git"), TimeoutExpired("git", 2)])
def test_git_failure_leaves_commit_unknown(source_checkout, monkeypatch, error):
    monkeypatch.setattr(config_module.subprocess, "run", Mock(side_effect=error))
    assert config_module.resolve_git_commit() is None


def test_installed_package_does_not_use_unrelated_checkout(source_checkout, monkeypatch):
    (source_checkout / ".git").rmdir()
    run = Mock()
    monkeypatch.setattr(config_module.subprocess, "run", run)
    assert config_module.resolve_git_commit() is None
    run.assert_not_called()
