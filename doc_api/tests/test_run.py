"""Launcher configuration; no server or deployment database is started."""
from unittest.mock import AsyncMock, Mock, call

import pytest

from doc_api import run
from doc_api.config import Config, config
from doc_api.db import db_create, db_update


@pytest.fixture
def runner_mocks(test_settings, monkeypatch):
    import logging.config
    import uvicorn

    operations = Mock()
    operations.attach_mock(AsyncMock(), "create")
    operations.attach_mock(Mock(), "migrate")
    operations.attach_mock(Mock(), "serve")
    monkeypatch.setattr(db_create, "create_database_if_does_not_exist", operations.create)
    monkeypatch.setattr(db_update, "init_and_update_db", operations.migrate)
    monkeypatch.setattr(uvicorn, "run", operations.serve)
    # LOGGING_CONFIG was constructed before the temporary settings were applied.
    monkeypatch.setattr(logging.config, "dictConfig", Mock())
    return operations


@pytest.mark.parametrize("value,expected", [(None, False), ("false", False), ("0", False), ("true", True), ("1", True)])
def test_production_configuration(monkeypatch, value, expected):
    monkeypatch.delenv("PRODUCTION", raising=False)
    if value is not None:
        monkeypatch.setenv("PRODUCTION", value)
    assert Config().PRODUCTION is expected


@pytest.mark.parametrize("production", [False, True], ids=["development", "production"])
@pytest.mark.parametrize("enabled_setting", [None, "DATABASE_REQUIRE_MATCHING_SCHEMA",
    "DATABASE_ALLOW_SCHEMA_UPDATE", "DATABASE_REQUIRE_MATCHING_COMMIT", "DATABASE_ALLOW_COMMIT_UPDATE"])
def test_runner_reload_and_database_setup(runner_mocks, monkeypatch, production, enabled_setting):
    monkeypatch.setattr(config, "PRODUCTION", production)
    for setting in ("DATABASE_REQUIRE_MATCHING_SCHEMA", "DATABASE_ALLOW_SCHEMA_UPDATE",
                    "DATABASE_REQUIRE_MATCHING_COMMIT", "DATABASE_ALLOW_COMMIT_UPDATE"):
        monkeypatch.setattr(config, setting, setting == enabled_setting)
    database_setup = enabled_setting is not None
    run.main()
    expected = [call.create(), call.migrate()] if database_setup else []
    expected.append(call.serve("doc_api.api.main:app", host=config.APP_HOST, port=config.APP_PORT,
                               timeout_keep_alive=config.HTTP_KEEP_ALIVE_TIMEOUT_SECONDS,
                               limit_concurrency=None if config.HTTP_CONCURRENCY_LIMIT == 0 else config.HTTP_CONCURRENCY_LIMIT,
                               backlog=config.HTTP_BACKLOG,
                               timeout_graceful_shutdown=None if config.HTTP_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS == 0
                               else config.HTTP_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS,
                               reload=not production, log_config=config.LOGGING_CONFIG))
    assert runner_mocks.mock_calls == expected
    if database_setup:
        runner_mocks.create.assert_awaited_once()


@pytest.mark.parametrize("operation", ["create", "migrate"])
def test_runner_database_failure_prevents_serving(runner_mocks, monkeypatch, operation):
    monkeypatch.setattr(config, "DATABASE_REQUIRE_MATCHING_SCHEMA", True)
    getattr(runner_mocks, operation).side_effect = RuntimeError("injected database setup failure")
    with pytest.raises(RuntimeError, match="injected database setup failure"):
        run.main()
    runner_mocks.create.assert_awaited_once()
    runner_mocks.serve.assert_not_called()
    if operation == "create":
        runner_mocks.migrate.assert_not_called()
    else:
        runner_mocks.migrate.assert_called_once_with()


@pytest.mark.parametrize("value,expected", [(None, 60), ("0", 0), ("15", 15), ("120", 120)])
def test_http_keep_alive_timeout_configuration(monkeypatch, value, expected):
    monkeypatch.delenv("HTTP_KEEP_ALIVE_TIMEOUT_SECONDS", raising=False)
    if value is not None:
        monkeypatch.setenv("HTTP_KEEP_ALIVE_TIMEOUT_SECONDS", value)
    assert Config().HTTP_KEEP_ALIVE_TIMEOUT_SECONDS == expected


@pytest.mark.parametrize("value", ["-1", "1.5", "nan", "inf", "invalid"])
def test_http_keep_alive_timeout_rejects_invalid_values(monkeypatch, value):
    monkeypatch.setenv("HTTP_KEEP_ALIVE_TIMEOUT_SECONDS", value)
    with pytest.raises(ValueError):
        Config()


@pytest.mark.parametrize("timeout", [0, 60, 120])
def test_runner_passes_http_keep_alive_timeout(runner_mocks, monkeypatch, timeout):
    monkeypatch.setattr(config, "HTTP_KEEP_ALIVE_TIMEOUT_SECONDS", timeout)
    for setting in ("DATABASE_REQUIRE_MATCHING_SCHEMA", "DATABASE_ALLOW_SCHEMA_UPDATE",
                    "DATABASE_REQUIRE_MATCHING_COMMIT", "DATABASE_ALLOW_COMMIT_UPDATE"):
        monkeypatch.setattr(config, setting, False)
    run.main()
    assert runner_mocks.serve.call_args.kwargs["timeout_keep_alive"] == timeout


RESOURCE_SETTINGS = [
    ("HTTP_CONCURRENCY_LIMIT", 64, "200", 200),
    ("HTTP_BACKLOG", 64, "128", 128),
    ("HTTP_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS", 30, "45", 45),
    ("DATABASE_POOL_SIZE", 10, "3", 3),
    ("DATABASE_MAX_OVERFLOW", 10, "2", 2),
    ("DATABASE_POOL_TIMEOUT_SECONDS", 10.0, "1.5", 1.5),
]


@pytest.mark.parametrize("setting,default,override,expected", RESOURCE_SETTINGS)
def test_resource_setting_defaults_and_overrides(monkeypatch, setting, default, override, expected):
    monkeypatch.delenv(setting, raising=False)
    assert getattr(Config(), setting) == default
    monkeypatch.setenv(setting, override)
    assert getattr(Config(), setting) == expected


@pytest.mark.parametrize("setting", [row[0] for row in RESOURCE_SETTINGS])
@pytest.mark.parametrize("value", ["-1", "nan", "inf", "invalid"])
def test_resource_settings_reject_invalid_values(monkeypatch, setting, value):
    monkeypatch.setenv(setting, value)
    with pytest.raises(ValueError):
        Config()


@pytest.mark.parametrize("setting", ["HTTP_BACKLOG", "DATABASE_POOL_SIZE", "DATABASE_POOL_TIMEOUT_SECONDS"])
def test_positive_resource_settings_reject_zero(monkeypatch, setting):
    monkeypatch.setenv(setting, "0")
    with pytest.raises(ValueError, match=setting):
        Config()


@pytest.mark.parametrize("setting", ["HTTP_CONCURRENCY_LIMIT", "HTTP_BACKLOG",
    "HTTP_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS", "DATABASE_POOL_SIZE", "DATABASE_MAX_OVERFLOW"])
def test_integer_resource_settings_reject_fractions(monkeypatch, setting):
    monkeypatch.setenv(setting, "1.5")
    with pytest.raises(ValueError, match=setting):
        Config()


@pytest.mark.parametrize("concurrency,backlog,shutdown", [(0, 64, 0), (64, 64, 30), (200, 128, 45)])
def test_runner_passes_resource_limits(runner_mocks, monkeypatch, concurrency, backlog, shutdown):
    monkeypatch.setattr(config, "HTTP_CONCURRENCY_LIMIT", concurrency)
    monkeypatch.setattr(config, "HTTP_BACKLOG", backlog)
    monkeypatch.setattr(config, "HTTP_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS", shutdown)
    for setting in ("DATABASE_REQUIRE_MATCHING_SCHEMA", "DATABASE_ALLOW_SCHEMA_UPDATE",
                    "DATABASE_REQUIRE_MATCHING_COMMIT", "DATABASE_ALLOW_COMMIT_UPDATE"):
        monkeypatch.setattr(config, setting, False)
    run.main()
    options = runner_mocks.serve.call_args.kwargs
    assert options["limit_concurrency"] == (None if concurrency == 0 else concurrency)
    assert options["backlog"] == backlog
    assert options["timeout_graceful_shutdown"] == (None if shutdown == 0 else shutdown)


@pytest.mark.parametrize("setting", ["HTTP_CONCURRENCY_LIMIT",
    "HTTP_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS", "DATABASE_MAX_OVERFLOW"])
def test_resource_settings_accept_explicit_zero(monkeypatch, setting):
    monkeypatch.setenv(setting, "0")
    assert getattr(Config(), setting) == 0
