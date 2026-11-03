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
@pytest.mark.parametrize("force", [False, True], ids=["prepare-db", "force-db"])
def test_runner_reload_and_database_setup(runner_mocks, monkeypatch, production, force):
    monkeypatch.setattr(config, "PRODUCTION", production)
    monkeypatch.setattr(config, "DATABASE_FORCE", force)
    run.main()
    expected = [] if force else [call.create(), call.migrate()]
    expected.append(call.serve("doc_api.api.main:app", host=config.APP_HOST, port=config.APP_PORT,
                               reload=not production, log_config=config.LOGGING_CONFIG))
    assert runner_mocks.mock_calls == expected
    if not force:
        runner_mocks.create.assert_awaited_once()


@pytest.mark.parametrize("operation", ["create", "migrate"])
def test_runner_database_failure_prevents_serving(runner_mocks, monkeypatch, operation):
    monkeypatch.setattr(config, "DATABASE_FORCE", False)
    getattr(runner_mocks, operation).side_effect = RuntimeError("injected database setup failure")
    with pytest.raises(RuntimeError, match="injected database setup failure"):
        run.main()
    runner_mocks.create.assert_awaited_once()
    runner_mocks.serve.assert_not_called()
    if operation == "create":
        runner_mocks.migrate.assert_not_called()
    else:
        runner_mocks.migrate.assert_called_once_with()
