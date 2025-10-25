import os
import asyncio
from typing import Optional

import httpx
import pytest
import pytest_asyncio

from doc_api.config import config

# -----------------------------------------------------------------------------
# CLI options & env fallbacks
# -----------------------------------------------------------------------------
def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--base-url",
        action="store",
        default=os.getenv("APP_BASE_URL", config.APP_BASE_URL),
        help="REQUIRED: Base URL of a running DocAPI instance (e.g., http://localhost:9999).",
    )
    parser.addoption("--api-key-readonly", action="store", default=os.getenv("TEST_READONLY_KEY", config.TEST_READONLY_KEY))
    parser.addoption("--api-key-user", action="store", default=os.getenv("TEST_USER_KEY", config.TEST_USER_KEY))
    parser.addoption("--api-key-worker", action="store", default=os.getenv("TEST_WORKER_KEY", config.TEST_WORKER_KEY))
    parser.addoption("--api-key-admin", action="store", default=os.getenv("TEST_ADMIN_KEY", config.TEST_ADMIN_KEY))
    parser.addoption("--http-timeout", action="store",
                     type=float,
                     default=float(os.getenv("TEST_HTTP_TIMEOUT", config.TEST_HTTP_TIMEOUT)),
                     help="HTTP client timeout in seconds.")


@pytest.fixture(scope="session")
def _opts(request):
    return {
        "APP_BASE_URL": request.config.getoption("--base-url"),
        "TEST_READONLY_KEY": request.config.getoption("--api-key-readonly"),
        "TEST_USER_KEY": request.config.getoption("--api-key-user"),
        "TEST_WORKER_KEY": request.config.getoption("--api-key-worker"),
        "TEST_ADMIN_KEY": request.config.getoption("--api-key-admin"),
        "TEST_HTTP_TIMEOUT": request.config.getoption("--http-timeout"),
    }


# -----------------------------------------------------------------------------
# Single event loop for the entire test session
# -----------------------------------------------------------------------------
@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


# -----------------------------------------------------------------------------
# Validate required APP_BASE_URL once per session
# -----------------------------------------------------------------------------
@pytest.fixture(scope="session", autouse=True)
def _require_base_url(_opts):
    if not _opts["APP_BASE_URL"]:
        pytest.exit(
            "Remote-only test run requires --base-url (or APP_BASE_URL env). "
            "Example: pytest --base-url http://localhost:9999",
            returncode=2,
        )


# -----------------------------------------------------------------------------
# httpx client pointed at the running instance
# -----------------------------------------------------------------------------
@pytest_asyncio.fixture()
async def client(_opts):
    timeout = httpx.Timeout(_opts["TEST_HTTP_TIMEOUT"])
    async with httpx.AsyncClient(base_url=_opts["APP_BASE_URL"], timeout=timeout, follow_redirects=True) as ac:
        yield ac


# -----------------------------------------------------------------------------
# Header fixtures (skip tests if a needed key isn't supplied)
# -----------------------------------------------------------------------------
def _headers_or_skip(key: Optional[str], which: str):
    if not key:
        pytest.skip(
            f"{which} API key not provided for remote target. "
            f"Pass --api-key-{which.lower()} or set TEST_{which.upper()}_KEY."
        )
    return {"X-API-Key": key}

@pytest.fixture()
def readonly_headers(_opts):
    return _headers_or_skip(_opts["TEST_READONLY_KEY"], "READONLY")

@pytest.fixture()
def user_headers(_opts):
    return _headers_or_skip(_opts["TEST_USER_KEY"], "USER")

@pytest.fixture()
def worker_headers(_opts):
    return _headers_or_skip(_opts["TEST_WORKER_KEY"], "WORKER")

@pytest.fixture()
def admin_headers(_opts):
    return _headers_or_skip(_opts["TEST_ADMIN_KEY"], "ADMIN")
