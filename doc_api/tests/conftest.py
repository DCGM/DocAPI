# conftest.py
import os
import asyncio
import hmac
import hashlib

import httpx
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy import select

from doc_api.db import model
from doc_api.config import config
from doc_api.db.db_create import create_database_if_does_not_exist
from doc_api.db.db_update import init_and_update_db

TEST_DB_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql+asyncpg://postgres:postgres@localhost:5432/doc_api_db_test",
)

# ---------- Single event loop for the whole test session ----------
@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

# ---------- One-time DB bootstrap + key seeding ----------
def _init_db_sync():
    # Make sure the app will see test env + test DB
    os.environ["TESTING"] = "1"
    os.environ["DATABASE_URL"] = TEST_DB_URL
    config.DATABASE_URL = TEST_DB_URL

    # Create DB and run migrations/initialization
    asyncio.run(create_database_if_does_not_exist())
    init_and_update_db()

# ---------- HMAC helpers & test secrets ----------
def hmac_sha256_hex(s: str, secret: str) -> str:
    return hmac.new(secret.encode(), s.encode(), hashlib.sha256).hexdigest()

TEST_SECRETS = {
    "USER":   "user-secret",
    "WORKER": "worker-secret",
    "ADMIN":  "admin-secret",
}

@pytest.fixture(scope="session", autouse=True)
def test_hmac_secret():
    old = config.HMAC_SECRET
    test_secret = "test-hmac-secret"
    config.HMAC_SECRET = test_secret
    try:
        yield test_secret
    finally:
        config.HMAC_SECRET = old

async def _seed_keys_once(db_url: str, hmac_secret: str):
    # Use a throwaway engine/session just for seeding, then dispose.
    eng = create_async_engine(db_url, future=True, poolclass=NullPool)
    Session = async_sessionmaker(bind=eng, expire_on_commit=False)

    async with Session() as session:
        async def ensure(label: str, role: model.KeyRole, plain_secret: str):
            digest = hmac_sha256_hex(plain_secret, hmac_secret)
            existing = await session.scalar(select(model.Key).where(model.Key.label == label))
            if existing is None:
                session.add(model.Key(label=label, role=role, key_hash=digest, active=True))
            else:
                existing.key_hash = digest
                existing.role = role
                existing.active = True

        await ensure("test-user",   model.KeyRole.USER,   TEST_SECRETS["USER"])
        await ensure("test-worker", model.KeyRole.WORKER, TEST_SECRETS["WORKER"])
        await ensure("test-admin",  model.KeyRole.ADMIN,  TEST_SECRETS["ADMIN"])
        await session.commit()

    await eng.dispose()

@pytest.fixture(scope="session", autouse=True)
def _bootstrap_db(test_hmac_secret):
    _init_db_sync()
    asyncio.run(_seed_keys_once(TEST_DB_URL, test_hmac_secret))
    return True

# ---------- HTTP client (no DB overrides; app manages its own DB) ----------
@pytest_asyncio.fixture()
async def client():
    # Import AFTER env is set & DB bootstrapped so the app initializes for tests.
    from doc_api.api.main import app
    async with LifespanManager(app):
        transport = httpx.ASGITransport(app=app)  # compatible across httpx versions
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac

# ---------- Headers ----------
@pytest.fixture()
def user_headers():
    return {"X-API-Key": TEST_SECRETS["USER"]}

@pytest.fixture()
def worker_headers():
    return {"X-API-Key": TEST_SECRETS["WORKER"]}

@pytest.fixture()
def admin_headers():
    return {"X-API-Key": TEST_SECRETS["ADMIN"]}

# ---------- Temp dirs ----------
@pytest.fixture(autouse=True)
def temp_dirs(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "BASE_DIR", str(tmp_path))
    return tmp_path
