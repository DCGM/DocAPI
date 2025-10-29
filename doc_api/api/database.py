import os, asyncio
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.pool import NullPool
from doc_api.config import config

global_engine = None
global_async_session_maker = None
_init_lock = None

def _engine_kwargs():
    # loop-agnostic for tests, pytest creates event loop per test function, not currently in use
    # testing = os.getenv("TESTING") == "1"
    # if testing:
    #     return dict(poolclass=NullPool, pool_pre_ping=False)
    # prod/dev settings
    return dict(pool_pre_ping=True, pool_size=20, max_overflow=60)

async def _get_lock():
    global _init_lock
    if _init_lock is None:
        _init_lock = asyncio.Lock()  # created on *current* loop
    return _init_lock

async def _ensure_session_maker():
    global global_engine, global_async_session_maker
    if global_async_session_maker is None:
        lock = await _get_lock()
        async with lock:
            if global_async_session_maker is None:
                global_engine = create_async_engine(config.DATABASE_URL, **_engine_kwargs())
                global_async_session_maker = async_sessionmaker(global_engine, expire_on_commit=False)
    return global_async_session_maker

async def get_async_session():
    sm = await _ensure_session_maker()
    async with sm() as session:
        yield session

@asynccontextmanager
async def open_session():
    sm = await _ensure_session_maker()
    async with sm() as session:
        yield session


class DBError(Exception):
    pass




