import asyncio
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from doc_api.config import config
import logging

logger = logging.getLogger(__name__)



global_engine = None
global_async_session_maker = None
_init_lock = asyncio.Lock()

async def _ensure_session_maker():
    global global_engine, global_async_session_maker
    if global_async_session_maker is None:          # fast-path
        async with _init_lock:                      # prevent races
            if global_async_session_maker is None:  # re-check inside lock
                global_engine = create_async_engine(
                    config.DATABASE_URL,
                    pool_pre_ping=True,
                    pool_size=20,
                    max_overflow=60,
                )
                global_async_session_maker = async_sessionmaker(
                    global_engine,
                    expire_on_commit=False,
                    autocommit=False,
                    autoflush=False,
                )
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




