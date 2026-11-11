import asyncio
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from doc_api.config import config

global_engine = None
global_async_session_maker = None
_init_lock = None

def _engine_kwargs():
    return dict(pool_pre_ping=True, pool_size=config.DATABASE_POOL_SIZE,
                max_overflow=config.DATABASE_MAX_OVERFLOW, pool_timeout=config.DATABASE_POOL_TIMEOUT_SECONDS)

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






@asynccontextmanager
async def transaction(db: AsyncSession):
    """Join a caller-owned transaction, or commit a standalone CRUD operation."""
    if db.in_transaction():
        yield
        await db.flush()
    else:
        async with db.begin():
            yield
