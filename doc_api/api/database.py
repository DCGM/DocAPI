from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import exc

from metakat.app.config import config
from typing import AsyncGenerator

import logging


global_engine = None
global_async_session_maker = None

logger = logging.getLogger(__name__)


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    global global_engine, global_async_session_maker
    if global_engine is None:
        global_engine = create_async_engine(config.DATABASE_URL,
                                            pool_pre_ping=True,
                                            pool_size=20,
                                            max_overflow=60)
        global_async_session_maker = async_sessionmaker(global_engine,
                                                        expire_on_commit=False,
                                                        autocommit=False,
                                                        autoflush=False)
    async with global_async_session_maker() as session:
        yield session


class DBError(Exception):
    def __init__(self, message: str, *, status_code: int = 400, code: str = "DB_ERROR"):
        super().__init__(message)
        self.status_code = status_code
        self.code = code




