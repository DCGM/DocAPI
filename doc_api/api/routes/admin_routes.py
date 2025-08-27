import logging
from fastapi import Depends

from sqlalchemy.ext.asyncio import AsyncSession

from metakat.app.api.authentication import require_api_key
from metakat.app.api.cruds import cruds
from metakat.app.api.database import get_async_session
from metakat.app.api.schemas import base_objects
from metakat.app.db import model
from metakat.app.api.routes import admin_router

from typing import List


logger = logging.getLogger(__name__)


require_admin_key = require_api_key(key_role=base_objects.KeyRole.ADMIN)


@admin_router.get("/keys", response_model=List[base_objects.Key], tags=["Admin"])
async def get_keys(
        key: model.Key = Depends(require_admin_key),
        db: AsyncSession = Depends(get_async_session)):
    db_keys = await cruds.get_keys(db)
    return [base_objects.Key.model_validate(db_key) for db_key in db_keys]


@admin_router.get("/generate_key/{label}", response_model=str, tags=["Admin"])
async def new_key(label: str,
        key: model.Key = Depends(require_admin_key),
        db: AsyncSession = Depends(get_async_session)):
    return await cruds.new_key(db, label)


@admin_router.put("/update_key", tags=["Admin"])
async def update_key(key_update: base_objects.KeyUpdate,
        key: model.Key = Depends(require_admin_key),
        db: AsyncSession = Depends(get_async_session)):
    await cruds.update_key(db, key_update)