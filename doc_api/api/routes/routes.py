import logging
from functools import partial

from fastapi import Depends

from sqlalchemy.ext.asyncio import AsyncSession

from metakat.app.api.authentication import require_api_key
from metakat.app.api.cruds import cruds
from metakat.app.api.database import get_async_session
from metakat.app.api.schemas import base_objects
from metakat.app.db import model
from metakat.app.api.routes import router

from typing import Optional, List
from uuid import UUID

from pydantic import BaseModel


logger = logging.getLogger(__name__)


class MetakatImageForJobDefinition(BaseModel):
    id: Optional[UUID] = None
    name: str
    order: int

class MetakatJobDefinition(BaseModel):
    id: Optional[UUID] = None
    images: List[MetakatImageForJobDefinition]
    alto_required: bool = False
    proarc_json_required: bool = False

# USER
require_user_key = require_api_key()
################################################
@router.get("/me", tags=["User"])
async def me(key: model.Key = Depends(require_user_key)):
    return key.label


# ADMIN
require_admin_key = require_api_key(admin=True)
################################################
@router.get("/keys", response_model=List[base_objects.Key], tags=["Admin"])
async def get_keys(
        key: model.Key = Depends(require_admin_key),
        db: AsyncSession = Depends(get_async_session)):
    return await cruds.get_keys(db)

@router.get("/generate_key/{label}", response_model=str, tags=["Admin"])
async def new_key(label: str,
        key: model.Key = Depends(require_admin_key),
        db: AsyncSession = Depends(get_async_session)):
    return await cruds.new_key(db, label)

@router.post("/update_key", tags=["Admin"])
async def update_key(key_update: base_objects.KeyUpdate,
        key: model.Key = Depends(require_admin_key),
        db: AsyncSession = Depends(get_async_session)):
    await cruds.update_key(db, key_update)
