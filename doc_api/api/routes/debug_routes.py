import logging
from fastapi import Depends, HTTPException

from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.authentication import require_api_key
from doc_api.api.database import get_async_session
from doc_api.api.schemas import base_objects
from doc_api.db import model
from doc_api.api.routes import debug_router

from typing import List


logger = logging.getLogger(__name__)


require_admin_key = require_api_key(key_role=base_objects.KeyRole.ADMIN)


@debug_router.get("/http_exception", response_model=List[base_objects.Key], tags=["Debug"])
async def get_keys(
        key: model.Key = Depends(require_admin_key),
        db: AsyncSession = Depends(get_async_session)):
    raise HTTPException(status_code=418, detail="This is a debug HTTP exception.")

