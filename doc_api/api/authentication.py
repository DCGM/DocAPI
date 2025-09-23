import enum
import hashlib
import hmac
import logging
from datetime import datetime, timezone
from typing import Optional, Callable

from fastapi import Depends, Security, HTTPException
from fastapi.security.api_key import APIKeyHeader, APIKeyQuery, APIKeyCookie
from sqlalchemy import update, select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.status import HTTP_403_FORBIDDEN


from app.api.database import get_async_session
from app.api.schemas.base_objects import KeyRole
from metakat.app.db import model
from metakat.app.config import config

logger = logging.getLogger(__name__)


# --- Accept keys from header, query, or cookie ---
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
api_key_query  = APIKeyQuery(name="api_key", auto_error=False)
api_key_cookie = APIKeyCookie(name="api_key", auto_error=False)

def hmac_sha256_hex(s: str) -> str:
    return hmac.new(config.HMAC_SECRET.encode(), s.encode(), hashlib.sha256).hexdigest()

async def lookup_key(db: AsyncSession, provided_key: str) -> model.Key | None | bool:
    digest = hmac_sha256_hex(provided_key)

    result = await db.execute(select(model.Key).where(model.Key.key_hash == digest))
    key = result.scalar_one_or_none()
    if key is None:
        return None
    if not key.active:
        return False

    # Best-effort touch; failure must not block auth
    try:
        now = datetime.now(timezone.utc)
        await db.execute(
            update(model.Key)
              .where(model.Key.key_hash == digest)
              .values(last_used=now)
        )
        await db.commit()
        key.last_used = now  # reflect locally
    except Exception:
        await db.rollback()

    return key

def require_api_key(*, key_role: KeyRole = KeyRole.USER) -> Callable[..., "model.Key"]:
    async def _dep(
        db: AsyncSession = Depends(get_async_session),
        k_hdr: Optional[str] = Security(api_key_header),
        k_q:   Optional[str] = Security(api_key_query),
        k_ck:  Optional[str] = Security(api_key_cookie),
    ) -> model.Key:
        provided = k_hdr or k_q or k_ck
        if not provided:
            raise HTTPException(HTTP_403_FORBIDDEN, detail={"code": "MISSING_API_KEY", "message": "Missing API key"})

        key = await lookup_key(db, provided)
        if key is None:
            raise HTTPException(HTTP_403_FORBIDDEN, detail={"code": "INVALID_API_KEY", "message": "Invalid API key"})
        if not key.active:
            raise HTTPException(HTTP_403_FORBIDDEN, detail={"code": "INACTIVE_API_KEY", "message": "Inactive API key"})
        if key.role != KeyRole.ADMIN and key_role != key.role:
            raise HTTPException(HTTP_403_FORBIDDEN, detail={"code": "INSUFFICIENT_API_KEY_ROLE", "message": "Insufficient API key role"})
        return key
    return _dep
