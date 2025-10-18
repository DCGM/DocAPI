import hashlib
import hmac
import logging
from datetime import datetime, timezone
from typing import Optional, Callable

import fastapi
from fastapi import Security
from fastapi.security.api_key import APIKeyHeader, APIKeyQuery, APIKeyCookie
from sqlalchemy import update, select
from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.database import open_session
from doc_api.api.schemas.base_objects import KeyRole
from doc_api.api.schemas.responses import DocAPIClientErrorException, AppCode
from doc_api.db import model
from doc_api.config import config

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
        k_hdr: Optional[str] = Security(api_key_header),
        k_q:   Optional[str] = Security(api_key_query),
        k_ck:  Optional[str] = Security(api_key_cookie),
    ) -> model.Key:
        provided = k_hdr or k_q or k_ck
        if not provided:
            raise DocAPIClientErrorException(
                status=fastapi.status.HTTP_401_UNAUTHORIZED,
                code=AppCode.API_KEY_MISSING,
                detail=f"Missing API key, please provide an API key via header, query parameter, or cookie. "
                        f"To obtain an API key for {config.SERVER_NAME}, contact: {config.CONTACT_TO_GET_NEW_KEY}.",
                headers={"WWW-Authenticate": f'ApiKey realm="{config.SERVER_NAME}"'}
            )

        async with (open_session() as db):
            key = await lookup_key(db, provided)
            if key is None:
                raise DocAPIClientErrorException(
                    status=fastapi.status.HTTP_401_UNAUTHORIZED,
                    code=AppCode.API_KEY_INVALID,
                    detail=f"Invalid API key. "
                            f"To obtain an API key for {config.SERVER_NAME}, contact: {config.CONTACT_TO_GET_NEW_KEY}.",
                    headers={"WWW-Authenticate": f'ApiKey realm="{config.SERVER_NAME}"'}
                )
            if not key.active:
                raise DocAPIClientErrorException(
                    status=fastapi.status.HTTP_403_FORBIDDEN,
                    code=AppCode.API_KEY_INACTIVE,
                    detail=f"Inactive API key."
                )
            if key.role != KeyRole.ADMIN and key_role != key.role:
                raise DocAPIClientErrorException(
                    status=fastapi.status.HTTP_403_FORBIDDEN,
                    code=AppCode.API_KEY_INSUFFICIENT_ROLE,
                    detail=f"Insufficient API key role."
                )
            return key
    return _dep
