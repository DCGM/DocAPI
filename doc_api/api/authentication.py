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
from doc_api.api.schemas.responses import DocAPIClientErrorException, AppCode, DocAPIResponseClientError
from doc_api.db import model
from doc_api.api.config import config


logger = logging.getLogger(__name__)


# --- Accept keys from header, query, or cookie ---
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
api_key_query  = APIKeyQuery(name="api_key", auto_error=False)
api_key_cookie = APIKeyCookie(name="api_key", auto_error=False)

AUTHENTICATION_RESPONSES = {
    AppCode.API_KEY_INVALID: {
        "status": fastapi.status.HTTP_401_UNAUTHORIZED,
        "description": "Provided API key is invalid or malformed.",
        "model": DocAPIResponseClientError,
        "detail": "Authentication failed: the API key is invalid. "
                  f"To obtain an API key for {config.SERVER_NAME}, contact: {config.CONTACT_TO_GET_NEW_KEY}.",
    },
    AppCode.API_KEY_MISSING: {
        "status": fastapi.status.HTTP_401_UNAUTHORIZED,
        "description": "Missing API key in request headers.",
        "model": DocAPIResponseClientError,
        "detail": "Authentication failed: no API key was provided. "
                  f"To obtain an API key for {config.SERVER_NAME}, contact: {config.CONTACT_TO_GET_NEW_KEY}.",
    },
    AppCode.API_KEY_ROLE_FORBIDDEN: {
        "status": fastapi.status.HTTP_403_FORBIDDEN,
        "description": "The API key’s role is not permitted to access this endpoint.",
        "model": DocAPIResponseClientError,
        "detail": "Access denied: the API key does not have the required role.",
    },
    AppCode.API_KEY_INACTIVE: {
        "status": fastapi.status.HTTP_403_FORBIDDEN,
        "description": "The provided API key is inactive or revoked.",
        "model": DocAPIResponseClientError,
        "detail": "Authentication failed: the API key is inactive or revoked.",
    }
}

def require_api_key(*roles: KeyRole) -> Callable[..., "model.Key"]:
    """
    Dependency enforcing API key authentication + role authorization.
    - ADMIN is always allowed.
    """
    allowed = set(roles) if roles else set()
    allowed.add(KeyRole.ADMIN)

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
                detail=AUTHENTICATION_RESPONSES[AppCode.API_KEY_MISSING]["detail"].format(server_name=config.SERVER_NAME,
                                                                                          api_key_contact=config.CONTACT_TO_GET_NEW_KEY),
                headers={"WWW-Authenticate": f'ApiKey realm="{config.SERVER_NAME}"'},
            )

        async with open_session() as db:
            key = await lookup_key(db, provided)
            if key is None:
                raise DocAPIClientErrorException(
                    status=fastapi.status.HTTP_401_UNAUTHORIZED,
                    code=AppCode.API_KEY_INVALID,
                    detail=AUTHENTICATION_RESPONSES[AppCode.API_KEY_INVALID]["detail"].format(server_name=config.SERVER_NAME,
                                                                                              api_key_contact=config.CONTACT_TO_GET_NEW_KEY),
                    headers={"WWW-Authenticate": f'ApiKey realm="{config.SERVER_NAME}"'},
                )

            if not key.active:
                raise DocAPIClientErrorException(
                    status=fastapi.status.HTTP_403_FORBIDDEN,
                    code=AppCode.API_KEY_INACTIVE,
                    detail=AUTHENTICATION_RESPONSES[AppCode.API_KEY_INACTIVE]["detail"],
                )

            if key.role not in allowed:
                raise DocAPIClientErrorException(
                    status=fastapi.status.HTTP_403_FORBIDDEN,
                    code=AppCode.API_KEY_ROLE_FORBIDDEN,
                    detail=AUTHENTICATION_RESPONSES[AppCode.API_KEY_ROLE_FORBIDDEN]["detail"],
                )

            return key

    _dep.__require_api_key__ = True
    _dep.__require_api_key_roles__ = tuple(allowed)
    return _dep

def hmac_sha256_hex(s: str) -> str:
    return hmac.new(config.HMAC_SECRET.encode(), s.encode(), hashlib.sha256).hexdigest()

async def lookup_key(db: AsyncSession, provided_key: str) -> Optional[model.Key]:
    digest = hmac_sha256_hex(provided_key)

    result = await db.execute(select(model.Key).where(model.Key.key_hash == digest))
    key = result.scalar_one_or_none()
    if key is None:
        return None

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

require_admin_key = require_api_key()