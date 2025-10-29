import base64
import hashlib
import hmac
import logging
import os
import re
from datetime import datetime, timezone
from typing import Optional, Callable, Tuple

import fastapi
from fastapi import Security
from fastapi.security.api_key import APIKeyHeader, APIKeyQuery, APIKeyCookie
from sqlalchemy import update, select
from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.database import open_session
from doc_api.api.schemas.base_objects import KeyRole
from doc_api.api.schemas.responses import DocAPIClientErrorException, AppCode, DocAPIResponseClientError
from doc_api.db import model
from doc_api.config import config


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


require_admin_key = require_api_key()


async def lookup_key(db: AsyncSession, provided_key: str) -> Optional[model.Key]:
    try:
        kid, secret = parse_api_key(provided_key)
    except ValueError:
        return None

    result = await db.execute(
        select(model.Key).where(model.Key.kid == kid)
    )
    key = result.scalar_one_or_none()
    if key is None:
        return None

    digest = salted_hmac_sha256_hex(secret, key.salt)
    if not hmac.compare_digest(digest, key.key_hash):
        return None

    # best-effort touch
    try:
        now = datetime.now(timezone.utc)
        await db.execute(
            update(model.Key).where(model.Key.id == key.id).values(last_used=now)
        )
        await db.commit()
        key.last_used = now
    except Exception:
        await db.rollback()

    return key


KEY_RE = re.compile(r"^[^.]+\.(?P<kid>[A-Za-z0-9_-]+)\.(?P<secret>[A-Za-z0-9_-]+)$")

def _rand_urlsafe(nbytes: int) -> str:
    # URL-safe, no padding
    return base64.urlsafe_b64encode(os.urandom(nbytes)).rstrip(b"=").decode("ascii")

def salted_hmac_sha256_hex(secret: str, salt: str) -> str:
    # Simple and solid: HMAC over secret using (global_secret || salt) as key
    key = (config.HMAC_SECRET + salt).encode()
    return hmac.new(key, secret.encode(), hashlib.sha256).hexdigest()

def issue_key_components():
    kid = _rand_urlsafe(6)      # 8 chars ≈ 48 bits
    secret = _rand_urlsafe(30)  # 40 chars ≈ 240 bits
    salt = os.urandom(16).hex()
    return kid, secret, salt

def parse_api_key(api_key: str) -> Tuple[str, str]:
    m = KEY_RE.match(api_key)
    if not m:
        raise ValueError("Invalid API key format. Should be: KEY_PREFIX.kid.secret")
    return m.group("kid"), m.group("secret")

def make_api_key(*, kid: str, secret: str) -> str:
    return f"{config.KEY_PREFIX}.{kid}.{secret}"