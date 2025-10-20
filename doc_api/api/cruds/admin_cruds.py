import secrets
import logging
from typing import Tuple, List, Optional
from uuid import UUID

from sqlalchemy import select, exc
from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.authentication import hmac_sha256_hex
from doc_api.api.database import DBError
from doc_api.api.schemas.responses import AppCode
from doc_api.config import config
from doc_api.db import model
from doc_api.api.schemas import base_objects


logger = logging.getLogger(__name__)


KEY_BYTES = 32  # 32 bytes ≈ 256-bit entropy (recommended)

def generate_raw_key() -> str:
    # URL-safe Base64 without padding-ish chars; good for headers, query, and cookies
    return config.KEY_PREFIX + secrets.token_urlsafe(KEY_BYTES)

async def new_key(*, db: AsyncSession, key_new: base_objects.KeyNew) -> Tuple[Optional[str], AppCode]:
    """
    Create a new API key, store HMAC(key), return the RAW key string.
    Callers must display/return this once to the user and never log it.
    """
    try:
        async with db.begin():
            result = await db.execute(
                select(model.Key).where(model.Key.label == key_new.label)
            )
            key = result.scalar_one_or_none()
            if key is not None:
                return None, AppCode.KEY_ALREADY_EXISTS

            # Retry loop in the vanishingly unlikely case of a hash collision
            for _ in range(3):
                raw_key = generate_raw_key()
                key_hash = hmac_sha256_hex(raw_key)

                # ensure uniqueness before insert (cheap existence check)
                existing = await db.execute(
                    select(model.Key.key_hash).where(model.Key.key_hash == key_hash)
                )
                if existing.scalar_one_or_none() is not None:
                    continue

                db.add(model.Key(
                    label=key_new.label,
                    role=key_new.role,
                    key_hash=key_hash
                ))
                return raw_key, AppCode.KEY_CREATED

            return None, AppCode.KEY_CREATION_FAILED

    except exc.SQLAlchemyError as e:
        raise DBError("Failed adding new key to database") from e


async def deactivate_key(*, db: AsyncSession, key_label: str) -> AppCode:
    try:
        async with db.begin():

            result = await db.execute(
                select(model.Key).where(model.Key.label == key_label).with_for_update()
            )
            key = result.scalar_one_or_none()
            if key is None:
                return AppCode.KEY_NOT_FOUND

            logger.info(f"Deactivating API key with label '{key_label}'")

            if not key.active:
                return AppCode.KEY_ALREADY_INACTIVE

            key.active = False

            return AppCode.KEY_DEACTIVATED

    except exc.SQLAlchemyError as e:
        raise DBError("Failed updating key in database") from e

async def get_keys(*, db: AsyncSession) -> Tuple[List[model.Key], AppCode]:
    try:
        async with db.begin():
            result = await db.scalars(select(model.Key).order_by(model.Key.label))
            return list(result.all()), AppCode.KEYS_RETRIEVED

    except exc.SQLAlchemyError as e:
        raise DBError('Failed reading keys from database') from e


