import secrets
import logging
from typing import Tuple, List, Optional

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

            secret, key_hash = await get_secret(db=db)
            if secret is None:
                return None, AppCode.KEY_CREATION_FAILED

            db.add(model.Key(
                label=key_new.label,
                role=key_new.role,
                key_hash=key_hash
            ))
            return secret, AppCode.KEY_CREATED

    except exc.SQLAlchemyError as e:
        raise DBError("Failed adding new key") from e


async def new_secret(*, db: AsyncSession, label: str) -> Tuple[Optional[str], AppCode]:
    try:
        async with db.begin():
            result = await db.execute(
                select(model.Key).where(model.Key.label == label).with_for_update()
            )
            key = result.scalar_one_or_none()
            if key is None:
                return None, AppCode.KEY_NOT_FOUND

            secret, key_hash = await get_secret(db=db)
            if secret is None:
                return None, AppCode.KEY_SECRET_CREATION_FAILED

            key.key_hash = key_hash

            return secret, AppCode.KEY_SECRET_CREATED


    except exc.SQLAlchemyError as e:
        raise DBError("Failed creating new secret for key") from e


async def get_secret(db: AsyncSession) -> Tuple[Optional[str], Optional[str]]:
    secret = None
    key_hash = None

    # Retry loop in the vanishingly unlikely case of a hash collision
    for _ in range(3):
        secret = generate_raw_key()
        key_hash = hmac_sha256_hex(secret)

        # ensure uniqueness before insert (cheap existence check)
        existing = await db.execute(
            select(model.Key.key_hash).where(model.Key.key_hash == key_hash)
        )
        if existing.scalar_one_or_none() is not None:
            continue

    return secret, key_hash


async def update_key(*, db: AsyncSession, label: str, key_update: base_objects.KeyUpdate) -> AppCode:
    try:
        async with db.begin():

            result = await db.execute(
                select(model.Key).where(model.Key.label == label).with_for_update()
            )
            key = result.scalar_one_or_none()
            if key is None:
                return AppCode.KEY_NOT_FOUND

            if key_update.label is not None:
                already_exists = await db.execute(
                    select(model.Key).where(
                        model.Key.label == key_update.label,
                        model.Key.id != key.id
                    )
                )
                if already_exists.scalar_one_or_none() is not None:
                    return AppCode.KEY_ALREADY_EXISTS
                key.label = key_update.label
            if key_update.role is not None:
                key.role = key_update.role
            if key_update.active is not None:
                key.active = key_update.active

            return AppCode.KEY_UPDATED

    except exc.SQLAlchemyError as e:
        raise DBError("Failed updating key") from e


async def get_keys(*, db: AsyncSession) -> Tuple[List[model.Key], AppCode]:
    try:
        async with db.begin():
            result = await db.scalars(select(model.Key).order_by(model.Key.label))
            return list(result.all()), AppCode.KEYS_RETRIEVED

    except exc.SQLAlchemyError as e:
        raise DBError('Failed reading keys') from e


