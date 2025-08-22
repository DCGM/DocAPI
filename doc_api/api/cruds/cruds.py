import logging
import secrets
from typing import List

from sqlalchemy import select, exc
from sqlalchemy.ext.asyncio import AsyncSession

from metakat.app.api.authentication import hmac_sha256_hex
from metakat.app.api.database import DBError
from metakat.app.db import model
from metakat.app.api.schemas import base_objects


logger = logging.getLogger(__name__)


async def get_keys(db: AsyncSession) -> List[base_objects.Key]:
    try:
        result = await db.scalars(select(model.Key).order_by(model.Key.label))
        rows = result.all()
        return [base_objects.Key.model_validate(row) for row in rows]
    except exc.SQLAlchemyError as e:
        raise DBError('Failed reading keys from database', status_code=500) from e


KEY_PREFIX = "mk_"
KEY_BYTES = 32  # 32 bytes ≈ 256-bit entropy (recommended)

def generate_raw_key() -> str:
    # URL-safe Base64 without padding-ish chars; good for headers, query, and cookies
    return KEY_PREFIX + secrets.token_urlsafe(KEY_BYTES)

async def new_key(db: AsyncSession, label: str) -> str:
    """
    Create a new API key, store HMAC(key), return the RAW key string.
    Callers must display/return this once to the user and never log it.
    """

    try:
        #result = await db.execute(
        #    select(model.Key).where(model.Key.label == label)
        #)
        #key = result.scalar_one_or_none()
        #if key is not None:
        #    raise DBError(f"Key with label '{label}' already exists", status_code=409)

        # Retry loop in the vanishingly unlikely case of a hash collision
        for _ in range(3):
            raw_key = generate_raw_key()
            key_hash = hmac_sha256_hex(raw_key)

            # ensure uniqueness before insert (cheap existence check)
            existing = await db.execute(
                select(model.Key.key_hash).where(model.Key.key_hash == key_hash)
            )
            if existing.scalar_one_or_none() is not None:
                continue  # collision; regenerate

            try:
                db.add(model.Key(
                    label=label,
                    key_hash=key_hash
                ))
                await db.commit()
                return raw_key
            except exc.SQLAlchemyError:
                await db.rollback()
                continue

    except exc.SQLAlchemyError as e:
        raise DBError("Failed adding new key to database", status_code=500) from e
    raise DBError("Failed adding new key to database", status_code=409)


async def update_key(db: AsyncSession, key_update: base_objects.KeyUpdate) -> None:
    try:
        result = await db.execute(
            select(model.Key).where(model.Key.id == key_update.id)
        )
        db_key = result.scalar_one_or_none()
        if db_key is None:
            raise DBError(f"Key with id '{key_update.id}' does not exist", status_code=404)

        result = await db.execute(
            select(model.Key).where(model.Key.label == key_update.label)
        )
        key = result.scalar_one_or_none()
        if key is not None:
            raise DBError(f"Key with label '{key_update.label}' already exists", status_code=409)

        if key_update.label is not None:
            db_key.label = key_update.label
        if key_update.active is not None:
            db_key.active = key_update.active
        if key_update.admin is not None:
            db_key.admin = key_update.admin

        await db.commit()

    except exc.SQLAlchemyError as e:
        raise DBError("Failed updating key in database", status_code=500) from e






