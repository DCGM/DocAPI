import logging
from typing import Tuple, List, Optional
from uuid import UUID

from sqlalchemy import select, exc, update, and_
from sqlalchemy.ext.asyncio import AsyncSession

from doc_api.api.authentication import salted_hmac_sha256_hex, issue_key_components, make_api_key
from doc_api.api.database import DBError
from doc_api.api.schemas.responses import AppCode
from doc_api.db import model
from doc_api.api.schemas import base_objects


logger = logging.getLogger(__name__)



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

            kid, secret, salt, digest = await get_secret(db=db)
            if secret is None:
                return None, AppCode.KEY_CREATION_FAILED

            db.add(model.Key(
                label=key_new.label,
                role=key_new.role,
                kid=kid,
                key_hash=digest,
                salt=salt
            ))
            return make_api_key(kid=kid, secret=secret), AppCode.KEY_CREATED

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

            kid, secret, salt, digest = await get_secret(db=db)
            if secret is None:
                return None, AppCode.KEY_SECRET_CREATION_FAILED

            key.kid = kid
            key.key_hash = digest
            key.salt = salt

            return make_api_key(kid=kid, secret=secret), AppCode.KEY_SECRET_CREATED


    except exc.SQLAlchemyError as e:
        raise DBError("Failed creating new secret for key") from e


async def get_secret(db: AsyncSession) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    kid = None
    secret = None
    salt = None
    digest = None

    # Retry loop in the vanishingly unlikely case of a hash collision
    for _ in range(3):
        kid, secret, salt = issue_key_components()
        digest = salted_hmac_sha256_hex(secret, salt)

        # ensure uniqueness before insert (cheap existence check)
        existing = await db.execute(
            select(model.Key.key_hash).where(model.Key.key_hash == digest)
        )
        if existing.scalar_one_or_none() is not None:
            kid = None
            secret = None
            salt = None
            digest = None
            continue

    return kid, secret, salt, digest


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


async def update_job(*, db: AsyncSession, job_id: UUID, job_update: base_objects.JobUpdate,
                     append_logs: bool = True) -> AppCode:
    try:
        async with db.begin():
            result = await db.execute(
                select(model.Job).where(model.Job.id == job_id).with_for_update()
            )
            db_job = result.scalar_one_or_none()

            if db_job is None:
                return AppCode.JOB_NOT_FOUND

            if job_update.state is not None:
                db_job.state = job_update.state

            if job_update.progress is not None:
                db_job.progress = job_update.progress

            if job_update.previous_attempts is not None:
                db_job.previous_attempts = job_update.previous_attempts

            if job_update.meta_json_uploaded is not None:
                db_job.meta_json_uploaded = job_update.meta_json_uploaded

            if job_update.meta_json_required is not None:
                db_job.meta_json_required = job_update.meta_json_required

            if job_update.alto_required is not None:
                db_job.alto_required = job_update.alto_required

            if job_update.page_required is not None:
                db_job.page_required = job_update.page_required

            if job_update.created_date is not None:
                db_job.created_date = job_update.created_date

            if job_update.started_date is not None:
                db_job.started_date = job_update.started_date

            if job_update.last_change is not None:
                db_job.last_change = job_update.last_change

            if job_update.finished_date is not None:
                db_job.finished_date = job_update.finished_date

            if job_update.log is not None:
                if append_logs:
                    if db_job.log is None:
                        db_job.log = job_update.log
                    else:
                        db_job.log += f"\n{job_update.log}"
                else:
                    db_job.log = job_update.log

            if job_update.log_user is not None:
                if append_logs:
                    if db_job.log_user is None:
                        db_job.log_user = job_update.log_user
                    else:
                        db_job.log_user += f"\n{job_update.log_user}"
                else:
                    db_job.log_user = job_update.log_user

            return AppCode.JOB_UPDATED

    except exc.SQLAlchemyError as e:
        raise DBError(f"Failed updating job in database") from e


async def new_engine(*, db: AsyncSession, engine_new: base_objects.EngineNew) -> Tuple[Optional[model.Engine], AppCode]:
    try:
        async with db.begin():
            result = await db.execute(
                select(model.Engine).
                where(model.Engine.name == engine_new.name).
                where(model.Engine.version == engine_new.version)
            )
            existing_engine = result.scalar_one_or_none()
            if existing_engine is not None:
                return None, AppCode.ENGINE_ALREADY_EXISTS

            # Unset existing defaults
            if engine_new.default:
                await db.execute(
                    update(model.Engine).
                    where(model.Engine.default.is_(True)).
                    values(default=False)
                )

            # Unset other active engines with the same name
            if engine_new.active:
                await db.execute(
                    update(model.Engine).
                    where(model.Engine.name == engine_new.name).
                    where(model.Engine.active.is_(True)).
                    values(active=False)
                )

            db_engine = model.Engine(
                name=engine_new.name,
                version=engine_new.version,
                description=engine_new.description,
                definition=engine_new.definition,
                default=engine_new.default,
                active=engine_new.active
            )

            db.add(db_engine)

            return db_engine, AppCode.ENGINE_CREATED

    except exc.SQLAlchemyError as e:
        raise DBError("Failed creating new engine") from e


async def update_engine(*, db: AsyncSession, engine_name: str, engine_version: str, engine_update: base_objects.EngineUpdate) -> AppCode:
    try:
        async with db.begin():
            result = await db.execute(
                select(model.Engine).
                where(model.Engine.name == engine_name).
                where(model.Engine.version == engine_version).with_for_update())
            db_engine = result.scalar_one_or_none()

            if db_engine is None:
                return AppCode.ENGINE_NOT_FOUND

            new_name = engine_update.name if engine_update.name is not None else db_engine.name
            new_version = engine_update.version if engine_update.version is not None else db_engine.version

            if (new_name, new_version) != (db_engine.name, db_engine.version):
                exists_row = await db.execute(
                    select(model.Engine.id)
                    .where(
                        model.Engine.name == new_name,
                        model.Engine.version == new_version,
                        model.Engine.id != db_engine.id,
                    )
                    .limit(1)
                )
                if exists_row.first() is not None:
                    return AppCode.ENGINE_ALREADY_EXISTS

            result = await db.execute(
                select(model.Engine).
                where(model.Engine.name == engine_update.name).
                where(model.Engine.version == engine_update.version).
                where(model.Engine.id != db_engine.id)
            )
            existing_engine = result.scalar_one_or_none()
            if existing_engine is not None:
                return AppCode.ENGINE_ALREADY_EXISTS

            if engine_update.name is not None:
                db_engine.name = engine_update.name

            if engine_update.version is not None:
                db_engine.version = engine_update.version

            if engine_update.description is not None:
                db_engine.description = engine_update.description

            if engine_update.definition is not None:
                db_engine.definition = engine_update.definition

            if engine_update.default is not None:
                if engine_update.default:
                    # Unset existing defaults
                    await db.execute(
                        update(model.Engine).
                        where(model.Engine.default.is_(True)).
                        values(default=False)
                    )
                db_engine.default = engine_update.default

            if engine_update.active is not None:
                if engine_update.active:
                    # Unset other active engines with the same name
                    await db.execute(
                        update(model.Engine).
                        where(model.Engine.name == db_engine.name).
                        where(model.Engine.active.is_(True)).
                        where(model.Engine.id != db_engine.id).
                        values(active=False)
                    )
                db_engine.active = engine_update.active

            return AppCode.ENGINE_UPDATED

    except exc.SQLAlchemyError as e:
        raise DBError("Failed updating engine") from e

