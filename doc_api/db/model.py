from typing import Dict, Any

from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship, DeclarativeBase
from sqlalchemy import ForeignKey, DateTime
from sqlalchemy.types import String

from datetime import datetime, timezone
import uuid

from doc_api.api.schemas.base_objects import ProcessingState
from doc_api.api.schemas.base_objects import KeyRole

# converts ORM row object to dict
orm2dict = lambda r: {c.name: getattr(r, c.name) for c in r.__table__.columns}

# converts CORE row object to dict
row2dict = lambda r: dict(r._mapping)


class Base(DeclarativeBase):
    pass


class Job(Base):
    __tablename__ = 'jobs'
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    owner_key_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('keys.id'), index=True, nullable=False)
    worker_key_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('keys.id'), index=True, nullable=True)

    definition: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False)

    alto_required: Mapped[bool] = mapped_column(index=True, default=False, nullable=False)
    page_required: Mapped[bool] = mapped_column(index=True, default=False, nullable=False)
    meta_json_required: Mapped[bool] = mapped_column(index=True, default=False, nullable=False)
    meta_json_uploaded: Mapped[bool] = mapped_column(index=True, default=False, nullable=False)

    state: Mapped[ProcessingState] = mapped_column(index=True, default=ProcessingState.NEW, nullable=False)
    progress: Mapped[float] = mapped_column(index=True, default=0.0, nullable=False)
    previous_attempts: Mapped[int] = mapped_column(index=True, nullable=True)

    created_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True, nullable=False)
    started_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True, nullable=True)
    last_change: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True, nullable=False)
    finished_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True, nullable=True)

    log: Mapped[str] = mapped_column(nullable=True)
    log_user: Mapped[str] = mapped_column(nullable=True)

    images: Mapped[list['Image']] = relationship(back_populates="job", foreign_keys='Image.job_id')


class Image(Base):
    __tablename__ = 'images'
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)

    name: Mapped[str] = mapped_column(String(300), index=True, nullable=False)
    order: Mapped[int] = mapped_column(index=True, nullable=False)
    imagehash: Mapped[str] = mapped_column(index=True, nullable=True)

    image_uploaded: Mapped[bool] = mapped_column(index=True, default=False, nullable=False)
    alto_uploaded: Mapped[bool] = mapped_column(index=True, default=False, nullable=False)
    page_uploaded: Mapped[bool] = mapped_column(index=True, default=False, nullable=False)

    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('jobs.id'), index=True, nullable=False)
    job: Mapped['Job'] = relationship(back_populates="images", foreign_keys='Image.job_id')


class Key(Base):
    __tablename__ = "keys"
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)

    # Store only a hash (e.g. sha256). Use String(64) for hex digest
    key_hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)

    label: Mapped[str] = mapped_column(String(255), nullable=False, index=True, unique=True)
    role: Mapped[KeyRole] = mapped_column(index=True, default=KeyRole.USER, nullable=False)

    active: Mapped[bool] = mapped_column(default=True, nullable=False, index=True)
    readonly: Mapped[bool] = mapped_column(default=False, nullable=False, index=True)

    created_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False, index=True)
    last_used: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
