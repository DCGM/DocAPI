import json
from typing import Dict, Any

from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship, DeclarativeBase
from sqlalchemy import ForeignKey, DateTime
from sqlalchemy.types import String

from datetime import datetime, timezone
import uuid
import numpy as np
import math

from metakat.app.api.schemas.base_objects import ProcessingState
from metakat.app.api.schemas.base_objects import KeyRole

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
    proarc_json_required: Mapped[bool] = mapped_column(index=True, default=False, nullable=False)

    state: Mapped[ProcessingState] = mapped_column(index=True, default=ProcessingState.NEW, nullable=False)

    proarc_json_uploaded: Mapped[bool] = mapped_column(index=True, default=False, nullable=False)

    created_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.now(timezone.utc), index=True, nullable=False)
    started_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True, nullable=True)
    last_change: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.now(timezone.utc), index=True, nullable=False)
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

    created_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.now(timezone.utc), index=True, nullable=False)

    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('jobs.id'), index=True, nullable=False)
    job: Mapped['Job'] = relationship(back_populates="images", foreign_keys='Image.job_id')


class Key(Base):
    __tablename__ = "keys"
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)

    # Store only a hash (e.g. sha256). Use String(64) for hex digest
    key_hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)

    label: Mapped[str] = mapped_column(String(255), nullable=False, index=True, unique=True)
    active: Mapped[bool] = mapped_column(default=True, nullable=False, index=True)
    role: Mapped[KeyRole] = mapped_column(index=True, default=KeyRole.USER, nullable=False)

    created_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.now(timezone.utc), nullable=False, index=True)
    last_used: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True, index=True)


def json_to_points(json_points, precision=1):
    return round_floats(json.loads(json_points), precision)

def json_to_np_points(str_points, precision=1):
    return np.asarray(json_to_points(str_points, precision))

def points_to_json(points, precision=1):
    return json.dumps(round_floats(points, precision))

def round_floats(obj, precision=1):
    if isinstance(obj, float):
        if math.isnan(obj):
            obj = 0
        r = round(obj, precision)
        int_r = int(r)
        if r == int_r:
            return int_r
        else:
            return r
    elif isinstance(obj, dict):
        return dict((k, round_floats(v, precision)) for k, v in obj.items())
    elif isinstance(obj, (list, tuple)):
        return list(map(lambda o: round_floats(o, precision), obj))
    if isinstance(obj, np.ndarray):
        return round_floats(obj.tolist(), precision)
    return obj
