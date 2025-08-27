import enum
from datetime import datetime
from typing import Optional, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class ProcessingState(str, enum.Enum):
    NEW = 'new'
    QUEUED = 'queued'
    PROCESSING = 'processing'
    ERROR = 'error'
    DONE = 'done'
    CANCELLED = 'cancelled'


class KeyRole(str, enum.Enum):
    USER = 'user'
    WORKER = 'worker'
    ADMIN = 'admin'


class Image(BaseModel):
    id: UUID

    name: str
    order: int

    image_uploaded: bool
    alto_uploaded: bool

    created_date: datetime

    model_config = ConfigDict(from_attributes=True, extra='ignore')


class Job(BaseModel):
    id: UUID

    state: ProcessingState

    created_date: datetime
    started_date: Optional[datetime] = None
    last_change: datetime
    finished_date: Optional[datetime] = None

    log_user: Optional[str] = None

    model_config = ConfigDict(from_attributes=True, extra='ignore')


class JobFinish(BaseModel):
    id: UUID

    state: Literal[ProcessingState.ERROR, ProcessingState.DONE]

    finished_date: datetime

    log: Optional[str] = None
    log_user: Optional[str] = None


class JobUpdate(BaseModel):
    id: UUID

    state: Optional[Literal[ProcessingState.PROCESSING]] = None

    last_change: datetime

    log: Optional[str] = None
    log_user: Optional[str] = None


class Key(BaseModel):
    id: UUID

    label: str
    active: bool
    role: KeyRole

    created_date: datetime
    last_used: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True, extra='ignore')


class KeyUpdate(BaseModel):
    id: UUID

    label: Optional[str] = None
    active: Optional[bool] = None
    role: Optional[KeyRole] = None

