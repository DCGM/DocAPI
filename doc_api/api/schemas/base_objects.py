import enum
from datetime import datetime
from typing import Optional, List
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class ProcessingState(str, enum.Enum):
    PRISTINE = 'pristine'
    QUEUED = 'queued'
    PROCESSING = 'processing'
    ERROR = 'error'
    DONE = 'done'
    CANCELLED = 'cancelled'


class Job(BaseModel):
    id: UUID

    state: ProcessingState

    created_date: datetime
    started_date: Optional[datetime] = None
    last_change: datetime
    finished_date: Optional[datetime] = None

    log_user: Optional[str] = None

    model_config = ConfigDict(from_attributes=True, extra='ignore')


class Key(BaseModel):
    id: UUID

    label: str
    active: bool
    admin: bool

    created_date: datetime
    last_used: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True, extra='ignore')

class KeyUpdate(BaseModel):
    id: UUID

    label: Optional[str] = None
    active: Optional[bool] = None
    admin: Optional[bool] = None

