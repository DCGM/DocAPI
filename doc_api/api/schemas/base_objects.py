import enum
from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class ProcessingState(str, enum.Enum):
    PRISTINE = 'pristine'
    QUEUED = 'queued'
    PROCESSING = 'processing'
    ERROR = 'error'
    DONE = 'done'
    CANCELLED = 'cancelled'


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

