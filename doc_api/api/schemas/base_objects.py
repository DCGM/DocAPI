import enum
from datetime import datetime
from typing import Optional, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class ProcessingState(str, enum.Enum):
    NEW = 'new'
    QUEUED = 'queued'
    PROCESSING = 'processing'
    ERROR = 'error'
    DONE = 'done'
    CANCELLED = 'cancelled'
    FAILED = 'failed'


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
    id: UUID = Field(
        ...,
        examples=["550e8400-e29b-41d4-a716-446655440000"],
        description="Unique identifier of the job."
    )

    state: ProcessingState = Field(
        ...,
        examples=["PROCESSING"],
        description="Current state of the job."
    )

    progress: float = Field(
        ...,
        examples=[0.65],
        description="Progress of the job (0.0 - 1.0)."
    )

    previous_attempts: Optional[int] = Field(
        None,
        examples=[1],
        description="Number of previous attempts if the job was retried."
    )

    created_date: datetime = Field(
        ...,
        examples=["2025-10-17T09:00:00Z"],
        description="UTC timestamp when the job was created."
    )

    started_date: Optional[datetime] = Field(
        None,
        examples=["2025-10-17T09:05:00Z"],
        description="UTC timestamp when processing started."
    )

    last_change: datetime = Field(
        ...,
        examples=["2025-10-17T09:45:00Z"],
        description="UTC timestamp of the last state change."
    )

    finished_date: Optional[datetime] = Field(
        None,
        examples=["2025-10-17T09:50:00Z"],
        description="UTC timestamp when the job was finished (if applicable)."
    )

    log_user: Optional[str] = Field(
        None,
        examples=["string"],
        description="User or worker identifier associated with this job."
    )

    model_config = ConfigDict(from_attributes=True, extra="ignore")


class JobUpdate(BaseModel):
    id: UUID

    progress: Optional[float] = None

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

def model_example(model_cls):
    schema = model_cls.model_json_schema()
    return {p: f.get("examples", [f"<{p}>"])[0] for p, f in schema["properties"].items()}

