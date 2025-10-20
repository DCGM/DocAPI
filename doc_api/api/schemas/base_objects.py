import enum
from datetime import datetime
from typing import Optional, Literal, get_origin, Union, get_args, List, Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class ProcessingState(str, enum.Enum):
    NEW = 'NEW'
    QUEUED = 'QUEUED'
    PROCESSING = 'PROCESSING'
    ERROR = 'ERROR'
    DONE = 'DONE'
    CANCELLED = 'CANCELLED'
    FAILED = 'FAILED'


class KeyRole(str, enum.Enum):
    USER = 'USER'
    WORKER = 'WORKER'
    ADMIN = 'ADMIN'


class Image(BaseModel):
    id: UUID = Field(
        ...,
        examples=["550e8400-e29b-41d4-a716-446655440000"],
        description="Unique identifier of the image."
    )

    name: str = Field(
        ...,
        examples=["page_001.jpg"],
        description="Original file name of the image."
    )

    order: int = Field(
        ...,
        examples=[1],
        description="Sequential order of the image within the associated document or job."
    )

    image_uploaded: bool = Field(
        ...,
        examples=[True],
        description="Indicates whether the image file has been successfully uploaded."
    )

    alto_uploaded: bool = Field(
        ...,
        examples=[False],
        description="Indicates whether the corresponding ALTO (OCR) file has been uploaded."
    )

    created_date: datetime = Field(
        ...,
        examples=["2025-10-17T09:00:00Z"],
        description="UTC timestamp when the image record was created."
    )

    model_config = ConfigDict(from_attributes=True, extra="ignore")


class ImageUpdate(BaseModel):
    image_uploaded: Optional[bool] = Field(
        None,
        examples=[True],
        description="Indicates whether the image file has been successfully uploaded."
    )

    alto_uploaded: Optional[bool] = Field(
        None,
        examples=[False],
        description="Indicates whether the corresponding ALTO (OCR) file has been uploaded."
    )

    imagehash: Optional[str] = Field(
        None,
        examples=["d41d8cd98f00b204e9800998ecf8427e"],
        description="MD5 hash of the image file for integrity verification."
    )


class Job(BaseModel):
    id: UUID = Field(
        ...,
        examples=["550e8400-e29b-41d4-a716-446655440000"],
        description="Unique identifier of the job."
    )

    state: ProcessingState = Field(
        ...,
        examples=[ProcessingState.QUEUED.value],
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
        examples=["2025-10-18T21:30:00+00:00"],
        description="UTC timestamp when the job was created."
    )

    started_date: Optional[datetime] = Field(
        None,
        examples=["2025-10-18T21:30:00+00:00"],
        description="UTC timestamp when processing started."
    )

    last_change: datetime = Field(
        ...,
        examples=["2025-10-18T21:30:00+00:00"],
        description="UTC timestamp of the last state change."
    )

    finished_date: Optional[datetime] = Field(
        None,
        examples=["2025-10-18T21:30:00+00:00"],
        description="UTC timestamp when the job was finished."
    )

    log_user: Optional[str] = Field(
        None,
        examples=["string"],
        description="User or worker identifier associated with this job."
    )

    model_config = ConfigDict(from_attributes=True, extra="ignore")


class JobUpdate(BaseModel):
    meta_json_uploaded: Optional[bool] = Field(
        None,
        description="Whether the metadata JSON has been uploaded for this job.",
        examples=[True]
    )


class JobProgressUpdate(BaseModel):
    """
    Partial update of an existing processing job.
    Sent periodically by the worker to report progress or append logs.
    """
    progress: Optional[float] = Field(
        None,
        description=(
            "Current completion percentage of the job (0.0–100.0). "
            "Omit if progress has not changed since the last update."
        ),
        examples=[42.5]
    )

    log: Optional[str] = Field(
        None,
        description=(
            "Technical or debug log text appended to the internal system log. "
            "Used for diagnostics or backend monitoring."
        ),
        examples=["Loaded 500 image tiles and initialized OCR engine."]
    )

    log_user: Optional[str] = Field(
        None,
        description=(
            "User-facing log message displayed in the interface. "
            "Helps indicate current processing step or progress in human-readable form."
        ),
        examples=["Processing page 12 of 58."]
    )

    model_config = ConfigDict(from_attributes=True, extra="ignore")



class JobLease(BaseModel):
    """
    Represents a temporary lease (heartbeat) information for a processing job.
    Returned when a worker reports activity to confirm that it is still alive.
    """
    id: UUID = Field(
        ...,
        description="Unique identifier of the job whose lease was renewed.",
        examples=["9fdc1a4c-022c-4ba3-9ea4-69dcfeb1b9d3"]
    )
    lease_expire_at: datetime = Field(
        ...,
        description=(
            "UTC timestamp when the job's lease will expire if no further heartbeats are received. "
            "After this moment, the job may be considered stale and reassigned."
        ),
        examples=["2025-10-18T21:30:00+00:00"]
    )
    server_time: datetime = Field(
        ...,
        description=(
            "UTC time on the server when this lease information was generated. "
            "Workers can use this to estimate when to send the next heartbeat."
        ),
        examples=["2025-10-18T21:30:00+00:00"]
    )

    model_config = ConfigDict(from_attributes=True, extra="ignore")


class Key(BaseModel):
    """
    Represents an API key that authorizes access to the system.
    Each key has a unique identifier, descriptive label, assigned role,
    and timestamps for creation and last usage.
    """

    id: UUID = Field(
        ...,
        description="Unique identifier of the API key (UUIDv4).",
        examples=["c69bb0b5-16b8-47d4-9f78-c5a1d3f4f2d7"],
    )

    label: str = Field(
        ...,
        description="Human-readable label for identifying the key, e.g., the client or worker name.",
        examples=["My Application Key"],
    )

    active: bool = Field(
        ...,
        description="Whether the key is currently active and permitted to access the API.",
        examples=[True],
    )

    role: KeyRole = Field(
        ...,
        description="Role associated with this key, determining access level (e.g., ADMIN, USER, WORKER).",
        examples=[KeyRole.USER.value],
    )

    created_date: datetime = Field(
        ...,
        description="Timestamp when the key was created (in UTC).",
        examples=["2025-01-15T10:24:30+00:00"],
    )

    last_used: Optional[datetime] = Field(
        None,
        description="Timestamp of the last successful usage of this key (in UTC). May be null if unused.",
        examples=["2025-10-20T07:55:10+00:00"],
    )

    model_config = ConfigDict(from_attributes=True, extra="ignore")


class KeyUpdate(BaseModel):
    id: UUID

    label: Optional[str] = None
    active: Optional[bool] = None
    role: Optional[KeyRole] = None


def model_example(model_type: Any) -> Any:
    """
    Build a simple example for either:
      - a Pydantic model class
      - List[<PydanticModel>]
      - Optional[...] wrappers around the above
    Falls back to placeholders like <field> when no examples are provided.
    """

    # 1) Unwrap Optional[T] / Union[T, None]
    origin = get_origin(model_type)
    if origin is Union:
        args = [a for a in get_args(model_type) if a is not type(None)]
        # If it's Optional[T], pick T
        if len(args) == 1:
            return model_example(args[0])

    # 2) Handle List[T] (or list[T]) recursively
    if origin in (list, List):
        (item_type,) = get_args(model_type) or (Any,)
        return [model_example(item_type)]

    # 3) Handle direct Pydantic model classes
    try:
        if issubclass(model_type, BaseModel):
            schema = model_type.model_json_schema()
            props = schema.get("properties", {}) or {}
            return {
                name: (field_schema.get("examples", [f"<{name}>"])[0])
                for name, field_schema in props.items()
            }
    except TypeError:
        # model_type might not be a class (e.g., typing annotations)
        pass

    # 4) Primitive fallbacks (if you ever call with plain types)
    primitives = {
        str: "<string>",
        int: 0,
        float: 0.0,
        bool: False,
    }
    if model_type in primitives:
        return primitives[model_type]

    # 5) Unknown type: generic placeholder
    return "<value>"

