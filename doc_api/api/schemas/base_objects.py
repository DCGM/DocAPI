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



class Job(BaseModel):
    id: UUID = Field(
        ...,
        examples=["550e8400-e29b-41d4-a716-446655440000"],
        description="Unique identifier of the job."
    )

    state: ProcessingState = Field(
        ...,
        examples=[ProcessingState.PROCESSING.value],
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

