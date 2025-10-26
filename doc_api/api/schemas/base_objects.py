import enum
from datetime import datetime
from typing import Optional, get_origin, Union, get_args, List, Any
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
    READONLY = 'readonly'
    USER = 'user'
    WORKER = 'worker'
    ADMIN = 'admin'


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

    page_uploaded: bool = Field(
        ...,
        examples=[False],
        description="Indicates whether the corresponding PAGE XML file has been uploaded."
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
        description="Indicates whether the corresponding ALTO XML file has been uploaded."
    )

    page_uploaded: Optional[bool] = Field(
        None,
        examples=[False],
        description="Indicates whether the corresponding PAGE XML file has been uploaded."
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

    meta_json_uploaded :  bool = Field(
        ...,
        examples=[False],
        description="Whether Meta JSON file has been uploaded for this job."
    )

    meta_json_required : bool = Field(
        ...,
        examples=[True],
        description="Whether Meta JSON file is required for this job."
    )

    alto_required : bool = Field(
        ...,
        examples=[True],
        description="Whether ALTO XML file is required for this job."
    )

    page_required : bool = Field(
        ...,
        examples=[False],
        description="Whether PAGE XML file is required for this job."
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


class JobWithImages(Job):
    images: List[Image] = Field(
        ...,
        description="List of images associated with this job."
    )


class JobUpdate(BaseModel):
    meta_json_uploaded: Optional[bool] = Field(
        None,
        description="Whether the metadata JSON has been uploaded for this job.",
        examples=[True]
    )


class JobProgressUpdate(BaseModel):
    state: Optional[ProcessingState] = Field(
        None,
        description="New state of the job.",
        examples=[ProcessingState.PROCESSING.value]
    )

    progress: Optional[float] = Field(
        None,
        description=(
            "Current completion percentage of the job (0.0–100.0). "
            "Omit if progress has not changed since the last update."
        ),
        examples=[0.5]
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

    model_config = ConfigDict(extra="forbid")


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

    role: KeyRole = Field(
        ...,
        description="Role associated with this key, determining access level (e.g., ADMIN, USER, WORKER).",
        examples=[KeyRole.USER.value],
    )

    active: bool = Field(
        ...,
        description="Whether the key is currently active and permitted to access the API.",
        examples=[True],
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


class KeyNew(BaseModel):
    label: str = Field(
        ...,
        description="Human-readable label for identifying the key, e.g., the client or worker name.",
        examples=["My Application Key"],
    )
    role: KeyRole = Field(
        ...,
        description=f"Role to assign to the new key, determining access level (e.g. {', '.join(r.value for r in KeyRole)}).",
        examples=[KeyRole.USER.value, KeyRole.WORKER.value, KeyRole.ADMIN.value],
    )

    model_config = ConfigDict(extra="forbid")


class KeyUpdate(BaseModel):
    label: Optional[str] = None
    role: Optional[KeyRole] = None

    active: Optional[bool] = None

    model_config = ConfigDict(extra="forbid")


def model_example(model_type: Any) -> Any:
    """
    Build a simple example for:
      - a Pydantic model class
      - List[<Model>] / list[<Model>]
      - Optional[...] wrappers around the above
    Resolves $ref/$defs, arrays, enums, and common formats.
    """
    origin = get_origin(model_type)

    # Optional[T] / Union[T, None]
    if origin is Union:
        args = [a for a in get_args(model_type) if a is not type(None)]
        if len(args) == 1:
            return model_example(args[0])

    # List[T]
    if origin in (list, List):
        (item_type,) = get_args(model_type) or (Any,)
        return [model_example(item_type)]

    # Direct Pydantic model
    try:
        if issubclass(model_type, BaseModel):
            root_schema = model_type.model_json_schema()
            # The model schema itself describes an object; reuse the same resolver
            return _example_from_schema(root_schema, root_schema)
    except TypeError:
        pass  # not a class

    # Primitive fallbacks
    primitives = {str: "<string>", int: 0, float: 0.0, bool: False}
    if model_type in primitives:
        return primitives[model_type]

    return "<value>"

def _get_root_defs(schema: dict) -> dict:
    # pydantic v2 uses "$defs"; keep "definitions" fallback just in case
    return schema.get("$defs") or schema.get("definitions") or {}

def _resolve_ref(ref: str, root_schema: dict) -> dict:
    # only supports internal refs like "#/$defs/Model"
    if not ref.startswith("#/"):
        return {}
    target = root_schema
    for part in ref[2:].split("/"):
        if part not in target:
            return {}
        target = target[part]
    return target

def _first_present(d: dict, *keys: str) -> Optional[Any]:
    for k in keys:
        if k in d:
            return d[k]
    return None

def _primitive_example(type_: str, fmt: Optional[str]) -> Any:
    if type_ == "string":
        if fmt in ("uuid", "uuid4"):
            return "550e8400-e29b-41d4-a716-446655440000"
        if fmt in ("date-time",):
            return "2025-10-18T21:30:00+00:00"
        if fmt in ("date",):
            return "2025-10-18"
        if fmt in ("email",):
            return "user@example.com"
        if fmt in ("uri", "url"):
            return "https://example.com/resource"
        return "<string>"
    if type_ == "integer":
        return 0
    if type_ == "number":
        return 0.0
    if type_ == "boolean":
        return False
    return "<value>"

def _example_from_schema(schema: dict, root_schema: dict) -> Any:
    # 0) direct example(s)
    if "examples" in schema and schema["examples"]:
        return schema["examples"][0]
    if "example" in schema:
        return schema["example"]

    # 1) $ref
    if "$ref" in schema:
        ref_schema = _resolve_ref(schema["$ref"], root_schema)
        if ref_schema:
            return _example_from_schema(ref_schema, root_schema)

    # 2) combinators
    for key in ("allOf", "oneOf", "anyOf"):
        if key in schema and schema[key]:
            if key == "allOf":
                # merge object parts if possible; else take first resolvable
                acc = None
                for part in schema[key]:
                    ex = _example_from_schema(part, root_schema)
                    if isinstance(ex, dict):
                        acc = (acc or {}) | ex
                    elif acc is None:
                        acc = ex
                if acc is not None:
                    return acc
            else:
                return _example_from_schema(schema[key][0], root_schema)

    # 3) enum
    if "enum" in schema and schema["enum"]:
        return schema["enum"][0]

    # 4) arrays
    if _first_present(schema, "type") == "array" or "items" in schema:
        items_schema = schema.get("items", {})
        return [_example_from_schema(items_schema, root_schema)]

    # 5) objects
    if _first_present(schema, "type") == "object" or "properties" in schema:
        props = schema.get("properties", {}) or {}
        result = {}
        for name, ps in props.items():
            result[name] = _example_from_schema(ps, root_schema)
        return result

    # 6) primitives
    typ = schema.get("type")
    fmt = schema.get("format")
    if typ:
        return _primitive_example(typ, fmt)

    # 7) last resort
    return "<value>"

