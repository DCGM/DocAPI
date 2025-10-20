import enum, logging
from typing import Generic, TypeVar, Optional, Any, Mapping, Dict, Type, get_origin, get_args

import fastapi
from pydantic import BaseModel, Field, model_validator, field_validator
from fastapi.responses import JSONResponse, Response
from collections import defaultdict

from doc_api.api.schemas.base_objects import model_example


logger = logging.getLogger(__name__)

# Naming convention for AppCode: CATEGORY_ACTION
class AppCode(str, enum.Enum):
    API_KEY_VALID = 'API_KEY_VALID'

    # User-related
    JOB_CREATED = 'JOB_CREATED'
    JOB_STARTED = 'JOB_STARTED'
    JOB_ALREADY_STARTED = 'JOB_ALREADY_STARTED'
    JOB_NOT_READY = 'JOB_NOT_READY'
    JOB_CANCELLED = 'JOB_CANCELLED'
    JOB_FINISHED = 'JOB_FINISHED'

    META_JSON_UPLOADED = 'META_JSON_UPLOADED'
    META_JSON_REUPLOADED = 'META_JSON_REUPLOADED'
    META_JSON_NOT_REQUIRED = 'META_JSON_NOT_REQUIRED'

    IMAGE_UPLOADED = 'IMAGE_UPLOADED'
    IMAGE_REUPLOADED = 'IMAGE_REUPLOADED'
    IMAGE_INVALID = 'IMAGE_INVALID'
    IMAGE_NOT_FOUND = 'IMAGE_NOT_FOUND'
    IMAGE_UPDATED = 'IMAGE_UPDATED'

    ALTO_UPLOADED = 'ALTO_UPLOADED'
    ALTO_REUPLOADED = 'ALTO_REUPLOADED'
    ALTO_NOT_REQUIRED = 'ALTO_NOT_REQUIRED'
    XML_PARSE_ERROR = 'XML_PARSE_ERROR'
    ALTO_SCHEMA_INVALID = 'ALTO_SCHEMA_INVALID'

    # Worker-related
    JOB_ASSIGNED = 'JOB_ASSIGNED'
    JOB_QUEUE_EMPTY = 'JOB_QUEUE_EMPTY'
    JOB_NOT_FOUND = 'JOB_NOT_FOUND'
    JOB_RETRIEVED = 'JOB_RETRIEVED'
    JOBS_RETRIEVED = 'JOBS_RETRIEVED'
    JOB_NOT_IN_PROCESSING = 'JOB_NOT_IN_PROCESSING'
    JOB_NOT_IN_NEW = 'JOB_NOT_IN_NEW'
    JOB_UPDATED = 'JOB_UPDATED'
    JOB_HEARTBEAT_ACCEPTED = 'JOB_HEARTBEAT_ACCEPTED'
    JOB_INVALID_STATE = 'JOB_INVALID_STATE'
    JOB_COMPLETED = 'JOB_COMPLETED'
    JOB_ALREADY_COMPLETED = 'JOB_ALREADY_COMPLETED'
    JOB_FAILED = 'JOB_FAILED'
    JOB_ALREADY_FAILED = 'JOB_ALREADY_FAILED'

    IMAGES_RETRIEVED = 'IMAGES_RETRIEVED'
    IMAGE_RETRIEVED = 'IMAGE_RETRIEVED'
    IMAGE_NOT_FOUND_FOR_JOB = 'IMAGE_NOT_FOUND_FOR_JOB'

    IMAGE_NOT_UPLOADED = 'IMAGE_NOT_UPLOADED'
    ALTO_NOT_UPLOADED = 'ALTO_NOT_UPLOADED'
    META_JSON_NOT_UPLOADED = 'META_JSON_NOT_UPLOADED'

    IMAGE_DOWNLOADED = 'IMAGE_DOWNLOADED'
    ALTO_DOWNLOADED = 'ALTO_DOWNLOADED'
    META_JSON_DOWNLOADED = 'META_JSON_DOWNLOADED'

    JOB_RESULT_RETRIEVED = 'JOB_RESULT_RETRIEVED'
    JOB_RESULT_NOT_READY = 'JOB_RESULT_NOT_READY'
    JOB_RESULT_GONE = 'JOB_RESULT_GONE'

    JOB_RESULT_INVALID = 'JOB_RESULT_INVALID'
    JOB_RESULT_UPLOADED = 'JOB_RESULT_UPLOADED'
    JOB_RESULT_MISSING = 'JOB_RESULT_MISSING'


    HTTP_ERROR = 'HTTP_ERROR'
    REQUEST_VALIDATION_ERROR = 'REQUEST_VALIDATION_ERROR'

    API_KEY_MISSING = 'API_KEY_MISSING'
    API_KEY_INVALID = 'API_KEY_INVALID'
    API_KEY_INACTIVE = 'API_KEY_INACTIVE'
    API_KEY_ROLE_FORBIDDEN = 'API_KEY_ROLE_FORBIDDEN'
    API_KEY_FORBIDDEN_FOR_JOB = 'API_KEY_FORBIDDEN_FOR_JOB'
    API_KEY_NOT_FOUND = 'API_KEY_NOT_FOUND'

    # 5xx
    INTERNAL_ERROR = 'INTERNAL_ERROR'

DETAILS_GENERAL = {
    # 4xx
    AppCode.HTTP_ERROR: "An HTTP error occurred.",
    AppCode.REQUEST_VALIDATION_ERROR: "The request could not be validated.",

    # 5xx
    AppCode.INTERNAL_ERROR: "An internal server error occurred.",
}

T = TypeVar("T")

class DocAPIResponseBase(BaseModel):
    status: int = Field(..., description="HTTP status code.")
    code: AppCode = Field(..., description="Application-specific code.")
    detail: str = Field(..., description="Human-readable message.")

    @field_validator("status")
    def check_valid_http_code(cls, v: int) -> int:
        if not (100 <= v <= 599):
            raise ValueError(f"Invalid HTTP status code: {v}")
        return v


class DocAPIResponseOK(DocAPIResponseBase, Generic[T]):
    """2xx envelope."""
    data: Optional[T] = Field(
        None,
        description="Optional data payload associated with the response."
    )

    @model_validator(mode="after")
    def _ensure_2xx(self):
        code = int(self.status)
        if not (200 <= code <= 299):
            raise ValueError(f"DocAPIResponseOK requires 2xx status_code, got {code}")
        return self


class DocAPIResponseClientError(DocAPIResponseBase):
    """4xx envelope."""
    details: Optional[Any] = Field(
        None, description="Optional error details."
    )

    @model_validator(mode="after")
    def _ensure_4xx(self):
        code = int(self.status)
        if not (400 <= code <= 499):
            raise ValueError(f"DocAPIResponseClientError requires 4xx status_code, got {code}")
        return self

class DocAPIClientErrorException(Exception):
    def __init__(self, *, status: int, code: AppCode, detail: str, headers: Optional[Mapping[str, str]] = None):
        self.status = status
        self.code = code
        self.detail = detail
        self.headers = headers
        super().__init__(detail)


class DocAPIResponseServerError(DocAPIResponseBase):
    """5xx envelope."""
    details: Optional[Any] = Field(
        None, description="Optional error details."
    )

    @model_validator(mode="after")
    def _ensure_5xx(self):
        code = int(self.status)
        if not (500 <= code <= 599):
            raise ValueError(f"DocAPIResponseServerError requires 5xx status_code, got {code}")
        return self


NO_BODY_STATUSES = {fastapi.status.HTTP_204_NO_CONTENT, fastapi.status.HTTP_205_RESET_CONTENT}

def validate_ok_response(payload: DocAPIResponseOK[T]) -> Response:
    """
    Render a 2xx response, for 200 strictly prefer returning Pydantic model
    directly from route and use FastAPI response_model for validation.
    Policy:
      - 204/205 => empty Response (no body) - RFC: 204/205 MUST NOT include a body.
      - Other 2xx => DocAPIResponseOK[T] as JSONResponse
    """
    if payload.status in NO_BODY_STATUSES:
        return Response(status_code=payload.status)

    return JSONResponse(status_code=payload.status, content=payload.model_dump(mode="json"))


def validate_client_error_response(payload: DocAPIResponseClientError, headers: Optional[Mapping[str, str]] = None) -> JSONResponse:
    """Render a validated 4xx error."""
    hdrs: Optional[dict[str, str]] = None
    if headers:
        filtered: dict[str, str] = {}
        for k, v in headers.items():
            if v is not None:  # skip None values
                filtered[str(k)] = str(v)
        hdrs = filtered or None

    return JSONResponse(
        status_code=int(payload.status),
        content=payload.model_dump(mode="json", exclude_none=True),
        headers=hdrs
    )


def validate_server_error_response(payload: DocAPIResponseServerError) -> JSONResponse:
    """Render a validated 5xx error."""
    return JSONResponse(
        status_code=int(payload.status),
        content=payload.model_dump(mode="json", exclude_none=True)
    )

GENERAL_RESPONSES = {
    AppCode.JOB_NOT_FOUND: {
        "status": fastapi.status.HTTP_404_NOT_FOUND,
        "description": "The specified job does not exist.",
        "model": DocAPIResponseClientError,
        "detail": "Job does not exist.",
    },
    AppCode.API_KEY_FORBIDDEN_FOR_JOB: {
        "status": fastapi.status.HTTP_403_FORBIDDEN,
        "description": "The API key does not have access to the specified job.",
        "model": DocAPIResponseClientError,
        "detail": "The API key does not have access to the job.",
    },
    AppCode.IMAGE_NOT_FOUND_FOR_JOB: {
        "status": fastapi.status.HTTP_404_NOT_FOUND,
        "description": "The specified image does not exist for the given job.",
        "model": DocAPIResponseClientError,
        "detail": "Image does not exist for the specified job.",
    },
    AppCode.XML_PARSE_ERROR: {
        "status": fastapi.status.HTTP_400_BAD_REQUEST,
        "description": "Invalid XML file.",
        "model": DocAPIResponseClientError,
        "detail": "Failed to parse the XML file.",
    }
}

def make_responses(spec: Dict[Any, Dict[str, Any]], inject_schema: bool = False) -> Dict[int, Dict[str, Any]]:
    """
    spec item format (one dict entry per AppCode):

      AppCode.SOMETHING: {
        "status": fastapi.status.HTTP_200_OK,
        "description": "...",
        "model": DocAPIResponseOK,
        "model_data": JobLease,
        # OR "model": DocAPIResponseClientError

        # Optional for JSON: override example payload
        # "example_value": {...}

        # Non-JSON (e.g., binary):
        # "content_type": "image/jpeg",
        # "example_value": "(binary image data)",
      }
    """
    grouped = defaultdict(lambda: defaultdict(lambda: {"examples": {}}))  # status -> ctype -> examples
    status_models: Dict[int, Optional[Type[Any]]] = {}
    status_schema_refs: Dict[int, str] = {}

    for app_code, cfg in spec.items():
        status: int = cfg["status"]
        desc: Optional[str] = cfg.get("description")
        ctype: str = cfg.get("content_type", "application/json")
        model_cls: Optional[Type[Any]] = cfg.get("model")
        model_data_cls: Optional[Type[Any]] = cfg.get("model_data")
        detail: str = cfg.get("detail", "")
        details: Any = cfg.get("details")
        example_value = cfg.get("example_value")

        # Build example value
        if ctype != "application/json":
            if example_value is None:
                raise ValueError(f"Non-JSON example requires 'example_value' for {app_code}.")
            value = example_value
        else:
            # JSON
            if example_value is not None:
                value = example_value
            elif model_cls is not None:
                value = _build_json_example(
                    model_cls=model_cls,
                    model_data_cls=model_data_cls,
                    app_code=app_code,
                    detail=detail,
                    status=status,
                    details=details)
                # Keep FastAPI "model" behavior for non-inject path
                status_models.setdefault(status, get_origin(model_cls) or model_cls)
                # Precompute the $ref we’ll inject when inject_schema=True
                status_schema_refs.setdefault(status, _schema_ref_from_model(model_cls))
            else:
                raise ValueError(f"JSON example needs 'model' for {app_code} (generic OK or non-generic).")

        grouped[status][ctype]["examples"][app_code.value] = {
            "summary": app_code.value,
            **({"description": desc} if desc else {}),
            "value": value,
        }

    _HTTP_STATUS_DESCRIPTIONS = {
        200: "OK",
        201: "Created",
        202: "Accepted",
        204: "No Content",
        400: "Bad Request",
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        405: "Method Not Allowed",
        409: "Conflict",
        410: "Gone",
        415: "Unsupported Media Type",
        422: "Unprocessable Entity",
        429: "Too Many Requests",
        500: "Internal Server Error",
        502: "Bad Gateway",
        503: "Service Unavailable",
    }

    # Assemble FastAPI responses shape
    if not inject_schema:
        responses: Dict[int, Dict[str, Any]] = {}
        for status, c_map in grouped.items():
            entry: Dict[str, Any] = {
                "description": _HTTP_STATUS_DESCRIPTIONS.get(status, f"Response status {status}."),
                "content": {},
            }

            if "application/json" in c_map and status in status_models and status_models[status] is not None:
                entry["model"] = status_models[status]

            for ctype, payload in c_map.items():
                entry["content"][ctype] = {"examples": payload["examples"]}

            responses[status] = entry
    else:
        responses = {}
        for status, c_map in grouped.items():
            entry: Dict[str, Any] = {
                "description": _HTTP_STATUS_DESCRIPTIONS.get(status, f"Response status {status}."),
                "content": {},
            }

            for ctype, payload in c_map.items():
                content_obj: Dict[str, Any] = {"examples": payload["examples"]}
                if ctype == "application/json":
                    schema_ref = status_schema_refs.get(status)
                    if schema_ref:
                        content_obj["schema"] = {"$ref": schema_ref}
                entry["content"][ctype] = content_obj

            responses[status] = entry

    return responses

def _build_json_example(
    *, model_cls: Type[Any], model_data_cls: Optional[Type[Any]],
    app_code, detail: str, status: int, details: Any
) -> dict:
    """
    Success (generic): instantiate model_cls(status, code, detail, data=model_example(T))
    Error (non-generic): instantiate model_cls(status, code, detail, details=...)
    """
    if model_data_cls is not None:
        data = model_example(model_data_cls)
        inst = model_cls(status=status, code=app_code, detail=detail, data=data)
    else:
        inst = model_cls(status=status, code=app_code, detail=detail, details=details)
    return inst.model_dump(mode="json", exclude_none=True)

def _schema_ref_from_model(model_tp: Type[Any], *, model_data_cls: Optional[Type[Any]] = None) -> str:
    """
    Compose a stable components schema $ref:
      - Success:  Origin_ModelData  (e.g., DocAPIResponseOK_JobLease)
      - Error:    Origin             (e.g., DocAPIResponseClientError)
    """
    base = getattr(model_tp, "__name__", str(model_tp))
    if model_data_cls is not None:
        arg = getattr(model_data_cls, "__name__", str(model_data_cls))
        return f"#/components/schemas/{base}_{arg}"
    return f"#/components/schemas/{base}"