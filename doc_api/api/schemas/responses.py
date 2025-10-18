import enum
from typing import Generic, TypeVar, Optional, Any, Mapping

import fastapi
from pydantic import BaseModel, Field, model_validator, field_validator
from fastapi.responses import JSONResponse, Response


# Naming convention for AppCode: CATEGORY_ACTION
class AppCode(str, enum.Enum):
    # 2xx

    # Worker-related
    JOB_ASSIGNED = 'JOB_ASSIGNED'
    JOB_QUEUE_EMPTY = 'JOB_QUEUE_EMPTY'
    JOB_RETRIEVED = 'JOB_RETRIEVED'
    JOB_UPDATED = 'JOB_UPDATED'
    JOB_HEARTBEAT_ACCEPTED = 'JOB_HEARTBEAT_ACCEPTED'
    JOB_COMPLETED = 'JOB_COMPLETED'
    JOB_ALREADY_COMPLETED = 'JOB_ALREADY_COMPLETED'
    JOB_FAILED = 'JOB_FAILED'
    JOB_ALREADY_FAILED = 'JOB_ALREADY_FAILED'

    IMAGES_RETRIEVED = 'IMAGES_RETRIEVED'
    IMAGE_RETRIEVED = 'IMAGE_RETRIEVED'

    IMAGE_NOT_UPLOADED = 'IMAGE_NOT_UPLOADED'
    ALTO_NOT_UPLOADED = 'ALTO_NOT_UPLOADED'
    META_JSON_NOT_UPLOADED = 'META_JSON_NOT_UPLOADED'

    IMAGE_DOWNLOADED = 'IMAGE_DOWNLOADED'
    ALTO_DOWNLOADED = 'ALTO_DOWNLOADED'
    META_JSON_DOWNLOADED = 'META_JSON_DOWNLOADED'

    RESULT_ZIP_INVALID = 'RESULT_ZIP_INVALID'
    RESULT_ZIP_UPLOADED = 'RESULT_ZIP_UPLOADED'
    RESULT_ZIP_MISSING = 'RESULT_ZIP_MISSING'

    # 4xx
    JOB_NOT_FOUND = 'JOB_NOT_FOUND'
    IMAGE_NOT_FOUND_FOR_JOB = 'IMAGE_NOT_FOUND_IN_JOB'
    JOB_NOT_IN_PROCESSING = 'JOB_NOT_IN_PROCESSING'

    HTTP_ERROR = 'HTTP_ERROR'
    REQUEST_VALIDATION_ERROR = 'REQUEST_VALIDATION_ERROR'

    API_KEY_MISSING = 'API_KEY_MISSING'
    API_KEY_INVALID = 'API_KEY_INVALID'
    API_KEY_INACTIVE = 'API_KEY_INACTIVE'
    API_KEY_INSUFFICIENT_ROLE = 'API_KEY_INSUFFICIENT_ROLE'
    API_KEY_FORBIDDEN_FOR_JOB = 'API_KEY_FORBIDDEN_FOR_JOB'
    API_KEY_NOT_FOUND = 'API_KEY_NOT_FOUND'

    # 5xx
    INTERNAL_ERROR = 'INTERNAL_ERROR'

DETAILS_GENERAL = {
    # 4xx
    AppCode.IMAGE_NOT_FOUND_FOR_JOB: "Image (id={image_id}) does not exist for Job (id={job_id}).",

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

def validate_no_data_ok_response(payload: DocAPIResponseOK[T]) -> Response:
    """
    Render a 2xx *no-data* response, for 200 with data return Pydantic model
    directly from route and use FastAPI response_model for validation.
    Policy:
      - ALL 2xx through this helper must have data is None.
      - 204/205 => empty Response (no body) - RFC: 204/205 MUST NOT include a body.
      - Other 2xx => JSON envelope with data=None.
    """
    code = int(payload.status)

    if payload.data is not None:
        raise ValueError(
            f"validate_ok_response only permits 2xx with data=None; "
            f"got status {code} and data={payload.data!r}"
        )

    if payload.status in NO_BODY_STATUSES:
        return Response(status_code=code)

    return JSONResponse(status_code=code, content=payload.model_dump(mode="json"))


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