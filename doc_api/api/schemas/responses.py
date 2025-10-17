import enum
from http import HTTPStatus
from typing import Generic, TypeVar, Optional

from pydantic import BaseModel, Field
from starlette.responses import JSONResponse


# Naming convention for AppCode: CATEGORY_ACTION
class AppCode(str, enum.Enum):
    JOB_QUEUE_EMPTY = 'JOB_QUEUE_EMPTY'
    JOB_ASSIGNED = 'JOB_ASSIGNED'

    KEY_NOT_FOUND = 'KEY_NOT_FOUND'

    INTERNAL_ERROR = 'INTERNAL_ERROR'

T = TypeVar("T")

class DocAPIResponse(BaseModel):
    """Response without data (4xx/5xx)."""
    status_code: HTTPStatus = Field(..., description="HTTP status code.")
    app_code: "AppCode" = Field(..., description="Application-specific code.")
    message: str = Field(..., description="Human-readable message.")

class DocAPIResponseWithOptionalData(DocAPIResponse, Generic[T]):
    """Response where data may or may not be present (2xx)."""
    data: Optional[T] = Field(
        None,
        description="Optional data payload associated with the response."
    )

class DocAPIResponseWithData(DocAPIResponse, Generic[T]):
    """Response with data (2xx)."""
    data: T = Field(
        ...,
        description="Data payload associated with the response."
    )

def make_doc_api_response(status_code: HTTPStatus, app_code: AppCode, message: str) -> JSONResponse:
    payload = DocAPIResponse(
        status_code=status_code,
        app_code=app_code,
        message=message
    )
    return JSONResponse(status_code=int(status_code), content=payload.model_dump(mode="json"))

