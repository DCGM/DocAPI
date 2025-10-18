import enum
from http import HTTPStatus
from typing import Generic, TypeVar, Optional, Literal

from pydantic import BaseModel, Field
from fastapi.responses import JSONResponse, Response


# Naming convention for AppCode: CATEGORY_ACTION
class AppCode(str, enum.Enum):
    JOB_QUEUE_EMPTY = 'JOB_QUEUE_EMPTY'
    JOB_ASSIGNED = 'JOB_ASSIGNED'

    KEY_NOT_FOUND = 'KEY_NOT_FOUND'

    INTERNAL_ERROR = 'INTERNAL_ERROR'

T = TypeVar("T")

class DocAPIResponseERROR(BaseModel):
    """Response without data (4xx/5xx)."""
    status_code: HTTPStatus = Field(..., description="HTTP status code.")
    app_code: AppCode = Field(..., description="Application-specific code.")
    message: str = Field(..., description="Human-readable message.")

class DocAPIResponseOK(DocAPIResponseERROR, Generic[T]):
    """Response where data may or may not be present (2xx)."""
    data: Optional[T] = Field(
        None,
        description="Optional data payload associated with the response."
    )

def make_validated_ok(
    *,
    status_code: HTTPStatus,
    app_code: AppCode,
    message: str,
) -> Response:
    """
    Build a validated 2xx response with NO data (for 200 with data use the Pydantic model
    directly from the route and FastAPI response_model to validate).
    - Raises ValueError if `status_code` is not 2xx.
    - For 204/205 → return an empty Response (no body allowed by RFC).
    - For other 2xx (201/202/206/…) → return a JSON envelope with data=None.
    """
    code = int(status_code)
    if code < 200 or code > 299:
        raise ValueError(f"make_validated_client_error only permits 2xx codes, got {code}")

    # 204/205 MUST NOT include a body
    if status_code in (HTTPStatus.NO_CONTENT, HTTPStatus.RESET_CONTENT):
        return Response(status_code=code)

    payload = DocAPIResponseOK[Literal[None]](
        status_code=status_code,
        app_code=app_code,
        message=message,
        data=None,
    )
    return JSONResponse(status_code=code, content=payload.model_dump(mode="json"))


def make_validated_client_error(
    *,
    status_code: HTTPStatus,
    app_code: AppCode,
    message: str
) -> JSONResponse:
    """
    Build a validated 4xx client error.
    - Raises ValueError if `status_code` is not 4xx.
    """
    code = int(status_code)
    if not (400 <= code <= 499):
        raise ValueError(f"make_validated_client_error only permits 4xx codes, got {code}")

    payload = DocAPIResponseERROR(
        status_code=status_code,
        app_code=app_code,
        message=message,
    )
    return JSONResponse(status_code=code, content=payload.model_dump(mode="json"))


def make_validated_server_error(
    *,
    status_code: HTTPStatus,
    app_code: AppCode,
    message: str
) -> JSONResponse:
    """
    Build a validated 5xx server error.
    - Raises ValueError if `status_code` is not 5xx.
    """
    code = int(status_code)
    if not (500 <= code <= 599):
        raise ValueError(f"make_validated_server_error only permits 5xx codes, got {code}")

    payload = DocAPIResponseERROR(
        status_code=status_code,
        app_code=app_code,
        message=message,
    )
    return JSONResponse(status_code=code, content=payload.model_dump(mode="json"))

