"""Wire types for the HTTP contract.

`ProcessRequest` deliberately accepts *only* `text` and ignores unknown keys.
The lab harness posts exactly `{"text": "..."}`, so any extra required field
would earn a 422 and take the whole assessment down with it.
"""

from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field

# Status values the harness recognises from GET /result/{id}.
STATUS_PROCESSING = "processing"
STATUS_COMPLETED = "completed"
STATUS_ERROR = "error"


class ProcessRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    text: str = Field(..., min_length=1, description="Text handed to the AI backend.")


class ProcessAccepted(BaseModel):
    """Returned the moment the message is enqueued, never after processing."""

    id: str
    status: str = STATUS_PROCESSING
    created_at: str


class ProcessResult(BaseModel):
    """`GET /result/{id}` body. Always carries an explicit `status` field."""

    id: str
    status: str
    text: Optional[str] = None
    result: Optional[str] = None
    error: Optional[str] = None
    retries: int = 0
    latency_s: Optional[float] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    metadata: Optional[dict[str, Any]] = None
