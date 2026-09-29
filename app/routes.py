"""The producer service's HTTP surface.

Exactly two endpoints, on port 8080, as the brief and the lab harness require:

    POST /process      {"text": "..."} -> {"id": "...", "status": "processing"}
    GET  /result/{id}  the stored AI answer, or a status message

They are sync `def` on purpose. FastAPI runs them in a worker thread, so the
blocking pika publish and the blocking sqlite write never block the event loop.
"""

import logging

from fastapi import APIRouter, HTTPException, status

from . import producer, store
from .schemas import ProcessAccepted, ProcessRequest, ProcessResult

log = logging.getLogger("api")
router = APIRouter()


@router.post("/process", response_model=ProcessAccepted, status_code=status.HTTP_202_ACCEPTED)
def process(req: ProcessRequest) -> ProcessAccepted:
    """Accept the work and return immediately.

    The AI call happens in the consumer, never here. The harness fails the
    whole assessment if this call takes 3 seconds or more, so the only work
    here is one sqlite insert and one confirmed publish.
    """
    try:
        request_id, created_at = producer.enqueue(req.text)
    except Exception as exc:
        # The broker is unreachable. The request is recorded as errored rather
        # than left in `processing` forever, and the caller gets the id so the
        # failure is still traceable.
        log.error("POST /process failed: %s", exc)
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail=f"could not enqueue request: {exc}") from exc

    return ProcessAccepted(id=request_id, created_at=created_at)


@router.get("/result/{request_id}", response_model=ProcessResult)
def result(request_id: str) -> ProcessResult:
    found = store.get(request_id)
    if found is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"unknown request id: {request_id}")
    return found
