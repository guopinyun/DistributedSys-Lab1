"""Ollama client for the consumer.

Step 3 Option A of the brief: a local model, no account, no rate limit. The
point of routing every failure through one exception type is that the consumer
must never have to know which library raised it -- it only asks "is this worth
retrying, or is the request itself broken?".
"""

import logging
import os
from typing import Any, Optional

import httpx

from . import config

log = logging.getLogger(__name__)

# Status codes worth trying again later. 429 is the rate limit the brief calls
# out, 502/503/504 are the gateway/overload family, and a bare 500 is a server
# side bug we cannot fix from here.
_TRANSIENT_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})


class AIError(Exception):
    """An AI call failed.

    `transient` decides routing: True means put it in the retry queue, False
    means the request is unacceptable and dead-lettering it immediately is the
    only sensible thing to do.
    """

    def __init__(self, message: str, *, transient: bool, retry_after_s: Optional[float] = None):
        super().__init__(message)
        self.transient = transient
        self.retry_after_s = retry_after_s


def _retry_after_seconds(response: httpx.Response) -> Optional[float]:
    raw = response.headers.get("retry-after")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def complete(text: str, *, client: Optional[httpx.Client] = None) -> str:
    """Send `text` to the model and return its reply.

    Raises AIError(transient=True) for anything that might succeed on a second
    attempt -- connection refused, timeout, 503, 429 -- and
    AIError(transient=False) for a request the backend will never accept.
    """
    owns_client = client is None
    client = client or httpx.Client(timeout=config.AI_TIMEOUT_S)
    url = f"{config.AI_BASE_URL.rstrip('/')}/api/generate"
    payload = {
        "model": config.AI_MODEL,
        "prompt": text,
        "stream": False,
        "options": {"num_predict": config.AI_MAX_TOKENS},
    }

    try:
        try:
            response = client.post(url, json=payload)
        except httpx.TimeoutException as exc:
            raise AIError(
                f"AI backend timed out after {config.AI_TIMEOUT_S}s: {exc}", transient=True
            ) from exc
        except httpx.HTTPError as exc:
            # Connection refused, DNS failure, reset: the backend is not there
            # right now. This is exactly what task B's "non-existent URL" test
            # produces.
            raise AIError(f"AI backend unreachable at {config.AI_BASE_URL}: {exc}", transient=True) from exc

        if response.status_code in _TRANSIENT_STATUS:
            retry_after = _retry_after_seconds(response)
            detail = f"HTTP {response.status_code} from AI backend"
            if retry_after is not None:
                detail += f" (retry-after {retry_after}s)"
            raise AIError(detail, transient=True, retry_after_s=retry_after)

        if response.is_error:
            # 400 bad prompt, 401 bad credentials, 404 unknown model: retrying
            # the identical request will fail identically.
            raise AIError(
                f"AI backend rejected the request: HTTP {response.status_code} "
                f"{response.text[:200]}",
                transient=False,
            )

        try:
            body: Any = response.json()
        except ValueError as exc:
            raise AIError(f"AI backend returned non-JSON: {exc}", transient=True) from exc

        answer = (body or {}).get("response", "")
        if not isinstance(answer, str) or not answer.strip():
            raise AIError("AI backend returned an empty completion", transient=True)
        return answer.strip()
    finally:
        if owns_client:
            client.close()


def describe() -> str:
    return f"{config.AI_BASE_URL} model={config.AI_MODEL} timeout={config.AI_TIMEOUT_S}s pid={os.getpid()}"
