"""Producer side: turn a request into a durable message on task_queue."""

import logging
import time
from typing import Any, Optional

import pika

from . import broker, store

log = logging.getLogger(__name__)


def build_payload(request_id: str, text: str) -> dict[str, Any]:
    """The envelope from exercise 1.1: {"id", "text", "timestamp"}.

    The `id` is the request id the client polls with, which is what lets
    GET /result/{id} find the work again after it has left the queue.
    """
    return {"id": request_id, "text": text, "timestamp": store.utc_now()}


def _publish(channel: pika.adapters.blocking_connection.BlockingChannel, text: str) -> str:
    """Register the request as in-flight, then hand it to the queue."""
    request_id = store.new_request_id()
    store.create_processing(request_id, text)
    # Without publisher confirms a basic_publish that cannot reach the broker
    # still returns happily, and "we sent 10" becomes a guess.
    broker.publish_work(channel, build_payload(request_id, text))
    return request_id


def enqueue(text: str, metadata: Optional[dict[str, Any]] = None) -> tuple[str, str]:
    """Publish one message. Returns the request id and its creation timestamp."""
    request_id = store.new_request_id()
    created_at = store.create_processing(request_id, text, metadata)

    connection: Optional[pika.BlockingConnection] = None
    try:
        connection = broker.connect()
        channel = connection.channel()
        broker.declare_topology(channel)
        channel.confirm_delivery()
        broker.publish_work(channel, build_payload(request_id, text))
    except Exception as exc:
        store.mark_error(request_id, f"could not enqueue: {exc}", 0)
        log.error("enqueue failed for %s: %s", request_id, exc)
        raise
    finally:
        if connection is not None and connection.is_open:
            try:
                connection.close()
            except Exception:  # pragma: no cover - best effort teardown
                pass

    return request_id, created_at


def send_batch(count: int, text_prefix: str = "Process this request", interval: float = 0.0) -> list[str]:
    """Publish `count` messages on one connection, for exercise 1.1.

    Publishes faster than any consumer can drain, so the depth in the
    management UI is the point of the exercise.
    """
    connection = broker.connect()
    try:
        channel = connection.channel()
        broker.declare_topology(channel)
        channel.confirm_delivery()

        ids: list[str] = []
        for n in range(1, count + 1):
            ids.append(_publish(channel, f"{text_prefix} {n}"))
            log.info("published %s (%d/%d)", ids[-1], n, count)
            if interval > 0 and n < count:
                time.sleep(interval)
        return ids
    finally:
        if connection.is_open:
            try:
                connection.close()
            except Exception:  # pragma: no cover - best effort teardown
                pass
