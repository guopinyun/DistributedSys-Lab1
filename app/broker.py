"""Broker connection and topology declaration.

Topology, all durable:

    task_queue         work queue. Carries x-dead-letter-* so the *broker* moves
                       rejected messages to the DLQ. This is the mechanism the
                       lab harness expects: a final failure is
                       basic_nack(requeue=False), not a hand-rolled publish.
    task_dlx           direct exchange, the "death warrant" named in task B.
    task_queue.dead    bound to task_dlx on routing key task_queue.dead.
    task_queue.retry   cooling area. Per-message `expiration` holds the backoff;
                       on expiry the broker dead-letters it back to task_queue
                       through the default exchange ("" + routing key = queue
                       name), which needs no extra exchange.

Every declare call is idempotent, so producer and consumer can both call
declare_topology() on startup without racing each other.
"""

import json
import logging
from typing import Any, Optional

import pika
from pika.exceptions import AMQPConnectionError, ChannelClosedByBroker, ProbableAccessDeniedError

from . import config

log = logging.getLogger(__name__)

_DELETE_FIX_HINT = (
    "RabbitMQ refuses to redeclare an existing queue with different arguments.\n"
    "    A queue is a fixed contract: you cannot turn durability on or off after it exists.\n"
    "    Delete it and let the service re-declare it:\n"
    "        docker exec rabbitMQ rabbitmqctl delete_queue {name}\n"
    "    (find your container name with: docker ps)"
)


def connection_params() -> pika.ConnectionParameters:
    return pika.ConnectionParameters(
        host=config.RABBITMQ_HOST,
        port=config.RABBITMQ_PORT,
        credentials=pika.PlainCredentials(config.RABBITMQ_USER, config.RABBITMQ_PASSWORD),
        heartbeat=config.RABBITMQ_HEARTBEAT,
        blocked_connection_timeout=30,
        connection_attempts=5,
        retry_delay=2,
    )


def connect(url: Optional[str] = None) -> pika.BlockingConnection:
    """Open one connection.

    pika's BlockingConnection is not thread-safe, so a connection belongs to
    exactly one thread. The producer uses it inline in request handlers (FastAPI
    runs sync endpoints in a worker thread each); the consumer owns one in its
    own daemon thread.
    """
    if url:
        return pika.BlockingConnection(pika.URLParameters(url))
    return pika.BlockingConnection(connection_params())


def declare_topology(channel: pika.adapters.blocking_connection.BlockingChannel) -> None:
    """Declare every queue, exchange and binding, or explain why we cannot.

    `passive=True` would tell us the queue exists without touching it, but it
    cannot report durability, so the work queue is always fully declared.
    """
    try:
        channel.exchange_declare(
            exchange=config.DEAD_LETTER_EXCHANGE,
            exchange_type="direct",
            durable=True,
        )
        channel.queue_declare(
            queue=config.QUEUE_NAME,
            durable=config.QUEUE_DURABLE,
            arguments={
                "x-dead-letter-exchange": config.DEAD_LETTER_EXCHANGE,
                "x-dead-letter-routing-key": config.DEAD_LETTER_ROUTING_KEY,
            },
        )
        channel.queue_declare(
            queue=config.DEAD_QUEUE_NAME,
            durable=True,
        )
        channel.queue_bind(
            queue=config.DEAD_QUEUE_NAME,
            exchange=config.DEAD_LETTER_EXCHANGE,
            routing_key=config.DEAD_LETTER_ROUTING_KEY,
        )
        # No queue-level x-message-ttl here on purpose: a per-queue TTL would
        # cap every message instead of letting each retry hop set its own
        # backoff. Backoff is carried per message via `expiration`.
        channel.queue_declare(
            queue=config.RETRY_QUEUE_NAME,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": config.QUEUE_NAME,
            },
        )
    except ProbableAccessDeniedError as exc:  # pragma: no cover - misconfiguration
        log.error("broker refused access: %s", exc)
        raise
    except ChannelClosedByBroker as exc:
        if exc.reply_code == 406:
            log.error("PRECONDITION_FAILED declaring %r: %s\n    %s",
                      config.QUEUE_NAME, exc.reply_text,
                      _DELETE_FIX_HINT.format(name=config.QUEUE_NAME))
        else:
            log.error("broker closed the channel (%s): %s", exc.reply_code, exc.reply_text)
        raise


def message_properties(
    headers: Optional[dict[str, Any]] = None,
    expiration_ms: Optional[int] = None,
) -> pika.BasicProperties:
    """Build publish properties.

    pika spells persistent delivery `delivery_mode=2`, not `persistent=True` as
    the brief's Celery-flavoured wording says. `expiration` is a longstr field,
    so it has to be handed over as a string.
    """
    return pika.BasicProperties(
        content_type="application/json",
        delivery_mode=2 if config.MESSAGE_PERSISTENT else 1,
        headers=headers or None,
        expiration=str(int(expiration_ms)) if expiration_ms is not None else None,
    )


def publish_work(
    channel: pika.adapters.blocking_connection.BlockingChannel,
    payload: dict[str, Any],
) -> None:
    channel.basic_publish(
        exchange="",
        routing_key=config.QUEUE_NAME,
        body=json.dumps(payload).encode("utf-8"),
        properties=message_properties(),
        mandatory=True,
    )


def retry_backoff_ms(attempt: int) -> int:
    """Exponential backoff for the nth retry: base, 2x base, 4x base, ..."""
    return config.RETRY_BASE_MS * (2 ** max(0, attempt - 1))


def publish_retry(
    channel: pika.adapters.blocking_connection.BlockingChannel,
    payload: dict[str, Any],
    original_headers: Optional[dict[str, Any]],
    attempt: int,
) -> int:
    """Hand the work to the cooling queue, where it waits out its backoff.

    `x-retry` is incremented here and nothing else, because `nack(requeue=True)`
    gives us no way to count attempts -- the broker hands the message straight
    back without touching its headers. The count therefore has to travel inside
    the republished copy, where it also shows up in the management UI.

    The caller acks the original delivery after this returns: the copy is now
    responsible for the work.
    """
    headers = dict(original_headers or {})
    headers[config.RETRY_HEADER] = attempt
    backoff = retry_backoff_ms(attempt)

    channel.basic_publish(
        exchange="",
        routing_key=config.RETRY_QUEUE_NAME,
        body=json.dumps(payload).encode("utf-8"),
        properties=message_properties(headers=headers, expiration_ms=backoff),
        mandatory=True,
    )
    return backoff


def publish_dead(
    channel: pika.adapters.blocking_connection.BlockingChannel,
    payload: dict[str, Any],
    original_headers: Optional[dict[str, Any]] = None,
    reason: str = "",
) -> None:
    """Dead-letter a message by hand.

    Only needed when we are not allowed to reject the delivery, i.e. under
    auto_ack=True where the broker has already discarded it and there is
    nothing left to nack. In the normal manual-ack path the work queue's
    x-dead-letter-exchange does this for us.
    """
    headers = dict(original_headers or {})
    if reason:
        headers["x-death-reason"] = reason[:512]
    channel.basic_publish(
        exchange=config.DEAD_LETTER_EXCHANGE,
        routing_key=config.DEAD_LETTER_ROUTING_KEY,
        body=json.dumps(payload).encode("utf-8"),
        properties=message_properties(headers=headers),
        mandatory=True,
    )


__all__ = [
    "AMQPConnectionError",
    "connect",
    "connection_params",
    "declare_topology",
    "message_properties",
    "publish_dead",
    "publish_retry",
    "publish_work",
    "retry_backoff_ms",
]
