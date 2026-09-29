# ADR-001: RabbitMQ instead of Kafka for the asynchronous AI pipeline

- **Status:** Accepted
- **Date:** 2026-10-05
- **Deciders:** Lab 1 team
- **Relates to:** Task A (async AI processing service), Task B (error handling and DLQ)

## Context

We are building a pipeline that accepts a piece of text over HTTP, calls a large
language model with it, and lets the caller poll for the answer. The
requirements that actually constrain the technology choice are these:

1. **Low throughput.** The AI backend is the bottleneck. On the lab machine
   `llama3.2:1b` answers in roughly 3–40 seconds depending on whether the model
   is warm, so the pipeline sustains about **0.1 messages per second**. Nobody
   is going to measure this system's throughput in the thousands per second.
2. **Per-message routing by header.** A retry attempt must carry its own
   attempt count, and the message must come back to the *same* logical work
   queue after a delay.
3. **Per-message delay.** Retries need exponential backoff (we use 2s, 4s, 8s)
   expressed as a property of the individual message, not a property of a whole
   topic.
4. **First-class dead-lettering.** A message that has exhausted its retries must
   be moved, with broker support, to a queue a human can inspect. The lab
   harness explicitly requires the work queue to declare
   `x-dead-letter-exchange` and for the final failure to be a
   `basic_nack(requeue=False)`.
5. **At-least-once delivery to a named consumer.** The consumer must hold a
   message unacknowledged across a ~20 second AI call and get it back if the
   process dies.
6. **A single logical work queue.** There is one kind of job, and one consumer
   group. We are not building an event log that several independent services
   read at their own offsets.

Requirements 2, 3 and 4 are the ones that matter. They are all about
*manipulating a single message in flight* and they are all things RabbitMQ does
as core, well-documented features.

## Decision

**We will use RabbitMQ** (broker version 4.3.5, image `rabbitmq:4-management`),
with:

- one durable work queue `task_queue` carrying `x-dead-letter-exchange=task_dlx`
  and `x-dead-letter-routing-key=task_queue.dead`;
- a durable direct exchange `task_dlx` bound to the durable queue
  `task_queue.dead`, which is the dead-letter destination for anything the
  consumer finally rejects;
- a durable queue `task_queue.retry` with **no** queue-level TTL, dead-lettering
  back to `task_queue` through the default exchange, used to hold a republished
  copy of a failed message for its backoff period;
- persistent delivery (`delivery_mode=2`) on every message, so a message
  survives a broker restart;
- manual acknowledgement (`auto_ack=False`) with `basic_qos(prefetch_count=1)`,
  so exactly one message is ever in flight and any unacknowledged message is
  redelivered after a consumer crash.

The retry counter travels in an `x-retry` AMQP header rather than being derived
from `x-death`, because `x-death` only increments when a message is actually
dead-lettered and would split the count across two queues.

## Consequences

### What we gain

- **Per-message TTL with zero extra infrastructure.** The retry queue dead-letters
  on expiry, so backoff needs no scheduler, no cron, and no delayed-message
  plugin. The alternative for this in Kafka is a separate retry topic plus a
  consumer that re-sleeps, or the delayed-message plugin.
- **Immediate, broker-native dead-lettering.** `basic_nack(requeue=False)` is a
  single broker-side action. The message cannot be duplicated by a crash
  between "publish to DLQ" and "ack the original", which is a real risk in the
  hand-rolled version of this pattern.
- **A complete failure audit trail for free.** A dead-lettered message arrives
  with `x-death` populated, which for us records both hops: `expired` from
  `task_queue.retry` (carrying `original-expiration`) and `rejected` from
  `task_queue`.
- **Per-message headers as a first-class routing concern.** Carrying `x-retry`
  through the dead-letter hop is completely natural.
- **A management UI that makes the lab demonstrable.** Queue depth, consumer
  count and message headers are visible in a browser, which is what the
  assessment is built around.

### What we pay

- **A single broker is a single point of failure.** If RabbitMQ is down,
  `POST /process` returns 503 and nothing is accepted. Clustering three or more
  nodes would fix availability, at the cost of significant operational
  complexity for a system processing 0.1 messages per second.
- **The broker is a stateful dependency, not a library.** We now run and
  operate a server. Kafka has the same property, so this is not a differentiator,
  but it is real work.
- **No retained log to replay from.** Once a message is acknowledged, it is
  gone. We cannot ask "what did the user submit last Tuesday?" from the queue.
  Our `results` table is the only history, and it holds final answers rather
  than raw request/response pairs.
- **Throughput ceiling is lower than Kafka's.** Irrelevant at 0.1 msg/s;
  genuinely disqualifying if this ever becomes a high-volume pipeline.
- **Consumer scaling is per-queue, not per-group.** To parallelise, we add more
  consumers on the same queue and they share the load round-robin. We cannot
  run two *independent* readers of the same stream without duplicating the
  queue, which Kafka does natively.

### The one that would change our mind

The decision should be revisited if **all** of the following become true:

- throughput needs to exceed roughly 10,000 messages per second;
- messages must be retained for days and replayed into new consumers;
- three or more independent services each need their own offset over the same
  event stream;
- the producer and consumers live in different regions and need replication.

At that point the work queue becomes an event log, and an event log is what
Kafka is. The features that made RabbitMQ the right call here — per-message TTL,
per-message header routing, immediate dead-lettering — all still exist in Kafka,
but none of them is as direct, and each would need compensating machinery.

## Alternatives considered

**Apache Kafka.** Rejected. Kafka is a partitioned, replicated log built for
sustained high throughput and independent replay. Every one of its advantages is
inactive at our scale. The costs are concrete: per-message delayed delivery is
not a core feature, dead-lettering is a convention rather than a queue
attribute, and a consumer group's offset model is a poor fit for a single
work queue where the broker already tracks acknowledgement for us. Choosing
Kafka here would mean building a retry service to work around missing
per-message TTL, and reimplementing dead-letter routing by hand.

**Redis Streams.** Rejected. The stream and consumer-group model is a good fit
for this shape of problem, and Redis is operationally simpler than a broker. It
was rejected because it has no native per-message dead-letter exchange, so the
DLQ would have to be built by hand — the exact pattern we want the broker to own
— and because the single Redis instance would become both a throughput ceiling
and a data-loss risk unless persistence is configured carefully.

**Amazon SQS.** Rejected. It maps well onto the requirement: per-message
visibility timeout, a native dead-letter queue, no operations. It was rejected
because it introduces a paid cloud dependency and an account, and because
SQS's visibility timeout is the *only* delay primitive available, so
exponential backoff is awkward to express and the ~15 minute maximum visibility
timeout caps how long a retry can wait.

**A direct synchronous HTTP call from the request handler.** Rejected outright.
It is the simplest thing that could possibly work and it fails every
non-functional requirement: the caller waits 20 seconds and times out, a slow
model makes the API unavailable, a rate limit returns a 429 straight to the
user with nobody to retry, and there is nowhere to put a message that failed
permanently. The queue exists precisely to move that work off the request path
and to give failure somewhere to go.

**A thread or `asyncio` queue inside the FastAPI process.** Rejected. This would
pass the lab's functional checks, but it fails the moment a worker restarts:
every in-flight message dies with it, there is no durability, and results are
lost. It also cannot be scaled horizontally, so the consumer would be pinned to
the same process as the API forever. The lab's criterion 2 exists specifically to
test that killing the consumer does not lose the message, and an in-process
queue cannot satisfy it.
