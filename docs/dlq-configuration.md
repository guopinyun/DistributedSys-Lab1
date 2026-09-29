# Dead-letter queue configuration

Everything below is declared in `app/broker.py` and is therefore the single
source of truth. Each entry is what the RabbitMQ Management UI
(<http://localhost:15672>) shows for the running broker.

## Topology

| Object | Type | Durable | Purpose |
|--------|------|---------|---------|
| `task_queue` | queue | yes | The work queue. Carries `x-dead-letter-exchange` and `x-dead-letter-routing-key`. |
| `task_dlx` | direct exchange | yes | Routes anything rejected off `task_queue` to the dead-letter queue. |
| `task_queue.dead` | queue | yes | The DLQ. Bound to `task_dlx` with routing key `task_queue.dead`. |
| `task_queue.retry` | queue | yes | Holds a failed message for its backoff window, then returns it to `task_queue`. |

### `task_queue` arguments

```python
{
    "x-dead-letter-exchange":    "task_dlx",
    "x-dead-letter-routing-key": "task_queue.dead",
}
```

### `task_dlx` bindings

| Exchange | Queue | Routing key |
|----------|-------|-------------|
| `task_dlx` | `task_queue.dead` | `task_queue.dead` |

### `task_queue.retry` arguments

```python
{
    "x-dead-letter-exchange":    "",
    "x-dead-letter-routing-key": "task_queue",
}
```

The empty-string exchange is the default exchange. Publishing to
`task_queue.retry` with routing key `task_queue.retry` lands the message in
that queue, and when its TTL expires the broker re-publishes it to the queue
named by the dead-letter routing key. This is how a per-message backoff is
achieved without a scheduler.

## Retry policy

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `MAX_RETRIES` | `3` | Retries after the first attempt. Total deliveries: 4. |
| `RETRY_BASE_MS` | `2000` | First backoff. Doubles each attempt. |

| Delivery | `x-retry` on arrival | Backoff applied before the next delivery |
|----------|----------------------|-------------------------------------------|
| 1 | 0 | — (first attempt) |
| 2 | 1 | 2 s |
| 3 | 2 | 4 s |
| 4 | 3 | 8 s |
| — | — | `basic_nack(requeue=False)` → DLQ |

The attempt counter lives in the `x-retry` message header, incremented by the
consumer before republishing. `x-death` is **not** used for counting, because
`x-death` entries are maintained per queue and would split the count across
`task_queue.retry` and `task_queue`. The header is simply copied along with the
body on each hop, so it survives both the retry round trip and the final
dead-letter.

## Failure classification

`app/ai_client.py` decides whether a failure is worth retrying.

| Condition | Classification | Effect |
|-----------|----------------|--------|
| Connection error to the AI backend | transient | Retry with backoff |
| Timeout, HTTP 408, 429, 5xx | transient | Retry with backoff |
| HTTP 400, 401, 403, 404, 422 | permanent | `basic_nack(requeue=False)` immediately, no retries |
| Unparseable response body | permanent | `basic_nack(requeue=False)` immediately |
| Malformed message JSON, missing `id`/`text` | permanent | `basic_reject(requeue=False)`, straight to DLQ |
| Consumer rate limit reached, `CONSUMER_MODE=log` | transient | Retry with backoff |

Retrying a permanent failure is the design error this avoids: a 400 from the AI
backend will fail identically four times, burning 14 seconds of backoff and
three consumer round trips to reach the exact same dead letter.

## Why `basic_nack(requeue=False)` and not a manual publish

The final hop is a **single broker-side action**. `task_queue` already declares
`x-dead-letter-exchange`, so rejecting the delivery causes RabbitMQ to route it
to `task_queue.dead` itself. Two reasons this matters:

1. **No window for message loss.** A hand-rolled version has to
   `basic_publish` to the DLQ and then `basic_ack` the original. If the consumer
   dies between those two operations, the message exists in the DLQ *and* gets
   redelivered from `task_queue` — a silent duplicate that no test will catch.
2. **`x-death` is populated correctly.** Only broker dead-lettering records the
   original queue, the reason, and the routing key. A manually published message
   would arrive in the DLQ with no `x-death` at all, so the failure audit trail
   would be missing from the very queue whose purpose is to be audited.

`app/broker.py` does contain a `publish_dead()` helper, but it is reachable only
on the `AUTO_ACK=true` path. There, the broker considers the message delivered
the instant it hands it over, so there is no delivery tag left to nack and the
copy has to be republished by hand. The default path — the one the harness
tests — never calls it.

## What the DLQ message looks like

Body is unchanged; the state travels in headers. This is real output from
`python tools/inspect_queue.py task_queue.dead` after four failed attempts
against an unreachable AI backend, with `RETRY_BASE_MS=2000`:

```json
{
  "delivery_mode": 2,
  "content_type": "application/json",
  "x-retry": 3,
  "x-death": [
    { "count": 1, "queue": "task_queue",       "reason": "rejected",
      "exchange": "", "routing-keys": ["task_queue"],
      "original-expiration": null },
    { "count": 1, "queue": "task_queue.retry", "reason": "expired",
      "exchange": "", "routing-keys": ["task_queue.retry"],
      "original-expiration": "8000" }
  ]
}
```

```json
{ "id": "350ca5367dc64051b6569bbb5bfe1742",
  "text": "doc header verification",
  "timestamp": "2026-09-25T18:34:35.505Z" }
```

Four things to read out of that.

**`delivery_mode: 2` survives the dead-letter hop.** A message sitting in the
DLQ is still persistent, so it is still there after a broker restart. This
matters because the DLQ is where messages go precisely when nobody is
watching, and losing them to a reboot would defeat the point.

**`original-expiration: "8000"`** is the backoff of the final retry, captured by
the broker when the message left `task_queue.retry`. It is broker-generated
data that the consumer never wrote, which is the clearest evidence that the
delay is being enforced by RabbitMQ rather than by a `sleep` somewhere in our
code.

**Both hops are recorded.** `expired` on `task_queue.retry` and `rejected` on
`task_queue` together reconstruct the entire failure path, and this is only
possible because the final hop used `basic_nack`. A hand-published DLQ message
would arrive with no `x-death` entries at all.

**`count` is 1 on both entries, not 3 on the retry queue.** This surprises people
and it is the clearest justification for keeping the counter in `x-retry` rather
than deriving it from `x-death`. Each retry publishes a *fresh copy* to
`task_queue.retry`, and each of those copies is a brand new message that has
never been dead-lettered before — so it expires from the retry queue exactly
once, and the counter for that queue never rises above 1. The three retries are
visible only in `x-retry`, which is exactly what makes that header necessary
rather than redundant.

### Inspecting the DLQ

The RabbitMQ 4.x management plugin no longer serves the queue `contents`
endpoint, so the usual management-API trick returns `405 Method Not Allowed`.
Use the bundled helper instead. It reads with `basic_get(auto_ack=False)`,
prints, and requeues every message it took before closing, so it never removes
anything and the queue depth is unchanged when it exits:

```powershell
python tools/inspect_queue.py task_queue.dead
python tools/inspect_queue.py task_queue --count 3
```

The UI still shows depth, consumer count and message rate, which is enough for
exercises 1.2 and 1.3. It just cannot show headers.

The Management API is still useful for verifying the topology itself:

```powershell
$h = @{Authorization = "Basic " + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("guest:guest"))}
# queue depth and durability, without waiting on the 5s statistics collector
(Invoke-RestMethod -Uri "http://localhost:15672/api/queues" -Headers $h) |
  ForEach-Object { "$($_.name)  messages=$($_.messages)  durable=$($_.durable)" }
```

## Configuration switches

| Variable | Default | Effect |
|----------|---------|--------|
| `QUEUE_DURABLE` | `true` | `false` makes both queues and the exchange transient. Messages are lost on broker restart. |
| `MESSAGE_PERSISTENT` | `true` | `false` publishes with `delivery_mode=1`, so messages do not survive a broker restart even on a durable queue. |
| `AUTO_ACK` | `false` | `true` acknowledges on delivery. Exercises the "what can you lose" scenario; failures are then republished and dead-lettered by hand. |

**`QUEUE_DURABLE` and `MESSAGE_PERSISTENT` are independent and both are
required.** A persistent message on a transient queue is lost on restart, and a
transient message on a durable queue is also lost on restart. The message has to
be persistent *and* the queue has to be durable.

Redelivering them requires deleting the queues first: RabbitMQ refuses to redeclare
a durable queue as transient and vice versa, returning a `PRECONDITION_FAILED`
`406`. A durable queue that is currently holding messages must be emptied or
deleted before the arguments can change.

## Verifying the configuration

```powershell
$h = @{Authorization = "Basic " + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("guest:guest"))}

# Is the DLX actually attached to the work queue?
(Invoke-RestMethod -Uri "http://localhost:15672/api/queues/%2F/task_queue" -Headers $h).arguments
# x-dead-letter-exchange: task_dlx
# x-dead-letter-routing-key: task_queue.dead

# Is the DLQ bound to the exchange?
(Invoke-RestMethod -Uri "http://localhost:15672/api/exchanges/%2F/task_dlx/bindings/source" -Headers $h |
  ForEach-Object { "$($_.source) -> $($_.destination) [$($_.routing_key)]" })
# task_dlx -> task_queue.dead [task_queue.dead]
```

If `arguments` is empty, the queue was created without dead-lettering. Delete
`task_queue`, restart the producer once to redeclare it, and check again.
