# COMP41720 Lab 1 — Asynchronous AI processing with RabbitMQ

A FastAPI service that accepts a piece of text, hands it to a local LLM through
RabbitMQ, and lets the client poll for the answer. The point of the exercise is
what happens when things go wrong: retries with backoff, a dead-letter queue,
and messages that survive a consumer being killed.

```
POST /process  {"text": "explain AMQPs"}   ->  202 {"id": "...", "status": "processing"}
GET  /result/{id}                          ->  200 {"status": "processing"}
                                             200 {"status": "completed", "result": "..."}
                                             200 {"status": "error", "error": "..."}
```

## Contents

- [Layout](#layout)
- [Prerequisites](#prerequisites)
- [Step 1 — the broker](#step-1--the-broker)
- [Step 2 — the service](#step-2--the-service)
- [Step 3 — the AI backend](#step-3--the-ai-backend)
- [Step 4 — the consumer](#step-4--the-consumer)
- [Part 1 — the exercises](#part-1--the-exercises)
  - [1.1 Publish and consume](#11-publish-and-consume)
  - [1.2 Throttling](#12-throttling)
  - [1.3 Acknowledgement and durability](#13-acknowledgement-and-durability)
- [Part 2 — the assessed system](#part-2--the-assessed-system)
- [Running the lab harness](#running-the-lab-harness)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)

## Layout

```
app/
  __init__.py
  config.py        every setting, loaded from .env with shell overrides
  schemas.py       request/response models
  store.py         SQLite result store (WAL)
  broker.py        queue/exchange/DLQ topology and publishing
  ai_client.py     Ollama client, transient vs permanent failure
  producer.py      insert row -> publish -> return id
  consumer.py      consume, process, retry, ack/nack
  routes.py        POST /process, GET /result/{id}
  main.py          FastAPI app and lifespan
docs/
  adr-001-rabbitmq-vs-kafka.md   Task C: why RabbitMQ
  architecture.md                Task C: diagrams and message flow
  architecture.drawio            the same diagram, editable
  dlq-configuration.md           Task B: DLQ setup, retry policy, x-death
consuming it
lab1_harness.py                  the provided checker
.env
requirements.txt
```

## Prerequisites

- Python 3.11+
- Docker, for RabbitMQ
- [Ollama](https://ollama.com) with a small model pulled

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Step 1 — the broker

```powershell
docker run -d --name rabbitMQ -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

- AMQP on `5672`
- Management UI on <http://localhost:15672> (`guest` / `guest`)

Wait for it to be ready, otherwise the first connection attempt fails:

```powershell
docker logs -f rabbitMQ          # look for "Server startup complete"
```

Do **not** add `--rm`. The container needs to survive a restart for exercise
1.3.

## Step 2 — the service

```powershell
$env:SERVICE_PORT = 8080
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8080
```

In a **second** terminal, start the consumer (see [Step 4](#step-4--the-consumer)).

The producer declares the topology on startup, so `task_queue`, `task_dlx`,
`task_queue.dead` and `task_queue.retry` appear in the UI after the first
request.

> If port 8080 is already taken — on Windows, a leftover
> `NIApplicationWebServer` service often holds it — run the service on another
> port and tell the harness:
> `--base http://127.0.0.1:8081`.

## Step 3 — the AI backend

```powershell
ollama serve
ollama pull llama3.2:1b
```

Confirm it answers:

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:11434/api/generate `
  -Body (@{model="llama3.2:1b"; prompt="say hi"; stream=$false} | ConvertTo-Json) |
  Select-Object -ExpandProperty response
```

Expect roughly **10–40 s** for the first call while the model loads, then about
3 s once it is warm. That latency is the reason the whole system is built around
a queue: it is three orders of magnitude slower than the HTTP request that
accepts the work.

## Step 4 — the consumer

```powershell
$env:CONSUMER_MODE = "ai"
.\.venv\Scripts\python.exe -m app.consumer
```

It should log:

```
consumer starting (mode=ai)
policy: max_retries=3 retry_base=2000ms queue_durable=True persistent=True db=...\lab.sqlite3
consuming task_queue (mode=ai rate=0.00/s prefetch=1 auto_ack=False)
```

Leave it running for Part 2. Stop it with Ctrl-C; the consumer handles `SIGINT`
gracefully so an in-flight message is returned to the broker rather than
duplicated.

## Part 1 — the exercises

### 1.1 Publish and consume

Send a request and watch the pipeline move it from queue to answer.

```powershell
$r = Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8080/process `
  -Body (@{text="What is an AMQP envelope?"} | ConvertTo-Json) `
  -ContentType "application/json"
$r
# id     : 7c1f...   status : processing   created_at : 2026-...

Invoke-RestMethod -Uri "http://127.0.0.1:8080/result/$($r.id)"
# status : processing          (immediately, before the model answers)

Start-Sleep -Seconds 15
Invoke-RestMethod -Uri "http://127.0.0.1:8080/result/$($r.id)" | Select-Object status, result
```

**What to observe.** `POST` returns in well under a second and the response
already contains the id, so the client is never blocked. The row is written
*before* the publish, which is why the first `GET` returns `processing` instead
of a 404. In the consumer log the same id appears once, first as `received` and
then as `completed`.

### 1.2 Throttling

`CONSUMER_MODE=log` processes at a fixed rate without calling the AI. This is
the only way to watch a clean 1 msg/s drain: a real `llama3.2:1b` call takes
longer than the interval you are trying to observe.

```powershell
# stop the ai consumer, then:
$env:CONSUMER_MODE = "log"
$env:CONSUMER_RATE = "1"
.\.venv\Scripts\python.exe -m app.consumer
```

Publish 10 requests from another terminal, then watch the consumer log:

```powershell
1..10 | ForEach-Object {
  Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8080/process `
    -Body (@{text="Process this request $_"} | ConvertTo-Json) -ContentType "application/json"
} | Out-Null

$h = @{Authorization = "Basic " + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("guest:guest"))}
1..13 | ForEach-Object {
  $q = Invoke-RestMethod -Uri "http://localhost:15672/api/queues/%2F/task_queue" -Headers $h
  "ready=$($q.messages_ready)  unacked=$($q.messages_unacknowledged)"
  Start-Sleep -Seconds 1
}
```

**Measured result.** The consumer log shows a clean 1.0–1.1 s per message, all
10 completing in about 10 s:

```
19:20:02 received  id=301aa727...  attempt=0  text='Process this request 1'
19:20:03 completed id=301aa727...  in 1.1s
19:20:03 received  id=d2f7f5f2...  attempt=0  text='Process this request 2'
19:20:05 completed id=d2f7f5f2...  in 1.1s
...
19:20:13 completed id=77f99c8c...  in 1.0s
```

`PREFETCH_COUNT=1` does the real work here: the broker is only ever allowed to
have one unacknowledged message in flight, so the depth you see is a truthful
count of outstanding work rather than something pika has buffered locally.

> The management API's `messages_ready` is served by a collector that runs on an
> interval (5 s by default), so a 1-second poll shows **stale and aliased
> values** — a depth of 5 sitting still, then a jump to 0. This is not a bug in
> the service. Read `/api/queues` for totals, or trust the consumer log, which
> is the ground truth.

### 1.3 Acknowledgement and durability

**1.3a — a killed consumer must not lose the message.** The crash demo is
built into the consumer, so you do not have to win a race against Ctrl-C.
`CRASH_AFTER_N=2` makes the consumer `os._exit(1)` after it has processed its
second message but *before* it acks it — the one window where a redelivery
actually proves something.

```powershell
$env:CONSUMER_MODE = "log"     # fast, so 3 messages arrive quickly
$env:CRASH_AFTER_N = "2"
$env:CRASH_AFTER_S = "1"
.\.venv\Scripts\python.exe -m app.consumer
```

Send three requests:

```powershell
1..3 | ForEach-Object {
  Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8080/process `
    -Body (@{text="crash-after-n test $_"} | ConvertTo-Json) -ContentType "application/json"
}
```

**Measured result.** The consumer acknowledges message 1, dies on message 2
without acking it, and is gone:

```
19:49:27 WARNING CRASH_AFTER_N=2 armed; this process will os._exit(1) after processing
                 message number 2, before acking it
19:49:27 received  id=d684250a...  redelivered=no   completed
19:49:27 received  id=767e04f6...  redelivered=no   completed
19:49:28 ERROR   CRASH_AFTER_N=2 reached on id=767e04f6..., simulating a crash in 1.0s
```

Message 2 is unacked, so the broker returns it, and message 3 was never
consumed. Restart the consumer and both come back:

```
19:51:14 received  id=767e04f6...  redelivered=yes  completed
19:51:14 received  id=d875989d...  redelivered=yes  completed
```

`redelivered=yes` is the broker telling us this message has been handed out
before. The work completed exactly once from the caller's point of view even
though the consumer processed message 2 twice.

To crash by hand instead, stop the consumer mid-request with Ctrl-C or
`Stop-Process`. `AI_DELAY_S=30` widens the window so the kill is not a race
against a 3 s model call.

**`AUTO_ACK=true` is what happens when you get this wrong.** Set
`AUTO_ACK=true`, restart the consumer, and kill it mid-flight. The message is
gone — the broker considered it delivered the moment it arrived, so the crash
destroyed it and the result stays `processing` forever. This is why the default
is `false`, and why `app/consumer.py` only acknowledges *after* the work is
durably recorded.

**1.3b — messages must survive a broker restart.** Stop the consumer, then
publish with nothing consuming:

```powershell
1..5 | ForEach-Object {
  Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8080/process `
    -Body (@{text="durability test $_"} | ConvertTo-Json) -ContentType "application/json"
}
```

Confirm 5 messages are waiting, then restart the broker:

```powershell
docker exec rabbitMQ rabbitmqctl list_queues name durable messages consumers
docker restart rabbitMQ
```

**Measured result.** All 5 messages are still there after the restart, and the
queue never lost its declaration:

```
before:  task_queue  true  5  0
after:   task_queue  true  5  0
```

Now flip `QUEUE_DURABLE=false` and/or `MESSAGE_PERSISTENT=false`, **delete the
queues**, redeclare, and repeat. The messages are lost. The lesson is that the
two settings are independent and you need both: a transient message on a
durable queue does not survive, and neither does a persistent message on a
transient queue.

Redelivering a queue with different arguments returns `406
PRECONDITION_FAILED` — a queue's arguments are fixed at declaration. Delete it
first:

```powershell
docker exec rabbitMQ rabbitmqctl delete_queue task_queue
```

## Part 2 — the assessed system

Assessed documents:

| Task | Document |
|------|----------|
| B — error handling and DLQ | [`docs/dlq-configuration.md`](docs/dlq-configuration.md) |
| C — architecture | [`docs/architecture.md`](docs/architecture.md), [`docs/architecture.drawio`](docs/architecture.drawio) |
| C — broker choice | [`docs/adr-001-rabbitmq-vs-kafka.md`](docs/adr-001-rabbitmq-vs-kafka.md) |

### Reproduce the dead-letter path

Point the consumer at a port where nothing is listening so every call fails, and
watch a request exhaust its retries:

```powershell
$env:AI_BASE_URL = "http://127.0.0.1:9"
.\.venv\Scripts\python.exe -m app.consumer
```

```powershell
$r = Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8080/process `
  -Body (@{text="this will fail"} | ConvertTo-Json) -ContentType "application/json"

Start-Sleep -Seconds 25
Invoke-RestMethod -Uri "http://127.0.0.1:8080/result/$($r.id)" | Select-Object status, retries
# status : error     retries : 3
```

**Measured consumer log.** The backoff is enforced by the broker, not by a
`sleep` in our code:

```
19:35:21 received id=350ca536... attempt=0
19:35:26 -> retry queue, backoff 2000ms
19:35:28 received id=350ca536... attempt=1
19:35:31 -> retry queue, backoff 4000ms
19:35:35 received id=350ca536... attempt=2
19:35:39 -> retry queue, backoff 8000ms
19:35:47 received id=350ca536... attempt=3
19:35:51 retries exhausted id=350ca536... after 4 attempt(s)
```

The message lands in `task_queue.dead` with its full history intact (can be observed in MQ Management UI):

```
=== message 1 ===
{
  "delivery_mode": 2,
  "x-retry": 3,
  "x-death": [
    { "count": 1, "queue": "task_queue",       "reason": "rejected", "original-expiration": null },
    { "count": 1, "queue": "task_queue.retry", "reason": "expired",  "original-expiration": "8000" }
  ]
}
```

`x-death` records both hops. `original-expiration: "8000"` is the final backoff,
written by the broker. `delivery_mode: 2` survives the dead-letter hop, so the
message is still persistent in the DLQ. `docs/dlq-configuration.md` explains why
the counter lives in `x-retry` and not in `x-death`.

### Verifying the topology

```powershell
$h = @{Authorization = "Basic " + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("guest:guest"))}

(Invoke-RestMethod -Uri "http://localhost:15672/api/queues/%2F/task_queue" -Headers $h).arguments
# x-dead-letter-exchange    : task_dlx
# x-dead-letter-routing-key : task_queue.dead

(Invoke-RestMethod -Uri "http://localhost:15672/api/exchanges/%2F/task_dlx/bindings/source" -Headers $h) |
  ForEach-Object { "$($_.source) -> $($_.destination) [$($_.routing_key)]" }
# task_dlx -> task_queue.dead [task_queue.dead]
```

## Running the lab harness

`lab1_harness.py` is the provided checker, copied here unmodified. With the
service on port 8080 and a consumer running:

```powershell
.\.venv\Scripts\python.exe lab1_harness.py --base http://localhost:8080 `
  --queue task_queue --dlq task_queue.dead
```

Service contract only, which takes seconds:

```powershell
.\.venv\Scripts\python.exe lab1_harness.py --base http://localhost:8080 `
  --queue task_queue --dlq task_queue.dead --quick
```

Verified result:

```
--- Criterion 1a - the service contract ---
  PASS  POST /process accepted the request (HTTP 202)
  PASS  request id returned immediately
  PASS  response was immediate (797 ms) - the call does not wait for the AI
  PASS  GET /result/{id} responds (HTTP 200, status: processing)
  PASS  request completed end-to-end (AI response stored and retrieved)
  PASS  3 concurrent requests accepted with 3 distinct ids
  PASS  all 3 completed
--- Criterion 1b - broker hygiene ---
  PASS  management API reachable at http://localhost:15672
  PASS  work queue identified: "task_queue"
  PASS  work queue is durable=True
  PASS  work queue dead-letters to exchange "task_dlx" with routing key "task_queue.dead"
  PASS  dead-letter queue found and bound: "task_queue.dead"
  ALL CHECKS PASSED   (12 passes, 0 warnings)
```

The full run additionally exercises criterion 2 (kill the consumer mid-flight
and prove the message is redelivered) and criterion 3 (poison a message and
prove it dead-letters after exactly 3 retries). It needs the `task_queue.dead`
queue to be reachable and takes a few minutes, so run it with no other test
traffic on the broker.

## Configuration

Edit `.env` . Real shell variables take precedence, so
you can override one setting per command without touching the file.

| Variable | Default | Purpose |
|----------|---------|---------|
| `RABBITMQ_HOST` / `RABBITMQ_PORT` | `127.0.0.1` / `5672` | Broker address |
| `QUEUE_NAME` | `task_queue` | Work queue name |
| `AUTO_ACK` | `false` | `true` = acknowledge on delivery; loses work on crash |
| `QUEUE_DURABLE` | `true` | Durable queues and exchange |
| `MESSAGE_PERSISTENT` | `true` | `delivery_mode=2` |
| `PREFETCH_COUNT` | `1` | Max unacked messages |
| `CONSUMER_MODE` | `ai` | `log` = fixed-rate, no AI; `ai` = the assessed path |
| `CONSUMER_RATE` | `0` | Messages/second in `log` mode; `0` = unlimited |
| `CRASH_AFTER_N` | `0` | Die on the Nth message, after processing, before acking. `0` = never |
| `CRASH_AT_MESSAGE` | *(empty)* | Die on one specific id, if you already know it |
| `CRASH_AFTER_S` | `1` | How long the crash pretends to work before dying |
| `AI_BASE_URL` | `http://127.0.0.1:11434` | Set to `http://127.0.0.1:9` to force failures |
| `AI_MODEL` | `llama3.2:1b` | Ollama model |
| `AI_TIMEOUT_S` | `60` | Per-call timeout |
| `AI_DELAY_S` | `0` | Artificial delay, to widen the crash-test window |
| `MAX_RETRIES` | `3` | Retries after the first attempt |
| `RETRY_BASE_MS` | `2000` | First backoff; doubles each retry |
| `SERVICE_PORT` | `8080` | HTTP port |
| `DB_PATH` | `lab.sqlite3` | SQLite result store |

Full descriptions and the reasoning behind each default are in `.env`.

## Troubleshooting

**`POST /process` returns 503.** The broker is unreachable. Check
`docker ps` and that `RABBITMQ_HOST`/`RABBITMQ_PORT` are right. The producer
opens a fresh connection per request, so it recovers as soon as the broker is
back.

**The consumer logs `broker connection lost ... reconnecting in 8s`.** It is
recovering on its own with exponential backoff. This is the normal response to
`docker restart rabbitMQ`; it will reattach and carry on. A full observed
sequence was 1s → 2s → 4s → 8s → 16s, then `consuming task_queue` again.

**Queue depth looks wrong in the management UI.** The UI updates on an
interval, so rapid changes appear aliased. Use `/api/queues` for current
totals, `docker exec rabbitMQ rabbitmqctl list_queues` for authoritative
numbers, or read the consumer log.

**`406 PRECONDITION_FAILED` on startup.** The queue already exists with
different arguments — usually `QUEUE_DURABLE` or `MESSAGE_PERSISTENT` was
changed after the first run. Delete the queues and restart:
`docker exec rabbitMQ rabbitmqctl delete_queue task_queue`.

**Results stuck at `processing` after a kill.** Expected under
`AUTO_ACK=true`. Restart the consumer on `AUTO_ACK=false`; a message that
survives is redelivered, one that did not is gone.

**`ollama` responses are slow on first call.** That is the model loading, not a
hang. The first request can take 40 s; the consumer log shows the duration for
every call.

**`docker exec` fails with `setns ... no such file or directory`.** A
Docker Desktop bug on some Windows hosts, unrelated to the lab. The management
API and the service still work; use them instead of `docker exec`.
