# Architecture

Two processes, one broker, one store. The producer service owns the HTTP
contract; the consumer service owns the work. They never call each other.

## Component diagram

```mermaid
flowchart TB
    Client["Client / harness<br/>POST /process, GET /result/{id}"]

    subgraph Producer["Producer Service :8080"]
        API["app/routes.py<br/>POST /process · GET /result/{id}"]
        Pub["app/producer.py<br/>enqueue()"]
    end

    subgraph Broker["RabbitMQ 4.3.5"]
        MQ["task_queue<br/>durable · DLX=task_dlx"]
        RETRY["task_queue.retry<br/>durable · DLX='' → task_queue"]
        DLX{{"task_dlx<br/>direct exchange"}}
        DLQ["task_queue.dead<br/>durable"]
    end

    subgraph Consumer["Consumer Service"]
        Cons["app/consumer.py<br/>prefetch=1 · auto_ack=False"]
        AI["app/ai_client.py<br/>transient / permanent"]
    end

    Ollama[("Ollama :11434<br/>llama3.2:1b")]
    DB[("results<br/>SQLite WAL")]

    Client -->|"1 POST /process {text}"| API
    API -->|"2 INSERT status=processing"| DB
    API -->|"3 202 {id, status}"| Client
    API --> Pub
    Pub -->|"4 basic_publish<br/>delivery_mode=2 · confirm"| MQ

    MQ -->|"5 basic_consume<br/>1 unacked max"| Cons
    Cons -->|"6 POST /api/generate"| AI
    AI --> Ollama

    Cons -->|"7 UPDATE status=completed"| DB
    Client -->|"8 GET /result/{id}"| API
    API -->|"9 SELECT"| DB

    Cons -->|"ok · 10 basic_ack"| MQ
    Cons -->|"transient, retry left<br/>11 publish x-retry+1<br/>expiration=2s/4s/8s"| RETRY
    RETRY -->|"12 TTL expiry → DLX ''"| MQ
    Cons -->|"permanent or retries exhausted<br/>13 basic_nack requeue=False"| MQ
    MQ -->|"14 x-dead-letter-*"| DLX
    DLX -->|"binding: task_queue.dead"| DLQ
    Cons -->|"UPDATE status=error"| DB

    classDef proc fill:#e8f0fe,stroke:#4a6fa5
    classDef queue fill:#fff4e5,stroke:#d9822b
    classDef ext fill:#e9f7ef,stroke:#2e8b57
    classDef data fill:#f3e8fd,stroke:#7b3fa0
    class API,Pub,Cons,AI proc
    class MQ,RETRY,DLX,DLQ queue
    class Ollama,Client ext
    class DB data
```

## Message flow, happy path

1. `POST /process {"text": "..."}` arrives.
2. The producer writes a `processing` row **before** publishing, so an
   immediate `GET /result/{id}` can never 404.
3. It publishes to `task_queue` with `delivery_mode=2` and waits for a publisher
   confirm. Then it returns `202 {"id", "status", "created_at"}` in about
   0.7 s, without ever touching the AI.
4. The consumer receives the delivery. `prefetch_count=1` means at most one
   message is unacked at a time, so the depth shown in the management UI is a
   truthful count of outstanding work.
5. The consumer calls Ollama and writes `completed` plus the answer.
6. **Only then** does it `basic_ack`. Everything before this point is
   re-doable; the ack is the commit.
7. `GET /result/{id}` reads the row and returns the stored answer.

## Message flow, failure path

| Step | Trigger | Action |
|------|---------|--------|
| 1 | AI call fails, `x-retry` < 3 | Republish to `task_queue.retry` with `x-retry+1` and `expiration` = 2s / 4s / 8s, then `basic_ack` the original |
| 2 | Retry queue TTL expires | Broker dead-letters the copy back to `task_queue` through the default exchange |
| 3 | AI call fails, `x-retry` >= 3 | `basic_nack(requeue=False)`; the broker applies `task_queue`'s `x-dead-letter-exchange` |
| 4 | — | Message arrives in `task_queue.dead` with `x-death` recording both the `expired` and `rejected` hops |
| 5 | Malformed body | `basic_reject(requeue=False)` straight to the DLQ; no retries burned |

## Failure path, sequence

```mermaid
sequenceDiagram
    autonumber
    participant C as Consumer
    participant Q as task_queue
    participant R as task_queue.retry
    participant A as Ollama
    participant D as results

    C->>Q: basic_consume (prefetch=1, auto_ack=False)
    Q-->>C: deliver (x-retry: 0)
    C->>A: POST /api/generate
    A--xC: connection refused
    C->>D: UPDATE retries=1
    C->>R: publish x-retry:1, expiration=2000
    C->>Q: basic_ack
    Note over R: 2s later, TTL expires
    R->>Q: dead-letter back to task_queue
    Q-->>C: deliver (x-retry: 1)
    C->>A: POST /api/generate
    A--xC: connection refused
    C->>R: publish x-retry:2, expiration=4000
    C->>Q: basic_ack
    Note over R: 4s later
    Q-->>C: deliver (x-retry: 2)
    C->>A: POST /api/generate
    A--xC: connection refused
    C->>R: publish x-retry:3, expiration=8000
    C->>Q: basic_ack
    Note over R: 8s later
    Q-->>C: deliver (x-retry: 3)
    C->>A: POST /api/generate
    A--xC: connection refused
    C->>D: UPDATE status=error, retries=3
    C->>Q: basic_nack(requeue=False)
    Q->>Q: apply x-dead-letter-exchange
    Q-->>Q: task_queue.dead  (x-death: expired, rejected)
```

## Why the producer and consumer are separate processes

They are separate for one reason: **the consumer has to be killable.** Harness
criterion 2 and exercise 1.3 both require killing the consumer mid-message and
proving the work comes back. A consumer running as a background thread inside
the API process could not be killed without taking the API down with it, and its
in-memory state would be lost either way.

Separating them also means the result store cannot be a plain dict. The producer
must be able to answer `GET /result/{id}` for work the consumer is doing, across
a process boundary, which is why `store.py` exists at all.

## The equivalent draw.io source

`architecture.drawio` in this directory contains the same diagram as editable
draw.io XML. Open it at <https://app.diagrams.net> to edit or re-export.
