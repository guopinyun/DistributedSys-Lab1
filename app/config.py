"""All tunables in one place.

Values are read from the process environment. `load_dotenv()` fills in anything
that is not already exported, so a `.env` file is enough to configure the whole
lab on any OS. Shell variables always win over `.env` (load_dotenv does not
override by default), which is what makes the Part 1 exercise walkthroughs
readable: edit `.env`, restart, observe.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


def _flag(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


def _number(name: str, default: float) -> float:
    return float(os.getenv(name, default))


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, default))


# --- broker connection (Step 1 of the brief) ---------------------------------
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "127.0.0.1")
RABBITMQ_PORT = _int("RABBITMQ_PORT", 5672)
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

# Low heartbeats make a hard-killed consumer disappear from the broker quickly.
# Ctrl-C closes the socket immediately and does not rely on this, but it bounds
# the damage when a process is killed rather than asked to stop.
RABBITMQ_HEARTBEAT = _int("RABBITMQ_HEARTBEAT", 30)

# --- queue names -------------------------------------------------------------
# The brief fixes the work queue name; the rest follow from the retry/DLQ design.
QUEUE_NAME = os.getenv("QUEUE_NAME", "task_queue")
DEAD_QUEUE_NAME = os.getenv("DEAD_QUEUE_NAME", f"{QUEUE_NAME}.dead")
RETRY_QUEUE_NAME = os.getenv("RETRY_QUEUE_NAME", f"{QUEUE_NAME}.retry")
DEAD_LETTER_EXCHANGE = os.getenv("DEAD_LETTER_EXCHANGE", "task_dlx")
DEAD_LETTER_ROUTING_KEY = os.getenv("DEAD_LETTER_ROUTING_KEY", DEAD_QUEUE_NAME)

# AMQP header carrying the retry attempt count across the retry queue.
RETRY_HEADER = "x-retry"

# --- exercise 1.3: acknowledgement, durability, crash ------------------------
QUEUE_DURABLE = _flag("QUEUE_DURABLE", True)
MESSAGE_PERSISTENT = _flag("MESSAGE_PERSISTENT", True)

# auto_ack=True hands the message to the consumer and forgets it exists, so a
# crash mid-processing destroys it. That is exercise 1.3's first observation.
AUTO_ACK = _flag("AUTO_ACK", False)

# Die on the Nth message this process handles, after processing it and before
# acking it. This is the usable trigger: POST /process generates the message id,
# so you cannot know it in advance. 0 = never crash.
CRASH_AFTER_N = int(_number("CRASH_AFTER_N", 0))

# Die on one specific message id instead. Only useful if you already know the
# id, e.g. re-testing a redelivery by hand. Blank = never crash.
CRASH_AT_MESSAGE = os.getenv("CRASH_AT_MESSAGE", "").strip()

# How long the crash simulation pretends to work before dying. It has to sit
# *after* processing and *before* the ack, which is the interesting window.
CRASH_AFTER_S = _number("CRASH_AFTER_S", 2.0)

# --- exercise 1.2 vs task A: one consumer, two behaviours --------------------
# "log" processes at a controlled rate without touching the AI, which is the
# only way to watch queue depth fall at 1 msg/s (a real llama3.2:1b call takes
# ~10s). "ai" is the assessed end-to-end path.
CONSUMER_MODE = os.getenv("CONSUMER_MODE", "ai").strip().lower()

# Messages per second in "log" mode. 0 means as fast as possible. In "ai" mode
# the model latency dominates this anyway (~0.1 msg/s measured on llama3.2:1b).
CONSUMER_RATE = _number("CONSUMER_RATE", 0.0)

# QoS: never let the broker push more than this many unacked messages at us,
# otherwise pika buffers the queue client-side and the management UI lies.
PREFETCH_COUNT = _int("PREFETCH_COUNT", 1)

# --- task A: AI backend (Step 3, Option A: local Ollama) ---------------------
AI_BASE_URL = os.getenv("AI_BASE_URL", "http://127.0.0.1:11434")
AI_MODEL = os.getenv("AI_MODEL", "llama3.2:1b")
AI_TIMEOUT_S = _number("AI_TIMEOUT_S", 60.0)
AI_MAX_TOKENS = _int("AI_MAX_TOKENS", 64)

# Artificial delay before the AI call, purely to widen the window in which a
# demonstrator has to hit Ctrl-C during the harness's criterion 2 check.
AI_DELAY_S = _number("AI_DELAY_S", 0.0)

# --- task A / task B: retry policy ------------------------------------------
# Total attempts = MAX_RETRIES + 1, so 3 means "try once, retry three times,
# then dead-letter", matching the brief's "after N failed retries (e.g. 3)".
MAX_RETRIES = _int("MAX_RETRIES", 3)

# Exponential backoff, per message `expiration`. Attempt n waits
# RETRY_BASE_MS * 2**n ms inside the retry queue before flowing back, so 2s, 4s,
# 8s with the default. Deliberately short so the whole dead-letter path settles
# well inside the harness's 120s wait.
RETRY_BASE_MS = _int("RETRY_BASE_MS", 2000)

# --- task A: result store ----------------------------------------------------
# Resolved against the project directory rather than the shell's working
# directory, so the producer and the consumer agree on the file even though
# they are separate processes started from different places.
_db_path = Path(os.getenv("DB_PATH", "lab.sqlite3"))
DB_PATH = str(_db_path if _db_path.is_absolute() else BASE_DIR / _db_path)

# --- service -----------------------------------------------------------------
SERVICE_HOST = os.getenv("SERVICE_HOST", "127.0.0.1")
SERVICE_PORT = _int("SERVICE_PORT", 8080)
