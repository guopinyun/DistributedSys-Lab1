#!/usr/bin/env python3
"""
COMP41720 Distributed Systems (2026-27) - Lab 1 Test Harness
Asynchronous Messaging with AI Tool Integration

This is the harness the demonstrator runs at your machine at the end of the
session. Run it yourself, early and often - if it is green here, it will be
green in the room.

WHAT IT CHECKS (the three pass criteria on the demonstrator's sheet):

  [1] Contract green: POST /process and GET /result/{id} on port 8080,
      asynchronous behaviour, results delivered end-to-end.
      Plus broker hygiene: durable queue, DLQ declared and bound.
  [2] At-least-once delivery, shown live: you kill your consumer mid-message
      and the harness proves the message was redelivered and still completed.
  [3] Dead-letter queue: you break your AI dependency, the harness sends a
      poison request and watches it land in the DLQ.

USAGE
  python3 lab1_harness.py                 full check (criteria 1-3, interactive)
  python3 lab1_harness.py --quick         criterion 1 + broker checks only
  python3 lab1_harness.py --queue NAME    name your work queue explicitly
  python3 lab1_harness.py --help          all options

Requires only Python 3.8+ (standard library). The broker checks talk to the
RabbitMQ management API on http://localhost:15672 (guest/guest by default) -
that port is mapped by the docker command in the lab brief.

No part of this file needs modifying. If the harness misreads your setup,
use --queue / --dlq / --base rather than editing checks.
"""

import argparse
import base64
import json
import sys
import threading
import time
import urllib.error
import urllib.request

VERSION = "1.0 (2026-08-26)"

# --------------------------------------------------------------------------
# output helpers
# --------------------------------------------------------------------------

USE_COLOUR = sys.stdout.isatty()

def _c(code, s):
    return f"\033[{code}m{s}\033[0m" if USE_COLOUR else s

def green(s):  return _c("32", s)
def red(s):    return _c("31", s)
def yellow(s): return _c("33", s)
def bold(s):   return _c("1", s)

PASSES, FAILS, WARNS = [], [], []

def ok(msg):
    PASSES.append(msg)
    print(f"  {green('PASS')}  {msg}")

def fail(msg, hint=None):
    FAILS.append(msg)
    print(f"  {red('FAIL')}  {msg}")
    if hint:
        print(f"        {yellow('hint:')} {hint}")

def warn(msg):
    WARNS.append(msg)
    print(f"  {yellow('WARN')}  {msg}")

def section(title):
    print()
    print(bold(f"--- {title} " + "-" * max(1, 66 - len(title))))

def pause(prompt):
    print()
    input(bold(f"  >> {prompt}  [press Enter] "))

# --------------------------------------------------------------------------
# HTTP helpers (stdlib only)
# --------------------------------------------------------------------------

def http_json(method, url, body=None, auth=None, timeout=10):
    """Return (status, parsed-json-or-text). Raises URLError on transport error."""
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    if auth:
        token = base64.b64encode(f"{auth[0]}:{auth[1]}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode(errors="replace")
            status = resp.status
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        status = e.code
    try:
        return status, json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError:
        return status, raw

def extract_id(payload):
    """Pull a request id out of the many shapes teams use."""
    if isinstance(payload, str):
        s = payload.strip().strip('"')
        return s if s and len(s) < 200 and "\n" not in s else None
    if isinstance(payload, dict):
        for key in ("id", "request_id", "requestId", "requestID", "uuid", "task_id", "taskId"):
            if key in payload and payload[key] is not None:
                return str(payload[key])
        # single-entry dict whose value looks like an id
        if len(payload) == 1:
            v = next(iter(payload.values()))
            if isinstance(v, (str, int)):
                return str(v)
    return None

def status_of(payload):
    """Normalise a GET /result/{id} body to processing | completed | error | unknown."""
    if isinstance(payload, dict):
        s = str(payload.get("status", "")).lower()
        if s:
            if "process" in s or s in ("pending", "queued", "in_progress", "in-progress"):
                return "processing"
            if "complet" in s or s in ("done", "ok", "success", "succeeded", "finished"):
                return "completed"
            if "error" in s or "fail" in s:
                return "error"
        for key in ("result", "response", "answer", "output", "text", "completion"):
            v = payload.get(key)
            if isinstance(v, str) and v.strip():
                return "completed"
        return "unknown"
    if isinstance(payload, str) and payload.strip():
        low = payload.lower()
        if "processing" in low or "pending" in low:
            return "processing"
        if "error" in low or "fail" in low:
            return "error"
        return "completed"
    return "unknown"

# --------------------------------------------------------------------------
# service checks - criterion 1
# --------------------------------------------------------------------------

def submit(base, text, timeout=10):
    t0 = time.monotonic()
    status, body = http_json("POST", f"{base}/process", {"text": text}, timeout=timeout)
    elapsed = time.monotonic() - t0
    return status, body, elapsed

def wait_completed(base, rid, timeout, poll=2.0, quiet=False):
    """Poll GET /result/{rid} until completed/error/timeout. Returns final status."""
    deadline = time.monotonic() + timeout
    last = "unknown"
    while time.monotonic() < deadline:
        try:
            code, body = http_json("GET", f"{base}/result/{rid}")
        except urllib.error.URLError:
            time.sleep(poll)
            continue
        if code == 404:
            last = "not-found"
        elif 200 <= code < 300:
            last = status_of(body)
            if last in ("completed", "error"):
                return last
        if not quiet:
            print(f"        ... {rid}: {last} ({int(deadline - time.monotonic())}s left)", end="\r")
        time.sleep(poll)
    return last

def check_criterion_1(base, timeout):
    section("Criterion 1a - the service contract (POST /process, GET /result/{id})")

    # 1. reachability + id
    try:
        code, body, elapsed = submit(base, "Harness check: reply with the single word OK.")
    except urllib.error.URLError as e:
        fail(f"cannot reach {base}/process ({e.reason})",
             "is your producer running, on port 8080, on this machine?")
        return None
    if not (200 <= code < 300):
        fail(f"POST /process returned HTTP {code}",
             "the endpoint must accept {\"text\": \"...\"} and return 2xx")
        return None
    ok(f"POST /process accepted the request (HTTP {code})")

    rid = extract_id(body)
    if rid is None:
        fail(f"could not find a request id in the response: {json.dumps(body)[:120]}",
             'return e.g. {"id": "..."} so the caller can poll /result/{id}')
        return None
    ok(f"request id returned immediately: {rid}")

    # 2. asynchronous return
    if elapsed < 3.0:
        ok(f"response was immediate ({elapsed*1000:.0f} ms) - the call does not wait for the AI")
    else:
        fail(f"POST /process took {elapsed:.1f}s - it appears to wait for processing",
             "return the id as soon as the message is enqueued; do the AI call in the consumer")

    # 3. result endpoint exists
    try:
        code, body = http_json("GET", f"{base}/result/{rid}")
    except urllib.error.URLError as e:
        fail(f"cannot reach {base}/result/{{id}} ({e.reason})")
        return rid
    if code == 404:
        time.sleep(1.0)   # tolerate a tiny enqueue race, then insist
        code, body = http_json("GET", f"{base}/result/{rid}")
    if 200 <= code < 300:
        ok(f"GET /result/{{id}} responds (HTTP {code}, status: {status_of(body)})")
    else:
        fail(f"GET /result/{rid} returned HTTP {code}",
             'return 2xx with {"status": "processing"} while the job is in flight')

    # 4. end-to-end completion
    final = wait_completed(base, rid, timeout)
    print()
    if final == "completed":
        ok(f"request {rid} completed end-to-end (AI response stored and retrieved)")
    elif final == "error":
        fail(f"request {rid} finished with status \"error\"",
             "is your AI backend (Ollama / API) running and configured?")
    else:
        fail(f"request {rid} did not complete within {timeout}s (last status: {final})",
             "check the consumer is running and can reach the AI backend; "
             "use --timeout to allow longer on a cold model")

    # 5. several requests, distinct ids
    print("        sending 3 concurrent requests ...")
    results = [None, None, None]
    def _worker(i):
        try:
            c, b, _ = submit(base, f"Harness load check {i}: reply with the number {i}.")
            results[i] = extract_id(b) if 200 <= c < 300 else None
        except urllib.error.URLError:
            results[i] = None
    threads = [threading.Thread(target=_worker, args=(i,)) for i in range(3)]
    for t in threads: t.start()
    for t in threads: t.join()
    ids = [r for r in results if r]
    if len(ids) == 3 and len(set(ids)) == 3:
        ok("3 concurrent requests accepted with 3 distinct ids")
        done = sum(1 for r in ids if wait_completed(base, r, timeout, quiet=True) == "completed")
        print()
        if done == 3:
            ok("all 3 completed")
        else:
            fail(f"only {done}/3 completed within {timeout}s")
    else:
        fail(f"expected 3 distinct ids, got {ids}")

    return rid

# --------------------------------------------------------------------------
# broker checks (RabbitMQ management API) - part of criterion 1, and the
# wiring behind criteria 2 and 3
# --------------------------------------------------------------------------

class Broker:
    def __init__(self, api, auth):
        self.api = api.rstrip("/")
        self.auth = auth

    def get(self, path):
        return http_json("GET", f"{self.api}{path}", auth=self.auth)

    def queues(self):
        code, body = self.get("/api/queues")
        return body if code == 200 and isinstance(body, list) else None

    def queue(self, name, vhost="%2F"):
        code, body = self.get(f"/api/queues/{vhost}/{name}")
        return body if code == 200 and isinstance(body, dict) else None

def dlx_of(q):
    """Return (dlx, dl_routing_key) configured on a queue, from arguments or policy."""
    for src in (q.get("arguments") or {}, q.get("effective_policy_definition") or {}):
        for k in ("x-dead-letter-exchange", "dead-letter-exchange"):
            if k in src:
                rk = src.get("x-dead-letter-routing-key", src.get("dead-letter-routing-key"))
                return src[k], rk
    return None, None

def looks_like_dlq(name):
    low = name.lower()
    return any(t in low for t in ("dlq", "dead", "dead-letter", "dead_letter", "poison"))

def check_broker(broker, queue_hint, dlq_hint):
    section("Criterion 1b - broker hygiene (durable queue, DLQ declared and bound)")
    try:
        code, _ = broker.get("/api/overview")
    except urllib.error.URLError as e:
        fail(f"cannot reach the RabbitMQ management API at {broker.api} ({e.reason})",
             "start RabbitMQ with the management ports mapped, exactly as in the brief: "
             "docker run -d -p 5672:5672 -p 15672:15672 rabbitmq:4-management")
        return None, None
    if code == 401:
        fail("management API rejected the credentials",
             "default is guest/guest; override with --rabbit-user/--rabbit-pass")
        return None, None
    ok(f"management API reachable at {broker.api}")

    qs = broker.queues() or []
    work_q = None
    if queue_hint:
        work_q = next((q for q in qs if q.get("name") == queue_hint), None)
        if work_q is None:
            fail(f"queue \"{queue_hint}\" (from --queue) not found on the broker")
            return None, None
    else:
        candidates = [q for q in qs if not looks_like_dlq(q.get("name", ""))]
        # prefer: has a consumer; then durable; then a likely name
        candidates.sort(key=lambda q: (
            -(q.get("consumers") or 0),
            not q.get("durable", False),
            0 if any(t in q.get("name", "").lower() for t in ("task", "work", "process", "request")) else 1,
        ))
        work_q = candidates[0] if candidates else None
        if work_q is None:
            fail("no work queue found on the broker",
                 "declare your queue and start the consumer, or pass --queue NAME")
            return None, None
    qname = work_q["name"]
    ok(f"work queue identified: \"{qname}\" "
       f"(consumers: {work_q.get('consumers', 0)}, ready: {work_q.get('messages_ready', 0)})")

    if work_q.get("durable"):
        ok("work queue is durable=True")
    else:
        fail(f"queue \"{qname}\" is not durable",
             "declare it with durable=True so messages survive a broker restart")

    if (work_q.get("consumers") or 0) < 1:
        warn("no consumer is currently attached to the work queue - start it before the live checks")

    # DLQ wiring
    dlx, dl_rk = dlx_of(work_q)
    dlq_q = None
    if dlq_hint:
        dlq_q = next((q for q in qs if q.get("name") == dlq_hint), None)
        if dlq_q is None:
            fail(f"DLQ \"{dlq_hint}\" (from --dlq) not found on the broker")
    if dlx is not None:
        ok(f"work queue dead-letters to exchange \"{dlx or '(default)'}\""
           + (f" with routing key \"{dl_rk}\"" if dl_rk else ""))
        if dlq_q is None:
            if dlx == "" and dl_rk:                       # default exchange -> queue named by rk
                dlq_q = next((q for q in qs if q.get("name") == dl_rk), None)
            else:
                code, binds = broker.get(f"/api/exchanges/%2F/{dlx}/bindings/source")
                if code == 200 and isinstance(binds, list):
                    for b in binds:
                        if b.get("destination_type") == "queue":
                            dlq_q = next((q for q in qs if q.get("name") == b.get("destination")), None)
                            if dlq_q:
                                break
    if dlq_q is None:
        dlq_q = next((q for q in qs if looks_like_dlq(q.get("name", ""))), None)

    if dlq_q is not None and dlx is not None:
        ok(f"dead-letter queue found and bound: \"{dlq_q['name']}\" "
           f"({dlq_q.get('messages', 0)} message(s) in it now)")
    elif dlq_q is not None:
        warn(f"a queue named like a DLQ exists (\"{dlq_q['name']}\") but the work queue "
             "declares no x-dead-letter-exchange - wire it up or messages will never arrive there")
        fail("work queue has no dead-letter exchange configured",
             'declare the work queue with arguments={"x-dead-letter-exchange": ...}')
    else:
        fail("no dead-letter queue configured",
             "Task B: declare a DLX + DLQ and route messages there after max retries")

    return work_q, dlq_q

# --------------------------------------------------------------------------
# criterion 2 - at-least-once, shown live
# --------------------------------------------------------------------------

def check_redelivery(base, broker, qname, timeout):
    section("Criterion 2 - at-least-once delivery (live kill + redelivery)")
    print("  The harness sends one request. Your job: kill the consumer (Ctrl-C)")
    print("  while that message is being processed, i.e. within a couple of seconds")
    print("  of pressing Enter. The harness then watches the broker for the")
    print("  redelivery and finally proves the same request still completes.")

    for attempt in (1, 2, 3):
        pause(f"attempt {attempt}/3 - press Enter to send, then IMMEDIATELY kill your consumer")
        try:
            code, body, _ = submit(base, "Harness redelivery check: reply with the word REDELIVERED.")
        except urllib.error.URLError as e:
            fail(f"could not send the probe request ({e.reason})")
            return False
        rid = extract_id(body) if 200 <= code < 300 else None
        if rid is None:
            fail("probe request was not accepted")
            return False
        print(f"        sent {rid}; watching queue \"{qname}\" for the kill ...")

        saw_kill = False
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            q = broker.queue(qname)
            if q is None:
                break
            consumers = q.get("consumers") or 0
            ready = q.get("messages_ready") or 0
            unacked = q.get("messages_unacknowledged") or 0
            print(f"        consumers={consumers} ready={ready} unacked={unacked}   ", end="\r")
            if consumers == 0:
                saw_kill = True
                if ready > 0:
                    break
            time.sleep(1.0)
        print()

        early = wait_completed(base, rid, 1, poll=0.5, quiet=True)
        if not saw_kill:
            if early == "completed":
                warn("the consumer finished before you killed it - too quick! trying again")
                continue
            warn("never saw the consumer disappear from the queue - was it killed?")
            continue

        q = broker.queue(qname)
        if q is not None and (q.get("messages_ready") or 0) + (q.get("messages_unacknowledged") or 0) > 0:
            ok("consumer killed and the in-flight message returned to the queue (not lost)")
        elif early == "completed":
            warn("consumer died but the message had already been acknowledged - "
                 "kill it faster, or slow processing (e.g. stop Ollama briefly)")
            continue
        else:
            ok("consumer killed; message not acknowledged (still owned by the broker)")

        pause("now RESTART your consumer, and press Enter once it is connected")
        final = wait_completed(base, rid, timeout)
        print()
        if final == "completed":
            ok(f"request {rid} was redelivered after the restart and completed - at-least-once shown")
            return True
        fail(f"request {rid} did not complete after the consumer restart (status: {final})",
             "is the consumer acknowledging only AFTER processing (manual ack)? "
             "with auto-ack the message dies with the consumer")
        return False

    fail("could not demonstrate a redelivery in 3 attempts",
         "make the window easier to hit: stop Ollama so processing hangs, kill the "
         "consumer, restart Ollama, restart the consumer")
    return False

# --------------------------------------------------------------------------
# criterion 3 - poison message lands in the DLQ
# --------------------------------------------------------------------------

def check_dlq(base, broker, dlq_name, timeout):
    section("Criterion 3 - poison message ends up in the dead-letter queue")
    print("  Break your AI dependency so processing fails every time - e.g. stop")
    print("  Ollama, or point the consumer's AI URL at a dead port and restart it.")
    print("  The harness sends one poison request and watches the DLQ. Your retry")
    print("  limit (e.g. 3 attempts) must be finite, or nothing will ever arrive.")

    q0 = broker.queue(dlq_name)
    if q0 is None:
        fail(f"cannot read DLQ \"{dlq_name}\" from the management API")
        return False
    before = q0.get("messages") or 0
    print(f"        DLQ \"{dlq_name}\" currently holds {before} message(s)")

    pause("break the AI dependency now, then press Enter to send the poison request")
    try:
        code, body, _ = submit(base, "Harness poison check: this request is expected to fail.")
    except urllib.error.URLError as e:
        fail(f"could not send the poison request ({e.reason})")
        return False
    rid = extract_id(body) if 200 <= code < 300 else None
    if rid is None:
        fail("poison request was not accepted by POST /process")
        return False
    print(f"        sent {rid}; waiting for it to exhaust its retries ...")

    deadline = time.monotonic() + max(90, timeout)
    while time.monotonic() < deadline:
        q = broker.queue(dlq_name)
        now = (q.get("messages") or 0) if q else before
        print(f"        DLQ depth: {now} ({int(deadline - time.monotonic())}s left)   ", end="\r")
        if now > before:
            print()
            ok(f"poison message arrived in \"{dlq_name}\" (depth {before} -> {now})")
            code, rbody = http_json("GET", f"{base}/result/{rid}")
            if 200 <= code < 300 and status_of(rbody) == "error":
                ok('GET /result/{id} reports status "error" for the poisoned request')
            else:
                warn('consider reporting status "error" on /result/{id} for dead-lettered requests')
            print()
            print(yellow("        remember to restore your AI configuration "
                         "(restart Ollama / fix the URL) before the next check."))
            return True
        time.sleep(2.0)
    print()
    fail(f"nothing arrived in \"{dlq_name}\" within {int(max(90, timeout))}s",
         "check: retries are capped (max 3), and after the last failure the message is "
         "rejected/nacked with requeue=False so RabbitMQ dead-letters it")
    return False

# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="COMP41720 Lab 1 test harness (async messaging).",
        epilog="Green here means green in the room. Run early, run often.")
    ap.add_argument("--base", default="http://localhost:8080",
                    help="base URL of your producer service (default: %(default)s)")
    ap.add_argument("--rabbit", default="http://localhost:15672",
                    help="RabbitMQ management API URL (default: %(default)s)")
    ap.add_argument("--rabbit-user", default="guest")
    ap.add_argument("--rabbit-pass", default="guest")
    ap.add_argument("--queue", help="name of your work queue (otherwise auto-detected)")
    ap.add_argument("--dlq", help="name of your dead-letter queue (otherwise auto-detected)")
    ap.add_argument("--timeout", type=int, default=120,
                    help="seconds to wait for a result to complete (default: %(default)s)")
    ap.add_argument("--quick", action="store_true",
                    help="run only criterion 1 + broker checks (no interactive parts)")
    args = ap.parse_args()

    print(bold(f"COMP41720 Lab 1 harness {VERSION}"))
    print(f"producer: {args.base}   broker API: {args.rabbit}")

    check_criterion_1(args.base, args.timeout)

    broker = Broker(args.rabbit, (args.rabbit_user, args.rabbit_pass))
    work_q, dlq_q = check_broker(broker, args.queue, args.dlq)

    c1 = not FAILS
    c2 = c3 = None
    if not args.quick:
        if work_q is not None:
            pre_fails = len(FAILS)
            c2 = check_redelivery(args.base, broker, work_q["name"], args.timeout)
        else:
            warn("skipping criterion 2 - no work queue identified")
        if dlq_q is not None:
            c3 = check_dlq(args.base, broker, dlq_q["name"], args.timeout)
        else:
            warn("skipping criterion 3 - no DLQ identified")

    # ---------------------------------------------------------------- summary
    def box(flag):
        if flag is None:
            return "[- ]"
        return green("[ x ]") if flag else red("[   ]")

    section("Summary - demonstrator tick-sheet")
    print(f"   {box(c1)}  1. Contract green: POST /process + GET /result/{{id}} on :8080,")
    print(f"            async end-to-end, durable queue, DLQ declared and bound")
    print(f"   {box(c2)}  2. At-least-once shown live, with a redelivery")
    print(f"   {box(c3)}  3. Poison message landed in the DLQ")
    if args.quick:
        print(f"\n   (criteria 2-3 not run: --quick. Run the full harness in the session.)")
    print()
    total_bad = len(FAILS)
    if total_bad == 0 and (args.quick or (c2 and c3)):
        print(green(bold("   ALL CHECKS PASSED")) + f"   ({len(PASSES)} passes, {len(WARNS)} warnings)")
    else:
        print(red(bold(f"   {total_bad} CHECK(S) FAILED")) + f"   ({len(PASSES)} passes, {len(WARNS)} warnings)")
        print("   failed:")
        for f_ in FAILS:
            print(f"     - {f_}")
    sys.exit(0 if total_bad == 0 else 1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\ninterrupted.")
        sys.exit(130)
