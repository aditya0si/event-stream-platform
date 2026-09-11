"""End-to-end smoke test against the compose stack.

This runs the system as a deployed unit rather than as a test harness: real containers, real
broker, real database, and requests crossing all of them. It grows with each milestone — the
checks below are the ones M1 can honestly make, and later milestones add to them.

Why a separate script instead of more Go tests: the Go suite proves components work when a
test constructs them. This proves the *deployment* works — that the image carries the right
entrypoints, that containers resolve each other by service name, that the migration container
completed before the service started, and that the topics the design depends on exist with the
partition counts the design requires. On this project's sibling those are exactly the questions
that found defects the unit tests could not see.

Exits non-zero on any failed check, so it is a gate rather than a report.
"""

import json
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone

INGEST = "http://localhost:8081"
# The SSE gateway. Separate process, separate port: it serves browsers, not producers.
GATEWAY = "http://localhost:8082"

results = []


def check(label, ok, detail=""):
    results.append((label, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}" + (f"  — {detail}" if detail else ""))


def req(path, base=INGEST, timeout=15):
    try:
        with urllib.request.urlopen(base + path, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, (json.loads(raw) if raw else {})
        except Exception:
            return e.code, {"_raw": raw[:200].decode(errors="replace")}
    except Exception as e:  # connection refused, timeout, DNS
        return 0, {"_error": str(e)}


def post(path, body, base=INGEST, timeout=15):
    """POST a JSON body, returning (status, parsed body) even for error responses."""
    data = json.dumps(body).encode()
    r = urllib.request.Request(base + path, data=data, method="POST")
    r.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, (json.loads(raw) if raw else {})
        except Exception:
            return e.code, {"_raw": raw[:200].decode(errors="replace")}
    except Exception as e:
        return 0, {"_error": str(e)}


def broker_message_count(topic):
    """Total records in a topic, summed from `rpk topic describe -p` high-watermarks.

    Consuming with a fixed --num would either block waiting for messages that never come or
    read fewer than exist. Reading the count first lets the consume below read exactly what is
    there and exit promptly.
    """
    ok, out = docker(["exec", "esp-redpanda", "rpk", "topic", "describe", topic, "-p"], timeout=60)
    if not ok:
        return None
    lines = [l.split() for l in out.splitlines() if l.strip()]
    for i, parts in enumerate(lines):
        if "HIGH-WATERMARK" in parts:
            col = parts.index("HIGH-WATERMARK")
            total = 0
            for row in lines[i + 1:]:
                if col < len(row) and row[col].isdigit():
                    total += int(row[col])
            return total
    return None


def docker(args, timeout=30):
    """Run a docker command, returning (ok, stdout+stderr)."""
    p = subprocess.run(["docker", *args], capture_output=True, text=True,
                       encoding="utf-8", errors="replace", timeout=timeout)
    return p.returncode == 0, (p.stdout + p.stderr).strip()


def docker_stdin(args, data, timeout=60):
    """Run a docker command with data on stdin (rpk topic produce needs it).

    `data` may be bytes: the payload here is often deliberately not valid UTF-8 text, and the
    caller should not have to think about that. It is decoded for the pipe because this function
    runs subprocess in text mode (the reply is parsed as text), and text mode requires a str on
    stdin — passing bytes is a TypeError, which is exactly how the first version of this
    function failed.
    """
    if isinstance(data, (bytes, bytearray)):
        data = bytes(data).decode("utf-8", errors="replace")
    p = subprocess.run(["docker", *args], input=data, capture_output=True,
                       encoding="utf-8", errors="replace", timeout=timeout)
    return p.returncode == 0, (p.stdout + p.stderr).strip()


def group_lag(group, topic):
    """Total consumer lag for a group on one topic, or None when it cannot be read.

    Lag is the only honest way to assert "the consumer did not stall": it is the broker's own
    count of records the group has not committed, so zero means every record — including any
    the consumer refused — has been dealt with.

    The LAG column's position is read from the header rather than hardcoded, because the first
    version of this function hardcoded it and was silently wrong: it assumed a five-column row
    and therefore summed LOG-END-OFFSET instead, reporting lag=10 for a group whose own
    TOTAL-LAG line — printed by rpk itself, in the same output — read 0. A monitor that invents
    a column layout reports an outage that is not happening, which is worse than reporting
    nothing. (The other parser in this file, for `rpk topic describe`, already read its header;
    this one did not.)
    """
    ok, out = docker(["exec", "esp-redpanda", "rpk", "group", "describe", group], timeout=45)
    if not ok:
        return None

    lag_col = None
    total = 0
    saw_row = False
    for line in out.splitlines():
        parts = line.split()
        if lag_col is None:
            # Everything above the per-partition table is the group summary; this is the header.
            if "TOPIC" in parts and "LAG" in parts:
                lag_col = parts.index("LAG")
            continue
        if len(parts) <= lag_col or parts[0] != topic:
            continue
        # A dash means "no committed offset and nothing to lag behind" — a partition that has
        # never received a record. It contributes nothing, and parsing it as a number would be
        # a crash rather than a wrong answer.
        if parts[lag_col] == "-":
            continue
        if re.fullmatch(r"-?\d+", parts[lag_col]):
            saw_row = True
            total += max(0, int(parts[lag_col]))
    return total if saw_row else None


print("=== 1. the stack is up ===")
ok, out = docker(["compose", "ps", "--format", "{{.Name}} {{.Status}}"])
for line in out.splitlines():
    print(f"    {line}")
# The migration container is expected to have exited 0 — it is a one-shot, and `compose ps`
# only lists running services, so its absence here is correct rather than a failure.
check("docker compose ps succeeded", ok)

print("\n=== 2. liveness and readiness ===")
s, body = req("/healthz")
check("/healthz is alive", s == 200 and body.get("status") == "alive", f"{s} {body}")

s, body = req("/readyz")
checks = body.get("checks", {}) if isinstance(body, dict) else {}
check("/readyz reports ready", s == 200 and body.get("status") == "ready", f"{s}")
# Each dependency is asserted individually: "not ready" with no reason is the failure this
# script exists to make specific.
for dep in ("postgres", "redis", "broker"):
    check(f"  readiness names {dep} as ok", checks.get(dep) == "ok", str(checks.get(dep)))

print("\n=== 3. metrics are exposed, including dependency gauges ===")
try:
    with urllib.request.urlopen(INGEST + "/metrics", timeout=15) as resp:
        metrics_text = resp.read().decode()
except Exception as e:
    metrics_text = ""
    check("/metrics reachable", False, str(e))
if metrics_text:
    check("/metrics reachable", True)
    # Only gauges are asserted here, because a gauge always exposes a value and a counter
    # vector does not: `ingest_requests_total` is a vec with no children until the
    # `/v1/events` endpoint exists (M2), so asserting its presence now would be asserting a
    # feature this milestone does not have. The gauge check below is the load-bearing one —
    # it must read 1, not merely exist.
    for name in ("db_up", "redis_up", "broker_up"):
        check(f"  exposes {name}", name in metrics_text)
    for name in ("db_up", "redis_up", "broker_up"):
        for line in metrics_text.splitlines():
            if line.startswith(name + " "):
                check(f"  {name} reads 1", line.strip().endswith(" 1"), line.strip())
                break

print("\n=== 4. the migration container did its job ===")
ok, out = docker(["inspect", "esp-migrate", "--format", "{{.State.Status}} exit={{.State.ExitCode}}"])
check("migrate container exited 0", ok and "exit=0" in out, out)

print("\n=== 5. topics exist with the partition counts the design requires ===")
# Read from the broker, not from the config: the point is to catch a topic that exists with
# the wrong shape, which would silently cap consumer parallelism.
ok, out = docker(["exec", "esp-redpanda", "rpk", "topic", "list"])
check("rpk topic list succeeded", ok, out.splitlines()[0] if out else "")
for topic, want in (("telemetry.raw.v1", 6), ("telemetry.dlq.v1", 3)):
    ok, out = docker(["exec", "esp-redpanda", "rpk", "topic", "describe", topic])
    # `rpk topic describe` prints a SUMMARY block whose PARTITIONS line is the count. The
    # first version of this script tried to count per-partition rows, which this rpk version
    # does not print — so it reported 0 partitions for a topic that had 6, and the failure
    # looked like a broker problem rather than a parsing bug.
    parts = None
    for line in out.splitlines():
        m = re.match(r"^PARTITIONS\s+(\d+)", line.strip())
        if m:
            parts = int(m.group(1))
            break
    check(f"{topic} exists with {want} partitions", parts == want,
          f"found {parts}" if parts is not None else "PARTITIONS line not found in describe output")

print("\n=== 6. ingest actually publishes to the log (M2) ===")
# A 202 on its own proves nothing about durability: a handler that accepted the batch and
# dropped it would answer identically. The proof is reading the event back off the broker,
# which is why this consumes rather than trusting the reply.
event_id = str(uuid.uuid4())
s, b = post("/v1/events", {"events": [{
    "vehicle_id": "smoke-v1",
    "route_id": "smoke-r1",
    "lat": 12.9716,
    "lon": 77.5946,
    "speed_kph": 21.5,
    "event_ts": datetime.now(timezone.utc).isoformat(),
    "sequence": 1,
    "event_id": event_id,
}]})
check("POST /v1/events answers 202", s == 202, f"{s} {b}")
check("  the batch was accepted", isinstance(b, dict) and b.get("accepted") == 1, str(b))

n = broker_message_count("telemetry.raw.v1")
if n is None:
    check("  the topic's high-watermarks are readable", False, "rpk topic describe -p failed")
elif n == 0:
    check("  the event reached the log", False,
          "the topic is empty after a successful POST: the handler accepted and published nothing")
else:
    # `timeout` runs inside the container (busybox provides it) so a stall fails this check
    # instead of hanging the script; the outer subprocess timeout is the backstop.
    ok, out = docker(["exec", "esp-redpanda", "timeout", "45", "rpk", "topic", "consume",
                      "telemetry.raw.v1", "-o", "start", "--num", str(n), "-f", "%v"], timeout=90)
    check("  the event reached the log", event_id in out,
          f"searched {n} record(s) for the posted event id")

print("\n=== 6b. the submission contract is pinned (flat observation, not the envelope) ===")
# Sending the envelope shape must be refused, and the refusal must name the field. This mistake
# has now been made twice — once in cmd/simulate, once in load/ingest.js — and each time it was
# silent: the batches returned 202 while every event inside them was rejected. A check that ties
# the two halves of the contract together is cheaper than finding it a third time.
_s, _b = post("/v1/events", {"events": [{
    "schema_version": 1,
    "event_id": str(uuid.uuid4()),
    "event_type": "vehicle.position",
    "produced_at": datetime.now(timezone.utc).isoformat(),
    "source": "smoke",
    "payload": {"vehicle_id": "smoke-envelope", "route_id": "smoke-r1", "lat": 1.0, "lon": 2.0,
                "event_ts": datetime.now(timezone.utc).isoformat(), "sequence": 1},
}]})
_rejected = (_b.get("rejected") or []) if isinstance(_b, dict) else []
check("an envelope-shaped submission is refused, naming the offending field",
      _s == 202 and len(_rejected) == 1 and "schema_version" in json.dumps(_rejected),
      f"{_s} {json.dumps(_b)[:150]}")

# And the flat shape is accepted, so the check above cannot pass by refusing everything.
_s, _b = post("/v1/events", {"events": [{
    "vehicle_id": "smoke-flat-" + uuid.uuid4().hex[:8], "route_id": "smoke-r1",
    "lat": 12.97, "lon": 77.59, "speed_kph": 12.5, "bearing_deg": 45.0,
    "event_ts": datetime.now(timezone.utc).isoformat(), "sequence": 1,
}]})
check("  and the flat observation it is paired with is accepted",
      _s == 202 and isinstance(_b, dict) and _b.get("accepted") == 1, f"{_s} {_b}")

print("\n=== 7. the consumer materializes events into Postgres (M3) ===")
# The proof that the whole deployed path works: an event posted over HTTP must end up as a
# row in the sink, which can only happen if the broker carried it AND the consumer service
# applied it. Asserting on the HTTP reply alone would prove nothing about either.
consumer_event_id = str(uuid.uuid4())
consumer_vehicle = "smoke-consumer-" + consumer_event_id[:8]
s, b = post("/v1/events", {"events": [{
    "vehicle_id": consumer_vehicle,
    "route_id": "smoke-r1",
    "lat": 12.9716,
    "lon": 77.5946,
    "speed_kph": 21.5,
    "event_ts": datetime.now(timezone.utc).isoformat(),
    "sequence": 1,
    "event_id": consumer_event_id,
}]})
check("POST /v1/events answers 202", s == 202, f"{s} {b}")

# Poll rather than sleep: the consumer is a separate process on a poll loop, so the row
# appears asynchronously. A fixed sleep would either be too short (a flaky failure on a slow
# runner) or too long (a slow gate); polling is bounded and finishes as soon as the row lands.
def psql(sql, timeout=20):
    ok, out = docker(["exec", "esp-postgres", "psql", "-U", "esp", "-d", "event_stream",
                      "-tAc", sql], timeout=timeout)
    return ok, out.strip()

applied = None
deadline = time.time() + 60
while time.time() < deadline:
    ok, out = psql(f"SELECT count(*) FROM vehicle_positions WHERE event_id = '{consumer_event_id}'")
    if ok and out == "1":
        applied = True
        break
    if ok and out not in ("0", ""):
        applied = out  # something unexpected: surface it rather than looping
        break
    time.sleep(1)
if applied is None:
    applied = "0"

check("the consumer applied the event to vehicle_positions", applied is True,
      f"count={applied!r} after 60s")

# The current-state row is written in the same transaction, so it must agree. Checking both
# is what distinguishes "the consumer ran" from "the consumer ran the code path I think it did".
if applied is True:
    ok, out = psql(f"SELECT last_event_id FROM vehicle_current WHERE vehicle_id = '{consumer_vehicle}'")
    check("  vehicle_current names the same event", ok and out == consumer_event_id, out or "(no row)")

# And the dedup record exists, which is what makes a redelivery a no-op rather than a
# second application.
if applied is True:
    ok, out = psql(f"SELECT count(*) FROM processed_events WHERE event_id = '{consumer_event_id}'")
    check("  the deduplication record exists", ok and out == "1", f"count={out}")

print("\n=== 8. a record that cannot be decoded is refused without stalling the stream (M4) ===")
# Published straight to the log rather than through POST /v1/events, and that is the point: the
# handler validates and rejects malformed events, so an undecodable record can only reach the
# log from a producer that bypassed validation — which is exactly the case the dead-letter path
# exists for.
poison_marker = "poison-" + uuid.uuid4().hex[:12]
# The trailing newline is required, not cosmetic: `rpk topic produce` reads one record per line
# from stdin, and a payload that ends without one produces "record read error: unexpected EOF".
poison_bytes = ("{not valid json, marker " + poison_marker + "\n").encode()
ok, out = docker_stdin(["exec", "-i", "esp-redpanda", "rpk", "topic", "produce",
                        "telemetry.raw.v1", "-k", "poison-vehicle"], poison_bytes, timeout=60)
check("a poison record was published straight to the log", ok, out.splitlines()[-1] if out else "")

# A healthy event after it, so "the consumer kept moving" is a statement about a record that
# followed the poison one rather than about a system that happened to be quiet.
healthy_id = str(uuid.uuid4())
healthy_vehicle = "smoke-after-poison-" + healthy_id[:8]
s, b = post("/v1/events", {"events": [{
    "vehicle_id": healthy_vehicle, "route_id": "smoke-r1",
    "lat": 13.0001, "lon": 77.6001, "speed_kph": 18.0,
    "event_ts": datetime.now(timezone.utc).isoformat(), "sequence": 1, "event_id": healthy_id,
}]})
check("  a healthy event follows it", s == 202, f"{s} {b}")

# Drain to zero lag: the broker's own measure that every record on every partition has been
# dealt with, refused ones included.
lag = None
deadline = time.time() + 90
while time.time() < deadline:
    lag = group_lag("sink-v1", "telemetry.raw.v1")
    if lag == 0:
        break
    time.sleep(2)
check("  the consumer group drains to zero lag", lag == 0, f"lag={lag}")

ok, out = psql(f"""SELECT event_id || '|' || reason FROM dead_letters
                   WHERE encode(raw_payload, 'escape') LIKE '%{poison_marker}%'
                   ORDER BY last_failed_at DESC LIMIT 1""")
poison_event_id = ""
if ok and "|" in out:
    poison_event_id, _, poison_reason = out.partition("|")
    check("  the refusal was recorded with reason 'decode'", poison_reason.strip() == "decode", out)
else:
    check("  the refusal was recorded", False, out or "(no row: the poison record vanished)")

ok, out = psql(f"SELECT count(*) FROM vehicle_positions WHERE event_id = '{healthy_id}'")
check("  the healthy event was still applied", ok and out == "1",
      f"count={out}: a poison record must not block the events behind it")

print("\n=== 9. an operator can replay a refusal through the same image (M4) ===")
if not poison_event_id:
    check("replay CLI", False, "no poison row to replay (section 8 failed)")
else:
    ok, out = docker(["compose", "run", "--rm", "replay", "replay", "--event-id", poison_event_id],
                     timeout=120)
    check("`replay replay --event-id` exits 0", ok, out.splitlines()[-1] if out else "")
    check("  it reports the republish", "republished" in out, out[-200:] if out else "")

    # The republished record must be on the log with its provenance. This is the durable proof
    # that replay put the *original bytes* back rather than writing an effect itself.
    #
    # The format is `%h{ ... }` rather than a bare `%h`: this rpk version requires a modifier
    # block for headers and refuses to run without one (verified against the pinned image).
    n = broker_message_count("telemetry.raw.v1") or 0
    ok, out = docker(["exec", "esp-redpanda", "timeout", "45", "rpk", "topic", "consume",
                      "telemetry.raw.v1", "-o", "start", "--num", str(n),
                      "-f", "%h{ %k=%v }|%v"], timeout=90)
    check("  the replayed record carries x-replay provenance", "x-replay" in out,
          f"searched {n} record(s)")
    check("  and the original bytes came back unchanged", poison_marker in out,
          "the marker from the refused payload was found in the log again")

print("\n=== 10. the live stream, and a reconnect that resumes exactly the missed frames (M5) ===")


def fetch_text(path, base=GATEWAY, timeout=10):
    """Fetch a response as text, for the endpoints that are not JSON.

    `req` parses JSON, so it would report the HTML viewer as a failure — and the viewer being
    served is one of the things this section has to check.
    """
    try:
        with urllib.request.urlopen(base + path, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace"), \
                resp.headers.get("Content-Type", "")
    except urllib.error.HTTPError as e:
        return e.code, "", e.headers.get("Content-Type", "")
    except Exception as e:
        return 0, "", str(e)


def stream_read(path, headers=None, until=None, seconds=12, base=GATEWAY):
    """Read an SSE stream, returning (status, text, error).

    The socket timeout is per read rather than per stream: an SSE response is open by design, so
    a timeout means "nothing arrived yet", not "this failed". The overall deadline is what ends
    the loop, and it should be the only thing that does: the read timeout is set above the
    gateway's heartbeat interval so ordinary silence never trips it.

    That margin matters because of how Python's socket layer behaves after a timeout — the file
    object behind the response is poisoned, and every later read raises OSError("cannot read
    from timed out object") instead of waiting again. The first version of this helper used a
    3-second timeout and reported exactly that string as the failure of a live-frames check: the
    error described the reader, not the stream. It is 20 s here (the gateway heartbeats at 15 s)
    and a poisoned object is treated as "nothing more will arrive" rather than as a fault.
    """
    text, status = "", 0
    deadline = time.time() + seconds
    try:
        req = urllib.request.Request(base + path, headers=headers or {})
        with urllib.request.urlopen(req, timeout=20) as resp:
            status = resp.status
            while time.time() < deadline:
                try:
                    line = resp.readline()
                except (socket.timeout, TimeoutError):
                    continue
                except OSError:
                    # TimeoutError is an OSError, so this arm is reached only for the poisoned
                    # object described above: no further bytes are coming on this connection.
                    break
                if not line:
                    break
                text += line.decode("utf-8", errors="replace")
                if until and until(text):
                    break
    except urllib.error.HTTPError as e:
        return e.code, text, f"HTTP {e.code}"
    except Exception as e:
        return status, text, str(e)
    return status, text, ""


s, b = req("/readyz", base=GATEWAY)
check("the gateway reports ready", s == 200, f"{s} {b}")

s, html, ctype = fetch_text("/")
check("the viewer is served at / and carries the live script",
      s == 200 and "new EventSource" in html and "text/html" in ctype, f"{s} {ctype}")

s, _, _ = fetch_text("/definitely-not-a-route")
check("  an unknown path is a 404, not the viewer", s == 404, f"{s}")

status, text, err = stream_read("/v1/stream", until=lambda t: '"resumed":false' in t, seconds=8)
ready_ok = status == 200 and "event: ready" in text
# The detail must describe the *passing* case too: an `err or "no frame"` fallback reads like a
# failure on a passing check, which makes the transcript argue with itself.
check("a fresh stream opens with a ready frame", ready_ok,
      f"{status}, {len(text)}B received" if ready_ok else (err or "no ready frame within 8s"))
# `id:` on a control frame would become the client's Last-Event-ID on reconnect, pointing at a
# frame the history has never heard of.
# The ready frame is isolated before asserting, so a position frame arriving in the same read
# window cannot make this check lie in either direction.
ready_frame = text.split("event: ready", 1)[1].split("\n\n", 1)[0] if "event: ready" in text else ""
check("  and its control frame carries no id", "id:" not in ready_frame,
      ready_frame.replace("\n", " ")[:100])

# A live frame, delivered over the bus rather than polled: the client connects *before* the event
# is posted, so Redis Pub/Sub is the only way it can arrive. This is the M5 wire end to end.
live_vehicle = "smoke-live-" + uuid.uuid4().hex[:8]
bucket = {}


def live_reader():
    bucket["status"], bucket["text"], bucket["err"] = stream_read(
        "/v1/stream", until=lambda t: '"event_id":"' in t, seconds=15)


reader = threading.Thread(target=live_reader, daemon=True)
reader.start()
time.sleep(1.0)  # let the subscription establish before the event exists

s, b = post("/v1/events", {"events": [{
    "vehicle_id": live_vehicle, "route_id": "smoke-r-live",
    "lat": 13.05, "lon": 77.62, "speed_kph": 24.0, "sequence": 1,
    "event_ts": datetime.now(timezone.utc).isoformat(),
}]})
check("  an event posted to a connected client is accepted", s == 202, f"{s} {b}")
reader.join(timeout=20)

m = re.search(r'"event_id":"([0-9a-f-]{36})"', bucket.get("text", ""))
live_id = m.group(1) if m else ""
if live_id:
    ok, out = psql(f"SELECT count(*) FROM vehicle_positions WHERE event_id = '{live_id}'")
else:
    ok, out = False, ""
check("  it reaches the stream over the live bus, and is durable",
      bool(live_id) and ok and out == "1",
      f"frame_id={live_id or '(none)'} row={out or '(none)'} err={bucket.get('err') or ''}")

# Resume. Three events share one event_ts on purpose: a resume that filtered on the timestamp
# alone would drop the two that follow the resume point inside the same second, which is exactly
# the difference between comparing (event_ts, event_id) and a plain `event_ts >`.
resume_vehicle = "smoke-resume-" + uuid.uuid4().hex[:8]
shared_ts = (datetime.now(timezone.utc) - timedelta(seconds=30)).replace(microsecond=0).isoformat()
s, b = 0, {}
for seq in (1, 2, 3):
    s, b = post("/v1/events", {"events": [{
        "vehicle_id": resume_vehicle, "route_id": "smoke-r-resume",
        "lat": 13.06, "lon": 77.63, "speed_kph": 20.0, "sequence": seq, "event_ts": shared_ts,
    }]})
    if s != 202:
        break
check("three events sharing one event_ts were accepted", s == 202, f"{s} {b}")

ids = []
deadline = time.time() + 60
while time.time() < deadline:
    ok, out = psql(f"SELECT event_id FROM vehicle_positions WHERE vehicle_id = '{resume_vehicle}' "
                   f"ORDER BY event_ts, event_id")
    ids = out.split() if ok else []
    if len(ids) == 3:
        break
    time.sleep(2)
check("  all three reached the history", len(ids) == 3, f"{len(ids)} row(s)")

if len(ids) != 3:
    check("a reconnect resumes from the client's last event id", False, "no rows to resume from")
else:
    resume_id = ids[0]
    status, text, err = stream_read("/v1/stream", headers={"Last-Event-ID": resume_id},
                                    until=lambda t: '"frames":' in t, seconds=12)
    resumed_ok = status == 200 and '"frames":' in text
    check("a reconnect resumes from the client's last event id", resumed_ok,
          f"{status}, resumed" if resumed_ok else (err or "no resumed frame within 12s"))
    check("  exactly the events newer than the resume point arrive",
          all(i in text for i in ids[1:]), f"expected {ids[1:]}")
    check("  and the resume point itself is not replayed", resume_id not in text,
          "the client already has that event")
    # Not an exact count: any event applied after the resume point is one the client genuinely
    # missed too, so the replay legitimately includes it. The claims worth making are that the
    # same-second siblings arrived and the resume point itself did not.
    m = re.search(r'"frames":(\d+)', text)
    frames_n = int(m.group(1)) if m else 0
    check("  the resumed frame reports at least the two missed frames", frames_n >= 2,
          f"frames={frames_n}")

# The read a viewer uses on first load. Its unit test runs against a fake history, so only the
# deployed path executes the SQL — a LEFT JOIN and a coalesce that no fake ever runs.
if ids:
    s, body = req(f"/v1/vehicles/{resume_vehicle}", base=GATEWAY)
    check("GET /v1/vehicles/{id} answers from the deployed sink",
          s == 200 and resume_vehicle in json.dumps(body), f"{s} {json.dumps(body)[:90]}")

s, _ = req("/v1/vehicles/smoke-never-seen-" + uuid.uuid4().hex[:8], base=GATEWAY)
check("  an unknown vehicle is a 404, not a 500", s == 404, f"{s}")

print("\n=== 11. kill the consumer outright: nothing lost, nothing doubled (M6) ===")

# The harshest failure this system claims to survive. SIGKILL gives the process no chance to
# finish a batch, commit an offset, or drain: whatever it applied but did not commit is
# redelivered to its replacement, and the deduplication record is what makes that redelivery a
# no-op rather than a second application.
#
# The container has to be the victim. A Go test can cancel a context, but only the deployed
# stack can have its process killed the way an OOM killer or a cluster eviction kills it.
chaos_prefix = "chaos-" + uuid.uuid4().hex[:8]


def post_chaos(tag, n):
    """Post one event per synthetic vehicle, returning (status, body, vehicle ids)."""
    vehicles = [f"{chaos_prefix}-{tag}{i}" for i in range(n)]
    events = [{
        "vehicle_id": v, "route_id": "chaos-r1",
        "lat": 12.90 + i * 0.01, "lon": 77.50 + i * 0.01,
        "speed_kph": 30.0, "sequence": 1,
        "event_ts": datetime.now(timezone.utc).isoformat(),
    } for i, v in enumerate(vehicles)]
    s, b = post("/v1/events", {"events": events})
    return s, b, vehicles


def consumer_metric(name, label):
    """Read one labelled counter from the consumer's own metrics port, or None."""
    try:
        with urllib.request.urlopen("http://localhost:9091/metrics", timeout=10) as resp:
            text = resp.read().decode("utf-8", errors="replace")
    except Exception:
        return None
    for line in text.splitlines():
        if line.startswith("#") or not line.startswith(name) or label not in line:
            continue
        try:
            return float(line.rsplit(" ", 1)[1])
        except (IndexError, ValueError):
            return None
    return None


s, b, pre_vehicles = post_chaos("a", 4)
check("events were flowing before the kill", s == 202, f"{s} {b}")

# Suspend the restart policy first: see the note above. Restored before the container is started.
ok, out = docker(["update", "--restart=no", "esp-consumer"])
check("  the restart policy was suspended so the kill sticks", ok, out.strip()[:70])

ok, out = docker(["kill", "esp-consumer"])
check("  the consumer was killed with SIGKILL (no chance to drain or commit)", ok, out.strip()[:70])

# The window that matters. A consumer that merely restarts quickly proves nothing; what has to
# hold is that the log keeps accepting events with nothing consuming them, and that the group
# resumes from its committed offset rather than from the head when it returns.
s, b, during_vehicles = post_chaos("b", 3)
check("  events posted while nothing was consuming are accepted", s == 202, f"{s} {b}")

ok, out = docker(["update", "--restart=unless-stopped", "esp-consumer"])
check("  the restart policy was restored", ok, out.strip()[:70])
ok, out = docker(["start", "esp-consumer"])
check("  the consumer was started again", ok, out.strip()[:70])

chaos_vehicles = pre_vehicles + during_vehicles
expected = len(chaos_vehicles)

# Catch-up. Generous, because a cold consumer re-reads whatever the kill left uncommitted before
# it reaches the head of the log.
applied, deadline = 0, time.time() + 120
while time.time() < deadline:
    ok, out = psql("SELECT count(*) FROM vehicle_current WHERE vehicle_id LIKE '" + chaos_prefix + "%'")
    applied = int(out.strip()) if ok and out.strip().isdigit() else 0
    if applied == expected:
        break
    time.sleep(2)
check("  every event from both phases reached current state", applied == expected,
      f"{applied}/{expected}")

ok, out = psql("SELECT count(*) FROM vehicle_positions WHERE vehicle_id LIKE '" + chaos_prefix + "%'")
check("  the history holds exactly one row per event",
      ok and out.strip() == str(expected),
      f"{out.strip() if ok else '?'} row(s) for {expected} event(s): a redelivered event must "
      f"deduplicate, not append a second row")

ok, out = psql("SELECT count(*) FROM dead_letters WHERE encode(raw_payload,'escape') LIKE '%" + chaos_prefix + "%'")
check("  the restart dead-lettered nothing",
      ok and out.strip() == "0", f"{out.strip() if ok else '?'} dead letter(s)")

lag, deadline = None, time.time() + 90
while time.time() < deadline:
    lag = group_lag("sink-v1", "telemetry.raw.v1")
    if lag == 0:
        break
    time.sleep(2)
check("  the group caught up to zero lag after the restart", lag == 0, f"lag={lag}")

# Everything above holds whether or not a redelivery actually happened: a kill that lands
# between two batches satisfies "7 rows for 7 events" without the deduplication path being
# exercised once. The assertion worth making is the opposite one — force the redelivery and show
# the row count does not move — so the group is rewound to the start of the log and every record
# it has already applied is delivered a second time.
#
# The consumer is stopped across the seek, and that is a requirement rather than tidiness:
# franz-go holds each partition's position in memory, so rewinding the coordinator's committed
# offset underneath a running consumer changes nothing until it restarts.
# Captured before the rewind, so the assertion after it can be an equality rather than a
# non-emptiness test: "the count is still this number" is falsifiable, "the count is a number"
# is not. Nothing is posted between here and the re-read, so the count cannot legitimately move.
ok, processed_before = psql("SELECT count(*) FROM processed_events")
check("  the deduplication table count was read before the rewind",
      ok and processed_before.strip().isdigit(),
      f"{processed_before.strip() if ok else '?'} record(s)")

ok, out = docker(["stop", "esp-consumer"])
check("  the consumer was stopped so its in-memory offset cannot mask the rewind",
      ok, out.strip()[:70])

ok, out = docker(["exec", "esp-redpanda", "rpk", "group", "seek", "sink-v1",
                  "--to", "start", "--topics", "telemetry.raw.v1"])
check("  the group was rewound to the start of the log", ok, out.strip()[:80])

ok, out = docker(["start", "esp-consumer"])
check("  the consumer was started again, and must re-read everything it already applied",
      ok, out.strip()[:70])

lag, deadline = None, time.time() + 180
while time.time() < deadline:
    lag = group_lag("sink-v1", "telemetry.raw.v1")
    if lag == 0:
        break
    time.sleep(3)
check("  it re-read the whole log and caught up", lag == 0, f"lag={lag}")

ok, out = psql("SELECT count(*) FROM vehicle_positions WHERE vehicle_id LIKE '" + chaos_prefix + "%'")
check("  the history still holds exactly one row per event",
      ok and out.strip() == str(expected),
      f"{out.strip() if ok else '?'} row(s), unchanged — every re-read event deduplicated")

ok, out = psql("SELECT count(*) FROM processed_events")
check("  the deduplication table did not grow either",
      ok and out.strip() == processed_before.strip(),
      f"{out.strip() if ok else '?'} record(s), was {processed_before.strip()} — every re-read "
      f"event found its existing row instead of inserting a second")

# Read after the rewind, so the number describes a process instance that is still running. A
# counter from before the kill would be describing a process that no longer exists.
dups = consumer_metric("consumer_events_total", 'result="duplicate"')
check("  and the consumer counted the redeliveries it absorbed", (dups or 0) > 0,
      'consumer_events_total{result="duplicate"} = '
      + (f"{dups:.0f}" if dups is not None else "absent"))

print("\n=== 12. the deterministic producer drives the viewer end to end (M7) ===")

# Started explicitly, through its compose profile. The default stack stays quiet on purpose: the
# sections above assert on drained lag and unmoved row counts, and a producer running throughout
# would turn those into measurements of the simulator.
# The container is removed first rather than reused. Its logs already hold the summary from
# any earlier run, and a stale summary would satisfy the read below even if this run's producer
# never started — a vacuous pass, which is worse than a failure because it reads as evidence.
_ = docker(["compose", "--profile", "demo", "rm", "-sf", "simulate"], timeout=120)
ok, out = docker(["compose", "--profile", "demo", "up", "-d", "--build", "simulate"], timeout=300)
check("the simulator started under its compose profile", ok,
      out.strip().splitlines()[-1] if out else "")

# A viewer connected while the fleet is producing. These frames can only arrive by the live path
# — ingest, log, consumer, bus, gateway — because the fleet posts to the same endpoint a real
# producer would and nothing else in this run reads its output.
status, text, err = stream_read("/v1/stream",
                                until=lambda t: '"vehicle_id":"veh-' in t, seconds=40)
sim_vehicles = sorted(set(re.findall(r'"vehicle_id":"(veh-\d+)"', text)))
check("frames from the simulated fleet reach a connected viewer",
      status == 200 and len(sim_vehicles) > 0,
      f"{len(sim_vehicles)} distinct simulated vehicle(s) in the stream" if sim_vehicles
      else (err or "no simulated frame within 40s"))

# The producer's own summary is the other half of the evidence: it says whether the pipeline
# accepted what the fleet offered. Read after a clean stop, because the summary is printed on
# shutdown — a SIGKILL would lose exactly the number that makes this section evidence rather
# than an anecdote.
ok, out = docker(["compose", "stop", "simulate"], timeout=120)
check("  the producer stopped cleanly", ok, out.strip().splitlines()[-1] if out else "")

ok, out = docker(["logs", "--tail", "80", "esp-simulate"])
summary = {}
for candidate in out.splitlines():
    stripped = candidate.strip()
    # By shape, not by prefix: Go sorts map keys when it encodes, so the line begins with
    # whichever field sorts first and any prefix test would be pinned to a key ordering that is
    # an implementation detail of the encoder.
    if stripped.startswith("{") and '"duration_s"' in stripped and '"accepted"' in stripped:
        try:
            summary = json.loads(stripped)
        except ValueError:
            summary = {}
check("  it reported its own summary", bool(summary),
      f"accepted={summary.get('accepted')} offered={summary.get('offered')}" if summary
      else "no summary line in the container logs")
if summary:
    accepted = summary.get("accepted", 0)
    offered = summary.get("offered", 0)
    rejected = summary.get("rejected", -1)
    # accepted == offered, not merely accepted > 0, and this is the check that would have named
    # the contract mismatch immediately: a producer whose payloads are wrong can have every
    # event refused while its batches all succeed. The earlier "not one batch was refused" read
    # as a pass while 680 of 680 events were rejected, because `failed_batches` counts HTTP
    # failures — a check whose name described a property it never tested.
    check("  every offered event was accepted", accepted > 0 and accepted == offered,
          f"accepted={accepted} offered={offered} rejected={rejected}")
    check("  no event was rejected and no batch failed",
          rejected == 0 and summary.get("failed_batches", -1) == 0,
          f"rejected={rejected} failed_batches={summary.get('failed_batches')}")

# The durable end of the same chain: a simulated vehicle's state is in the sink, which can only
# happen if its events survived ingest, the log, and the consumer's transaction.
ok, out = psql("SELECT count(*) FROM vehicle_current WHERE vehicle_id LIKE 'veh-%'")
check("  simulated vehicles are materialised in the sink",
      ok and out.strip().isdigit() and int(out.strip()) > 0, f"{out.strip()} row(s)")

print("\n=== SUMMARY ===")


passed = sum(1 for _, ok, _ in results if ok)
print(f"  {passed}/{len(results)} checks passed")
for label, ok, detail in results:
    if not ok:
        print(f"  FAILED: {label}  — {detail}")
sys.exit(0 if passed == len(results) else 1)
