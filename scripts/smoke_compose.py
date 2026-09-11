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
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone

INGEST = "http://localhost:8081"

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

print("\n=== SUMMARY ===")
passed = sum(1 for _, ok, _ in results if ok)
print(f"  {passed}/{len(results)} checks passed")
for label, ok, detail in results:
    if not ok:
        print(f"  FAILED: {label}  — {detail}")
sys.exit(0 if passed == len(results) else 1)
