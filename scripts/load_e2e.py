"""End-to-end latency against NFR-2: publish -> delivered to a subscriber, p95 <= 500 ms.

    python scripts/load_e2e.py                 # 1000 events over ~20s, then a summary
    python scripts/load_e2e.py --events 500 --rate 25

# What this measures, exactly

One number per event, from "the producer stamped it" to "a subscribed browser received it":

    ingest POST -> log -> consumer transaction -> Postgres -> fan-out bus -> gateway -> client

That path is the whole system, and it is why this is measured here rather than with k6: k6 can
time an HTTP response, but the response is a 202 that arrives *before* any of the work that
matters. The latency a user experiences is the time until the map moves, not until the POST
returns.

# What it does not measure

The clock is the client's own, and `produced_at` is stamped by this script on the same host as
the stack, so there is no skew to correct. A real deployment across hosts would need disciplined
clocks (NTP) for this measurement to mean anything across them — the honest statement is that
these numbers describe a single-host deployment, which is what `docker compose up` is.

# The submission shape, and the check that goes with it
#
# A submission is a flat observation, NOT the envelope in docs/DESIGN.md. The ingest service
# builds schema_version, event_type, produced_at and source itself — unknown fields are refused
# rather than ignored — because a client that could set them could claim a schema version the
# platform does not implement.
#
# This script got that wrong, and it is worth recording how the failure looked: every POST
# returned 202, all 1000 events were rejected inside those batches, and the script reported
# "1000 events sent" followed by "1000 of 1000 never reached the stream". It blamed the pipeline
# for a payload the server had refused, because it counted what it *sent* rather than what the
# server *accepted*. Every count below now comes from the response body.
#
# The pipeline is drained before the clock starts
#
# The first version did not, and the failure is worth recording because it looked like a pipeline
# defect: the 2,500/s NFR-1 run left the consumer ~50,000 events behind, this script sent its
# 1,000 events into that backlog, and after a fixed twenty-second wait it reported "1000 of 1000
# never reached the stream". They had all been applied — the sink held 1,000 rows and lag was 0
# moments later. The run had measured queue depth and reported it as a delivery failure, which is
# the same mistake as counting sends instead of acceptances: a measurement harness has to know
# which quantity it is actually observing.
#
# Why the events are sent one at a time and paced

A run that floods the pipeline measures queueing latency, not service latency, and would report a
p95 that says more about the offered rate than about the system. The default (50 events/s) is
deliberately far below the measured ingest ceiling so the number is a property of the path rather
than of an overloaded one.
"""

import argparse
import json
import re
import socket
import subprocess
import statistics
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone

INGEST = "http://localhost:8081"
GATEWAY = "http://localhost:8082"
NFR2_P95_MS = 500.0

# The consumer group whose lag gates a measurement, and the container to ask. The service names
# are the compose ones; this script is only ever run against the local stack.
CONSUMER_GROUP = "sink-v1"
REDPANDA_CONTAINER = "esp-redpanda"


def group_lag(group=CONSUMER_GROUP):
    """The consumer group's total lag, or None when it cannot be read.

    TOTAL-LAG is read as a single number from rpk's own summary line rather than by summing a
    column: the smoke script's first lag parser hardcoded a column index and silently summed
    log-end offsets instead, reporting a 10-event backlog for a fully drained group. A summary
    line printed by the tool cannot drift out from under the parser that way.
    """
    try:
        out = subprocess.run(
            ["docker", "exec", REDPANDA_CONTAINER, "rpk", "group", "describe", group],
            capture_output=True, text=True, timeout=45,
        ).stdout
    except Exception:
        return None
    m = re.search(r"^TOTAL-LAG\s+(-?\d+)", out, re.MULTILINE)
    return int(m.group(1)) if m else None


def wait_for_drained(timeout_s):
    """Block until the consumer group has caught up. Returns (lag, drained)."""
    deadline = time.time() + timeout_s
    lag = group_lag()
    announced = False
    while lag is None or lag > 0:
        if time.time() > deadline:
            return lag, False
        if not announced:
            print(f"the consumer is {lag} event(s) behind; waiting for it to drain first "
                  f"(<= {timeout_s:.0f}s)")
            announced = True
        time.sleep(5)
        lag = group_lag()
    if announced:
        print("  drained; the clock starts from a quiet pipeline")
    return lag, True


def post_event(event_id, vehicle_id, observed_at):
    """Post one observation, returning (status, accepted, rejections).

    The rejections are returned rather than discarded because a 202 says nothing about whether
    the events inside it were believed: one malformed report is named in `rejected` while its
    siblings are published, and a batch whose every event is refused still answers 202.
    """
    body = {
        "events": [{
            # Flat observation, not the envelope: the platform owns schema_version, event_type,
            # produced_at and source, and refuses fields it did not ask for.
            "event_id": event_id,
            "vehicle_id": vehicle_id,
            "route_id": "e2e-route",
            "lat": 12.95,
            "lon": 77.60,
            "speed_kph": 25.0,
            "bearing_deg": 45.0,
            "event_ts": observed_at,
            "sequence": 1,
        }],
    }
    raw = json.dumps(body).encode()
    req = urllib.request.Request(INGEST + "/v1/events", data=raw, method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=15) as resp:
        payload = resp.read()
        try:
            decoded = json.loads(payload)
        except ValueError:
            decoded = {}
        return resp.status, decoded.get("accepted", 0), decoded.get("rejected", []) or []


class StreamWatcher:
    """Reads one SSE stream and records arrival times by event id.

    A single goroutine owns the socket, which is a requirement rather than tidiness: an SSE
    response is a continuous byte stream, and two readers on one connection would each see a
    fragment of the frames. (The smoke test's first version of this pattern made the opposite
    mistake and its assertions saw empty strings.)
    """

    def __init__(self, url):
        self.url = url
        self.arrivals = {}          # event_id -> wall clock at arrival
        self.order = []             # arrival order of event ids
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.error = None
        self.saw_ready = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.thread.start()
        return self

    def _run(self):
        try:
            with urllib.request.urlopen(self.url, timeout=20) as resp:
                buffer = ""
                while not self.stop.is_set():
                    try:
                        chunk = resp.read(1)
                    except (socket.timeout, TimeoutError):
                        # Per-read timeout: an SSE response stays open by design, so a timeout
                        # means "nothing yet", not "this failed".
                        continue
                    except OSError:
                        # CPython poisons the file object behind a socket after a read timeout:
                        # every later read raises OSError("cannot read from timed out object")
                        # instead of waiting again. TimeoutError is itself an OSError, so this
                        # arm is reached only for that poisoned object — no further bytes are
                        # coming on this connection, and stopping beats reporting a stream
                        # failure this reader caused itself.
                        break
                    if not chunk:
                        break
                    buffer += chunk.decode("utf-8", errors="replace")
                    while "\n\n" in buffer:
                        frame, buffer = buffer.split("\n\n", 1)
                        self._frame(frame)
        except Exception as e:  # pragma: no cover - reported on the summary
            self.error = str(e)

    def _frame(self, frame):
        event_id = None
        name = None
        data = []
        for line in frame.splitlines():
            if line.startswith("id: "):
                event_id = line[4:].strip()
            elif line.startswith("event: "):
                name = line[7:].strip()
            elif line.startswith("data: "):
                data.append(line[6:])
        if name == "ready":
            self.saw_ready.set()
        if name != "position" or not event_id:
            return
        with self.lock:
            self.arrivals[event_id] = time.perf_counter()
            self.order.append(event_id)

    def close(self):
        self.stop.set()
        self.thread.join(timeout=5)


def percentile(values, pct):
    """The pct-th percentile, nearest-rank.

    Nearest-rank rather than an interpolated quantile: with a thousand samples the difference is
    fractions of a millisecond, and "the 950th of 1000 sorted latencies" is a sentence a reader
    can check against the raw data this script leaves behind.
    """
    if not values:
        return None
    ordered = sorted(values)
    k = max(0, min(len(ordered) - 1, int(round(pct / 100.0 * len(ordered))) - 1))
    return ordered[k]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=1000)
    ap.add_argument("--rate", type=float, default=50.0, help="events per second, paced")
    ap.add_argument("--out", default="load/e2e-results.json")
    ap.add_argument("--settle-timeout", type=float, default=300.0,
                    help="seconds to wait for the consumer to drain before measuring")
    ap.add_argument("--drain", type=float, default=90.0,
                    help="seconds to wait for the events to be delivered after the last send")
    args = ap.parse_args()

    # Drain first. This is the correction to the run that reported "1000 of 1000 never reached
    # the stream": it had sent its events into the ~50,000-event backlog the NFR-1 benchmark
    # left behind, then timed out after twenty seconds while the consumer was still working
    # through it. The events were applied moments later — the sink held all 1,000 rows and lag
    # was zero — so the run had measured queue depth and reported a delivery failure.
    lag_before, drained = wait_for_drained(args.settle_timeout)
    if not drained:
        print(f"FAIL: the consumer is {lag_before} event(s) behind and did not drain within "
              f"{args.settle_timeout:.0f}s; a latency measurement taken now would time the "
              f"backlog rather than the path", file=sys.stderr)
        return 1

    watcher = StreamWatcher(GATEWAY + "/v1/stream").start()
    if not watcher.saw_ready.wait(timeout=15):
        print("the gateway never sent its ready frame", file=sys.stderr)
        watcher.close()
        return 1

    print(f"stream open; sending {args.events} event(s) at ~{args.rate:g}/s ...")
    interval = 1.0 / args.rate
    sent = {}          # event_id -> perf_counter at send, for the events the server accepted
    http_seconds = []
    rejections = []    # first few reasons, so a contract mismatch names itself
    refused = 0
    next_send = time.perf_counter()

    for i in range(args.events):
        event_id = str(uuid.uuid4())
        vehicle_id = f"e2e-veh-{i % 20:03d}"
        produced_at = datetime.now(timezone.utc).isoformat()

        # Paced against a schedule, not by sleeping the interval after each send: a fixed sleep
        # accumulates the send's own duration, so the achieved rate drifts below the target and
        # the offered load becomes a property of the client's speed.
        now = time.perf_counter()
        if next_send > now:
            time.sleep(next_send - now)
        next_send += interval

        t0 = time.perf_counter()
        try:
            status, accepted, rejected = post_event(event_id, vehicle_id, produced_at)
        except urllib.error.HTTPError as e:
            print(f"  ingest refused an event: HTTP {e.code}", file=sys.stderr)
            refused += 1
            continue
        except Exception as e:
            print(f"  ingest unreachable: {e}", file=sys.stderr)
            break
        elapsed = time.perf_counter() - t0
        if status != 202:
            refused += 1
            continue
        if rejected:
            # Collected rather than printed per event: at a few hundred a second the reason is
            # the same every time, and the summary at the end is where a reader looks.
            refused += 1
            if len(rejections) < 3:
                rejections.append(rejected[0])
            continue
        if accepted != 1:
            # A 202 that accepted nothing: counted as refused so the totals cannot silently
            # disagree with what the server reported.
            refused += 1
            continue

        sent[event_id] = t0
        http_seconds.append(elapsed)

    # Drain. The last events need time to cross the pipeline; the deadline is bounded so a
    # stalled consumer fails this measurement instead of hanging it.
    deadline = time.time() + args.drain
    while time.time() < deadline:
        with watcher.lock:
            if len(watcher.arrivals) >= len(sent):
                break
        time.sleep(0.05)
    time.sleep(0.5)
    lag_after = group_lag()
    watcher.close()

    # Latency is measured from the send, and that choice includes the POST's own duration — a
    # few milliseconds — in every sample. The alternative (the envelope's produced_at, stamped
    # just before the POST) would exclude it, and the difference is smaller than the run-to-run
    # variation. Both are stated here rather than picked silently.
    latencies_ms = []
    with watcher.lock:
        for event_id, t0 in sent.items():
            arrival = watcher.arrivals.get(event_id)
            if arrival is not None:
                latencies_ms.append((arrival - t0) * 1000.0)

    missing = len(sent) - len(latencies_ms)
    result = {
        "events_refused_by_ingest": refused,
        "first_rejections": rejections,
        "consumer_lag_at_start": lag_before,
        "consumer_lag_at_end": lag_after,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scenario": {
            "events_sent": len(sent),
            "target_rate_per_s": args.rate,
            "ingest": INGEST,
            "gateway": GATEWAY,
            "nfr2_p95_ms": NFR2_P95_MS,
        },
        "delivered": len(latencies_ms),
        "undelivered": missing,
        "http_post_ms": {
            "p50": round(percentile(http_seconds, 50) * 1000, 2) if http_seconds else None,
            "p95": round(percentile(http_seconds, 95) * 1000, 2) if http_seconds else None,
        },
        "e2e_ms": {
            "p50": round(percentile(latencies_ms, 50), 2) if latencies_ms else None,
            "p95": round(percentile(latencies_ms, 95), 2) if latencies_ms else None,
            "p99": round(percentile(latencies_ms, 99), 2) if latencies_ms else None,
            "max": round(max(latencies_ms), 2) if latencies_ms else None,
            "mean": round(statistics.fmean(latencies_ms), 2) if latencies_ms else None,
        },
    }

    print(json.dumps(result, indent=2))
    try:
        with open(args.out, "w", encoding="utf-8", newline="\n") as f:
            json.dump(result, f, indent=2)
            f.write("\n")
        print(f"\nwrote {args.out}")
    except OSError as e:
        print(f"could not write {args.out}: {e}", file=sys.stderr)

    p95 = result["e2e_ms"]["p95"]
    if refused:
        # Named before the latency verdict, because a run whose payloads were refused has no
        # latency to report — and saying "p95 none" would point at the pipeline rather than at
        # the payload that never entered it.
        print(f"FAIL: the ingest service refused {refused} event(s); the submission shape is "
              f"wrong or the payload is invalid", file=sys.stderr)
        for r in rejections:
            print(f"      rejected: {json.dumps(r)}", file=sys.stderr)
        return 1
    if missing:
        # Describes what was observed rather than asserting what happened: the first version of
        # this message said the events "never reached the stream" while they were, in fact,
        # still queued and would be applied seconds later.
        print(f"FAIL: {missing} of {len(sent)} event(s) had not been delivered when the "
              f"{args.drain:.0f}s window closed (consumer lag then: {lag_after})",
              file=sys.stderr)
        return 1
    if p95 is None or p95 > NFR2_P95_MS:
        print(f"FAIL: p95 {p95} ms exceeds NFR-2's {NFR2_P95_MS} ms", file=sys.stderr)
        return 1
    print(f"PASS: p95 {p95} ms <= {NFR2_P95_MS} ms, {len(latencies_ms)} samples")
    return 0


if __name__ == "__main__":
    sys.exit(main())
