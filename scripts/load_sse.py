"""Concurrent SSE fan-out against NFR-3: >= 200 clients on one gateway, bounded memory.

    python scripts/load_sse.py                    # 200 clients for 30s
    python scripts/load_sse.py --clients 400 --duration 20

# What this measures, exactly

NFR-3 has two halves and it fails if either does: the gateway must *hold* 200 concurrent
streams, and it must do so without unbounded memory growth. So this reports both — streams
established and frames delivered per client, plus the gateway process's resident memory before
the clients connect, while they are connected, and after they leave.

# Why the harness is a single-threaded event loop

The first version used a Python thread per socket, and it produced an artifact that contradicted
itself: it reported `clients_registered: 200` while the gateway's own `gateway_clients` gauge read
49, and registration took 101 seconds. The gateway was not at fault — its disconnect counter
showed 218 `client_closed` and **zero** `slow_consumer` events, so it had shed nobody. The
instrument was the problem: 200 threads doing `read(1)` on one GIL starve each other, so
connections ramped up far slower than the script believed and the gauge sample landed mid-ramp.

200 sockets is I/O concurrency, not CPU concurrency, and asyncio is built for exactly this. One
event loop, 200 tasks, no GIL contention, and the whole thing finishes in seconds instead of
minutes.

# The check that keeps the artifact honest

Every count below comes from one of two places — the client's own view, or the gateway's metrics
— and the two are compared rather than assumed to agree. `clients_registered` must equal the
requested count **and** the gateway's gauge at the same moment **and** the number of clients that
received frames. A harness that reports its own belief while the system reports something else is
measuring itself.

# Why the producer has to be running

A stream with nothing to say is not a test of fan-out. Run this with the demo profile up
(`docker compose --profile demo up -d simulate`) so frames are flowing; the script says so and
fails fast if the stream stays silent, rather than reporting 200 healthy connections that
received nothing.
"""

import argparse
import asyncio
import json
import re
import subprocess
import sys
import time
import urllib.request

GATEWAY = "http://localhost:8082"
GATEWAY_CONTAINER = "esp-gateway"
NFR3_CLIENTS = 200

# How long a single read may block before it is treated as no-data-yet. The gateway heartbeats
# every 15 s, so this only needs to be comfortably above that.
READ_TIMEOUT_S = 45.0


def gateway_metric(name):
    """One gauge or counter value from the gateway's own metrics, or None."""
    try:
        with urllib.request.urlopen(GATEWAY + "/metrics", timeout=10) as resp:
            text = resp.read().decode("utf-8", errors="replace")
    except Exception:
        return None
    for line in text.splitlines():
        if line.startswith(name + " ") or line.startswith(name + "{"):
            try:
                return float(line.rsplit(" ", 1)[1])
            except (IndexError, ValueError):
                return None
    return None


def gateway_clients_gauge():
    return gateway_metric("gateway_clients")


def rss_mib(container=GATEWAY_CONTAINER):
    """Resident memory of the container process, in MiB, or None."""
    try:
        out = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", container],
            capture_output=True, text=True, timeout=30,
        ).stdout.strip()
    except Exception:
        return None
    m = re.match(r"([\d.]+)\s*([KMG]iB)", out)
    if not m:
        return None
    value, unit = float(m.group(1)), m.group(2)
    return round(value * {"KiB": 1 / 1024, "MiB": 1.0, "GiB": 1024.0}[unit], 1)


class Stats:
    """What one client observed. A plain object, so the event loop never blocks on a lock."""

    __slots__ = ("frames", "first_frame_s", "connected", "closed", "error")

    def __init__(self):
        self.frames = 0
        self.first_frame_s = None
        self.connected = False
        self.closed = False
        self.error = None


async def read_stream(host, port, index, duration, stats, ready_event, all_ready):
    """Open one SSE stream, read frames for `duration` seconds, and report what arrived."""
    started = time.perf_counter()
    writer = None
    try:
        reader, writer = await asyncio.open_connection(host, port)

        # A raw HTTP/1.1 request rather than a client library: the response never ends, and a
        # library that waits for content-length or EOF would wait forever.
        writer.write(
            b"GET /v1/stream HTTP/1.1\r\n"
            b"Host: " + host.encode() + b"\r\n"
            b"Accept: text/event-stream\r\n"
            b"Cache-Control: no-cache\r\n"
            b"Connection: keep-alive\r\n"
            b"\r\n"
        )
        await writer.drain()

        # Status line and headers, through the blank line that ends them.
        head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=30)
        if b"200" not in head.split(b"\r\n", 1)[0]:
            stats.error = "unexpected response line: " + head.split(b"\r\n", 1)[0].decode(
                "utf-8", errors="replace")
            return
        if b"text/event-stream" not in head.lower():
            stats.error = "response is not an event stream"
            return

        stats.connected = True
        ready_event.set()
        await all_ready.wait()

        buffer = b""
        while time.perf_counter() - started < duration:
            try:
                chunk = await asyncio.wait_for(reader.read(4096), timeout=READ_TIMEOUT_S)
            except asyncio.TimeoutError:
                # No bytes for READ_TIMEOUT_S; the gateway heartbeats every 15 s, so this means
                # something is wrong with the stream rather than that it is merely quiet.
                stats.error = f"no data for {READ_TIMEOUT_S:.0f}s"
                return
            if not chunk:
                return
            buffer += chunk
            while b"\n\n" in buffer:
                frame, buffer = buffer.split(b"\n\n", 1)
                text = frame.decode("utf-8", errors="replace")
                if "event: position" in text:
                    if stats.first_frame_s is None:
                        stats.first_frame_s = time.perf_counter() - started
                    stats.frames += 1
    except Exception as e:  # reported on the summary, never raised
        stats.error = f"{type(e).__name__}: {e}"
    finally:
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
        stats.closed = True


def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    k = max(0, min(len(ordered) - 1, int(round(pct / 100.0 * len(ordered))) - 1))
    return ordered[k]


async def drive(clients, host, port, duration, settle_s):
    """Start every client, wait until all are connected, then let them run."""
    stats = [Stats() for _ in range(clients)]
    ready = [asyncio.Event() for _ in range(clients)]
    all_ready = asyncio.Event()
    tasks = []
    for i in range(clients):
        ev = ready[i]

        async def runner(idx=i, event=ev):
            # Each task sets its own event; all_ready is set by the watcher below, so a slow
            # connection cannot hold the others' start.
            await read_stream(host, port, idx, duration, stats[idx], event, all_ready)

        tasks.append(asyncio.create_task(runner()))

    # Wait for every client to have its event loop-level event set, then release them together.
    started = time.perf_counter()
    deadline = started + settle_s
    while time.perf_counter() < deadline:
        if all(ev.is_set() for ev in ready):
            break
        await asyncio.sleep(0.05)

    connected = sum(1 for s in stats if s.connected)
    registration_s = time.perf_counter() - started

    # Sampled while the clients are connected — that is the number that matters, and reading it
    # after they leave would describe an idle gateway. Taken *after* every client has seen its
    # open frame, which is the moment the gateway has certainly registered them all.
    gauge_mid = gateway_clients_gauge()
    rss_mid = rss_mib()

    all_ready.set()
    await asyncio.gather(*tasks)

    return stats, connected, registration_s, gauge_mid, rss_mid


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--clients", type=int, default=NFR3_CLIENTS)
    ap.add_argument("--duration", type=float, default=30.0)
    ap.add_argument("--settle", type=float, default=20.0,
                    help="seconds allowed for every client to establish its stream")
    ap.add_argument("--out", default="load/sse-results.json")
    args = ap.parse_args()

    host, port = "localhost", 8082

    if gateway_clients_gauge() is None:
        print("the gateway's metrics are unreachable; is the stack up?", file=sys.stderr)
        return 1

    # Frames must already be flowing, or this measures sockets rather than fan-out.
    saw_frame = False
    try:
        with urllib.request.urlopen(GATEWAY + "/v1/stream", timeout=5) as probe:
            probe.read(64)
        import socket as _socket
        probe_deadline = time.time() + 20
        with urllib.request.urlopen(GATEWAY + "/v1/stream", timeout=25) as probe:
            buffer = ""
            while time.time() < probe_deadline and not saw_frame:
                try:
                    chunk = probe.read(1)
                except (TimeoutError, OSError):
                    break
                if not chunk:
                    break
                buffer += chunk.decode("utf-8", errors="replace")
                if "event: position" in buffer:
                    saw_frame = True
    except Exception:
        pass
    if not saw_frame:
        print("no position frames arrived within 20s: start the producer first\n"
              "    docker compose --profile demo up -d simulate", file=sys.stderr)
        return 1

    rss_before = rss_mib()
    gauge_before = gateway_clients_gauge()
    print(f"gateway RSS before {args.clients} clients: {rss_before} MiB "
          f"(clients gauge {gauge_before})")

    stats, connected, registration_s, gauge_mid, rss_mid = asyncio.run(
        drive(args.clients, host, port, args.duration, args.settle))

    time.sleep(2)  # let the gateway notice the disconnects
    gauge_after = gateway_clients_gauge()
    rss_after = rss_mib()

    frame_counts = [s.frames for s in stats]
    firsts = [s.first_frame_s for s in stats if s.first_frame_s is not None]
    errors = [s.error for s in stats if s.error]
    receiving = sum(1 for n in frame_counts if n > 0)

    print(f"{connected}/{args.clients} streams established in {registration_s:.2f}s")

    result = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "scenario": {
            "gateway": GATEWAY,
            "clients_requested": args.clients,
            "duration_s": args.duration,
            "nfr3_clients": NFR3_CLIENTS,
        },
        "clients_connected": connected,
        "registration_seconds": round(registration_s, 2),
        "clients_receiving_frames": receiving,
        "frames_per_client": {
            "total": sum(frame_counts),
            "min": min(frame_counts) if frame_counts else None,
            "p50": percentile(frame_counts, 50),
            "max": max(frame_counts) if frame_counts else None,
        },
        "time_to_first_frame_s": {
            "p50": round(percentile(firsts, 50), 3) if firsts else None,
            "p95": round(percentile(firsts, 95), 3) if firsts else None,
            "max": round(max(firsts), 3) if firsts else None,
        },
        "client_errors": len(errors),
        "first_error": errors[0] if errors else None,
        # The gateway's own view at the sampling moment, and the client's view at the end. They
        # are recorded side by side because a disagreement between them is the finding.
        "gateway_clients_gauge": {"before": gauge_before, "while_connected": gauge_mid,
                                  "after": gauge_after},
        "gateway_rss_mib": {"before": rss_before, "while_connected": rss_mid, "after": rss_after},
        "gateway_rss_growth_mib": (round(rss_mid - rss_before, 1)
                                   if (rss_mid is not None and rss_before is not None) else None),
    }

    print(json.dumps(result, indent=2))
    try:
        with open(args.out, "w", encoding="utf-8", newline="\n") as f:
            json.dump(result, f, indent=2)
            f.write("\n")
        print(f"\nwrote {args.out}")
    except OSError as e:
        print(f"could not write {args.out}: {e}", file=sys.stderr)

    # The three counts must agree, and the comparison is the point: a harness that reports its
    # own belief while the system reports something else is measuring itself. The first version
    # of this script claimed 200 registered while the gateway's gauge read 49.
    if connected < args.clients:
        print(f"FAIL: only {connected}/{args.clients} streams established", file=sys.stderr)
        return 1
    if receiving < args.clients:
        print(f"FAIL: {args.clients - receiving} client(s) received no frames", file=sys.stderr)
        return 1
    if gauge_mid is not None and int(gauge_mid) != args.clients:
        print(f"FAIL: the harness counted {connected} clients but the gateway reports "
              f"{gauge_mid:.0f} connected — the two views disagree, so neither is trustworthy",
              file=sys.stderr)
        return 1

    if args.clients >= NFR3_CLIENTS:
        print(f"PASS: {connected} concurrent streams, all receiving frames, gateway confirms "
              f"{gauge_mid:.0f} connected (NFR-3 asks >= {NFR3_CLIENTS}); "
              f"RSS +{result['gateway_rss_growth_mib']} MiB")
    else:
        print(f"note: {connected} streams held, below NFR-3's {NFR3_CLIENTS} — this run is a "
              f"smoke of the harness, not the requirement")
    return 0


if __name__ == "__main__":
    sys.exit(main())
