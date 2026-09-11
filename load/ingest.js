// load/ingest.js — ingest throughput and latency against NFR-1 (>= 2,000 events/s).
//
// What this measures, precisely: HTTP POST rate and latency at the ingest endpoint, and how many
// events the endpoint *accepted* (the 202 body carries the count). It does not measure the
// consumer, the sink, or fan-out — a run of this script that passes NFR-1 says the front door
// absorbed the offered load, and nothing more. The end-to-end number is measured separately
// (scripts/e2e_latency.py) because a request's latency and an event's journey are different
// quantities, and collapsing them into one number is how a benchmark starts lying.
//
// Run:
//   k6 run load/ingest.js                                   # defaults: 100 req/s x 25 = 2,500/s offered
//   REQ_RATE=80 BATCH=25 DURATION=60s k6 run load/ingest.js  # 2,000/s offered, longer window
//
// The offered rate is deliberately above the NFR. A run that targets exactly the threshold
// cannot tell you how much headroom exists, and would fail on threshold noise alone.

import http from 'k6/http';
import crypto from 'k6/crypto';
import { Counter } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:8081';
const BATCH = Number(__ENV.BATCH || 25);
const REQ_RATE = Number(__ENV.REQ_RATE || 100);
const DURATION = __ENV.DURATION || '40s';
const TARGET_EVENTS_S = Number(__ENV.TARGET_EVENTS_S || 2000);
const VEHICLES = Number(__ENV.VEHICLES || 40);

// Counted from the 202 responses, not from what was sent. "Offered" is a property of this
// script; "accepted" is what the system did, and only the second one belongs in a threshold.
const accepted = new Counter('events_accepted');
const rejected = new Counter('events_rejected');

export const options = {
  scenarios: {
    ingest: {
      executor: 'constant-arrival-rate',
      rate: REQ_RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: Math.max(20, Math.ceil(REQ_RATE / 2)),
      maxVUs: Math.max(200, REQ_RATE * 4),
      gracefulStop: '15s',
    },
  },
  thresholds: {
    // Non-2xx responses and transport errors. Above 1% means the offered rate is beyond what
    // this deployment absorbs, and the run should not be quoted as a pass.
    http_req_failed: ['rate<0.01'],
    // NFR-1 itself, as a threshold, so k6 exits non-zero when the pipeline did not sustain it.
    // A counter's `rate` is its per-second rate over the run.
    events_accepted: ['rate>=' + TARGET_EVENTS_S],
  },
};

// uuidv4, in the init context's spirit but callable per event. The server validates event_id as
// a UUID: it is the deduplication key, and an identifier that cannot be compared for equality
// cannot deduplicate. v4 is fine here — the sink's ordering key is event_ts, and the id's job is
// uniqueness, not ordering.
function uuidv4() {
  const bytes = new Uint8Array(crypto.randomBytes(16));
  const hex = [];
  for (let i = 0; i < 16; i++) {
    hex.push((bytes[i] + 0x100).toString(16).slice(1));
  }
  hex[6] = ((parseInt(hex[6], 16) & 0x0f) | 0x40).toString(16).padStart(2, '0');
  hex[8] = ((parseInt(hex[8], 16) & 0x3f) | 0x80).toString(16).padStart(2, '0');
  return (
    hex.slice(0, 4).join('') + '-' +
    hex.slice(4, 6).join('') + '-' +
    hex.slice(6, 8).join('') + '-' +
    hex.slice(8, 10).join('') + '-' +
    hex.slice(10).join('')
  );
}

export default function () {
  const nowIso = new Date().toISOString();
  const events = [];

  for (let i = 0; i < BATCH; i++) {
    events.push({
      // A submission is a flat observation, NOT the envelope. The ingest service builds
      // schema_version, event_type, produced_at and source itself, because a client that could
      // set them could claim a schema version the platform does not implement.
      //
      // This is the second time that shape caught me out: cmd/simulate sent the envelope too,
      // and the deployed smoke test found it as 680 rejections out of 680 events. Here it was
      // invisible until the benchmark ran — every batch returned 202, `http_req_failed` read
      // 0.00, and the threshold that caught it was `events_accepted`, which counts what the
      // server says it accepted rather than what this script says it sent. Put the threshold on
      // the number the system reports, not the number the load generator hopes for.
      event_id: uuidv4(),
      // A bounded set of vehicles, so events land on a bounded set of partitions and the
      // consumer's per-partition ordering has something to order. A fresh vehicle per event
      // would spread the load perfectly and measure nothing about keying.
      vehicle_id: 'k6-veh-' + String(i % VEHICLES).padStart(3, '0'),
      route_id: 'k6-route',
      lat: 12.9 + Math.random() * 0.2,
      lon: 77.5 + Math.random() * 0.2,
      speed_kph: 30.0,
      bearing_deg: 90.0,
      event_ts: nowIso,
      sequence: __ITER + 1,
    });
  }

  const res = http.post(BASE + '/v1/events', JSON.stringify({ events: events }), {
    headers: { 'Content-Type': 'application/json' },
    timeout: '30s',
    tags: { endpoint: 'ingest' },
  });

  if (res.status === 202) {
    const body = res.json();
    accepted.add(body.accepted || 0);
    rejected.add((body.rejected || []).length);
  }
  // A non-202 is counted by http_req_failed; adding it to `rejected` would conflate "the server
  // refused these events" with "the request never completed", which are different failures.
}

// handleSummary replaces k6's default output, so it prints the same object it writes — which
// keeps the console transcript and the committed artifact byte-identical.
export function handleSummary(data) {
  const m = data.metrics;
  const pick = (name) => (m[name] ? m[name].values : null);

  const thresholds = {};
  for (const name of Object.keys(m)) {
    if (m[name].thresholds) {
      thresholds[name] = m[name].thresholds;
    }
  }

  const out = {
    generated_at: new Date().toISOString(),
    scenario: {
      base_url: BASE,
      batch: BATCH,
      request_rate_target: REQ_RATE,
      duration: DURATION,
      offered_events_per_s: REQ_RATE * BATCH,
      vehicles: VEHICLES,
      nfr1_target_events_per_s: TARGET_EVENTS_S,
    },
    http_reqs: pick('http_reqs'),
    http_req_failed: pick('http_req_failed'),
    http_req_duration: pick('http_req_duration'),
    events_accepted: pick('events_accepted'),
    events_rejected: pick('events_rejected'),
    thresholds: thresholds,
  };

  const text = JSON.stringify(out, null, 2) + '\n';
  return {
    'load/ingest-results.json': text,
    stdout: text,
  };
}
