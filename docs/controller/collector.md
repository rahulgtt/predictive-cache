# Controller event collector

Issue #59 implements the ingestion slice of ML spec sections 8.1-8.2:
HTTP/replay source, bounded queue, receipt-time stamping, shared cleaning,
bounded dedupe and handle reconstruction, and update delivery to a callback.
`pcache-controller run` runs ingestion only. It does not load a model, score,
plan, issue warming reads, or implement a FeatureStore or health/readiness API.
FeatureStore receiver integration remains deferred to #30.

## Current Chimera handoff

The implemented producer emits raw `nfs4_read_v2` READ records. The existing
[Chimera importer](../data-prep/chimera-v2-adapter.md) validates the collection
evidence and converts them to normalized event-schema-v1 tables. This
collector replays those normalized tables; its HTTP endpoint accepts normalized
v1 arrays, not raw Chimera v2 records. It does not replace the importer or
provide a live raw-event exporter/normalizer.

The verified collection described in the
[private dataset guide](../data-prep/owner-private-controlled-dataset.md)
contains 300 READ records with `handle_id=null` and `origin=synthetic`.
Replaying it should accept and process all 300, count 300 `origin_synthetic`
exclusions, and emit no demand updates. A completed ClearML `datagen:tiny`
artifact is a generated fixture; it is not proof that this original Chimera
collection has been published there. Keep the original collection and its
private location on the VM.

[Issue #104](https://github.com/rahulgtt/predictive-cache/issues/104) owns the
READ-demand semantic profile. READ observations must not become fabricated
OPEN/CLOSE or application-session facts. The legacy handle-tracker updates
in this slice remain generic offline-fixture compatibility, not a READ-only
live feature contract. Do not feed standalone foreground READs through that
legacy path as lifecycle demand. Real READ-only FeatureStore integration
awaits #104 and [#30](https://github.com/rahulgtt/predictive-cache/issues/30),
verified metadata availability, origin attribution and loss coverage.

The current replay check establishes normalization/ingestion and exclusion
behavior only. It does not establish model quality, cache benefit, foreground
safety or a continuously connected Chimera-to-Controller deployment.

## Run in the remote build container

Run builds and tests on `nas-vm` (the local `hackathon` SSH alias), in the
designated CUFS or Chimera build container, as required by the repository.
Use a separate validation copy and the existing Python 3.12 dependencies.
Use the registered `pcache-controller` entry point, or
`PYTHONPATH=src python -m predictive_cache.controller.cli`.
Create an ingestion configuration in that environment such as:

```yaml
clean:
  read_mode: ranges
controller:
  ingest:
    source: http
    listen: 127.0.0.1:8090
    queue_max: 200000
    allowed_volumes: [fixture-volume]
    http:
      max_body_mib: 4
      max_events: 10000
      max_concurrent: 8
      read_timeout_s: 2
      token_env: PCACHE_INGEST_TOKEN
```

Supply `PCACHE_INGEST_TOKEN` through a secret-backed environment variable
(for example a Kubernetes Secret). For an interactive check inside the remote container in bash:

```bash
read -r -s -p 'Collector token: ' PCACHE_INGEST_TOKEN
export PCACHE_INGEST_TOKEN
pcache-controller run --config collector.yaml
unset PCACHE_INGEST_TOKEN
```

Do not put the token in YAML, command-line arguments, committed examples or
request logs. The process requires a nonempty token and explicit volume
allowlist. Configuration accepts only this ingestion slice and `clean`;
unsupported Controller/model settings fail clearly with a sanitized error.
Bind to loopback for container validation. HTTP itself supplies no TLS; a deployment
needs an approved protected network or TLS termination and source authorization.

The protocol is `POST /v1/events`, `Content-Type: application/json`, one
unambiguous `Content-Length`, and `Authorization: Bearer <secret>` with a
JSON array of schema-v1 event objects. Chunked transfer is unsupported.

- `202 {"accepted": n, "dropped": m}` counts enqueue acceptance and immediate
  drops only. It never waits for cleaning or callback delivery. Accepted
  records may later be excluded as invalid, duplicate, nonforeground or failed.
- Missing/wrong credentials return `401` using constant-time token comparison.
  Unsupported route/method return `404`/`501`; unsupported content type `415`.
- Missing length returns `411`; ambiguous/invalid framing, malformed JSON,
  nonarray bodies, nonobject rows or short bodies return `400`. Excess body
  length or event count returns `413`. An incomplete body times out with `408`.
  Header timeout closes the connection. Disconnected clients receive no reply.
- At most eight workers handle connections, with no pending worker queue.
  Additional accepted connections receive immediate best-effort `503` and close.
  Absolute two-second deadlines bound each header/body read phase, including
  trickle clients. Limits may be lowered but cannot exceed these maxima.
- Wrong-volume, queue-overflow and lock-contention events are dropped and
  counted. The core also bounds each queued encoded event to 64 KiB. Neither
  request errors nor exception responses include raw events, paths or credentials.

The CLI drains/advances the consumer every 50 ms independently of requests.
Ctrl-C stops accepting connections, waits for bounded in-flight reads and
prints aggregate counters and update-kind counts. It prints no event rows,
identifiers, source paths, token values or exception payloads. `degraded`,
`tracker_failed`, `tracker_failure_reason` and `tracker_exhausted` expose
ingestion loss, malformed source lifecycles and fail-closed tracker capacity.
These are diagnostic flags, not production readiness assessments. There is
no live durable restart orchestration in this CLI. Core `state`/`restore`
are a versioned checkpoint seam for the later Controller state owner.

## Deterministic replay

Replace `controller.ingest` with:

```yaml
source: replay
queue_max: 200000
allowed_volumes: [fixture-volume]
replay:
  dataset: /approved/synthetic-dataset
  batch_size: 1000
```

Replay opens only manifest-listed, hash-verified `events/` metadata, never
`truth/` or the file paths described by events. A bounded temporary SQLite
spool sorts partitions by `(ingest_time, event_id)` and is removed on stop.
The synchronous CLI advances a zero-speed replay clock to each next receipt,
offers bounded batches, drains strictly earlier timestamps and finally
advances through handle timeout/grace. It includes arrivals beyond the
manifest's observation period. Same-timestamp rows remain pending across
batch boundaries to preserve the canonical update order. Replay uses source
`ingest_time` as `known_at`; live mode replaces it with strictly increasing
Controller receipt stamps.

Batch size greater than queue capacity can deliberately cause overflow.
Dedupe-window expiry, count-cap evictions, queue drops, tracker capacity and
consumer failures are explicit counters. Excluded origins never become demand.
Legacy lifecycle-fixture parity checks cover valid source lifecycles with
these exceptions absent. Those fixtures must assign a unique handle to each
lifecycle, with exactly one OPEN per lifecycle handle. A distinct repeated OPEN for an active or completed
handle is a lifecycle conflict. Core fails closed with `tracker_failed: true`
and `tracker_failure_reason: lifecycle_conflict`, clears pending tracker state
and suppresses further updates. An earlier emitted prefix cannot be retracted;
the real Controller must suppress plans whenever `tracker_failed` is true,
including after checkpoint restore. `tracker_exhausted` additionally identifies
capacity failures. No FeatureStore health claim follows from an emitted prefix
or HTTP acceptance. Generated `origin: foreground` rows remain synthetic evidence.

The producer must export asynchronously through its own bounded buffer,
outside foreground read completion, and drop on failure. Receiver bounds
alone do not prove foreground safety. End-to-end producer attribution,
authorization, loss accounting, real-history parity and deployment remain
separate gates.

## Validation

Run these in the isolated remote validation copy inside the designated build
container, never in the workstation checkout:

```bash
pcache-controller --help
PYTHONPATH=src python -m pytest tests/controller tests/prep tests/sources/chimera
ruff check src/predictive_cache/controller tests/controller tests/sources/chimera/test_importer.py
```

The importer-to-replay regression uses fictional authoritative collection
fixtures in JSONL and Parquet. It checks that normalized READ-only rows retain
null handles and synthetic origin, are all processed without queue loss, and
produce exclusions rather than lifecycle updates. The original 300-record
collection is an additional private replay check, not a committed test fixture.

HTTP tests use real loopback sockets and synthetic bodies. They cover auth,
malformed/oversized bodies, framing, queue-only replies, immediate capacity
rejection, absolute header/read deadlines, quiet logging and shutdown.
These tests require loopback socket access in the remote container. A denied
bind is an environment limitation, not evidence of a service failure.


### Verified compatibility on 2026-11-04

Validation ran in an isolated copy inside the designated Chimera build
container on `nas-vm`, against base `7b8334c` plus the collector integration.
The source collection stayed read-only, and its payload checksums matched
before and after the checks.

| Check | Result |
| --- | --- |
| Controller, shared cleaner and Chimera importer tests | 92 passed, 1 skipped |
| Installed `pcache-controller --help` and collection replay | Passed |
| Ruff over `src/` and `tests/` | Passed |
| Replay of the 300 normalized controlled READs | 300 accepted and processed; 300 synthetic exclusions; zero updates, drops or consumer errors |
| Loopback HTTP ingestion of those same normalized records | Same counts and exclusions; zero updates, drops or consumer errors |
| Fresh batch cleaning of the normalized collection copy | Same exclusions; zero retained demand rows |

The skipped test is the opt-in small-dataset truth-oracle acceptance check;
its separate `PCACHE_SMALL_INPUT` and `PCACHE_SMALL_CLEAN` inputs were not
provided. The JSONL/Parquet importer-to-replay regression passed.

These checks use already-collected metadata. They do not connect the running
Chimera producer to HTTP, validate a raw-v2 streaming adapter, apply updates
to a real FeatureStore, or establish READ-demand feature/model parity. Those
remain separate integration work under #34, #104 and #30. No new target-file
I/O, warming or deployment was performed by this validation.
