# Controlled Chimera READ bridge

This component connects raw `nfs4_read_v2` envelopes to canonical controlled READ
observations and the Controller's private execution-target map. It preserves the
producer's client-asserted provenance. It does not turn observed lab traffic into
trusted customer demand.

```text
Authenticated raw envelope -> bounded raw queue
                                   |
                               drain at tick
                                   |
                     strict ChimeraEvent validation
                         /                     \
             private TargetMap          successful controlled READs
             all valid origins          canonical READ projection
                         \                     /
                          existing runtime tick
                          source gate: memory_gap
                          warming plans suppressed
```

## HTTP wiring

The existing receiver defaults to normalized-v1 arrays. Raw mode is explicit:

```python
from predictive_cache.controller.chimera_reads import ChimeraReadAdapter, tick_chimera
from predictive_cache.controller.http import HttpSource
from predictive_cache.controller.sources import RealClock
from predictive_cache.controller.targets import TargetMap

clock = RealClock()
targets = TargetMap({7: "approved-lab-export"})
adapter = ChimeraReadAdapter(
    targets,
    allowed_volumes=[approved_volume_id],
    shippers=[registered_pod_uid],
    clock=clock,
)
source = HttpSource(
    ("127.0.0.1", 8090),
    token=token_from_environment,
    wire_format="nfs4_read_v2",
    on_reject=adapter.reject_input,
)
source.start(adapter.offer)
# The owner supplies the runtime with targets=targets and a dry-run Dispatcher.
# At each tick, use tick_chimera(adapter, runtime, clock.now()).
# Stop the HTTP source in the owner's finally block.
```

The raw request shape is [ML spec §8.1.1](../ml/spec.md).
The endpoint remains `POST /v1/events`, with bearer authentication, bounded bodies,
eight workers and absolute header/body deadlines. Raw responses include
`accepted`, `dropped` and `duplicate`. `202` acknowledges in-memory admission,
not durable storage, projection or completed warming.

HTTP callbacks validate the bounded envelope and queue rows. They do not invoke
the model, feature engine or NFS reader. Raw row validation and target updates
occur during `drain`. Malformed authenticated requests have sanitized responses
and aggregate loss accounting. Unauthenticated requests do not change adapter
state. A rejection-accounting callback failure returns `503` and sets the
receiver's `rejection_failed` flag. Source owners must treat that flag as a fault.

## Observation and identity rules

- Shippers and volumes must be allowlisted. Producer epochs, sequence ranges,
  synced watermarks and health fields are checked before admission.
- Controller receipt timestamps increase strictly. Drain includes observations
  with `known_at <= tick`; records arriving after a completed drain cannot be
  backdated into it.
- The raw queue is bounded by count and serialized bytes. Epoch, pending-sequence
  and retained duplicate-fingerprint state are also bounded. Gaps, losses,
  inconsistent counters and rejected records remain visible.
  The queue accepts at most 100,000 rows, matching the runtime's per-tick batch
  limit even when an operator increases the queue's byte budget.
- Every valid allowed target refreshes `TargetMap`, including failed reads,
  warmer reads and unknown traffic. These refreshes do not become demand.
- Only successful `controlled_demand` READs become canonical demand observations.
  They remain `origin=synthetic`, `origin_trust=client_asserted` and
  `data_kind=synthetic`. Untagged traffic does not become foreground demand.
- Target handles stay in private memory, private checkpoints or private plans.
  Export names come from the operator allowlist. A stale-result handler must
  invalidate the exact resolved target version, preserving newer refreshes.

## Required source gate

`adapter.source_gates` always contains `memory_gap` in this component. Observed
loss additionally sets `ingest_loss`. `tick_chimera` verifies that the runtime
uses the adapter's actual TargetMap and forwards those gates on every tick.
Do not replace this connection with an ungated generic replay tick.

This is deliberate: a clean heartbeat proves neither complete history nor
current-window coverage. The bridge emits no complete coverage observations.
It can exercise canonical admission and scoring with explicit fixture history,
but the raw-source connection must produce zero plans and zero dispatches.

There is an unresolved contract boundary: §8.1.1 certifies an hour only after
its end plus clock-skew allowance, while canonical window features require
coverage through the prediction tick. The current hour therefore cannot become
complete under that rule. Coverage/feature timing needs an agreed contract
before this path may enable warming. The adapter does not invent current-hour
completeness, backdate evidence or change the feature definitions to bypass it.

## Validation and remaining work

Tests use fictional handles and loopback HTTP. They cover raw transport,
canonical projection, target updates, origin exclusions, dedupe, sequence/epoch
loss, resource bounds, private state, and source gates with otherwise complete
fixture history. A separate explicit fixture test checks observed TargetMap
bindings through the planner and Dispatcher `dry_run`; that is not raw-source
coverage evidence.

Run tests in the designated CUFS/Chimera build container on `nas-vm`. Existing
normalized ingestion and READ replay remain separate modes.

No shipper is deployed by this component. Queue/dedupe continuity, complete
Controller checkpoints, restart reconciliation, live warming and matched
performance trials remain follow-up work. Restarting the adapter does not recover
acknowledged rows; its target-only state seam is not a Controller checkpoint.
