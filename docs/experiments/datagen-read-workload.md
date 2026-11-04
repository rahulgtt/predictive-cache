# Datagen-driven controlled READ workload

This capability prepares a deterministic, planted-pattern workload for a
controlled NFSv4.1 READ collection. The default candidate is three
hours; datagen deterministically selects the exact successful 4 KiB READ count
and variable schedule over the owner-approved 60-fixture catalog. Live
execution remains impossible without an immutable approved plan-bound permit
and every authorization gate below.

The workload uses the shared `read-demand-v1` semantic contract from
`contracts/read-demand-v1.json`. It emits READ intent only. Synthetic actor,
session, project and pattern labels are separate generator truth; they are not
claimed as Chimera observations.

> **Invariant:** Do not invent protocol lifecycle facts from temporal
> proximity when READ evidence cannot prove them. READ co-occurrence never
> creates OPEN, CLOSE, handle or session facts.

## Default candidate

| Property | Proposed value |
| --- | --- |
| Duration | 3 hours |
| Operations | 165 for the seed-42 default; frozen in manifest v3 |
| Average rate | 0.0152778 READ/s (one every 65.45 s on average), variable by window |
| Requested bytes | 675,840 bytes (165 × 4,096) |
| Concurrency | 1 |
| Protocol / operation | NFSv4.1 successful READ only |
| Correlation tag | Empty |
| Fixtures | 60 frozen allowlisted entries, each at least 65,536 bytes |
| Ranges | 4 KiB at offsets 0, 12,288, 24,576 and 36,864 and 49,152 |
| Popularity membership | 6 high, 18 medium, 36 low |
| Operation quotas | 99 high, 49 medium, 17 low for the seed-42 default |
| Range recurrence | 92 unique fixture/ranges and 73 repeated operations |
| Co-access truth | 4 planted three-file groups |
| Co-access sequences | 17 planted three-step sequences |
| Timing | Four non-hour-aligned burst windows and four quiet windows |
| Rate bounds | ≥20 s between offers; ≤3 per half-open 60 s; ≤25 per half-open 900 s |
| Repeat correlation | Identical file/range repeats are more than 150 s apart |

`duration_ms`, `operation_count`, the matching 60/30/10 quotas, seed and burst
positions are configurable. Accepted schedules must preserve the minimum
20-second gap, at most three operations in any half-open 60-second window, at
most 25 in any half-open 900-second window, and concurrency one. These values
are proposed workload controls, not evidence that a live profile is authorized
or safe.

## Artifacts and evidence boundaries

`WorkloadExecutionPlan` is truth-free. It contains unique per-occurrence
`intent_id` values, relative millisecond timing and allowlisted fixture/range
identity. A run-bound ledger ID is derived as:

```text
SHA256(run_id, execution_plan_sha256, intent_id)
```

`WorkloadTruth` is separate and keyed by `intent_id`. It contains synthetic
actor/session/project labels, popularity membership, planted co-access groups,
sequential range position and burst/quiet annotations. The Chimera importer
never reads this file. `workload-truth-binding.json` freezes its canonical hash
against the execution-plan and semantic-contract identities; initialization
and recovery reject altered truth while import/clean bytes remain truth-free.

The aggregate recovery report is not a dataset manifest. It reports only
planned/realized counts, popularity quotas, co-access transitions and immutable
lineage hashes. It cannot be passed to the cleaner, feature builder, snapshot
builder or trainer.

All imported rows remain `data_kind=synthetic` and `origin=synthetic`.
Production cleaning must retain zero demand rows and record
`origin_synthetic` exclusions. This path proves plumbing and planted-pattern
realization only; it is not representative history, model quality, cache
benefit or performance evidence.

## Offline workflow

Apply the issue #104 semantic-contract patch first. The plan command imports
`SEMANTIC_VIEW_ID` and `SEMANTIC_VIEW_SHA256` from
`predictive_cache.semantics.read_demand`.

```bash
pcache-collect plan-workload \
  --manifest-template tests/collection/fixtures/frozen-manifest.json \
  --catalog /owner-private/frozen-60-catalog.json \
  --run-id offline-datagen-read-v1 \
  --start-time 2026-11-04T00:00:00Z \
  --out /private/workload-bundle

pcache-collect init \
  --manifest /private/workload-bundle/manifest.json \
  --catalog /private/workload-bundle/fixture-catalog.json \
  --cluster-provenance contracts/cluster-provenance-v1.json \
  --event-schema contracts/nfs4-read-v2.schema.json \
  --budget-evidence tests/collection/fixtures/budget-evidence-v1.json \
  --execution-plan /private/workload-bundle/execution-plan.json \
  --workload-config /private/workload-bundle/workload-config.json \
  --workload-truth /private/workload-bundle/workload-truth.json \
  --workload-truth-binding \
    /private/workload-bundle/workload-truth-binding.json \
  --out /private/workload-run

pcache-collect run --run-dir /private/workload-run --dry-run

pcache-collect simulate-workload-source \
  --run-dir /private/workload-run \
  --out /private/workload-simulated-source

pcache-chimera-import \
  --event-sources /private/workload-simulated-source/event-sources.json \
  --observed /private/workload-simulated-source/observed.jsonl \
  --run-manifest /private/workload-run/manifest.json \
  --offered-ledger /private/workload-run/offered.jsonl \
  --fixture-catalog /private/workload-run/fixture-catalog.json \
  --checkpoints /private/workload-simulated-source/checkpoints.jsonl \
  --execution-plan /private/workload-run/execution-plan.json \
  --out /private/workload-imported \
  --format jsonl

pcache-prep clean \
  --input /private/workload-imported \
  --out /private/workload-cleaned \
  --format jsonl

pcache-collect recover-workload \
  --run-dir /private/workload-run \
  --imported /private/workload-imported \
  --cleaned /private/workload-cleaned \
  --out /private/workload-recovery.json
```

The simulated source is explicitly `offline_simulated_fixture`. It is not the
live provider and cannot satisfy a permit, attestation, reconciliation,
finalization or live-read gate.

## Plan-bound live profile

Manifest v3 uses a separate permit and never translates to manifest v2. The
permit binds the exact canonical bytes and semantic hashes for:

- run manifest, workload config, execution plan, workload truth, truth binding,
  fixture catalog and `read-demand-v1`;
- validated code head, merged commit and result tree;
- provider package/script, collector, validator, runbook, adapter wrapper,
  cleaner wrapper and interpreter;
- selected plan indexes, intent IDs, run-bound operation IDs, original
  plan-relative timestamps and the authorized execution schedule.

The sole truth-reading permit verifier deterministically chooses the first
relation-complete ordinary/repeated/sequential triple. For the seed-42
contract fixture this is indexes `3`, `43` and `50`: an ordinary first
occurrence, a later identical fixture/range, and the next occurrence of that
fixture at the next frozen offset. This does **not** prove a complete planted
three-file co-access triplet; that remains offline truth/parity and full-run
evidence.

One- and two-operation permits are wiring-only prefixes of that triple. Only
the exact three-operation relation-complete result can produce the bounded PASS
attestation required before a full collection. The default bounded execution
schedule is separately bound at relative offsets `0`, `170000` and `190000`
milliseconds. It preserves concurrency one, rolling rate caps and an
identical-range gap greater than the 120-second correlation window plus
30-second tolerance without waiting for the original 53-minute plan span.
Evidence records both original and bounded timestamps; bounded validation does
not claim full-schedule fidelity.

The harness checks proposed **actual** offer time against every prior actual
offer before the provider probe, offered-ledger append or READ. A late
operation never compresses the following gap or creates a catch-up burst;
minimum-gap or rolling-cap failure stops before I/O. The importer repeats the
same actual-cadence validation.

A full-collection permit contains the exact SHA-256 of the successful
three-role bounded attestation. Before provider setup or the first full-run
checkpoint/READ, the entrypoint validates that attestation's plan, truth
binding, catalog, semantic, code/tree and complete provider-stack lineage plus
its completion/expiry window. Missing, short, failed, stale or foreign bounded
evidence cannot start the three-hour run.

The provider receives the exact permit path on every v3 action. Its required
interface is:

```text
checkpoint RUN_DIR --permit-path PERMIT_PATH --scheduled-at UTC
probe RUN_DIR --permit-path PERMIT_PATH --operation-id ID
post-read RUN_DIR --permit-path PERMIT_PATH --operation-id ID
```

The real bounded adapter wrapper is still invoked with exactly two arguments:
`RUN_DIR PERMIT_PATH`. It must explicitly forward the permit and execution
plan, workload config, truth binding and harness-owned
`workload-evidence.jsonl` to `pcache-chimera-import`; permit auto-discovery is
forbidden.

V3-only harness evidence binds each plan index/intent/operation/permit and both
schedules to the provider sidecar, immutable raw chunk/line hashes and
canonical observed-row hash. Import, cleaning, reconciliation, provider
declaration, bounded attestation and finalization retain the same plan and
permit lineage. Workload truth is never an importer input or an observed
lifecycle/session fact.

## Live-read readiness checklist

The following gates are cumulative. None is satisfied merely by the offline
workflow.

- [ ] Issue #104 `read-demand-v1` contract and parity tests are merged first.
- [ ] Current cluster/environment state is re-discovered; historical Kind,
      provider and catalog observations are not reused as current health.
- [ ] The exact owner-private 60-fixture catalog and all manifest input hashes
      are approved for the new run.
- [x] Manifest v3 has a complete plan-bound live evidence chain:
      permit, provider declaration, reconciliation, bounded attestation,
      finalization and cleanup/retention handling.
- [ ] Provider, adapter and cleaner compatibility is reviewed against repeated
      empty-tag ranges and the new operation-ID derivation.
- [ ] Focused regressions remain green: bounded validation reconciles its
      permit prefix without weakening the full-run count; raw empty tag `""`
      maps to canonical observed `null`; producer sequences preserve a nonzero
      baseline as `B+1..B+N`; manifest-ordered multi-chunk accumulation is
      stable; the real adapter wrapper receives `RUN_DIR` plus `PERMIT_PATH`
      and forwards named evidence arguments.
- [ ] A new plan-bound one-to-three READ permit is approved. The historical
      300-operation permit is not reused.
- [ ] Bounded real validation passes read-only NFSv4.1, aligned `O_DIRECT`,
      checkpoint, pre-read barrier, post-read durability, zero-drop,
      identity, stop-file and 5,000 ms probe-plus-read gates.
- [ ] The proposed sustained schedule and rolling burst caps receive explicit
      workload/target-owner approval; bounded validation does not prove
      three-hour safety.
- [ ] Bounded reconciliation, import and clean pass with all controlled rows
      remaining synthetic and excluded from production demand.
- [ ] The frozen three-hour manifest, plan, truth, provider stack, catalog,
      start time, cleanup owner and stop rules receive explicit full-run
      approval.
- [ ] Only then may the three-hour live run start. A separate approval is
      required; preparing these artifacts is not authorization.

## Limitations

- The owner-private 60-fixture catalog is not committed.
- `dir_id` and `file_size` come from the controlled fixture catalog. A verified
  production metadata source and rename/replacement semantics remain open.
- Empty-tag matching is controlled inference bounded by range, time and order;
  it does not authenticate origin.
- Repository tests and offline fixtures do not authorize a permit, mount,
  bounded READ, or full collection. The provider package and owner-private
  catalog must be regenerated/reviewed for the approved run; historical v2
  artifacts remain immutable compatibility evidence only.
