# Pass-2 runtime bridge

The Pass-2 bridge is a batch-only Controller command that turns a frozen
logical prediction schedule into short-lived schema-v2 plan files for the
existing Dispatcher. It does not ingest Chimera events, score candidates,
reload a model, invoke the Warmer, or authorize live NFS reads.

```text
frozen schedule v1
  + owner-only approved target map v1
  + explicit Pass-2 run start
        |
        v
pcache-controller run-pass2
        |
        v
schema-v2 plan files -> existing pcache-dispatcher
```

## Frozen schedule assumption

The schedule producer owns the complete schedule contract. The bridge expects
schema version 1 with `semantic_view`, `snapshot`, `package`, policy, logical
`(volume_id, file_id)` identities, scores, reasons, logical byte ranges, and
integer `warm_offset_s` values. Source/clean lineage is indirect through the
snapshot manifest; it is not duplicated as top-level schedule fields. The
bridge verifies the schedule's canonical self-hash, provenance hashes,
ordering, budgets, and the following timing rule:

```text
warm_offset_s =
    warm_start_offset_s + (rank - 1) * warm_spacing_s

due_at = pass2_run_start + warm_offset_s
```

`time_basis.score_quantization` is expected to be
`12-significant-digits`. The schedule must not contain paths, exports,
endpoints, or NFS handles.

The parser is aligned with the landed producer in
`predictive_cache.ml.read_demand_schedule`. Field additions or renamed
provenance members require an explicit contract update and focused producer
and consumer tests.

## Owner target map

The target map is a strict owner-controlled JSON file. Its parent directory
must be mode `0700`; the file must be a mode-`0600`, single-link regular file
owned by the runtime UID. It is bound to one `schedule_id` and contains:

```text
schema_version, target_map_id, authorization, schedule_id, created_at, targets
```

Authorization must be `approved-pass2-treatment`. Each target contains:

```text
approved, volume_id, file_id, export_name, export_id, target_fh_b64,
target_valid_until, offset, length
```

The bridge publishes the schedule's logical range only when it is wholly
inside the approved target range. It never shrinks, shifts, guesses, or
discovers a target. Missing, unapproved, expired, malformed, duplicate, or
out-of-range bindings produce no plan item.

## Timing and publication

The operator supplies an explicit UTC `--run-start`. A prediction becomes
publishable at `due_at - publish_lead_seconds` and becomes late after
`due_at` plus the configured clock-tolerance window. The scheduler wakes at
release, due, and local rate-reservation expiry boundaries so a coarse poll
cannot skip the only publishable instant. Publication uses the actual wall
time as plan `created_at`; the deadline is
`created_at + plan_ttl_seconds`. The target must remain valid through that
deadline.

Predictions sharing one `warm_offset_s` are published in one plan. Plan and
task IDs are deterministic. `plan.max_bytes` is exactly the sum of item
lengths, every item has `max_bytes == length`, and the completed object is
validated by `predictive_cache.dispatcher.schema.Plan` before publication.
Provenance and digest indexes are committed before the plan becomes visible.

The bridge requires a fresh dedicated Dispatcher root. The Dispatcher may
poll that root concurrently because the bridge uses `pass2-runtime.lock`,
not `dispatcher.lock`. Configure the same per-minute item and byte limits on
both commands. The bridge rejects the schedule before creating run state when
any rolling 60-second due window cannot fit those limits; it does not silently
complete an oversized treatment. The Dispatcher's durable start-time rate
state remains authoritative.

## Run

Create a fresh owner-only Dispatcher root and supply a freshly approved target
map. This command publishes only due plans:

```bash
pcache-controller run-pass2 \
  --schedule /approved/pass-1/schedules/schedule-....json \
  --target-map /approved/pass-2/target-map.json \
  --dispatcher-root /approved/pass-2/dispatcher \
  --run-id pass2-treatment-001 \
  --run-start 2026-11-04T14:00:00Z \
  --publish-lead-seconds 5 \
  --plan-ttl-seconds 120 \
  --max-bytes-per-min 67108864 \
  --max-items-per-min 60
```

Use `--once` for an external batch scheduler that invokes the command near
each release time. Without it, the process waits until all schedule items are
published or skipped, or until the Dispatcher root's owner-only `stop` file
appears.

Run the existing Dispatcher separately. `--no-real-reads` exercises the exact
claim/validation/result path without starting the Warmer:

```bash
pcache-dispatcher run \
  --root /approved/pass-2/dispatcher \
  --warmer-command /absolute/path/to/pcache-warmer \
  --host 192.0.2.10 \
  --no-real-reads \
  --max-bytes-per-min 67108864 \
  --max-items-per-min 60 \
  --once
```

## Evidence and limitations

State and audit files are owner-only under
`pass2-runs/<run_id>/`. Audit records contain logical identities and hashes,
not handles. Plan files necessarily contain the approved execution handle.

This is hackathon control-plane plumbing. Focused producer and consumer tests
cover the landed schedule shape, but no approved full Pass-1-to-Pass-2 run has
been recorded. The bridge does not prove that a target survives a server
restart, that NFS handle mode works live, that bytes remain cached, or that
warming helps foreground demand. No live collection or NFS read is part of
this path.
