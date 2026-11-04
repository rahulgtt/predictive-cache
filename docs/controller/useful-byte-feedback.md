# Useful-byte feedback

`UsefulByteFeedback` is a standalone, bounded accounting component for #81.
It measures overlap between proven completed warming ranges and later independent
foreground reads. It does not prove cache retention, saved cloud work or latency
improvement. No producer, planner, dispatcher or live workload is started.

## Inputs and trust

Create one ledger per frozen experiment/version, declaring `experiment_id` and
`usefulness_s` (default 300 seconds). `observe_result(task, result, known_at=...)`
joins the existing validated `WarmerTask` and dispatcher `ResultRecord` by exact
run/plan/task identity and requested length. The caller must supply the trusted
original task; a result alone has no file identity or offset. Only a completed
result with matching exact completed Warmer terminal evidence contributes an
interval. Partial, failed, cancelled and lower-bound outcomes have separate status
and uncertain-requested counters; their byte totals never become guessed ranges.

`observe_read` accepts canonical `ReadDemandEvent` records. Only real, trusted,
complete-coverage foreground demand is eligible. Warmer, controlled synthetic,
unknown and untrusted records are excluded. Read rows must already be successful
READ projections; this module does not infer OPEN/CLOSE or application sessions.

`coverage` consumes authoritative observation intervals; `complete=True` must be
asserted only by the coverage owner after the interval closes. A read's own
coverage flag is insufficient to declare a full warming window observed.
`invalidate` consumes independently verified write/replacement evidence. These
APIs are internal trust boundaries, not new public event schemas or authenticated
producer implementations.

## Frozen accounting rules

- Each completed interval is `[offset, offset + length)`.
- Demand qualifies strictly after warm completion and through
  `completion + usefulness_s`, inclusive. Optional `use_until` can shorten that
  window. Execution deadlines alone are not interpreted as usefulness deadlines.
- Reports use only result, demand, coverage and invalidation evidence known by
  `as_of`. Report times cannot move backward.
- Within each run/volume/file, overlapping completed intervals form a unique-byte
  union. Overlapping demand and retries cannot count a byte twice. Different runs
  are separate experiment cohorts, not one physical global-byte denominator.
- `requested_bytes` and `completed_bytes_sum` retain task-attempt sums; the
  eligible-completed denominator is the unique union, which can be smaller.
- Observed qualifying overlap is useful. Unmatched bytes become unused only after
  the full window closes with continuous authoritative coverage. Gaps, explicit
  incomplete coverage, open windows and state loss make the remainder unknown.
- A write/replacement from execution start through usefulness-window end makes that entire window's target
  identity uncertain, conservatively withdrawing its usefulness evidence.
- A late read arriving after a report marks accounting incomplete. A later report
  may revise prior unused bytes to useful/unknown; earlier reports are snapshots,
  not immutable causal truth. Repeated immutable results/reads are idempotent;
  contradictory deliveries fail closed and make remaining negatives unknown.

For every reported run/volume:

```text
useful_bytes + unused_bytes + unknown_bytes = eligible_completed_unique_bytes
```

Uncertain requested bytes from noncompleted tasks are an upper-bound exposure
counter outside this denominator. They are not actual completed bytes.

## Bounds, reporting and limitations

Task, demand and coverage/invalidation evidence have explicit count caps.
`max_duration_s` (default one day) limits accepted receipt-time span. Overflow or
out-of-span arrivals are counted, refused and prevent further unused claims.
There is no silent eviction and no unlimited daemon history: callers retain one
bounded ledger per experiment and release it after exporting the report.
`complete_accounting` means no known ledger loss; it does not certify complete
observation coverage or zero unknown bytes. The ledger is single-consumer; its
owner must serialize updates and reports. Reports contain run/volume aggregates
and the caller's experiment identity, never
handles, target paths, file contents or per-file details. No file writer is added.

This implementation is fixture-tested plumbing. Trusted live foreground origin,
coverage finalization, result ingestion and an authorized controlled end-to-end
run remain prerequisites before live usefulness reporting. The existing
READ-only producer does not supply write/replacement invalidations, so live
identity continuity is not established by this module.

Run `tests/controller/test_feedback.py` only on `nas-vm` in the designated build
container, following repository instructions.
