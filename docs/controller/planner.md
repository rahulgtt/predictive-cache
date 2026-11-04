# READ planner and private file publisher

This standalone component chooses bounded prefix reads from already scored
READ candidates. It is not wired into a live Controller loop. Synthetic tests
exercise selection, private files and the existing dispatcher in dry-run mode;
they do not establish source trust, cache benefit or model quality.

## Input and output

`ScoredCandidate` contains `(volume_id, file_id)`, a finite score, seconds since
its last read, its known file size, and optional fixed candidate-source tags.
`select_plan` receives those candidates plus the verified active READ package's
`serving_policy`, an `is_eligible(volume_id, file_id)` callback, a structural
`TargetResolver`, cooldown timestamps and an explicit `plans_allowed` boolean.
The caller owns correct as-of timestamps, origin filtering, coverage, package
semantic compatibility and readiness. A `cold_open` policy is refused.

The resolver matches `TargetMap.resolve(volume_id, file_id, t)` and declares
`target_max_age_s`. Its result supplies the private handle, export binding,
last observed size and `seen_at`. Future or expired targets are skipped. If
both candidate and current target sizes are known, the shorter size bounds
the read. No paths, file contents or event payloads pass through the planner.

`Selection` contains the private schema-v2 plan, prediction dictionaries and
exact resolved target objects. Retain these exact objects when correlating a
later stale-handle result; replacing them by an identity could invalidate a
newer target. Bindings are private and must not enter logs or ClearML. The
object's representation omits them. Reasons are fixed gate names; arbitrary
explanation strings are not accepted. Candidate-source tags are a deliberately
small vocabulary, not an inferred upstream candidate contract.

## Selection rules and timers

Candidates are sorted by descending score, then file ID, then volume ID. Gates
run in order: eligibility, READ recency, cooldown, score threshold, known
nonempty size, executable target, and requested-byte/item budgets. An oversized
candidate does not stop the walk. Operational caps can only lower package caps.
Capacity and readiness can suppress publication after these selection gates.
Every selected file is included in prediction output, alongside the configured
top N candidates. Inputs are count bounded; nonfinite scores and duplicate
identities refuse the tick.

The prediction horizon remains the package's horizon, normally 300 seconds.
Each target expires at the earliest of `t + target_valid_s` (default 120),
`seen_at + target_max_age_s`, and the dispatcher's 300-second TTL cap. The
plan's execution deadline is the earlier of the horizon endpoint and every
selected target's expiry. Target validity is never extended to fit a forecast.
This is a Controller safety limit, not an NFS handle lifetime guarantee.

Each item reads offset zero and at most the package range cap. Its requested
byte allowance equals its length; the plan allowance equals their sum. There
are no retries. The existing dispatcher schema validates every output item;
its `InvalidPlanItem` records are explicitly refused before publication. Plans
exceeding the dispatcher's default 4 MiB JSON file limit refuse the tick before
any artifact is published.

## Durable publication and bounded state

`PlannerPublisher.publish` serializes calls under one lock. It selects against
its cooldown timestamps, reserves one pending-publication slot, atomically
writes complete prediction JSONL, then atomically publishes a nonempty plan.
It uses the dispatcher's secure filesystem helpers: owner-only 0700 directories,
0600 files, no symlink traversal and no overwrite. The supplied root must
already exist with those permissions. Plan IDs use a controller ID and a
monotonic sequence; use a distinct controller ID/root for separate runs.
Identifiers that become directories or plan filenames are checked with the
dispatcher's naming helpers before publication. Hidden or temporary plan names
and `.` / `..` directory names are refused.

Sequence and cooldown timestamps change only after successful publication.
Prediction and plan files are separate atomic writes, not a transaction. Any
publication exception or name collision latches the publisher as failed,
retains the one private pending slot and blocks subsequent publication. An
exception can occur after the plan rename, so the dispatcher might already
have claimed the plan. An operator must reconcile existing predictions,
plans, claimed plans and results; creating a fresh publisher is not recovery.

Cooldown history is bounded by `max_cooldowns` (default 100,000 identities).
The component never evicts history, including expired timestamps, because a
later model package can increase the cooldown interval. At capacity it skips
new identities with `cooldown_capacity`; existing identities can update their
slots. Size this bounded experimental runtime deliberately. Automatic pruning,
durable checkpoints, restart recovery and cross-process ownership remain
separate work. This module cannot support unattended restarts safely.

## Validation

Run `tests/controller/test_planner.py` in the designated build container on
`nas-vm`, along with dispatcher schema/serving-contract regressions. The tests
use invented identities and opaque synthetic handles. They include separate
300-second forecasting/120-second execution, earliest target expiry, budgets,
READ gates, cooldown capacity, privacy, sink collisions, publication failures
before and after rename, and a real dispatcher `skipped/dry_run` handoff with
zero actual bytes. Requested bytes describe the planned allowance; the dry run
creates no task and issues no NFS call.
