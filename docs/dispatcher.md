# Plan-file dispatcher

The MVP dispatcher is an offline-safe bridge between owner-controlled plan
files and the separately owned Cache Warmer. It is implemented under
`src/predictive_cache/dispatcher/` for story #63. This implementation does not
perform NFS operations itself and does not establish that warming is safe or
effective on a live cluster. The dispatcher is Linux-only because safe claims
require `renameat2(RENAME_NOREPLACE)` and child containment requires
`prctl(PR_SET_PDEATHSIG)`.

## File layout and trust boundary

The operator creates one dispatcher root owned by the dispatcher UID with mode
`0700`. The dispatcher holds a process lock in that root. Canonical durable
identity is `(run_id, plan_id, task_id)`, stored below
`runs/<run_id>/plans/<plan_id>/`. Each plan directory contains:

| Area | Contents |
| --- | --- |
| `state/` | Durable running, terminal, and ambiguous attempt state. |
| `tasks/` | Exact schema-v2 single-task files passed to the Warmer. |
| `outputs/` | Bounded per-attempt stdout JSONL and stderr evidence. |
| `results/` | Atomic terminal dispatcher results. |

The root also contains transport directories `plans/` and `claimed/`.
Durable `completed-plans/` markers prevent retained historical plans from
being reparsed on every poll.

Directories must be mode `0700`. Input and generated files are mode `0600`,
regular, single-link, owner-UID files. Reads use directory file descriptors
and `O_NOFOLLOW`; committed inputs are non-hidden `*.json` names excluding
`*.tmp.json`. Claimed plans, task files, state records, bounded output logs,
and results are retained for PoC evidence. The dispatcher never auto-prunes
them. Cleanup is an explicit, per-run operator action after the experiment
manifest is reviewed; it must be scoped and auditable.

The planner owns atomic publication: write and `fsync` a temporary file, then
rename it to the committed plan name. The dispatcher does not consume
temporary names.

Issue #112's focused
[`read-demand-v1-narrow-mvp` publisher](experiments/datagen-to-warmer-mvp.md)
implements that boundary for controlled synthetic integration. It verifies
snapshot/package/target-map identities, validates the completed schema-v2
READ plan before publication, and commits owner-only provenance and digest
indexes before making the plan visible. Its documented Dispatcher invocation
uses `--no-real-reads --once`; it does not authorize a live read.

## Schema-v2 plan contract

Plans reject missing or extra fields. The top-level keys are:

```text
schema_version, run_id, plan_id, model_version, created_at, deadline,
max_bytes, items
```

Ordered task items contain:

```text
task_id, volume_id, file_id, export_name, export_id, target_fh_b64,
target_valid_until, offset, length, max_bytes, score, reasons
```

Identifiers are 1-128 characters from `[A-Za-z0-9._:-]`; `model_version`
also permits the ML package contract's `@` separator. Timestamps are UTC
RFC3339 values ending in `Z`. File handles are canonical unpadded base64url
values decoding to 1-128 bytes. Tasks are capped at 16 MiB, and target validity
cannot exceed 300 seconds from plan creation. Because the dispatcher never
retries automatically, each task's `max_bytes` must equal its requested
`length`; another attempt requires a new explicit `task_id`.

An explicit retry may include optional `supersedes` with the original
`run_id`, `plan_id`, and `task_id`. The old tuple is never cleared, reused, or
overwritten; lineage is retained in the new task state.

The optional plan-item field `operation` may only be `"read"`; omission means
the same fixed read operation. It is not copied to the Warmer task because the
Warmer schema-v2 contract is read-only. Write or mutation operations are
skipped as unauthorized items.

Plan-level identity, version, duplicate-task, and integrity errors reject the
whole plan. An item with a valid unique `task_id` but a stale, expired,
malformed, unauthorized, or out-of-range target receives a precise terminal
`skipped` result; later valid items continue in original plan order.

The Warmer task file has exactly the fields agreed with the Warmer owner:

```text
schema_version, run_id, plan_id, task_id, volume_id, file_id, export_name,
export_id, target_fh_b64, offset, length, max_bytes, created_at, deadline,
target_valid_until
```

## Execution and recovery

Items run sequentially with one global subprocess. The configurable safety
defaults are 60 task starts and 64 MiB of requested bytes in the previous
rolling 60 seconds; they are controls, not performance targets. Starts are
charged durably and never refunded. If capacity is unavailable, the dispatcher
waits in plan order while reacting to stop, SIGTERM, enable-file removal, and
the task deadline. A task whose capacity cannot open in time is skipped with
`rate_limit_deadline`.

Before starting an item,
the dispatcher checks stop state, deadline, target validity, plan
requested-byte budget, and the persistent rolling one-minute byte/item limits.
Real reads require both `real_reads: true` and the explicit
`enable-real-reads` file. If either gate is absent, the fake reader records a
terminal `skipped` result with reason `dry_run`, requested bytes preserved,
actual bytes zero, and `bytes_read_is_lower_bound: false`; fake completion or
assumed bytes are never reported. A separate pressure-pause feature is deferred
beyond this MVP.

Before invoking the Warmer, the dispatcher durably reserves rate capacity and
writes running state with attempt, dispatcher, and process evidence. Every
state transition uses a temporary file, file `fsync`, atomic rename, and
directory `fsync`. A terminal result prevents rerun. On restart, running state
without a result becomes `failed` with reason `ambiguous_manual_review` and
`automatic_retry_allowed: false`; it is never retried automatically. The
operator may explicitly requeue after review. Stop files are preserved.

The trusted endpoint and AUTH identity are dispatcher-local configuration, not
plan data. Invocation is:

```text
pcache-warmer --host <numeric-ip> --port 2049 --task-file <path>
  --timeout-seconds <0.1..300> --uid 1002 --gid 1002
```

The Warmer executable path must have no symlink components and resolve to a
single-link regular executable owned by root or the dispatcher UID, with no
group/world write bits. The dispatcher opens and validates it once, then
executes that pinned inode through `/proc/self/fd`.

The dispatcher incrementally captures owner-only per-attempt JSONL and stderr.
Stdout is limited to 5,000 lines, 16 KiB per line, and 16 MiB total; stderr is
limited to 1 MiB. Every accepted stdout line is fsynced. Overflow, malformed
framing, or a partial final line terminates the process group and produces an
ambiguous failure once a read attempt may have been admitted.

Exactly one valid terminal JSON record must agree with the process exit code:
`completed` uses 0; `partial`, `error`, and `timeout` use 1; `cancelled` uses
130 or 143. Exit 2 without a terminal record is an invalid-arguments failure
only when accompanied by proven pre-validation evidence. Normal terminal
records use the Warmer's exact result shape and require exact requested bytes
for completion. Their `plan_ref` and `task_ref` values are the first 32
lowercase hexadecimal characters of SHA-256 over the ASCII plan and task IDs.

If task-file loading fails before any worker or NFS operation starts, the
Warmer may emit exactly one `pre_validation_result` containing status, a
sanitized reason, and `execution_started: false`, with no identity or byte
fields. Only a matching exit code and no prior COMPOUND/READ record allow the
dispatcher to synthesize canonical IDs and zero-byte accounting from its
trusted task. Missing, duplicate, malformed, mixed, or contradictory signals
are ambiguous failures. Dispatcher results use
`completed | partial | skipped | failed | cancelled` and include the reason,
actual byte accounting, lower-bound flag, and start/finish times.

## CLI

The operator must configure nonzero byte and item rates:

```bash
pcache-dispatcher run \
  --root /var/lib/predictive-cache/dispatcher \
  --warmer-command /usr/local/bin/pcache-warmer \
  --host 192.0.2.10 \
  --real-reads \
  --max-bytes-per-min 67108864 \
  --max-items-per-min 60
```

`--once` processes current work and exits. Without it, the dispatcher polls
until its stop file, SIGTERM, or SIGINT requests shutdown. Real reads remain
disabled unless the empty owner-only enable file exists.
