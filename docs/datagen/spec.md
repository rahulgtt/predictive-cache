# Synthetic access-event generator (`pcache-datagen`): spec v1

> **Status:** v1 planning spec for story
> [#11](https://github.com/rahulgtt/predictive-cache/issues/11). Schemas,
> patterns and defaults are expected to change as we learn more. Change them
> by PR to this file, never silently in code.

## 1. Purpose

The [decisions and handoff note](decisions-and-handoff.md) preserves the
conversation context, workload rationale and deferred integration questions.

There is no confirmed real access history yet (see [README](../../README.md)).
The ML lane still needs data now to build cleaning, features, snapshots,
models and the replay evaluator. `pcache-datagen` produces that data:

- **Access events** in the v1 event schema that a real CUFS/Chimera event
  source is expected to emit, so downstream code doesn't change when real
  data arrives.
- **Realistic, configurable access patterns with controlled randomness**:
  popularity skew, weekly schedules, co-access workflows, project lifecycles,
  bursts, batch jobs, new files and noise.
- **Data-quality faults** that real collection will have: duplicates, late
  events, clock skew, loss windows, warmer/synthetic/unknown origins, failed
  reads, invalid ranges and identity breaks.
- **Ground truth** for every planted pattern and fault, so models and
  cleaners can be tested for whether they recover what was planted.

> **Rule:** results computed on generated data are **plumbing and
> pattern-recovery evidence only**. Never report them as real-workload model
> quality or performance.

### 1.1 Non-goals

- Simulating the CUFS cache, cold/warm state, faults or latency. Those
  fields are emitted as `null` (§5.1). Cache simulation belongs to the replay
  simulator, a later story.
- Defining labels (what counts as a cold open), features or models.
- Matching any real workload's statistics. Calibrating against real data is
  a follow-up once real data exists.

### 1.2 Consumers (black-box contract)

Consumers depend **only** on the output layout (§3) and schemas (§5):

| Consumer | Reads |
| --- | --- |
| Event cleaner / ingestion | `events/`, `loss/`, `manifest.json` |
| Feature store, snapshot builder, models | Cleaned events, `catalog/` |
| Replay simulator / evaluator | Cleaned events, `catalog/` |
| Tests and recovery checks **only** | `truth/` |

`truth/` is never read by feature, model, planner or Controller code.

The additive [`read-demand-v1` projection](../data-prep/read-demand-v1.md)
does not change these generic schemas or default dataset hashes. The
`project-read-demand` command writes a separate READ-only projection and
separate generator truth outside the source dataset.
Reading it outside tests and evaluation reports is a leakage bug.

### 1.3 Planner objective neutrality

The planner objective is still open. Candidates include cold opens avoided,
bytes saved, or user wait time saved; the last favours large files, because
a cold huge file costs far more wall-clock time than a small one. The
generator therefore always emits `file_size`, per-read byte ranges and
per-handle totals. It also includes large files that are read in full, so
any of these objectives can be evaluated later on the same data.

### 1.4 Generation boundary

This story implements **event emission in Python**: it creates a fictional
world, schedules access intentions, and writes synthetic operation records.
It does not create the modeled files, execute their reads, watch local user
activity, or connect to CUFS. A 50 GiB catalog entry consumes metadata space,
not 50 GiB of disk. Only dataset output files are written.

An optional future workload executor could perform those intentions against
dedicated lab files and capture actual outcomes. That is outside #11–#19;
it must use distinct provenance and must not present local timing as CUFS
timing. All outputs here have `manifest.data_kind=synthetic`, including
events whose simulated `origin` is `foreground`. Origin describes the actor
inside the scenario, not whether the dataset is real.

## 2. Pipeline overview

```text
config.yaml + seed
      │
      ▼
build_world ──► World (volumes, calendars, projects, dirs, files, actors)  ──► catalog/
      │
      ▼
PatternGenerators (P1..P8) ──► Intents (who opens what, when, why)          ──► truth/patterns.jsonl
      │  merged in time order
      ▼
EventRealizer (sessions, read profiles, metadata churn P9) ──► clean events ──► truth/event_truth
      │
      ▼
FaultInjectors (dup, late, skew, loss, origins, failures, ...) ──► observed events ──► events/, loss/
      │
      ▼
writers + manifest + quality report ──► manifest.json, report/
```

Generation streams per volume and per week, so memory stays bounded on
large presets.

## 3. Output layout

```text
<out>/
  manifest.json                       # §5.5
  config.resolved.yaml                # fully-resolved config incl. seed and preset defaults
  catalog/
    volumes.parquet                   # §5.2
    directories.parquet
    files.parquet
    file_paths.parquet                # path history (renames)
    actors.parquet
  events/
    volume_id=<v>/date=<YYYY-MM-DD>/part-00000.parquet   # §5.1; date = UTC date of ingest_time
  loss/
    loss_counters.parquet             # §5.3
  truth/                              # TESTS/EVAL ONLY (§1.2)
    patterns.jsonl                    # §5.4
    event_truth.parquet
    dropped_events/volume_id=<v>/date=<YYYY-MM-DD>/part-00000.parquet
  report/
    quality.json                      # §13
    summary.md
```

- `--format jsonl` writes `.jsonl` files with the same names and
  partitioning. Use it for small fixtures checked into tests.
- Within each events partition, rows are sorted by `(ingest_time, event_id)`.
  This is **replay order**, i.e. the order a collector would receive events.
- Parquet: zstd compression, no dictionary-dependent nondeterminism, fixed
  row-group size, so output is byte-identical for a given seed (§4).

## 4. Conventions

| Topic | Convention |
| --- | --- |
| IDs | Opaque strings with type prefixes: `vol-`, `dir-`, `file-`, `act-`, `ses-`, `hdl-`, `ev-`, `pat-`, `int-`. Derived deterministically from the seed (e.g. hash of seed + counter), never random UUIDs. |
| Time | `timestamp[us, tz=UTC]` in Parquet; ISO-8601 with `Z` and microseconds in JSONL. Behaviour (work hours, schedules) is defined in the volume's local IANA timezone, including DST. |
| Units | Bytes as int64. Durations as int64 milliseconds (`*_ms`). |
| Enums | Lowercase snake_case strings. |
| Nullability | Every field is listed as required or nullable. Nullable means the value may be absent in **real** data too; consumers must handle `null`. |
| Versioning | `schema_version` on every event and `manifest_version` on the manifest. Breaking changes bump the version and update this spec. |
| Determinism | Same resolved config, seed, generator revision and locked dependencies give byte-identical files under `catalog/`, `events/`, `loss/` and `truth/`. Record runtime/dependency versions in the manifest. Use `numpy.random.SeedSequence` with fixed component/volume IDs, not enabled-component enumeration, so disabling a fault does not resample the world. Never use Python's process-randomized `hash()`. Cross-version binary identity is not promised. |
| Privacy | All names and paths are synthetic, built from a fixed word list. No real usernames, paths or contents. |

## 5. Schemas

### 5.1 Access event (`events/`, `truth/dropped_events/`)

One row per filesystem operation as observed by the event source.

| Field | Type | Req | Description |
| --- | --- | --- | --- |
| `schema_version` | int16 | yes | `1`. |
| `event_id` | string | yes | Unique per original event. A **duplicate** reuses the same `event_id`. |
| `event_time` | timestamp | yes | When the operation happened, by the client/node clock. May be skewed (§9). |
| `ingest_time` | timestamp | yes | Collector time: true operation time plus nonnegative delivery delay. Can precede the skewed `event_time`; clock skew does not move collector time. |
| `volume_id` | string | yes | Volume. |
| `file_id` | string | yes | Opaque file identity. Stable across rename (unless the identity-break fault is on). Replace (delete + create) yields a new `file_id`. |
| `dir_id` | string | yes | Parent directory at `event_time` (destination dir for `rename`). |
| `operation` | enum | yes | `open`, `read`, `close`, `create`, `write`, `rename`, `delete`. |
| `handle_id` | string | nullable | Links `open` → `read`* → `close` of one file handle. Null for `create`/`rename`/`delete`. |
| `offset` | int64 | nullable | Start of range for `read`/`write`. |
| `bytes_requested` | int64 | nullable | Requested length for `read`/`write`. |
| `bytes_read` | int64 | nullable | Bytes actually returned for `read` (≤ requested; less at EOF or on error). |
| `handle_bytes_read` | int64 | nullable | On `close` only: total bytes read through the handle. |
| `handle_duration_ms` | int64 | nullable | On `close` only: open → close duration. |
| `file_size` | int64 | yes | File size at `event_time` (after the op for `write`/`create`). |
| `result` | enum | yes | `ok`, `enoent`, `eacces`, `eio`, `einval`. |
| `origin` | enum | nullable | `foreground` (real client demand, including batch jobs), `warmer` (Cache Warmer reads), `synthetic` (benchmark/probe traffic **in the simulated system**; unrelated to this dataset being generated), `unknown`. `null` is treated as `unknown`. |
| `session_id` | string | nullable | Actor session (§8.1). Null when `emit.session_id: false` (real sources may lack it). |
| `cohort_id` | string | nullable | Opaque team/cohort of the actor. Null when `emit.cohort_id: false`. |
| `client_id` | string | nullable | Opaque client/actor pseudonym. |
| `app` | string | nullable | Application class: `office`, `cad`, `viewer`, `media`, `batch-indexer`, `backup`, `warmer`, `probe`. |
| `cache_outcome` | enum | nullable | Reserved: `hit`, `miss`, `partial`. **Always null in v1** (no cache model). |
| `latency_ms` | int64 | nullable | Reserved. **Always null in v1.** |

Invariants for clean (pre-fault) data:

- Each successful `open` has exactly one `close` with the same `handle_id`,
  later in true time. Failed opens (`enoent`/`eacces`) have no reads or close.
- With `read_events: ranges`, a handle's `close.handle_bytes_read` equals the
  sum of its `read.bytes_read`. With `none`, totals come from the same
  internal read plan, but individual reads are deliberately not emitted;
  validate totals against that plan in realizer tests, not an empty read sum.
- `0 ≤ offset` and `offset + bytes_read ≤ file_size`.
- No operations on a `file_id` after its `delete` except failed `open`
  (`enoent`).

### 5.2 Catalog (`catalog/`)

The world as built (§6). This is ground-truth metadata that a real system
could also provide, so consumers may read it.

Catalogs include future history for replay. At a feature cutoff `t`, consumers
must exclude files/directories created after `t`, ignore future deletion and
replacement fields, and resolve paths using `valid_from <= t < valid_to`
(null end means unbounded). Future interval endpoints are not features.
These catalogs assume metadata is observable at its effective time; real
sources need an availability-time policy before using this assumption.

**`volumes`**: `volume_id`, `timezone` (IANA), `work_days` (list of ISO
weekday ints), `work_start_local` / `work_end_local` (`HH:MM`), `holidays`
(list of dates), `period_start`, `period_end`.

**`directories`**: `dir_id`, `volume_id`, `parent_dir_id` (null at root),
`project_id` (null for non-project dirs), `depth`, `created_at`.

**`files`**: `file_id`, `volume_id`, `kind` (§6.3), `size_at_creation`,
`created_at` (may precede the period), `deleted_at` (nullable),
`replaced_by_file_id` (nullable), `project_id` (nullable).

**`file_paths`**: `file_id`, `dir_id`, `path` (synthetic,
e.g. `/proj-amber-falcon/drawings/rev-3/plan-0042.dwg`), `valid_from`,
`valid_to` (nullable). Rename history; paths are for the Warmer side, never
features.

**`actors`**: `actor_id`, `volume_id`, `kind` (`user` / `service`),
`cohort_id`, `client_id`, `project_ids` (list), `schedule_start_local`,
`schedule_end_local`, `work_days`.

### 5.3 Loss counters (`loss/loss_counters.parquet`)

Simulate the counters a real best-effort event hook would export. One row
per volume per UTC hour.

| Field | Type | Description |
| --- | --- | --- |
| `volume_id` | string | |
| `hour_start` | timestamp | UTC hour bucket by original **true** operation time, before clock skew. |
| `offered` | int64 | Events produced by the source, before loss/sampling, excluding duplicates. |
| `emitted` | int64 | Events delivered (unique `event_id`s). |
| `dropped` | int64 | Lost events (loss windows). |
| `sampled_out` | int64 | Removed by sampling. |
| `duplicates` | int64 | Extra duplicate deliveries. |
| `late` | int64 | Unique delivered events whose first delivery is more than `faults.late.threshold_s` after true operation time. |

Invariant: `offered = emitted + dropped + sampled_out`.

Loss windows take precedence over sampling: an event gets exactly one of
`emitted`, `dropped`, `sampled_out`. Duplicates are extra deliveries of emitted
events only. Both removed categories are retained in `truth/dropped_events/`
with their respective truth tags. Observed row count equals `emitted +
duplicates`. Known gaps include nonzero loss windows and sampling intervals.

### 5.4 Ground truth (`truth/`), tests and eval only

**`patterns.jsonl`**: one JSON object per planted pattern instance.

```json
{
  "pattern_id": "pat-000123",
  "kind": "weekly_schedule",
  "volume_id": "vol-0001",
  "active_from": "2026-01-05T00:00:00Z",
  "active_to": null,
  "actors": ["act-0007", "act-0011"],
  "files": ["file-000451", "file-000452"],
  "params": {"weekday": 1, "local_time": "09:00", "jitter_min": 10, "attendance": 0.85}
}
```

The `params` per kind are listed in §7. Workflow chains add
`"edges": [{"from": "file-a", "to": "file-b", "p": 0.7, "delay_median_s": 120}]`.

**`event_truth.parquet`**: one row per event in `events/` **and** in
`dropped_events/`.

| Field | Description |
| --- | --- |
| `event_id` | Joins to events. Duplicates share the row and add `duplicate` to `faults`. |
| `intent_id` | The intent that produced the event (null for metadata churn). |
| `cause` | `pattern_id` of the generating pattern, or `background`, `noise`, `churn`, `warmer_sim`, `probe_sim`. |
| `faults` | List of applied faults: `duplicate`, `late`, `skew`, `dropped`, `sampled_out`, `origin_unknown`, `failed_result`, `invalid_range`, `identity_break`. Empty when clean. |
| `true_event_time` | Event time before clock skew. |
| `true_file_id` | `file_id` before identity break. |
| `original_fields_json` | Canonical JSON object of original values for fields mutated by faults, including nulls; `{}` when none. Capture each original value once, before the first mutation. Tests use it to restore results, ranges, origin and timestamps; never expose it to consumers. |

There is one truth row per **unique** event ID, not per duplicate delivery.
For duplicate deliveries, restoration uses the first delivery, then applies
`original_fields_json`. Added warmer/probe events have their own event IDs
and causes. Store aggregate rates and denominators in the quality report;
truth tags alone must not be used as production demand labels.

### 5.5 Manifest (`manifest.json`)

```json
{
  "manifest_version": 1,
  "dataset_id": "synth-small-s42-1a2b3c4d",
  "data_kind": "synthetic",
  "generator": {"name": "pcache-datagen", "version": "0.1.0", "git_sha": "abc1234"},
  "event_schema_version": 1,
  "seed": 42,
  "preset": "small",
  "config_sha256": "…",
  "period": {"start": "2026-01-05T00:00:00Z", "end": "2026-03-02T00:00:00Z"},
  "volumes": ["vol-0001"],
  "counts": {
    "events": 0, "dropped_events": 0, "files": 0, "actors": 0, "patterns": 0,
    "by_operation": {}, "by_origin": {}, "by_fault": {}
  },
  "known_gaps": [
    {"volume_id": "vol-0001", "start": "…", "end": "…", "reason": "loss_window", "loss_fraction": 0.8}
  ],
  "files": [{"path": "events/volume_id=vol-0001/date=2026-01-05/part-00000.parquet", "rows": 0, "sha256": "…"}],
  "created_at": "…"
}
```

`dataset_id` = `synth-<preset|custom>-s<seed>-<first 8 hex of config_sha256>`.
Downstream manifests (cleaned data, snapshots, models) reference it.

## 6. World model

### 6.1 Volumes and calendars

- Each volume has an IANA `timezone`, work days (default Mon–Fri), work
  hours (default 08:00–18:00 local) and a holiday list. Holidays reduce user
  activity to `calendar.holiday_activity` (default 0.05) and skip
  schedules.
- The `medium` preset uses volumes in different timezones, and at least one
  period that spans a DST change.

### 6.2 Projects and directories

- A **project** is a top-level directory with a lifecycle:
  `ramp` → `active` → `cooldown` → `archived`. Each phase has a
  duration in weeks (sampled from config ranges) and an activity multiplier
  (default 0.3 → 1.0 → 0.3 → 0.02, linearly interpolated during ramps).
- Some projects start before `period.start` (already active or cooling).
  New ones start mid-period at `projects.new_per_month`.
- Each project has a subdirectory tree: depth 1–`projects.max_depth`
  (default 3), with branching ~ Poisson(`projects.branching_mean`, default 3).
- Each volume also has a few shared non-project dirs (`/shared/templates`,
  `/shared/standards`), with long-lived, moderately popular files.

### 6.3 Files

Files are assigned a **kind**. The kind defines the size distribution and
the default read profile (§8.2). Defaults:

| Kind | Share | Size (log-normal, median; clip range) | Default read profile |
| --- | --- | --- | --- |
| `doc` | 35% | 400 KiB; 4 KiB–50 MiB | `full` |
| `drawing` | 15% | 20 MiB; 100 KiB–500 MiB | `full` |
| `cad_model` | 8% | 150 MiB; 5 MiB–4 GiB | `full` |
| `image` | 12% | 4 MiB; 50 KiB–200 MiB | `full` |
| `media` | 5% | 300 MiB; 10 MiB–20 GiB | `head` (p=0.6) else `full` |
| `dataset` | 5% | 1 GiB; 50 MiB–50 GiB | `ranges` |
| `small` | 20% | 8 KiB; 100 B–256 KiB | `full` |

- Files per directory ~ Poisson(`files.per_dir_mean`, default 25). Total
  file count is controlled by the preset.
- Each file has a `created_at`. Most predate the period; new files come
  from P7.

### 6.4 Actors

- **Users** belong to **teams** (`cohort_id`). Each team is assigned 1–3
  projects; users may also touch `/shared`.
- Each user has a personal schedule: start ~ N(work_start, 30 min), end
  ~ N(work_end, 45 min), and work days. Vacations are 1–2 week blocks with
  probability `actors.vacation_weeks_per_quarter`.
- **Service actors** (`kind=service`) run P6 batch jobs.
- Each user has a dominant `app` mix by file kind, e.g. `cad` for
  drawings/models and `office` for docs.

## 7. Demand patterns

Each pattern is a `PatternGenerator` (§11). It emits **intents** ("actor X
opens file F at time t because of pattern P") and truth records. All
randomness flows from its own seeded stream. The global
`randomness` knobs (§10) scale jitter and noise everywhere.

| ID | Kind | What it models | Key params (defaults) | Predictable by |
| --- | --- | --- | --- | --- |
| P1 | `popularity` | Everyday background work: each active user opens files in their projects at a rate following a diurnal curve (morning ramp, lunch dip, afternoon taper). The file is chosen by Zipf popularity within the project, with a recency boost for recently created/written files. | `rate_per_user_hour: 6`, `zipf_s: 1.1`, `recency_boost_half_life_days: 3`, diurnal curve points | Frequency, recency, time-of-day |
| P2 | `weekly_schedule` | Recurring meetings and reviews: a set of 1–20 files opened at a fixed local weekday/time by 1–N team members, e.g. "Monday 09:00 model + drawings". Includes daily variants (every workday 09:15). Schedules can start or stop mid-period. | `instances_per_team: 2`, `jitter_min: 10`, `attendance: 0.85`, `files_per_set: [1,20]` | Time-of-week |
| P3 | `workflow_chain` | Co-access workflows: after opening entry file A, the actor continues along a Markov chain (A→B→C, with branching) with log-normal delays. Some edges **drift** (change target) mid-period. | `chains_per_project: 3`, `length_geom_p: 0.35`, `delay_median_s: 120`, `delay_sigma: 1.0`, `continue_p: 0.7`, `drift_share: 0.2` | Co-access graph |
| P4 | `project_lifecycle` | Scales P1/P2/P3 intensity per project over weeks (§6.2). Emits truth only, no direct intents. | see §6.2 | Decayed frequency, drift handling |
| P5 | `burst` | One-off, non-recurring events: a **review day** (team opens most of a directory within 1–3 h) or a **directory scan** (one actor reads heads of every file in a subtree). Randomly placed. | `bursts_per_volume_month: 3`, `scan_share: 0.3` | Unscheduled onset; shared demand after onset |
| P6 | `batch_job` | Nightly/weekly service jobs (indexer, backup verify) reading file heads over a subtree at a local time. `origin=foreground` (real demand), `app=batch-indexer`/`backup`. | `jobs_per_volume: 2`, `local_time: "02:00"`, `head_bytes: 1 MiB` | Time-of-week (and app filter) |
| P7 | `new_file` | Files created during the period: `create` + `write`s, then a burst of opens by the team decaying over days. This is the cold-start case. | `new_files_per_project_week: 5`, `open_decay_half_life_days: 2` | Directory activity priors |
| P8 | `noise` | Uniformly random opens of any file, including archived projects; the unpredictable long tail. | share of all foreground opens: `randomness.noise_share: 0.05` | Nothing |
| P9 | `churn` | Metadata ops: `write` (size change), `rename` (within/between dirs), `delete`, **replace** (delete + create at the same path, new `file_id`). Implemented in the realizer (§8.3). | `writes_per_file_month: 0.5`, `rename_rate: 0.01/file/month`, `delete_rate: 0.005`, `replace_rate: 0.005` | n/a |

Interactions:

- P1/P2/P3 intensities are multiplied by the project's P4 activity and
  suppressed by holidays and actor vacations.
- P3 chains can be triggered by any P1/P2 open of an entry file, so
  schedules can start chains ("Monday model → related drawings").
- All patterns skip files that are deleted at the intended time, except an
  `enoent` rate controlled by §8.3 (stale opens).

### 7.1 Workload controls and acceptance mapping

These controls extend existing patterns. Values are scenario assumptions,
not calibrated human statistics. Include them in resolved config and pattern
truth. Probabilities are in [0, 1]; counts are positive; time spans are bounded.

| Configuration key (default) | Behavior | Owner / deterministic check |
| --- | --- | --- |
| `patterns.popularity.working_set_size: 20`, `working_set_share: 0.7`, `revisit_share: 0.2`, `revisit_history: 10` | Each actor has a stable subset of live files in assigned projects. For each P1 selection, first choose a prior file with revisit probability; otherwise choose the personal subset with working-set probability, else the project population. Use Zipf weights within the selected pool. Empty pools fall back to live project files; empty projects emit no intent. | #14 preserves actor/project interfaces; #16 implements personal working sets and revisits. Controlled fixtures in that issue verify set membership and history selection. |
| `patterns.workflow_chain.context_order: 1` (1 or 2), `max_steps: 20` | Order 1 uses current file; order 2 uses (previous, current) within the same workflow run. Missing order-2 transitions fall back to order 1. `continue_p` controls continuation; stopping represents task abandonment. Bound loops by max_steps. | #16: A→B→C and D→B→E remain distinguishable with interleaved actors. |
| `patterns.workflow_chain.interruption_p: 0.05` | Before each continuation, optionally insert one unrelated live project-file open, then resume saved chain context. If no unrelated file exists, skip the interruption. Inserted opens do not trigger another chain. | #16: probability 0 inserts nothing; probability 1 inserts one interruption per continuation when possible. |
| `patterns.burst.participation: 0.7`, `shared_file_share: 0.6`, `arrival_spread_s: 3600`, `duration_s: 10800` | Review burst: independently select team participants. Partition the selected directory's live files into a common subset and a remainder. Each participant reads the common subset plus an independently sampled half of the remainder. Starts are uniform within arrival_spread_s; reads are uniform over the remaining time to the burst's duration_s deadline, then sorted. Scans retain their single-actor behavior. | #16: controlled fixtures force multiple participants and verify shared and actor-specific reads; zero participation emits none. |
| `realize.interactive_think_time_max_s: 300`, `service_think_time_max_s: 0`, `read_mbps_sigma: 0.5` | Per-handle lognormal throughput and bounded think time provide different speeds. Profiles still determine full/head/range reads. Throughput only schedules synthetic times. | #17: profile bounds and handle totals hold at timing extremes. |

Cap working-set and burst subset sizes to available files. Round subset
counts down; share=1 selects all. Empty burst subsets are valid and reported.
Require arrival_spread_s <= duration_s. Order-2 transition truth includes
`previous` alongside `from`, `to`, `p`; order-1 edges use null `previous`.
Private working sets and transition tables never enter model-input events.

P5 onset is not predictable from a weekly schedule. Once it begins, growing
demand across independent sessions **can** predict continued demand. Separate
unexpected onset from within-burst sharing in recovery reports.

Controls support neutral values: working_set_share=0, revisit_share=0,
context_order=1, interruption_p=0, shared_file_share=0. The isolated P1 Zipf
check disables working-set/revisit and recency boosts so it tests the specified
distribution. Mixed-workload reports describe the resulting distribution
without requiring the original exponent to survive these transformations.
Holdout scenarios may vary seeds, overlap, noise and drift; downstream training
still uses chronological splits and excludes truth and future catalog history.

## 8. From intents to events

### 8.1 Sessions

Intents of one actor within one volume are grouped into sessions: a new `session_id` starts
after an inactivity gap > `sessions.gap_min` (default 10). Service actors
get one session per job run. Session IDs are emitted only if
`emit.session_id` is true.

Sessionization uses true intent time before skew/delivery faults. A cohort
is a team, not a session. Globally adjacent events from different actors do
not form workflow edges. Session/handle state persists across weekly windows.
Private workflow/job run IDs distinguish concurrent runs; they are internal
context, not additional event-schema columns.

### 8.2 Read profiles

| Profile | Behaviour |
| --- | --- |
| `full` | Sequential read of the whole file, offset 0 → `file_size`. |
| `head` | Sequential read of the first `U(1, 8) MiB` (or `head_bytes` for P6). |
| `ranges` | 1–20 random ranges, each `U(64 KiB, 64 MiB)`, within the file. |

- Reads are emitted as **coalesced range events**, each ≤
  `realize.max_read_event_bytes` (default 16 MiB). A 1 GiB full read is
  64 `read` events.
- Read timing follows a per-handle throughput ~ log-normal(median
  `realize.read_mbps_median`, default 200 MB/s). It only spaces event times
  and implies nothing about cold/warm latency.
- `close` follows the last read after `U(0, think_time_max_s)`, default up
  to `realize.interactive_think_time_max_s` for interactive apps and
  `realize.service_think_time_max_s` for service actors.
- `realize.read_events: none` emits only `open` + `close` (with handle
  totals). This lets us test whether per-range events are needed.

### 8.3 Results and churn

- Opening a deleted file (stale reference) → `open` with `result=enoent` and
  no reads/close, at `realize.stale_open_rate` (default 0.002) of opens of
  recently deleted files.
- `eacces` on open at `realize.eacces_rate` (default 0.001).
- P9 churn events are interleaved in time order. `write` changes
  `file_size` for later events and updates "recently written" for P1's
  recency boost. `rename` updates `file_paths` and `dir_id`.

For v1, defer writes, deletes and replacements until active handles on that
file close. Record the effective time in catalog history. This keeps range
and size invariants deterministic without modeling filesystem concurrency;
it is a simplification, not a claim about CUFS behavior.

## 9. Data-quality fault injection

Faults are applied **after** realization, in the order below. Each one is
independently configurable and recorded in `truth/event_truth.faults`. With
all faults disabled, `events/` equals the clean stream.

Origin-generated warmer/probe traffic is realized into complete handles first,
then passes through the remaining faults. Arrival times are based on true
operation time, not the skewed client clock. Late delays are capped at 6 h;
duplicate delivery uses a positive delay capped at 6 h. Fault mutations retain
original values in truth (§5.4). Failure injection deliberately may make an
observed close total disagree with mutated reads; validate the restored clean
stream and report the observed discrepancy as an expected injected fault.

| Fault | Default | Effect | Downstream must |
| --- | --- | --- | --- |
| `warmer_origin` | on | A naive simulated warmer: each night, for each volume, reads `head` of the top-N (default 200) most-opened files of the last 7 days, with `origin=warmer`, `app=warmer`. These correlate with future demand **on purpose**. | Exclude from features and labels. Leakage tests should fail if they don't. |
| `synthetic_origin` | on | Benchmark probes: a few files read every hour with `origin=synthetic`, `app=probe`. | Exclude. |
| `unknown_origin` | 0.2% | `origin` set to `unknown` or `null` on foreground events. | Exclude and count. |
| `failed_result` | 0.3% of reads | `result=eio`, `bytes_read=0`. | Ignore as demand. |
| `invalid_range` | 0.05% of reads | `offset > file_size` or `bytes_read > bytes_requested`; `result=einval`. | Reject and count. |
| `identity_break` | **off** | On rename, emit later events with a new `file_id` (as if the source can't keep identity). | Measure fragmentation. |
| `clock_skew` | on | Per-client offset ~ N(0, 2 s), plus 1% of clients with a ±5 min offset. Shifts `event_time` only. | Tolerate reordering. |
| `loss_window` | 2 per volume per month | Window of 1–6 h in which 30–100% of events are dropped. Dropped events are moved to `truth/dropped_events/`, counted in `loss/`, and listed in `manifest.known_gaps`. | Mask the window as unknown, not "no demand". |
| `sampling` | off | Uniform sampling rate per volume. | Scale or mask. |
| `late` | 1% | `ingest_time = true_event_time + LogNormal(median 5 min)`, tail to ~6 h. | Handle late arrival. |
| `duplicate` | 0.5% | Re-deliver the same event (same `event_id`) with a later `ingest_time`. | Deduplicate by `event_id`. |

## 10. Configuration

### 10.1 Shape

YAML, validated by a pydantic model. Every key has a default; a preset
supplies defaults and the file overrides them.

```yaml
preset: small            # tiny | small | medium | custom
seed: 42
format: parquet          # parquet | jsonl
period:
  start: "2026-01-05"    # Monday; local midnight in each volume's tz
  weeks: 8
volumes:
  - timezone: "America/New_York"
    holidays: ["2026-01-19", "2026-02-16"]
    work_days: [1, 2, 3, 4, 5]
    work_hours: ["08:00", "18:00"]
projects:
  initial: 20
  new_per_month: 2
  max_depth: 3
  branching_mean: 3
  lifecycle_weeks: {ramp: [1, 3], active: [4, 16], cooldown: [2, 6]}
files:
  target_count: 5000
  per_dir_mean: 25
  kinds: {doc: 0.35, drawing: 0.15, cad_model: 0.08, image: 0.12, media: 0.05, dataset: 0.05, small: 0.20}
actors:
  users: 50
  teams: 6
  service: 2
  vacation_weeks_per_quarter: 0.5
patterns:
  popularity: {rate_per_user_hour: 6, zipf_s: 1.1, recency_boost_half_life_days: 3, working_set_size: 20, working_set_share: 0.7, revisit_share: 0.2, revisit_history: 10}
  weekly_schedule: {instances_per_team: 2, jitter_min: 10, attendance: 0.85}
  workflow_chain: {chains_per_project: 3, delay_median_s: 120, continue_p: 0.7, drift_share: 0.2, context_order: 1, max_steps: 20, interruption_p: 0.05}
  burst: {bursts_per_volume_month: 3, scan_share: 0.3, participation: 0.7, shared_file_share: 0.6, arrival_spread_s: 3600, duration_s: 10800}
  batch_job: {jobs_per_volume: 2, local_time: "02:00"}
  new_file: {new_files_per_project_week: 5, open_decay_half_life_days: 2}
  churn: {writes_per_file_month: 0.5, rename_rate: 0.01, delete_rate: 0.005, replace_rate: 0.005}
randomness:
  noise_share: 0.05      # P8 share of foreground opens
  jitter_scale: 1.0      # multiplies all timing jitters
  rate_scale: 1.0        # multiplies all event rates
realize:
  read_events: ranges    # ranges | none
  max_read_event_bytes: 16777216
  read_mbps_median: 200
  read_mbps_sigma: 0.5
  interactive_think_time_max_s: 300
  service_think_time_max_s: 0
sessions: {gap_min: 10}
emit: {session_id: true, cohort_id: true, client_id: true}
faults:
  warmer_origin: {enabled: true, top_n: 200, local_time: "01:00"}
  synthetic_origin: {enabled: true}
  unknown_origin: {rate: 0.002}
  failed_result: {rate: 0.003}
  invalid_range: {rate: 0.0005}
  identity_break: {enabled: false}
  clock_skew: {enabled: true, sigma_s: 2, outlier_share: 0.01, outlier_s: 300}
  loss_window: {per_volume_month: 2, hours: [1, 6], loss_fraction: [0.3, 1.0]}
  sampling: {rate: 1.0}
  late: {rate: 0.01, median_s: 300, threshold_s: 60}
  duplicate: {rate: 0.005}
```

### 10.2 Every pattern and fault can be disabled

`patterns.<name>.enabled: false` and `faults.<name>.enabled: false` (or a
rate of 0) must work. Model tests use this to build single-pattern datasets,
e.g. "only P2 + P8".

### 10.3 Presets (targets, tune in #19)

| Preset | Volumes | Files | Users | Weeks | Approx. events | Runtime target | Use |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `tiny` | 1 | ~300 | 10 | 2 | 10k–50k | < 10 s | Manual smoke tests, JSONL fixtures |
| `small` | 1 | ~5k | 50 | 8 | 1–3 M | < 2 min | Dev loop, recovery checks |
| `medium` | 3 (different tz, one DST) | ~50k | 300 | 12 | 10–30 M | < 15 min | Scale/perf of downstream code |

## 11. Internal interfaces (for parallel work)

Sub-issues code against these. They're defined in the foundation task (#13)
and may be refined there, with this section updated in the same PR.

### 11.1 Types

```python
# src/predictive_cache/datagen/model.py  (sketch; frozen dataclasses or pydantic)
class World:
    volumes: list[Volume]                  # incl. tz, calendar
    projects: list[Project]                # incl. lifecycle -> activity(t) -> float
    directories: list[Directory]
    files: list[File]                      # incl. kind, size_at(t), deleted_at
    actors: list[Actor]                    # incl. schedule, is_working(t)
    def activity(self, project_id: str, t: datetime) -> float: ...
    def is_holiday(self, volume_id: str, t: datetime) -> bool: ...

@dataclass(frozen=True)
class Intent:
    intent_id: str
    at: datetime                           # UTC, true time
    volume_id: str
    actor_id: str
    file_id: str
    app: str
    read_profile: Literal["full", "head", "ranges"]
    head_bytes: int | None
    cause: str                             # pattern_id | "background" | "noise"
    workflow_run_id: str | None = None      # private context, never event columns
    job_run_id: str | None = None           # one service session per run
    operation: Literal["open", "create", "write", "rename", "delete", "replace"] = "open"

@dataclass(frozen=True)
class PatternRecord:                       # one line of truth/patterns.jsonl (§5.4)
    pattern_id: str; kind: str; volume_id: str
    active_from: datetime; active_to: datetime | None
    actors: list[str]; files: list[str]; params: dict
```

Events are Arrow record batches (or dicts) matching §5.1. Truth rows match §5.4.

### 11.2 Protocols and pipeline

```python
class PatternGenerator(Protocol):
    kind: ClassVar[str]
    def plan(self, world: World, cfg: Config, rng: np.random.Generator) -> list[PatternRecord]: ...
    def intents(self, world: World, window: tuple[datetime, datetime],
                rng: np.random.Generator) -> Iterator[Intent]: ...    # time-ordered within window

def build_world(cfg: Config, rng: np.random.Generator) -> World: ...

class EventRealizer(Protocol):
    def realize(self, world: World, intents: Iterator[Intent], window: tuple[datetime, datetime],
                rng: np.random.Generator) -> Iterator[tuple[EventRow, EventTruthRow]]: ...  # includes P9 churn

class FaultInjector(Protocol):
    name: ClassVar[str]
    def apply(self, events: Iterator[tuple[EventRow, EventTruthRow]], world: World,
              rng: np.random.Generator) -> Iterator[FaultRecord]: ...

@dataclass(frozen=True)
class FaultRecord:
    event: EventRow
    truth: EventTruthRow
    disposition: Literal["emitted", "dropped", "sampled_out", "duplicate"]

def run(cfg: Config, out_dir: Path) -> Manifest:
    # 1. rng streams from SeedSequence(seed): world, one per pattern, realizer, one per fault
    # 2. world = build_world(...); retain mutable history for final catalog output
    # 3. for each pattern: plan(...) -> truth/patterns.jsonl
    # 4. per volume, per week window: heapq.merge(pattern.intents(...)) -> realize -> faults
    # 5. sort by (ingest_time, event_id) within partitions; write events, truth, loss
    # 6. finalize catalog history after churn; write manifest + report
```

The week-window streaming must not cut chains or handles: generators may
emit intents up to `window_end + spill` (default 6 h), and the runner
carries spill-over into the next window.

The fault-chain runner adapts these records between injectors and preserves
terminal dropped/sampled dispositions; they never disappear from accounting.
Only emitted events can acquire duplicate deliveries. Each injector consumes
prior truth and extends it rather than replacing earlier tags/original values.

Workflow expansion is owned by P3: the runner merges base intents, expands
P1/P2 entry-file triggers once, then merges continuation intents by true time.
Generated continuation/interruption intents never retrigger P3. Retain session,
workflow, job, popularity-history and fault-window state across chunks. Chunk
size must not change output. At period end, stop new work and finish admitted
handles/chains; retain delayed deliveries and report their extent separately
from the configured demand period. Spill is a buffering parameter, not a
license to truncate events. Global partition output must merge arrivals from
adjacent generation windows before finalizing file order and hashes.

## 12. CLI

```text
pcache-datagen generate (--preset NAME | --config FILE) --out DIR [--seed N] [--format parquet|jsonl] [--overwrite]
pcache-datagen validate DIR        # §13 checks; non-zero exit on failure
pcache-datagen describe DIR        # prints manifest summary + report/summary.md
```

## 13. Validation and acceptance checks

`validate` and the test suite check the following:

| Check | Rule |
| --- | --- |
| Schema | Every file matches §5. Enums are valid and required fields are non-null. |
| Referential integrity | Event `file_id`/`dir_id`/`volume_id`/`client_id` exist in `catalog/` (except `identity_break` IDs, which exist in truth). |
| Handles | Restore the clean stream from delivered and removed events, deduplicate, and undo all field mutations using truth. Successful opens have one close; failed opens have neither reads nor close. Check read sums in range mode; use realizer plan tests for summary mode (§5.1). |
| Loss reconciliation | §5.3 invariant per row. `manifest.known_gaps` covers every loss window. |
| No truth leakage | `events/` contains no truth columns, and no event value encodes `pattern_id`/`cause`. |
| Determinism | Two runs with the same seed give identical `sha256` for all data files. |
| Fault rates | On `small`, check probabilistic rates against eligible populations at the corresponding injection stage, not all rows. Report numerator, denominator and expected count. Retain issue #18’s ±20% acceptance tolerance on `small`; tune the acceptance fixture to provide sufficient eligible samples (target expected count ≥100). Sparse custom/tiny runs report insufficient statistical support and use endpoint fixtures; they cannot satisfy the small-preset acceptance gate. Fixed counts and boolean enabled flags are not probabilities. |
| Manifest | Hashes and row counts match the files. |

**Pattern recovery checks** use only events and catalog for the statistic,
and `truth/` only to know where to look. They prove the patterns are
actually present and learnable:

| Pattern | Check (on `small`) |
| --- | --- |
| P1 popularity | Zipf exponent fitted on per-project open counts is within ±0.15 of `zipf_s`. |
| P2 weekly schedule | ≥ 80% of the instance's expected occurrences have an open of its files within ±1 h of the slot (non-holiday, attendance-adjusted). |
| P3 workflow chain | For each planted edge with `p ≥ 0.5`, lift(A→B within 15 min) ≥ 3. |
| P4 lifecycle | Weekly open counts per project correlate (Pearson > 0.8) with the planted activity curve. |
| P5 burst | Burst directories show no significant weekly periodicity. |
| P6 batch job | ≥ 90% of job runs appear within ±15 min of `local_time`. |
| P7 new file | Opens per day after creation decrease (Spearman < −0.5). |
| P8 noise | Noise share of foreground opens is within ±20% of `noise_share`. |

Validate injected semantic violations against truth: an intentionally invalid
range or broken identity is an expected diagnostic, not an automatic dataset
failure. Schema errors, untagged violations, unmatched truth claims, bad hashes
and unexplained loss fail validation. Check the restored clean stream as well
as observed data; a generated fault must not excuse unrelated corruption.

Recovery thresholds apply to controlled, adequately populated fixtures with
competing patterns/faults disabled or explicitly accounted for. Mixed scenarios
report diagnostics separately. Short `tiny` runs do not claim weekly recovery.
P2 reports both the fraction of generated schedule opens within ±1 h (issue
#16) and attendance-adjusted occurrence coverage. P3 edges are measured within
an actor/session and separately before/after drift, never across the global
stream. Constant lifecycle curves, zero planted instances and too few samples
are reported as not applicable/insufficient data, not successful recovery.
Add endpoint and interleaving checks from §7.1 to the respective task suites.

`report/quality.json` contains:

- counts by operation, origin, fault and app;
- events per hour-of-week per volume;
- the file popularity curve;
- the file-size distribution by kind;
- the bytes-read distribution per open;
- session-length stats;
- the results of all checks above.

`report/summary.md` renders the same as Markdown.

## 14. Implementation layout and stack

```text
pyproject.toml                         # uv; console script pcache-datagen
src/predictive_cache/__init__.py
src/predictive_cache/datagen/
  __init__.py
  config.py        # pydantic config + presets (§10)
  schema.py        # Arrow schemas, enums (§5)
  model.py         # World, Intent, PatternRecord (§11.1)
  world.py         # build_world (§6)                       (#14)
  patterns/        # one module per pattern (§7)             (#15, #16)
  realize.py       # sessions, read profiles, churn (§8)    (#17)
  faults.py        # fault injectors, loss counters (§9)    (#18)
  io.py            # writers, partitioning, manifest (§3, §5.5)
  validate.py      # checks + recovery checks (§13)          (#19)
  report.py        # quality.json, summary.md                (#19)
  pipeline.py      # run() (§11.2)
  cli.py           # (§12)
  wordlist.txt     # synthetic names
tests/datagen/
```

Python 3.12, `uv`, `numpy`, `pyarrow`, `pydantic` v2, `pyyaml`, `typer` (or
`click`); `pytest`, `ruff`. Vectorize with numpy where event counts are large.
Generated datasets go outside the repo or in an ignored local directory;
never commit generated data except tiny JSONL fixtures under `tests/`.

## 15. Work breakdown

| Issue | Scope | Spec sections |
| --- | --- | --- |
| [#12](https://github.com/rahulgtt/predictive-cache/issues/12) | This spec | all |
| [#13](https://github.com/rahulgtt/predictive-cache/issues/13) | Foundation: scaffold, config, schemas, writers, manifest, pipeline stubs, CLI | §3–5, §10–12, §14 |
| [#14](https://github.com/rahulgtt/predictive-cache/issues/14) | World builder | §6 |
| [#15](https://github.com/rahulgtt/predictive-cache/issues/15) | Patterns I: P1, P4, P7, P8 | §7 |
| [#16](https://github.com/rahulgtt/predictive-cache/issues/16) | Patterns II: P2, P3, P5, P6 | §7 |
| [#17](https://github.com/rahulgtt/predictive-cache/issues/17) | Event realization and churn (P9) | §8 |
| [#18](https://github.com/rahulgtt/predictive-cache/issues/18) | Fault injection and loss accounting | §5.3, §9 |
| [#19](https://github.com/rahulgtt/predictive-cache/issues/19) | Validation, report, recovery checks, presets | §10.3, §12, §13 |

#13 should land first, but #14–#18 can start in parallel against §11 using
stubs. #19's rules can be built against fixtures and run end to end last.

## 16. Open questions (expected to change)

- **Event granularity**: is per-range `read` realistic and affordable from
  CUFS, or only `open` + summary? Covered by `realize.read_events`; confirm
  with Protocol.
- **Session / cohort keys**: will real sources have them? Covered by the
  `emit.*` switches.
- **File identity** across rename/replace: covered by `identity_break`;
  confirm with Protocol.
- **Realistic distributions**: rates, sizes and pattern mixes are guesses.
  Add a calibration task once any real trace exists.
- **Planner objective** (opens vs bytes vs wait time saved; see §1.3): no
  generator change expected.

## 17. Foundation contract refinements (issue #13)

The initial implementation uses fixture stubs; §6–9 workload behavior and
§10.3 scale targets remain work for #14–#19. All workload tuning is accepted
and validated in config now, but the stubs do not implement those controls.
Reports and `manifest.limitations` explicitly state this distinction.

### Schemas and serialization

`src/predictive_cache/datagen/schema.py` defines Arrow types and nullability.
Catalog identifiers/text are strings, counts and sizes are int64, time columns
are UTC microsecond timestamps, holidays are date32 lists, and weekday/project
lists are typed lists. Actor cohort/client IDs are nullable to support omitted
identity signals. Other nullable catalog fields are those identified in §5.2.
Pattern `params` is a JSON object in `patterns.jsonl`; the internal Arrow schema
stores its canonical JSON string. No new columns are added to event schema v1.
The P4 pattern kind is `project_lifecycle` as in §7.

Manifest `files` covers every output artifact except the manifest itself,
including config and reports. `rows` is null for non-tabular artifacts.
`generator.runtime` records Python and serialization dependency versions;
`limitations` records foundation restrictions. The resolved config hash is
SHA256 of compact, sorted, UTF-8 JSON with nonfinite numbers forbidden, not of
YAML presentation. Dependency versions are pinned in `uv.lock`.

JSONL timestamps use exactly six fractional digits and Z. Empty tables preserve
Arrow schemas in Parquet; JSONL empty files are interpreted through their named
schema. Partition date is UTC ingestion date, not event date. Complete artifact
validation is required before `--overwrite`; arbitrary/symlinked directories
and unlisted files are rejected. Full behavioral validation remains #19.

### Internal API and scope

`load_config(path=None, preset=None, seed=None, format=None)` resolves preset
then nested YAML values then CLI overrides. Unknown keys fail. Custom uses
small defaults before explicit overrides; the CLI requires exactly one of
`--preset` or `--config`. All pattern/fault enable switches are part of config.
The shared `World`, domain records, `Intent`, `PatternRecord`, `FaultRecord`
and protocols live in `model.py`; fixture implementations live in `stubs.py`,
so later issues can replace them without making consumers depend on stubs.

`run(cfg, out_dir, overwrite=False)` delegates serialization to
`write_dataset(cfg, out_dir, tables, patterns, known_gaps, overwrite=False)`.
The table dictionary includes all catalog tables, events, removed events,
loss counters and event truth; pattern records are separate. The foundation
buffers its small fixture; bounded scale streaming and cross-window workload
state must be integrated with real patterns in subsequent tasks. Fixed numeric
RNG component keys avoid changing the world when a component is disabled.

This section clarifies draft interfaces and manifest metadata for reviewer
attention; it does not change the acceptance requirements of #14–#19.

## 18. World-builder implementation choices (issue #14)

`world.build_world(cfg, rng)` creates metadata only. `catalog.catalog_rows`
projects it onto unchanged §5.2 schemas. The pipeline uses this world while
access patterns, event realization and faults remain fixtures (#15–#18).
The old stub world remains solely for small contract tests.

- File/user/team/service targets are global, split across volumes by
  quotient/remainder. Initial projects and new_per_month are per volume.
  A volume without teams has unteamed users (null cohort) assigned local
  projects; teams without users are implicit. Membership can include future
  projects, but future access patterns must check file/project availability.
- Volume root depth is 0, project root depth is 1, and max_depth bounds levels
  below project roots (global limit max_depth+1). Shared directories have depth
  2. Poisson branching has an occupancy-derived directory cap. Poisson weights
  followed by multinomial file placement give the exact global file target;
  these final occupancies are conditioned, not independent Poisson samples.
- `files.size_distributions.<kind>` adds median_bytes, sigma, min_bytes and
  max_bytes. Defaults follow §6.3 with natural-log sigma 1.0; sample lognormal
  then clip to positive ordered bounds. Sigma 0 supports fixed-size fixtures.
  Small-preset checks use 0.03 absolute share, 25% median and 35% q10/q90
  tolerances against clipped theoretical values. These are sampling tolerances,
  not real-workload calibration.
- Internal File.read_profile records §6.3 profile defaults; no reads are
  executed. Actor.app_weights records application preferences. The empty
  Actor.working_set_file_ids interface is reserved for #16; this issue does not
  select working sets, implement revisits, chains, bursts or demand timing.
  Private fields are excluded from catalog/event wire schemas.
- Project phase times started_at/ramp_end/active_end/cooldown_end and
  phase_activity support linear ramp/cooldown, active plateau and archival.
  Legacy activity_history keeps its step semantics when phases are absent.
  This is project state; demand-rate lifecycle effects belong to #15.
- Local HH:MM user schedules use IANA timezone conversion across DST. Holiday
  and vacation checks suppress working status. Vacation configuration means
  expected weeks per 13-week quarter, sampled as 1–2 local-calendar-week blocks.
  Service actors have no user vacations. Holiday demand-rate effects, batch
  scheduling and actual read timing remain in subsequent pattern issues.

Names come from a packaged fictional word list. No modeled contents, personal
paths or real read measurements are collected. These internal/config choices
are called out in the PR description; event and catalog schemas remain v1.

## 19. Patterns I implementation choices (issue #15)

P1/P4/P7/P8 now run through the existing CLI. Event/catalog schemas remain v1.
PatternGenerator.plan receives the full Config to read global randomness
and calendar controls. Intent.operation adds create/write with default open;
only the P7 metadata bridge consumes it. Full realization remains #17.

- **P1:** fixed seeded Zipf ranks within each project; each user's configured
  hourly rate is divided equally among assigned projects, then multiplied by
  P4 activity, local schedule and a linear diurnal curve. Activity above one
  amplifies rates. Disabling P4 gives existing projects multiplier one.
  popularity.diurnal_curve is an ordered list of local-hour/multiplier pairs;
  defaults are (0,0),(8,.3),(10,1),(12,.55),(14,1),(17,.6),(19,0),(24,0).
  Weekends and vacations suppress opens; holidays use holiday_activity.
  Recency weights multiply by 1 + 2**(-age_days / half_life), using creation
  or latest past size-history timestamp. Half-life zero disables the boost.
  Working sets and revisits remain #16.
- **P4:** truth records preserve project phase times/multipliers. Its only
  demand effect here is on P1; it emits no independent opens.
- **P7:** Poisson new-file arrivals add to the initial files.target_count.
  Arrivals start no earlier than project existence and end before period end.
  Existing file-kind/size sampling is reused; unique fictional paths and
  identities join the catalogs. Each file has create plus initial write
  metadata, then team opens with exponential decay. New control
  new_file.initial_opens_per_day: 12 is the initial proposal rate; working
  calendars/holidays can reduce observed opens. No assigned users means no
  team opens; metadata uses an available same-volume actor, or stays catalog
  only if the volume has no actor. Truth records that choice.
- **P8:** for each base open, an independent Bernoulli noise_share decision
  replaces its target and actor with a uniformly selected live file and user
  in the same volume. Archived projects are eligible. This preserves total
  opens and reduces non-noise amplitudes; create/write metadata is untouched.
  Noise actor selection is independent of the selected user's calendar.
  Share zero changes nothing and share one replaces every base open. If both
  P1 and P7 are disabled, standalone noise uses P1's configured hourly rate
  and local user calendars; noise_share thins proposals and all emitted
  opens are noise. With no base demand there is no meaningful mixed fraction.
  Truth explicitly identifies this standalone denominator.
- rate_scale scales open rates, not file births or create/write metadata.
  jitter_scale scales uniform offsets about canonical hourly midpoints:
  zero fixes the midpoint, one spans the hour, larger values clip within the
  hour. P1/P7 evaluate availability/calendars at the resulting timestamp.
  P7 births stay uniform; mixed P8 retains base timing. Independent hourly
  streams make requested window boundaries reproducible; P8 decisions are
  keyed by opaque intent ID. Planning runs once per world, P7 before others.

The runner buffers intents and output; scale targets remain #19. Fixture
realization still emits one-byte reads, fixed synthetic handle duration and
per-open sessions. No actual files are read or created, and cache/latency
fields stay null. Validation remains schema/artifact validation; controlled
small-preset tests compute #15 recovery from open events and catalog metadata,
with truth only supplying planted parameters/instance selection. Mixed
scenarios are not required to retain the isolated Zipf exponent.


## 20. Combined workloads, realization and faults (issues #16–18)

The CLI now connects all pattern families, complete operations and faults.
Wire schemas stay v1; the internal Intent.operation additionally accepts
rename/delete/replace. Workloads.plan/intents combines P2/P5/P6; WorkflowChain
expands P1/P2 entries once and returns only continuation/interruption intents.
P3 inherits lifecycle intensity from its triggers rather than multiplying it
again. Personal working sets and chronological revisit history are private.

Generation stays buffered for this hackathon implementation. Pattern windows
filter deterministic plans; admitted bursts/chains and handles finish beyond
the nominal period. P9 writes update effective size/path histories but do
not feed back into already planned P1 recency choices. Metadata mutations wait
for active handles; replacement preserves the path with a new identity.
Non-stale later opens can resolve that replacement. Pure deleted-file demand
is skipped except the configured failed stale-open fraction.

Realization supports ranges and summary mode using the same internal read
plan. Throughput/think-time only schedule synthetic operations; latency and
cache outcomes remain null. Faults add past-seven-day nightly warmer reads
and hourly probes before mutation/removal. Origin handles crossing a finalized
size/deletion boundary are omitted as whole handles. Identity breaks map
subsequent records after a rename while truth retains real simulated identity.

Fault streams are independent. Loss takes precedence over sampling, duplicates
only copy delivered events, and truth has one row per unique event ID with
original values retained. known_gaps includes loss/sampling coverage; counters
use true UTC hours and satisfy offered = emitted + dropped + sampled_out.
quality.json contains eligible/affected/expected fault-stage counts. Sampling
counts removals, and skew outliers use source/client groups as their denominator.
Validation and recovery reporting are described in §21; preset measurements
and remaining scale limits are recorded in [the issue #19 handoff](issue-19-handoff.md).

## 21. Validation and reporting (issue #19)

validate now verifies artifact integrity and restored event semantics. It joins
one truth row per unique ID, validates duplicate/removal/mutation claims and
restores original fields before checking references, path lifetimes, handles,
ranges and exact true-hour loss accounting. Tagged expected faults remain
diagnostics; unexplained invalid records fail. Summary-mode byte totals are
covered by realizer plan-parity tests because individual read rows are absent.

quality.json and summary.md include observed counts, per-volume local-hour
histograms, restored foreground popularity/byte/session statistics, file sizes,
fault-stage rates and P1–P8 recovery diagnostics. The report's semantic checks
run on generated rows; validate independently checks the serialized artifacts.
Recovery statuses are passed/failed only for supported controlled cases;
mixed scenarios are diagnostic, small samples insufficient_data, and absent or
constant signals not_applicable. Noise attribution explicitly uses truth for
accounting and is not an independent noise-detection score.

Recovery failures are distinct from structural corruption: the command checks
the dataset contract while reports expose planted-pattern results. Controlled
positive/negative fixtures and sufficiently populated fault-rate tests gate
the report formulas when tests are run manually. Production consumers must never read recovery
truth as features or use simulated restoration as their cleaner.

Parquet timestamp conversion and schema checks use exact typed paths; JSONL
retains strict value validation. Wire schemas and deterministic data-file
guarantees remain unchanged. Tiny was measured locally for generation plus
semantic validation after environment setup, including Python package imports,
and checked against the runtime and 10k–50k observed-row targets. Automated
GitHub workflows are excluded at the user's request for this one-time generator.

[Downstream guide](downstream-guide.md) provides commands, loading/filtering
examples, dataset sharing, origin semantics, gap handling and as-of catalog
rules. The generator remains buffered; runtime measurements describe the
tested host/config/revision rather than a universal hardware guarantee.

Current acceptance evidence and the open small/medium preset gaps are recorded
in [the issue #19 handoff](issue-19-handoff.md). The preset table remains a
target, not a claim that every preset meets it.
