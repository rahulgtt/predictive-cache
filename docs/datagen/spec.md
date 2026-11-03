# Synthetic access-event generator (`pcache-datagen`): spec v1

> **Status:** v1 planning spec for story
> [#11](https://github.com/rahulgtt/predictive-cache/issues/11). Schemas,
> patterns and defaults are expected to change as we learn more. Change them
> by PR to this file, never silently in code.

## 1. Purpose

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
Reading it outside tests and evaluation reports is a leakage bug.

### 1.3 Planner objective neutrality

The planner objective is still open. Candidates include cold opens avoided,
bytes saved, or user wait time saved; the last favours large files, because
a cold huge file costs far more wall-clock time than a small one. The
generator therefore always emits `file_size`, per-read byte ranges and
per-handle totals. It also includes large files that are read in full, so
any of these objectives can be evaluated later on the same data.

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
| Determinism | Same resolved config and seed give byte-identical files under `catalog/`, `events/`, `loss/` and `truth/`. Only `manifest.created_at` and `manifest.generator.git_sha` may differ. Use `numpy.random.SeedSequence` to spawn independent per-volume / per-pattern streams. |
| Privacy | All names and paths are synthetic, built from a fixed word list. No real usernames, paths or contents. |

## 5. Schemas

### 5.1 Access event (`events/`, `truth/dropped_events/`)

One row per filesystem operation as observed by the event source.

| Field | Type | Req | Description |
| --- | --- | --- | --- |
| `schema_version` | int16 | yes | `1`. |
| `event_id` | string | yes | Unique per original event. A **duplicate** reuses the same `event_id`. |
| `event_time` | timestamp | yes | When the operation happened, by the client/node clock. May be skewed (§9). |
| `ingest_time` | timestamp | yes | When the collector received the event. Normally ≥ `event_time`; can be much later for late events. |
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

- Each `open` has exactly one `close` with the same `handle_id`, later in
  `event_time`.
- A handle's `close.handle_bytes_read` equals the sum of its `read.bytes_read`.
- `0 ≤ offset` and `offset + bytes_read ≤ file_size`.
- No operations on a `file_id` after its `delete` except failed `open`
  (`enoent`).

### 5.2 Catalog (`catalog/`)

The world as built (§6). This is ground-truth metadata that a real system
could also provide, so consumers may read it.

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
| `hour_start` | timestamp | UTC hour bucket (by `event_time` of the original event). |
| `offered` | int64 | Events produced by the source, before loss/sampling, excluding duplicates. |
| `emitted` | int64 | Events delivered (unique `event_id`s). |
| `dropped` | int64 | Lost events (loss windows). |
| `sampled_out` | int64 | Removed by sampling. |
| `duplicates` | int64 | Extra duplicate deliveries. |
| `late` | int64 | Delivered more than `faults.late.threshold_s` after `event_time`. |

Invariant: `offered = emitted + dropped + sampled_out`.

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
| `event_id` | Joins to events. Duplicates share the row and set `fault`. |
| `intent_id` | The intent that produced the event (null for metadata churn). |
| `cause` | `pattern_id` of the generating pattern, or `background`, `noise`, `churn`, `warmer_sim`, `probe_sim`. |
| `faults` | List of applied faults: `duplicate`, `late`, `skew`, `dropped`, `sampled_out`, `origin_unknown`, `failed_result`, `invalid_range`, `identity_break`. Empty when clean. |
| `true_event_time` | Event time before clock skew. |
| `true_file_id` | `file_id` before identity break. |

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
| P5 | `burst` | One-off, non-recurring events: a **review day** (team opens most of a directory within 1–3 h) or a **directory scan** (one actor reads heads of every file in a subtree). Randomly placed. | `bursts_per_volume_month: 3`, `scan_share: 0.3` | Nothing (false-positive bait) |
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

## 8. From intents to events

### 8.1 Sessions

Intents of one actor are grouped into sessions: a new `session_id` starts
after an inactivity gap > `sessions.gap_min` (default 10). Service actors
get one session per job run. Session IDs are emitted only if
`emit.session_id` is true.

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
  to 300 s for interactive apps, 0 for service actors.
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

## 9. Data-quality fault injection

Faults are applied **after** realization, in the order below. Each one is
independently configurable and recorded in `truth/event_truth.faults`. With
all faults disabled, `events/` equals the clean stream.

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
| `late` | 1% | `ingest_time = event_time + LogNormal(median 5 min)`, tail to ~6 h. | Handle late arrival. |
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
  popularity: {rate_per_user_hour: 6, zipf_s: 1.1, recency_boost_half_life_days: 3}
  weekly_schedule: {instances_per_team: 2, jitter_min: 10, attendance: 0.85}
  workflow_chain: {chains_per_project: 3, delay_median_s: 120, continue_p: 0.7, drift_share: 0.2}
  burst: {bursts_per_volume_month: 3, scan_share: 0.3}
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
| `tiny` | 1 | ~300 | 10 | 2 | 10k–50k | < 10 s | Unit tests, CI, JSONL fixtures |
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
    def plan(self, world: World, cfg: PatternConfig, rng: np.random.Generator) -> list[PatternRecord]: ...
    def intents(self, world: World, window: tuple[datetime, datetime],
                rng: np.random.Generator) -> Iterator[Intent]: ...    # time-ordered within window

def build_world(cfg: Config, rng: np.random.Generator) -> World: ...

class EventRealizer(Protocol):
    def realize(self, world: World, intents: Iterator[Intent], window: tuple[datetime, datetime],
                rng: np.random.Generator) -> Iterator[tuple[EventRow, EventTruthRow]]: ...  # includes P9 churn

class FaultInjector(Protocol):
    name: ClassVar[str]
    def apply(self, events: Iterator[tuple[EventRow, EventTruthRow]], world: World,
              rng: np.random.Generator) -> Iterator[tuple[EventRow, EventTruthRow]]: ...

def run(cfg: Config, out_dir: Path) -> Manifest:
    # 1. rng streams from SeedSequence(seed): world, one per pattern, realizer, one per fault
    # 2. world = build_world(...); write catalog
    # 3. for each pattern: plan(...) -> truth/patterns.jsonl
    # 4. per volume, per week window: heapq.merge(pattern.intents(...)) -> realize -> faults
    # 5. sort by (ingest_time, event_id) within partitions; write events, truth, loss
    # 6. manifest + report
```

The week-window streaming must not cut chains or handles: generators may
emit intents up to `window_end + spill` (default 6 h), and the runner
carries spill-over into the next window.

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
| Handles | In the **clean** stream (events + dropped, after removing duplicates and undoing skew/identity faults from truth), every `open` has one `close` and totals match. |
| Loss reconciliation | §5.3 invariant per row. `manifest.known_gaps` covers every loss window. |
| No truth leakage | `events/` contains no truth columns, and no event value encodes `pattern_id`/`cause`. |
| Determinism | Two runs with the same seed give identical `sha256` for all data files. |
| Fault rates | Each enabled fault's observed rate is within ±20% of the configured rate (on `small`). |
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
| [#19](https://github.com/rahulgtt/predictive-cache/issues/19) | Validation, report, recovery checks, presets, CI | §10.3, §12, §13 |

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
