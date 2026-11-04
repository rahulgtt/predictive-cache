# Data preparation: cleaning, features, labels and snapshots (`pcache-prep`), spec v1

> **Status:** v1 planning spec for stories
> [#23](https://github.com/rahulgtt/predictive-cache/issues/23) (cleaning) and
> [#24](https://github.com/rahulgtt/predictive-cache/issues/24) (features,
> labels, snapshots). Nothing here is implemented. Defaults are starting
> points to tune on validation data, never on test data. Change schemas and
> rules by PR to this file, not silently in code.

## 1. Purpose

Read the [README glossary](../../README.md#basic-terms) and
[file-B example](../../README.md#example-a-feature-row-and-a-label) first.
The README teaches the concepts; this spec defines the detailed v1 cleaning,
feature, label and snapshot contracts within its design rules.

`pcache-prep` turns access events into model-ready tables:

1. **Clean**: produce a trusted, versioned demand dataset from raw events.
   Warmer, synthetic and unknown origins, failures, invalid ranges and
   duplicates are excluded and counted. Incompletely observed time is
   marked. Each successful foreground open becomes one row with the bytes it
   successfully consumed.
2. **Snapshot**: at past moments *t*, compute what the Controller would have
   known (features), which files it would have considered (candidates), and
   what happened next (labels). Write chronological train/validation/test
   tables.

The **same feature-store code** runs in the snapshot builder and in the live
Controller. That is the main defence against train/serve skew and leakage.

Input today is datagen output ([datagen spec](../datagen/spec.md) §3, §5);
later it is a real source mapped to the same event schema
([#25](https://github.com/rahulgtt/predictive-cache/issues/25)). Results on
synthetic input are plumbing and planted-pattern evidence only.

### 1.1 Non-goals

- Models, training, calibration, baselines, planner and budgets. Those are
  later Training, Evaluation and Inference stories; they consume §8 output.
- A cache simulator. Labels predict **demand**, not avoided misses (§7).
- Repairing identity breaks or clock skew. They are measured (§4.8) and
  bounded (§4.4), not repaired.
- Weekly / time-of-week modeling. It's cut for the hackathon; time features
  exist but are disabled by default (§5.2).

### 1.2 Decisions inherited and what is configurable

| Topic | v1 default | Source | Alternative kept configurable |
| --- | --- | --- | --- |
| Prediction unit | File `(volume_id, file_id)` | PRD, team plan | Ranges are later work |
| Target | Successful foreground **demand** in `[t+lead, t+lead+H)` | [Team plan](../predictive-cache-48-hour-team-plan.md) "The ML brain" | `cold_open` (PRD quiet-gap rule) |
| Horizon H / lead | 300 s / 0 s | Team plan: freeze after measuring retrieval lead time | Any |
| Features | Recency, recent frequency, size, project/dir activity, distinct sessions, demand growth, co-access | Team plan; [datagen handoff](../datagen/decisions-and-handoff.md) | Time-of-day group (off) |
| Co-access | Past-only: train ticks use periodic causal refits; val/test use one table frozen at `val_start` | Team plan: "past-only features and preprocessing fit on training only" | Refit interval |
| Splits | Chronological, boundary-protected | README, team plan | Explicit boundaries |
| Modes | One tick-based mode | Team plan (one classifier) | The PRD's scheduled/reactive split is not in v1 |

The PRD's cold-open target and the team plan's demand target are not settled
yet. Both are implemented behind `labels.target` so the Evaluation story
decides with evidence (§15).

### 1.3 Worked example: from file B to a training row

A **tick** is a scheduled prediction time, written as `t`. A **snapshot**
records what was known about the candidate files at that time, together with
the answers learned afterward for training or evaluation. It is a table of
model examples, not a filesystem backup snapshot. The live model receives
the features without those later answers.

Consider the README's fictional file B. For this example, use a tick at
`2026-11-03T09:00:00Z`, the default `demand` target, no lead time, and a
five-minute prediction window. Assume B is an eligible candidate and all
inputs below were known before the tick. These are selected columns, not a
complete snapshot row or a real measurement.

| Column | Example value | Role |
| --- | --- | --- |
| `volume_id` | `vol-001` | Identifies the volume; not a model feature. |
| `file_id` | `file-b` | Identifies B; not a model feature. |
| `t` | `2026-11-03T09:00:00Z` | Identifies the prediction time; not a model feature. |
| `opens_5m` | `0` | Feature: successful opens in the previous five minutes. |
| `opens_15m` | `0` | Feature: successful opens in the previous fifteen minutes. |
| `secs_since_last_open` | `1200` | Feature: seconds since B's last successful open. |
| `file_size` | `1048576` | Feature: latest known size in bytes. |

The feature values together form B's **feature row**. The v1 open-count
features count successful opens; they do not claim that each open consumed
bytes. Labels below require demand as defined in §7.

Afterward, attach a **label** to this earlier row:

| What happened in `[09:00:00, 09:05:00)` UTC? | Label |
| --- | --- |
| A qualifying foreground read consumed B's data at 09:03. | `1`: yes, B was needed. |
| There was no qualifying demand, the whole window was observed, and no open had unknown consumption. | `0`: no, B was not needed in that window. |
| No qualifying demand was observed, but collection had a gap or an open had unknown consumption. | Unknown; do not guess `0`. |

This assumes the label facts arrive before the next split boundary and
the tick passes the boundary checks (§7, §8.2). A positive label needs
qualifying demand, not a fully observed window. An unknown label is dropped
from the snapshot rows and counted in the output statistics (§8.5).
The 09:03 read never becomes an input to the 09:00 prediction.

Actual tick scheduling follows §8.1; the full feature list is in §5.2 and
the output schema is in §8.5. The alternative `cold_open` target has extra
history requirements (§7); the simple labels above describe `demand` only.

## 2. Pipeline and boundaries

```text
datagen output / real source (event schema v1)
   │  pcache-prep clean          (§4)
   ▼
cleaned dataset: events_clean, excluded, opens, open_ranges, meta, coverage, catalog, report
   │  updates_from_clean()       (§5.1)   known_at order
   ▼
FeatureStore.apply(...)  ──►  candidates(t)  ──►  features_as_of(t)   (§5, §6)
   │                                   ▲
   │       CoAccessTable.fit(opens known before fit_end)   (§5.4)
   ▼
labels from opens / open_ranges in [t+lead, t+lead+H) + coverage   (§7)
   │  pcache-prep snapshots      (§8)
   ▼
snapshots/ (train | val | test), universe/, artifacts/ ──► Training / Evaluation stories
```

| Consumer | Reads | Never reads |
| --- | --- | --- |
| Feature store, candidates, co-access fit | `opens`, `meta`, `catalog` (as initial state only, §3.1) via `updates_from_clean()` | `excluded`, `truth/`, future catalog fields |
| Label rules | `opens`, `open_ranges`, `coverage`, cleaned `manifest.period` | `truth/` |
| Training / evaluation stories | `snapshots/`, `universe/`, `artifacts/` | `truth/`, raw events |
| Live Controller (later) | The same `FeatureStore`, fed by live updates | — |
| Tests only | `truth/` (via the input dataset path) | — |

## 3. Conventions and time semantics

The datagen spec §4 conventions apply (IDs, UTC microsecond timestamps,
bytes as int64, `*_ms` durations, determinism, privacy). In addition:

| Name | Definition | Used for |
| --- | --- | --- |
| `event_time` | Source clock time of the operation. May be skewed. | Diagnostics only |
| `known_at` | `ingest_time`: when the collector had the event. | **Visibility**: what the Controller could know |
| `effective_time` | `min(event_time, ingest_time)`. An operation can't happen after it was received; this bounds forward skew. | **When it happened**: recency, windows, labels |

**As-of rule.** A feature, candidate or fitted artifact used for prediction
time *t* may depend only on updates with `known_at < t`. Their
`effective_time` is then also `< t`. The feature store enforces this with a
watermark (§5.3). Labels use future `effective_time` and never feed features.

**Two-stage facts about an open.** That a successful open happened is known
at `open_known_at`. Its byte facts (bytes read, full read, whether it
consumed anything) are known only at `completed_known_at` (§4.5). Updates
carry each fact at its own time (§5.1); a fact must never be backdated to an
earlier update.

**Clock uncertainty.** Datagen loss counters bucket by true time; cleaned
times are skewed by up to the client clock error. Coverage therefore
expands every incomplete interval by `clean.coverage.skew_margin_s` on both
sides (§4.4).

### 3.1 Catalog as initial state

The datagen `catalog/` contains the full history of the period, including
**future** creation, deletion, replacement and path intervals (datagen §5.2).
This section is the availability-time policy datagen §5.2 asks consumers to
define. It is stricter than resolving paths at *t*, because in-period
metadata is only trusted once its event is known. Consumers outside tests
use the catalog **only as the state at `period.start`**:

- **Files** with `created_at < period.start` and their path row: the row
  with the latest `valid_from ≤ period.start`. Size is `size_at_creation`;
  later sizes come from events.
- **Directories** with `created_at < period.start`, plus `parent_dir_id` and
  `project_id`. A directory's project never changes.
- **Volumes:** timezone and calendar.

Never read `deleted_at`, `replaced_by_file_id`, `valid_to`, or any row with
`created_at`/`valid_from ≥ period.start`. In-period creation, writes,
renames, deletes and replacements reach state only through `MetaChanged`
updates at their `known_at`. Directories created in-period get their
project from their nearest known ancestor. Without a catalog, all state
comes from events and catalog-derived features are null.

**Catalog misses** (a `file_id` in events that is not in the initial
catalog and has no known `create`, e.g. identity breaks) are kept. The file
is first seen at its first update, and catalog-derived features are null.
They are counted (§4.8).

## 4. Cleaning (`pcache-prep clean`, story #23)

### 4.1 Input

A dataset directory in the datagen spec §3 layout: `manifest.json`,
`events/`, `loss/` (optional for real data), `catalog/` (optional for real
data). `truth/` may exist and **must not be read** by cleaning code; only
tests read it. Input is read in partition order and must not be assumed
globally sorted. `manifest.period` defines the observed period.

### 4.2 Output layout

```text
<out>/
  manifest.json                      # §4.7
  config.resolved.yaml
  events_clean/volume_id=<v>/date=<YYYY-MM-DD>/part-00000.parquet   # date = UTC date of known_at
  excluded/volume_id=<v>/date=<YYYY-MM-DD>/part-00000.parquet
  opens/volume_id=<v>/date=<YYYY-MM-DD>/part-00000.parquet           # date = UTC date of open_time
  open_ranges/volume_id=<v>/date=<YYYY-MM-DD>/part-00000.parquet     # date = UTC date of effective_time
  meta/volume_id=<v>/part-00000.parquet
  coverage/coverage.parquet
  catalog/                           # copied unchanged from input if present
  report/quality.json
  report/summary.md
```

`--format jsonl` mirrors this with `.jsonl` for fixtures. Row order is
deterministic: `events_clean`/`excluded` by `(known_at, event_id)`,
`opens` by `(open_time, open_id)`, `open_ranges` by `(open_id, offset)`.

### 4.3 Exclusion rules

Applied in order; the **first** matching rule assigns the `reason`. Every
input row lands in exactly one of `events_clean` or `excluded`.

| # | Reason | Rule |
| --- | --- | --- |
| 1 | `schema_invalid` | A required field is missing, an enum value is unknown, or a type is wrong (datagen §5.1). |
| 2 | `duplicate` | Same `event_id` as an earlier delivery. The canonical delivery is the one with the smallest `(ingest_time, input file path, row index)`; it continues through later rules. All other deliveries are excluded. |
| 3 | `origin_warmer` | `origin = warmer`. |
| 4 | `origin_synthetic` | `origin = synthetic`. |
| 5 | `origin_unknown` | `origin` is `unknown` or null. |
| 6 | `invalid_range` | **read:** `offset` or `bytes_requested` null or < 0; `offset > file_size`; `bytes_read > bytes_requested`; or `result = ok` and `offset + bytes_read > file_size`. **write:** `offset` or `bytes_requested` null or < 0. Structural checks run before the result check, so malformed `einval` rows get this reason. |
| 7 | `result_failed` | `result != ok`. Failed opens and reads are not consumption. |
| 8 | `time_invalid` | `event_time > ingest_time + clean.max_future_skew_s` (default 3600) or `ingest_time − event_time > clean.max_late_s` (default 7 d). |

Kept events gain `effective_time` and `known_at` (§3). Nothing is rewritten.
`excluded` rows carry the original fields plus `reason` and an optional
`detail` string. Unknown `file_id`s are **not** excluded (§3.1).

### 4.4 Coverage

**`coverage` table:** one row per volume per UTC hour in `manifest.period`.

- With loss counters: `loss_fraction = (dropped + sampled_out) / offered`
  (0 when `offered = 0`). Status is `complete` if
  `loss_fraction ≤ clean.coverage.max_loss_fraction` (default 0.0, so any
  loss makes the hour `incomplete`), else `incomplete`.
- Hours overlapping a `manifest.known_gaps` window are always `incomplete`.
- Without counters: status is `no_counters`. It is treated as not complete
  unless `clean.coverage.assume_complete_without_counters: true`, which is
  then recorded in the manifest.

**Observed interval rule.** Label code (§7) calls an interval `[a, b)` of a
volume **observed** iff both hold:

1. `[a, b) ⊆ [period.start, period.end)`, and
2. no hour whose status is not `complete`, expanded by
   `clean.coverage.skew_margin_s` (default 600 s, which must be ≥ the
   source's maximum clock error) on both sides, overlaps `[a, b)`.

Anything outside the period, or with a missing coverage row, is unobserved.
`pcache-prep` exposes this as `Coverage.is_observed(volume_id, a, b)` (§10)
so that every caller applies the same rule.

### 4.5 Opens, ranges and metadata reconstruction

**Grouping.** Group kept `open`/`read`/`close` events by `handle_id`. An
event with a null `handle_id` forms its own single-event group,
`orphan-<event_id>`. Null handles are never grouped together. One `opens`
row is written per group that has an `open` or at least one `read`:

- **`open_time` / `open_known_at`:** from the `open`. If the open is
  missing (lost), use the earliest-time read for `open_time`, the earliest
  `known_at` among the reads for `open_known_at`, and set
  `open_missing = true`.
- **`close_time` / `close_known_at`:** from the `close`. Null with
  `close_missing = true` if there is no close.
- **`completed_known_at`:** when the open's byte facts become visible. It
  must be computable identically in batch and live (ML spec §8.2), so it
  is defined by an **incremental handle tracker** that processes the
  group's kept events in `(known_at, event_id)` order:
  - *With a close:*
    `completed_known_at = close_known_at + clean.completion_grace_s`
    (default 60 s).
  - *Without a close:* the handle completes when no event of the group
    arrives for `clean.handle_timeout_s` (default 3600 s). Then
    `completed_known_at = last event's known_at + handle_timeout_s`.
  - Reads with `known_at > completed_known_at` are **late after
    completion**:
    - they are excluded from the open's byte facts below and counted
      (`late_reads_after_completion`);
    - they stay in `open_ranges`, because labels describe demand, not
      visibility.

  `completed_known_at ≥ open_known_at` always holds. The batch cleaner
  **must** run the same `HandleTracker` (§10) over events sorted by
  `(known_at, event_id)`, so batch and live agree by construction.
- **Byte reconciliation.** Failed and invalid reads were already excluded
  (§4.3), and late-after-completion reads don't count. The source's read
  mode is **declared** in `clean.read_mode` (part of the serving contract,
  ML spec §5), never inferred per handle. Inferring it would let a late
  read silently switch a handle to close totals.
  - *`read_mode: ranges`* (default; datagen `realize.read_events: ranges`):
    - `bytes_read` = sum of kept reads' `bytes_read` known by
      `completed_known_at`. It is 0 if there are none and a successful open
      or close exists, and null if the group has neither;
    - `distinct_bytes_read` = size of the union of their ranges.
    - Close totals are **never** used for byte facts. If a close's
      `handle_bytes_read != bytes_read`, set `bytes_inconsistent = true`
      (failed, lost or late reads).
  - *`read_mode: summary`* (datagen `read_events: none`; the source emits
    no per-range reads):
    - `bytes_read` = the close's `handle_bytes_read` (null without a
      close);
    - `distinct_bytes_read` is null.
    - Any read events that do appear are counted as
      `unexpected_reads_in_summary_mode` and ignored for byte facts.
- **`full_read`:** `coalesce(distinct_bytes_read, bytes_read) ≥ 0.9 × file_size`;
  null if both are null.
- **`consumed`** (nullable bool, a byte fact known at
  `completed_known_at`): `true` if `bytes_read > 0`, `false` if
  `bytes_read = 0`, null if unknown. Only `true` is demand.

**`open_ranges`.** One row per kept successful read: `open_id` (the group
ID above), `volume_id`, `file_id`, `offset`, `length` (= `bytes_read`),
`effective_time`, `known_at`, `file_size`. Labels use it for exact bytes.

**`meta`.** Kept `create`/`write`/`rename`/`delete` events, one row each.

### 4.6 Schemas

**`events_clean`**: datagen §5.1 fields plus `effective_time` and
`known_at` (timestamps, §3).

**`excluded`**: datagen §5.1 fields plus `reason` (enum, §4.3) and `detail`
(string, nullable).

**`opens`**:

| Field | Type | Req | Description |
| --- | --- | --- | --- |
| `open_id` | string | yes | `handle_id`, or `orphan-<event_id>` (§4.5). |
| `volume_id`, `file_id`, `dir_id` | string | yes | From the open (or first read). |
| `session_id`, `cohort_id`, `client_id`, `app` | string | nullable | From the open. |
| `open_time` | timestamp | yes | `effective_time` of the open, or the first read. |
| `open_known_at` | timestamp | yes | `known_at` of the open, or the first read. |
| `close_time`, `close_known_at` | timestamp | nullable | From the close. |
| `completed_known_at` | timestamp | yes | When byte facts become visible. |
| `file_size` | int64 | yes | At open. |
| `bytes_read`, `distinct_bytes_read` | int64 | nullable | §4.5 |
| `read_event_count` | int32 | yes | Kept successful reads in the group. |
| `full_read` | bool | nullable | §4.5 |
| `consumed` | bool | nullable | §4.5 |
| `open_missing`, `close_missing`, `bytes_inconsistent` | bool | yes | Reconstruction flags. |

**`open_ranges`**: §4.5.

**`meta`**: `event_id`, `volume_id`, `file_id`, `dir_id`, `operation`,
`effective_time`, `known_at`, `file_size`.

**`coverage`**: `volume_id`, `hour_start`, `offered`, `emitted`, `dropped`,
`sampled_out` (nullable when there are no counters), `loss_fraction`
(nullable), `status` (`complete` | `incomplete` | `no_counters`).

### 4.7 Manifest (cleaned)

```json
{
  "manifest_version": 1,
  "dataset_id": "clean-<parent dataset_id>-<first 8 hex of config_sha256>",
  "data_kind": "synthetic",
  "stage": "clean",
  "parent": {"dataset_id": "synth-small-s42-1a2b3c4d", "manifest_sha256": "…"},
  "tool": {"name": "pcache-prep", "version": "0.1.0", "git_sha": "…"},
  "config_sha256": "…",
  "period": {"start": "…", "end": "…"},
  "counts": {
    "input_events": 0, "kept_events": 0, "excluded_by_reason": {},
    "opens": 0, "opens_open_missing": 0, "opens_close_missing": 0,
    "opens_bytes_inconsistent": 0, "late_reads_after_completion": 0,
    "open_ranges": 0, "meta_events": 0,
    "catalog_miss_files": 0, "catalog_miss_events": 0
  },
  "coverage": {"hours": 0, "complete": 0, "incomplete": 0, "no_counters": 0,
               "skew_margin_s": 600, "assume_complete_without_counters": false},
  "files": [{"path": "…", "rows": 0, "sha256": "…"}],
  "created_at": "…"
}
```

`period` is copied from the parent. `data_kind` is inherited and never
upgraded from `synthetic` to `real`.

### 4.8 Quality report

`report/quality.json` (and `summary.md` with the same content) includes:

- input, kept and excluded counts by reason, per volume per day;
- duplicate rate (extra deliveries ÷ unique event IDs);
- late-arrival quantiles of `ingest_time − event_time` (p50/p90/p99/max),
  plus the count of negative values (evidence of skew);
- coverage hours by status, and the incomplete windows after margin
  expansion;
- opens: count, `open_missing` / `close_missing` / `bytes_inconsistent`
  counts, consumed share;
- successful returned bytes (sum of `bytes_read` over opens) and distinct
  bytes;
- catalog misses: files and events with an unknown `file_id` (an
  identity-fragmentation proxy);
- origin and app mix of the input;
- opens per hour of day and per weekday, per volume.

## 5. Feature store (story #24)

### 5.1 Updates and state

`updates_from_clean()` turns the cleaned dataset into a single stream in
non-decreasing `known_at` (ties broken by update kind order `MetaChanged`,
`OpenObserved`, `OpenCompleted`, then by ID):

| Update | Emitted at | Carries |
| --- | --- | --- |
| `OpenObserved` | `open_known_at` | `open_id`, volume/file/dir, session key, cohort, app, `open_time`, `file_size`. **No byte facts.** |
| `OpenCompleted` | `completed_known_at` | `open_id`, file, `bytes_read`, `distinct_bytes_read`, `full_read`, `consumed` |
| `MetaChanged` | `known_at` | `operation`, file/dir, `effective_time`, `file_size` |

The store is constructed with the initial-state catalog view (§3.1). In the
live Controller the same update types are built from live events.

**Session key** = `session_id`, else `client_id`, else null. If it's null,
session-dependent features are null.

**State is bounded per volume.** Implementations may choose structures, but
must produce exactly the §5.2 semantics:

- **Per file:**
  - exact open times within the last hour;
  - hourly open-count buckets for 7 days;
  - last open time;
  - last full-read time;
  - a session-key → last-open-hour map, evicted after 24 h;
  - byte aggregates in daily buckets for 7 days;
  - size, first-seen time, last write, deleted flag, current dir.
- **Per directory and project:** exact open times within the last hour, and
  a session-key → last-open map evicted after 1 h.
- **Per session key:** files opened within the last `coaccess.delta_s`.
- **Per volume:** exact open times within the last 5 min.

### 5.2 Feature set `fv1`

- **Visibility:** every feature at *t* uses only updates with
  `known_at < t`. Open counts and recency use `OpenObserved` (successful
  opens); byte features use `OpenCompleted`.
- **Window semantics:**
  - *Exact windows* (≤ 1 h) count events with `effective_time ∈ [t − w, t)`.
  - *Bucketed windows* (24 h, 7 d) count events whose UTC hour (or day)
    bucket starts in `[floor_hour(t) − w, floor_hour(t))` (or the daily
    equivalent), plus the exact count since `floor_hour(t)`.
- **Missing inputs** give **null**, never a guessed value.
  `feature_schema.json` records each feature's name, dtype, group,
  definition and availability.

| Group | Feature | Definition |
| --- | --- | --- |
| Recency | `secs_since_last_open` | `t −` last successful open of the file; null if none. |
| | `secs_since_last_full_read` | `t −` `open_time` of the last completed open with `full_read`; null if none. |
| Frequency | `opens_5m`, `opens_15m`, `opens_1h` (exact); `opens_24h`, `opens_7d` (bucketed) | Successful opens of the file. |
| Sessions | `distinct_sessions_1h` (exact), `distinct_sessions_24h` (bucketed) | Distinct session keys opening the file. |
| Growth | `growth_15m_vs_1h` | `opens_15m / (opens_1h / 4 + 1)`. Rising shared demand, e.g. bursts. |
| Size / bytes | `file_size` | Latest known size. |
| | `mean_bytes_per_open_7d` | Mean `bytes_read` over completed opens (daily buckets); null if none. |
| | `full_read_share_7d` | Share of completed opens with `full_read`; null if none. |
| Activity | `dir_opens_15m`, `dir_opens_1h` | Successful opens in the file's current directory (exact). |
| | `dir_distinct_sessions_1h` | Distinct session keys in that directory. |
| | `project_opens_1h` | Opens in the file's project; null if the project is unknown. |
| | `volume_opens_5m` | Volume load context. |
| Co-access | `coacc_max_conf` | Max `conf(A → file)` over files A with a successful open known before *t* and `open_time ∈ [t − coaccess.delta_s, t)` (any session key). |
| | `coacc_max_lift`, `coacc_sum_conf`, `coacc_sources` | Max lift, sum of confidences, and count of such A with an edge to the file. |
| Age / churn | `file_age_s` | `t −` initial-catalog `created_at` or in-period `create`, else first-seen time. |
| | `secs_since_last_write` | Null if no write seen. |
| Time (off) | `hour_of_day_local`, `is_workday` | Only with `features.time.enabled`; needs the volume timezone. |

The co-access features read the `CoAccessTable` the caller supplies for *t*
(§5.4, §8.2); without one they are null. The feature registry makes adding
or removing a feature a one-file change plus a version bump (`fv2`, …).

### 5.3 `features_as_of` contract

```python
store.features_as_of(volume_id, t, file_ids, coaccess) -> pyarrow.Table
```

- Returns one row per requested `file_id`, in request order, with columns
  `volume_id`, `file_id`, `t` plus every enabled feature.
- **Raises** if `store.watermark ≥ t`, i.e. if any update with
  `known_at ≥ t` was applied. Callers apply all updates with
  `known_at < t`, query, then continue applying.
- **Prefix equivalence:** a store built from all updates with `known_at < t`
  returns the same result whatever later updates exist in the source
  stream. Equivalently, the result depends only on that prefix.
- Pure with respect to state: calling it twice gives identical output.
- Vectorised over `file_ids`. Performance target: §12.3.

### 5.4 Co-access table

`CoAccessTable.fit(opens, fit_start, fit_end, cfg)` uses only `opens` rows
with `open_known_at < fit_end` and `open_time ≥ fit_start`. It needs only
the open facts, so completion time doesn't matter. Opens without a session
key are skipped.

1. Within each session key, order opens by `(open_time, open_id)`.
2. `n(A)` = opens of file A. N = all opens.
3. `support(A → B)` = number of **A-opens** followed by at least one open
   of B ≠ A in the same session key within `coaccess.delta_s` (default
   300 s). Each A-open counts at most once per B.
4. `conf(A → B) = support(A → B) / n(A)`, i.e. P(B follows within Δ | an
   A-open).
5. `base(B) = Σ_{X ≠ B} support(X → B) / N`, i.e. P(B follows within Δ | a
   random open). Because each open contributes at most once per B,
   `base(B) ≤ 1`. `lift = conf / base(B)`.
6. Keep edges with `support ≥ coaccess.min_support` (3) and
   `lift ≥ coaccess.min_lift` (1.5). Keep the top `coaccess.max_out_edges`
   (50) per A by confidence, ties broken by `to_file_id`.
7. Persist as Parquet (`from_file_id`, `to_file_id`, `support`, `n_from`,
   `conf`, `lift`), with `fit_start`, `fit_end`, `delta_s` and the parent
   dataset ID recorded in the snapshot manifest.

How fits map to ticks is in §8.2. Order-2 context (A→B→C vs D→B→E, datagen
§7.1) is not in v1 (§15).

### 5.5 Checkpoint and restore

`store.checkpoint() -> bytes` and `FeatureStore.restore(data, cfg, catalog)`
must round-trip exactly: features after restore equal features without the
restart. The format is versioned (msgpack or Arrow IPC). The live Controller
later stores checkpoints in the predictive Kvrocks namespace, **never** CUFS
volume Kvrocks; v1 writes files.

## 6. Candidates

Candidate generation is a **pure function** (owned by #31) over a read-only
`CandidateView` that the feature store exposes (owned by #30, §10):

```python
generate_candidates(view: CandidateView, volume_id, t, coaccess, cfg) -> pyarrow.Table
```

It returns `file_id` and `candidate_sources` (a list). `FeatureStore.candidates()`
delegates to it. The view answers only from state known before *t*, so the
function can't leak. The result is a bounded union:

| Source | Rule (via `CandidateView`) | Cap |
| --- | --- | --- |
| `recent` | `view.recent_files(window=candidates.recent_window_s)`: files with a successful open in the window, most recent first | 200 |
| `neighbor` | `coaccess.out_edges(a)` for `a in view.recent_files(window=coaccess.delta_s)`, ranked by confidence | 200 |
| `project` | `view.files_in_dir(d)` for `d in view.active_dirs(window=900)`, ranked by `view.open_counts(…, "7d")` | 200 |
| `popular` | `view.top_files(window="24h", k=100)` | 100 |

- **Union and cap:** dedupe the union, keeping all sources per file, and cap
  at `candidates.max_candidates` (500). The cap keeps `recent`, then
  `neighbor`, then `project`, then `popular`. Ties are broken by `file_id`.
- **Known at *t*:** `view.is_known(file_id)`: the file is in the initial
  catalog (§3.1), or has an update with `known_at < t`.
- **Eligible:** `view.is_eligible(file_id)`: known, not deleted per a
  `MetaChanged` known before *t*, latest known `file_size > 0`, and inside
  `candidates.allow_dirs` if set. A future catalog `deleted_at` is never
  consulted. Ineligible files are dropped and counted.
- **Coverage** is measured in the snapshot `universe` table (§8.5), never
  inferred.

## 7. Labels

A `LabelRule` decides, per candidate and tick, using `opens`,
`open_ranges` and `Coverage.is_observed` (§4.4) only. Label code may look
at the whole cleaned dataset, past and future; labels never feed features.

- **Window:** `W = [t + labels.lead_s, t + labels.lead_s + labels.horizon_s)`,
  default `[t, t + 300 s)`.
- **Demand in W** is defined by **when consumption happened**, not when the
  handle opened:
  - *`read_mode: ranges`:* successful `open_ranges` rows of the file with
    `effective_time ∈ W`;
  - *`read_mode: summary`:* an open with `consumed = true` and
    `open_time ∈ W`.
  - An open in W whose consumption is unknown (`consumed` null), with no
    other demand in W, makes the label unknown.
  - **Label availability across splits:** a label fact is a contributing
    read's `known_at`, or a summary-mode open's `completed_known_at`. If
    **any** fact for the file in W has `known_at` ≥ the start of the next
    split, the label is **unknown** and the row is dropped. This holds even
    for a fact that would have made the row positive. A row's label
    therefore never depends on data first observed in a later split. It is
    counted as `label_unknown_late_fact`.

  Call the opens that contribute demand in W **qualifying opens**.
- **`demand` (default):**
  - `1` if there is demand in W, even if W is not fully observed;
  - else `0` if `is_observed(W)` and no unknown-consumption open is in W;
  - else unknown.
- **`cold_open` (alternative, PRD):** let `G = labels.cold_gap_s` (86 400).
  Demand at time τ (the first qualifying read or open time in W) is **cold**
  if the file has no demand (by the rule above) in `[τ − G, τ)`.
  - `1` if the demand is cold **and** `is_observed([τ − G, τ))`;
  - `0` if `is_observed(W)` and either there is no demand in W, or it is
    warm (earlier demand was seen in the lookback);
  - else unknown. This includes cold-looking demand whose lookback is not
    observed, e.g. near `period.start`.
- **Unknown** rows are dropped and counted (§8.5).
- **Label details (positives; zeros/nulls for negatives):**
  - `label_first_time`: first demand time in W, for timeliness analysis;
  - `label_n_opens`: number of qualifying opens;
  - `label_distinct_bytes`: size of the union of the file's `open_ranges`
    intervals with `effective_time ∈ W`. Reads outside W never count. For
    summary-mode qualifying opens, add `bytes_read`, set
    `label_bytes_exact = false`, and cap the total at `label_max_file_size`;
  - `label_max_file_size`: max `file_size` over the contributing reads and
    opens.

Overlapping reads count once. Invariant:
`label_distinct_bytes ≤ label_max_file_size`.

## 8. Snapshots (`pcache-prep snapshots`)

### 8.1 Ticks

- Ticks every `snapshots.tick_s` (900) aligned to the UTC epoch, from
  `period.start + snapshots.warmup_s` (1 day) up to
  `period.end − (lead + H)`. Later ticks would have label windows past the
  observed period and are recorded as `dropped_period_end`.
- Ticks are per volume. A tick with no candidates is recorded in `universe`
  and produces no rows.

### 8.2 Splits and co-access fits

- **Boundaries:** `snapshots.splits` gives `val_start` and `test_start`. If
  null, they come from fractions of `[period.start + warmup, period.end)`
  (train 0.6 / val 0.2 / test 0.2), rounded down to UTC midnight.
- **Assignment:** each tick belongs to the split containing *t*.
- **Boundary protection:** a tick is dropped (`dropped_boundary`) if
  `t + lead + H > boundary − clean.coverage.skew_margin_s` for the next
  split's boundary. The label window plus a clock-skew embargo must end
  before the next split. Without the embargo, a next-split operation with
  negative skew could land in this split's window. The `cold_open` lookback
  goes into the past, so it may cross a boundary.
- **Co-access, train ticks:** causal periodic refits. At each refit time
  `r_k = period.start + k × coaccess.train_refit_every_s` (1 day), fit on
  `[period.start, r_k)` with `open_known_at < r_k`. A train tick *t* uses
  the latest fit with `r_k ≤ t`. Before the first refit, it uses no table
  and the co-access features are null.
- **Co-access, val and test ticks:** one table fitted on
  `[period.start, val_start)` with `open_known_at < val_start`, frozen
  for all val and test ticks. It is written to `artifacts/coaccess.parquet`
  as the deployable artifact.
- **Store features in val/test** still use every update known before *t*,
  including earlier val/test events, as the live Controller would. Only
  fitted artifacts are frozen.

### 8.3 Sampling and weights

- **Train only:** keep every positive. Sample negatives per tick with
  probability `q = min(1, max(snapshots.min_negatives, neg_per_pos × P) / N_neg)`,
  where P = positives at the tick (`neg_per_pos` 10, `min_negatives` 50).
  Decide each negative deterministically from
  `hash(snapshots.seed, snapshot_id, file_id) < q`, using a stable hash
  such as `blake2b`, never Python's `hash()`. Kept negatives get
  `sample_weight = 1/q`; positives get 1.
- **Val/test:** never sampled. All rows are written with weight 1.

### 8.4 Output layout

```text
<out>/
  manifest.json                     # §8.6
  config.resolved.yaml
  feature_schema.json               # §5.2 registry export
  snapshots/split=<train|val|test>/volume_id=<v>/part-00000.parquet
  universe/volume_id=<v>/part-00000.parquet
  artifacts/coaccess.parquet        # frozen val/test table (§8.2)
  report/summary.md
  report/stats.json
```

### 8.5 Schemas

**`snapshots`** (one row per tick × candidate with a known label):

| Field | Type | Description |
| --- | --- | --- |
| `snapshot_id` | string | `<volume_id>@<t ISO>` |
| `t` | timestamp | Prediction time. |
| `split` | enum | `train`, `val`, `test` |
| `volume_id`, `file_id` | string | |
| `candidate_sources` | list<string> | §6 |
| `coaccess_fit_end` | timestamp | Nullable; which co-access table was used. |
| *features* | per `feature_schema.json` | §5.2 |
| `label` | int8 | 1 / 0 |
| `label_first_time` | timestamp | Nullable |
| `label_n_opens` | int32 | 0 for negatives |
| `label_distinct_bytes`, `label_max_file_size` | int64 | 0 for negatives |
| `label_bytes_exact` | bool | |
| `sample_weight` | float64 | §8.3 |

**`universe`** (one row per tick, including dropped and empty ticks):

| Field | Description |
| --- | --- |
| `snapshot_id`, `t`, `split` | `split` is null for dropped ticks. |
| `status` | `ok`, `warmup`, `dropped_boundary`, `dropped_period_end` |
| `n_candidates`, `n_ineligible` | §6 |
| `n_positive_files` | Positive files that were **eligible and known at *t*** (§6), whether candidates or not. |
| `n_positive_in_candidates` | Of those, how many were candidates. |
| `positive_bytes`, `positive_bytes_in_candidates` | Same, by `label_distinct_bytes`. |
| `n_positive_unknown_at_t` | Positives not known at *t* (e.g. created later): the cold-start limit, reported separately. |
| `n_positive_ineligible` | Positives known but ineligible at *t*. |
| `n_unknown_labels` | Candidate rows dropped as unknown. |

- **Candidate coverage** = `n_positive_in_candidates / n_positive_files`.
- **Evaluation recall** must use `n_positive_files` as the denominator, so
  a narrow candidate set can't inflate it. Report
  `n_positive_unknown_at_t` alongside.
- **Non-candidate labels:** computing `n_positive_files` requires labelling
  positives among non-candidates. The builder does this from `opens` in W
  directly, not by scoring every file.

### 8.6 Manifest (snapshots)

Same shape as §4.7, with `stage: "snapshots"`, plus:
- `parent` = cleaned dataset;
- `feature_version`;
- `labels` (target, lead, horizon, cold gap);
- `candidates` config;
- `splits` (actual boundaries);
- `coaccess` (refit interval, train refit count, frozen fit window, edge
  count);
- counts per split (ticks, rows, positives, unknown dropped, ticks dropped
  per status);
- mean candidate coverage per split;
- **`serving_contract`**: `{"config": …, "sha256": …}`. `config` is the
  canonical resolved configuration of everything that affects feature
  values, eligibility or candidates:
  - the `clean` section (incl. `read_mode`, `completion_grace_s`,
    `handle_timeout_s`, `dedupe_window_s`);
  - the `features`, `coaccess` and `candidates` sections;
  - the catalog policy (§3.1);
  - `feature_version`.

  `sha256` is computed over its canonical JSON (sorted keys, no
  whitespace). Model packages copy it, and the Controller enforces it (ML
  spec §5, §8.6). Snapshot-only settings (ticks, splits, sampling, labels)
  are recorded separately and are not part of the contract.

## 9. Configuration

```yaml
clean:
  max_future_skew_s: 3600
  max_late_s: 604800
  completion_grace_s: 60
  handle_timeout_s: 3600
  read_mode: ranges             # ranges | summary; declared per source (§4.5)
  dedupe_window_s: 21600        # live only (ML spec §8.2); batch dedupes globally
  coverage:
    max_loss_fraction: 0.0
    skew_margin_s: 600
    assume_complete_without_counters: false
  timezone_overrides: {}        # volume_id -> IANA tz when no catalog
features:
  version: fv1
  time: {enabled: false}
coaccess:
  delta_s: 300
  min_support: 3
  min_lift: 1.5
  max_out_edges: 50
  train_refit_every_s: 86400
candidates:
  recent_window_s: 3600
  max_candidates: 500
  caps: {recent: 200, neighbor: 200, project: 200, popular: 100}
  allow_dirs: null
labels:
  target: demand                # demand | cold_open
  lead_s: 0
  horizon_s: 300
  cold_gap_s: 86400
snapshots:
  seed: 7
  tick_s: 900
  warmup_s: 86400
  splits: {val_start: null, test_start: null, fractions: [0.6, 0.2, 0.2]}
  neg_per_pos: 10
  min_negatives: 50
```

One YAML file may hold both stages; each command reads its own sections and
writes the resolved config into its output.

## 10. Interfaces (for parallel work)

```python
# src/predictive_cache/prep/clean/pipeline.py
def run_clean(input_dir: Path, cfg: CleanConfig, out_dir: Path) -> Manifest: ...

# src/predictive_cache/prep/clean/rules.py      (shared with the live Controller, ML spec §8.2)
def classify_event(event: dict, now_or_ingest: datetime, cfg: CleanConfig) -> str | None: ...
# returns an exclusion reason (§4.3 rules 1 and 3–8) or None; duplicates are checked by the caller

# src/predictive_cache/prep/clean/handles.py    (shared with the live Controller, ML spec §8.2)
class HandleTracker:
    def __init__(self, cfg: CleanConfig): ...
    def push(self, event: dict) -> list[Update]: ...       # kept events in (known_at, event_id) order;
                                                          # returns OpenObserved / MetaChanged now
    def advance(self, now: datetime) -> list[Update]: ...  # emits OpenCompleted whose completed_known_at <= now
    def flush(self) -> list[Update]: ...                   # end of input: complete all open handles
    late_reads_after_completion: int

# src/predictive_cache/prep/coverage.py
class Coverage:
    @classmethod
    def load(cls, clean_dir: Path) -> "Coverage": ...      # coverage table + manifest period + margin
    def is_observed(self, volume_id: str, a: datetime, b: datetime) -> bool: ...   # §4.4

# src/predictive_cache/prep/catalog.py  (initial state only, §3.1)
class InitialCatalog:
    @classmethod
    def load(cls, clean_dir: Path) -> "InitialCatalog | None": ...
    period_start: datetime
    def files(self, volume_id: str) -> pa.Table: ...      # file_id, dir_id, size, created_at, kind
    def project_of_dir(self, dir_id: str) -> str | None: ...
    def parent_of_dir(self, dir_id: str) -> str | None: ...
    def timezone(self, volume_id: str) -> str | None: ...

# src/predictive_cache/prep/features/updates.py
@dataclass(frozen=True)
class OpenObserved:
    open_id: str; volume_id: str; file_id: str; dir_id: str
    session_key: str | None; cohort_id: str | None; app: str | None
    open_time: datetime; known_at: datetime; file_size: int

@dataclass(frozen=True)
class OpenCompleted:
    open_id: str; volume_id: str; file_id: str; known_at: datetime
    bytes_read: int | None; distinct_bytes_read: int | None
    full_read: bool | None; consumed: bool | None

@dataclass(frozen=True)
class MetaChanged:
    volume_id: str; file_id: str; dir_id: str; operation: str
    effective_time: datetime; known_at: datetime; file_size: int

Update = OpenObserved | OpenCompleted | MetaChanged
def updates_from_clean(clean_dir: Path, volume_id: str | None = None) -> Iterator[Update]: ...

# src/predictive_cache/prep/features/store.py
class FeatureStore:
    def __init__(self, cfg: FeatureConfig, catalog: InitialCatalog | None): ...
    def apply(self, u: Update) -> None: ...                # ValueError if u.known_at < watermark
    @property
    def watermark(self) -> datetime | None: ...
    def candidates(self, volume_id: str, t: datetime, coaccess: "CoAccessTable | None") -> pa.Table: ...
    # delegates to generate_candidates(self.view(volume_id, t), ...)
    def view(self, volume_id: str, t: datetime) -> "CandidateView": ...   # raises if watermark >= t
    def features_as_of(self, volume_id: str, t: datetime, file_ids: Sequence[str],
                       coaccess: "CoAccessTable | None") -> pa.Table: ...
    def checkpoint(self) -> bytes: ...
    @classmethod
    def restore(cls, data: bytes, cfg: FeatureConfig, catalog: InitialCatalog | None) -> "FeatureStore": ...

class CandidateView(Protocol):            # implemented by #30; read-only; answers as of t
    def recent_files(self, window_s: int) -> list[tuple[str, datetime]]: ...      # (file_id, last open_time), newest first
    def active_dirs(self, window_s: int) -> list[str]: ...                        # dirs with a successful open in window
    def files_in_dir(self, dir_id: str) -> list[str]: ...                         # known, current members
    def open_counts(self, file_ids: Sequence[str], window: Literal["1h", "24h", "7d"]) -> list[int]: ...
    def top_files(self, window: Literal["24h", "7d"], k: int) -> list[tuple[str, int]]: ...
    def is_known(self, file_id: str) -> bool: ...
    def is_eligible(self, file_id: str) -> bool: ...                              # §6; allow_dirs applied by caller

# src/predictive_cache/prep/features/candidates.py   (#31)
def generate_candidates(view: CandidateView, volume_id: str, t: datetime,
                        coaccess: "CoAccessTable | None", cfg: CandidateConfig) -> pa.Table: ...

# src/predictive_cache/prep/features/coaccess.py
class CoAccessTable:
    @classmethod
    def fit(cls, opens: pa.Table, fit_start: datetime, fit_end: datetime, cfg: CoAccessConfig) -> "CoAccessTable": ...
    fit_end: datetime
    def out_edges(self, file_id: str) -> list[tuple[str, int, float, float]]: ...   # (to, support, conf, lift)
    def in_edges(self, file_id: str) -> list[tuple[str, int, float, float]]: ...    # (from, support, conf, lift)
    def to_parquet(self, path: Path) -> None: ...
    @classmethod
    def from_parquet(cls, path: Path) -> "CoAccessTable": ...

# src/predictive_cache/prep/labels.py
class LabelRule(Protocol):
    name: ClassVar[str]
    def label(self, volume_id: str, t: datetime, file_ids: Sequence[str]) -> pa.Table: ...
    # columns: file_id, label (1/0/null=unknown), label_first_time, label_n_opens,
    #          label_distinct_bytes, label_max_file_size, label_bytes_exact
    def positives(self, volume_id: str, t: datetime) -> pa.Table: ...   # all positive files in W (for universe)
def make_label_rule(cfg: LabelConfig, clean_dir: Path, coverage: Coverage) -> LabelRule: ...

# src/predictive_cache/prep/snapshots/builder.py
def build_snapshots(clean_dir: Path, cfg: SnapshotConfig, out_dir: Path) -> Manifest: ...
```

Builder flow per volume:
1. Load coverage, catalog and opens. Fit the train refits and the frozen
   val/test co-access tables.
2. Create the store, then for each tick: apply updates with `known_at < t`;
   pick the co-access table for *t* (§8.2); candidates; features; labels and
   positives.
3. Apply boundary and period-end rules, then sampling.
4. Write output.

## 11. CLI

```text
pcache-prep clean --input DIR --out DIR [--config FILE] [--format parquet|jsonl] [--overwrite]
pcache-prep snapshots --input CLEAN_DIR --out DIR [--config FILE] [--overwrite]
pcache-prep describe DIR        # prints manifest summary + report/summary.md
```

## 12. Tests and acceptance

### 12.1 Cleaning (#23)

- **Per-rule unit tests** (§4.3). These cover first-match-wins (a malformed
  `einval` read gets `invalid_range`) and the partition: every input row
  ends up in exactly one of kept or excluded.
- **Reconstruction fixtures** (§4.5):
  - missing close;
  - missing open;
  - `read_mode: summary` (datagen `read_events: none`);
  - `read_mode: ranges` with a range read delivered after
    `close_known_at + completion_grace_s`: it is excluded from byte facts,
    and the close total is never substituted;
  - overlapping ranges (distinct bytes counted once);
  - a close total that disagrees with the kept reads after a failed read
    (`bytes_inconsistent`, read-derived bytes kept);
  - every read of a handle failed: `bytes_read = 0`, `consumed = false`,
    the close total is not used;
  - failed open;
  - null `handle_id` reads (separate orphan groups);
  - rename keeps `file_id`; replace changes it;
  - an unknown `file_id` is kept and counted as a catalog miss.
- **Coverage fixtures:** a loss window next to an hour boundary with ±skew
  (margin expansion); windows outside the period are unobserved;
  `no_counters`.
- **Truth-based accuracy on datagen `small`.** Truth is read in tests only.
  Required results:
  - 100% of `warmer`/`synthetic`/`unknown`-origin events are excluded with
    the matching origin reason;
  - for every duplicated `event_id`, exactly one canonical delivery is kept
    and all extra deliveries are excluded as `duplicate`;
  - 100% of injected invalid ranges are excluded as `invalid_range`;
  - false exclusion of valid foreground events is ≤ 0.1%;
  - every hour overlapping a `known_gaps` window is not `complete`;
  - with `identity_break` enabled, events are kept and catalog misses are
    reported.
- **Determinism:** the same input and config give identical data-file
  hashes.
- **Reconciliation:** quality report counts match the manifest.

### 12.2 Features, labels, snapshots (#24)

- **Prefix equivalence (property test):** for random update streams and
  ticks *t*, `features_as_of(t)` on a store fed only the `known_at < t`
  prefix equals the result on a store fed the same prefix in a fresh
  replay. Feeding any update with `known_at ≥ t` and then querying *t*
  raises.
- **No backdating:**
  - an open known before *t* whose zero-byte close is known after *t*: byte
    features at *t* don't reflect the close, and at a later
    *t′* > `completed_known_at` they do;
  - a read delivered after its close but within the grace period is part of
    the open's byte facts, with `completed_known_at = close_known_at +
    completion_grace_s`. A read delivered after that is
    late-after-completion: counted, excluded from byte facts, kept in
    `open_ranges`.
- **Catalog isolation:**
  - a file created after *t* is not visible;
  - a file that exists at *t* with a future catalog `deleted_at` stays
    visible and eligible until a delete `MetaChanged` with `known_at < t`.
    Catalog `deleted_at` is never consulted;
  - a rename after *t* doesn't move the file's directory at *t*.
- **Checkpoint parity:** replay with checkpoint/restore at random points
  gives features equal to an uninterrupted replay.
- **Fit isolation:**
  - adding opens with `open_known_at ≥ fit_end` leaves the co-access table
    unchanged, including old-`open_time` opens delivered late;
  - train ticks never use a fit with `r_k > t`;
  - val/test use only the frozen table.
- **Labels:**
  - demand vs `cold_open` fixtures;
  - positives kept in unobserved windows;
  - `cold_open` with an unobserved lookback becomes unknown;
  - a long handle whose open is before W but whose reads fall in W is
    positive, counting only bytes read in W;
  - a handle opened in W with all reads after W is not positive from those
    reads;
  - a label fact (range read, or summary-mode completion) in W with
    `known_at` after the next split boundary makes the label unknown, in both
    range and summary mode, including facts delivered after `val_start` and
    `test_start`;
  - ticks whose window plus skew embargo crosses a split boundary, or whose
    window passes `period.end`, are dropped. Include a ±max-skew fixture on
    both sides of each boundary;
  - grow/shrink writes inside W keep
    `label_distinct_bytes ≤ label_max_file_size`.
- **Universe:** positives created after *t* go to
  `n_positive_unknown_at_t`, not the coverage denominator.
- **Sampling:** val/test are never sampled; weighted negative counts
  reproduce the unsampled count within 5% on `small`. The same seed gives
  the same rows.
- **Planted patterns** use controlled datagen fixtures with truth read in
  tests only:
  - **P3:** a P3-only scenario with faults off,
    `interruption_p = 0`, `context_order = 1`, and inter-open delays inside
    `coaccess.delta_s`. Each planted edge with enough support (≥ 20 entry
    opens) is present in the frozen table with `lift ≥ 3`, aligned with
    datagen §13.
  - **P5:** a review-burst scenario. During the burst, the shared files'
    `growth_15m_vs_1h` and `distinct_sessions_1h` are higher than in the
    hour before.

### 12.3 Performance targets (record measured values)

- **`features_as_of`:**
  - state from replaying datagen `small` through 6 weeks;
  - 500 candidates; warm store; 20 repetitions;
  - excludes candidate generation;
  - **p50 < 100 ms, p95 < 200 ms** on the developer machine class used for
    the run (record CPU model and Python version).
- **End to end:** `clean` + `snapshots` on `small` (~5k ticks × ≤ 500
  candidates) runs in **< 30 min** on the same machine. If it doesn't,
  report the measured time and the dominant stage.

**CI scope:** unit, fixture, leakage and parity tests run in CI on `tiny`
and hand-built fixtures. The `small` checks (truth-based accuracy, planted
patterns, performance) run in a documented local or nightly suite.

Synthetic results above prove plumbing and planted-pattern recovery only.
They are never model-quality evidence.

## 13. Code layout and stack

```text
src/predictive_cache/prep/
  __init__.py
  config.py                 # CleanConfig, FeatureConfig, CoAccessConfig, LabelConfig, SnapshotConfig (§9)
  catalog.py                # InitialCatalog (§3.1)
  coverage.py               # Coverage.is_observed (§4.4)
  clean/                    # #27, #28, #29
    rules.py  opens.py  report.py  pipeline.py  schema.py
  features/                 # #30, #31
    updates.py  store.py  registry.py  coaccess.py  candidates.py  checkpoint.py
  labels.py                 # #32
  snapshots/                # #32
    builder.py  schema.py  report.py
  cli.py                    # pcache-prep
tests/prep/                 # fixtures as small JSONL (datagen §3 layout)
```

Same stack as datagen (datagen spec §14): Python 3.12, `uv`, `numpy`,
`pyarrow`, `pydantic` v2, `pyyaml`, `pytest`, `ruff`; `hypothesis` for the
property tests. Reuse datagen's schema and IO modules for reading events.
Put shared helpers (manifest hashing, partitioned writers) in
`src/predictive_cache/common/` when a second user needs them. Never commit
generated datasets; only tiny JSONL fixtures under `tests/`.

## 14. Work breakdown

| Issue | Story | Scope | Spec sections |
| --- | --- | --- | --- |
| [#26](https://github.com/rahulgtt/predictive-cache/issues/26) | #23 | This spec | all |
| [#27](https://github.com/rahulgtt/predictive-cache/issues/27) | #23 | Cleaning package, config, exclusion rules, cleaned events, manifest, CLI | §3, §4.1–4.3, §4.6–4.7, §9–11, §13 |
| [#28](https://github.com/rahulgtt/predictive-cache/issues/28) | #23 | Coverage (incl. `Coverage`), opens, open_ranges, meta, initial catalog view | §3.1, §4.4–4.6 |
| [#29](https://github.com/rahulgtt/predictive-cache/issues/29) | #23 | Quality report and truth-based accuracy tests | §4.8, §12.1 |
| [#30](https://github.com/rahulgtt/predictive-cache/issues/30) | #24 | Feature store: updates, fv1, as-of contract, `CandidateView`, checkpoint | §3, §5.1–5.3, §5.5, §6 (view), §9, §10 |
| [#31](https://github.com/rahulgtt/predictive-cache/issues/31) | #24 | Co-access table and `generate_candidates` over `CandidateView` | §5.4, §6, §10 |
| [#32](https://github.com/rahulgtt/predictive-cache/issues/32) | #24 | Labels, splits, co-access fit schedule and snapshot builder | §7, §8, §9, §11 |
| [#33](https://github.com/rahulgtt/predictive-cache/issues/33) | #24 | Leakage, parity, planted-pattern and performance tests | §12.2, §12.3 |
| [#34](https://github.com/rahulgtt/predictive-cache/issues/34) | #25 | Real source adapter and field-availability report (blocked) | §4, §15 |
| [#35](https://github.com/rahulgtt/predictive-cache/issues/35) | #25 | Datagen calibration from real data (blocked) | — |

**Order and parallelism:**
- #27 needs the datagen package scaffold (#13). #28 and #29 follow #27.
- #30 can start at once from hand-built update fixtures and a stub
  `InitialCatalog`.
- #31 codes against the `CandidateView` protocol (§10) with a fake view, so
  it can run in parallel with #30.
- #32 needs #28's `Coverage`/`open_ranges`, #30 and #31.
- #33 comes last.
- Everything runs on JSONL fixtures before datagen produces real output.

## 15. Open questions

- **Target:** demand (team plan) vs cold open (PRD). Both are implemented;
  the Evaluation story decides with validation evidence and must not tune on
  test.
- **Horizon and lead:** 300 s / 0 s until retrieval lead time is measured
  (team plan). Changing them needs no code change.
- **Co-access order 2** (A→B→C vs D→B→E): add `coacc2_*` features if order-1
  co-access misses datagen §7.1 order-2 structure on validation.
- **Co-access in production:** v1 freezes one table for val/test, per the
  team plan. The live Controller may later refit periodically like the
  train schedule; that needs an evaluation that mirrors it.
- **Multiple datasets / seeds:** the datagen handoff recommends distinct
  seeds and keeping correlated copies together. v1 builds one dataset per
  run; cross-dataset evaluation is up to the Evaluation story.
- **Real data:** cache domain, session/job keys, identity across
  rename/replace, loss counters, clock error bound (`skew_margin_s`) and
  timezone without a catalog. Tracked in #34; cleaning must accept nulls for
  anything the source lacks.
- **Range-level prediction:** later. `label_distinct_bytes` and the byte
  features keep the large-file / wait-time objective evaluable.
