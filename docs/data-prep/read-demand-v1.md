# READ-demand semantic contract v1

`read-demand-v1` is the hackathon MVP semantic view for successful NFSv4.1
READ demand. It is an additive projection: generic datagen v1 remains
unchanged. The executable contract is
[`contracts/read-demand-v1.json`](../../contracts/read-demand-v1.json), with
Python enforcement in
[`predictive_cache.semantics.read_demand`](../../src/predictive_cache/semantics/read_demand.py).

| Contract value | Frozen value |
| --- | --- |
| Semantic view | `read-demand-v1` |
| Contract SHA-256 | `37d56855d690678e4394621ef53c74a07a71226aed9faca0bc6ad5c5c91df7f8` |
| Protocol / operation | NFSv4.1 successful `READ` only |
| Identity | `(volume_id, file_id)`; production rename/replacement semantics unresolved |
| Ordering | Features consume rows in `(known_at, event_id)` order |
| Event time | UTC READ completion time |
| Known-at time | UTC durable collector/import availability time |
| Units | offsets, request sizes, returned sizes and file sizes are bytes |
| Real-demand admission | trusted foreground origin plus complete coverage |
| Controlled collection | synthetic `controlled_demand`; plumbing evidence only |

> **Do not invent protocol lifecycle facts from temporal proximity when READ
> evidence cannot prove them.** OPEN/CLOSE and NFS session operations are
> Warmer mechanics, not demand events. `read-demand-v1` never reconstructs an
> open, close, handle lifetime or application session from READ timing.

## Field mapping

`origin` records traffic provenance. `demand_class` records whether a row may
participate in a particular evidence mode. They are intentionally separate:
a controlled READ remains `origin=synthetic` and
`demand_class=controlled_demand`.

| Raw source | Canonical field | Availability / nullability | Units / identity scope | Known at | Trust / origin | Loss behavior | Consumers |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Chimera `local_id`; datagen `event_id` | `event_id` | Required | Source-local dedupe ID | Delivery | Source evidence | Duplicate delivery rejected | Replay ordering, audit |
| `event_time_ns`; `event_time` | `event_time` | Required | UTC | READ completion | Producer clock; bounded separately | Row invalid if unusable | Feature windows, labels |
| observed `durable_at`; `ingest_time` | `known_at` | Required | UTC | Durable availability | Import/collector | Late rows cannot affect earlier ticks | Offline/live as-of |
| `volume_id` | `volume_id` | Required | Volume scope | Delivery | Validated identity | Missing/invalid blocks row | All features/packages |
| `file_id` | `file_id` | Required | Opaque ID within volume | Delivery | Chimera identity validation or fixture | Missing/invalid blocks row | Candidate identity |
| fixture/generator catalog | `dir_id` | Nullable | Opaque directory ID | Catalog as-of time | Fixture today; production unresolved | Missing stays unknown | Metadata-dependent features |
| `requested_offset`; `offset` | `offset` | Required | Bytes | Delivery | READ evidence | Invalid range rejects row | Distinct ranges |
| `requested_count`; `bytes_requested` | `bytes_requested` | Required | Bytes | Delivery | READ evidence | Invalid value rejects row | Range/accounting |
| `returned_bytes`; `bytes_read` | `bytes_read` | Required | Bytes returned | Delivery | Successful READ evidence | Must not exceed request | Bytes/popularity/labels |
| producer/catalog size | `file_size` | Nullable | Bytes | Producer or catalog as-of | Fixture/verified metadata only | Missing stays unknown; no imputation under same name | Size feature, bounds |
| source event | `origin` | Required enum | Provenance | Delivery | `foreground`, `warmer`, `synthetic`, `unknown` | Warmer/unknown excluded from real demand | Admission/evidence |
| source profile | `demand_class` | Required enum | Evidence mode | Projection | foreground, controlled, fixture, warmer, unknown | Unknown cannot be negative demand | Admission/evidence |
| source profile | `origin_trust` | Required enum | Trust level | Projection | trusted, fixture, client asserted, unknown | Untrusted cannot be real demand | Package/runtime gate |
| hourly counters/gaps | `coverage_status` | Required enum | Volume/hour window | Counter checkpoint | Complete/incomplete/unknown | Incomplete/unknown makes labels unknown | Labels/package gate |
| catalog join | `metadata_status` | Required enum | Row metadata | Join completion | verified, fixture, missing | Missing blocks required metadata features | Feature availability |
| source manifest | `data_kind` | Required | Dataset provenance | Manifest | real or synthetic | Synthetic cannot prove model quality | Evidence reporting |
| Contract constant | `protocol`, `operation` | Required constants | NFSv4.1 / READ | Projection | Contract | Any other value rejected | All consumers |

Chimera transport `session_ref`, file handle and compound tag remain diagnostic
source evidence. Datagen actor/session/project/pattern/handle fields remain in
the separate truth artifact. None is projected as an observed application
session or lifecycle fact.

## Projection and cleaner behavior

`pcache-datagen project-read-demand DATASET --out DIRECTORY` validates a
generic datagen dataset, then writes deterministic successful-READ rows and a
separate generator-truth file plus the source hourly coverage table. Each READ
uses its event-time coverage bucket; `known_at` controls visibility only. The
projection writes outside the source dataset, does not change datagen `Config`,
hashes or IDs, and leaves generic validation intact.

`pcache-prep clean --semantic-view read-demand-v1` branches after v1 schema
validation. It rejects non-READ operations, does not instantiate
`HandleTracker`, writes first-class `read_events`, and preserves empty
`opens/` and `open_ranges/` artifacts for compatibility. Generic cleaning is
unchanged.

## Feature and label registry

Feature names are versioned as `read-demand-fv1` and are disjoint from generic
`fv1`. Reusing an open/session/write feature name for READ semantics is a
contract error.

| Feature | Classification | Definition / missingness |
| --- | --- | --- |
| `read_count_{5m,15m,1h,24h,7d}` | Supported | Successful eligible READ count; zero only for completely observed windows |
| `returned_bytes_{5m,1h,24h,7d}` | Supported | Sum of returned bytes; unknown when required coverage is incomplete |
| `distinct_read_ranges_{1h,24h}` | Supported | Count of distinct `(offset, bytes_read)` ranges |
| `secs_since_last_read` | Supported | Tick minus latest known eligible READ; null if none |
| `read_days_7d` | Supported | Distinct UTC days with eligible READs |
| `volume_read_share_1h` | Supported | File READ count divided by volume READ count |
| `coread_from_anchor_24h` | Supported, context-dependent | Direct predecessor-to-target transitions only when the predecessor is in the same volume and within 300 seconds; null without an anchor |
| `file_size_bytes` | Metadata-dependent | Verified as-of metadata only; null/missing blocks packages that require it |
| Controlled planted patterns | Synthetic-only | Recovery/plumbing evidence; never real model-quality evidence |
| Open/close totals, handle durations, `cold_open`, session counts, writes | Out of scope | Loader/registry rejects them for `read-demand-v1` |

The default label is successful eligible READ demand inside the declared
future horizon. A negative is valid only after the complete horizon has
elapsed with complete coverage. Incomplete or unknown coverage produces an
unknown label, not zero.

## Co-read table and candidates

Generic data-prep §5.4 (co-access) and §6 (candidates) are defined over
opens, session keys and directories. `read-demand-v1` has none of these, so
under this view both are replaced by the READ-only rules below. Generic
behaviour is unchanged. #31 implements both.

**Visibility at tick *t*:** a row is visible iff `known_at ≤ t`. This is
the same boundary `ReadDemandEngine.features` and
`build_read_demand_feature_rows` use. Everything below (recency,
popularity, anchor, co-read edges) uses only visible rows, and any
`event_time` window also requires `0 ≤ t − event_time`. Future-skewed rows
are therefore never "recent".

**Eligible READ rows** are those the feature engine admits
(`ReadDemandEngine.update`). Rows with `origin` or `demand_class` `warmer` or
`unknown` never create recency, popularity, co-read edges or anchors. This
stops the Warmer's own reads from re-proposing the files it just warmed.

### Transition stream (shared with `coread_from_anchor_24h`)

Per volume, order eligible rows by `(event_time, event_id)`. A
**transition** is each pair of adjacent rows `(p, q)` in that order with
`0 ≤ q.event_time − p.event_time ≤ 300 s`. This is exactly the stream
`coread_from_anchor_24h` counts. The engine currently sorts by
`event_time` alone, so #31 must add `event_id` as the tie-breaker in
`ReadDemandEngine.features` so that both use one order.

- The feature counts every transition, including `A → A` (re-read) when
  the anchor is the candidate itself.
- The co-read table skips `A → A` transitions: a co-read edge is between
  different files.
- A **run** is a maximal sequence of adjacent rows of the same `file_id`.
  A non-self transition `A → B` is always a run boundary, so a chunked read
  of one large file adds one to the denominator below, not hundreds.

Client, session, handle and compound-tag fields are diagnostic only and
are **not** used to split the order.

### `CoReadTable` (replaces `CoAccessTable` under this view)

`CoReadTable.fit(read_events, fit_start, fit_end, cfg)` uses only eligible
rows with `known_at < fit_end` and `fit_start ≤ event_time < fit_end`, and
the transitions and runs built from them:

1. `n(A)` = runs of A. `N` = all runs in the fit window.
2. `support(A → B)`, `B ≠ A` = transitions from the last row of an A-run to
   the first row of a B-run.
3. `conf(A → B) = support(A → B) / n(A)`. Each run ends in at most one
   non-self transition, so `Σ_B conf(A → B) ≤ 1`.
4. `base(B) = Σ_{X ≠ B} support(X → B) / N` (≤ 1); `lift = conf / base(B)`.
5. Thresholds, top-K and ties are the generic `coaccess.min_support` (3),
   `coaccess.min_lift` (1.5) and `coaccess.max_out_edges` (50), ties
   broken by `to_file_id`. `coaccess.delta_s` is fixed at 300 for this
   view, and a config that sets another value is rejected.
6. The Parquet columns are the same as generic (`from_file_id`,
   `to_file_id`, `support`, `n_from`, `conf`, `lift`), so the `out_edges` /
   `in_edges` interface is unchanged. The snapshot manifest also records
   `coaccess_kind: "coread-direct-v1"`.

Fits map to ticks exactly as in data-prep §8.2 (past-only fits).

**Known limitation:** with many clients interleaving reads on one volume,
the direct successor is often another user's file, which dilutes `conf`.
This is accepted for the MVP. A trusted per-client key would need a
reviewed contract change; it is not inferred from `client_ref` or
`session_ref`.

### Anchor

The anchor for `(volume_id, t)` is the `file_id` of the latest visible
eligible READ in the volume (by `(event_time, event_id)`), provided
`0 ≤ t − event_time ≤ 300 s`. Otherwise it is null. Every candidate row of that tick carries this
`anchor_file_id`, and the engine computes `coread_from_anchor_24h` from it.
A null anchor makes the feature null, not zero. A package may require the
feature only when the snapshot manifest declares
`runtime_capabilities.anchor_context: true`, which means the Controller
computes the anchor by this same rule.

### Narrow offline MVP profile

Issue #112 adds the deliberately distinct
`read-demand-v1-narrow-mvp` inference-only profile for controlled synthetic
integration. It is not the canonical complete candidate profile above. It
uses only visible eligible `controlled_demand` and `fixture_demand` READ
identities, requires complete `read_count_24h` coverage, and orders candidates
by 24-hour count descending, recency, `file_id`, then `volume_id`. Incomplete
required coverage omits the identity with
`required_coverage_incomplete`; it never converts missing evidence to zero.

The profile declares `verified_file_size=false` and
`anchor_context=false`. It therefore cannot load file-size or
`coread_from_anchor_24h` package requirements and has no OPEN/CLOSE,
application-session, directory, project, lifecycle, write, or `cold_open`
candidate source. Its profile and snapshot-serving identities are separately
hashed and bound into the non-trained synthetic baseline package. See the
[offline integration runbook](../experiments/datagen-to-warmer-mvp.md).

### Candidate sources

`CandidateView` keeps its generic signature. Under this view its methods
are answered from eligible READ rows:

| Generic method | `read-demand-v1` meaning |
| --- | --- |
| `recent_files(window_s)` | Files with a visible eligible READ with `0 ≤ t − event_time ≤ window_s`, newest first by last READ |
| `top_files(window, k)` | Files ranked by eligible READ count in the window (ties by `file_id`) |
| `open_counts(…)` | Eligible READ counts (the method name is kept for the interface only) |
| `active_dirs`, `files_in_dir` | Unavailable: `dir_id` is not verified (see Remaining production dependencies). They return empty lists |

| Source | Rule | Cap |
| --- | --- | --- |
| `recent` | `recent_files(candidates.recent_window_s)` (3600 s) | 200 |
| `neighbor` | `CoReadTable.out_edges(anchor)` ranked by `conf`; none when the anchor is null | 200 |
| `project` | Disabled: needs verified `dir_id` | 0 |
| `popular` | `top_files("24h", k=100)` | 100 |

- **Union and cap** follow generic §6: dedupe, keep all sources per file,
  cap at `candidates.max_candidates` (500), and keep `recent`, then
  `neighbor`, then `popular`. Ties are broken by `file_id`.
- **Known at *t*:** the file has a visible eligible READ, or is in a
  verified or fixture catalog as of *t*.
- **Eligible:** known, latest known `file_size` is not `0`, and inside
  `candidates.allow_dirs` only when `dir_id` is verified. An unknown size
  (`file_size_valid=false` or missing) is **eligible**. The planner bounds
  range length separately.
- **No deletion signal.** READ telemetry cannot see deletes or renames, so
  stale candidates are not filtered here. Offline, a file never read again
  is simply a negative. Live, the target map's age limit and the
  dispatcher's stale-handle feedback (ML spec §8.11) bound the cost.
- **Coverage:** when the volume's coverage is incomplete, `recent` and
  `popular` may be partial. Labels are already unknown in that case.
  Coverage is still measured in the snapshot `universe` table, not inferred.
- **Provenance:** the snapshot manifest records `candidate_profile:
  "read-demand-v1"` and the enabled sources, so a package cannot be
  evaluated against a different candidate rule silently.

## Snapshot, package and serving gate

Legacy package version 1 and its payload file set remain unchanged. Legacy
manifests without semantic keys retain generic-v1 behavior. A package that
declares `read-demand-v1` must bind all of:

- `semantic_view_id` and `semantic_view_sha256`;
- `feature_schema_sha256` for the exact `read-demand-fv1` registry;
- `serving_contract_sha256`;
- `target=demand` and the 300-second direct-predecessor co-read definition;
- every required feature in the runtime availability set.

The loader refuses missing hashes, semantic or feature-name mismatches,
unavailable metadata, forbidden lifecycle features and `cold_open`. Snapshot
manifests declaring the semantic view require the same bindings. Offline
replay and incremental live updates use the same feature engine; the fixed-tick
parity test therefore checks one shared implementation fed through both APIs.

## Remaining production dependencies and approval gates

- Production `dir_id`, `file_size`, rename/replacement identity and safe
  resolution semantics are unresolved. Lab fixture catalogs do not close this.
- A trusted production foreground-origin source is unresolved. Client-asserted
  controlled tags are not authenticated foreground evidence.
- Loss counters and durable known-at evidence are mandatory. Unknown observed
  dispositions block controlled import.
- Integration may proceed only after remote `nas-vm` tests, Ruff, a fresh
  blocker-only review, and reconciliation with the datagen workload contract.
- Live reads, deployment, collection, commit, push and PR publication require
  their separately stated approvals.
