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
