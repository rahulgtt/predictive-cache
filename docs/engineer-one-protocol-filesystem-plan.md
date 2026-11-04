# Engineer 1 protocol/filesystem implementation record

**Status:** Current implementation and evidence record, reconciled against
`main` at `05d51b602e3d1a16fb9a8e11cf87c4183977a512` plus the reviewed
workload-v3 integration on 2026-11-04.

**Owner:** Engineer 1 - protocol/filesystem.

**Authoritative boundary:** the hackathon MVP observes and executes successful
NFSv4.1 `READ` operations only. `OPEN` and `CLOSE` remain necessary protocol
mechanics, but they are not demand telemetry. `WRITE`, `CREATE`, mutation,
NFSv3, SMB, S3, and generic filesystem-operation prediction are outside the
MVP.

This document replaces the earlier chronological plan with a reconciled
implementation record. Historical handoff hashes remain for provenance, but
merged repository state and verified runtime outcomes take precedence.

## Reader navigation

- [Current status](#1-current-status)
- [Scope and safety boundary](#2-scope-and-safety-boundary)
- [Responsibilities and dependencies](#3-responsibilities-and-dependencies)
- [Implemented architecture](#4-implemented-architecture)
- [Component implementation details](#5-component-implementation-details)
- [Verified collection and training results](#6-verified-collection-and-training-results)
- [Ten work groups and final outcomes](#7-ten-work-groups-and-final-outcomes)
- [PR state and validation record](#8-pr-state-and-validation-record)
- [Historical handoffs versus final state](#9-historical-handoffs-versus-final-state)
- [Remaining gates and limitations](#10-remaining-gates-and-limitations)
- [Datagen-driven three-hour READ workload](#11-datagen-driven-three-hour-read-workload)
- [Canonical references](#12-canonical-references)

## 1. Current status

| Surface | Current result | Evidence boundary |
| --- | --- | --- |
| Chimera READ event producer | Reviewed external `nfs4_read_v2` patch and repository-safe provenance are tracked under `chimera/`. | The patch is not applied by this repository. It records protocol decisions, not client receipt or cache benefit. |
| Reliable event sink | Disposable-lab rollout passed one-event durability, ordered sequence, drained barrier, and zero-drop checks. | Per-node HostPath is lab evidence, not a production storage design. |
| Controlled collection | `pcache-collect` completed a five-hour, 300-operation, Warmer-disabled NFSv4.1 READ run. | The workload is controlled synthetic plumbing evidence, not representative demand. |
| Chimera importer | `pcache-chimera-import` strictly reconciles raw events, durable sidecars, source envelopes, offered operations, catalogs, and checkpoints. | Missing, duplicate, inconsistent, or unexplained evidence fails closed. |
| Production cleaner | All 300 controlled events were imported and all 300 were excluded as synthetic; zero demand events were retained. | This is the required policy result, not a failed collection. |
| READ demand semantics | `read-demand-v1` is merged across datagen projection, Chimera import, cleaning, feature computation, snapshots, packages, training, and evaluation. | Successful READ facts only; lifecycle inference is rejected and production metadata remains a gap. |
| Planted workload v3 | A deterministic three-hour, 165-READ offline workload plan, truth, simulated source, import, clean, and recovery path is implemented. | Offline synthetic plumbing only; all live, permit, attestation, reconciliation, and finalization entry points reject v3. |
| Dispatcher | `pcache-dispatcher` securely claims schema-v2 plans, persists task state and evidence, enforces limits, and invokes one Warmer process at a time. | Automated Controller integration and live efficacy are still gated. |
| Warmer | Experimental standalone NFSv4.1 range reader is implemented and tested; one separately authorized path-mode 4 KiB read completed. | Handle-mode live interoperability, cache residency, and performance benefit remain unverified. |
| CUFS integration | No CUFS source patch is active. The PoC uses the historical CUFS runtime/development package pair and a build-only compatibility wrapper. | Birth-time semantics are intentionally not claimed. |
| Historical evidence disposition | Historical finalization selected `retain_all` for all 13 declared roots, with `delete_by: null` and no deletion inventory or cleanup schedule. | [PR #97](https://github.com/rahulgtt/predictive-cache/pull/97) merged the final package-snapshot and retention wording. |
| Predictive cache benefit | Not established. | Equivalent cold state, per-range cache evidence, unrelated-foreground guardrails, and matched control/treatment trials remain open. |

The project is therefore an implemented data-and-execution foundation, not a
validated predictive-cache service. The Controller, automated model-to-Warmer
loop, representative demand collection, and performance claim remain gated.

## 2. Scope and safety boundary

### 2.1 Foreground independence

Foreground reads never wait for event collection, prediction, model state,
the Dispatcher, or the Warmer. Event export uses a bounded nonblocking
foreground enqueue and an off-path worker. Failure, saturation, or shutdown
must preserve the foreground READ result.

### 2.2 READ-only execution

- The controlled harness opens an already-mounted read-only NFSv4.1 export.
- Live collection uses aligned 4 KiB `O_DIRECT` reads with no buffered
  fallback.
- Dispatcher plan items may specify only `operation: "read"`; omission means
  the same fixed READ operation.
- The Warmer uses read-only `OPEN`, `READ`, and `CLOSE` protocol operations.
- No plan, task, or workload schema accepts file-content mutation.

`OPEN` and `CLOSE` may occur on the wire because NFS requires them. Chimera
emits only READ decision events for this MVP; those lifecycle operations are
never captured or inferred as demand telemetry.

### 2.3 Data-quality boundary

- Warmer, synthetic, and unknown-origin events are excluded from demand
  features, labels, and evaluation.
- Client-selected COMPOUND tags are correlation evidence, not authenticated
  origin.
- Missing capture is never treated as a negative label.
- Synthetic data proves contracts and plumbing only.
- Predictive state is never written to CUFS volume Kvrocks.

Schema-v1 compatibility is not sufficient for the live Chimera shape. The
chosen MVP is the merged, versioned **reads-only training/evaluation
projection** `read-demand-v1` from
[PR #107](https://github.com/rahulgtt/predictive-cache/pull/107).
Features and labels may use successful READ identity, range, returned bytes,
request time, durable availability time, order, and explicit loss evidence.
OPEN/CLOSE lifecycle, sessions, and handle-derived lifecycle features are
disabled and deferred. The controlled lab catalog supplies `dir_id` and
`file_size`; a trustworthy production metadata source remains an open gap.

**Do not invent protocol lifecycle facts from temporal proximity when READ evidence cannot prove them.**
This applies to OPEN, CLOSE, session, and handle inference. READ co-occurrence
may be described only as co-read timing, not lifecycle proof.

### 2.4 Privacy boundary

Repository documentation may contain opaque public-safe hashes, schemas,
counts, and sanitized provenance. It must not contain owner-private paths,
credentials, IP addresses, executable handles, raw file handles, raw UUIDs,
or pod, client, and session identities.

Authorized users receive the private package root through the owner-mediated
`PCACHE_DATASET_ROOT` handoff documented in
[`owner-private-controlled-dataset.md`](data-prep/owner-private-controlled-dataset.md).
Dataset bytes and private evidence remain outside Git.

## 3. Responsibilities and dependencies

| Owner | Responsibility | Inputs | Outputs / dependency |
| --- | --- | --- | --- |
| Engineer 1: protocol/filesystem | Chimera event producer, sink semantics, collection harness, source adapter, Dispatcher, Warmer, filesystem validation, and this record. | Approved NFS export, frozen catalog, repository contracts, model/planner task contract. | Durable READ evidence, normalized synthetic events, bounded execution results, and explicit blockers. |
| Engineer 2: ML | Features, training, scoring, model packages, baselines, and offline evaluation. | Cleaned demand snapshots with chronological boundaries. | Ranked opaque file/range candidates and frozen model evidence. |
| Engineer 3: data/telemetry | Event transport, quality, loss accounting, normalization, and authorized data access. | Chimera source events and collector checkpoints. | Versioned datasets, quality reports, coverage, and origin exclusions. |
| Engineer 4: performance | Cold-state protocol, matched trials, foreground/pollution guardrails, and reproduction. | Bounded plans, cache-state evidence, and controlled workload. | Pass/fail/inconclusive mechanism and performance decisions. |

### Dependency order

1. A reviewed Chimera event contract and reliable sink establish what was
   observed.
2. The provider and collection harness establish durable availability and
   offered-operation reconciliation.
3. The importer and cleaner establish whether records are eligible demand.
4. The model/planner produces opaque candidates and private executable tasks.
5. The Dispatcher and Warmer execute bounded READs.
6. Independent cache and performance evidence determines whether execution
   helped later foreground reads.

No downstream layer may repair an upstream identity, origin, sequence, or
loss ambiguity by inference.

## 4. Implemented architecture

```text
Authorized read-only NFSv4.1 fixture export
        |
        v
pcache-collect
  - absolute monotonic schedule
  - write-ahead offered ledger
  - aligned 4 KiB O_DIRECT preadv
  - execution-result ledger
        |
        +-------------------------------+
        |                               |
        v                               v
private provider bridge          Chimera NFSv4.1 READ path
  - checkpoint                     protocol -> VFS -> CUFS
  - pre-read probe                         |
  - post-read capture                      v
  - exact source bytes              reliable per-node JSONL sink
  - durable sidecar                       |
        |                                  |
        +----------------+-----------------+
                         v
                canonical observed events
                         |
                         v
               pcache-chimera-import
                         |
                         v
                  pcache-prep clean
                         |
              +----------+-----------+
              |                      |
              v                      v
       excluded synthetic      retained demand
           300 rows               0 rows
```

The warming path is separate:

```text
model/planner candidate
        |
        v
owner-only schema-v2 plan
        |
        v
pcache-dispatcher
  - atomic claim
  - deadline/budget/rate/stop checks
  - durable running/terminal state
        |
        v
owner-only single-task file
        |
        v
pcache-warmer
  - one supervised process
  - bounded NFSv4.1 READ
  - structured attempt/result evidence
        |
        v
Dispatcher terminal result
```

The model never receives a mutable path or endpoint. The trusted planner and
dispatcher own private target resolution and execution configuration.

## 5. Component implementation details

### 5.1 Chimera reliable READ-event sink

The reviewed external patch is based on an evidence-qualified historical
Chimera candidate, not a proven release source. It emits strict
`nfs4_read_v2` JSONL records with:

- producer epoch and one-based producer sequence;
- NFSv4.1 operation identity and compound operation index;
- opaque volume and file identity;
- export identity and private executable target evidence;
- requested offset/count and returned bytes;
- NFS status, outcome, EOF, and validated file-size evidence;
- bounded tag evidence and client-asserted origin claim.

The foreground path uses a bounded queue and nonblocking admission. The worker
writes to an owner-only regular JSONL file, uses partial-write rollback and
durable rotation, and advances `last_synced_sequence` only after successful
`fdatasync`.

The disposable MVP uses one per-node HostPath directory, a 64 MiB active file,
and one backup. A valid durability barrier requires:

```text
last_enqueued_sequence == last_synced_sequence
queue_depth == 0
worker_in_flight == 0
delivery_clean == 1
all authoritative drop counters == 0
```

The event proves a server-side protocol decision reached the sink. It does not
prove final wire delivery, client receipt, authenticated origin, cache hit,
cache fault, persistence, or performance benefit.

Tracked provenance:

| Artifact | Public-safe identity |
| --- | --- |
| Chimera candidate base | `00f207bb358c3a6fa2e914e3e42b301e1c726b80` |
| Reviewed v2 patch SHA-256 | `ce08c4523b1024e61cebeda45df708882158b81e7e6c71f88d633a65efeab72e` |
| Stable patch ID | `2b75d7775ec6a3301a77baa71327bbb2f8c7503a` |
| Latest sink OCI SHA-256 | `14f37754982567cbe6717694c36c83255a32cbb4681fb22992f3a93acee95315` |

See [`chimera/README.md`](../chimera/README.md).

### 5.2 Provider checkpoint, probe, and post-read bridge

The private provider is a control and evidence bridge, not an event producer.
It exposes three bounded actions:

| Action | Timing | Responsibility |
| --- | --- | --- |
| `checkpoint` | Start, each 15-minute boundary, final | Capture producer identity, enqueue/sync/count watermarks, queue/in-flight state, drops, sink/storage health, and cluster/export readiness. |
| `probe` | Immediately before every offered READ | Confirm the previous event is durable and current producer state remains safe. |
| `post-read` | Immediately after a successful direct READ | Wait for one new synced event, preserve exact source bytes, and write one durable sidecar. |

The provider never writes canonical `observed.jsonl`, assigns offered
operation IDs, fabricates origin, or renumbers producer sequences. The
collection harness is the single writer for offered-operation correlation and
canonical observed-event disposition.

### 5.3 `pcache-collect` CLI and direct-I/O mechanism

`pcache-collect` provides manifest initialization, dry-run, bounded live
validation, full run, reconciliation, finalization, and historical
finalization workflows.

The completed uniform collection used:

- 60 immutable 64 KiB fixtures;
- five nonoverlapping 4 KiB offsets per fixture;
- one deterministic operation for each fixture/offset pair;
- seed 32 and one operation per minute;
- concurrency one;
- a five-hour interval;
- Warmer disabled.

The live executor:

1. verifies the mount is read-only NFSv4;
2. traverses directories with directory file descriptors and `O_NOFOLLOW`;
3. opens the final regular file with `O_RDONLY | O_DIRECT | O_NOFOLLOW`;
4. verifies the frozen device identity and file size;
5. allocates a 4 KiB-aligned buffer;
6. issues `os.preadv`;
7. rejects unsupported direct I/O, alignment errors, short reads, and checksum
   mismatches;
8. records the execution result before requesting post-read evidence.

There is no buffered fallback because client page cache and readahead could
satisfy a later operation without producing a corresponding Chimera READ.

### 5.4 Manifests, catalogs, ledgers, checkpoints, and hashes

The collection evidence model separates intent, execution, source bytes,
durability, correlation, health, and final disposition:

| Artifact | Purpose |
| --- | --- |
| Run manifest | Frozen duration, rate, count, workload seed, safety profile, contract hashes, and synthetic provenance. |
| Fixture catalog | Allowlisted opaque volume/file/export identity, size, and legal ranges. |
| Offered ledger | Fsynced operation intent recorded before each READ. |
| Execution ledger | Client-side bytes, status, direct-I/O latency, and checksum result. |
| Durable sidecar | Provider proof that exact source bytes passed the post-sync barrier. |
| Observed ledger | Harness-owned correlation and durable/excluded disposition. |
| Health ledger | One pre-read barrier result for every offered operation and producer. |
| Checkpoints | Start, quarter-hour, and final producer/sink state. |
| Collector manifest | Exact source-file envelopes and checksums. |
| Reconciliation report | Offered/executed/durable/observed identity and count agreement. |

The frozen repository contracts are:

| Contract | SHA-256 |
| --- | --- |
| Scoped cluster provenance | `5b1045ffaffc4fee3c72dc79ddeb691b4af213d96a84854d2479d5ddae11c02b` |
| `nfs4_read_v2` schema | `3c15595d89d5780fc08a6eae667aafa848da85bf0f7f195c933a0fb3c13dbeb7` |

See [`contracts/README.md`](../contracts/README.md).

### 5.5 Sequence baselines and multichunk adapter behavior

The importer does not assume a producer starts at sequence one for each run.
The authenticated start checkpoint supplies the immutable epoch-global
baseline `B`. Run events must be exactly:

```text
B + 1, B + 2, ... B + N
```

Historical rows at or below `B` are excluded without renumbering. The final
checkpoint must prove a drained watermark at `B + N`.

When one producer's evidence spans multiple source chunks, the importer:

- preserves collector-manifest order rather than filename order;
- merges chunks before sequence validation;
- keeps one baseline for the entire producer epoch;
- rejects duplicate declarations, overlap, gaps, rollback, hard-link aliases,
  unexpected epoch changes, and unmanifested chunks;
- re-hashes exact source ranges and raw lines;
- requires source envelope, checkpoint, durable sidecar, observed record,
  offered ledger, and catalog identity to agree.

Empty-tag mode remains strict: raw/provider/offered evidence must carry an
exact empty tag and its hash, while the canonical observed representation is
null. It is not a permissive missing-tag fallback.

### 5.6 Adapter, cleaner, reconciliation, and finalization

`pcache-chimera-import` strict-parses raw JSON, rejects duplicate keys and
unknown fields, validates source hashes and sequence continuity, and produces
the source layout consumed by `pcache-prep clean`.

Controlled records are imported with `data_kind=synthetic` and
`origin=synthetic`. Production cleaning therefore excluded all 300 records and
retained zero demand events. The reads-only projection does not synthesize
opens, closes, sessions, or handle lifecycle from nearby READ timestamps.

Finalization replays and re-hashes the declared owner-only evidence before
writing a terminal disposition. The selected historical disposition is
`retain_all`: all 13 declared roots remain retained indefinitely,
`delete_by` is null, and no deletion inventory, schedule, completion, or
cleanup exists.

The unchanged private `MANIFEST.json` remains a pre-finalization
package-creation snapshot. Its historical status does not override the
separate terminal retention record. This distinction is now merged through
[PR #97](https://github.com/rahulgtt/predictive-cache/pull/97).

### 5.7 Plan-file Dispatcher

The Linux-only Dispatcher:

- atomically claims plans with `renameat2(RENAME_NOREPLACE)`;
- validates exact schema-v2 plans and owner-only filesystem metadata;
- uses `(run_id, plan_id, task_id)` as durable identity;
- writes running, terminal, failed, and ambiguous state with file and directory
  `fsync`;
- preserves claimed plans, task files, outputs, state, and results;
- never retries automatically;
- requires a new task ID and explicit lineage for a later attempt;
- launches one Warmer subprocess group with parent-death containment;
- resets inherited termination signals before execution;
- enforces persistent stop state and two-key real-read enablement;
- charges rolling item and byte limits before launch and never refunds them;
- bounds stdout, stderr, lines, and individual line length;
- requires one terminal JSON record to agree with the process exit code.

Default PoC controls are one in-flight task, 60 starts per rolling minute,
64 MiB of requested bytes per rolling minute, a 16 MiB task maximum, and zero
automatic retries. These are safety defaults, not measured safe capacity.

See [`docs/dispatcher.md`](dispatcher.md).

### 5.8 Experimental Warmer

The Warmer is a supervised single-task NFSv4.1 reader:

- one child process and at most one READ COMPOUND awaiting a response;
- one range up to 16 MiB;
- READ chunks up to 4 KiB;
- a bounded 0.1-300 second lifetime;
- validated path mode or owner-only schema-v2 handle-task mode;
- structured per-COMPOUND attempt evidence;
- actual returned-byte accounting;
- no automatic fill retry after a short read;
- explicit partial, error, timeout, cancellation, and ambiguous outcomes.

It establishes NFSv4.1 session state, performs read-only `OPEN`, bounded
`READ`, and `CLOSE`, and then tears down the session/client where safe.
Read-only does not mean zero protocol state or zero metadata effects.

One separately authorized path-mode 4 KiB read completed. That result proves
basic NFSv4.1 interoperability for that attempt only. Handle-mode live
interoperability, server-side tag correlation, cache materialization,
retention, and performance remain unverified.

See [`docs/manual-warmer.md`](manual-warmer.md).

### 5.9 CUFS PoC provenance and limitation

No CUFS source or runtime feature patch is active. The historical matched
build used the existing in-tree adapter and the verified CUFS
`0.2.0-48` runtime/development package pair.

| Artifact | SHA-256 |
| --- | --- |
| Build-only `Dockerfile.poc-compat` | `55b48c7b60732219072ff3f1ad0e924eab02cf1ce726a92474d6543c692d9b0e` |
| Build-only compatibility header | `dac238c11ad335e354431c4410c54e33d9d5cc5d4ce10ae0b6e19e5bfce0aed2` |
| CUFS runtime RPM | `ad539181ca82b2136cc9ebba114ce0aba7bf4a072d5788e71effaa2f39c33f9b` |
| CUFS development RPM | `0bdb065c3cd6e039bbfe8c36e7f8833d328538362c92e4653df4ca91b2c16ee3` |
| Effective runtime library | `66109c8514b459a1125faabeb0ae061a49b70304a9dc355329238cbbc99222e7` |

The build-only wrapper aliases the installed `struct stat` layout as
`cufs_stat_t` and maps unavailable `st_btim` access to `st_ctim`. Structure
layout was accepted for the disposable PoC, but the mapping does not provide
correct birth time. This record makes no birth-time, SMB CreationTime,
production ABI, or replacement-detection claim.

See [`cufs/README.md`](../cufs/README.md).

### 5.10 `read-demand-v1` semantic contract

[PR #107](https://github.com/rahulgtt/predictive-cache/pull/107) merged one
executable READ-only semantic source across datagen, Chimera import, cleaning,
feature computation, snapshots, model packages, training, and evaluation.

The canonical bindings are:

| Contract | SHA-256 |
| --- | --- |
| Semantic view | `37d56855d690678e4394621ef53c74a07a71226aed9faca0bc6ad5c5c91df7f8` |
| `read-demand-fv1` feature schema | `8cd7d65ea17db046a553d7a2f02c55d2237012cf3a9dfb9cc7a6e5f5a87d0318` |
| Strict serving contract | `790d345fdabc938eaccd71ba1f6724c5c651ceb9d077f0cd91bcb0282ef85a99` |

The implementation:

- projects only successful NFSv4.1 READ identity, range, returned bytes,
  request time, durable availability time, order, and explicit coverage/loss;
- keeps missing runtime metadata null rather than fabricating numeric facts;
- uses one known-at and coverage-aware feature engine for offline snapshots and
  incremental serving;
- binds snapshots and packages to the canonical semantic, feature-schema, and
  serving-contract hashes;
- rejects coordinated schema/serving mutations even when two artifacts agree
  with each other;
- rejects unavailable runtime features instead of treating offline catalog
  availability as live capability;
- never reconstructs OPEN, CLOSE, handle, or application-session lifecycle.

The controlled catalog may supply `dir_id` and `file_size` in the lab. A
verified production metadata source, rename/replacement semantics, and live
capability evidence remain open.

See [`docs/data-prep/read-demand-v1.md`](data-prep/read-demand-v1.md) and the
[contracts index](contracts/README.md).

### 5.11 Datagen workload-plan v3

The reviewed workload-v3 implementation adds an offline-only manifest, config,
execution plan, separate truth, truth binding, simulated Chimera source,
import/clean flow, and aggregate recovery report.

The trust boundary is explicit:

- `WorkloadExecutionPlan` is truth-free and contains per-occurrence intent,
  schedule, fixture, and range identity.
- `WorkloadTruth` contains synthetic actors, sessions, projects, popularity,
  co-read groups, sequential positions, and burst/quiet annotations.
- The Chimera importer never reads truth.
- The shared `read-demand-v1` constants are imported directly; no fallback or
  duplicate semantic constants exist.
- V3 uses `source_evidence_mode=offline_simulated_fixture`, so production
  READ-demand cleaning retains zero demand rows.
- Existing manifest-v2 five-hour behavior and tests remain unchanged.
- Mount, permit, bounded validation, checkpoint/reconcile, attestation,
  finalization, and cleanup paths reject v3 until a complete plan-bound live
  evidence chain is implemented and approved.

The sealed workload patch SHA-256 is
`8f5965e648a8bfda1f273a55b6cae0744a019968b93ba3bb41897e2f61c4a8a6`;
its reviewed result tree is
`91ad5de48a1a1f273276d4599b3e0b8606cdb684`.

See
[`docs/experiments/datagen-read-workload.md`](experiments/datagen-read-workload.md).

## 6. Verified collection and training results

### 6.1 Five-hour collection

| Measure | Verified result |
| --- | --- |
| Duration and rate | Five hours; one offered READ per minute |
| Fixtures and ranges | 60 fixtures; five nonoverlapping 4 KiB ranges per fixture |
| Operation records | 300 offered, 300 execution, 300 durable, 300 canonical observed |
| Pre-read safety evidence | 900 health-barrier rows |
| Scheduled checkpoints | 63 total: 21 per producer |
| Delivery | Zero recorded queue, encoding, sink, rollback, or rotation drops |
| Maximum monitored latency | `1,835.348 ms` probe-plus-read, below the frozen `5,000 ms` stop guardrail |
| Import | 300 synthetic events imported with no unresolved reconciliation gap |
| Production cleaning | 300 excluded as `origin_synthetic`; zero retained demand |
| Coverage | Five complete coverage hours |

The latency is a collection safety observation, not a foreground benchmark.
It is not evidence that CUFS warming improved user-visible latency.

### 6.2 Owner-private package

The owner-private package contains:

- 349 unchanged payload/evidence files;
- 352 regular files total, including three private control files;
- owner-only `0700` directories and `0600` files;
- no broadened POSIX ACLs;
- package-level checksum and source-comparison verification.

The two package-level fingerprints approved for repository documentation are:

| Control | SHA-256 |
| --- | --- |
| Checksum catalog | `ccd04fdc2095d9bc3e26077e0bd9b48b291776bbfbba77f663abb0f799cde0d3` |
| Private package manifest | `950cbbc7a70e839c2cd17b9fb2ed1432b92bd4c8b7480cc7a2c9b4731d24c315` |

The private manifest records package creation. Historical finalization records
the later terminal disposition for all 13 roots. PR #97 merged this
distinction into the access guide.

### 6.3 Training-pipeline smoke test

The training smoke test used explicit synthetic fixture snapshots, not the
zero-demand production-clean output:

- trained all five supported model packages;
- verified all five package contracts;
- evaluated 96 ticks and 4,410 rows per model;
- ran 123 targeted tests with three skips;
- kept ClearML disabled.

The 300-event controlled collection was independently imported and cleaned to
confirm policy behavior. It was not relabeled as demand and was not used to
claim model quality.

The dated summary originally identified a missing `pcache-eval` CLI. That
statement is superseded by merged
[PR #78](https://github.com/rahulgtt/predictive-cache/pull/78), which added
`pcache-eval`. A production `pcache-prep snapshots` clean-output-to-training
snapshot command remains absent.

### 6.4 Three-hour planted workload

The seed-42 offline candidate selected by datagen is:

| Measure | Verified offline result |
| --- | --- |
| Duration | Three hours |
| Operations | 165 READs, selected by datagen rather than fixed at 180 or 300 |
| Average schedule | `0.0152777778` READ/s; `65.4545 s` average interval |
| Variable gaps | `20.0-201.816 s` |
| Requested bytes | 675,840 bytes |
| Concurrency / alignment / tag | One; 4 KiB aligned; exact empty tag |
| Range recurrence | 92 unique fixture/ranges and 73 repeated operations |
| Popularity membership | 6 high, 18 medium, 36 low |
| Realized quotas | 99 high, 49 medium, 17 low |
| Synthetic truth | 10 actors, four co-read groups, 17 complete three-step sequences |
| Timing truth | Four burst windows and four quiet windows |

Frozen workload hashes:

| Artifact | SHA-256 |
| --- | --- |
| Workload config | `fa95914e38050d04688fcb53d4ae66f2197d7ccfee90ecf5dd2d233e809f3d9b` |
| Execution plan | `90bf6abcd1e5839809266eb75c2731cc8617141fad453f806933789edb4167a8` |
| Workload truth | `73c875be236d8cf54cbc1fa002853e153cd410416cc2de528f51230c0c751e09` |
| Shared semantic view | `37d56855d690678e4394621ef53c74a07a71226aed9faca0bc6ad5c5c91df7f8` |

The focused offline E2E generated the plan, ran the exact dry-run schedule,
created an offline simulated source, imported it, applied the READ-demand
cleaner, and recovered planted aggregate patterns. All rows remained synthetic
and ineligible for production demand.

## 7. Ten work groups and final outcomes

The attached Nov 3-4 summary grouped 23 coordinated sessions into ten work
groups. The table below preserves those groups and updates them with final
verified outcomes after the PDF was generated.

| # | Work group | Final verified outcome |
| ---: | --- | --- |
| 1 | Nasuni cluster access documentation | [PR #67](https://github.com/rahulgtt/predictive-cache/pull/67) merged the repository-safe access workflow. Current operational access still requires owner authorization and fresh cluster discovery. |
| 2 | Protocol/filesystem plan and README repair | [PR #69](https://github.com/rahulgtt/predictive-cache/pull/69) expanded the Engineer 1 plan and [PR #70](https://github.com/rahulgtt/predictive-cache/pull/70) removed README conflict markers. This document now reconciles that plan with final merged/runtime outcomes. |
| 3 | Historical CUFS/Chimera evidence correlation | Investigation matched historical source, package, header, adapter, runtime-library, patch, and image evidence. Results are preserved in `chimera/`, `cufs/`, collection provenance, and the historical finalizer; exact release-source provenance remains qualified rather than asserted. |
| 4 | Plan Dispatcher implementation | Strict schemas, atomic claiming, durable state, stop control, process containment, rate/byte limits, zero automatic retries, and result/exit agreement merged in [PR #84](https://github.com/rahulgtt/predictive-cache/pull/84). |
| 5 | Chimera event adapter | Strict v2 parsing, source-envelope checks, sequence baselines, multichunk handling, durable-sidecar equality, and synthetic normalization merged through [PR #84](https://github.com/rahulgtt/predictive-cache/pull/84), [PR #86](https://github.com/rahulgtt/predictive-cache/pull/86), and [PR #90](https://github.com/rahulgtt/predictive-cache/pull/90). |
| 6 | Event-queue hang and sink durability | Diagnostic work identified queue/sink failure modes. The final patch uses ordered sequence assignment, durable watermarks, partial-write rollback, bounded rotation, explicit drop counters, and a drained barrier. The one-event live durability gate passed. |
| 7 | Controlled collection harness | `pcache-collect`, typed evidence, provider supervision, direct I/O, checkpoints, reconciliation, finalization, and retention controls merged through #84, #86, #90, [PR #91](https://github.com/rahulgtt/predictive-cache/pull/91), and [PR #95](https://github.com/rahulgtt/predictive-cache/pull/95). The five-hour 300-READ collection and retain-all finalization passed. |
| 8 | Demo design and full MVP integration | The demo defined a transparent fixture-to-execution story. [PR #84](https://github.com/rahulgtt/predictive-cache/pull/84) vendored the reviewed, unapplied Chimera patch and contracts, and integrated the importer, Dispatcher, Warmer, collection harness, CUFS PoC compatibility inputs, and documentation. Follow-ups fixed validation and finalization contracts. |
| 9 | Training-pipeline smoke testing | Five packages trained and verified; 96 ticks and 4,410 rows per model were evaluated; 123 targeted tests passed with three skips. The controlled collection correctly produced zero production-demand rows. `pcache-eval` subsequently merged in [PR #78](https://github.com/rahulgtt/predictive-cache/pull/78). |
| 10 | Private dataset packaging and access guide | [PR #94](https://github.com/rahulgtt/predictive-cache/pull/94) merged the owner-mediated `PCACHE_DATASET_ROOT` guide without private data or paths. [PR #95](https://github.com/rahulgtt/predictive-cache/pull/95) added terminal retain-all behavior. [PR #97](https://github.com/rahulgtt/predictive-cache/pull/97) merged the final package-snapshot and indefinite-retention wording. [PR #102](https://github.com/rahulgtt/predictive-cache/pull/102) merged the dated work summary. |

## 8. PR state and validation record

GitHub state was rechecked immediately before this record was written.

| PR | State / merge commit | Relevant validation or result |
| --- | --- | --- |
| [#67](https://github.com/rahulgtt/predictive-cache/pull/67) | Merged at `2c9cf889fecc` | Access documentation only. |
| [#69](https://github.com/rahulgtt/predictive-cache/pull/69) | Merged at `0e450c1ebaec` | Canonical Engineer 1 plan expanded. |
| [#70](https://github.com/rahulgtt/predictive-cache/pull/70) | Merged at `64a2caefd6c8` | README conflicts removed. |
| [#78](https://github.com/rahulgtt/predictive-cache/pull/78) | Merged at `952d7f0fd8fd` | Added the `pcache-eval` freeze/run/report workflow and superseded the dated missing-CLI statement. |
| [#84](https://github.com/rahulgtt/predictive-cache/pull/84) | Merged at `f43e74445a2d` | 1,005 tests passed, one expected skip, one warning; Ruff and offline lock passed; independent combined-diff review found no release blockers. |
| [#86](https://github.com/rahulgtt/predictive-cache/pull/86) | Merged at `9647b8260449` | 74 focused tests and 1,017 full tests passed; one expected skip and one warning; Ruff and offline lock passed. |
| [#90](https://github.com/rahulgtt/predictive-cache/pull/90) | Merged at `5f7071273212` | 1,088 tests passed, two skipped, one warning; Ruff, offline lock, real-evidence replay, and blocker-only review passed. |
| [#91](https://github.com/rahulgtt/predictive-cache/pull/91) | Merged at `3cdb6d257c3d` | 1,110 tests passed, two skipped, one warning; Ruff, offline lock, historical declaration audit, dry-run attestation, and blocker-only review passed. |
| [#94](https://github.com/rahulgtt/predictive-cache/pull/94) | Merged at `ebcadfacefd8` | Markdown links, restricted-pattern scan, approved hash allowlist, and `PCACHE_DATASET_ROOT` checks passed; no private data command ran. |
| [#95](https://github.com/rahulgtt/predictive-cache/pull/95) | Merged at `4c939839192d` | 1,120 tests passed, two skipped, one warning; 32 focused tests, Ruff, offline lock, and independent review passed. |
| [#97](https://github.com/rahulgtt/predictive-cache/pull/97) | Merged at `7b8334ce13d2` | Clarified that the private manifest is a pre-finalization snapshot and that all 13 roots have an indefinite retain-all disposition. |
| [#100](https://github.com/rahulgtt/predictive-cache/pull/100) | Merged at `f4c3c46fbf5b` | Three datagen publication tests and Ruff passed; four synthetic size-ladder configs loaded and were exercised through the worker. |
| [#102](https://github.com/rahulgtt/predictive-cache/pull/102) | Merged at `d6da9e2f7a4f` | Markdown diff check, ten-section count, and linked-PR resolution passed. |
| [#106](https://github.com/rahulgtt/predictive-cache/pull/106) | Merged at `05d51b602e3d` | Added the C1-C5 contracts index and aligned the ML plan/target-map documentation with merged schema-v2 dispatcher fields. |
| [#107](https://github.com/rahulgtt/predictive-cache/pull/107) | Merged at `2f15393fb675` | 1,031 tests passed with two skips outside the baseline-blocked harness; 47 focused tests, Ruff in both build containers, and blocker review passed. |

This task does not edit issues or comments. The next workload implementation is
tracked by
[#103](https://github.com/rahulgtt/predictive-cache/issues/103).

## 9. Historical handoffs versus final state

| Historical record | Meaning then | Final repository/runtime state |
| --- | --- | --- |
| Dispatcher/Warmer patch SHA-256 `d1e78b813030506e3a03eede9b8242583be8d93c3ff2107e3a78bcb271f2f617`, stable patch ID `9803695483d6d4e63020267a73411d8d83400be7` | Reviewed uncommitted handoff on an earlier main snapshot. | Superseded by merged PR #84 and correctness/finalization follow-ups. Do not apply this patch to current main. |
| Historical Chimera v1 stable patch ID `0703197c05079c30926285ac1c6e92a384a0d397` | Minimal READ event without sufficient target identity. | Rejected for model ingestion and superseded by reviewed v2 patch SHA-256 `ce08c4523b1024e61cebeda45df708882158b81e7e6c71f88d633a65efeab72e`. |
| READ semantics patch SHA-256 `3372c3508704cfd7da61a5ba371849888e78277245355eac5b432d71ecace551`, stable patch ID `fa42f9b44be1e764ff228a89e9c8763f2535b46e` | Reviewed issue #104 handoff before publication. | Superseded by merged PR #107; current source and canonical hashes are authoritative. |
| Workload-v3 patch SHA-256 `8f5965e648a8bfda1f273a55b6cae0744a019968b93ba3bb41897e2f61c4a8a6` | Reviewed offline workload handoff on main including #104. | Integrated in this change; live collection remains separately gated. |
| "Warmer source absent from main" | Correct before integration. | Superseded by PR #84; `src/predictive_cache/warmer/` and tests are merged. |
| "Dispatcher is specification-only" | Correct before integration. | Superseded by PR #84; `src/predictive_cache/dispatcher/` is merged. |
| "Collection remains locked" | Correct before bounded validation and full run. | Bounded validation, five-hour collection, reconciliation, and historical finalization passed. New live workloads still require their own gates and approval. |
| PDF addendum: PR #94 remained open | Correct when the PDF was generated. | PR #94, PR #95, and PR #97 are merged; the access guide now records the final indefinite retain-all disposition. |
| Root `plan.md` status table | Frozen demo snapshot at main `1495af4`. | Historical presentation artifact only; it is not current implementation status. |

Historical hashes support auditability. They do not override current source,
contracts, PR state, or final evidence.

## 10. Remaining gates and limitations

| Gate | Current state | Pass condition |
| --- | --- | --- |
| Foreground isolation | Design and bounded event enqueue are implemented; collection safety checks passed. | Representative load shows no material foreground error/latency regression under the frozen guardrails. |
| Trusted origin | Client-asserted tags and controlled-run provenance only. | Reviewed authenticated or otherwise independently trustworthy attribution; unknown remains excluded. |
| Representative demand | Not available. | Authorized versioned real workload with privacy, coverage, loss, and retention evidence. |
| Cache materialization | Not proven per range. | Independent CUFS state/fault evidence shows the intended range transitioned as expected. |
| Equivalent cold state | Not established. | Repeatable non-warming reset/probe establishes matched starting state. |
| Warming benefit | Not tested to a decision. | Predeclared matched control/baseline/model trials clear the effect threshold and every guardrail. |
| Handle-mode Warmer | Offline tested, live unverified. | Authorized live handle task validates file identity, range, protocol result, cleanup, and failure handling. |
| Automated Controller loop | Not implemented. | Controller produces a schema-v2 plan that the Dispatcher claims and executes unchanged, with correlated feedback. |
| READ-only semantic parity | Passed in [PR #107](https://github.com/rahulgtt/predictive-cache/pull/107). | Preserve canonical semantic, feature-schema, serving-contract, known-at, coverage, and runtime-capability enforcement. |
| Workload-v3 live evidence | Offline plan/import/clean/recovery passes; all live paths reject v3. | Implement a complete plan-bound evidence chain and pass one exact three-operation bounded live validation before the three-hour run can be approved. |

### Fail-closed stop conditions

Stop before the next READ or reject the artifact when any of these occur:

- producer epoch, pod envelope, or source identity changes unexpectedly;
- enqueued/synced watermarks are not drained;
- queue, encoding, sink, rollback, or rotation drops are nonzero;
- a source chunk, sidecar, ledger, catalog, or checkpoint hash disagrees;
- producer sequences gap, overlap, duplicate, or roll back;
- direct I/O is unavailable, unaligned, short, or checksum-invalid;
- the export, fixture, range, deadline, budget, or operation is unauthorized;
- the stop file exists or real-read enablement is incomplete;
- terminal JSON and process exit status disagree;
- execution may have started but no trustworthy terminal result exists.

No calendar or demo target waives these conditions.

## 11. Datagen-driven three-hour READ workload

Implementation is tracked by
[#103](https://github.com/rahulgtt/predictive-cache/issues/103).
That feature is part of the matched live-warming performance story
[#82](https://github.com/rahulgtt/predictive-cache/issues/82).

### 11.1 Outcome

This change adds a deterministic datagen-driven workload-plan mode that plants
model-relevant patterns, maps them onto the authorized fixture catalog, and
executes only offline simulated NFSv4.1 READ evidence through the existing
import and cleaning contracts.

The selected second-collection candidate is **three hours** and 165 READs.
Datagen derived that count and variable rate from the planted-pattern
constraints rather than a fixed 180/300 target. Collection manifest v2
continues to require its original exact five-hour window and its tests remain
unchanged. The planted workload uses a separate manifest-v3 profile.

Offline planning, dry-run execution, simulated source generation, import,
READ-demand cleaning, and planted aggregate recovery are implemented. The
implementation deliberately rejects live mounts, permits, reconciliation,
attestation, finalization, and cleanup for v3.

### 11.2 Planted patterns

The generator explicitly models:

- hot, warm, and cold popularity tiers;
- recurrence at declared lags;
- actor/session/project co-access;
- sequential adjacent ranges;
- shared bursts and scan-like bursts;
- quiet periods and non-hour-aligned timing;
- repeated reads of the same fixture/range.

Pattern truth belongs in a separate synthetic truth artifact. Chimera observes
only actual READ decisions; it does not supply actor, session, project, or
pattern labels.

The workload generator preserves the same reads-only projection. It may
use planted actor/session/project facts to construct workload truth, but it
must not claim those fields came from Chimera or reconstruct protocol
lifecycle from READ timing.

### 11.3 READ-only boundary

In scope:

- NFSv4.1 READ workload plans;
- aligned authorized fixture ranges;
- synthetic truth and workload metadata;
- `pcache-collect` scheduling, provider, health, checkpoint, direct-I/O,
  evidence, importer, and cleaner contracts;
- concurrency one and exact empty-tag evidence with its hash;
- offline pattern recovery and bounded live validation.

Out of scope:

- OPEN/CLOSE demand telemetry or lifecycle inference;
- WRITE, CREATE, REMOVE, RENAME, SETATTR, or any mutation;
- NFSv3, SMB, S3, and `READ_PLUS`;
- representative customer-demand or model-quality claims;
- weakening the existing uniform-coverage mode;
- changing collection manifest v2's exact five-hour duration validation;
- starting a live three-hour run in the implementation PR.

The client may perform `OPEN` and `CLOSE` as NFS mechanics, but the workload
contract contains READ operations only and the event source records READ
decisions only.

**Do not invent protocol lifecycle facts from temporal proximity when READ evidence cannot prove them.**
OPEN, CLOSE, session, and handle lifecycle features remain disabled. Repeated
or nearby READs may support co-read timing features only.

### 11.4 Workload-plan truth

Each planned operation needs:

```text
schema and generator version
stable operation ID and index
planned relative and absolute time
authorized fixture ID and relative catalog reference
offset and count
synthetic actor, session, and project/group
popularity, recurrence, co-access, sequential, and burst labels
expected planted transitions and popularity ranks
data_kind=synthetic
seed and config hash
```

These are generator facts, not claims about Chimera telemetry.

The versioned training/evaluation preset must consume only successful READ
identity, range, bytes, time, order, and loss evidence. The lab catalog may
supply `dir_id` and `file_size`; production use remains blocked until an
authorized metadata source supplies equivalent fields.

### 11.5 Three approval gates

#### Gate A: offline - passed

The reviewed implementation passed these requirements:

1. identical seed/config/catalog produces byte-identical plan and truth
   hashes;
2. every operation maps to one allowlisted aligned READ range, uses the exact
   empty-tag mode, and preserves concurrency one;
3. repeated ranges retain unique operation IDs;
4. the plan satisfies exact popularity, recurrence, co-access, sequential,
   burst, and quiet-period tolerances;
5. every timestamp is inside the configured three-hour interval;
6. dry-run offered order and count exactly match the plan;
7. synthetic import and production cleaning exclude every row;
8. explicit synthetic evaluation recovers the planted patterns at
   predeclared fixture-level thresholds;
9. the existing uniform collection tests remain unchanged and passing.

#### Gate B: bounded validation - next

Use a fresh owner-only run directory and a separately reviewed plan-bound
validation contract for exactly three operation IDs from the frozen three-hour
workload manifest. It must preserve the merged permit-bound count, strict
empty-tag representation, nonzero baseline sequences, manifest-ordered
multichunk accumulation, and real wrapper invocation with both `RUN_DIR` and
`PERMIT_PATH`. Run the real provider checkpoint/probe/post-read path, direct
I/O, importer, cleaner, and mount cleanup. Any ambiguity fails closed. This
implementation remains offline-only and does not create that live-validation
permit.

#### Gate C: live three-hour run

Require a separate target-owner approval after offline and bounded-validation
review. Freeze the exact manifest, catalog, truth, provider/package hashes,
stop rules, storage capacity, cleanup/retention disposition, and monitoring.
No live NFS READ may occur before this gate is recorded.

### 11.6 Implementation evidence and remaining handoff

The reviewed offline implementation returned:

- result tree `91ad5de48a1a1f273276d4599b3e0b8606cdb684`;
- patch SHA-256
  `8f5965e648a8bfda1f273a55b6cae0744a019968b93ba3bb41897e2f61c4a8a6`;
- 31 focused E2E/importer/clean/live-regression tests passed;
- changed-code Ruff passed;
- independent blocker review passed;
- no dependency or lock files changed;
- no live READ, permit, deployment, commit, push, or PR occurred in the
  implementation worktree.

The next handoff must provide the exact three-operation bounded-validation
permit/evidence, target-owner approval, current environment discovery, and
pass/fail evidence for the live provider, NFS, durability, drop, identity,
latency, cleanup, and synthetic-exclusion gates.

## 12. Canonical references

| Topic | Canonical document |
| --- | --- |
| Project architecture, terminology, and evidence gates | [`README.md`](../README.md) |
| Engineer 1 current implementation record | This document |
| Dated 23-session summary | [`docs/project-history/2026-11-03-work-summary.md`](project-history/2026-11-03-work-summary.md) |
| Cluster access and authorization | [`docs/nasuni-cluster-access.md`](nasuni-cluster-access.md) |
| Component contracts index | [`docs/contracts/README.md`](contracts/README.md) |
| Frozen collection contracts | [`contracts/README.md`](../contracts/README.md) |
| Controlled collection runbook | [`docs/experiments/controlled-chimera-collection.md`](experiments/controlled-chimera-collection.md) |
| Datagen-driven workload v3 | [`docs/experiments/datagen-read-workload.md`](experiments/datagen-read-workload.md) |
| Chimera v2 importer | [`docs/data-prep/chimera-v2-adapter.md`](data-prep/chimera-v2-adapter.md) |
| READ demand semantic contract | [`docs/data-prep/read-demand-v1.md`](data-prep/read-demand-v1.md) |
| Production cleaning | [`docs/data-prep/spec.md`](data-prep/spec.md) |
| Owner-private package access | [`docs/data-prep/owner-private-controlled-dataset.md`](data-prep/owner-private-controlled-dataset.md) |
| Dispatcher | [`docs/dispatcher.md`](dispatcher.md) |
| Warmer | [`docs/manual-warmer.md`](manual-warmer.md) |
| Chimera patch provenance | [`chimera/README.md`](../chimera/README.md) |
| CUFS PoC provenance | [`cufs/README.md`](../cufs/README.md) |
| ML contracts and serving policy | [`docs/ml/spec.md`](ml/spec.md) |

`plan.md` is a frozen two-minute demo artifact tied to an older main revision.
Use it for presentation history, not current implementation status.
