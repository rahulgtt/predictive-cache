# Controlled Chimera READ collection

This harness prepares a **five-hour, Warmer-disabled, synthetic controlled**
NFSv4.1 READ workload. It supports offline `--dry-run` and an explicit
`--mount-root` for an already-mounted, read-only NFSv4.1 export. It never
mounts, unmounts, writes, creates, renames, or deletes target files.

The output is plumbing and collection-integrity evidence. It is **not**
representative production demand, model-quality evidence, cache-residency
evidence, or performance evidence. Offered records use `origin: synthetic`, so
`pcache-prep clean` excludes them from demand features and labels. Collector
events whose origin is not independently verified remain `unknown`.

The manifest-v2 profile in this runbook remains the frozen unique-coverage
collection. A separate
[datagen-driven workload profile](datagen-read-workload.md) prepares repeated
READ ranges and planted patterns and has a distinct manifest-v3 permit,
provider, reconciliation, attestation and finalization path. It depends on the
shared `read-demand-v1` semantic contract. The v3 path never translates to this
v2 profile, and this v2 runbook's seed-32/300-operation controls remain frozen.

## Frozen inputs

Start from:

- `tests/collection/fixtures/frozen-manifest.json`
- `tests/collection/fixtures/frozen-catalog.json`

Replace every environment placeholder before a real approved run:

- cluster/namespace/volume/export identifiers;
- cluster manifest, fixture catalog, and event schema SHA-256 values;
- exact Chimera/CUFS image and source revisions;
- explicit offered rate and count;
- latency guardrail, retention dates, and named cleanup owners.

The manifest requires an exact five-hour window, concurrency `1`, checkpoint
interval `900` seconds, an explicit positive rate, and an offered count that
does not exceed that rate cap. It rejects a Warmer-enabled run.

The direct mount executor requires `tag_mode: empty`; it is controlled
synthetic demand, not the Warmer. Kernel and NFS client caching can satisfy a
buffered repeated range without another Chimera READ. Manifest v2 therefore
uses only declared, unique, non-overlapping offsets and the executor requires
aligned `O_DIRECT`; there is no buffered fallback.

For tag-capable clients, derive `run_ref = sha256(run_id UTF-8)[:32]` and use
exactly `pc1:d:{run_ref}:{operation_id[:32]}:{operation_id[32:]}` (104 ASCII
bytes; separator colons at offsets 5, 38 and 71). The kernel mount executor
does not emit this tag.

## Offline dry run

Use the existing `nas-vm` Python environment. Do not install packages, mount an
export, contact a cluster, or use a Strider development host.

```bash
mkdir -m 700 /tmp/pcache-controlled-run
python -m predictive_cache.collection.cli init \
  --manifest tests/collection/fixtures/frozen-manifest.json \
  --catalog tests/collection/fixtures/frozen-catalog.json \
  --cluster-provenance contracts/cluster-provenance-v1.json \
  --event-schema contracts/nfs4-read-v2.schema.json \
  --budget-evidence tests/collection/fixtures/budget-evidence-v1.json \
  --out /tmp/pcache-controlled-run
python -m predictive_cache.collection.cli run \
  --run-dir /tmp/pcache-controlled-run \
  --dry-run
```

For an operator-approved, already-mounted read-only NFSv4.1 export:

```bash
python -m predictive_cache.collection.cli run \
  --run-dir /private/path/controlled-run \
  --mount-root /already/mounted/export \
  --checkpoint-command /trusted/path/collection-health-provider \
  --stop-file /private/path/controlled-run/STOP
```

Mount mode verifies the root is read-only `nfs4`, traverses with directory
file descriptors and `O_NOFOLLOW`, opens only a final regular file with
`O_RDONLY|O_DIRECT|O_NOFOLLOW`, checks frozen size and filesystem continuity,
allocates a 4096-byte-aligned buffer, and performs one aligned `preadv`.
Unaligned input, unsupported `O_DIRECT`, short/EOF, error, cancellation or
timeout stops the run after its result is durably recorded. It records only
byte count, status, latency and SHA-256; returned content is discarded.

The trusted provider is invoked without a shell:

- `python3 collection-health-provider.py checkpoint RUN_DIR --scheduled-at
  <scheduled-UTC-RFC3339>` emits one `Checkpoint` JSON object per pod on
  stdout;
- `python3 collection-health-provider.py probe RUN_DIR --operation-id
  <operation_id>` emits one `PreReadHealth` JSON object per pod immediately
  before that operation.
- `python3 collection-health-provider.py post-read RUN_DIR --operation-id
  <operation_id>` blocks until the accepted raw event and its sidecar record
  are fsynced, then emits exactly one `collector_durable_event_v1` JSON object.

Nonzero exit, timeout, empty output, late/missing full checkpoint, identity or
epoch disagreement, any drop/error/unhealthy field, or a persistent stop file
stops before the next READ. Full checkpoints are scheduled at run start, every
900 seconds, and the final boundary. `scheduled_at` is the frozen boundary;
`captured_at` must be monotonic and within 30 seconds. The runner appends and
fsyncs provider output; it never fabricates checkpoint timestamps.

Operation deadlines are absolute boundaries derived from the frozen UTC start
and a monotonic anchor. Probe/read duration is subtracted from the next wait;
the runner never adds a full interval after completion and never bursts to
catch up. If an operation is more than
`operation_schedule_tolerance_s` (default 30 seconds) late, it stops before
offering that READ.

Each pre-read object is operation-bound and includes the exact 64-hex
`operation_id`, pod/name/node/epoch identity, one-based enqueue/sync state,
queue and worker state, `delivery_clean`, `barrier_ready`, all five Chimera
drop counters, health booleans and error counters. The returned identity set
must exactly equal the latest full checkpoint set, and every capture must be
fresh within the 30-second barrier window.

The probe intentionally runs **before** the current operation is appended to
`offered.jsonl`; it receives the proposed operation ID on argv and must not
require that ID to exist in the ledger. After a clean probe, the harness
appends and fsyncs the offered record (including parent-directory fsync on
first creation) before invoking the read executor.

The post-read call must complete and return the final event with
`durable_at < end_time`; otherwise the run fails. The provider is the sole
writer of immutable raw chunks and `durable-events.jsonl`. It never writes
`observed.jsonl` and never assigns a harness offered-operation ID.

`collector_durable_event_v1` contains `run_id`, UUID event/pod/epoch identity,
one-based producer sequence, 64-hex server operation ID, diagnostic
`sink_time_ns`, canonical volume/file/export/range, returned bytes/NFS
status/outcome/EOF, authoritative `durable_at`, decoded tag, and raw chunk
path/whole-file hash/line number/record hash. The harness verifies the exact
raw line and hashes, uniqueness and result/range consistency before allowing
the next READ.

The harness is the sole writer of canonical immutable `observed.jsonl`.
After every post-read it validates current correlation; after the final full
checkpoint it atomically publishes `ObservedEvent` v2 rows. The adapter is
read-only and must never create, append or repair either provider or canonical
collection artifacts.

The harness-owned `execution.jsonl` result ledger uses `ExecutionResult` v2:
`operation_id`, `completed_at_utc`, status (`ok`, `short`, `eof`, `error`,
`cancelled`, `timeout`), actual `bytes_read`, `latency_ms`, optional
`content_sha256`, `executor_kind`, filesystem type and optional error. The
provider reads this durable ledger for foreground-read-error and maximum
latency reporting; those values are never caller-supplied flags.

## Authorized first live shape

After discovery supplies the 60 canonical catalog entries, derive the live
manifest with:

- duration 5 hours, rate `1/60` reads/second, count 300, concurrency 1;
- 60 deterministic 65,536-byte fixtures;
- one 4,096-byte direct read per declared offset per file:
  `0`, `12,288`, `24,576`, `36,864`, `49,152`;
- frozen seed, `warmer_enabled: false`, `tag_mode: empty`.
- hard 5,000 ms maximum for the entire monotonic pre-read-probe plus direct
  read interval. A breach stops the run and is not a performance result.

The live manifest/catalog are generated only after discovery. Committed
fixtures remain offline contract tests.

The combined gate must not use `--dry-run`, which bypasses the provider and
real I/O. Use bounded real validation with one to three operations:

Use only the owner-approved frozen provider package:

| Artifact | SHA-256 |
| --- | --- |
| Provider package archive | `3c6d3a4cb164cc4df87c36480dab49621e140f9307a170e393233eb8221feece` |
| Provider | `51d92af0b0beab61ac77dd4dd7b5d19cd9a4fd83e4a709e74582b3be244682d4` |
| Shared collector | `09158174889f25a6abd7a70a33d24ad1d2efaba3f8ec935e0d5344eba5959225` |
| Validator | `8d3dc5aad205c6fca55aa8b7c34e933906ca3f7758befb9c1bdf1d835c53e7da` |
| Bounded-validation runbook | `cf5e2238aa026aa5f3f8f80c7e46b8658859a349c3a2748b7a425e4bb3b405be` |

The approved private runbook supplies the actual locations. In order: initialize
the harness and copy its frozen inputs, install the specifically approved
permit, initialize the provider in the dedicated validation run directory, and
only then start the bounded harness run:

```bash
python3 "$PRIVATE_PROVIDER_SCRIPT" init "$VALIDATION_RUN_DIR"
python -m predictive_cache.collection.cli run \
  --run-dir "$VALIDATION_RUN_DIR" \
  --mount-root /already/mounted/export \
  --checkpoint-command "$PRIVATE_PROVIDER_SCRIPT" \
  --provider-package "$PRIVATE_PROVIDER_PACKAGE" \
  --validation-operations 1 \
  --adapter-command "$INTEGRATED_ADAPTER" \
  --cleaner-command "$INTEGRATED_CLEANER"
```

This still performs provider checkpoint/pre-read/post-read, aligned O_DIRECT
fixture I/O, canonical observed publication, adapter ingestion and cleaning
under the normal stop rules. It is validation evidence, not the five-hour run.
Before the first READ, the harness immutably writes
`adapter-validation-permit.json`. The permit binds the unchanged full
300-operation manifest hash, exact deterministic first one-to-three operation
indexes and IDs, one-hour expiry, unique permit ID, and the approved
package/provider/collector/validator/runbook hashes. The adapter receives that
permit explicitly. Without it, the adapter still requires all 300 ledger rows;
the permit cannot authorize a non-prefix row, a rewritten manifest, or more
than three operations. The harness invokes the adapter callback with exactly
two arguments: `RUN_DIR` and the immutable permit path. The wrapper must pass
that second argument as the importer's explicit `--validation-permit`; permit
auto-discovery from a short ledger is forbidden.

Hash byte sources are non-self-referential:

- `cluster_manifest_sha256` is SHA-256 of the exact bytes of
  `RUN_DIR/cluster-provenance-v1.json`, copied and fsynced before
  `manifest.json` from the exact tracked
  `contracts/cluster-provenance-v1.json`.
  It is UTF-8 canonical JSON (sorted keys, `,`/`:` separators, no insignificant
  whitespace) followed by exactly one LF. It must not contain
  `cluster_manifest_sha256`, the run-manifest hash, or the run manifest.
  The scoped bundle explicitly separates the full baseline BOM hash and
  Chimera Helm revision 7 rendered-manifest hash; neither alone is described
  as full cluster identity.
- `event_schema_sha256` is SHA-256 of the exact tracked bytes of
  `contracts/nfs4-read-v2.schema.json`, copied to
  `RUN_DIR/event-schema-v2.json`, with the same canonical JSON plus one-LF
  serialization. The schema file must not contain its own hash.
- `budget_evidence_sha256` binds `RUN_DIR/budget-evidence-v1.json`. Its 2.382 s
  and 0.582 s observations are explicitly read-only partial measurements, not
  complete probe-plus-read results. Authoritative full intervals are recorded
  separately as `ExecutionResult.supervised_latency_ms`.

Raw chunks, durable sidecars, ledgers and canonical observed evidence remain
private and must be securely deleted no later than seven days after validated
dataset finalization by the named cleanup owner. The run manifest contains
only `trigger: validated_dataset_finalization`, `delete_within_days: 7` and a
null `retain_until`; it must not invent an absolute collection-time deadline.
Finalization atomically writes `CleanupScheduleRecord` with the actual
`finalized_at` and exact `delete_by = finalized_at + 7 days`. Cleanup writes a
durable `CleanupCompletionRecord` with completion time, owner and evidence.
The hashed deletion inventory includes exact raw chunks plus private
`manifest.json`, `fixture-catalog.json`, offered/execution ledgers, durable and
canonical event sidecars, source envelope, health/checkpoint evidence and
reconciliation output when present. Bounded provider-backed validation has no
independent finalizer or deletion deadline. Its attestation retains a distinct
validation run ID and candidate-manifest identity and binds the validated
commit/tree/source archive, permit prefix, approved provider stack, wrappers,
contracts and PASS outputs. Retain its four original owner-only roots until
full dataset finalization. At that point, inventory those four roots and the
live run's four roots independently under one seven-day cleanup schedule.
Repository-tracked provenance/schema contracts and sanitized validation
summaries are outside that private deletion inventory.

## Reported direct-I/O preflight

The coordinating session reported a live preflight PASS on `nasuni-cluster2`:
UID/GID 1002, read-only NFSv4.1 `test-ro`, one aligned 4,096-byte `O_DIRECT`
`preadv`, checksum
`e00e153a038f7017f129fbca1d00e69652e39e4befab87f6e0e2e31f233f32d9`,
aggregate VFS Read `+1`, no buffered fallback, and healthy cleanup. This is
mechanism evidence supplied by the coordinator, not a five-hour collection or
an independently reproduced result in this worktree.

The coordinator subsequently reported the live collector gate PASS at Helm
revision 7 with 3/3 ready and exactly one empty-tag durable event whose epoch,
synced/drained state, `delivery_clean`, and zero-drop counters agreed. This
unblocks real catalog/manifest derivation but is still not the five-hour run.

The runner appends and `fsync`s each `offered.jsonl` operation before invoking
the executor. Restart validates the existing prefix by deterministic operation
ID and continues without re-offering it. Dry-run writes only private evidence;
it never opens fixture paths or performs NFS/network I/O. Its accelerated
timestamps are generated inside the frozen window and every offered record
marks `timing_mode: simulated`; a future real executor must use
`timing_mode: wallclock`.

## Collector interface

The planned Chimera collector integrates by writing two versioned JSONL
interfaces instead of being reimplemented here:

- checkpoints matching `Checkpoint`, every 15 minutes per pod;
- observed events matching `ObservedEvent`, using a canonical `operation_id`
  when correlation succeeds, or `status: unknown` when it does not.

Each checkpoint includes producer epoch, enqueued/synced sequence, synced count, queue
depth, worker in-flight, `delivery_clean`, `barrier_ready`, Chimera drop stages
`queue`, `encode`, `sink`, `rollback`, `rotation`, active/backup JSONL size and hash, pod
UID/restarts, health fields, parse/identity/checksum errors, read errors, and
the maximum harness-observed latency. That latency guardrail only bounds this
controlled offered load; it does not establish foreground safety.

Validation stops immediately for any drop, epoch reset, unexplained restart,
sink/storage fault, unhealthy cluster/volume/export, parse/identity/checksum
mismatch, read error, evidence-file mismatch, cadence mismatch, or guardrail
breach.

Producer epochs are canonical lowercase UUID strings. Producer sequences are
one-based; the no-event sentinel is last-enqueued `0`, last-synced `0`,
synced-count `0`. Otherwise `synced_count == last_synced_sequence`.

Raw Chimera files are bound to collector identity by an immutable source-file
envelope:

```json
{
  "collector_file_manifest_version": 1,
  "run_id": "controlled-demo",
  "files": [{
    "raw_jsonl_path": "collector/<pod_uid>/<producer_epoch>/events-00000.jsonl",
    "sha256": "<64 lowercase hex>",
    "pod_name": "collector-0",
    "pod_uid": "<Kubernetes pod UID>",
    "node_name": "node-0",
    "producer_epoch": "<canonical lowercase UUID>"
  }]
}
```

The raw event identity is `(local_id, envelope pod_uid, producer_epoch)`.
`ObservedEvent.event_id` stores `local_id`; `producer_sequence` remains
one-based; `server_operation_id` remains distinct from the harness offered
operation ID. `durable_at` is the authoritative downstream availability time.
Optional `sink_time_ns` is only the producer's pre-write-attempt diagnostic.
The adapter joins catalog identity on canonical `(volume_id, file_id)` and
validates integer `export_id` plus decoded `export_name`; it must not alias
friendly IDs.

## Reconciliation and cleanup

```bash
python -m predictive_cache.collection.cli checkpoint \
  --run-dir /tmp/pcache-controlled-run \
  --input /private/path/checkpoints.jsonl
python -m predictive_cache.collection.cli reconcile \
  --run-dir /tmp/pcache-controlled-run \
  --observed /private/path/observed.jsonl \
  --checkpoints /private/path/checkpoints.jsonl \
  --sources /private/path/collector-file-manifest.json
python -m predictive_cache.collection.cli finalize \
  --run-dir /tmp/pcache-controlled-run \
  --dataset-report /private/path/validated-dataset-report.json \
  --dataset-root /private/path/validated-dataset \
  --finalization-root /private/path/finalization-control \
  --bounded-validation-attestation \
    /private/path/finalization-control/validation-attestation.json \
  --cleanup-root bounded_harness_evidence=/private/path/bounded-harness \
  --cleanup-root bounded_provider_evidence=/private/path/bounded-provider \
  --cleanup-root bounded_adapter_dataset=/private/path/bounded-adapter \
  --cleanup-root bounded_clean_dataset=/private/path/bounded-clean \
  --cleanup-root live_harness_evidence=/private/path/live-harness \
  --cleanup-root live_provider_evidence=/private/path/live-provider \
  --cleanup-root live_adapter_dataset=/private/path/live-adapter \
  --cleanup-root live_clean_dataset=/private/path/validated-dataset
```

Finalization starts the retention deadline only after it re-hashes every
declared dataset output and validation-provenance file and confirms that those
declarations exactly cover the owner-only live clean dataset. The bounded
attestation remains separate from the live manifest/reconciliation and cannot
satisfy a live field. The live dataset report declares its own four roots,
manifest, reconciliation, adapter manifest and cleaned manifest. All eight
roots are re-inventoried by canonical path, path hash, device/inode, kind,
per-file hash/mode and root-manifest hash. Missing/extra roots, nesting,
aliases, symlinks, nonregular files, unsafe modes, root-kind swaps, copied
substitutes and stale hashes fail closed. Finalization also requires successful
canonical reconciliation whose manifest, offered-ledger and observed hashes
match fresh live files and whose counts cover all 300 offers.
Both bounded and live provider declarations must bind the approved package,
provider, collector, validator, absolute interpreter and fixed argument
contract. The harness writes `provider-invocation.json` before collection and
binds its run, manifest, interpreter, snapshot, actions and argument contract.
Finalization re-hashes the actual interpreter, the inventoried
package/collector/validator artifacts and the
`.approved-tools/checkpoint-provider` snapshot from the inventoried harness
root; a replaced artifact, altered invocation or declaration-only hash fails
closed. The package archive and entry script are copied into the run-local
`.approved-tools` root and executed with Python isolated mode (`-I -S`) and a
fixed bootstrap path, so caller `PYTHONPATH`, user site packages and
`sitecustomize` cannot substitute provider modules.
Adapter and clean event files must use canonical
`volume_id=<id>/date=<UTC-date>/part-00000` partitions whose identity, date and
row order match their contents. Per-day clean summaries are derived exactly
from those adapter rows, and duplicate JSON object keys are rejected in every
JSONL evidence record. The complete raw identity set must equal the durable and
observed identity set; an otherwise valid contiguous extra raw event fails
finalization. The actual `collector/` regular-file inventory must also exactly
equal the source manifest and provider chunk references; extra, missing,
symlinked or unexpectedly nested chunks fail. Clean output accepts only exact
declared artifact classes such
as `events_clean/`, `excluded/`, `coverage/` and `validation.json`; lookalike
prefixes are rejected.
The bounded attestation and finalizer-generated deletion inventory/schedule
reside in a separate owner-only control directory, outside all eight evidence
roots. This avoids self-referential manifests and prevents finalization from
changing a root after its complete snapshot.
Duplicate, missing, unexpected or unknown events and unapproved exclusions
fail closed. A declared exclusion reason may explain a gap, but remains visible
in the report.

Evidence directories must be mode `0700`; files are written mode `0600`.
Store output only in owner-approved private storage. The retention owner must
perform the manifest's cleanup action after the retention deadline. Never
commit real manifests, catalogs, paths, events, or raw collection output.

### Historical pre-finalizer compatibility

`finalize-historical` is a separate, versioned path for the one approved
collection that predates run-local provider invocation records and tool
snapshots. It never accepts a current run and does not weaken `finalize`.

The historical attestation records thirteen physical roots exactly once:
four validation run/input pairs, live run/input, shared provider evidence, and
the read-only adapter and cleaner replay roots. Semantic references may point
inside a physical root, so the successful validation's nested import and clean
outputs are not relabeled as separate roots. The shared provider root is
inventoried but retained; the other twelve roots are proposed deletion targets
until the user supplies a separate post-merge approval artifact binding every
canonical path, device/inode and action.

Executed provenance remains distinct from later verification provenance. The
attestation binds the original commit, tree and stored source archive; the
integration merge is explicitly non-executed; and the later compatibility
verifier records its own commit, tree and source archive. Missing historical
`provider-invocation.json` and `.approved-tools` artifacts are recorded as
`legacy_not_recorded`, never synthesized.

Every physical root must be owner-owned mode `0700`, symlink-free and
nonaliasing. Historical replay outputs may retain their recorded descendant
directory/file modes because the `0700` root prevents traversal; those modes
are inventoried exactly and any drift fails closed. An accessible top-level
root is always rejected.

The command re-runs the current importer and cleaner in temporary control-root
directories and compares reports and tabular outputs to immutable historical
artifacts. `finalize-historical` independently proves that the attested
verifier commit is the supplied merged Git ref and that the source archive
materializes exactly that tree. Only then, and only with the separate exact
outcome approval, may it finalize. A deletion approval generates the inventory
and seven-day schedule. A retain-all approval instead writes an immutable
`historical-retention-record.json` with every root set to `retain`, a null
`delete_by`, and no deletion inventory or cleanup schedule. Cleanup uses the
separate `complete-cleanup-historical` contract only for the deletion outcome.
Both outcomes first claim the same immutable
`historical-disposition.json`; once one outcome is claimed, the opposite
outcome and cleanup completion fail closed. An interrupted same-outcome run may
resume only while its terminal record or schedule is absent.
