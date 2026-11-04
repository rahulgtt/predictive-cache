# Chimera v2 controlled-run adapter

`pcache-chimera-import` converts a fully reconciled controlled collection into
the event and loss tables consumed by `pcache-prep clean`. It accepts exact
`nfs4_read_v2` JSONL, but trusts collector durability and offered-operation
correlation only through the collection evidence described below.

The run remains `manifest.data_kind=synthetic`, and every emitted access row
has `origin=synthetic`. The `pc1:d` tag is client-asserted correlation
evidence, not authenticated foreground origin.

## Command

```bash
uv run pcache-chimera-import \
  --event-sources event-sources.json \
  --observed observed.jsonl \
  --run-manifest run.json \
  --offered-ledger offered.jsonl \
  --fixture-catalog catalog.json \
  --checkpoints checkpoints.jsonl \
  --out /tmp/pcache-chimera-run

uv run pcache-prep clean \
  --input /tmp/pcache-chimera-run \
  --out /tmp/pcache-chimera-clean
```

All evidence files must be regular, non-symlink, owner-owned `0600` files.
The adapter output is an owner-only `0700` directory. Inputs are hashed into
the output report and manifest runtime metadata.

## Authoritative inputs

| Input | Purpose |
| --- | --- |
| Collector file manifest | `collector_file_manifest_v1` maps each checksummed `collector/<pod_uid>/<producer_epoch>/<file>.jsonl` path to trusted `pod_name`, `pod_uid`, `node_name`, and exact Chimera producer-epoch UUID. Raw lines do not supply pod identity. |
| Raw Chimera JSONL | Exact `nfs4_read_v2` records. Decimal uint64 fields remain JSON strings, as emitted by Chimera. |
| Collection manifest | Frozen v2 five-hour synthetic NFSv4.1 READ run, Warmer disabled, canonical volume/export identity, schema/image hashes, source-compatible `run_ref`, unique non-overlapping `range_offsets`, checkpoint tolerance, untagged-correlation window, tag mode and stop rules. |
| Offered ledger | Durable pre-issue operation records with canonical operation IDs, fixture identity, range, client and exact tag. |
| Observed JSONL | Authoritative durable/excluded disposition and offered-operation correlation. |
| Fixture catalog | Canonical v2 32-hex volume ID, 64-hex file ID, numeric export ID, export name/path, fixture directory and immutable controlled path/size. |
| Checkpoints | Twenty-one records per pod/epoch from run start through the final 15-minute boundary, with `captured_at` no more than 30 seconds late and exact producer identity/watermarks. |

### Single-writer boundary

The deployment provider owns raw chunks and a separate immutable
`collector_durable_event_v1` sidecar written only after its post-fsync barrier
accepts an event. That sidecar is keyed by
`(local_id, pod_uid, producer_epoch, producer_sequence)` and carries source
identity, range, durability time and diagnostics. It does not claim the
offered-operation ID.

The collection harness is the **only writer** of canonical `ObservedEvent v2`
and `observed.jsonl`. It owns the offered ledger and the tagged or untagged
correlation policy, so it alone can assign `operation_id` and the final
`durable`, `excluded` or `unknown` disposition. The harness writes the
canonical file once as immutable evidence; the provider and adapter never
append to or repair it.

The adapter is read-only. It validates canonical observed records against raw
events, the source envelope, offered ledger, fixture catalog and checkpoints.

The exact raw-to-observed join is
`(local_id, pod_uid, producer_epoch)`. `pod_uid` comes only from the trusted
source envelope. The observed record then joins the offered ledger by its canonical 64-hex
`operation_id`. Its canonical volume/file/range, `producer_sequence`, optional
`sink_time_ns`, and `server_operation_id` must agree with the raw event. The
raw/server operation ID is a separate NFSv4.1 replay identity and remains
diagnostic.

## Correlation tag

For tag-capable clients, `tag_mode=pc1:d` uses:

```text
pc1:d:<sha256(run_id UTF-8)[:32]>:<operation_id[:32]>:<operation_id[32:]>
```

The manifest's `run_ref` must equal the first expression. The adapter checks
the decoded raw tag, observed tag, offered tag and raw `compound_tag_id`.
Chimera's `origin_claim=controlled_demand` remains client-asserted.

The mounted kernel-client run uses `tag_mode=empty`. In that mode the adapter
requires an exact unique match on canonical volume/file/export identity and
one of the manifest's non-overlapping offered offset/count pairs, a bounded
offered-to-durable interval, and
monotonic offered/disposition order. Ambiguous or inconsistent candidates
fail closed. Empty tags remain `origin=synthetic`.

## Time semantics

- `event_time_ns` is the source operation time.
- `sink_time_ns` is the Chimera sink-attempt time before serialization,
  write and `fdatasync`; it is retained only in `report/import.json`.
- `ObservedEvent.durable_at`, accepted after collector file-read and synced
  barrier checks, becomes event-schema `ingest_time` / downstream `known_at`.
- Nanoseconds are validated exactly, then timestamps are floored to the
  event contract's UTC microsecond precision.
- No ordering assertion is made between `durable_at` and diagnostic
  `sink_time_ns`; the latter has no availability authority.

## Identity, range and result rules

- Raw volume/file IDs must exactly match a fixture on `(volume_id, file_id)`.
- Raw numeric export ID and decoded absolute export path must match the fixture
  and offered record. Missing or conflicting identity fails closed.
- The fixture's `dir_id` supplies the event directory; the adapter never
  invents a directory ID.
- The fixture size supplies required event `file_size`; a valid raw size must
  match it. Requested and returned ranges may not exceed that size.
- NFS status `0` maps to `ok`; `PERM`, `NOENT`, `IO`, `ACCESS`, and `INVAL`
  map to the corresponding strict result enum. Other numeric errors map to
  `eio`. Failed reads must return zero bytes and `eof=false`.
- The fixture catalog is not copied into the prep source because the
  collection catalog intentionally lacks datagen timezone, calendar, file-kind
  and project metadata. Omitting it is safer than fabricating those fields.

## Sequence, checkpoint and loss reconciliation

- Every source file has one exact producer envelope, and checkpoint/source
  `(pod_name, pod_uid, node_name, producer_epoch)` identity sets must agree.
- The exact start checkpoint supplies each stream's immutable sequence
  baseline `B`. Run raw/sidecar/observed rows must remain in source order at
  exactly `B+1..B+N`; historical rows at or below `B`, gaps, duplicates,
  rollback and cross-epoch resets fail closed.
- Each pod/epoch has exactly twenty-one scheduled checkpoints. Missing records or
  capture later than `checkpoint_tolerance_s` after the scheduled time fail
  the import.
- Epoch changes, sequence resets, unexplained restarts, drops, unhealthy
  state, parse/identity/checksum/read errors and latency guardrail breaches
  fail the import.
- For a full run, the final checkpoint must prove
  `last_enqueued_sequence == last_synced_sequence == synced_count == B + raw
  event count`, with zero queue depth and no worker in flight. A bounded
  validation permit accepts only the captured schedule prefix and still
  requires exact `B+1..B+N` raw/sidecar/observed continuity.
- Every offered operation has exactly one durable or approved-excluded
  disposition. Unknown, missing, duplicate, contradictory and unexpected
  dispositions fail closed.
- Hourly prep counters use offered time. Durable dispositions are `emitted`;
  approved exclusions are `dropped` and create an incomplete known-gap hour.
- JSONL evidence rejects duplicate object keys. Imported and cleaned event
  artifacts use canonical `volume_id=<id>/date=<UTC-date>/part-00000`
  partitions; each row's volume, UTC ingest date and canonical order must
  match its partition.

## Publication and privacy

The adapter writes to a sibling staging directory and atomically renames it
into a new destination. Existing destinations are never replaced: portable
directory replacement cannot guarantee continuous atomic visibility to
concurrent readers. Output paths may not traverse symlinks.

The prep dataset contains opaque IDs and event metadata, not contents,
credentials, target file handles, raw fixture paths, node names or export
paths. Source-only diagnostics include sink-attempt time, producer sequence,
pod UID, producer epoch, NFS operation ID and status.
### Producer sequence baseline and raw-chunk boundary

The authenticated run-start checkpoint defines a lifetime producer baseline
for each exact `(pod_name, pod_uid, node_name, producer_epoch)`. Provider
initialization records a drained checkpoint and copies only subsequently
accepted events into the run's immutable source chunks. Chunks for one stream
retain their authoritative `CollectorFileManifest.files` order and are merged
before validation; filename sorting or per-chunk baseline reset is forbidden.
their combined producer sequences must be exactly `baseline + 1` through
`baseline + N`. The minimum raw `sink_time_ns` must be at or after the start
checkpoint's actual `captured_at`.

The final drained checkpoint must report
`last_enqueued_sequence == last_synced_sequence == synced_count == baseline + N`.
Every source chunk is hash-verified. Checkpoint active/backup references must
name manifested chunks with exact hashes and sizes; the start and final active
files must match the first and last manifest entries for the stream.
Duplicate paths, hard-link aliases, sequence gaps, rollback, producer epoch,
pod or node changes fail closed. A new producer epoch is a distinct stream
and requires its own authenticated start checkpoint baseline.
