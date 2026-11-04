# Private execution target map

The standalone controller TargetMap implements the in-memory C5 boundary from
[ML spec §8.11](../ml/spec.md#811-file-identity-and-the-target-map-c5). It maps
model identity (volume_id, file_id) to an export-specific NFS handle. It does
not generate demand or execute reads. The opt-in raw HTTP target-ingestion
mode below now supplies validated events to this map.

Create a TargetMap with a trusted exports dictionary (export ID to name),
max_entries (default 100,000) and target_max_age_s (default 3,600).
update(event, seen_at) revalidates a ChimeraEvent, validates its wrapped handle
with the dispatcher validator, and uses the Controller's UTC receipt timestamp.
The event's export name and source clock do not supply configuration or
freshness. All origins may refresh execution metadata; this never admits
Warmer, synthetic or unknown events as demand.

resolve(volume_id, file_id, t) returns a frozen Target or None for missing,
stale, future or expired entries. It uses the configured export name. The
exact age limit is inclusive. Access updates LRU order; insertion/resolution
prune expired entries, and capacity evicts the least recently used entry.
This is current operational state, not a historical as-of database. Resolution
does not prove permission, continued handle validity or cache residency.
Planner and Warmer validation and budget gates remain required.

mark_stale(resolved_target) takes the exact previously resolved version rather
than just file identity: an old task's feedback cannot invalidate a newer
refresh. Equal or older receipt timestamps cannot replace an entry or clear
its stale flag. A strictly newer validated event can refresh it. This version
binding refines the illustrative stale-feedback interface in the ML spec.

state() returns private versioned checkpoint bytes. restore() validates
configuration, bounds and every entry before replacing memory atomically.
LRU order and stale flags survive restore; diagnostic counters do not.
This is a serialization seam for #62, not full ControllerState or restart
continuity. There is no disk writer: the future aggregate state owner must
persist these bytes only in its owner-only checkpoint (mode 0600).

Handles are hidden from Target representations and excluded from aggregate
stats(). Resolved records and checkpoint bytes remain private. Callers must
not log them, add them to datasets/features, or upload them to ClearML.
No real handles are test fixtures. CatalogTargetMap, planner integration and result correlation remain separate
work.
This component alone does not complete #105.

Run tests/controller/test_targets.py and controller/dispatcher checks on
nas-vm (hackathon) in a designated build container. Fictional fixtures cover
validity, receipt ordering, expiry/LRU, stale refresh races and checkpoints
without NFS or cluster I/O.

## Raw HTTP target ingestion

The existing collector CLI supports explicit `controller.ingest.mode:
chimera-targets` with `source: http`. Supply `allowed_volumes`, registered
`shippers` (expected pod UID strings), and `targets.exports` (trusted export
ID to name mapping). For example, inside the designated remote environment:

```yaml
controller:
  ingest:
    source: http
    mode: chimera-targets
    listen: 127.0.0.1:8090
    queue_max: 200000
    allowed_volumes: ["<allowed-volume-id>"]
    shippers: ["<registered-pod-uid>"]
    targets:
      exports: {7: test-ro}
      max_entries: 100000
      target_max_age_s: 3600
```

Use the existing bearer-token environment setting. Requests use the raw
`nfs4_read_v2` envelope in ML spec §8.1.1, including pod/epoch, first/last
sequence, health and events. Envelope fields, epoch/range consistency and
synced watermarks are checked before admission; unregistered pods receive
403 and malformed envelopes 400. Raw event validation and map mutation run
only when the separate consumer drains the bounded queue. A 202 response
reports `accepted`, `dropped`, and `duplicate`, not validation success.

Validated Warmer and failed READ records can refresh execution targets.
No event in this mode reaches demand features or lifecycle reconstruction.
The CLI explicitly reports `demand_enabled: false`, `coverage_status:
unknown`, and `restart_continuity: false`. Health is checked for the
forwarding watermark, not used to claim complete coverage or authenticated
foreground origin. Full READ feature/coverage integration and the per-pod
shipper remain separate work.

Duplicate identity is exactly `(pod_uid, producer_epoch, producer_sequence)`.
Only queue-admitted sequences are reserved, so retries cannot refresh target
age. Per stream, a contiguous high-water mark and at most 4,096 out-of-order
sequence positions bound deduplication. At most four epochs per registered
pod are retained; further epochs are counted and dropped rather than silently
evicting deduplication history. These are process-local operational bounds,
not restart continuity. Raw ingress `state()` explicitly raises
`NotImplementedError`; TargetMap's private checkpoint seam remains separate.
Generic normalized-v1 HTTP/replay mode and its checkpoint behavior are unchanged.
