# Pass-1 frozen prediction schedule

This hackathon bridge scores one exact validation tick from an existing
`read-demand-v1` snapshot with one trained package and writes one immutable,
handle-free JSON schedule for a later Pass-2 consumer.

```text
collected READ dataset
→ existing read-demand-v1 snapshots
→ trained package score/explain
→ frozen prediction schedule
→ Pass-2 target resolver / Dispatcher / Warmer (separate work)
```

The command performs no collection, target lookup, Dispatcher publication,
Warmer launch, or NFS operation:

```bash
uv run pcache-model publish-read-demand-schedule \
  --snapshot /private/pass1/snapshots \
  --package /private/models/logreg-v1-....zip \
  --prediction-cutoff 2026-11-04T08:00:00Z \
  --warm-start-offset-s 0 \
  --warm-spacing-s 1 \
  --out /private/pass1/schedules
```

The cutoff must exactly match a `split=val` tick, retain unit sample weights,
and leave the complete serving horizon inside the snapshot period. The loader
reuses the canonical semantic, feature, serving-policy, runtime-capability,
package-hash, and smoke-score checks. The command accepts explicit trained
packages and legacy `logreg-v1`, `gbdt-v1`, or `gbdt-rank-v1` packages with
sealed training metadata; non-trained and baseline packages are rejected.

## Schedule v1

The filename is `schedule-<digest>.json`. The digest is the first 24 hex
characters of SHA-256 over canonical schedule JSON before `schedule_id` is
added. Repeating the same inputs is idempotent and produces byte-identical
output.

Each item contains:

- opaque `volume_id` / `file_id`;
- UTC `prediction_cutoff`;
- one-based `rank` and `warm_offset_s`;
- finite, 12-significant-digit `score` and model `reasons`;
- `logical_range` with byte `offset`, `length`, and unit.

For Pass-2 run start `S`, the consumer computes:

```text
due_at = S + warm_offset_s
warm_offset_s = warm_start_offset_s + (rank - 1) * warm_spacing_s
```

The artifact freezes the package ID/hashes, snapshot manifest and validation
file hashes, scoring-row hash, semantic hashes, exact serving policy, budgets,
ordering, time basis, rejection counts, and admitted byte total. It never
contains NFS handles, endpoints, exports, paths, or credentials.

## MVP shortcuts and remaining consumer work

- The input is an existing validation snapshot, not a new unlabeled serving
  snapshot. Restricting to unsampled validation rows with a complete future
  horizon avoids training-negative downsampling, but this remains a hackathon
  bridge rather than the final Controller inference view.
- Source and clean provenance are inherited indirectly through the snapshot
  manifest's parent binding; this command does not reopen the raw collected
  dataset.
- Only the success artifact is retained. Invalid input and zero admissions
  fail nonzero but do not publish a generalized rejection bundle.
- The logical range starts at byte zero. Verified file size may shorten it;
  otherwise the package range cap is used.
- Pass 2 must validate this schedule, bind each logical identity to a fresh
  authorized target/handle, enforce `cutoff <= pass2_run_start`, translate
  ranges into bounded schema-v2 Dispatcher items, and preserve the schedule
  and prediction IDs in result evidence.

This is plumbing evidence only. It does not establish model quality, cache
residency, NFS interoperability, performance improvement, or production
readiness.
