# Owner-private controlled dataset

The logical dataset `controlled-nfs-read-historical`, version `v1`, is an
owner-private package on `nas-vm`. It contains the frozen five-hour controlled
synthetic NFSv4.1 READ collection used to exercise collection reconciliation,
the Chimera importer, and production cleaning.

The package is not committed to Git, published through ClearML, or available
through a repository-relative path. Its current retention is indefinite by
user decision. The owner is the `automation` account. Package roots are
owner-private mode `0700`, and package files are mode `0600`.

## Evidence boundary

This collection proves that the controlled collection and data-preparation
contracts can process a fully reconciled synthetic run. It does **not** prove
that the records represent production demand or that a model, cache policy, or
Warmer improves foreground behavior.

| Property | Repository-safe summary |
| --- | --- |
| Duration and rate | Five hours; one offered READ per minute |
| Operations | 300 offered, executed, durable, and observed records |
| Fixtures and ranges | 60 fixture identities; five non-overlapping 4,096-byte offsets per fixture: `0`, `12288`, `24576`, `36864`, and `49152` |
| Read path | NFSv4.1 `O_DIRECT`; aligned reads; no buffered fallback; Warmer disabled |
| Health evidence | 900 pre-read health rows and 63 checkpoints across three producers, with 21 quarter-hour checkpoints per producer |
| Loss and latency | Zero recorded drops; maximum monitored probe-plus-read latency `1,835.348 ms`, below the frozen `5,000 ms` safety guardrail |
| Production cleaner result | All 300 records excluded as `origin_synthetic`; zero retained demand events; five complete coverage hours |

The latency value is a collection safety observation, not a benchmark. It
must not be compared with foreground latency or used to claim cache benefit.

## Package contents

The owner-private package has a top-level `MANIFEST.json`, `SHA256SUMS`, and
`ACCESS.md`. Its logical data groups are:

| Package-relative artifact | Purpose |
| --- | --- |
| `data/live_run/manifest.json` | Frozen controlled-run identity, synthetic provenance, five-hour period, safety profile, and contract hashes |
| `data/live_run/fixture-catalog.json` | The 60 immutable fixture identities, sizes, and five allowed ranges |
| `data/live_run/offered.jsonl` | The 300 write-ahead offered operations |
| `data/live_run/execution.jsonl` | Read results recorded by the controlled harness |
| `data/live_run/durable-events.jsonl` | Provider durability evidence |
| `data/live_run/observed.jsonl` | Canonical offered-to-observed reconciliation records |
| `data/live_run/pre-read-health.jsonl` | Per-operation health barriers |
| `data/live_run/checkpoints.jsonl` | Start, quarter-hour, and final counter checkpoints |
| `data/live_run/collector-file-manifest.json` and `data/live_run/collector/` | Trusted source envelope and private raw source chunks |
| `data/live_input/` and `data/shared_provider/` | Frozen input contracts and provider evidence |
| `data/live_adapter_dataset/` | Output of `pcache-chimera-import` |
| `data/live_clean_dataset/` | Output of production `pcache-prep clean` |

The detailed records include private identifiers and must stay inside the
approved package boundary. Do not copy record contents, file listings,
checksums, or paths into issues, pull requests, chat, logs, or test fixtures.

## Request access

1. Ask the `automation` owner through the approved team access channel. State
   the engineering purpose, requested access duration, host account, and
   whether importer, cleaner, or reconciliation checks are required.
2. The owner grants a time-bound, least-privilege read-only ACL or creates an
   owner-mediated private copy. Do not change permissions, traverse unrelated
   directories, or attempt to discover the package independently.
3. The owner supplies the approved package path out of band. Set it only in a
   private shell on `nas-vm`:

   ```bash
   export PCACHE_PRIVATE_PACKAGE='<owner-supplied-path>'
   ```

4. Keep source access read-only. Write importer, cleaner, snapshot, and model
   outputs to separate fresh private directories with `umask 077`.
5. When access is no longer needed, remove only outputs you created and ask the
   owner to revoke the ACL. Do not delete, rename, rewrite, archive, or upload
   any part of the source package.

Access to `nas-vm` does not imply access to this package. This guide does not
authorize bypassing host, filesystem, or package permissions.

## Verify the package

Run verification only after the owner has supplied `PCACHE_PRIVATE_PACKAGE`.
The checksum command is silent on success and avoids publishing individual
hashes or names:

```bash
set -eu
umask 077
: "${PCACHE_PRIVATE_PACKAGE:?owner must supply the approved package path}"
test -r "$PCACHE_PRIVATE_PACKAGE/MANIFEST.json"
test -r "$PCACHE_PRIVATE_PACKAGE/SHA256SUMS"

python3 - <<'PY'
import json
import os
from pathlib import Path

root = Path(os.environ["PCACHE_PRIVATE_PACKAGE"])
json.loads((root / "MANIFEST.json").read_text())
PY

if (
  cd "$PCACHE_PRIVATE_PACKAGE"
  sha256sum --status -c SHA256SUMS
); then
  echo "Package integrity: OK"
else
  echo "Package integrity: FAILED" >&2
  exit 1
fi
```

Do not run checksum verification with verbose output in shared logs. A missing
or mismatched file is a stop condition; ask the owner rather than repairing the
package.

## Import and clean without modifying the package

Use fresh private output directories that are not inside
`PCACHE_PRIVATE_PACKAGE`:

```bash
set -eu
umask 077
: "${PCACHE_PRIVATE_PACKAGE:?owner must supply the approved package path}"
: "${PCACHE_IMPORT_OUT:?set a fresh private importer output directory}"
: "${PCACHE_CLEAN_OUT:?set a fresh private cleaner output directory}"
test ! -e "$PCACHE_IMPORT_OUT"
test ! -e "$PCACHE_CLEAN_OUT"

run="$PCACHE_PRIVATE_PACKAGE/data/live_run"
uv run pcache-chimera-import \
  --event-sources "$run/collector-file-manifest.json" \
  --observed "$run/observed.jsonl" \
  --run-manifest "$run/manifest.json" \
  --offered-ledger "$run/offered.jsonl" \
  --fixture-catalog "$run/fixture-catalog.json" \
  --checkpoints "$run/checkpoints.jsonl" \
  --out "$PCACHE_IMPORT_OUT"

uv run pcache-prep clean \
  --input "$PCACHE_IMPORT_OUT" \
  --out "$PCACHE_CLEAN_OUT"
```

Verify the expected policy result without printing private rows:

```bash
python3 - <<'PY'
import json
import os
from pathlib import Path

imported = json.loads(
    (Path(os.environ["PCACHE_IMPORT_OUT"]) / "manifest.json").read_text()
)
cleaned = json.loads(
    (Path(os.environ["PCACHE_CLEAN_OUT"]) / "manifest.json").read_text()
)

assert imported["data_kind"] == "synthetic"
assert imported["counts"]["events"] == 300
assert imported["counts"]["by_origin"] == {"synthetic": 300}
assert imported["known_gaps"] == []

assert cleaned["data_kind"] == "synthetic"
assert cleaned["counts"]["input_events"] == 300
assert cleaned["counts"]["kept_events"] == 0
assert cleaned["counts"]["excluded_by_reason"] == {
    "origin_synthetic": 300
}
assert cleaned["coverage"]["hours"] == 5
assert cleaned["coverage"]["complete"] == 5
PY
```

This zero-row cleaner result is the required production policy: controlled
synthetic activity must not become demand features, labels, or evaluation
events.

## Training and plumbing use

Use the private collection for importer, cleaner, reconciliation, schema, and
origin-exclusion checks. Do not build foreground-demand snapshots from these
300 records and do not relabel them.

Trainer plumbing is a separate repository fixture path:

```bash
set -eu
umask 077
: "${PCACHE_SNAPSHOT_OUT:?set a fresh private snapshot output directory}"
: "${PCACHE_TRAIN_OUT:?set a fresh private training output directory}"
test ! -e "$PCACHE_SNAPSHOT_OUT"
test ! -e "$PCACHE_TRAIN_OUT"

uv run pcache-model fake-snapshots \
  --out "$PCACHE_SNAPSHOT_OUT" \
  --seed 7 \
  --days 3
uv run pcache-train \
  --snapshots "$PCACHE_SNAPSHOT_OUT" \
  --out "$PCACHE_TRAIN_OUT" \
  --models baseline-mru \
  --no-track

for package in "$PCACHE_TRAIN_OUT"/packages/*.zip; do
  uv run pcache-model verify "$package"
done
```

Fake snapshots have `data_kind=fixture` and a planted signal. They establish
training and package-verification plumbing only. Neither they nor the private
controlled collection support model-quality, production-demand,
cache-benefit, or performance claims.
