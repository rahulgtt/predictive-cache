# Datagen-to-Warmer offline MVP

This runbook exercises issue #112's **controlled fixture integration path**:

```text
datagen successful READs
→ read-demand-v1 clean tables
→ read-demand-v1-narrow-mvp inference snapshot
→ non-trained baseline-read-frequency package
→ bounded schema-v2 READ plan
→ Dispatcher skipped/dry_run results
```

It performs no NFS read and provides no model-quality, cache-residency,
performance, representative-demand, or production-readiness evidence. The
baseline is explicitly `trained=false` and `data_kind=synthetic`.

## Preconditions

Run on `nas-vm` in the designated CUFS or Chimera build container after
rechecking the target and initializing submodules. Do not run this workflow in
a local checkout. Use a fresh owner-private directory outside Git:

```bash
git submodule update --init --recursive
uv sync --locked

umask 077
export WORK_ROOT="$HOME/predictive-cache-mvp-112"
test ! -e "$WORK_ROOT"
mkdir -m 0700 "$WORK_ROOT"

# ClearML must be offline and unconfigured for this path.
export PCACHE_TRACKING=off
unset CLEARML_API_HOST CLEARML_WEB_HOST CLEARML_FILES_HOST
```

The target map contains opaque handles and target identities. It must remain
owner-only and must come from the fixture owner. This runbook never discovers,
guesses, derives, or substitutes an export, handle, path, size, or identity.

## Build the verified snapshot and baseline

Generate and validate the synthetic source. The additive projection is kept as
semantic evidence; cleaning still consumes the validated datagen dataset:

```bash
uv run pcache-datagen generate --preset tiny --out "$WORK_ROOT/source"
uv run pcache-datagen validate "$WORK_ROOT/source"
uv run pcache-datagen project-read-demand \
  "$WORK_ROOT/source" --out "$WORK_ROOT/read-projection"
uv run pcache-prep clean \
  --input "$WORK_ROOT/source" \
  --semantic-view read-demand-v1 \
  --out "$WORK_ROOT/clean"

TICK="$(
  uv run python - "$WORK_ROOT/source/manifest.json" <<'PY'
import json
import sys
print(json.load(open(sys.argv[1]))["period"]["end"])
PY
)"

uv run pcache-model build-read-demand-snapshot \
  --clean "$WORK_ROOT/clean" \
  --source-manifest "$WORK_ROOT/source/manifest.json" \
  --tick "$TICK" \
  --out "$WORK_ROOT/snapshot"

# A dirty reviewed worktree must be identified by its sealed patch or source
# bundle. Clean committed source omits this option.
SOURCE_PATCH_ARG=
PUBLISH_SOURCE_PATCH_ARG=
if test -n "$(git status --porcelain --untracked-files=normal)"; then
  : "${SOURCE_PATCH_SHA256:?set the reviewed patch/source-bundle SHA-256}"
  SOURCE_PATCH_ARG="--source-patch-sha256=$SOURCE_PATCH_SHA256"
  PUBLISH_SOURCE_PATCH_ARG="--expected-source-patch-sha256=$SOURCE_PATCH_SHA256"
fi
REPOSITORY_REVISION="$(git rev-parse HEAD)"

PACKAGE="$(
  uv run pcache-model build-read-frequency-package \
    --snapshot "$WORK_ROOT/snapshot" \
    $SOURCE_PATCH_ARG \
    --out "$WORK_ROOT/packages"
)"
uv run pcache-model verify "$PACKAGE" --snapshot "$WORK_ROOT/snapshot"
uv run pcache-model inspect "$PACKAGE" --snapshot "$WORK_ROOT/snapshot"
```

The snapshot candidate profile is `read-demand-v1-narrow-mvp`. It admits only
visible eligible synthetic controlled/fixture READ identities, requires
complete `read_count_24h` coverage, and ranks by count, recency, `file_id`, then
`volume_id`. It has no OPEN/CLOSE, application-session, project, directory,
write, `cold_open`, file-size, or anchor/co-read dependency.

## Supply the approved fixture target map

Set `FIXTURE_TARGET_MAP` to the freshly approved owner-private JSON file. Its
directory must be mode `0700`, the file mode `0600`, and both must be owned by
the current UID. The publisher accepts exactly this top-level contract:

```text
schema_version=1
target_map_id
data_kind=synthetic
authorization=approved-fixture-dry-run
snapshot_id
snapshot_manifest_sha256
source_dataset_id
clean_dataset_id
candidate_profile_sha256
targets[]
```

Each target has exactly `approved`, `volume_id`, `file_id`, `export_name`,
`export_id`, `target_fh_b64`, `target_valid_until`, `offset`, and `length`.
Only `approved=true` entries may be admitted. The map must bind the exact
source, clean dataset, snapshot manifest, and candidate-profile hash. Missing,
duplicate, malformed, unauthorized, stale, over-TTL, or mismatched targets
fail closed or are recorded as explicit rejections.

```bash
export FIXTURE_TARGET_MAP='<owner-supplied absolute path>'
test -f "$FIXTURE_TARGET_MAP"
test "$(stat -c '%a' "$(dirname "$FIXTURE_TARGET_MAP")")" = 700
test "$(stat -c '%a' "$FIXTURE_TARGET_MAP")" = 600
test "$(stat -c '%u' "$FIXTURE_TARGET_MAP")" = "$(id -u)"
```

## Publish and dispatch without real reads

Create a fresh Dispatcher root and obtain the freshly sealed target map
immediately before publication. Every admitted target must satisfy
`created_at < deadline <= target_valid_until`, and target TTL from creation
must be at most 300 seconds. If the supplied validity does not cover the
computed deadline within that bound, stop and request a new approved map.

```bash
export DISPATCHER_ROOT="$WORK_ROOT/dispatcher"
mkdir -m 0700 "$DISPATCHER_ROOT"

CREATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
DEADLINE="$(
  uv run python - "$CREATED_AT" <<'PY'
import sys
from datetime import datetime, timedelta
t = datetime.fromisoformat(sys.argv[1].replace("Z", "+00:00"))
print((t + timedelta(seconds=120)).isoformat().replace("+00:00", "Z"))
PY
)"

PUBLISH_OUTPUT="$(
  uv run pcache-model publish-read-demand-plan \
    --snapshot "$WORK_ROOT/snapshot" \
    --package "$PACKAGE" \
    --target-map "$FIXTURE_TARGET_MAP" \
    --dispatcher-root "$DISPATCHER_ROOT" \
    --run-id run-datagen-warmer-mvp \
    --created-at "$CREATED_AT" \
    --deadline "$DEADLINE" \
    --expected-repository-revision "$REPOSITORY_REVISION" \
    $PUBLISH_SOURCE_PATCH_ARG
)"
printf '%s\n' "$PUBLISH_OUTPUT"

WARMER_COMMAND="$(realpath .venv/bin/pcache-warmer)"
uv run pcache-dispatcher run \
  --root "$DISPATCHER_ROOT" \
  --warmer-command "$WARMER_COMMAND" \
  --host 192.0.2.10 \
  --no-real-reads \
  --max-bytes-per-min 67108864 \
  --max-items-per-min 60 \
  --once
```

`192.0.2.10` is a documentation-only numeric address. With
`--no-real-reads`, the Dispatcher must not start the Warmer or contact it.
Nonzero rate limits are still required so this invocation uses the same
bounded configuration shape as an enabled run.

If publication admits zero items, it writes no plan, writes an owner-only
rejection/provenance report, and `pcache-model` exits **3**. Do not dispatch.

## Assert the exact dry-run result

The following assertion checks the immutable plan/provenance index, exact
run/plan/task IDs, expected hash references, terminal `skipped/dry_run`, zero
bytes, exact byte accounting, and absence of ambiguity:

```bash
uv run python - "$DISPATCHER_ROOT" <<'PY'
import hashlib
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
indexes = sorted(
    p for p in (root / "indexes").glob("plan-*.json")
    if not p.name.endswith("-rejection.json")
)
assert len(indexes) == 1, indexes
index = json.loads(indexes[0].read_text())
plan_path = root / index["plan_path"]
if not plan_path.exists():
    plan_path = root / "claimed" / plan_path.name
provenance_path = root / index["provenance_path"]
sha = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
assert sha(plan_path) == index["plan_sha256"]
assert sha(provenance_path) == index["provenance_sha256"]

plan = json.loads(plan_path.read_text())
provenance = json.loads(provenance_path.read_text())
assert plan["schema_version"] == 2
assert plan["run_id"] == provenance["run_id"] == index["run_id"]
assert plan["plan_id"] == provenance["plan_id"] == index["plan_id"]
assert plan["max_bytes"] >= sum(item["length"] for item in plan["items"])
assert all(item.get("operation", "read") == "read" for item in plan["items"])
assert all(item["max_bytes"] == item["length"] for item in plan["items"])

expected = {entry["task_id"]: entry for entry in provenance["tasks"]}
results_dir = (
    root / "runs" / plan["run_id"] / "plans" / plan["plan_id"] / "results"
)
tasks_dir = results_dir.parent / "tasks"
results = [json.loads(path.read_text()) for path in sorted(results_dir.glob("*.json"))]
assert len(results) == len(plan["items"]) == len(expected)
for result in results:
    task = expected[result["task_id"]]
    wanted = task["expected_dispatcher_result"]
    assert result["run_id"] == plan["run_id"]
    assert result["plan_id"] == plan["plan_id"]
    assert result["status"] == wanted["status"] == "skipped"
    assert result["reason"] == wanted["reason"] == "dry_run"
    assert result["bytes_requested"] == wanted["bytes_requested"]
    assert result["bytes_read"] == wanted["bytes_read"] == 0
    assert result["bytes_read_is_lower_bound"] is False
    assert result["warmer_terminal"] is None
    assert wanted["plan_ref"] == hashlib.sha256(
        plan["plan_id"].encode("ascii")
    ).hexdigest()[:32]
    assert wanted["task_ref"] == hashlib.sha256(
        result["task_id"].encode("ascii")
    ).hexdigest()[:32]
assert not tasks_dir.exists() or not list(tasks_dir.iterdir())
assert not list(results_dir.parent.glob("ambiguous*"))
print(f"verified {len(results)} exact skipped/dry_run results; zero bytes")
PY
```

Any budget, rate, deadline, target-expiry, identity, result-count, status,
reason, byte, hash-reference, or ambiguity difference fails this run. Preserve
the owner-only artifacts; do not retry automatically.

## Separate one-READ live gate — do not run here

A later live validation requires a new explicit approval naming one target and
authorizing exactly one READ. Freeze and review all of these conditions first:

1. numeric NFSv4.1 endpoint and AUTH_SYS UID/GID;
2. one approved `(volume_id, file_id, export_name, export_id)` fixture;
3. trusted wire handle, named human attester, authorized acquisition source,
   identity-binding evidence, target-map checksum, and validity no more than
   300 seconds after plan creation;
4. offset `0`, length/max bytes `4096`, one item, one in-flight task, nonzero
   rate limits capped to that read, and a short deadline;
5. owner acknowledgement that this is the first live handle-mode
   interoperability validation, distinct from the earlier path-mode read;
6. a fresh dedicated owner-only `0700` Dispatcher root with empty pending,
   claimed, running, ambiguous, and completed inventories; preflight records
   one approved plan digest and refuses a second plan;
7. an empty owner-only `enable-real-reads` file created only after preflight
   and removed immediately after the terminal result;
8. reviewed STOP procedure and confirmation that enable-file removal prevents
   any new task;
9. provider/telemetry health capable of recording the warmer marker;
10. expected terminal result/exit-code agreement and exact plan/task hash refs;
11. retrieval of the exact collector event bound to those refs, trusted warmer
    attribution, and replay through the same cleaner/snapshot path proving no
    candidate, feature, or label changes; missing, unknown, or foreground
    attribution fails without retry;
12. rollback removes the enable file, creates STOP, preserves evidence, never
    automatically retries ambiguity/mismatch, and follows a reviewed residual
    NFS server-state check/escalation path for an unanswered or incompletely
    cleaned-up OPEN/session.

No command in this document authorizes or performs that live gate.
