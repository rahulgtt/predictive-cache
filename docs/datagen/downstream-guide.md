# Using generated data in downstream pipelines

This generator produces fictional access records for integration and controlled
pattern tests. It does not observe personal files, read CUFS, measure cache
performance, or establish model quality on real workloads.

## Generate, validate, inspect

Use Python 3.12 and uv from the repository root:

```bash
uv sync --locked
uv run pcache-datagen generate --preset tiny --out /tmp/pcache-demo
uv run pcache-datagen validate /tmp/pcache-demo
uv run pcache-datagen describe /tmp/pcache-demo
```

On a checkout with an existing environment, `.venv/bin/pcache-datagen` is
also available if uv is not on your shell PATH.

Use a fresh output directory. Parquet is the default; add `--format jsonl`
for human-readable records. Use `--seed 43` for another deterministic sample.
`--overwrite` replaces only a complete, validated generator-owned dataset.

Use `tiny` for current integration work. `small` and `medium` retain their
scenario definitions, but their size/runtime targets are not yet verified or
met. Read [the measured limitations](issue-19-handoff.md) before a large run;
generation and validation buffer records in memory.

For a smaller custom scenario, save this as `scenario.yaml`:

```yaml
preset: tiny
seed: 42
period: {weeks: 1}
files: {target_count: 100}
actors: {users: 4, teams: 1, service: 1}
randomness: {rate_scale: 0.5}
realize: {read_events: ranges}
```

Then run `uv run pcache-datagen generate --config scenario.yaml --out /tmp/pcache-custom`.
files.target_count sets the initial catalog size; new/replaced files add entries.
It is not an event-row count. One open can produce multiple
reads and one close; churn and telemetry add/remove/duplicate rows. Duration,
rates, actors and read chunk size affect output size. There is no exact-N-rows flag.

For basic compatibility testing with faults disabled:

```bash
uv run python - <<'PY'
from pathlib import Path
import yaml
from predictive_cache.datagen.config import load_config
cfg = load_config(preset="tiny")
for _, fault in cfg.faults:
    fault.enabled = False
Path("/tmp/pcache-clean.yaml").write_text(yaml.safe_dump(cfg.model_dump(mode="json")))
PY
uv run pcache-datagen generate --config /tmp/pcache-clean.yaml --out /tmp/pcache-clean
```

Turning off telemetry faults does not turn off legitimate failed opens or
file changes. Those have their own `realize` and `patterns.churn` controls.

## What each consumer should load

| Output | Use |
| --- | --- |
| `manifest.json` | Dataset ID, synthetic provenance, schema/config versions, period, artifact hashes and gaps. Record dataset_id in downstream runs. |
| `events/` | Observed operation records; partitioned by volume and UTC ingestion date. |
| `catalog/` | File, directory, actor and volume metadata; file paths include effective history. |
| `loss/loss_counters.*` | Per-volume, true-UTC-hour offered/delivered/removed/duplicate accounting. |
| `report/quality.json` | Counts, distributions, fault denominators, validation and recovery diagnostics. |
| `report/summary.md` | Human-readable review of those results. |
| `truth/` | Tests and recovery only. Never model features, production cleaner input or labels treated as real ground truth. |
| `config.resolved.yaml` | Reproduce the scenario and identify enabled behaviors. |

Copy the complete directory, including manifest and truth, when sharing a
reproducible test artifact. Restrict consumer reads to the table families above.
For example, from a laptop with SSH configured:

```bash
scp -r kind-deploy:/tmp/pcache-demo ./pcache-demo
```

For larger outputs use the team's artifact storage or an archive. Commit source
and scenario configs to Git; do not commit generated datasets.

## Minimal Parquet loading example: next-file opens

Run `validate` before loading. This small-dataset example deduplicates by
event ID and selects successful simulated foreground opens:

```python
import json
from pathlib import Path
import pyarrow.parquet as pq

root = Path("/tmp/pcache-demo")
manifest = json.loads((root / "manifest.json").read_text())
assert manifest["event_schema_version"] == 1
assert manifest["data_kind"] == "synthetic"  # isolated synthetic-data experiment

rows = []
for artifact in manifest["files"]:
    if artifact["path"].startswith("events/"):
        rows.extend(pq.ParquetFile(root / artifact["path"]).read().to_pylist())
rows.sort(key=lambda row: (row["ingest_time"], row["event_id"]))

seen, opens = set(), []
for row in rows:
    if row["event_id"] in seen:
        continue
    seen.add(row["event_id"])
    if row["operation"] == "open" and row["result"] == "ok" and row["origin"] == "foreground":
        opens.append(row)
print(manifest["dataset_id"], len(opens))
```

For larger datasets read Parquet batches and retain deduplication state across
partitions. File enumeration alone is not global replay order across volumes.
JSONL uses the same columns and UTC timestamps with six fractional digits.

This is a starting filter, not the complete downstream cleaner: clock skew,
late observations, incomplete intervals and identity fragmentation still need
their own handling. Tests may use truth to check a cleaner's output; models
must not use it to repair observations or construct features.

## Event semantics that matter

- `event_id` identifies an operation; duplicate deliveries reuse it.
- `file_id` identifies the simulated file; rename preserves it and replacement
  creates a new one. An optional telemetry fault deliberately breaks this.
- `handle_id` joins open/read/close. Failed opens have no successful handle.
- `client_id` and `session_id` scope co-access; global neighboring rows from
  different actors are not a workflow.
- In ranges mode, close.handle_bytes_read equals clean read-byte totals.
  Summary mode omits read rows but retains the planned close total. An injected
  read failure can deliberately make observed read sums disagree with the close.
- `event_time` can contain simulated clock skew. `ingest_time` drives replay
  arrival order. Delayed deliveries and admitted work can extend past the
  declared demand period; the report records their extent.
- Exclude warmer, synthetic, unknown and null origins from simulated foreground
  demand. Dataset `data_kind: synthetic` describes the entire artifact;
  event `origin: synthetic` specifically means simulated probe traffic.
- Read demand additionally requires successful, nonempty, in-bounds reads.
  Null cache outcomes and latency are unknown, never zero/hit/miss.
- Missing observations in declared loss/sampling gaps are not reliable negatives.
  Use chronological splits and do not build labels across incomplete windows.
- Catalogs include future history for replay. At feature cutoff t, filter
  creation times and resolve path validity as of t; do not expose future
  replacement/deletion information to a model.

## Interpreting validation and reports

Validation checks artifact hashes/schema, references, clean reconstructed
handles/ranges, loss accounting, truth leakage and consistency of injected
faults. Expected tagged faults are diagnostics; unexplained corruption fails.

Recovery reports distinguish supported controlled checks from mixed-workload
diagnostics, insufficient samples and inapplicable patterns. A sparse or constant
signal is not a passing recovery check. Different workloads compete, so the
default mixed dataset need not preserve every isolated pattern's threshold.

Synthetic recovery proves the generator planted the requested signal. It does
not establish that a trained model will work on human access data.
