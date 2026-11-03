# Issue 13 foundation plan

Issue: https://github.com/rahulgtt/predictive-cache/issues/13 (assigned dherazs).
Base: 6303d0b. Branch: dherazs-predictive-cache. Active checkout:
/home/automation/workspaces/sgorla/predictive-cache on nas-vm. Leave all work
uncommitted and unpushed. Project access verified 2026-11-03.

## Scope and ownership

Implement only foundation: Python 3.12/uv scaffold, strict nested Pydantic v2
configuration (all spec controls, tiny/small/medium, deep overrides), Arrow
schemas/enums and manifest, deterministic JSONL/Parquet writers, shared world /
intent / pattern / realizer / fault interfaces, replaceable fixture stubs,
pipeline and generate/validate/describe CLI. No actual workload patterns,
cache simulation, labels or models; no local/cluster reads. All data synthetic.

Coordinator owns scaffold, io.py, validate.py, cli.py, integration, docs and
uv.lock. Executor A owns config.py and tests/datagen/test_config.py. Executor B
owns schema.py, model.py, stubs.py, pipeline.py and associated tests. These
ownership boundaries are frozen before edits; preserve other agents' changes.
Fresh critic reviews plan and final hashed snapshot, read-only, without
inheriting author history. No commits, pushes or external comments.

## Contract before delegation

Config/load_config(path=None, preset=None, seed=None, format=None) with typed
nested fields and model_dump(mode='json'); validate unknown keys and ranges.
Schema exports SCHEMAS mapping table names to Arrow schemas; validate_rows(name,
rows) and rows_to_table(name,rows); Manifest model validates §5.5. Pattern params
use canonical JSON internally for Arrow storage, patterns.jsonl retains object.
Coordinator and executor agree any final API adjustment before integration.
Pipeline run(cfg,out_dir,overwrite=False) returns Manifest. Pipeline calls writer
write_dataset(cfg,out_dir,tables,patterns,known_gaps,overwrite=False); tables keys
are events, volumes, directories, files, file_paths, actors, loss_counters,
event_truth, dropped_events. This function owns safe directory creation,
serialization, partitioning, schema checking, reports and manifest. Empty data
has typed files and directories. Deterministic ordering uses stable domain keys.
Stub data must be nonempty, seeded and honest about unimplemented controls.
Patterns and fault stubs preserve interface shape; no claim of #14–#19 delivery.

## Acceptance evidence

1. uv run pcache-datagen generate --preset tiny --out <unique /tmp directory>
   produces §3 layout and a manifest explicitly labeling foundation/stub scope.
2. Generate/validate both formats; all schema families have round-trip tests.
   Validate nullability, enum values, unknown fields, UTC microsecond timestamps;
   reject corrupt/missing schemas and malformed manifest/config. Full recovery
   and referential checks remain #19. Avoid truth leakage into events.
3. Same config/seed gives matching data-file hashes (two subprocess runs), also
   test changed seed changes data; record locked runtime dependencies. Sort
   partitions by ingest_time,event_id; preserve timezone and empty-table types.
4. Config tests cover presets, nested overrides, all §7.1 knobs, bad values and
   unknown keys. Manifest tests verify paths, row counts, SHA256 and config hash.
5. Test CLI errors and existing-output protection; --overwrite may replace only
   a validated generator-owned directory, never arbitrary user files/symlinks.
6. Run uv run pytest and uv run ruff check; format check. Docs record exact
   commands, results, snapshot hashes and review dispositions. Python tooling
   may need workspace-local bootstrap; no global/shared VM changes.

## Evidence boundaries and handoff

Data-baseline plumbing contribution only: no real-data or performance gate is
passed. Synthetic origin describes simulated probes; dataset provenance is
synthetic even for foreground rows. Cache outcome and latency remain null.
State explicitly that volume/file/user/event target sizes and workload knobs
are contracts for later implementations, not fulfilled by foundation stubs.
Readme/spec updates document nullable catalog fields and interface refinements.
GitHub board status may be updated; user retains commit/push control. Keep
issue open until user reviews; implementation and review evidence go in a
separate completion record so the frozen plan remains reproducible.
