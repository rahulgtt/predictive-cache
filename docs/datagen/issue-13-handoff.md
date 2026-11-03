# Issue 13 foundation handoff

Issue: https://github.com/rahulgtt/predictive-cache/issues/13, part of #11.
Owner: dherazs. Date: 2026-11-03. Branch: `dherazs-predictive-cache`.
Base revision: `6303d0b5fd131511f4ec721c9efa14e973a0cba0`.
Active checkout: `/home/automation/workspaces/sgorla/predictive-cache` via
`ssh nas-vm`. Work stays uncommitted and unpushed for developer review.

## Delivered scope

- Python 3.12 package, uv lockfile, pytest and Ruff, installed CLI.
- Pydantic config: tiny/small/medium, nested YAML overrides, bounds and timezone
  validation, all workload tuning controls from spec §7.1.
- Arrow schemas and enums for event, catalog, loss and truth tables; manifest
  models; typed World/Intent/domain records and extension protocols.
- Deterministic Parquet/JSONL output, UTC partitioning, config hash, artifact
  row counts/hashes and runtime provenance.
- Nonempty, seeded fixture stubs through the pipeline, reserved cache/latency
  fields null, no actual reads of modeled files and no private data collection.
- Generate, validate and describe; safe output replacement and corruption tests.

This is foundation plumbing. The fixture contains one successful handle per
configured volume (open/read/close, or open/close in summary mode). It does not
implement P1–P9, production file/actor counts, fault injection, recovery or
preset runtime/scale targets. Those remain #14–#19. Configuring those future
controls is supported; claiming they affect fixture workload behavior is not.

## Acceptance evidence

Commands ran on NAS with `/usr/bin/python3.12` (3.12.15), uv 0.13.7 and locked
dependencies. This checkout has uv in an ignored `.tools` environment; for this
session use `export PATH="$PWD/.tools/bin:$PATH"`. No shared interpreter or
other user's SSH configuration was changed for this implementation.

| Issue requirement | Evidence | Result |
| --- | --- | --- |
| Tiny generate writes §3 layout using stubs | `uv run pcache-datagen generate --preset tiny --out /tmp/pcache-issue13-RS5fZb/parquet` | Passed: 13 manifest-listed artifacts; 3 events; all catalog/loss/truth/report paths present |
| Both formats validate against §5 | Same generation with `--format jsonl`, then `uv run pcache-datagen validate` on both outputs | Passed; schema and artifact integrity only |
| Same seed gives identical file hashes | `test_subprocess_generation_determinism_and_cli` runs separate processes for both formats, compares every listed artifact hash, and tests changed seed | Passed |
| Config, schema round-trip and manifest unit tests | `uv run pytest -q` | 105 passed in 5.36 s (after review fixes) |
| Static checks | `uv run ruff check .`; `uv run ruff format --check src tests`; `git diff --check` | Passed |
| Describe command | `uv run pcache-datagen describe /tmp/pcache-issue13-RS5fZb/parquet` | Passed; reports fixture scope explicitly |

Additional tests cover schema corruption even after rehash, unsafe manifest
paths, unexpected files, existing-output and symlink protection, empty typed
tables, omitted identity fields, summary-read mode, unknown config keys,
invalid presets/overrides, fixed RNG component streams and fault toggles that
do not resample the world.

A broad `ruff format --check .` also inspects existing Markdown code examples
with the pinned formatter and reports their preexisting style. Source/test
formatting is the scoped check above; no broad Markdown reformat was applied.
No real data, live cluster, model-quality or cache-performance tests were run.

## Plan and implementation review

The `pc-milestone` workflow was used. Executors had disjoint ownership: config
and config tests; schema/domain/stubs/pipeline and their tests; coordinator
owned integration, CLI/writers, validation and docs. A fresh no-history critic
reviewed the plan before implementation; another fresh critic reviews the
implementation. Role instructions were supplied explicitly to collaboration
agents; this does not claim automatic discovery of custom roles by the runtime.

Frozen plan SHA256:
`320731e27758f660b1b99fe10c8b5d268d1cedf6b2e083986e5470312983f83a`.
Plan review found no blockers. Its advisory fixed-component RNG test was added
and passed.

Initial implementation snapshot manifest (NAS):
`/tmp/dherazs-issue13-review-snapshot.json`, SHA256
`1c159f97ca2caa3a72237b8dcb5e4c4e1ea39b05b1764a8751ee125c6cd31dd0`.
The matching `.tar.gz` freezes the 21 files for review. This handoff is a review
record, excluded from its own source snapshot. First review: two blocking P2 findings, both high confidence. Empty-partition
volume IDs bypassed path validation; revision metadata depended on caller CWD.
Both were fixed in `io.py`. Five regression cases test every partition source,
unrelated caller directories and installed-package unknown revision fallback.
The reviewer independently ran the initial 100-test suite successfully.

Repaired snapshot: `/tmp/dherazs-issue13-review-v2.json`, SHA256
`9ce28d88159a358bcb315c9c35b41014d49877690f0b0fd943073b58b4e464f8`,
with matching `.tar.gz`. Source changes since initial snapshot are limited to
`io.py` and `test_io_cli.py`. Final suite: 105 passed; Ruff and source formatting
checks passed; diff whitespace check passed. Targeted independent re-review passed: 16 tests in 5.43 s plus an unrelated
committed-repository provenance probe. The critic verified the v2 digest and
all 21 source hashes and found no remaining blockers. Both P2 findings are
closed as fixed. Issue #13 foundation acceptance is met for this snapshot;
user review and commit/push remain pending. No later workload gates are claimed.

## Contract notes for reviewers

Spec §17 records refinements: complete Arrow nullability/types, canonical
object params in pattern JSONL, manifest hashes for all artifacts, nullable
row counts for non-tabular files, runtime/limitations metadata, config merge
precedence and the deliberately small buffered fixture. Event v1 columns are
unchanged. Consumer code must not use truth as features or labels, or future
catalog values at an earlier cutoff.

The existing Python tools environment and generated files are ignored. Git
should show source, tests, docs, project metadata and uv.lock only. GitHub
project access now works and #13 is In progress. No external issue comments,
PR, commit or push was made in this implementation session. Keep the issue
open for developer review; the user will authorize add/commit/push separately.

## Next steps and recovery

Review the uncommitted diff in NAS VS Code. Re-run the acceptance commands after
any edits and invalidate review hashes if implementation changes. For the next
issue, #14 can replace the world stub through the shared types; #15–#18 consume
the same config/interfaces. #19 supplies full behavioral validation and scale
presets. Generated fixture data is not the final training dataset.

Rollback is removal/reversion of this issue's uncommitted files only, after
checking for concurrent user changes. Do not reset shared work or delete the
retained Mac clone. Dataset examples remain in the dedicated /tmp directory;
no generated dataset is proposed for Git history.

## Follow-up discussed during review: dataset sizing

The user defines a row as a read-result event for model input, not file bytes
or an entire open/read/close session. The current CLI has no exact read-count
option; `files.target_count` counts modeled files, not event rows. A future
workload-generation control should distinguish requested read-event count from
additional open/close/metadata events, and specify whether it counts attempted,
successful foreground, or emitted post-fault reads. Do not silently truncate
handles to meet a row cap. Resolve this contract with #15–#18 before adding it;
it is not part of #13's acceptance or an implemented option.

Measured with the foundation writer: 1,000 illustrative read rows occupied
615,000 bytes JSONL and 15,520 bytes zstd Parquet. This was a repetitive sizing
fixture (100 file IDs, 25 sessions, sequential times), not a generated realistic
workload or training-quality claim. Counts exclude catalog, truth, manifest,
loss and open/close artifacts. More varied data may compress substantially less.
