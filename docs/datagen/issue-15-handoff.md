# Issue 15 review handoff

Issue: https://github.com/rahulgtt/predictive-cache/issues/15.
Checkout: kind-deploy:/home/automation/workspaces/sgorla/predictive-cache.
Branch: dherazs-predictive-cache; base: 01e5ef397badbed99b5385f248822ad9381f6d53.
Changes are uncommitted and unpushed for developer review.

## Acceptance evidence

| Criterion | Implementation and result |
| --- | --- |
| P1 popularity and P4 lifecycle | Seeded project Zipf selection, past-only recency, local diurnal/calendar rates and lifecycle multipliers. Controlled small recovery: fitted exponent 1.1093 against 1.1 (tolerance 0.15); weekly activity correlation 0.9999 (required >0.8). |
| P7 new-file decay | Fictional catalog additions, create/write metadata and team opens. Complete-followup sample of 687 files: daily-open Spearman -1.0 (required <-0.5). |
| P8 configurable noise | Uniform live-file mixture including archived projects. Tests cover 0, 0.2, 0.7 and 1 shares, preserving metadata; standalone-noise denominator documented. |
| Determinism and interfaces | Seed/window-split checks, multi-volume ordering, JSONL/Parquet CLI generation and cross-process artifact hashes pass. Pattern truth remains separate from events. |

## Validation and review

- Existing remote Python 3.12 environment; no dependency installation.
- Focused development checks passed; final `.venv/bin/pytest -q`: **145 passed in 266.64 seconds**, exit 0. Log: /tmp/issue15-pytest.log.
- `.venv/bin/ruff check .`, `.venv/bin/ruff format --check src tests`, and `git diff --check`: passed.
- Plan review found no blocker; implementation accounts for activity multipliers above one and unique new-file paths.
- Fresh final critic verified all 18 snapshot file hashes and archive members, inspected implementation/spec/tests, and found no material blockers or advisories. Critic did not independently rerun the suite; the passing final run completed on the same frozen source.
- Snapshot: /tmp/issue15-review-manifest.json; SHA256: 79e32192dcf993a5610018c6b9a8995250968ecab58cc4a67461f7c258610bbd. Archive: /tmp/issue15-review.tar.gz. This handoff is added after source review.

## Reviewer notes and proposed PR

Title: **[feature/ml] Generate popularity, lifecycle, new-file and noise workloads**

Closes #15. Connect P1/P4/P7/P8 to the existing generate command, retaining v1 event/catalog output. New internal/config choices: full Config passed to pattern planning, Intent.operation for create/write, diurnal_curve and initial_opens_per_day, and documented noise replacement semantics. No new dependencies.

Limitations: one-byte fixture reads, per-open sessions and fixed synthetic durations remain until #17; the runner buffers output and does not promise preset scale targets. #16 workflows, #18 faults and #19 full reports remain deferred. No modeled files or personal reads are performed; latency/cache outcomes stay null. Recovery is synthetic pattern evidence only. Reverting this change restores the prior fixture generator.
