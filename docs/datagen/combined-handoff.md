# Combined datagen handoff: issues 16–18

Base: 7ff999a; branch: dherazs-predictive-cache.
Host: kind-deploy; checkout: /home/automation/workspaces/sgorla/predictive-cache.
User requested one concise combined PR for downstream pipeline use.

## Delivered
- #16: schedules, workflow branches/context/drift/interruptions, shared bursts/scans,
  batch jobs, personal working sets and chronological revisits.
- #17: complete read profiles/ranges, sessions, handle totals, failed opens,
  effective writes, renames, deletions and replacements.
- #18: warmer/probe handles, telemetry faults, original-value restoration,
  unique truth, removals, stage denominators and reconciled hourly counters.

## Evidence
- Final regression: 161 passed in 196.47s. Log: /tmp/combined-datagen-pytest.log.
- Ruff lint/format and git diff checks passed.
- Realized-open recovery: P2 slot share 1.0 (2,961 opens); P3 controlled
  edge lifts 5/5/5/10/10 (500 opens); P5 concentration 0.1153 (55 bursts).
- Fault rate checks use controlled small-config fixtures with sufficient
  eligible samples; bounds/totals, restoration and reconciliation passed.
- Default tiny command generated/validated 34,785 events in 27 artifacts,
  about 2.8 MiB at /tmp/pcache-combined-smoke-20261103.
- Independent plan and final reviewers found no material blockers. Final
  reviewer verified the frozen source/archive and regression log, without
  rerunning the suite. Reviewed manifest SHA256:
  1d54b127a6400ec489d5d0411ae55cd0135a3f5c30c78db3b985546f15961b79.
  Subsequent CLI wording changes were separately inspected.
- Snapshot: /tmp/combined-datagen-review.json and matching .tar.gz.

No new dependencies or modeled file contents. Existing output schema/CLI
remain. Spec §20 records buffered generation, no write feedback into prior
P1 choices, historical origin-handle filtering, and deferred #19 reports/scale
targets. These results establish synthetic behavior and compatibility only.
