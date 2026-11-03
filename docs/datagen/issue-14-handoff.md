# Issue 14 review handoff

Issue: https://github.com/rahulgtt/predictive-cache/issues/14.
Checkout: kind-deploy:/home/automation/workspaces/sgorla/predictive-cache.
Branch: dherazs-predictive-cache; base 11ee405. Leave uncommitted/unpushed.

| Acceptance | Implementation / evidence |
| --- | --- |
| World and catalog contracts | world.build_world creates volumes, lifecycle projects, bounded directories, files and actors; catalog projection round-trips unchanged v1 schemas in JSONL and Parquet. |
| Size distributions | Configurable per-kind clipped lognormal sizes; small-preset share/median/quantile tolerances and fixed-size overrides pass. |
| DST | Local schedules remain local across the New York spring transition; holiday/vacation and schedule boundaries tested. |
| Deterministic seed | Repeated world/catalog generation matches; changed seed changes output; existing subprocess artifact-hash tests pass. |

Completed on kind-deploy with Python 3.12.13 and existing uv.lock:
- `uv run pytest -q`: 130 passed in 20.37 s after final scope adjustment.
- `uv run ruff check .`, `uv run ruff format --check src tests`,
  `git diff --check`: passed.
- Wheel build includes the 24-word fictional naming resource.
- CLI small Parquet and JSONL catalogs (5,000 metadata rows), and medium
  Parquet catalog (50,000 rows), generated and validated. These were pre-final
  working-set-scope smoke checks, not workload/recovery or event-scale claims.
  Samples: /tmp/pcache-issue14-ruekwy8u. Modeled contents were not created.

The pc-milestone plan review found no blockers; its sparse-team advisory is
covered (null cohort when a volume has no teams, without cross-volume joining).
The final fresh critic verified all source hashes, ran 25 focused tests
(7.96 s), and found no material blockers for issue #14.
Final snapshot: /tmp/dherazs-issue14-review-final.json and matching tar.gz;
SHA256 0a92adf69cc1e8a2a1ac6f17dd7bde5e511db5881d522ae06b881ac45b3771c4.

The frozen original plan's working-set selection was superseded by the user's
scope clarification: #14 retains an empty internal interface, while selection
and revisit behavior belong to #16. Demand lifecycle effects (#15), patterns
(#16), actual range/event behavior (#17), faults (#18), and recovery/preset
quality work (#19) remain out of scope. Event output is still a small fixture;
cache outcome/latency remain null and dataset provenance remains synthetic.

Reviewer notes: spec §18 records global count allocation, bounded/conditioned
directory placement, configurable size sigma, internal lifecycle fields and
sparse cohort handling. No event/catalog columns changed. Personal paths and
file contents are never read. No PR was opened, and no changes were staged,
committed or pushed. Issue remains open for developer review.
