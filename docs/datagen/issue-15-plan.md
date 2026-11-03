# Issue 15 implementation plan

Base: 01e5ef3; branch dherazs-predictive-cache; kind-deploy only.
Acceptance source: issue #15 and spec §§7, 11, 13. Scope P1/P4/P7/P8.
Leave all changes uncommitted and unpushed for developer review.

## Frozen implementation scope

- Add pattern implementations against PatternGenerator. Each plans truth and
  emits time-ordered intents. P4 emits truth only. Use independent seeded
  component streams; canonical hourly substreams preserve window boundaries.
- P1: hourly Poisson demand per user, local working hours/weekdays, vacations,
  holiday multiplier and interpolated diurnal points. Choose assigned project
  uniformly, then multiply demand by its P4 activity (never normalize the
  lifecycle multiplier away). Fixed shuffled per-project Zipf ranks select
  live files. Multiply weights by 1 + exp(-ln(2)*age/half_life), using creation
  or latest past size-history time. Half-life zero disables recency. Personal
  working sets/revisits remain #16.
- P4: truth records contain lifecycle times and phase multipliers. Disabling
  P4 means activity one for existing projects, zero before project birth.
- P7: Poisson file arrivals per project-week between max(project birth,
  period start) and period end. Append fictional file/path metadata to World
  lists using existing world file sampling, in existing project directories;
  initial files.target_count remains the initial population. No file contents
  are created. Add NewFile.initial_opens_per_day (default 12), then exponential
  decay by configured half-life; select among users assigned the project.
  Plan once. Creation timing is independent of demand rate_scale; rate_scale
  scales opens. Files with no assigned user still have catalog/create/write
  metadata; use a volume actor for metadata when available. No eligible actor
  means catalog only and truth documents no event source.
- Minimal internal Intent.operation default open, accepting create/write, adds
  P7 create plus initial write at the file creation time. StubRealizer bridges
  these metadata intents to existing event schema, without implementing P9
  churn, read profiles or sessions. Creation precedes opens; paths and initial
  sizes match catalog. No measured latency/cache outcomes.
- P8: mix over base open intents using its own RNG. Bernoulli noise_share
  replaces file and actor with uniform eligible live files and volume users,
  including archived projects. Never replaces create/write intents. Base
  P1/P7 streams do not change when noise is toggled. At share 0 none; share 1
  all opens. If P1/P7 are disabled, a standalone Poisson proposal stream uses
  popularity.rate_per_user_hour and rate_scale and every retained open is noise;
  noise_share thins proposals. Report this endpoint/denominator in truth.
- randomness.rate_scale scales open rates. jitter_scale scales within-hour
  offsets around each hourly midpoint, bounded to that hour (0 midpoint,
  1 uniform, larger values clipped inside the hour). P1/P7 use the resulting
  timestamp for liveness/schedule checks. P7 file births stay uniform and are
  not jittered. P8 inherits base timing. Truth includes effective controls.
- Integrate CLI/pipeline and actual pattern truth, retaining explicitly labeled
  foundation event realization and no-fault bridge. Indexed realizer lookups
  avoid repeated linear catalog scans. Update existing fixture assertions,
  report limitations, README scope, and spec interface/mixture details.

## Verification

Controlled small-preset fixtures with unrelated patterns/faults disabled:
Zipf fitted from realized per-project open counts within 0.15; weekly project
open counts Pearson correlation above 0.8 with nonconstant lifecycle truth;
new-file daily opens Spearman below -0.5. Compute statistics from event rows
and catalog, using truth only for parameters/selection. Test noise share
endpoints and configured fractions, deterministic seed, chunk equivalence,
future/deleted-file exclusion, per-volume isolation, disabled patterns,
zero rate, holiday/vacation and jitter controls. Run tiny CLI JSONL/Parquet
validation and required pytest/ruff checks once after focused development.
No medium-scale output or physical modeled files. Recovery proves planted
synthetic behavior only. Public recovery reports/presets remain #19.

Owned files: patterns/, config.py, model.py, pipeline.py, stubs.py, io.py,
relevant tests, README.md and docs/datagen/spec.md. Coordinator owns review,
bookkeeping and final handoff. No new dependencies. Stop for a necessary
contract change outside this stated scope; preserve unrelated work.
