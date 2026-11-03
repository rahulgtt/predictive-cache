# Issue 14 world-builder plan

Issue https://github.com/rahulgtt/predictive-cache/issues/14, story #11.
Host kind-deploy only; checkout /home/automation/workspaces/sgorla/predictive-cache,
branch dherazs-predictive-cache, base 11ee405. Leave work unstaged, uncommitted,
unpushed; prepare a PR description for developer review. No PR publication.

## Scope and contracts

Implement world.py build_world(cfg,rng)->World, synthetic wordlist, catalog
export, and connect the production world builder to the existing fixture
pipeline. Event patterns/realizer/faults stay stubs (#15–#18). Synthetic world
metadata does not create modeled file contents or access local/cluster files.

Volumes preserve local calendars and UTC period bounds across DST. Projects
have explicit start/phase times with sampled week ranges, piecewise-linear
ramp and cooldown, active plateau, archived multiplier. Initial projects
predate the period; Poisson new projects start inside it. Teams get 1–3 project
assignments. Actor working sets contain only initially live assigned-project
files, capped by availability; future P1 controls select/revisit those sets.
Users have jittered local HH:MM schedules, holidays, 1–2 week vacations and
application weights; service actors are separate and have no user vacations.
Future project/file availability is retained, never treated as past evidence.

Global files.target_count/users/teams/service counts are distributed across
volumes by quotient/remainder; projects.initial and new_per_month are per
volume. Sparse volumes are valid; fixture intents skip volumes without a live
file and actor. Empty populations should not crash serialization. Directory
trees use capped Poisson branching within max_depth; directory budget derives
from file target/per_dir_mean to avoid exponential growth. File placement uses
Poisson occupancy weights conditioned on the exact global target; report this
conditioning rather than claiming independent Poisson counts. Include shared
nonproject templates/standards dirs. IDs are seeded opaque values; path names
come only from a checked-in wordlist and synthetic suffixes.

Add configurable per-kind size profiles under files.size_distributions:
median_bytes, sigma (default 1.0), min_bytes, max_bytes; validate ordered bounds
and finite values. Use spec §6.3 medians/clips; lognormal then clip. Internal
File.read_profile holds full/head/ranges; media head probability 0.6. No new
catalog/event columns. Add Actor.working_set_file_ids and app_weights and
Project lifecycle metadata to internal dataclasses with backward-compatible
defaults; preserve old stub-based tests and existing helper semantics where
possible. Explicitly document any helper behavior changes. No raw user paths.

## Ownership and review

Fresh critic reviews this frozen plan first. Executor owns world.py, wordlist,
config.py/model.py extensions and targeted unit tests for extensions. Coordinator
owns pipeline/stub integration, catalog helper relocation compatibility,
independent world acceptance tests, docs/README and reports. Preserve all
concurrent edits. One fresh independent critic reviews final hashed snapshot.
All tests execute on kind-deploy with isolated ignored .tools/.venv and uv.lock.

## Acceptance and evidence

- World conforms to §11; catalog rows round-trip through every §5.2 Arrow schema
  in both formats. Referential/temporal integrity, unique paths, bounded trees,
  exact target totals, team memberships and working-set eligibility tested.
- Small preset per-kind shares within 0.03 absolute, sizes within configured
  clips and empirical medians within 25% of configured clipped medians; verify
  log-space spread/quantiles with documented sampling tolerances. Controlled
  sigma=0 overrides prove config drives output rather than hardcoded defaults.
- DST: same local user work time before/after spring transition, UTC offset
  changes; holidays/vacations suppress work; both start and end boundaries.
- Same seed identical World/catalog/data hashes; changed seed changes data.
  Fault switches do not resample world. Include multivolume and sparse cases.
- Project phase interpolation/archival and midperiod births tested; file
  creation cannot precede owning directory/project. Service/user distinction,
  synthetic naming and read profiles checked.
- Run full pytest, Ruff, source format, CLI tiny/small/medium in both formats as
  appropriate, git diff --check. Measure actual runtime without claiming later
  workload/event-count goals. Record exact commands, results and snapshot.
- Document scope and draft PR from repository template; issue remains open and
  user controls commit/push. No real-data/model-performance gate is claimed.
