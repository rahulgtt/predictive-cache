# Datagen decisions and model-team handoff

This note preserves the discussion behind story [#11](https://github.com/rahulgtt/predictive-cache/issues/11).
The [spec](spec.md) is the implementation contract. This note records intent,
alternatives and deferred work; it does not override the spec.

## Immediate objective and scope

This is preparation for a two-day hackathon. The immediate objective is to
provide schema-compatible synthetic access data as soon as possible so the
teammate building the model can develop against it. No project schedule has
been changed or proposed as part of this work. Start with the spec (#12), then
foundation (#13), and implement #14–#19 against their acceptance criteria.

The merged spec PR #20 supplies the starting point. This revision preserves
that work and adds the workload controls and contract clarifications below.
The active working copy is `/home/automation/workspaces/sgorla/predictive-cache`
via `ssh nas-vm`, on branch `dherazs-predictive-cache`. The Mac clone is retained
but inactive after the one-time transfer of uncommitted documentation changes.
All future edits belong in the NAS checkout, which the developer opens in
VS Code Remote SSH. Leave changes uncommitted and unpushed for the developer
to review, commit and push. No PR merge is authorized by this note.

## Why the predictor needs these events

Two use cases motivated the generator:

1. Individual workflow continuation: after A, a session often reads B; after
   A→B it may read C, while D→B may lead to E.
2. Shared demand: several independent sessions start consuming overlapping
   files, and the system should recognize growing interest in that dataset.

Globally consecutive reads from unrelated sessions do not establish A→B.
Sessions/jobs, application and timing provide workflow context; cohorts
represent teams and cannot substitute for a session. Distinct-session demand
and its growth matter in addition to total read count.

The eventual cache objective is to choose useful files/ranges within a time
window and byte/I/O budget, not merely name the next file. Existing residency,
fetch time and cache scope affect that decision. This generator has no cache
model, so cache outcome and latency remain null. A frequent file is not
necessarily cold, and a completed warmer read does not establish residency
or benefit. Labels, candidate generation, feature construction, models,
warming and evaluation remain outside #11–#19.

## One generator, two possible modes: decision

The discussion considered a common request planner with two outputs:

- **Emit:** invent a scenario and write operation records. No modeled file
  is opened. Times, outcomes and access choices are simulated.
- **Execute:** perform those requests against dedicated demo files, then log
  actual outcomes and elapsed time through a Python read wrapper.

**Only emit is in the current story.** The fictional world is metadata; even
large modeled files need not be allocated. The Python program writes dataset
artifacts, not user-file contents. It does not monitor this machine, read
personal files, or automatically capture other applications or cluster users.

A future execute mode would use a dedicated lab directory and capture only
operations routed through its wrapper. Its behavior would still be synthetic;
actual local reads would not establish human realism or Strider latency. A
CUFS telemetry hook is a separate integration, not something emit mode provides.

## Workload tuning agreed in discussion

The generator should model tasks and their context, rather than independent
uniform file choices or a fixed A→B probability alone.

| Idea | Example / agreed implementation direction | Spec / task |
| --- | --- | --- |
| Task-shaped sequences | Config/index → selected related inputs → further work; branch or stop early | P3, §7.1 / #16 |
| Longer history | A→B→C versus D→B→E; context stays within a workflow run | §7.1 / #16 |
| Personal working sets | Each actor favors a stable subset and revisits recent files | §7.1 / #14, #15 |
| Shared interests | Teams have overlapping projects and shared files | §6.4, §7.1 / #14, #16 |
| Shared-demand trigger | A review or new dataset causes staggered, partly overlapping reads | P5/P7, §7.1 / #15, #16 |
| Partial reads | Headers, random ranges and whole-file reads vary by file kind/task | §8.2 / #17 |
| Interruptions/exceptions | Unrelated open, abandonment, branching; fault cases test failures | §7.1, §9 / #16, #18 |
| Changing popularity | Lifecycle, new-file decay and workflow edge drift | P3/P4/P7 / #15, #16 |
| Different speeds | Interactive pauses, batch activity, variable inter-open delay | P3/P6, §7.1, §8.2 / #16, #17 |
| Unpredictable activity | One-off scans and random opens prevent every read being predictable | P5/P8 / #15, #16 |

All probabilities are configurable assumptions, not observations of people.
Random pauses alone do not create realistic behavior: task dependencies,
working sets, common triggers and changing intent supply the structure.
Actor-specific reads are independent draws and may coincidentally overlap.
Production-like app/job behavior matters as much as manual human opens.
Automatic retries and a full task-result dependency graph were discussed as
possible examples, but are not additional required mechanisms in v1.

## Schema reasoning and deferred integration questions

Keep raw events separate from derived model rows. Use the spec's exact names
and enums (`result=ok`, `app`, `client_id`, etc.); earlier conversational JSON
was illustrative and is superseded. In particular, no separate per-event
`provenance` field is required: `manifest.data_kind=synthetic` records dataset
provenance, while `origin` describes the actor inside the simulated world.

The schema supports stable file identity, current directory, ranges, actual
simulated bytes, operation results, session/cohort/client context and ingestion
time. Failed opens are not consumption; successful reads define bytes consumed.
Warmer/probe/unknown-origin activity is excluded from simulated demand just
as it would be in real ingestion. Truth is for generator tests only, never
model input. Data-quality faults, deduplication, clock skew, late arrival and
loss accounting are explicit requirements, not evidence of absent demand.

Before integrating real data, resolve the following with owners:

- Which cache domain is predicted: client, node or shared CUFS cache? A volume
  ID is not proof that all clients share cache residency.
- Where observations originate: application, filesystem or backend? Backend
  misses alone omit reads served by upstream caches. Include relevant hits
  as well as misses when observing demand at the chosen layer.
- Whether memory-mapped access, request start/completion, source sequence,
  file generation/content versions and reliable session/job IDs are available.
- How writes, truncation, replacements and rename affect identity/resolution.
  V1 keeps identity on rename and changes identity on replacement; it defers
  destructive churn until active handles close as a simulation simplification.
- Whether outcomes and latency really identify the relevant cache layer.
  Do not infer cache hits from elapsed time alone or invent missing fields.

Real collection requires owner-approved sources, scope, retention and access.
No contents, raw usernames or identifying paths are needed here. The original
README says no representative historical dataset is confirmed; this work
does not change that conclusion.

## Dataset handoff and limits

Deliver resolved configuration, manifest with provenance/seed/version/hashes,
events, catalog, loss counters and validation report using the spec layout.
Keep truth separately accessible to test code only. Start with `tiny` to
exercise the contract, then `small` for planted-pattern checks; preset counts
and runtime limits remain targets until measured. No training data or working
generator is claimed by this documentation change.

The model teammate can use the events to build cleaning and training code.
Potential features discussed were recent read sequences, recency/frequency,
distinct sessions, growth in demand, application and range/size context.
Simple session transitions and popularity are sensible later baselines.
Avoid using future catalog entries, pattern IDs or truth as shortcuts.

For later labels, declare prediction unit and horizon first. Only future
foreground consumption within the horizon supplies positives. Missing reads
are reliable negatives only in adequately observed windows. Count overlapping
warmed bytes once for byte usefulness; repeated accesses can separately inform
demand or latency value. Candidate recall and prefetch timeliness also matter.

More rows are not automatically more independent evidence. Repeating one
workflow or replaying it many times does not create new behavioral diversity.
Use distinct seeds and varied scenarios, keep correlated copies together, and
split chronologically with label windows protected at boundaries. Tests should
include changed overlap/noise/drift, not only repeats of training scenarios.

Synthetic outcomes demonstrate plumbing and recovery of planted structure,
not real-workload accuracy or product performance. Shadow predictions can
later measure demand alignment; actual speedup requires controlled warming
experiments with foreground latency, useful/wasted bytes, late prefetches,
backend traffic and eviction effects. Real traces would support calibration
and learning curves across independent workloads; that is later work.

## Acceptance checklist for the spec revision

- #12's original spec was merged through #20. Task issues #13–#19 already link
  the spec and their implementation sections; this revision retains those
  section numbers and adds §7.1 controls mapped to each owner.
- Verify every story/task acceptance item has a spec rule; resolve failed-open,
  read-summary, restoration, clock/loss and temporal-catalog ambiguities.
- Add regression requirements for control endpoints, interleaved actors,
  chunk boundaries and expected faults without claiming implementation passed.
- Runtime, determinism and pattern recovery remain implementation acceptance
  work in #13–#19. Nothing here closes those issues.
- Project-board updates need GitHub project scope; the current CLI token lacks
  it. Repository access works. Do not claim board bookkeeping was completed.
