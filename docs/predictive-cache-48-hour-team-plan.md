# Predictive Cache

For a short team briefing, use the [quick glance](plan/team-quick-glance.md)
or the [three-page PDF](../artifacts/pdf/predictive-cache-48-hour-team-plan.pdf).
This document keeps the full setup, contracts, ML and test details.

48-hour hackathon team plan | November 3-4, 2026 | Demo November 5

Customer promise: prepare likely project data before users wait for cloud retrieval, while protecting foreground work and limiting speculative reads.

**The deliverable**

A Python Controller learns from validated demand history and selects a bounded warm plan. A separate Python Warmer executes read-only NFS reads through Strider. A repeatable experiment compares ML selection with simple policies.

### What must work by the end

- A trained model changes its predictions as new demand arrives; plans are not hardcoded.

- The Controller sends a versioned plan to the Warmer, which records actual results.

- A later independent demand read is measured, with warming cost and foreground impact.

- Results compare no warming, simple MRU/frequency selection and ML at the same issuance budget.

- A second engineer can reproduce the result, or the team clearly reports which evidence gate remains blocked.

### System boundary

```text
Validated demand events -> Controller -> bounded plan -> Warmer -> NFS -> Strider
                               ^                         |
                               +---- correlated results -+
```

Scope: 3-4 engineers; Python only; one vCenter cluster, one volume, one protocol and one bounded candidate set. Platform-neutral Controller interfaces; Strider adapter first. Go requires a separate reason and user approval.

### Starting point and repository

Observed November 3: three Ready RKE2 nodes; Chimera/CUFS running; appliance-cm 1/2 Ready; no Volume resources or NFS mounts observed. Registration and test storage remain the first dependency. Historical health is not a substitute for the hour-zero check.

Remote: [github.com/rahulgtt/predictive-cache](https://github.com/rahulgtt/predictive-cache)
Local: /Users/rgattu/cdroot/predictive-cache
Repository reviewed at 48c8efe. The codebase is documented as a design proposal.

Planning note: this is the requested 48-hour schedule proposal. The README explains the system and retains its data, safety and evidence gates. Earlier working-week estimates are historical context. vCenter follows the explicit team decision.

## Milestones and gates

Elapsed project hours, including overnight handoffs; not 48 continuous working hours.

| Milestone | Window | Exit evidence |
| --- | --- | --- |
| M0 - Contracts and ownership | H0-1 | Name lead and owners. Freeze event, candidate, prediction, plan and result schemas. All streams run against fixtures. |
| M1 - Environment feasibility | H0-4 | Registration, dedicated cloud-backed test volume/export and client mount. Checksummed read/write. Supported cold-state probe/reset and persistent NFS warming benefit investigated. |
| M2 - Data and first model | H1-8 | Captured session and quality report; baseline and training pipeline run. Held-out quality evaluation only if authorized real observations and complete windows are available. |
| M3 - Integrated prototype | H8-16 | An event changes a score; a plan triggers actual NFS reads; independent demand and warm results share a run ID. Origin filtering and stop control work. |
| M4 - First valid comparison | H16-24 | First matched comparison across no warming, simple selection and ML. Raw latency, traffic and foreground evidence; missing measurements identified. |
| M5 - Safety and integration freeze | H24-32 | Exercise failures, cancellation, pressure and cold start. Fix critical issues. Freeze contracts, model choice and integration at H32. |
| M6 - Repeat and package | H32-40 | Repeat valid trials; freeze dataset/model/configuration. Second engineer reproduces. Result bundle, demo command and backup recording exist. |
| M7 - Record and submit | H40-48 | Two-minute video, Confluence write-up, GitHub code link, setup/run instructions, evidence and explicit limitations are ready. |

**Protect the critical path**

Start data/model fixture work at H0 while one owner resolves infrastructure. By H4, select the live comparison method or narrow the deliverable. Do not spend day one waiting for a volume.

### Integration checkpoints

H1: publish contracts and fixture files. H4: environment/data go-no-go. H8: model and baseline output review. H16: integrated run. H24: evidence review. H32: freeze. H40: submission rehearsal.

At each checkpoint record owner, run/commit, evidence, unresolved dependency and next action. A schedule deadline does not waive an evidence gate.

## Parallel ownership

Three core streams; a fourth engineer strengthens validation and presentation.

| Stream / owner | Responsibility | Handoff |
| --- | --- | --- |
| A - Data and evaluation<br>Engineer 1 | Demand capture, origin/data-quality validation, deterministic replay, baselines and experiment analysis. | Versioned event dataset and manifest; baseline rankings; trial results. |
| B - ML and planning<br>Engineer 2 | Features/labels, trained model, inference, bounded candidates and plan selection. Synthetic fixtures only for interface tests. | Model artifact and feature schema; ranked candidates; immutable warm plans. |
| C - Strider and Warmer<br>Engineer 3 | Registration/volume/mount; read-through and reset probe; NFS adapter; bounded execution and integration. | Validated cache probe; plan consumer; per-task results and stop control. |
| D - Performance and demo<br>Optional Engineer 4 | Own independent trial validity, pressure tests, repeatability, correlated display and submission packaging. | Reproduced result bundle; readable display; video and Confluence draft. |

With three engineers, A owns trial validity and the terminal results view. C owns integration; all contribute to recording. With four, D takes over performance validation and presentation from A.

Name a technical lead to decide scope and gates, not to implement every integration.

### First-hour contracts

| Record | Required fields |
| --- | --- |
| Demand event | schema_version, event_id, run/session, event time, volume/file identity, operation/range, actual bytes, result, origin; latency if validated. |
| Candidate / prediction | Identity/version, size/range, eligibility; score, horizon, prediction time and model version. Unknown identity or origin must remain explicit. |
| Plan / task | Run/plan/task IDs, model version, validated target and ranges, deadline, issuance limit, score/reason. Immutable after dispatch. |
| Result / run manifest | Requested and actual bytes, timing, outcome and retry/partial state. Dataset, software, model, topology, settings and initial-state procedure. |

### Keep the implementation small

Keep ingestion, features, inference, planning and feedback in one Controller deployable. Keep the Warmer separately stoppable.

Use fixture plan/result files so streams can work independently from the start. Event and plan files are an interface harness, not proof of a production queue.

If predictive runtime state is needed, validate isolated, versioned prefixes and TTLs in session Kvrocks. Never write predictive state into volume Kvrocks. A dedicated store requires owner approval. Model artifacts are versioned files/images, not filesystem-state records.

## The ML brain

Learn future demand first; apply cache and resource policy separately.

**Initial prediction target**

For an eligible file or range, will successful foreground demand consume it in the next five minutes, starting at time t? Five minutes is a proposed starting horizon; freeze it after measuring retrieval lead time.

A positive demand prediction does not establish an avoided cache miss.

### Training and inference sequence

- Define observations: validate existing structured sources first. An instrumented Python client is a proposed bounded-scope source; validate its origin and coverage. It does not observe arbitrary applications.

- Generate candidates: a bounded set of approved files, recent neighbors and project-related candidates. Record eligibility and candidate coverage; do not scan the whole volume on every tick.

- Build features: recency, recent frequency, size, project activity and training-derived co-access. Add time features only if supported by enough representative history.

- Train: begin with an interpretable lightweight classifier, such as regularized logistic regression. Compare with MRU and frequency/static hot set. A tree model is optional after the pipeline works.

- Infer and plan: score current candidates, revalidate target/version, rank expected benefit versus cost and emit a bounded plan. Treat scores as scores until calibration is validated.

### Data gates that cannot be skipped

| Requirement | Acceptance check |
| --- | --- |
| Authorization and privacy | Immutable source/schema manifest; approved scope/retention; opaque identities. No file contents, raw usernames, credentials or real access datasets committed. |
| Origin and coverage | Exclude warmer and synthetic events. Unknown origin is not demand. Quantify missing/duplicate events and successful returned bytes. |
| Temporal validity | Chronological train/validation/test periods. Past-only features and preprocessing fit on training only. Prevent overlapping label windows across boundaries. |
| Reliable labels | Only complete observation horizons produce negatives. Missing collection means unknown. Count distinct overlapping bytes, not repeated reads as additional useful bytes. |
| Frozen evaluation | Tune on validation; freeze model, thresholds, horizon and byte budget before untouched test data. Preserve feature/model versions and replay order. |

Evidence boundary: repository policy permits synthetic fixtures for plumbing and contract tests only. They cannot support model-quality or product-performance claims. If suitable real data is unavailable, deliver the training/inference pipeline and report quality evaluation as blocked. Two days of new data cannot establish weekly prediction quality.

## Execution and safety

Read-only warming with explicit limits, attribution and a reversible rollout.

### Environment setup checklist

- Register the appliance using private serial/auth inputs; create a dedicated authorized cloud-backed volume and NFS export. Keep secrets out of Git, logs and plan artifacts.

- Confirm client access, permissions, checksums and backend preservation. Define what cold means at the tested CUFS/cache layer. Use only supported eviction/reset after preservation is verified.

- Create a project Python virtual environment; pin the working dependency set and record versions. Keep cluster deployment tools separate from project dependencies.

- Validate event capture, NFS warmer origin, clocks, run IDs and pressure/fault metrics. Do not assume OTel spans or atime are complete demand history.

- Validate predictive-state compatibility and isolation before reuse of session Kvrocks. Assign collection, cleanup and experiment ownership.

### Worker controls and failure behavior

| Boundary | Required behavior |
| --- | --- |
| Scope and identity | Allowlisted volume/path/ranges; recheck identity, size/version and authorization. Skip renamed/replaced/deleted or expired targets safely. |
| Resources | Bound concurrency, issued bytes, bytes in flight, request rate, duration, retries and per-file work. Include retry/overlap accounting in limits. |
| Pressure and cancellation | Pause on agreed foreground/cache pressure signals. Stop new reads immediately on cancellation; report residual in-flight work. Unknown pressure permits only an explicit conservative lab mode. |
| Errors and restart | Timeout/partial/error results are explicit. Stable task IDs and durable completion records prevent blind repeat execution. Test duplicate and restart behavior before relying on leases. |
| Failure isolation | Controller/store/telemetry failure stops or degrades speculation. Normal foreground reads never depend on the predictive system. Telemetry queues are bounded and nonblocking. |

**Budget wording matters**

An NFS read limit bounds issued/read bytes, not necessarily cloud bytes or cache occupancy. Existing fetch granularity and prefetch may amplify traffic. Measure the relationship before claiming a hard cloud/cache budget.

Rollback: stop the Warmer and Controller, restore the last known model/configuration and remove only project-owned instrumentation.

Already warmed data may remain; reset the experiment before further comparisons. Do not modify CUFS schemas or introduce automatic pinning for this MVP.

## Testing and proof

The experiment must distinguish good prediction, actual warming and incremental benefit.

| Test level | Minimum checks / evidence |
| --- | --- |
| Contract and unit | Event validation, unknown origin, deduplication, feature cutoffs, completed label windows, ranking, overlapping ranges, plan limits and task states. |
| Integration and failure | A plan executes through NFS and returns correlated results. Test denied/deleted/changed files, partial reads, duplicate tasks, worker restart, timeouts, stop and missing telemetry/state. |
| Filesystem feasibility | Verified cloud preservation and cold state; bounded warm; later independent read benefits. Separate client cache effects, open timing, first usable data and range completion. |
| Offline real-data replay | Chronological frozen partitions; MRU, frequency/static and ML under equal byte allowances. File/byte ranking curves and data-quality report; no synthetic efficacy claims. |
| Live controlled benchmark | Equivalent starting states, balanced/randomized treatment order, concurrent unrelated foreground work, raw measurements and independent reproduction. |

### Three treatments, one experiment protocol

Control: normal cache behavior without added warming. Simple: MRU/frequency warm selection. ML: learned selection. Hold eligibility, issued-byte allowance, client behavior, workload, topology and existing prefetch settings constant.

Before final trials, freeze initial-state verification, trial order, exclusions, safety thresholds, horizon and stopping rules. Establish the no-warmer noise floor. Keep every valid and excluded run with its reason; another engineer reproduces the runbook.

| Metric | Interpretation |
| --- | --- |
| Demand latency | Precisely define first usable data and range completion. Report median and tail only with adequate samples; include trial count, variance and uncertainty. |
| Cost and safety | Total remote/cloud bytes where validated; issued/returned warming bytes; demand errors; unrelated foreground latency; cache pressure/evictions where observable. |
| Usefulness | Distinct warmed bytes later consumed within the window. Useful bytes are not automatically incremental misses avoided; the latter needs matched controls and trustworthy attribution. |

**When cold reset is unavailable**

Use matched datasets and repeated randomized assignment only as an observational fallback, with explicit confounding. Do not claim a causal Strider speedup. If real data or attribution is unavailable, report that gate as blocked; fixture replay remains an interface demonstration.

## Finish, fallbacks and handoff

Protect the evidence gates; reduce features when time runs short.

| Deadline | Decision and fallback |
| --- | --- |
| H4 | No cloud-backed probe, trusted demand source or equivalent reset: keep fixture work moving, time-box integration diagnosis, and narrow live claims. |
| H8 | No usable real data: finish baseline/training/inference interfaces using fixtures, but mark quality evaluation blocked. No evidence thresholds waived. |
| H16 | No live prediction-to-read cycle: freeze interfaces and prioritize one demonstrable path; defer added models and UI. |
| H24 | ML does not beat the baseline: inspect validity and lead time, report the loss, retain simple policy as operational fallback. Do not tune on the test period. |
| H32 / H40 | Freeze integration at H32. By H40, retain a backup recording, reproducible result bundle and complete submission draft. |

Cut first: custom UI, second model, application identification, weekly modeling, native CUFS changes, multiple protocols/sites. Keep origin validation, foreground safety, cold-state validity and reproducibility.

### Two-minute recording

0-15s: customer problem. 15-40s: new demand changes learned selections. 40-75s: bounded NFS warming and later independent demand. 75-105s: measured baseline comparison with traffic, waste and safety. 105-120s: stop control, evidence limits and adoption work. Change project activity or the allowance for a reactive moment.

### Submission and team handoff

- GitHub: code link, pinned dependencies, setup/run commands, model/feature schema, tests and rollback.

  Keep issue/PR work aligned with the existing epic and project board; this PDF export performs no publishing.

- Evidence: dataset manifest, approved data location, model artifact/version, plan/results, raw trials, analysis, exclusions and known gaps. No secrets or real access data committed.

- Confluence: problem, theme, mechanism, what actually works, result and uncertainty, future work, code and video links. Record owners for all open gates.

### Environment quick reference

Development host: vm hackathon / 10.84.57.145
API VIP: 10.84.252.113:6443 | Protocol VIP: 10.84.252.114
VM kubeconfig: /home/automation/.kube/config-hackathon-cache-vcenter
Node IPs: 10.84.33.135, 10.84.33.132, 10.84.33.136
Recheck health and registration at kickoff; these details are not a guarantee of current state.

### Source of truth and planning status

[Repository README and AGENTS.md](https://github.com/rahulgtt/predictive-cache) reviewed at 48c8efe; chat decisions define Python, 3-4 engineers, vCenter and 48 hours. [Event execution details](https://nasuni.atlassian.net/wiki/spaces/ENG/pages/6535577689/2026+Q4+-+Hackathon+Execution+Details) define submissions. This is a proposed schedule, not an implementation or benchmark result. Repository design files were not changed.
