# Predictive cache warming

> **Status: design proposal, not an implemented service.** This repository
> currently contains the project documentation, not the Controller, Warmer,
> data pipeline, or benchmark described below. The implementation plan is a
> draft; feasibility and data gates must be resolved before performance claims.

## What we are trying to prove

Cold first opens can wait on data fetched over the WAN. The idea is to predict
which files or ranges a workload will need and warm them during an appropriate
window, before foreground clients ask for them. The primary outcome is lower
first-open latency **without harming foreground I/O or wasting cache capacity**.
The demonstration must establish that prediction and warming caused the
improvement, rather than merely showing a faster second read.

The original hackathon brief proposes learning from access events, a
time-of-week model and a co-access graph, a budgeted planner, read-through
warming, and feedback ([source PDF, project 1, pp. 3–5](#sources)). Its
48-hour schedule and pitch numbers are not the current delivery commitment or
measured results. The current plan spreads preparation over roughly one
working week, with all engineers starting simultaneously and spillover if
validation requires it.

## System model

Chimera is the existing SMB/NFS protocol gateway; **CUFS is its primary
production backend**. CUFS uses volume-specific Kvrocks for filesystem state.
`memfs`, `diskfs`, and other alternative backends are for testing Chimera as a
protocol server, not the production target for this project.

The proposed prototype adds **two deployables**, not one service per box in
the original sketch:

1. **Predictive Cache Controller:** ingest and validate observations, extract
   features, score candidates, make bounded plans, and correlate feedback.
2. **Cache Warmer:** independently constrained execution of those plans, with
   its own I/O, concurrency, and resource limits.

```text
Foreground SMB/NFS client
          |
          v
  Chimera protocol gateway --> CUFS --> volume Kvrocks / cache
          |                     |
          |  access observations (source and fields to be validated)
          +---------------------+
                    |
                    v
        Predictive Cache Controller
  ingest -> features -> model -> planner -> feedback
                  |              |
      predictive state           | bounded, versioned plan
                  v              v
       isolated Kvrocks     Cache Warmer
                                  |
                      MVP: bounded NFS reads
                                  v
                       Chimera --> CUFS / cache

 Controller + Warmer + foreground measurements --> OTel / Grafana
```

In the intended flow, foreground access observations become features and
predictions; the planner applies budgets before sending work to the Warmer.
The Warmer reports what actually happened, and the Controller compares that
with later **foreground** demand. Observability correlates the entire run. The
event path, feedback, and state store are proposals, not evidence that the
corresponding integrations already exist.

### Warming path and safeguards

The recommended MVP is read-only, bounded userspace NFS reads through the
existing Chimera → CUFS path. First prove that those reads actually materialize
the intended CUFS data and that the benefit survives until a later foreground
read. Verify NFS authentication, range handling, cache residency, and
measurement semantics. A direct CUFS prefetch API is a possible future
design, **not a confirmed existing API**. If read-through cannot provide a
durable effect, stop and reconsider the mechanism rather than claiming success.

The Warmer must not block foreground reads or become a foreground dependency.
Use bounded concurrency, bytes in flight, request and bandwidth rates,
per-volume and per-file limits, deadlines, cancellation, cooldown, and a
foreground-pressure pause. CPU/memory limits alone do not guarantee network or
storage bandwidth limits. Enforce an explicit byte/cache-space budget, avoid
already-resident work when residency can be established, and monitor eviction
and unrelated workloads for pollution.

Mark and **exclude warmer-origin events** from demand features, labels, and
evaluation so warming cannot recursively train or trigger itself. Validate
the marker and filtering at the actual collection point; a service account
or suppression window alone is not proof of correct attribution. Telemetry
must be bounded and nonblocking: collection or prediction failure must not
change the result of a foreground read.

### Kvrocks boundaries

| State | Role and boundary |
| --- | --- |
| CUFS volume Kvrocks | Existing filesystem state. Do not write predictive state into its private schema or treat it as a general-purpose queue. |
| Session Kvrocks | A separate instance observed historically and **proposed** for predictive events/features, scores, plans, leases, and results under isolated, versioned prefixes and TTLs. Reuse depends on verified access, command support, capacity, retention, and workload isolation. |
| Dedicated predictive Kvrocks | Fallback if session-store isolation, capacity, or compatibility is inadequate; provision only after the relevant owners approve it. Do not fall back to volume Kvrocks. |

Model artifacts belong in a versioned image or artifact store, rather than
assuming Kvrocks is a model-binary store. Validate atomic claim, lease expiry,
idempotency, bounded retries, and recovery semantics before selecting a queue
design. Separate experiment/run and volume namespaces, use least privilege,
and record retention and cleanup ownership.

## Access data and contracts

### Establish the evidence first

There is **no confirmed representative historical access dataset** in this
repository or in the supplied evidence. Ask the data and filesystem owners
which authorized structured sources exist, what time span and workloads they
cover, and whether their events can be used in the lab. Existing logs or audit
events may lack required fields. If they do, assess a minimal nonblocking
CUFS telemetry hook with the owners; do not infer missing semantics from
unstructured logs. Synthetic fixtures are useful for deterministic plumbing
and contract tests only; they cannot establish real-workload model quality or
a product-performance claim.

Proposed versioned event schema (field availability and semantics are gates):

| Field | Intended purpose and questions |
| --- | --- |
| `schema_version`, `event_id`, `timestamp` | Version, deduplicate, and order observations; assess clock quality, late arrival, loss, sampling, and duplicates. |
| `volume_id`, opaque `file_id`, current path | Identify and locate the target. An earlier proposal suggested inode plus generation; availability, reuse, rename/replacement behavior, generation semantics, and safe path resolution are **unverified**. Do not assume inode alone is globally stable. |
| `operation`, `offset`, requested and actual bytes, result | Define the accessed range and successful demand. Confirm which open/read/range and outcome details are recorded. |
| `origin` | Distinguish foreground from warmer/synthetic activity and exclude the latter. Verify this end to end. |
| Opaque cohort/session key, application | Support co-access and workload segmentation if legitimately available; cohort identity and privacy characteristics are **unverified**. |
| Fault/cache outcome, latency | Help distinguish cold demand and measure effects. Availability and the meaning of fault/residency signals are **unverified**. |

Do not collect file contents, raw usernames, credentials, or unnecessary
identifying paths. Agree with data owners on authorization, pseudonymization,
volume allowlists, access controls, retention, and deletion. Produce an
immutable dataset manifest with source, collection period, schema and
anonymization versions, counts, known gaps, and quality checks. Document
whether missing events reflect no demand or incomplete collection.

Define the prediction unit and horizon before labeling. For example, label a
file/range as useful only if a later **foreground** read consumes it within
the declared usefulness window. Construct labels after the feature cutoff;
exclude warmer events and avoid treating missing data as a reliable negative.
Ensure windows crossing the train/validation/test boundaries cannot leak
future demand.

### Controller–Warmer contract

Design a versioned, idempotent plan containing run and plan IDs, model
version, volume and validated file identity/current path, ranges, score or
reason, deadline, and maximum bytes. The Warmer validates scope and budget,
deduplicates tasks, and returns per-task status, actual bytes, timing, and
errors. Handle expired, stale, duplicate, cancelled, and partially completed
work explicitly. Exact transport and field names are proposed interfaces, not
existing APIs; the two owners should publish a shared contract before coding.

## Modeling and measurement

Start with interpretable baselines: most-recently-used, global or decayed
frequency, and a static hot set. Compare them with the proposed hour-of-week
frequency and session-based co-access models, individually and as a blend.
Candidate features include recency, frequency, time bucket, prior session
neighbors, and range/size or cohort context **only when available and
validated**. Choose prediction horizon, byte budget, and primary offline
metric before inspecting the final test partition.

Split chronologically: train on earlier periods, tune on a later validation
period, freeze model and thresholds, and evaluate on an untouched subsequent
test period. Report time boundaries, data quality and coverage, class
balance, feature availability, and model versions. Compare at the same byte
budget using precision/recall, byte-weighted precision, useful bytes, and
ablation of each component. An offline win is not proof of a live latency
win; nor should synthetic data be presented as representative history.

### Causal cold/warm benchmark

The performance owner should publish a runbook and raw, run-correlated
evidence. Compare a verified **cold control** with an equivalent cold state
followed by predictive warming and the same foreground workload. Include a
simple static/MRU warm baseline and, if useful, an oracle upper bound clearly
labeled as non-deployable. Use matched files/ranges, comparable cache state,
fixed workload and topology, randomized or balanced trial order, and
predeclared exclusions. Account for client/OS caching and natural cache
warming; the probe itself must not materially warm the measured range.

Collect enough independent valid trials to characterize variance and report
the distribution, median and tail latency, effect size, confidence intervals,
exclusions, and safety results. The draft's 30 trials, 30% gain, 60% useful
bytes, and foreground-regression limits are **proposed thresholds**, not
observations or proven outcomes. Agree on and freeze appropriate thresholds
and stopping rules before the final evaluation. If cold reset or cache
residency cannot be validated, report that limitation rather than making a
causal claim.

The dashboard should correlate a run ID across predictions, accepted/rejected
plan bytes, queue age, warm throughput/errors/latency, bytes read, subsequent
foreground useful versus unused bytes, first-open latency and fault/remote
bytes where observable, foreground p95/p99 and errors, evictions, and resource
use. Define the denominator and usefulness window for each efficiency metric.
Monitor unrelated foreground traffic too: a faster selected file does not
justify cache pollution or resource contention elsewhere. Pause or stop
warming when agreed guardrails are breached.

## Four engineers, concurrent from Day 1

Data gathering is shared across these lanes; it is not deferred until another
engineer finishes. Interfaces and fixtures enable parallel work, but fixture
results are not final evidence. A fifth integration/data engineer can own
replay, deployment, and cross-service integration if available.

| Owner | Day 1 | Handoff and next work |
| --- | --- | --- |
| **Protocol / filesystem** | Trace the deployed Chimera → CUFS read/fault path; identify event and file-identity owners; define a cold fixture and safe origin marker. | Publish access semantics and the cache probe contract; test NFS warm persistence, authentication, cancellation, and foreground isolation with Performance. |
| **ML / AI** | Define prediction unit, feature/label windows, baselines, temporal split, and a fixture contract. | Publish model and planner requirements to Data/Controller; implement and compare time-of-week and co-access models against baselines using validated observations. |
| **Telemetry / visualization** | Identify event and OTel sources, gaps, data permissions, run correlation, and collection/privacy requirements; sketch metrics and dashboard. | Deliver versioned event and metric contracts, quality checks, collection/replay coordination, and correlated dashboards to all lanes. |
| **Performance analysis** | Define workload, trial manifest, cold-state probe, safety metrics, and benchmark skeleton in parallel. | Validate reset equivalence and noise floor with Protocol; run controlled treatments, analyze foreground impact and pollution, and deliver reproducible evidence. |

The Controller's ingestion, planning, and feedback implementation is shared
between the ML and integration/data owners; the protocol owner leads the
Warmer. Agree on versioned event, plan, result, and metric contracts early;
integrate daily and surface incompatibilities immediately.

### Milestones and go/no-go gates

1. **Feasibility:** demonstrate persistent NFS read-through benefit, validate
   event/origin semantics, session Kvrocks isolation, representative data
   access, and an equivalent cold reset. Stop or redesign if these fail.
2. **Data baseline:** publish an authorized, versioned dataset and quality
   report, deterministic replay, and the initial baseline/noise-floor report.
3. **Offline model:** freeze baselines and thresholds; compare models on
   held-out real data under a fixed byte budget.
4. **End-to-end prototype:** Controller plans, Warmer executes bounded reads,
   feedback correlates, synthetic-origin suppression and failure isolation
   work, and the safety controls are exercised.
5. **Proof and readout:** run matched trials, report confidence and
   foreground/pollution impact, reproduce the result with a second engineer,
   and review the evidence and limitations before claiming success.

The draft allocates about five working days to protocol and model work and
three to four to telemetry and performance; these are estimates, not
deadlines. Do not remove the data, cold-state, safety, or reproducibility gates
to preserve the schedule.

## Environment and historical observations

The following are scoped historical observations from the supplied
conversation, **not current cluster health**:

- On **2026-10-09**, `cluster-manager` reported Kind cluster
  `ash-strider-test2` healthy on `strider-dev`. The inventory observed three
  ready nodes, a Chimera/CUFS deployment, separate volume and session Kvrocks
  deployments, and an OTel/LGTM/Grafana stack. It did not establish the
  required event fields, NFS warming behavior, or predictive-state isolation.
- On **2026-11-03**, SSH to `nas-vm` worked, but a check found no Kind clusters
  on that host. This does not place `ash-strider-test2` on `nas-vm` or prove
  its current status elsewhere.

Use `cluster-manager` to locate and inspect the intended Strider cluster
before work; use the **Kind** backend for provisioning unless explicitly
directed otherwise. Confirm the current topology, permissions, capacity, and
observability with the owners. Do not assume that historical inventory,
endpoints, or service configurations are unchanged.

Do not build or run tests in local checkouts. Use `strider-dev` or
`strider-dev2` and the designated CUFS and Chimera build containers; initialize
submodules before testing where applicable. Use existing CI-integrated
harnesses for SMB regression and unit tests rather than external-fixture-only
tests that normally skip in PR CI.

## Sources

- [Private design conversation export](https://gist.github.com/arawat-nasuni/e028801f06bc780a80e04a316532e909):
  user decisions and the 2026-10-09 Kind inventory; earlier five-service and
  direct-API suggestions were superseded by the two-deployable MVP.
- `Predictive_Cache_Warming_Implementation_Plan.md`, dated 2026-10-09:
  §§2–4 architecture and validation gates; §7 staffing, workstreams, and
  benchmark protocol; §8 proposed service and state contracts. This source
  plan is not checked into this repository.
- `Nasuni_Hackathon_Deep_Dive_Projects_1-5.pdf`, project 1, pp. 3–5:
  original motivation, component sketch, demo, and risks. The PDF is not
  checked into this repository; its internal assumptions and pitch targets
  require validation.
- The 2026-11-03 `nas-vm` observation and build/test constraints were supplied
  in the task briefing. Neither is evidence of current cluster health.
