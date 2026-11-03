# Validation and operations reference

[README](../../README.md) explains the system, record formats and evidence gates.
This file keeps the detailed collection, state, failure and experiment procedures
from the earlier README. These are design requirements and proposed checks,
not evidence that the integrations or tests already exist.

## How the README and team plan now fit together

| Earlier ambiguity | Current explanation |
| --- | --- |
| Normal I/O and predictive work appeared in one pipeline. | README separates foreground reads from background observations and warming. The Controller never serves file bytes. |
| Five-day capacity estimates and a 48-hour plan both looked active. | The 48-hour plan is the hackathon target; older estimates below are historical context. Evidence gates remain mandatory. |
| Time-of-week and graph models looked like the only model choices. | A lightweight trained classifier is the plan's starting proposal. Baselines, horizon, available history and the open planner objective still determine the experiment. |
| Event examples used both `timestamp`/`event_time` and `success`/`ok`. | README teaching examples follow datagen's `event_time`, `ingest_time` and `result: "ok"`. Datagen remains a planning schema, not a verified real source. |
| File events, predictions and warm results were easy to conflate. | README gives separate records with consistent candidate/task IDs and explains prediction versus execution versus usefulness. |
| Generated foreground traffic could look like real evidence. | Dataset provenance and event origin are checked separately. A generated foreground row remains part of a synthetic dataset. |

The diagrams and JSON examples are explanatory. Actual event-source coverage,
identity resolution, plan transport, store isolation and cache/fault measurements
remain dependencies to validate. `run_id` belongs to the illustrative plan/result
and experiment metadata; it is not added to datagen v1 access rows.

## Collection and labels

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
| `schema_version`, `event_id`, `event_time`, `ingest_time` | Version, deduplicate, and order observations; assess clock quality, late arrival, loss, sampling, and duplicates. |
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

Read the [event and provenance explanation](../../README.md#data-flow-and-formats)
before interpreting an event. In generated fixtures, `origin: "foreground"`
means simulated client demand, while manifest `data_kind: "synthetic"` still
limits all results to plumbing and pattern-recovery checks. Event
`origin: "synthetic"` instead represents simulated probe/benchmark traffic.
Missing origin is unknown. None of these distinctions authorizes synthetic
efficacy claims or use of `truth/` in model inputs.

Reliable collection needs more than a schema: check offered versus emitted
events, sampling, duplicate rates, ordering, clock skew, invalid ranges,
unknown origins, dropped data, path churn, and cohort coverage. Record and
review gaps by volume and time window. Preserve a deterministic replay order
and a manifest of transforms so another engineer can reproduce a dataset.
When using a best-effort telemetry hook, quantify its losses; nonblocking
delivery protects foreground traffic but makes unqualified negatives unsafe.

## Controller-Warmer handoff

Design a versioned, idempotent plan containing run and plan IDs, model
version, volume and validated file identity/current path, ranges, score or
reason, deadline, and maximum bytes. The Warmer validates scope and budget,
deduplicates tasks, and returns per-task status, actual bytes, timing, and
errors. Handle expired, stale, duplicate, cancelled, and partially completed
work explicitly. Exact transport and field names are proposed interfaces, not
existing APIs; the two owners should publish a shared contract before coding.

The [README examples](../../README.md#3-warm-plan-a-bounded-instruction) define
`max_bytes` as requested bytes across all attempts for that example, including
retries. Freeze the shared contract with both owners before implementing it.
The cap does not establish a cloud-byte or cache-occupancy limit. Include an
execution reason, version/identity checks and all required correlation fields
in the agreed interface, even when teaching examples omit optional context.

## Warming path and safeguards

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
storage bandwidth limits. Bound issued reads and monitor actual cache headroom and remote traffic.
Do not promise a hard cache-space or WAN-byte limit without verified enforcement.
Avoid already-resident work when residency can be established, and monitor
eviction and unrelated workloads for pollution.

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

For illustration, a predictive namespace might organize keys by version and
volume, with separate records for events, features, scores, plans, task
leases, and results. Namespacing alone is **not** resource isolation: verify
quotas, access controls, retention, capacity and contention with the store
owner. A lease is temporary ownership of a task, not proof that the task
completed. On expiry, a new worker may need to retry; it must not double-count
results or let concurrent workers exceed a global budget. Confirm the
deployed store's atomic operations and failure behavior before depending on
any specific Redis-compatible command or queue design.

### Failure and state ownership

| Failure or race | Required behavior / design question |
| --- | --- |
| Event queue full or exporter unavailable | Never delay or fail the foreground read. Drop or coalesce according to an explicit policy, count the loss, and treat affected data as incomplete. |
| Controller or predictive store unavailable | Foreground reads continue normally. Stop issuing new work or expose degraded readiness; use bounded retries and queues. Do not silently substitute CUFS volume state. |
| Warmer restart, timeout, or duplicate delivery | Record partial results, make retries idempotent, and define lease expiry and ownership. A lease alone cannot prevent an old worker from continuing after expiry; the execution design must handle that race. |
| File renamed, replaced, truncated, or permission changed | Revalidate identity, allowed path and bounds near execution. Skip or fail safely rather than reading a different object or assuming a stale path is authoritative. |
| Foreground pressure or low cache headroom | Pause/cancel or reduce speculative work under agreed policy; observe actual in-flight work and foreground recovery. |
| Metrics or origin attribution missing | Surface incomplete evidence. Do not count unknown-origin traffic as demand or claim success from an unobservable run. |

## Benchmark procedures

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

#### Trial procedure and interpretation

1. Freeze workload, source/dataset, model, budgets, component versions,
   topology, outcome definitions, trial order, exclusions, and safety
   thresholds before final trials. Establish a no-warmer noise floor first.
2. Prepare equivalent cold state for the selected file/range and verify it
   without warming the measured data. Record the evidence and any uncertainty
   about the client cache, CUFS cache, and upstream data state.
3. Run the assigned treatment: no warming, simple baseline, or predictive
   plan. For warm treatments, capture the completed ranges and the interval
   between warm completion and the foreground request.
4. Execute the same foreground workload, including unrelated competing
   traffic. Capture client timings, errors, fault/remote-byte proxies, warmer
   work, cache pressure and node/resource conditions using a common run ID.
5. Repeat with balanced or randomized order, retaining the raw observations,
   all exclusions and their predeclared reasons. Analyze paired results only
   when equivalent reset and pairing are actually established.
6. Report the treatment effect and uncertainty alongside guardrail results.
   Separate a statistically detectable effect from one large enough to
   justify the cost. Have another engineer reproduce the run from the
   recorded state and instructions.

Compare first-usable-byte and initial-range completion time as distinct
measurements; an NFS `open` alone may not cause the data read being studied.
An oracle treatment that warms exactly the future demand is an upper bound,
not a deployable forecast. If a treatment warms the entire dataset, the
result says little about predictive selection. If only selected files get
faster while unrelated reads regress, the system has not met its safety goal.

#### Metrics and evidence boundaries

| Metric | Definition / interpretation |
| --- | --- |
| First-open/read latency | Client-observed duration from a precisely defined start to first usable data or range completion; report median and tail separately. |
| Useful-byte ratio | Distinct warmed bytes later read by foreground demand within the declared window divided by the chosen denominator, such as distinct successfully warmed bytes. State the denominator and attribution rules explicitly. |
| Prediction precision and recall | Correctly predicted demand relative to planned candidates and to eligible future demand, respectively; define file/range/byte units and eligibility. |
| Remote bytes or fault count | A possible coldness proxy if the source's semantics are validated; a lower count alone is not proof of better user latency. |
| Foreground latency/errors | Compare matched concurrent foreground workloads with and without warming; include unrelated data to expose contention or eviction. |
| Wasted work and cache pressure | Bytes not used before the usefulness deadline, rejected work, evictions, occupancy and relevant resource use; missing signals must be called out. |

Keep the raw trial data, workload and dataset manifests, model version, plan,
actual task results, telemetry export, analysis, exclusions and conclusion
together. A dashboard is useful for explaining a run, but it does not replace
the raw evidence or independent reproduction.

## Validation and failure gates

### Validation and test map

These are recommended future checks, **not tests that already exist in this
repository**. Each level answers a different question:

| Level | Examples | What it proves / does not prove |
| --- | --- | --- |
| Contract and unit | Event validation, unknown origins, temporal windows, deduplication, model ranking, plan budgets, overlapping ranges, stale identity and task-state transitions. | Local rules behave as specified; it does not prove NFS warms CUFS. |
| Integration / fault injection | Duplicate delivery and lease expiry, partial reads, cancellation, unavailable Kvrocks/NFS/telemetry, full event queue, and foreground reads while warming is paused or saturated. | Components handle boundaries and failures; fixture behavior alone does not establish real-world model quality. |
| Filesystem feasibility | Verify cold state, issue a bounded warm, and compare the subsequent read and fault/residency evidence through the deployed gateway. Include rename/replacement and access-permission cases. | The chosen mechanism can work in the tested environment; it does not establish broader performance impact. |
| Offline real-data replay | Chronological split, frozen models, simple baselines, quality reports and byte-budget curves on authorized representative data. | Ranking quality under the recorded workload; not live latency or safety. |
| Live controlled benchmark | Matched cold and warm treatments, foreground pressure and pollution checks, raw evidence and independent reproduction. | The measured end-to-end effect and its limits for that workload and environment. |

### Gate owners and fallback decisions

| Gate | Owner(s) | If the gate fails |
| --- | --- | --- |
| Read-through has a persistent, observable CUFS benefit | Protocol + Performance | Stop the NFS approach and investigate cache semantics or propose a direct API; do not claim warming worked. |
| Structured demand and warmer-origin attribution are trustworthy | Protocol + Telemetry/Data | Investigate an approved, bounded nonblocking hook; do not train on contaminated or unknown-origin demand. |
| Predictive state is compatible and isolated | Integration/Data + platform owner | Use a separately approved store if needed; never use the CUFS volume schema as fallback. |
| Cold reset and foreground control are reproducible | Performance + Protocol | Repair the fixture/probe or limit the result to an observational study. |
| Real data and workloads are authorized and representative | Data owner + ML | Collect appropriate evidence or report a blocked evaluation; synthetic fixtures cannot replace it. |
| Foreground safety and cache pollution are acceptable | Performance + technical lead | Pause/stop the warmer, reduce budgets or redesign; a fast selected read is not a pass. |

## Historical planning context

The earlier working-week breakdown was a capacity estimate, not the current
hackathon schedule. The active team schedule targets 48 hours and 3-4 engineers;
use the [quick glance](../plan/team-quick-glance.md) and
[detailed plan](../predictive-cache-48-hour-team-plan.md). Retain its data,
cold-state, safety and reproduction gates even when a deadline is missed.
The original protocol, ML, telemetry and performance responsibilities are
combined into the three core workstreams and optional fourth stream in that plan.

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
before work. The generic repository default is Kind, but the explicit team
choice for this hackathon is vCenter. Confirm the topology, permissions, capacity, and
observability with the owners. Do not assume that historical inventory,
endpoints, or service configurations are unchanged.

Do not build or run tests in local checkouts. Use `strider-dev` or
`strider-dev2` and the designated CUFS and Chimera build containers; initialize
submodules before testing where applicable. Use existing CI-integrated
harnesses for SMB regression and unit tests rather than external-fixture-only
tests that normally skip in PR CI.

## Sources

- [Day 1: Four parallel assignments](../../artifacts/pdf/day-one-assignments.pdf): the
  supplied one-page ownership, tasks, and handoff summary.
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
