# Predictive Cache: team quick glance

November 3-4, 2026 | Team 32 | 3-4 engineers | Python | Demo November 5

## 1. Goal and ownership

### What we are building

Prepare likely project data before users wait for cloud retrieval. A trained model scores files; the planner chooses which ones to warm within the limits. The Warmer reads the selected data through NFS.

Two deployables: the Controller receives read records, builds model inputs, scores files, chooses warming tasks and checks later demand. The Warmer performs the approved reads and can be stopped independently.

Start with the [README glossary](../../README.md#basic-terms) and [file-B example](../../README.md#example-a-feature-row-and-a-label) for features, feature rows and labels.

### The demo must show

- New demand changes the model's selections.
- A plan causes actual NFS reads through Strider.
- Later independent demand is measured against a simple policy.
- The results show latency, warming cost and foreground impact.

### Start all streams together

| Stream | Owns | First handoff |
| --- | --- | --- |
| A - Data and evaluation | Capture demand, validate origins, build replay and baselines, analyze trials. | Event schema, sample events and baseline output. |
| B - ML and planning | Features, training, inference and budgeted selection. | Ranked candidates and versioned warm plans. |
| C - Strider and Warmer | Registration, test volume, NFS client, cache probe and bounded worker. | Working read path and correlated warm results. |
| D - Optional fourth engineer | Independent trials, pressure tests, results display and submission. | Reproduced evidence and demo recording. |

With three engineers, A also owns trial validity and the display; C owns integration. Everyone contributes to the recording. Name one technical lead for scope decisions.

### Start here

By H1, agree on event, candidate, prediction, plan and result schemas. Use fixtures to unblock development while C resolves infrastructure.

At the November 3 inspection, all three vCenter nodes were Ready. Registration and test storage were still open. Recheck at kickoff.

## 2. The 48-hour scoreboard

Hours are elapsed project time, including handoffs and overnight gaps. These are targets, not promises that evidence gates will pass.

| Milestone | Target | Evidence to bring to the checkpoint |
| --- | --- | --- |
| M0 - Agree contracts | H0-1 | Owners named; schemas and fixtures shared. |
| M1 - Prove the environment | H0-4 | Registered appliance, cloud-backed test volume and mount; cold-state and persistent warming probe. |
| M2 - First learning pipeline | H1-8 | Captured session, quality checks, baseline and trained model. Real-data quality claims need valid held-out observations. |
| M3 - Close the live loop | H8-16 | Demand -> prediction -> plan -> NFS read -> correlated result. Stop control works. |
| M4 - First fair comparison | H16-24 | No warming vs simple selection vs ML, with equal issuance allowances and comparable starting states. |
| M5 - Freeze integration | H24-32 | Critical failure and pressure checks run. Freeze contracts, model choice and integration. |
| M6 - Repeat and package | H32-40 | Second engineer reproduces; results, run command and backup recording ready. |
| M7 - Submit | H40-48 | Two-minute video, Confluence write-up, GitHub link and setup instructions. |

### Bring to every checkpoint

Owner, run/commit, evidence, blocker and next action. GitHub issues and the project board hold live assignments and status.

### Keep the ML scope small

Start with one lightweight trained classifier. Use recency, frequency, size and validated co-access features. Begin with a five-minute horizon; confirm retrieval lead time before freezing it.

Compare with most-recently-used, frequency or a static hot set. Tune on validation data, then freeze the model before the untouched test period.

### Cut these first

Custom UI, a second model, application identification, weekly modeling, native CUFS changes, multiple protocols and multiple sites.

## 3. Proof, fallbacks and demo

### Do not skip these checks

- Exclude warmer, synthetic and unknown-origin events from demand. Synthetic fixtures prove plumbing only.
- Use past-only features and chronological splits. Without suitable authorized real data, model-quality evaluation stays blocked.
- Verify cold state and client-cache effects before claiming a Strider speedup. A faster second read is insufficient.
- Bound reads, concurrency, retries and duration. Test stop, partial reads, stale files and denied access. Measure foreground errors, latency and cache pollution.
- Keep normal reads independent of the Controller. Never store predictive state in CUFS volume Kvrocks.

### Decide early; report limits honestly

| When | If blocked or unsuccessful |
| --- | --- |
| H4 - Data or cache probe | Continue fixture-based interface work. Time-box diagnosis; narrow live claims. |
| H8 - No suitable real data | Finish the pipeline. Report model-quality evaluation as blocked. |
| H24 - Simple policy wins | Check validity and lead time; report the loss. Do not tune on the test data. |
| H32 / H40 | Freeze integration / have a backup recording and submission draft. |

### Measure what matters

Demand latency, warming bytes, validated remote bytes where available, and unrelated foreground impact. Issued-read limits are not hard cloud-byte limits.

### Two-minute recording

0-15s: customer pain. 15-40s: demand changes predictions. 40-75s: bounded warming and later demand. 75-105s: baseline comparison and cost. 105-120s: stop control and limits.

### Open the detail only when needed

[Detailed team plan](../predictive-cache-48-hour-team-plan.md) covers setup, contracts, ML, safety and testing. [Evidence-gate map](milestones.md) defines the separate gate checkpoints; hour-based M0-M7 IDs belong to this schedule.

[Repository](https://github.com/rahulgtt/predictive-cache) | [Project board](https://github.com/users/rahulgtt/projects/3) | [Architecture and rules](../../README.md)
