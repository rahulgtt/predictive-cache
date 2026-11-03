# Agent checkpoint map

[README.md](../../README.md#milestones-and-gono-go-gates) owns the five project
gates and architecture. This map adds an agent-setup checkpoint and maps ownership and
evidence to those gates. It is not a duplicate backlog or a delivery promise.
GitHub issues and the [project board](https://github.com/users/rahulgtt/projects/3)
hold tasks, assignments, and live status. Project evidence gates below remain
unverified until linked evidence establishes them; no product services or tests
exist yet. These are named evidence gates, not alternative milestone numbers
for an hour-based team schedule. Use that schedule's IDs separately.

| Gate | Accountable responsibilities | Evidence needed to pass |
| --- | --- | --- |
| Agent setup | Coordinator + independent reviewer; [issue #2](https://github.com/rahulgtt/predictive-cache/issues/2) | Repository instructions, native executor/critic roles, milestone skill, scoped task/review records; fresh-context, leakage, clean-case, and resume smoke results from [workflow](../ai/workflow.md#activation-and-smoke-protocol). Verify GitHub/project access before claiming tracking works. |
| Feasibility | Protocol/Warmer + performance, with data/platform owners | Persistent benefit from bounded NFS reads through the deployed gateway; verified origin/events, session-store isolation, authorized representative data access, equivalent cold reset. Reinspect current environment; stop or redesign if these fail. |
| Data baseline | Data/Controller + ML + performance | Authorized versioned dataset, provenance/privacy/quality report, deterministic replay, baseline and control noise-floor reports. Unknown and warmer/synthetic origins excluded from demand. Synthetic fixtures are plumbing evidence only. |
| Offline model | ML + independent reviewer | Chronological boundaries and available-at-prediction features; frozen models, thresholds and evaluation plan; held-out real-data comparison against MRU/frequency/static baselines at equal byte budget, with raw reproducible evidence. Report a baseline win. |
| End-to-end prototype | Controller + Warmer owners, performance/observability | Versioned event/plan/result/metric contracts; Controller plans and correlated feedback, independently bounded NFS execution, origin suppression, recovery/failure isolation, foreground and pollution controls exercised. No predictive state in CUFS volume Kvrocks. |
| Proof and readout | Performance + second reproducing engineer + technical lead | Matched, verified cold/warm trials with balanced order, frozen outcomes and stopping rules, uncertainty, foreground/pollution results, private raw evidence, sanitized report, limitations, and independent reproduction. Gate review precedes any success claim. |

For 3-4 engineers, assign explicit owners across Controller/ML/data,
Warmer/protocol, and performance/observability; share responsibilities as
needed without adding deployables. Work starts concurrently where contracts
permit. Interface stubs can unblock development but cannot close real-data,
filesystem, or performance gates. Python is the initial implementation choice,
subject to the existing integration and remote build constraints.

At each gate, record the canonical issue, accountable owner, exact frozen
snapshot, required/actual evidence, independent review, decision, blocker,
and next action using the [task record](../ai/templates/task.md). Durable
decisions belong in tracked docs and the issue; raw sensitive evidence stays
in approved private storage. A reviewer finding does not change README policy.

No calendar target justifies skipping data, cold-state, safety, or reproduction
evidence. Failed gates stay blocked until corrected or an explicitly approved
design change updates README and the relevant contracts. Record limited or
negative results honestly; never substitute fixtures for efficacy evidence.

For the hour-based team schedule, use the [quick glance](team-quick-glance.md)
and [detailed plan](../predictive-cache-48-hour-team-plan.md). Those schedule
IDs do not replace the evidence gates above.
