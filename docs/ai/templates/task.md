# Task and handoff record

Copy for one issue-sized slice. This is a working record; the GitHub issue
and project board remain canonical. Populate facts and mark unknowns.

| Field | Value |
| --- | --- |
| Issue / project item | URL, parent story/epic, observed status and timestamp |
| Evidence gate | Named README gate from [the checkpoint map](../../plan/milestones.md); team-schedule milestone ID separately if applicable |
| Coordinator / executor / critic | Named owners; review must be independent |
| Branch / worktree | Repository, branch, base revision, unique worktree |
| Scope | Allowed files/modules, concrete deliverable, excluded work |
| Shared contracts / dependencies | Versions, other owners, readiness, blockers |
| Plan snapshot | ID, digest, source revision, approved-storage evidence ID |
| Review snapshot | Plan ID/digest, base/head, full diff and file digests |

## Acceptance and checks

- Observable acceptance criteria, each tied to an issue requirement.
- README invariants affected: foreground independence; origin exclusion;
  predictive-state isolation; temporal boundaries; equal-budget baselines;
  data privacy; evidence limits. State the applicable checks explicitly.
- Targeted test/check, input, expected outcome, command, and execution host.
  Distinguish existing checks, proposed checks, fixture checks, and real-data
  or live-cluster checks. Record unavailable dependencies and stop conditions.
- Actual result: exact command, environment/version, exit status, sanitized
  outcome, and private evidence ID where needed. Never claim an unrun check.
- Rollback/recovery and explicit permissions required for external actions.

## Review and decision

- Frozen plan review: snapshot, fresh-context reviewer, finding dispositions.
- Implementation review: snapshot, fresh-context reviewer, blocking/advisory
  findings, targeted checks, and evidence of resolution.
- Acceptance decision: passed / blocked / incomplete, deciding owner, reason.
  A changed snapshot needs new review; two cycles do not waive blockers.

## Next handoff

- Current Git state and issue state, with timestamps; preserve unrelated edits.
- Completed scope, durable decisions, contract changes, evidence references.
- Remaining blockers, responsible owner, smallest next action, and dependencies.
- Exact next check and expected outcome; approvals/access still required.
- Resume by verifying revision/digests, ownership, task state, and review
  freshness. Do not infer completion from a prior chat's summary alone.
