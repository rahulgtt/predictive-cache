---
name: pc-milestone
description: Coordinate a Predictive Cache milestone with bounded execution agents, independent criticism and evidence-based handoffs. Use for project planning or delivery across workstreams, rather than isolated factual questions.
---

Read the repository's AGENTS.md and README.md. Use
[the workflow](../../../docs/ai/workflow.md) for agent boundaries and
[milestones](../../../docs/plan/milestones.md) to locate the current evidence gate.
Existing issues and the project board remain the work backlog; reconcile them
with the current checkout before choosing tasks.

## Frame and assign

- Preserve settled decisions. Define the requested deliverable, acceptance
  evidence, authorization, owner and dependencies using the
  [task packet](../../../docs/ai/templates/task.md).
- Ask a fresh `pc-critic` to review consequential plans before implementation.
  Give it the requirements, frozen proposal and supporting evidence. Where
  supported, spawn with `fork_turns="none"`; otherwise use a separate review
  session. If independent context is unavailable, disclose that limitation.
- Delegate independent implementation to `pc-executor`, with explicit file
  ownership. Tell each executor it is not alone and must preserve others' work.
  Use an explorer for a bounded research question. Keep dependent or trivial
  work in the main thread; the repo limit is three concurrent subagents.
- Serialize changes to shared contracts and integration. Coordinate worktree,
  branch and shared VM ownership across humans as well as agents.

## Validate and challenge

- Require concrete outputs and actual validation evidence from executors.
  Freeze the proposal/diff and identify its snapshot before review; later edits
  invalidate affected reviews. Keep author persuasion out of the critic packet.
- Ask an independent `pc-critic` to inspect the result against the acceptance
  criteria. Use the [review record](../../../docs/ai/templates/review.md).
  A critic may return no material findings. Missing evidence is not a pass.
- Resolve each material finding as fixed, rejected with evidence, or open.
  Prefer at most two repair/review cycles before surfacing a persistent blocker;
  never waive the README's evidence gates to meet that limit or a deadline.
- Update the authorized issue/PR and durable handoff with scope, snapshot,
  validation, review outcome, blockers and next action. Publish only sanitized
  evidence. If GitHub access or permission scope fails, report the blocker and
  preserve a local handoff without claiming remote tracking succeeded.

## Continue or stop

Resume by checking the task record, Git state, ownership and current environment;
old chat context or historical cluster inventory is not current evidence. Add
roles or supporting scripts only when a concrete repeated task justifies them.
This skill guides Codex; it is not a guaranteed dispatcher or unattended daemon.
Separate sessions with enforced permissions or a Python runner need their own
approved setup when those guarantees are required.
