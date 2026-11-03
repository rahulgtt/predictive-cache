# Agent workflow

Read [AGENTS.md](../../AGENTS.md), [README.md](../../README.md), and the linked
issue before work. README owns architecture, contracts, evidence gates, and
terminology. This workflow adds coordination, not another design or backlog.
The repository includes a synthetic datagen foundation and an experimental
manual NFS reader, but no Controller, automated Warmer service, complete
pipeline, or benchmark. Python is the intended starting language; the two
proposed deployables remain Controller and bounded read-only NFS Warmer.

## Roles and authority

The primary Codex session coordinates: scope the issue, freeze plans, assign
work, request independent review, integrate, validate, and hand off. Native
`pc-executor` subagents implement owned slices; `pc-critic` subagents review.
The project skill `$pc-milestone` guides this process when invoked. A skill
is not a dispatcher or an always-running process. Roles inherit applicable
permissions; a read-only role instruction is not a security boundary.

Use at most three subagents, subject to the root configuration and available
runtime capacity. Each executor owns explicit files or modules and must
accommodate others' work. Use separate worktrees for concurrent changes when
file ownership alone is insufficient. Only this chat's coordinator
integrates its work. With several human coordinators, claim issue and file
ownership first and coordinate contract changes through the issue; do not
let independent chats integrate overlapping edits concurrently.

Current setup authorization covers repository documentation/configuration
only. It does not activate personal configuration, change a VM or cluster,
push, open a PR, or post externally. In later implementation sessions, routine
GitHub bookkeeping follows AGENTS.md and that session's authorized scope.
GitHub issues and the project board are canonical. Issue
[#2](https://github.com/rahulgtt/predictive-cache/issues/2) tracks AI setup.
The observed CLI token has `repo` but lacks `project`; setup board verification
is blocked until project access works. No remote updates were performed in
this documentation task. AGENTS.md's access/stop rule remains unchanged; do
not claim tracking was updated or silently skip it.

## One issue-sized cycle

1. Verify the repository/worktree, branch, dirty files, current issue/board
   state, owners, access, and relevant README gate. Read existing evidence.
   Recheck the target with `cluster-manager` before environment work. Follow
   the current explicit environment decision and repository defaults where no
   decision exists; historical inventory is not current cluster health.
   Respect the README's remote build/test rules.
2. Use the [task record](templates/task.md) to define acceptance, allowed
   files, dependencies, contracts, tests, data permissions, and stop conditions.
   Choose the smallest slice that one engineer can own. Assign 3-4 engineers
   across Controller/ML/data, Warmer/protocol, and performance/observability;
   combine the last two responsibilities if needed, keeping explicit owners.
3. Freeze a plan snapshot with a stable ID and content digest. Include the
   issue acceptance criteria, source revision, scoped files, invariants,
   contract versions, checks, and dependencies. Give a fresh critic this
   snapshot and relevant source evidence, without the author's rationale or
   self-assessment. Resolve blocking plan findings before implementation.
4. Delegate owned slices to executors, then integrate in the coordinator's
   worktree. Recheck contracts and run targeted available validation. Record
   exact commands, environment, exit results, and missing checks. Fixtures
   validate plumbing only; they never prove predictive quality or efficacy.
5. Freeze the review snapshot: plan ID/digest, base/head revisions, complete
   diff, and manifests/digests for relevant uncommitted or untracked files.
   Preserve the exact reviewed content in approved storage. Send a fresh
   critic this bundle, acceptance criteria, source context, and raw validation
   evidence. Use `fork_turns="none"` where available; otherwise start a
   separate review session. If fresh review is unavailable, record that gap
   and obtain it before claiming the review gate passed.
6. Use the [review record](templates/review.md). Findings need a concrete
   trigger, impact, source location, confidence, and targeted test. No findings
   is valid. Do not request invented criticism or a minimum finding count.
   Fix blockers and repeat review on a new frozen snapshot. Two fix/review
   cycles are a useful default; then surface unresolved blockers and the
   decision needed. Never automatically waive them or relabel them to pass.
7. Close the slice only when acceptance, validation, independent review, and
   the applicable gate evidence are satisfied. Capture durable decisions and
   a next-action handoff; report unavailable checks and remaining blockers.

Any relevant source, plan, contract, or acceptance change invalidates its
review. Preserve superseded review records and request a current review.
Review is evidence for a decision, not authority to merge or change a gate.

## Evidence, privacy, and resuming

Keep shared summaries, critical decisions, sanitized validation outcomes,
contract versions, and handoffs tracked in `docs/` and linked from the issue.
Keep access data, raw logs, and sensitive reports in approved private storage
with access/retention owners; record sanitized evidence IDs and retrieval
instructions for authorized reviewers. Never commit real access data,
identifying paths, secrets, or private report contents. Missing raw evidence
must stay an explicit limitation, not a fabricated result.

On resume, verify Git state and task/issue state before reusing any plan or
review. Compare current file digests, revisions, contracts, evidence, and
ownership with the handoff. Resume at the first invalid or incomplete gate;
stale review is not approval. Background subagents work during an active
session. Scheduled continuation requires a separately configured automation
and an available app/machine; this setup creates no schedule.

## Activation and smoke protocol

Start a new local session with `codex -C /path/to/predictive-cache`, then ask:
`$pc-milestone inspect issue #2 and propose the next unblocked repository slice`.
Confirm project instructions, skill discovery, and native role discovery in
that runtime before depending on them. Record actual checks and limitations in
[the setup validation record](setup-validation.md); this protocol alone does
not establish that the checks passed.

1. Verify `pc-milestone`, `pc-executor`, and `pc-critic` are discoverable in a
   fresh session. Have the executor return a scoped read-only plan and the
   critic review it in fresh context; verify neither exceeds its authority.
2. Give the critic a frozen fixture plan whose features include observations
   after prediction time. Expect a blocking temporal-leakage finding tied to
   the offending text and a chronological-boundary test. Keep it out of live
   demand/evaluation inputs; it demonstrates review behavior only.
3. Give a clean, narrowly scoped documentation-only fixture with complete
   acceptance and evidence. Accept an evidence-based no-findings result.
4. Save a handoff, reopen in fresh context, and verify Git/task state. Change
   one reviewed fixture file and confirm the old review is rejected as stale.
   Record results, failures, and unavailable runtime capabilities in issue #2
   when external tracking is authorized and access works.

Native behavior references: [subagents](https://learn.chatgpt.com/docs/agent-configuration/subagents),
[skills](https://learn.chatgpt.com/docs/build-skills), and
[worktrees](https://learn.chatgpt.com/docs/environments/git-worktrees).
