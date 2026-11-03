# Agent instructions

Keep this file within **100 lines**. Move detailed procedures into linked docs.
Read [README.md](README.md) and the [contributor handbook](docs/ai/contributor-handbook.md)
before starting work. The handbook preserves the detailed repository rules and
is required guidance, not optional background. README owns architecture,
contracts, terminology and evidence gates; update it when the design changes.

## Project and design invariants

- Team 32 builds Predictive Cache for Strider: a Python Controller and a
  separately bounded, read-only NFS Warmer. The project remains a design
  proposal until implementation and evidence establish otherwise.
- Prefer Python. Go requires a concrete reason and explicit user approval.
- Foreground reads must never depend on prediction, telemetry, predictive
  state or the Warmer, including their failures and latency paths.
- Exclude warmer and synthetic events from demand features, labels and
  evaluation. Unknown origin is not demand.
- Never write predictive state into CUFS volume Kvrocks. Validate isolated,
  versioned session-store prefixes or obtain approval for a dedicated store.
- Use chronological splits and past-only features. Freeze models and
  thresholds before test evaluation; prevent temporal leakage.
- Synthetic fixtures prove plumbing/contracts only. Compare real-data models
  against MRU/frequency/static baselines at equal byte budgets; report losses.
- Support performance/accuracy claims with raw reproducible evidence and the
  README gates. Label proposals, unrun checks and missing evidence explicitly.
- Do not collect file contents, raw usernames, credentials or unnecessary
  identifying paths. Never commit real access data or secrets.

## Environment and files

- Locate/recheck the target with `cluster-manager`; historical inventory is
  not current health. Use Kind for provisioning unless explicitly directed otherwise.
- Build/test CUFS, Chimera and other Strider components on `strider-dev` /
  `strider-dev2` in designated containers, not local checkouts; initialize submodules.
- Keep durable Markdown knowledge in `docs/`, PDFs in `artifacts/pdf/`, and
  conventions/templates in `.github/`. Use kebab-case documentation names.
- `local-space/` is ignored private scratch space; never force-add its files.
  Committed code/docs/config/tests must not reference or depend on that path;
  only this file and `.gitignore` are exceptions. Shared inputs must be tracked.

## Work tracking and approvals

- At session start, verify authenticated `gh` with repo/project access or a
  GitHub MCP connection supporting issues, PRs and projects. If unavailable,
  stop and ask for setup; do not silently skip tracking or delegate bookkeeping.
- Agents own routine issue/label/branch/PR creation, progress comments, epic
  links and project statuses within the current session's authorized scope.
- Track every issue/PR in [Predictive Cache](https://github.com/users/rahulgtt/projects/3).
  Follow the handbook's exact labels, hierarchy and status lifecycle.
- Use the single `[epic] Predictive Cache`; do not create another without agreement.
- Create the issue before its branch: `<github-username>/<issue-number>-<description>`.
- Use `[<type>/<area>]` issue/PR titles, one `type/*` and at least one `area/*` label.
  Fill the [PR template](.github/pull_request_template.md); keep PRs issue-scoped.
- Explicit approval is required to merge, force-push shared branches, delete
  issues/branches or close unfinished issues. Preserve unrelated/concurrent edits.
- Related Strider repositories provide read-only context; do not modify them
  as part of this project's work. Their map is in the handbook.

## Delegation and review

- Use [$pc-milestone](.agents/skills/pc-milestone/SKILL.md) and the
  [workflow](docs/ai/workflow.md) for consequential milestone planning/delivery.
  The [checkpoint map](docs/plan/milestones.md) maps tasks to README gates;
  GitHub remains the backlog, not an independent local task list.
- The main agent coordinates. Delegate independent bounded tasks to
  `pc-executor` or explorers; assign file ownership and tell workers they are
  not alone. Coordinate shared contracts/VMs across humans; serialize integration.
- Use a fresh `pc-critic` for consequential plans and completed changes.
  Supply requirements, frozen snapshot and evidence, not author persuasion.
  Use `fork_turns="none"` where supported or a separate review session.
- Findings need evidence; no material findings is valid. Record dispositions
  and blockers. Changed behavior needs current validation/review before completion.
  Small factual answers and routine updates do not need separate review.
- Roles inherit parent permissions; instructions do not enforce a read-only
  sandbox or guaranteed dispatcher. Models/permissions remain developer choices.
- Scheduling, programmatic runners and personal installation need their
  applicable authorization. Repo templates under `tools/ai/personal/` are inactive.
