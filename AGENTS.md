# AGENTS.md

Guidance for AI coding agents and humans working in this repository. Read this
file first, then [README.md](README.md) for the full design.

## Project in one paragraph

Hackathon Team 32: **Predictive Cache** for Strider. A Strider cluster serves
SMB/NFS clients through the Chimera protocol gateway, backed by CUFS. CUFS
caches data locally and faults cold data in from the cloud/S3 backend over
the WAN. This project predicts which files/ranges will be hot from historical
access patterns and pre-warms them on the cluster, so first opens do not wait
on the backend. The MVP has two deployables:

- **Predictive Cache Controller** handles ingestion, features, models,
  planning and feedback.
- **Cache Warmer** performs bounded, read-only NFS reads that execute plans.

Our team owns the ML side: event memory/state, feature extraction, model
training, inference and evaluation. We also work with the protocol (CUFS),
telemetry (o11y) and performance lanes.

> **Status:** design proposal. No Controller, Warmer, pipeline or benchmark
> code exists yet. [README.md](README.md) is the source of truth for
> architecture, contracts, gates and terminology. Don't duplicate it here.
> Update it when the design changes.

## Repository layout

| Path | Purpose |
| --- | --- |
| `README.md` | Design, system model, contracts, modeling and benchmark plan, gates. |
| `AGENTS.md` | This file: working conventions for agents and contributors. |
| `docs/` | All supporting knowledge: PDFs, design docs, architecture notes, decision records, findings, investigation write-ups. |
| `.github/` | PR template and issue forms. |
| `local-space/` | Personal scratch space, git-ignored (see below). |

New code directories (e.g. controller, warmer, ML pipeline) should be added
as the work lands and listed in this table.

## Non-negotiable design rules

These come from [README.md](README.md). Any code, issue or PR must respect them:

1. **Foreground reads are never blocked or made dependent on this system.**
   Telemetry, prediction, Kvrocks or Warmer failures must not change a
   foreground read's result or latency path.
2. **Exclude warmer-origin (and synthetic) events** from demand features,
   labels and evaluation. Unknown origin is not demand.
3. **Never write predictive state into CUFS volume Kvrocks.** Use the session
   Kvrocks under isolated, versioned prefixes, or a separately approved
   dedicated instance.
4. **No temporal leakage.** Use chronological train/validation/test splits.
   Features may use only data available at prediction time. Freeze models and
   thresholds before touching the test period.
5. **Synthetic fixtures are for plumbing and contract tests only.** They are
   never evidence of model quality or performance.
6. **Always compare against simple baselines** (MRU, frequency/static hot set)
   at the same byte budget. If a baseline wins, report it.
7. **No unproven claims.** Don't state performance or accuracy results without
   the raw, reproducible evidence the README's gates require. Label proposals
   as proposals.
8. **Privacy:** never collect file contents, raw usernames, credentials or
   unnecessary identifying paths. Never commit real access data or secrets.

## Environment and builds

- Use `cluster-manager` to locate/inspect the target Strider cluster. Use the
  **Kind** backend for provisioning unless told otherwise. Don't assume
  historical cluster inventory is still valid.
- For CUFS, Chimera and other Strider components, **do not build or run tests
  in local checkouts**. Use `strider-dev` / `strider-dev2` and the designated
  build containers. Initialize submodules first.

## Local workspace: `local-space/`

- `local-space/` is git-ignored. Use it for personal scratch work: local test
  runs, sample data, experiment outputs, notebooks, notes.
- **Committed files must never reference `local-space/`.** Code, docs, configs,
  scripts, tests and READMEs must not import from it, link to it or rely on it.
  This file and `.gitignore` are the only exceptions. Anything that is needed
  by others belongs in a tracked location (e.g. `docs/` or a code directory).
- Agents may freely create scratch files there. Never `git add -f` anything
  from it.

## Documentation: `docs/`

- Put every durable piece of knowledge in `docs/`: PDFs, architecture and
  design docs, decision records, data/contract specs, investigation findings,
  benchmark write-ups. Future sessions should be able to rebuild context from
  `README.md` + `docs/`.
- Prefer Markdown. Use kebab-case file names (e.g. `docs/event-schema.md`).
  Subfolders are fine when a topic grows.
- PDFs in `docs/` are marked binary in `.gitattributes`.
- When a doc changes the design, update [README.md](README.md) or link the
  new doc from it.

## Work management

All work is tracked as **GitHub issues** in
[`rahulgtt/predictive-cache`](https://github.com/rahulgtt/predictive-cache).
Progress is shared on the
[Predictive Cache project board](https://github.com/users/rahulgtt/projects/3).
Every issue and PR must be added to that project.

### The agent owns GitHub bookkeeping

**The agent does all routine GitHub interactions:**

- creating issues, labels, branches and PRs
- linking issues to the epic and stories
- adding items to the project board
- moving items through statuses
- commenting progress and closing issues

Developers interact through:

- **the agent chat session:** describe the work and make decisions; the agent
  turns that into issues, branches and PRs;
- **PRs:** review and approve, when review is needed;
- **manual work:** devs are free to test, run PoCs and experiment by hand
  (e.g. in `local-space/`). The agent still records any resulting work on
  GitHub.

**Setup (agent: check this at session start).** The agent needs one of these
to work with GitHub:

1. **`gh` CLI (preferred):** authenticated, with the `project` scope.
   - Run `gh auth login`, then `gh auth refresh -s project`.
   - Verify with `gh auth status` (scopes should include `repo` and `project`).
2. **GitHub MCP server** configured in the agent with write access to issues,
   PRs and projects.

If neither works, the agent must stop and ask the dev to set one up. Don't
silently skip tracking or ask the dev to do the bookkeeping by hand.

### Status lifecycle

The board's `Status` field values are `Backlog`, `Ready`, `In progress`,
`In review` and `Done`. The agent keeps them current:

| Event | Agent action | Status |
| --- | --- | --- |
| Work identified in chat | Create the issue with labels. Link it to the epic/story. Add it to the project. | `Backlog` (todo) |
| Scoped, accepted and ready to pick up | Fill in acceptance criteria. Set `Priority`/`Size` if known. | `Ready` |
| Work starts | Assign the dev, create the branch, comment the plan if useful. | `In progress` |
| PR opened | Open the PR from the template, link the issue, add the PR to the project. | `In review` |
| PR merged or work otherwise complete | Ensure the issue is closed, with a short closing comment if not obvious. | `Done` |

Also post an issue comment for notable progress, blockers or decision changes.

Useful `gh` commands:

```bash
gh project item-add 3 --owner rahulgtt --url <issue-or-pr-url>
gh project field-list 3 --owner rahulgtt --format json   # Status/Priority/Size field + option IDs
gh project item-list 3 --owner rahulgtt --format json    # item IDs
gh project item-edit --project-id <project-id> --id <item-id> \
  --field-id <status-field-id> --single-select-option-id <option-id>
```

Get `<project-id>` with `gh project view 3 --owner rahulgtt --format json -q .id`.

Agents **must not** do any of these without explicit dev approval:

- merge PRs
- force-push shared branches
- delete issues/branches
- close issues that aren't finished

### Labels

Every issue gets **exactly one `type/*` label** and **at least one
`area/*` label**.

| Type label | Use for |
| --- | --- |
| `type/epic` | The single top-level "Predictive Cache" epic (see below). |
| `type/story` | A user-facing or deliverable slice of work under the epic. |
| `type/feature` | An improvement or new feature. |
| `type/bug` | Something is broken or behaves incorrectly. |
| `type/chore` | Maintenance, setup, tooling, docs housekeeping, no behaviour change. |

| Area label | Scope |
| --- | --- |
| `area/ml` | Features, labels, models, training, inference, offline evaluation. |
| `area/controller` | Controller ingestion, planning, feedback, predictive state. |
| `area/warmer` | Cache Warmer execution, budgets, rate limits, NFS reads. |
| `area/protocol-cufs` | Chimera → CUFS read/fault path, event hooks, file identity, origin marking. |
| `area/telemetry-data` | Event collection, data quality, OTel metrics, dashboards. |
| `area/performance` | Workloads, cold-state reset, benchmarks, trial analysis. |
| `area/infra` | Clusters, Kvrocks, deployment, CI. |
| `area/docs` | Documentation in `README.md`, `AGENTS.md`, `docs/`. |

If a label is missing, create it with `gh label create` using these exact names.

### Epic and issue hierarchy

- There is **one epic**: the issue titled **"Predictive Cache"**
  (`type/epic`). Don't create additional epics without team agreement.
- The epic's body keeps a task list linking **every story**, whatever its
  area (ML, CUFS, o11y/metrics, performance, ...). Add a link there whenever
  a story is created.
- Each story's body starts with `Part of #<epic-number>`. Bugs, features and
  chores reference the story (or the epic) they belong to.
- Issue forms in `.github/ISSUE_TEMPLATE/` exist for story, feature, bug and
  chore. They apply the `type/*` label and add the issue to the project. They
  exist for the rare case a dev files an issue in the web UI; the agent
  normally creates issues with `gh`/MCP and sets all labels itself. There is
  no epic form because there is only one epic.

## Branches

```text
<github-username>/<issue-number>-<short-description>
```

Use lowercase kebab-case for the description, e.g.
`hgandhe-debug/12-event-schema-draft`. Every branch maps to an issue. Create
the issue first.

## Pull requests

- Fill in [.github/pull_request_template.md](.github/pull_request_template.md):

  | Heading | What goes there |
  | --- | --- |
  | **Summary** | What changed and why, in 1–3 sentences. |
  | **Linked issue** | `Closes #N` (or `Part of #N` if it doesn't finish the issue). |
  | **Area** | The `area/*` label(s) this touches. Apply the same labels to the PR. |
  | **Changes** | Bulleted list of the notable changes. |
  | **Testing / Evidence** | How it was verified (commands, results, links). For model/perf claims, link raw evidence and state whether data was fixture or real. |
  | **Risks & rollback** | What could break (especially foreground impact) and how to revert. |
  | **Notes for reviewers** | Anything to focus on, open questions, follow-ups. |

- Keep PRs focused on one issue. Add the PR to the project board.

## Related Strider repositories

The wider system lives in other `nasuni/*` repos. Clone them alongside this
repo for read-only context; don't modify them as part of this project's work.

| Repo | Relevance |
| --- | --- |
| `nasuni/cufs` | CUFS filesystem: read/fault path (`cufs_op_read` → `File::read` → `Dkv::readData`), the event-hook location. |
| `nasuni/strider-cm` | Strider edge control-plane pod. |
| `nasuni/strider-operators` | `volume-operator`: per-volume Kvrocks provisioning, mount to Chimera. |
| `nasuni/strider-o11y` | LGTM / OTel Collector Helm charts, the telemetry and dashboard stack. |
| `nasuni/strider-k8s` | Kubernetes/Helm manifests for Strider. |
| `nasuni/strider-deploy` | OpenTofu/Ansible cluster deployment. |
| `nasuni/strider-build-tools` | Shared build/CI/Tekton conventions and build containers. |
| `nasuni/strider-tests` | Test suites and ReportPortal orchestration. |

The Chimera gateway source is not yet in this list. Add it once identified.

## Agent checklist

- At session start, verify GitHub access (`gh auth status` with `project`
  scope, or the GitHub MCP server). If missing, ask the dev to set it up.
- Read [README.md](README.md) and relevant `docs/` before proposing designs.
- Proactively create and update issues, labels, project items, branches and
  PRs as work happens. Keep board statuses accurate. Only merges and
  destructive actions need explicit approval.
- Scratch work goes in `local-space/`. Durable findings go in `docs/`.
- Keep this file current when conventions change.
