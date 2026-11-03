# Agent setup validation

Date: 2026-11-03. Work item: [issue #2](https://github.com/rahulgtt/predictive-cache/issues/2).
Scope: repository documentation, native role definitions and skill sources.
The existing design and user changes are preserved; no Controller, Warmer,
cluster, personal installation or external publication is part of this update.

## Checks performed

| Check | Result and scope |
| --- | --- |
| Skill frontmatter | The bundled skill-creator `quick_validate.py` passed for `pc-milestone` and the optional generic skill. This validates format, not discovery or execution. |
| TOML parsing | Python `tomllib` parsed project configuration and all three role definitions; roles contain `name`, `description`, `developer_instructions`, without ineffective permission overrides. |
| Local references | Markdown targets resolve against the staged update plus current repository files. |
| Independent content review | A no-history subagent inspected the draft against AGENTS.md and README.md; no material findings. Source changes require a current review. |
| ML plan fixture | An independent subagent applying the critic instructions identified future-event features, preprocessing fit on held-out data, and test-set threshold tuning as blockers, with discriminating checks. |
| Implementation fixture | The same independent test identified requested/returned byte-budget confusion, unchanged ranges and an EOF loop in an isolated faulty reader. The reader was not executed. |
| Clean documentation fixture | No material finding; the critic stated that a supplied validation summary was not independent evidence of the actual final diff. |

The fixture pass exercised instructions through a general delegated agent.
It does not prove discovery/invocation of the named `pc-critic` role in a fresh
native Codex session, enforced read-only permissions, model quality or product
performance. Fixtures remain outside the repository and are not demand data.

## Remaining activation checks

- Start a fresh trusted-project Codex session and confirm both native roles
  and `$pc-milestone` are discovered and invoked.
- Exercise a real task handoff/resume and stale-snapshot rejection, plus
  independent concurrent task ownership in the chosen runtime.
- Verify GitHub project-board access: the observed CLI token has repository
  access but lacks project scope. No remote issue/board update was performed.
- Exercise personal role/skill discovery only after a separately approved
  personal installation. Those sources are inactive in this update.

Agent setup remains incomplete until the required activation checks pass.
Project filesystem, data, ML and performance evidence gates remain unverified.
