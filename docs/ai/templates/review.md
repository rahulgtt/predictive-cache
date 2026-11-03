# Independent review record

Review only the frozen plan or implementation snapshot supplied. Start with
fresh context, inspect relevant source, and assess acceptance and README
invariants. Author reasoning and self-assessment are not review evidence.
Report missing evidence; do not fabricate findings or validation results.

| Field | Value |
| --- | --- |
| Issue / review stage | URL; plan or implementation |
| Reviewer / fresh-context method | Identity; no-history subagent or separate session |
| Plan snapshot | ID/digest and relevant source revision |
| Implementation snapshot | Base/head, complete diff and relevant file digests |
| Evidence inspected | Source locations, commands/results, sanitized evidence IDs |
| Review coverage / limitations | Inspected scope, unavailable evidence/checks |

## Findings

Use one record per finding. An empty findings list is valid when supported
by the inspected evidence and stated coverage.

- **ID / title:** concise defect or evidence gap.
- **Severity:** blocking or advisory. Blocking means acceptance, a README
  invariant, required validation, or a gate is unmet; advisory is an optional
  improvement that does not prevent the defined acceptance criteria.
- **Confidence:** high / medium / low, with the supporting evidence or gap.
- **Trigger:** concrete input, state, or sequence that exposes the problem.
- **Impact:** observable consequence and affected criterion/invariant.
- **Source:** exact file/line or frozen plan section; evidence reference.
- **Suggested correction:** smallest justified change, if known.
- **Targeted test:** reproducible check and expected outcome that distinguishes
  the defect from correct behavior; mark proposed versus actually run.
- **Disposition:** open / fixed / disputed; owner and resolution evidence.

## Decision and follow-up

- Result: blockers present / no blockers found / review incomplete.
- Accepted advisory items and deferred work, linked to canonical issues.
- Required fixes/checks, owner, and next snapshot to review.
- A no-findings result is limited to this snapshot and coverage. It is not a
  performance claim, gate waiver, merge approval, or proof of absent defects.
- Re-review changed artifacts and dependent assumptions. After two suggested
  fix cycles, surface unresolved blockers for a human decision; never
  automatically waive them or present incomplete review as passed.
