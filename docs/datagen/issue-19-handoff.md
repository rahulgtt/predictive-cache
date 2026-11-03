# Issue 19: validation, reports and downstream handoff

Base: 1c307ed; branch: dherazs-predictive-cache.
Host: kind-deploy; checkout: /home/automation/workspaces/sgorla/predictive-cache.
Status: partial #19; larger preset acceptance remains open. No real access data.

## Acceptance and scope

| Requirement | Implementation/evidence | Status |
| --- | --- | --- |
| Corruption rejected per rule | Schema, references, pairing, totals, ranges, loss, leakage, truth and hash corruption fixtures; valid injected faults restore correctly | Passed |
| Counts, hourly histogram, popularity, size/fault/origin report | quality.json and readable summary.md; explicit measurement bases and denominators | Passed |
| P1–P8 recovery checks | Existing controlled generator tests plus positive/negative/sparse report checks; mixed cases are diagnostic | Passed within controlled fixture scope |
| Tiny 10k–50k rows, generation + validation under 10s | 13,092 rows in 4.343s locally | Local passed; automated CI excluded by user request |
| Small 1–3M rows under 120s | Default before final scan optimizations: 529,709 rows, generation alone 122.386s. Final tuning attempt: 693,947 rows, 184.357s including validation | Not met |
| Medium 10–30M rows under 15min | Not benchmarked; buffered memory use is a constraint on this host | Unverified |

Small tuning was NOT adopted. Tiny retains 300 initial files, 10 users and two
weeks, with rate_scale=0.5, warmer top_n=10 and 64MiB maximum read chunks.
No changes to schema v1, dependencies or CLI commands. The data remains fictional
metadata/events; no modeled file contents or personal reads are created.

## Verification

- Full regression: 190 passed in 142.59s; /tmp/issue19-pytest.log.
- After final live-pool/probe-scan optimizations: 17 pattern/fault tests passed
  in 14.11s; all 18 tiny event/truth shard hashes matched the previous output.
- Ruff check, Ruff format check and git diff --check passed.
- Final tiny: generation 3.362s (including imports), validation 0.980s,
  1,667,516 bytes (~1.59MiB), process peak RSS 248,640KiB (~243MiB).
  Dataset /tmp/issue19-tiny-final; measurements /tmp/issue19-tiny-final-benchmark.json.
- The documented Parquet loading example ran against that artifact and selected
  2,643 deduplicated successful simulated foreground opens.
- Small tuning experiment: rate_scale=0.5, maximum read chunks=4MiB; generation
  128.072s, validation 56.285s, peak RSS ~3.02GiB. Dataset and resolved config
  are retained at /tmp/issue19-small-tuned and /tmp/issue19-small-tuned.yaml.
- Environment: Python 3.12.13, NumPy 2.5.4, PyArrow 23.0.1,
  Pydantic 2.14.2, PyYAML 6.0.3, Click 8.5.0. Wall times describe this shared
  host and these exact scenarios, not universal performance guarantees.

Reproduce the functional checks from the checkout:

```bash
.venv/bin/pytest -q
.venv/bin/ruff check src tests
.venv/bin/ruff format --check src tests
.venv/bin/pcache-datagen generate --preset tiny --out /tmp/my-pcache-tiny
.venv/bin/pcache-datagen validate /tmp/my-pcache-tiny
.venv/bin/pcache-datagen describe /tmp/my-pcache-tiny
```

Checks are run manually for this one-time data-generation use case. No GitHub
Actions workflow is included, per the user's scope decision. Dependency setup
is outside the timing measurement. Generated artifacts stay outside Git.

## Review and next action

Plan review accepted the scope with required controlled recovery checks.
Final implementation review was started against /tmp/issue19-review-final.json
and its matching archive, but not completed. Further review rounds were
stopped at the user's request to publish promptly; this is not a review pass.

Use [the downstream guide](downstream-guide.md) to generate, transfer, load and
interpret test data. It covers duplicate handling, observed arrival order,
origin filtering, gaps and as-of catalog history; truth is test-only.
The guide's loader is a compatibility example, not a complete cleaner.

Keep #19 open for small/medium scale and runtime work. Reaching those targets
may require memory/throughput changes beyond this focused hackathon PR.
Summary-mode byte totals are checked through realizer parity tests; absent
read rows cannot independently prove byte totals. Synthetic recovery never
establishes model quality on human workloads.
