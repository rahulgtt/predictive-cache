# Issue 23: cleaned demand dataset handoff

The implementation is on `dherazs-cleanup` in the kind-deploy worktree
`/home/automation/workspaces/sgorla/predictive-cache-cleanup`. It adds the
`pcache-prep clean` and `describe` commands, versioned cleaned tables,
coverage and handle reconstruction, an initial catalog view, and a quality
report. The cleaner does not read `truth/`; the opt-in acceptance test does.

## Synthetic acceptance evidence

- Input: datagen `small` dataset `synth-small-s42-0aa1075f` at
  `/tmp/issue19-small-tuned` on kind-deploy. Its manifest SHA-256 is
  `ec02caba1d5d95b75454f6daeb40e41476b116b291af05ddddafc7b35563b94f`.
- Output: `/tmp/pcache-prep-small`, dataset
  `clean-synth-small-s42-0aa1075f-49eca909`. It contains 693,947 input
  deliveries, 641,973 kept events and 51,974 excluded deliveries.
- Exclusions: 3,506 duplicates, 295 invalid ranges, 33,000 warmer,
  12,087 synthetic-origin, 1,304 unknown-origin and 1,782 failed-result.
  The truth-based test found 0 false exclusions among 641,973 valid
  foreground events and checked all 295 injected invalid ranges.
- Coverage: 1,344 volume-hours, of which 1,342 are complete and 2 are
  incomplete. Both known loss-window hours were marked incomplete.
- The report and manifest counts reconcile. The output contains 29,734
  reconstructed opens, 578,544 successful read ranges and 4,167 metadata
  events. Exact output files and hashes are in its manifest.

The `small` dataset was a retained tuned artifact from datagen issue #19;
it has 693,947 deliveries, below that generator's separate 1–3 million
target. This run verifies the cleaning story's functional thresholds on a
`small` preset artifact. It does not establish real-workload model quality,
the full data-baseline evidence gate, or generator scale targets.

## Validation and review

From the remote worktree, with `PYTHONPATH=src` and the kind-deploy Python
environment:

```bash
ruff check .
ruff format --check src tests
pytest -q
PCACHE_SMALL_INPUT=/tmp/issue19-small-tuned PCACHE_SMALL_CLEAN=/tmp/pcache-prep-small pytest -q -s tests/prep/test_small_acceptance.py
```

The full suite passed with 266 passed and 1 skipped; the skip is the opt-in
small acceptance test without its environment variables. The separate small
truth test passed. Focused prep tests cover exclusion precedence, canonical
duplicates, malformed rows, deterministic files, CLI/JSONL, catalog isolation,
coverage edges, and handle/range reconstruction. An independent patch review
found malformed canonical duplicate and report-dimension edge cases; both
were fixed and covered by regression tests.

Generated datasets remain on kind-deploy outside Git. Reproduce from the
input dataset path above, or generate a new datagen `small` dataset and run
`pcache-prep clean` against it. Results on synthetic data are contract and
plumbing evidence only.
