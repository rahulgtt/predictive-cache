# ML spec v1: scorers, model package, evaluation and Controller runtime

> **Status:** v1 planning spec for stories
> [#49](https://github.com/rahulgtt/predictive-cache/issues/49) (model v1),
> [#50](https://github.com/rahulgtt/predictive-cache/issues/50) (evaluation),
> [#51](https://github.com/rahulgtt/predictive-cache/issues/51) (Controller
> runtime) and [#52](https://github.com/rahulgtt/predictive-cache/issues/52)
> (pipelines, backlog). Nothing here is implemented. Formats marked
> *illustrative* in the [README](../../README.md) are made concrete here for
> v1. Change them by PR to this file.

## 1. Purpose and decisions

Start with the [README glossary](../../README.md#basic-terms) and
[file-B example](../../README.md#example-a-feature-row-and-a-label).
The README teaches the system concepts. The [data-preparation spec](../data-prep/spec.md)
defines the input tables; this spec defines the detailed v1 model package,
scoring, evaluation and Controller runtime contracts within the README's design rules.

### Training and running predictions are different steps

1. **Train outside the live read path.** Use historical feature rows and
   their later labels to fit a model. Choose settings on validation data,
   freeze them before test evaluation, and save the model package.
2. **Receive current read records.** The Controller collects events for its
   scheduled processing loop. A user's SMB/NFS read does not wait for the
   Controller, a prediction or model training.
3. **Predict on a tick.** A tick is a scheduled prediction time. The proposed
   v1 loop uses the loaded model to score eligible files at each tick, then
   the planner chooses reads within the limits. The package's `tick_s`
   controls this cadence (initially 60 seconds; §8.3). An event can change
   inputs for a later tick; it does not trigger model retraining.
4. **Check what happened later.** The Warmer reports its reads; independent
   user demand supplies the later answers used for demand labels. A
   completed warm read alone does not prove usefulness or saved latency.
   Training a replacement model is a separate step.

The historical snapshot interval and the live prediction interval are
separate settings: data-prep's `snapshots.tick_s` initially uses 900 seconds,
while the live package's `serving_policy.tick_s` initially uses 60 seconds.
Both use the same feature rules at their respective prediction times.

This spec covers everything after the [data-prep](../data-prep/spec.md)
snapshots: training a first model, judging it fairly, and running it live
inside the Controller.

| Decision | v1 choice | Why / later |
| --- | --- | --- |
| First model | **Regularised logistic regression** | Gets the loop running; the team plan's first classifier. LightGBM, sequence models etc. are later `Scorer`s (§3) with no other change. |
| Baselines | MRU, frequency, static hot set, co-access only | README and team plan: a model must beat these at equal budgets, and a loss is reported. |
| Model unit / target | File; demand within `[t+lead, t+lead+H)` (data-prep §7) | Inherited; the target choice (demand vs cold open) is settled in evaluation (§7.6). |
| Artifact | A **model package** (§5) loaded the same way by evaluation and the Controller | One format; numpy-only scoring at runtime. |
| Tracking | ClearML ([ML platform](../infra/ml.md)), via a thin offline-safe wrapper (§9) | Nothing requires the server. |
| Runtime | One Controller process: ingestion, memory (`FeatureStore`), tick loop, planner (§8) | README: the Controller is one deployable; the model is a swappable file inside it. |
| Model selection | A pointer file from a mounted ConfigMap pins an immutable package (§8.6) | Explicit promotion, never "latest". |

Synthetic data proves plumbing and planted-pattern recovery only. It is
never real-workload model quality or product performance (README,
AGENTS.md).

## 2. Flow

```text
snapshots/ (train, val, test) + universe/ + artifacts/coaccess.parquet + feature_schema.json   [data-prep §8]
   │ pcache-train (§6): fit on train, select on val
   ▼
model packages (§5): logreg-v1, baseline-*          ──► ClearML model registry (tag: candidate)
   │ pcache-eval freeze (§7.4) → freeze.json        ──► tag: frozen
   │ pcache-eval run --split test (§7.5), once      ──► tag: evaluated, report
   ▼
pointer.yaml (ConfigMap) → Controller (§8): live events → memory → candidates → features → score → planner → warm plans
```

## 3. `Scorer` interface

Every baseline and model implements one protocol:

```python
class Scorer(Protocol):
    model_type: ClassVar[str]                  # e.g. "logreg-v1", "baseline-mru"
    @property
    def feature_columns(self) -> tuple[str, ...]: ...   # ordered; becomes package.json feature_columns
    def fit(self, train: pa.Table, val: pa.Table | None, cfg: dict) -> None: ...
    def score(self, rows: pa.Table) -> np.ndarray: ...     # float64, same order/length as rows; higher = more likely
    def explain(self, rows: pa.Table, k: int = 2) -> list[list[str]]: ...   # top-k reason strings per row
    def to_payload(self) -> dict: ...          # JSON-serialisable; becomes model.json (written with sorted
                                               # keys: encode ordering in lists, never dict key order)
    @classmethod
    def from_payload(cls, payload: dict, feature_schema: dict) -> "Scorer": ...
```

- `rows` always contain the **key columns** `volume_id`, `file_id` and `t`,
  plus at least the model's `feature_columns` (§5). Other columns (labels,
  weights, `expected_score`) are ignored. A missing feature column is an
  error. Key columns are not features and are not listed in
  `feature_schema.json`. Scorers may read them (e.g. the hot set uses
  `file_id`).
- `score` must be **deterministic** and depend only on numpy and pyarrow
  at runtime. Training-only libraries (scikit-learn) may be used in `fit`.
- Scores are **ranking scores**. They are called probabilities only if
  evaluation shows calibration (§7.3).
- Ties are broken downstream by `file_id` ascending (§7.1, §8.4).

## 4. Baselines

| `model_type` | Score | Nulls | `fit` |
| --- | --- | --- | --- |
| `baseline-mru` | `−secs_since_last_open` | null → −∞ | none |
| `baseline-frequency` | `opens_24h + 1e-3 × opens_7d` | null → 0 | none |
| `baseline-hotset` | `N − rank` for files in the frozen hot set, else 0 | — | Top N (500) `file_id`s per volume by the count of `label = 1` train rows, weighted by `sample_weight`. Train split only. |
| `baseline-coaccess` | `coacc_max_conf + 1e-3 × opens_1h` | null → 0 | none |

Baselines are packaged like models (§5), so evaluation and the Controller
treat them the same way. `baseline-frequency` is also the Controller's
built-in fallback (§8.6).

## 5. Model package

A directory, also zipped for upload as `<model_type>-<package_id>.zip`:

```text
package.json          # manifest (below)
model.json            # Scorer.to_payload()
feature_schema.json   # copied from the snapshots used for training (data-prep §5.2)
coaccess.parquet      # copied from snapshots artifacts/coaccess.parquet (frozen val/test table, data-prep §8.2)
smoke.parquet         # ≤ 50 feature rows from val + expected scores, for load-time verification
```

- **`feature_schema.json`** needs at least
  `{"feature_version": "fv1", "features": [{"name": …, "dtype": …, "group": …, "definition": …, "available": …}]}`.
  The loader relies only on `feature_version` and the feature `name`s.
- **`coaccess.parquet`** has the columns `from_file_id`, `to_file_id`,
  `support`, `n_from`, `conf` and `lift` (data-prep §5.4). It may be empty.
- **`smoke.parquet`** holds the key columns, the feature columns and
  `expected_score`.

`package.json`:

```json
{
  "package_version": 1,
  "package_id": "<first 16 hex of sha256(canonical JSON of this manifest without package_id and created_at)>",
  "model_type": "logreg-v1",
  "feature_version": "fv1",
  "feature_columns": ["secs_since_last_open", "opens_5m", "..."],
  "serving_contract": {"sha256": "…", "config": {}},
  "serving_policy": {"target": "demand", "lead_s": 0, "horizon_s": 300, "tick_s": 60,
                     "min_score": 0.0, "recent_skip_s": 300, "cooldown_s": 1800,
                     "max_items": 20, "max_bytes": 268435456, "range_cap": 16777216},
  "data": {"snapshots_dataset_id": "…", "snapshots_manifest_sha256": "…", "clean_dataset_id": "…",
           "source_dataset_id": "…", "data_kind": "synthetic"},
  "training": {"git_sha": "…", "config_sha256": "…", "params": {"C": 1.0},
               "selected_by": "val:recall_at_bytes_256MiB", "val_metrics": {}},
  "files": {"model.json": "<sha256>", "feature_schema.json": "<sha256>",
            "coaccess.parquet": "<sha256>", "smoke.parquet": "<sha256>"},
  "created_at": "…"
}
```

- **`package_id`** covers the whole manifest, including every file hash,
  the serving contract and the serving policy, so any material change
  gives a new ID. ClearML IDs are **not** stored in the package. The
  ClearML model carries `package_id` as metadata instead.
- **`serving_contract`** is copied from the snapshots manifest (data-prep
  §8.6). It is the canonical resolved configuration of everything that
  changes feature values or candidates:
  - `clean` (rules, `read_mode`, `completion_grace_s`, `handle_timeout_s`,
    `dedupe_window_s`);
  - `features`, `coaccess` and `candidates`;
  - the catalog policy;
  - the feature version.

  Its sha256 is computed over canonical JSON (sorted keys, no
  whitespace).
- **`serving_policy`** fixes how scores become plans (§8.4). It is chosen
  on val and frozen with the model. v1 requires `lead_s = 0`; the loader
  rejects others for serving.

**Loader** (`load_package(path, expected_feature_version, expected_contract_sha256=None)
-> LoadedPackage`) checks, in order, and raises `PackageError(reason)` on
the first failure:

1. All files exist, and every sha256 matches.
2. `package_id` recomputes correctly.
3. `package_version` and `model_type` are known, and `serving_policy` is
   servable:
   - all keys are present;
   - `target` ∈ {demand, cold_open};
   - `lead_s = 0`;
   - positive integer horizon, tick and limits;
   - `range_cap` ≤ 16 MiB;
   - `min_score` is `null` (no threshold) or a finite number.
4. `feature_version` equals the expected version.
5. The `serving_contract.sha256` recomputes from its `config`. If an
   expected hash is given, it must match.
6. Every `feature_columns` entry exists in `feature_schema.json`.
7. Scoring `smoke.parquet` reproduces its `expected_score` column within
   1e-9.

`model_version` in prediction and plan records is
`<model_type>@<package_id>`.

## 6. Logistic regression v1 and `pcache-train`

### 6.1 Preprocessing (fitted on train only)

Each `fv1` feature gets a transform by kind. The transform parameters are
stored in `model.json`:

| Kind | Features | Fill null with | Transform |
| --- | --- | --- | --- |
| seconds | `secs_since_last_open`, `secs_since_last_full_read`, `secs_since_last_write`, `file_age_s` | 604 800 (7 d) | `log1p(min(x, 604800))` |
| count | `opens_*`, `distinct_sessions_*`, `dir_*`, `project_opens_1h`, `volume_opens_5m`, `coacc_sources` | 0 | `log1p(x)` |
| bytes | `file_size`, `mean_bytes_per_open_7d` | 0 | `log1p(x)` |
| ratio | `growth_15m_vs_1h`, `full_read_share_7d`, `coacc_max_conf`, `coacc_sum_conf` | 0 | `x` |
| lift | `coacc_max_lift` | 0 | `log1p(x)` |

- **Null indicators:** for every nullable feature, add a column
  `isnull__<name>` (0/1) **before** filling.
- **Standardise:** after transforming, standardise with the train mean and
  std (std 0 → 1), and clip to ±10.
- **Unknown features:** a feature in `feature_schema.json` that isn't in
  this table makes training fail. New features must be given a kind here.
- **Time features** (`features.time.enabled`) are not in v1.

### 6.2 Fit and selection

- **Model:** L2 `LogisticRegression` (lbfgs, `max_iter` 2000) on train
  rows with `sample_weight` (data-prep §8.3).
- **Grid:** `C ∈ {0.01, 0.1, 1, 10}`. Pick by the **primary metric on
  val** (§7.2); ties go to the smaller C.
- **Calibration** is not fitted in v1. The val reliability curve and ECE
  are reported (§7.3).
- **Export** to `model.json`:
  `{"features": [...ordered transformed columns...], "preprocess": [...], "coef": [...], "intercept": b}`.
- **Scoring** = `sigmoid(w·x + b)` in numpy. It must equal scikit-learn
  `predict_proba[:, 1]` within 1e-9.
- **`explain`:** the top-2 terms `w_i·x_i` by value, rendered as
  `"<feature>:+0.83"`.

### 6.3 `pcache-train`

```text
pcache-train (--snapshots DIR | --snapshots-dataset ID) [--out DIR] [--config FILE]
             [--models logreg-v1,baseline-mru,...] [--tag T ...] [--queue Q] [--no-track]
pcache-model fake-snapshots --out DIR [--seed N] [--days N]   # schema-valid fixture with a planted signal
```

- **Input:**
  - a local data-prep snapshot directory, or a ClearML Dataset ID
    (`Dataset.get(...).get_local_copy()`);
  - the snapshots manifest must carry `serving_contract` (data-prep §8.6);
  - `pcache-model fake-snapshots` writes a `data_kind: fixture` directory
    in the same layout, so training can run before data prep produces
    real snapshots.
- **Reads only the `train` and `val` splits.** Reading any `split=test`
  file is a bug. A test corrupts the test files and asserts training
  still succeeds.
- **Output:**
  - `packages/<model_type>-<package_id>/` and `.zip` for each model;
  - `val_report.json` and `val_report.md` (§7 metrics on val for every
    model);
  - `train_manifest.json` (stage `train`, parent snapshots dataset, config
    hash, git sha).
- **With tracking (§9):**
  - **one** ClearML task `train:<models>` per run in
    `predictive-cache/train`, with `inputs` (the dataset ID) and the
    `train` config connected, so a cloned task can be edited and
    re-enqueued;
  - val metrics logged as scalars per model, and the C sweep;
  - each package zip registered as an OutputModel named by `model_type`,
    tagged `candidate` and `data:<kind>`, with metadata `package_id`.
  - Deviation from earlier drafts (one task per model): ClearML allows one
    task per process, and one task keeps the dataset parameter in one
    place.
- **Tracking switches on only when a server is explicitly configured**
  (`CLEARML_API_HOST`, `CLEARML_CONFIG_FILE` or `~/clearml.conf`). An
  unconfigured SDK must never fall back to a public demo server.
- **Remote runs:** `src/predictive_cache/clearml/train.py` is the
  file-path entry agents re-run (same pattern as the datagen task). `--queue`
  requires `--snapshots-dataset`, because agents can't see local
  directories.
- **Serving-policy selection on val:** candidate thresholds are
  **quantiles of this model's val scores** at
  `{0, 0.5, 0.8, 0.9, 0.95}`. Quantile 0 means no threshold
  (`min_score: null` in the package; JSON has no −∞), so negative-score
  baselines such as MRU still plan.
  - For each threshold, compute the primary metric on val with the §7.1
    selector applied only to rows with `score ≥ min_score`. A threshold
    that excludes no row is skipped, since it's the same as none.
  - Choose the **highest** threshold whose primary value is ≥
    `train.serving_policy.min_score_keep` (0.95) × the unthresholded
    value. This saves bytes at little recall cost; if none qualifies,
    there is no threshold.
  - The chosen numeric `min_score` (it may be negative) and its quantile
    are written to `serving_policy`. The other policy values come from
    config.

## 7. Evaluation (`pcache-eval`)

### 7.1 Per-tick selection

For each `universe` tick with `status = ok`, take the snapshot rows of that
tick and score them with each model. Rows are already restricted to
eligible candidates (data-prep §6). Then sort by `(score desc, file_id asc)`
and select under each budget:

- **Top-K:** the first K rows, `K ∈ {10, 25, 50}`.
- **Bytes:** walk the sorted rows and add a row if the cumulative
  `cost ≤ B`, otherwise skip it and continue.
  - `cost = min(file_size, range_cap)`, where `range_cap` is 16 MiB (the
    Warmer's per-invocation cap). Null `file_size` costs `range_cap`.
  - `B ∈ {64, 256, 1024} MiB`.

This byte rule is the same as the planner's step 7 (§8.4). That parity
covers the **stateless selector only**: the planner also applies recent-read
skips, cooldown, threshold, paths and item caps across ticks. The
sequential policy replay (§7.7) measures the shipped policy. Snapshot ticks
(data-prep: 900 s) and live ticks (60 s) differ, so per-tick budgets here
measure **ranking quality**, not byte rates.

### 7.2 Metrics

Micro-aggregated over the ticks of a split, per volume and overall:

| Metric | Definition |
| --- | --- |
| `recall@b` | Σ selected positives ÷ Σ `n_positive_files` (universe: positives eligible and known at *t*, candidates or not) |
| `precision@b` | Σ selected positives ÷ Σ selected rows |
| `coverage` | Σ `n_positive_in_candidates` ÷ Σ `n_positive_files`. The ceiling on recall. |
| `warmed_bytes@b` | Σ cost of selected rows |
| `positive_warmed_share@b` | Σ cost of selected positives ÷ `warmed_bytes@b` (byte-precision proxy) |
| `pr_auc` | Average precision over all candidate rows of the split (diagnostic) |
| `unknown_at_t_share` | Σ `n_positive_unknown_at_t` ÷ (Σ `n_positive_files` + Σ `n_positive_unknown_at_t`). The cold-start limit, reported alongside. |

**Primary metric:** `recall@bytes_256MiB`. It is fixed in the freeze record
(§7.4) and may only be changed **before** freezing.

### 7.3 Calibration and uncertainty

- **Reliability:** 10 equal-count bins and ECE on val (and test at
  evaluation). Scores are described as probabilities only if test ECE ≤
  0.05.
- **Confidence intervals:** a day-block bootstrap (resample UTC days of
  *t* with replacement, 1000 reps, seeded) gives a 95% percentile interval
  for `primary(model) − primary(best_baseline)`. `best_baseline` is chosen
  **on val** and fixed in the freeze record.

### 7.4 Freeze

```text
pcache-eval freeze --snapshots DIR --packages P1 P2 ... --out freeze.json [--primary recall@bytes_256MiB]
```

`freeze.json` records:
- package paths, `package_id`s and sha256s (the exact set: no more, no
  fewer may be evaluated on test);
- each package's `serving_policy` and `serving_contract.sha256`;
- the best baseline (by val primary);
- the primary metric and budgets;
- the eval config hash;
- **the test data binding:**
  - the snapshots manifest sha256;
  - every manifest-listed file's sha256 for the test split and `universe`;
  - the split boundaries;
  - the label config;
- the git sha and creation time;
- `freeze_id` = the first 16 hex of the sha256 of the record's canonical
  JSON (sorted keys, no whitespace) **with the `freeze_id` field
  omitted**. Verification recomputes it the same way.

With tracking, packages are tagged `frozen`, and the freeze record is
uploaded as an artifact of an `eval:freeze-<freeze_id>` task. After
freezing, any package, policy, config or data change needs a **new** freeze
record, and earlier test results stay recorded.

### 7.5 Run

```text
pcache-eval run --snapshots DIR --split val|test --out DIR [--freeze freeze.json] [--packages ...]
```

- **`val`:** free to run, for development and selection.
- **`test`:** requires `--freeze`. **Before reading any test row**, it
  verifies the package set, every package hash, the config hash and the
  data binding (manifest and file hashes, boundaries). It refuses on any
  mismatch or on unlisted files.
  - **Once per freeze:** a second test run for the same `freeze_id` is
    refused unless `--rerun-reason "<text>"` is given. The reason is
    printed in the report header.
  - **Ledger:** each test run is appended to a ledger, both locally
    (`<freeze dir>/test_runs.jsonl`) and, with tracking, as a ClearML task
    in `predictive-cache/eval`. The ClearML task is the authoritative
    record, because a local file can be deleted. The report states the run
    count.
  - **Limitation:** this is lab-grade enforcement, not access control. The
    report says so.
- **Output:** `metrics.json`, `report.md` and `curves/*.png`:
  - recall vs K and vs bytes;
  - precision vs recall;
  - reliability.

  The report's header states `data_kind`. For synthetic data it reads
  "plumbing / planted-pattern evidence only".
- **With tracking:** a task `eval:<split>-<freeze id>` in
  `predictive-cache/eval`; packages are tagged `evaluated` after a test
  run.

### 7.6 Decisions made through evaluation

The demand vs cold-open target, horizon, lead and default budgets are
compared on **val** only. The chosen values are frozen with the model.
Changing them after a test run requires a new freeze and is reported.

### 7.7 Policy replay (sequential)

```text
pcache-eval policy --plans DIR --clean CLEAN_DIR --from T0 --to T1 --out DIR
```

This measures the **shipped** planner. Its input is the `plans/` produced
by `pcache-controller replay` (§11.1) over a period, at the deployment
cadence, using the package's serving policy. For each plan item it checks
whether the file has demand (data-prep §7 rule) in `[created_at, deadline)`.
It reports:

- **plan precision:** items with demand ÷ items;
- **unique demand coverage:** distinct demanded `(file, 5-min bucket)`
  pairs covered by a plan item ÷ all eligible ones in the period;
- **issued bytes per hour**, and the share for files with demand;
- **skip reasons** by count.

Repeated planning of the same file counts once per demand bucket. Use it on
val for development. A test-period replay counts as a test run under the
same freeze rules (§7.5).

## 8. Controller runtime (story #51)

One Python process, **single replica**: two replicas would issue duplicate
plans until leader election exists.

```text
EventSource ─► ingest queue (bounded) ─► clean (rules + dedupe) ─► HandleTracker ─► updates
                                                                                   │
pointer.yaml ─► ModelManager (verify, stage) ──────┐                               ▼
                                                    ▼                FeatureStore (memory)
every tick: drain → apply → generate_candidates → features_as_of → Scorer.score → Planner → sinks
```

### 8.1 Event sources and clock

```python
class EventSource(Protocol):
    def start(self, sink: Callable[[list[dict]], None]) -> None: ...   # pushes raw schema-v1 events
    def stop(self) -> None: ...

class Clock(Protocol):
    def now(self) -> datetime: ...                                     # UTC
```

| Adapter | Use | Behaviour |
| --- | --- | --- |
| `replay` | Development and demo | Reads a datagen dataset's `events/` in `(ingest_time, event_id)` order. Emits each event when `ReplayClock.now() ≥ ingest_time`. The clock runs `speed`× real time (default 60×) from `period.start + warmup`. |
| `http` | Live | `POST /v1/events` with a JSON array of schema-v1 events. Answers `202 {"accepted": n, "dropped": m}` without waiting on processing (limits below). |

**Authoritative `known_at`.** The Ingestor stamps every event it accepts
with `known_at`, and that value alone drives cleaning, the HandleTracker and
the watermark:

- **Live:** the Controller receipt time.
- **Replay:** the event's `ingest_time`, i.e. the simulated receipt time.
  This equals batch, where `known_at = ingest_time`.

**Ordering:** live stamps are **strictly increasing**: `max(receipt,
previous + 1 µs)`. Events in one offered batch are sorted by `event_id` and
stamped consecutively. Replay pushes events in `(ingest_time, event_id)`
order, the same order batch cleaning uses (data-prep §4.5). Updates
therefore never arrive behind the store watermark, and tie order is
defined. A source-supplied `ingest_time` is kept only for diagnostics.
`classify_event` uses this `known_at` as its ingest time, so time-validity
rules are identical in batch and live.

**HTTP limits** (`ingest.http`), checked before parsing where possible:
- body ≤ 4 MiB and ≤ 10 000 events per request;
- at most 8 concurrent requests, enforced by a bounded worker pool. Excess
  connections get `503` at once;
- socket read timeout 2 s;
- auth: a bearer token from a Kubernetes Secret (`Authorization: Bearer`).
  Missing or wrong → `401`;
- only `ingest.allowed_volumes` are accepted. Others are dropped and
  counted.

**Foreground safety** is the event source's job as much as ours. The
source (CUFS hook, OTel exporter or instrumented client) **must** export
asynchronously, from a bounded local buffer outside the foreground
read-completion path, and drop on failure. Controller limits alone can't
guarantee that, and this requirement is recorded for the protocol lane
(§15).

- **Ingest queue:** bounded (`ingest.queue_max`, default 200 000 events).
  When full, new events are dropped and counted (`dropped_overflow`).
- **Live event source:** the CUFS hook, OTel or the instrumented client is
  owned by the protocol lane (§15).

### 8.2 Streaming cleaning (parity with batch)

These share code with data prep:

- **Rules:** `prep.clean.rules.classify_event` (data-prep §4.3 rules 1 and
  3–8), with `known_at` as the ingest time.
- **Dedupe:** an `event_id` seen within `clean.dedupe_window_s` (6 h), with
  memory bounded by window and count (1 M IDs, LRU). Two cases are counted,
  not hidden:
  - a duplicate older than the window slips through
    (`dedupe_window_misses` cannot be known live; it is measured in replay
    tests);
  - an ID evicted by the count cap while still inside the window
    (`dedupe_capacity_evictions`).
- **Read mode:** `clean.read_mode` (`ranges` | `summary`) is declared, not
  inferred (data-prep §4.5). It is part of the serving contract.
- **Handles:** `prep.clean.HandleTracker` (data-prep §4.5/§10).
  - Kept events are pushed in `known_at` order, which the Ingestor
    guarantees.
  - `advance(now)` is called every tick before applying updates.
  - `OpenCompleted` is emitted at `close_known_at + completion_grace_s`, or
    after `handle_timeout_s` of silence without a close.
- **Excluded events** (warmer/synthetic/unknown origin, failures, invalid,
  time) are counted per reason and never reach the memory.

**Parity requirement:** replaying a dataset through the streaming path
yields the same update sequence as `updates_from_clean()` on the
batch-cleaned dataset with the same serving contract. The documented
exceptions are duplicates older than the dedupe window, dedupe capacity
evictions, and queue overflow. Replay tests report all three, and the
parity test uses fixtures where they are zero (§13).

### 8.3 Tick loop

- **Cadence:** every `serving_policy.tick_s` of the active package (60;
  replay uses the replay clock), at `t = clock.now()`:
  1. drain the ingest queue through cleaning;
  2. `HandleTracker.advance(t)`;
  3. apply all updates with `known_at < t` and keep the rest for the next
     tick (watermark rule, data-prep §5.3);
  4. `generate_candidates(view(t), …, coaccess = active package's table)`;
  5. `features_as_of(t, candidates, coaccess)`;
  6. `scorer.score`;
  7. planner (§8.4);
  8. sinks (§8.5).
- **Deadline:** `tick_deadline_fraction × tick_s` (0.8). An overrun tick
  still finishes, the next tick is skipped, and `tick_overruns` is counted.
- **Model swaps** happen only between ticks (§8.6).

### 8.4 Planner v1

- **Policy source:** the planner uses the **active package's
  `serving_policy`** (§5). Controller config may only *lower* `max_items`
  and `max_bytes` (an operational cap) and never change the other values.
- **Plan gates:** no plans are written while readiness has a gating reason
  (§8.7/§8.8: `memory_gap`, `restore_failed`, `ingest_loss`,
  `contract_mismatch`). Predictions are still logged, and the tick is
  counted as `plans_suppressed`.

Inputs: scored candidates with their features, `CandidateView`, a
`PathResolver` and cooldown state. Rules apply in order, and every skip
records a reason:

| # | Skip reason | Rule |
| --- | --- | --- |
| 1 | `ineligible` | `view.is_eligible(file)` is false |
| 2 | `recently_read` | `secs_since_last_open < recent_skip_s` (300). Likely still cached. |
| 3 | `cooldown` | Planned within `cooldown_s` (1800) |
| 4 | `below_threshold` | `score < min_score` (chosen on val, §6.3) |
| 5 | `unknown_size` | `file_size` null |
| 6 | `no_path` | `resolver.resolve(volume, file, t)` returned None, or the path fails the Warmer path rules: absolute; valid UTF-8; ≤ 4096 bytes; components non-empty and ≤ 255 bytes; no `.` or `..`. These rules are a shared function with the Warmer CLI. |
| 7 | `budget` | Taken in `(score desc, file_id asc)` order while items < `max_items` and Σ length ≤ `max_bytes`. An item that doesn't fit is skipped and the walk continues. |

- **Range:** `offset 0`, `length = min(file_size, range_cap)` (16 MiB, the
  Warmer cap). Choosing ranges other than the prefix is later work (§15).
- **Retry allowance:** 0 in v1, so a plan's `max_bytes` = Σ item lengths.
  This follows README §3, where `max_bytes` covers all attempts including
  retries.
- **Deadline:** `t + horizon_s`. Because `lead_s = 0` in v1, items may
  start at once. The dispatcher must not start an item after the deadline.
- **Selector parity:** with rules 2–6 disabled, the planner's selection
  equals §7.1's byte selection. That is a test. The full policy is measured
  by policy replay (§7.7).

### 8.5 Records and sinks

The v1 transport is **JSON files in directories**. This is the README's
fixture transport, not a production queue. Each file is written to a temp
name and renamed atomically.

**Prediction** (`predictions/<tick>.jsonl`; the top `sinks.log_top_n`
(100) scored candidates plus every planned file). README §2 shape:

```json
{"schema_version": 1, "model_version": "logreg-v1@3f2a9c1d0b7e4a51", "prediction_time": "2026-11-03T09:00:00.000000Z",
 "volume_id": "vol-0001", "file_id": "file-000451", "horizon_seconds": 300, "score": 0.82,
 "calibrated": false, "candidate_sources": ["recent", "neighbor"], "reasons": ["coacc_max_conf:+0.83", "dir_opens_15m:+0.41"],
 "planned": true, "skip_reason": null}
```

**Warm plan** (`plans/<plan_id>.json`). README §3 shape, plus `created_at`
and per-item `score`/`reasons`:

```json
{"schema_version": 1, "run_id": "<controller.run_id>", "plan_id": "<controller_id>-20261103T090000Z",
 "model_version": "logreg-v1@3f2a9c1d0b7e4a51", "created_at": "2026-11-03T09:00:00.120000Z",
 "deadline": "2026-11-03T09:05:00.000000Z", "max_bytes": 25165824,
 "items": [{"task_id": "<plan_id>-001", "volume_id": "vol-0001", "file_id": "file-000451",
            "path": "/proj-amber-falcon/drawings/plan-0042.dwg", "offset": 0, "length": 16777216,
            "score": 0.82, "reasons": ["coacc_max_conf:+0.83", "dir_opens_15m:+0.41"]}]}
```

- **Empty plans** are not written. The tick is counted instead.
- **Paths:** appear only in plan files, never in predictions, logs or
  metrics. Plan and result files are created with mode `0600`, and pruned
  after `sinks.retention_days` (default 2).
- **Warm results** (README §4) are written by the dispatcher (§8.10). The
  Controller only reads them to count completed bytes for metrics.
  Correlating them with later demand is a later story.

### 8.6 Model pointer and hot swap

**Pointer** (ConfigMap, mounted as a directory volume, **no `subPath`**, so
updates propagate) at `controller.model_pointer`:

```yaml
model_uri: clearml://<model_id>          # or file:///models/logreg-v1-3f2a9c1d0b7e4a51.zip
feature_version: fv1
fallback: baseline-frequency
```

**`ModelManager`:**

1. **Poll:** every `model.poll_s` (30 s), it hashes the pointer file.
   - Unchanged, with a successfully active model → nothing happens.
   - Unchanged, but the last load **failed** → retry after `model.retry_s`
     (300), with bounded logging and the last error kept for `/readyz`.
   - Changed → fetch.
2. **Fetch:** `clearml://` uses the ClearML SDK to get a local copy of the
   model's package zip (credentials from a Kubernetes Secret via env).
   `file://` reads locally. Packages are cached in `model.cache_dir` by
   `package_id`. Because the ID covers the whole manifest (§5), cached
   copies can't disagree.
3. **Verify** with `load_package` (§5), with
   `expected_contract_sha256` = the **memory's** serving contract (the
   contract the running `FeatureStore` and `HandleTracker` were built
   with). Feature version and contract must both match, and
   `serving_policy.lead_s` must be 0.
   - A package with a different contract can't use the current memory.
     It is refused with `degraded: contract_mismatch`, which suppresses
     plans until the pointer is fixed.
   - Alternatively, run `pcache-controller run --reset-memory`, which
     starts cold under the new contract.
   - **At first start with no memory,** the first valid package's contract
     configures cleaning, features and candidates. Controller config never
     overrides them.
4. **Stage,** and swap **atomically at the next tick boundary**: scorer,
   co-access table and serving policy together. There is never a
   half-loaded model.
5. **On any failure:** keep the current model, set readiness to
   `degraded: model_load_failed` (not a plan gate while a good model is
   active), and retry per step 1.
6. **With no valid model** (at startup or after repeated failures): use the
   built-in `fallback` baseline with the co-access table from the last good
   package, or none. It runs under a conservative built-in policy
   (`max_items` 5, `max_bytes` 64 MiB, other values as §8.4 defaults).
   Readiness is `degraded: fallback_model`. With no contract known at all
   (first start, no package), the Controller is `not_ready` and doesn't
   plan.

Promotion is explicit: someone, or the pipeline, edits the ConfigMap.
Nothing follows tags or "latest".

### 8.7 Controller state: checkpoint and restore

- **Checkpoint:** every `memory.checkpoint_s` (300), write one atomic
  checkpoint of **all correctness-relevant state** to
  `memory.state_dir/checkpoint-<t>.bin` (atomic rename, keep 3, then update
  `latest`). It contains:
  - the `FeatureStore` checkpoint, and the `HandleTracker` state including
    open handles and late-read counters;
  - updates not yet applied (`known_at ≥` last tick);
  - the dedupe window (IDs and their first-seen `known_at`);
  - the last `known_at` stamp;
  - planner cooldown state and the plan sequence number;
  - the replay source cursor (replay mode);
  - the serving contract hash and the active `package_id`;
  - the raw ingest queue (events accepted but not yet cleaned);
  - a `clean_shutdown` flag, true only for a checkpoint written during an
    orderly shutdown (SIGTERM).
- **On start:** restore `latest` if its feature version and contract match
  the package to be activated.
  - If the restore fails or the checkpoint is corrupt →
    `degraded: restore_failed` and a cold start.
  - **Unclean restart** (`clean_shutdown` false), with the `http` source →
    `degraded: memory_gap` **regardless of checkpoint age**. Accepted
    events after the checkpoint can't be replayed, so continuity can't be
    proven.
  - Replay mode resumes from the cursor.
  - **Clean restart:** events sent while the Controller was down are lost
    at the source, so `memory_gap` applies if the downtime exceeds
    `memory.max_gap_s` (900).
  - With no checkpoint → a cold start with `degraded: memory_gap`.
- **Gating:** `memory_gap` and `restore_failed` **suppress plans** (§8.4)
  until `memory.warmup_s` (3600) of live events have been applied since
  start. `ingest_loss` suppresses plans until the loss rate stays ≤ 1% for
  5 minutes. `fallback_model` and `model_load_failed` don't suppress plans;
  the fallback runs under its conservative policy (§8.6).
- **Storage:** v1 uses a directory (an emptyDir or PVC). Predictive Kvrocks
  prefixes are used only after the README's isolation checks pass, and
  **never** CUFS volume Kvrocks.

### 8.8 Health and metrics

- **HTTP:** `/healthz` returns 200 if the process is alive. `/readyz`
  returns 200 for `ready` or `degraded` (with a JSON reasons list), and 503
  for `not_ready` (not started, or no scorer at all). `/metrics` is
  Prometheus text.
- **Counters:**
  - events received, kept, and excluded by reason;
  - `dropped_overflow`, dedupe-window size, `late_reads_after_completion`;
  - updates applied;
  - tick duration (summary), `tick_overruns`;
  - candidates per tick, planned items and bytes per tick, skips by reason;
  - info gauges for the active `model_version` and readiness.
- **Readiness** also becomes `degraded: ingest_loss` (a plan gate, §8.7)
  when drops exceed 1% of received events over 5 min. Drops are overflow
  plus excluded `unknown` origin plus the source's reported loss, if
  available.
- **Counters** also include `plans_suppressed` by reason,
  `dedupe_capacity_evictions` and `http_rejected` by reason.

### 8.9 Packaging

- **Image:** `predictive-cache-controller`, built from `python:3.12-slim`
  with `uv sync --no-dev --extra controller`. Entry point:
  `pcache-controller run --config /etc/pcache/controller.yaml`.
- **Example manifests** in `deploy/controller/`:
  - a Deployment with `replicas: 1` and `strategy: Recreate`;
  - a ConfigMap for `controller.yaml`, and a separate ConfigMap for
    `pointer.yaml`, mounted as a directory;
  - a Secret reference for ClearML credentials (no values committed);
  - an emptyDir or PVC for `state_dir` and the model cache;
  - a Service for `/v1/events` and the health endpoints.
  - Resource requests 1 CPU / 2 GiB.
- **Network:** confirm with the platform owner that the cluster can reach
  ClearML's file server (§15).

### 8.10 Plan-file dispatcher (Warmer side, #63)

The component that turns plan files into Warmer reads. It is owned by the
Warmer lane and runs next to the Warmer, separately stoppable from the
Controller.

**Safety gate.** The manual Warmer is documented as not ready for a live
pilot: it has no rate limit, pressure pause or global budget
([manual-warmer](../manual-warmer.md)). So the dispatcher ships with
`dispatcher.real_reads: false`, and uses a **fake reader** that records
what it would read. Real reads require **all** of:

- an explicit operator enable (`real_reads: true` **and** an enable file
  present);
- a global rate limit (`max_bytes_per_min`, `max_items_per_min`) and one
  in-flight read per volume;
- a pressure pause: the stop file, or a configured foreground-latency or
  cache-headroom signal from the protocol lane;
- the README feasibility/pilot gate signed off by the protocol and
  performance owners.

Until then, plans exercise the loop end to end without touching NFS.

- **Claim:** atomically rename `plans/<plan_id>.json` into
  `plans/claimed/`. A plan that is already claimed is skipped, so each
  plan is processed once per dispatcher. v1 runs one dispatcher.
- **Per item, in plan order:**
  1. Stop if `now ≥ deadline`. The remaining items get status `skipped`,
     reason `deadline`.
  2. Stop if cumulative requested bytes + `length` > the plan's
     `max_bytes`. Status `skipped`, reason `budget`.
  3. **Revalidate the target** close to execution: the path passes the
     shared Warmer path rules, it exists, and `offset + length ≤` the
     current size. Otherwise status `skipped`, reason `stale_target`.
     Identity beyond path + size isn't verifiable in v1 (§15).
  4. Run one `pcache-warmer` invocation (≤ 16 MiB, its own timeout ≤
     remaining time to the deadline).
  5. Write one result record.
- **Result** (`results/<plan_id>/<task_id>.json`, README §4 shape plus
  `reason`):

  ```json
  {"schema_version": 1, "run_id": "…", "plan_id": "…", "task_id": "…", "status": "completed",
   "reason": "ok", "bytes_requested": 16777216, "bytes_read": 16777216, "bytes_read_is_lower_bound": false,
   "started_at": "…", "finished_at": "…"}
  ```

  `status` ∈ `completed | partial | skipped | failed | cancelled`.
  `bytes_read` is taken from the Warmer's terminal record and never
  assumed.
- **Stop control:** a stop file (`dispatcher.stop_file`) or SIGTERM halts
  new items. An in-flight item finishes or hits its timeout. The remaining
  items are recorded as `cancelled`.
- Files are mode `0600` and pruned after `retention_days`. Warmer reads
  carry `origin = warmer` in the observation path, so their events are
  excluded from demand (§8.2).

## 9. ClearML tracking wrapper

`src/predictive_cache/tracking.py`:

```python
@contextmanager
def tracked(project: str, name: str, config: dict, tags: list[str]) -> Iterator["Tracker"]: ...

class Tracker:
    task_id: str | None
    def scalar(self, title: str, series: str, value: float, iteration: int = 0) -> None: ...
    def table(self, title: str, rows: list[dict]) -> None: ...
    def artifact(self, name: str, path: Path) -> None: ...
    def register_model(self, package_zip: Path, name: str, tags: list[str]) -> str | None: ...   # model_id
    def tag_model(self, model_id: str, tags: list[str]) -> None: ...
```

- **No-op tracker** (same API, returns None, still writes local outputs)
  when any of these hold: `clearml` isn't installed, `--no-track` is
  passed, or `PCACHE_TRACKING=off`.
- `CLEARML_OFFLINE_MODE=1` uses ClearML's offline mode.
- Project, task names and tags follow the ML platform doc §3.
- Never log secrets or paths.

## 10. Interfaces (for parallel work)

```python
# src/predictive_cache/ml/scorer.py           (#54)
class Scorer(Protocol): ...                                    # §3
def scorer_class(model_type: str) -> type[Scorer]: ...         # registry
def feature_matrix(rows: pa.Table, columns: Sequence[str]) -> pa.Table: ...

# src/predictive_cache/ml/package.py          (#54)
@dataclass(frozen=True)
class LoadedPackage:
    root: Path; package_id: str; model_type: str; model_version: str; feature_version: str
    feature_columns: tuple[str, ...]; scorer: Scorer; serving_policy: dict
    serving_contract_sha256: str; coaccess_path: Path; manifest: dict
    def coaccess(self) -> pa.Table: ...        # wrap with prep's CoAccessTable.from_parquet once it exists
    def score(self, rows: pa.Table) -> np.ndarray: ...   # checks key + feature columns, then scorer.score
class PackageError(Exception): reason: str
def write_package(scorer: Scorer, feature_schema: dict, coaccess_path: Path, smoke_rows: pa.Table,
                  meta: dict, out_dir: Path) -> Path: ...      # returns zip path
def load_package(path: Path, expected_feature_version: str,
                 expected_contract_sha256: str | None = None) -> LoadedPackage: ...
KEY_COLUMNS = ("volume_id", "file_id", "t")

# src/predictive_cache/ml/baselines.py        (#55)
class MruScorer, FrequencyScorer, HotsetScorer, CoaccessScorer  # implement Scorer

# src/predictive_cache/ml/logreg.py           (#56)
class LogRegScorer  # implements Scorer; fit uses scikit-learn, score is numpy-only

# src/predictive_cache/ml/train.py            (#56)
def train(snapshots_dir: Path, cfg: TrainConfig, out_dir: Path, tracker: Tracker) -> list[Path]: ...

# src/predictive_cache/ml/metrics.py          (#57)
@dataclass(frozen=True)
class Budget: kind: Literal["top_k", "bytes"]; value: int     # Budget.parse("top_25" | "bytes_256MiB"); .name
def select(rows: pa.Table, scores: np.ndarray, budget: Budget, range_cap: int = 16 MiB,
           min_score: float = float("-inf")) -> np.ndarray: ...   # bool mask; rows with score < min_score never selected
def evaluate(snapshots: pa.Table, universe: pa.Table, scores: np.ndarray, budgets: list[Budget], *,
             split: str | None = None, range_cap: int = 16 MiB, min_score: float = float("-inf")) -> dict: ...
# split None: inferred from the snapshots' single split; ambiguous -> ValueError (universe spans all splits)
# -> {"overall": {...}, "per_volume": {vol: {...}}, "per_tick": pa.Table, "n_ticks", "n_rows"};
#    metric keys "recall@<budget>", "precision@…", "warmed_bytes@…", "positive_warmed_share@…",
#    "coverage", "pr_auc", "unknown_at_t_share"; undefined ratios (zero denominator) are None
def micro(per_tick: pa.Table, metric: str) -> float | None: ...
def bootstrap_diff(per_tick_a: pa.Table, per_tick_b: pa.Table, metric: str, reps: int, seed: int) -> tuple[float, float]: ...
def reliability(scores: np.ndarray, labels: np.ndarray, bins: int = 10) -> tuple[list[dict], float] | None: ...   # None unless scores in [0, 1]

# src/predictive_cache/ml/evaluate.py         (#58)
def freeze(snapshots_dir: Path, packages: list[Path], cfg: EvalConfig, out: Path) -> dict: ...
def run(snapshots_dir: Path, split: str, packages: list[Path], freeze_path: Path | None, cfg: EvalConfig, out_dir: Path, tracker: Tracker) -> dict: ...

# src/predictive_cache/controller/sources.py  (#59)
class EventSource(Protocol): ...; class Clock(Protocol): ...    # §8.1
class ReplaySource, HttpSource; class RealClock, ReplayClock
class Ingestor:                                                  # stamping + queue + rules + dedupe + HandleTracker
    def offer(self, events: list[dict], receipt: datetime) -> tuple[int, int]: ...   # (accepted, dropped); never blocks;
                                                                  # stamps known_at = max(last, receipt) (replay: ingest_time)
    def drain(self, now: datetime) -> list[Update]: ...
    def state(self) -> bytes: ...; def restore(self, data: bytes) -> None: ...      # §8.7

# src/predictive_cache/warmer/paths.py        (shared with #40)
def validate_warmer_path(path: str) -> str | None: ...           # None if valid, else reason

# src/predictive_cache/dispatcher/            (#63)
def run_dispatcher(cfg: DispatcherConfig, reader: Callable[[dict], dict]) -> None: ...   # reader = one Warmer invocation

# src/predictive_cache/controller/planner.py  (#60)
class PathResolver(Protocol):
    def resolve(self, volume_id: str, file_id: str, t: datetime) -> str | None: ...
class CatalogPathResolver: ...                                   # datagen catalog file_paths, valid_from <= t < valid_to
def plan(scored: pa.Table, view: CandidateView, resolver: PathResolver, cooldown: "Cooldown",
         cfg: PlannerConfig, t: datetime, model_version: str, run_id: str) -> tuple[dict | None, pa.Table]: ...   # (plan, decisions)

# src/predictive_cache/controller/loop.py     (#60)
class Controller:
    def tick(self, t: datetime) -> None: ...
    def run(self) -> None: ...

# src/predictive_cache/controller/models.py   (#61)
class ModelManager:
    def poll(self) -> None: ...                                  # fetch + verify + stage
    def active(self) -> LoadedPackage | FallbackModel: ...       # swapped only at tick boundaries
    def readiness(self) -> tuple[str, list[str]]: ...

# src/predictive_cache/controller/state.py    (#62)
@dataclass
class ControllerState:                       # everything in §8.7, one atomic unit
    store: FeatureStore; tracker: HandleTracker; ingestor_state: bytes   # queue, dedupe, last known_at
    pending_updates: list[Update]; cooldown: dict[tuple[str, str], datetime]; plan_seq: int
    source_cursor: dict | None; contract_sha256: str; package_id: str | None
    clean_shutdown: bool; saved_at: datetime
def save_state(state: ControllerState, state_dir: Path) -> Path: ...
def load_state(state_dir: Path, cfg) -> ControllerState | None: ...    # raises StateError on corruption
```

`FeatureStore`, `CandidateView`, `generate_candidates`, `CoAccessTable`,
`HandleTracker` and `classify_event` come from data prep
([spec](../data-prep/spec.md) §10).

## 11. CLI and configuration

### 11.1 Commands

```text
pcache-train --snapshots DIR --out DIR [--config FILE] [--models ...] [--no-track]
pcache-eval freeze --snapshots DIR --packages ... --out freeze.json [--config FILE]
pcache-eval run --snapshots DIR --split val|test --out DIR [--freeze FILE] [--packages ...] [--rerun-reason TEXT] [--no-track]
pcache-eval policy --plans DIR --clean CLEAN_DIR --from T0 --to T1 --out DIR [--freeze FILE]
pcache-model inspect|verify PACKAGE_ZIP                           # prints manifest; runs load_package
pcache-model dummy --out DIR [--value 0.5]                         # constant-score fixture package (data_kind: fixture)
pcache-controller run --config FILE [--reset-memory]               # live (http source)
pcache-controller replay --config FILE --dataset DIR [--speed 60] [--from T0 --to T1]   # replay source + clock
pcache-dispatcher run --config FILE                                # plan-file dispatcher (§8.10)
```

### 11.2 Configuration

Cleaning, feature, co-access and candidate semantics are **not** Controller
config. They come from the active package's serving contract (§5, §8.6).

```yaml
train:
  models: [logreg-v1, baseline-mru, baseline-frequency, baseline-hotset, baseline-coaccess]
  logreg: {C_grid: [0.01, 0.1, 1, 10], max_iter: 2000}
  hotset: {top_n: 500}
  serving_policy:                    # defaults written into packages; min_score chosen on val (§6.3)
    tick_s: 60
    recent_skip_s: 300
    cooldown_s: 1800
    max_items: 20
    max_bytes_mib: 256
    range_cap_mib: 16
    min_score_quantiles: [0, 0.5, 0.8, 0.9, 0.95]
    min_score_keep: 0.95
eval:
  budgets: {top_k: [10, 25, 50], bytes_mib: [64, 256, 1024]}
  range_cap_mib: 16
  primary: recall@bytes_256MiB
  bootstrap: {reps: 1000, seed: 11}
controller:
  run_id: dev-run-001
  controller_id: ctl-0
  tick_deadline_fraction: 0.8         # of the package's serving_policy.tick_s
  model_pointer: /etc/pcache/model/pointer.yaml
  ingest:
    source: http
    listen: "0.0.0.0:8090"
    queue_max: 200000
    allowed_volumes: [vol-0001]
    http: {max_body_mib: 4, max_events: 10000, max_concurrent: 8, read_timeout_s: 2,
           token_env: PCACHE_INGEST_TOKEN}
  sinks: {predictions_dir: /var/lib/pcache/out/predictions, plans_dir: /var/lib/pcache/out/plans,
          results_dir: /var/lib/pcache/out/results, log_top_n: 100, retention_days: 2}
  planner_caps: {max_items: null, max_bytes_mib: null}   # optional; can only lower the package policy
  model: {poll_s: 30, retry_s: 300, cache_dir: /var/lib/pcache/models}
  memory: {state_dir: /var/lib/pcache/state, checkpoint_s: 300, max_gap_s: 900, warmup_s: 3600}
dispatcher:
  plans_dir: /var/lib/pcache/out/plans
  results_dir: /var/lib/pcache/out/results
  stop_file: /var/lib/pcache/out/STOP
  retention_days: 2
  real_reads: false                  # fake reader until the §8.10 gate is met
  enable_file: /var/lib/pcache/out/ENABLE_REAL_READS
  max_bytes_per_min: 67108864
  max_items_per_min: 20
  warmer: {host: null, port: 2049, uid: null, gid: null}   # from the protocol lane; no defaults committed
```

### 11.3 Pipelines (story #52, backlog)

Each stage CLI above, plus `pcache-datagen` and `pcache-prep`, is a step.
Datasets and models are registered per the ML platform doc §3. Tasks are
created once datagen and data prep are close to done.

## 12. Dependencies

```toml
[project.optional-dependencies]
clearml = ["clearml>=2.1,<2.2"]                           # tracking, datasets, model registry, remote runs
ml = ["scikit-learn>=1.5,<2"]                             # training only; runtime scoring is numpy/pyarrow only
```

`matplotlib` for evaluation curves is added with #58. The Controller
needs only `clearml` (to fetch packages) on top of the base dependencies.

The Controller uses the stdlib `http.server` (a threading server) for
`/v1/events` and the health endpoints. There is no web framework in v1.

## 13. Tests and acceptance

**Model package (#54):**
- write → load gives identical scores;
- each loader check (§5) has a failing fixture;
- changing any manifest field (policy, contract, columns) changes
  `package_id`;
- key columns and extra columns are accepted; a missing feature column is
  rejected;
- the dummy package works.

**Baselines (#55):** hand-built rankings; the hot set ignores val/test.

**Logreg (#56):**
- numpy vs scikit-learn within 1e-9;
- preprocessing statistics come only from train;
- a test-split file access raises;
- an unknown feature kind fails;
- `min_score` is chosen from val score quantiles and written to `serving_policy`;
- every packaged baseline (including MRU, whose scores are negative) yields
  a non-empty plan on an eligible fixture;
- runs with tracking off and without ClearML installed.

**Metrics (#57):**
- hand-computed fixtures for every metric;
- zero-positive ticks;
- positives outside the candidates count in the denominator;
- the byte budget skips and continues;
- the bootstrap is deterministic per seed.

**Eval (#58):**
- test without freeze → refused;
- a changed package hash, an extra package, a changed test file or
  manifest, or changed boundaries → refused before reading test rows;
- a second test run without `--rerun-reason` → refused, and with one → the
  reason is in the report;
- `freeze_id` recomputes, with the field omitted;
- test runs are logged;
- policy replay counts repeated planning once per demand bucket;
- the synthetic disclaimer is present.

**Ingestion (#59):**
- **streaming/batch parity** of update sequences on fixtures and datagen
  `tiny`;
- out-of-order HTTP batches and stale or future source `ingest_time` still
  give non-decreasing `known_at`;
- a late close, and a range read delivered after
  `close + completion_grace_s` with `read_mode: ranges` (never falls back
  to the close total);
- dedupe capacity eviction is counted;
- HTTP limits: oversize body, too many events, concurrency limit → 503,
  bad token → 401, disallowed volume dropped;
- overflow drops are counted without blocking;
- warmer-origin events never reach the store.

**Tick loop and planner (#60):**
- planner skip reasons, including Warmer path rules;
- `max_bytes` and the 16 MiB cap;
- cooldown;
- the policy comes from the package, and config caps can only lower it;
- plans are suppressed under gating readiness reasons;
- selector parity (§8.4);
- plans validate against §8.5, with files mode 0600;
- live-vs-batch **feature and score parity** at the same *t* on `tiny`
  replay;
- tick p95 recorded on `small` replay.

**Model manager (#61):**
- a valid swap takes effect next tick;
- a corrupt package, a feature-version mismatch, a **contract mismatch** or
  `lead_s ≠ 0` is refused and the old model kept;
- an unchanged failed pointer is retried after the backoff;
- the fallback runs under its conservative policy;
- no contract at first start → `not_ready`;
- no tick runs with a half-loaded model.

**State and packaging (#62):**
- checkpoint/restore parity of all §8.7 state (features, pending updates,
  dedupe, cooldown, cursor);
- `memory_gap` and `restore_failed` suppress plans until warm-up;
- an unclean restart with the `http` source → `memory_gap` regardless of
  checkpoint age; a clean restart within `max_gap_s` → no gap;
- manifests contain no secrets or `subPath`.

**Dispatcher (#63):**
- the default `real_reads: false` uses the fake reader;
- real reads stay refused without the enable file;
- rate limits;
- with a fake reader: deadline, budget and stale-target skips, stop
  control (`cancelled`);
- each plan is claimed once;
- results validate against §8.10.

**CI** runs unit and fixture tests and the `tiny` parity tests. `small`
replays and benchmarks are local or nightly, and measured values are
recorded.

## 14. Layout

```text
src/predictive_cache/ml/            scorer.py package.py baselines.py logreg.py train.py metrics.py evaluate.py cli.py
src/predictive_cache/controller/    sources.py ingest.py loop.py planner.py models.py state.py http.py cli.py
src/predictive_cache/tracking.py
deploy/controller/                  Dockerfile deployment.yaml configmap-controller.yaml configmap-pointer.yaml service.yaml
tests/ml/  tests/controller/
```

## 15. Work breakdown and open questions

| Issue | Story | Scope | Sections |
| --- | --- | --- | --- |
| [#53](https://github.com/rahulgtt/predictive-cache/issues/53) | #49 | This spec (+ data-prep §4.5 completion amendment) | all |
| [#54](https://github.com/rahulgtt/predictive-cache/issues/54) | #49 | Scorer interface, model package, loader, dummy package | §3, §5, §10 |
| [#55](https://github.com/rahulgtt/predictive-cache/issues/55) | #49 | Baselines | §4 |
| [#56](https://github.com/rahulgtt/predictive-cache/issues/56) | #49 | Logreg v1, `pcache-train`, tracking wrapper | §6, §9, §11, §12 |
| [#57](https://github.com/rahulgtt/predictive-cache/issues/57) | #50 | Metrics | §7.1–7.3 |
| [#58](https://github.com/rahulgtt/predictive-cache/issues/58) | #50 | `pcache-eval`, freeze, reports | §7.4–7.6, §11 |
| [#59](https://github.com/rahulgtt/predictive-cache/issues/59) | #51 | Event sources, streaming cleaning | §8.1–8.2 |
| [#60](https://github.com/rahulgtt/predictive-cache/issues/60) | #51 | Tick loop, planner, resolver, sinks | §8.3–8.5 |
| [#61](https://github.com/rahulgtt/predictive-cache/issues/61) | #51 | Model pointer and hot swap | §8.6 |
| [#62](https://github.com/rahulgtt/predictive-cache/issues/62) | #51 | Checkpoint, health/metrics, image, manifests | §8.7–8.9 |
| [#63](https://github.com/rahulgtt/predictive-cache/issues/63) | #51 (Warmer lane) | Plan-file dispatcher | §8.10 |

**Order and parallelism:**
- #54 first: the dummy package unblocks #60 and #61 on day one.
- #55, #57 and #59 can start immediately against fixtures.
- #56 needs #54 and #57.
- #58 needs #57.
- #60 needs data prep #30/#31 (stubs until then).
- #62 needs #30's checkpoint.

**Open questions:**
- **Live event source** (CUFS hook, OTel or instrumented client) and its
  origin semantics. It **must** export asynchronously from a bounded buffer
  outside the foreground read path (§8.1). Protocol lane; #34.
- **Target identity at execution:** v1 revalidates path + size only
  (§8.10). A generation or file-handle check needs protocol support.
- **Live path resolution** (`file_id → path`) for real volumes. Events
  carry no paths. Options: a resolver backed by the protocol lane, or a
  bounded directory listing of allowed folders. Owner approval needed.
- **Cluster → ClearML file server reachability,** and the Secret
  distribution. If either is unavailable, bake the package into the image
  or mount it from a volume.
- **Range choice** beyond the 16 MiB prefix, the planner objective (wait
  time vs opens, data-prep §15), and the Warmer's retry allowance.
- **Warm-result feedback:** correlating results with later demand and
  online useful-bytes metrics. Later Inference story.
- **Better models:** LightGBM ranker, sequence-model features. Each is a
  new `Scorer`; evaluate against the same freeze protocol.
- **Session Kvrocks** for checkpoints and plan queues, after the README
  isolation checks.
