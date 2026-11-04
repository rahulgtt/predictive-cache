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

Model packages used with the READ-only MVP must satisfy the
[`read-demand-v1` semantic and hash gate](../data-prep/read-demand-v1.md).
Generic `fv1` open/session/write names are not aliases for READ features;
the separate `read-demand-fv1` registry uses explicit READ names.
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

### 6.4 Gradient-boosted trees (`gbdt-v1`, `gbdt-rank-v1`)

This is rung 4 of the model PRD. Both are `Scorer`s
(`predictive_cache.ml.gbdt`) over the package's raw feature columns, with
no preprocessing. Trees handle scale and nulls natively.

- **Training:** LightGBM (`ml` extra; on macOS also `brew install libomp`).
  `gbdt-v1` is binary log-loss with `sample_weight`, so its scores are
  probabilities. `gbdt-rank-v1` is LambdaRank grouped by `snapshot_id`
  (per-tick ordering), so its scores are not probabilities. Both
  early-stop on val (log-loss, NDCG@10). `pcache-train` picks
  `num_leaves` from `train.gbdt.num_leaves_grid` (default `[15, 63]`) by
  the val primary metric, like the logreg `C` sweep. Defaults:
  `learning_rate` 0.05, `rounds` 1000, `early_stopping` 50,
  `min_data_in_leaf` 200.
- **Package:** `model.json` holds the trees as flat JSON arrays (`feature`,
  `threshold`, `default_left`, `missing`, `left`, `right`, `value`). It is
  never a pickle or a LightGBM binary.
- **Serving:** pure numpy. The scorer reproduces LightGBM's numerical
  split and missing-value rules, parity-tested against `Booster.predict`.
  Loading a package or hot-swapping it in the Controller (§8.6) therefore
  needs no LightGBM. `explain` uses path (Saabas) contributions.
- **Swap compatibility:** a package is swappable for another exactly when
  its feature version and serving contract match (§5, §8.6). The model
  type never matters.

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
pcache-eval freeze (--snapshots DIR | --snapshots-dataset ID) --packages P1 --packages P2 ... --out freeze.json [--config FILE]
```

A package reference is a local zip/dir or `clearml://<model_id>` (fetched
from the model registry). The freeze needs at least one `baseline-*`
package, and every package must match the snapshots' serving contract.
A freeze record is never overwritten.

`freeze.json` records:
- package references, `package_id`s and the sha256 of each package's
  `package.json` (the exact set: no more, no fewer may be evaluated on
  test). `package.json` lists every payload hash and the loader verifies
  them, so this digest pins the whole package whether it is stored as a zip
  or a directory;
- each package's `serving_policy` and `serving_contract.sha256`;
- the best baseline (by val primary);
- the primary metric and budgets;
- the eval config hash;
- **the test data binding:**
  - the snapshots manifest sha256;
  - the sha256 of every file under `snapshots/split=test/` and `universe/`,
    computed from disk;
  - the split boundaries;
  - the label config;
- the git sha and creation time;
- `freeze_id` = the first 16 hex of the sha256 of the record's canonical
  JSON (sorted keys, no whitespace) **with the `freeze_id` field
  omitted**. Verification recomputes it the same way.

With tracking, `clearml://` packages are tagged `frozen` and
`freeze:<freeze_id>`, and the freeze record is uploaded as an artifact of
an `eval:freeze` task tagged `freeze:<freeze_id>`. After
freezing, any package, policy, config or data change needs a **new** freeze
record, and earlier test results stay recorded.

### 7.5 Run

```text
pcache-eval run (--snapshots DIR | --snapshots-dataset ID) --split val|test --out DIR
                [--freeze freeze.json] [--packages ...] [--rerun-reason TEXT] [--config FILE]
```

A run with `--freeze` reuses the frozen evaluation config unless `--config`
is given. A test run must then match it exactly.

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
    in `predictive-cache/eval` tagged `freeze:<freeze_id>` and `split:test`.
    Prior runs are counted from both (completed tasks carrying both tags).
    ClearML is the authoritative record, because a local file can be
    deleted. Even `--no-track` runs query it when a server is configured.
    An untracked run is recorded only in the local ledger. The report
    states the run number.
  - **Limitation:** this is lab-grade enforcement, not access control. The
    report says so.
- **Output:** `metrics.json` and `report.md`:
  - per model: overall and per-volume metrics, the primary at the
    package's `min_score`, and the difference vs the best baseline with a
    95% CI;
  - recall per budget (the curve);
  - reliability bins and ECE where scores are in [0, 1].

  Curves are stored as data in `metrics.json` and, with tracking, plotted
  as ClearML scalars. PNG files are not written, to avoid a plotting
  dependency.

  The report's header states `data_kind`. For synthetic data it reads
  "plumbing / planted-pattern evidence only".
- **With tracking:** a task `eval:<split>-<freeze id>` in
  `predictive-cache/eval`; `clearml://` packages are tagged `evaluated`
  after a test run. A refused run is marked `failed` with the reason.
- **Policy replay** (§7.7) is tracked separately in #77.

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
- **Live event source:** Chimera `nfs4_read_v2` through the read shipper
  (§8.1.1). Other sources (CUFS hook, OTel) are not planned for the MVP.

#### 8.1.1 Live Chimera transport (`read-demand-v1`, #59)

The reviewed Chimera patch (`chimera/chimera-nfs4-read-events-v2.patch`)
has **no network push**. Each Chimera pod's event worker appends bounded
`nfs4_read_v2` JSON lines to `nfs4_read_event_path`. This is an owner-only
`0600` regular file that rotates at 64 MiB to one `.1` backup. Durability
is published only as metrics:

- `chimera_nfs4_read_event_producer_info{producer_epoch}`;
- `chimera_nfs4_read_event_state{state=last_synced_sequence|queue_depth|worker_in_flight|delivery_clean|barrier_ready}`;
- `chimera_nfs4_read_events_dropped{stage=queue|encode|sink|rollback|rotation}`.

A visible line is not proof of durability. Live ingest is therefore **pull
at the pod, push to the Controller**:

```text
Chimera pod ┌ event worker ─▶ events.jsonl (+ .1) ─▶ read-shipper (sidecar) ─POST /v1/events─▶ Controller
            └ metrics: epoch, last_synced_sequence, barrier, drops ─────▶┘
```

**Read shipper** (a new small component, a sidecar in the Chimera pod).
It runs as the daemon UID, mounts the sink directory read-only and reads
metrics from localhost. It never touches the NFS request path; the
patch's worker queue already isolates foreground threads.

- **Tail:** it follows the active file by `(inode, offset)`. On rotation
  (the inode changes) it finishes the `.1` file from its saved offset, then
  switches to the new file. If the cursor's data has already rotated out of
  `.1`, the gap is counted as `rotated_unsent`.
- **Forwarding gate (per line):** the line's `producer_epoch` equals the
  live `producer_info` epoch and its `producer_sequence ≤
  last_synced_sequence`, so the line has completed `fdatasync`. Lines from
  an older epoch that were never sent after a restart are dropped and
  counted (`stale_epoch`). Counters are never subtracted across epochs.
- **Completeness barrier (for coverage, not forwarding):** the patch's full
  consumer barrier means all of the following hold in one coherent scrape:
  - `barrier_ready=1` and `delivery_clean=1`;
  - `queue_depth=0` and `worker_in_flight=0`;
  - all five drop counters are unchanged.

  Waiting for that before forwarding every line would stall under steady
  load, which the queue rarely drains. So forwarding uses only the synced
  watermark (low latency, best effort), and a coverage hour becomes
  `complete` only on a full barrier (Controller handling, below). This is
  a deliberate split of the patch's single consumer rule, and it keeps the
  patch's rule for any data used as evidence.
- **Batch:** at most 10 000 events, 4 MiB or 1 s, whichever comes first.
  With no traffic it sends an empty heartbeat batch every 10 s.
- **At-least-once:** it advances and persists its cursor `(producer_epoch,
  inode, offset, last_sequence)` only after a `202`. On a `5xx` or timeout
  it retries the same batch with backoff. The sink file is the only buffer
  (bounded to about 128 MiB by rotation); the shipper holds no unbounded
  memory.

**Request** (`POST /v1/events`, `Content-Type: application/json`, the same
HTTP limits and bearer auth as above):

```json
{
  "schema": "nfs4_read_v2",
  "pod_uid": "…", "producer_epoch": "<uuid>",
  "first_sequence": "1200", "last_sequence": "1450",
  "health": {"scraped_at_ns": "…", "last_synced_sequence": "1450",
             "barrier_ready": true, "delivery_clean": true, "queue_depth": 0, "worker_in_flight": 0,
             "dropped": {"queue": 0, "encode": 0, "sink": 0, "rollback": 0, "rotation": 0},
             "shipper_dropped": {"rotated_unsent": 0, "stale_epoch": 0}},
  "events": [ { "schema": "nfs4_read_v2", "local_id": "…", "...": "raw line, unmodified" } ]
}
```

- `health` is one coherent metrics scrape taken after the batch's last
  line was read. `dropped` holds cumulative Chimera counters for this
  epoch; `shipper_dropped` holds cumulative shipper counters.
- Every event's `producer_epoch` must equal the envelope's, and its
  `producer_sequence` must lie in `[first_sequence, last_sequence]`.
  Otherwise the whole request gets `400`.
- The response is `202 {"accepted": n, "dropped": m, "duplicate": d}`, and
  the Controller does not wait on processing. A body without
  `"schema": "nfs4_read_v2"` is a schema-v1 replay/test batch (the generic
  path in §8.2).
- `401`, `413` and `503` are as in the HTTP limits above. A malformed
  envelope gets `400` and is never retried.

**Controller handling**

- **Dedupe key:** `(pod_uid, producer_epoch, producer_sequence)`, which
  is exact and replaces `event_id` dedupe on this path. Chimera's `local_id` stays
  the canonical `event_id` for diagnostics. The per-`(pod_uid, epoch)`
  high-water marks are part of the Ingestor checkpoint (§8.7).
- **`known_at`:** the Controller receipt time, stamped strictly increasing
  as above (within a batch, in `producer_sequence` order), and passed as `durable_at` to
  `semantics.read_demand.project_chimera_read`. The shipper's gate ensures
  the line was already synced, so receipt time is never before durable
  availability.
- **Coverage:** buckets are event-time hours per `(volume_id, hour)` in
  `ingest.allowed_volumes`. Every hour starts `unknown`.

  It becomes `complete` only when every registered shipper has delivered a
  batch whose `health` scrape satisfies the full completeness barrier, with
  `scraped_at ≥ hour_end + ingest.event_time_skew_s` (default 30 s), and
  all sequences up to that scrape's `last_synced_sequence` have been
  accepted. In addition, all of the following held from the hour's start
  to that scrape:
  - sequences were contiguous within one epoch (no gaps);
  - heartbeats were at most `shipper_heartbeat_max_s` apart;
  - `delivery_clean` stayed true;
  - no Chimera or shipper drop counter changed;
  - no Controller-side loss occurred. Controller-side loss means:
    - `dropped_overflow`;
    - any row rejected for schema, `identity_valid=false` or projection
      errors (charged to its volume, or to every allowed volume when the
      volume can't be read);
    - a `400` batch from a registered shipper.

  Otherwise the hour is `incomplete`. An epoch change (Chimera restart)
  marks every hour that was not yet complete as `incomplete`. A row whose
  `event_time` falls in an hour that is already `complete` is counted
  (`late_after_final`) and makes that hour `incomplete` again. Failed
  READs and `warmer`/`unknown` rows are not loss: they are not demand.
  Status goes to `ReadDemandEngine.update_coverage`, so labels and
  windowed features become unknown rather than zero (read-demand-v1).
  Shippers are registered by config (`ingest.shippers`, expected
  `pod_uid`s). An unregistered `pod_uid` is rejected with `403`.
- **Origin:** projection follows the frozen contract. Every Chimera row is
  `origin=synthetic, origin_trust=client_asserted`. `demand_class` comes
  from `origin_claim`: `pc1:d` maps to `controlled_demand` and `pc1:w` to
  `warmer`; untagged reads map to `unknown`. **Consequence for the MVP:**
  only controlled-demand traffic (the `pc1:d:` tag) feeds features live.
  Untagged user reads are counted and excluded until a trusted foreground
  origin exists (read-demand-v1 § Remaining production dependencies, #34).
  Any lab-only policy that admits untagged reads is a contract change,
  not a config flag.
- **Metadata:** `file_size` is taken from the event when
  `file_size_valid`. `dir_id` is null. `metadata_status` follows the
  projection, and the package's `runtime_capabilities` must match
  (`verified_file_size`, `anchor_context`).

### 8.2 Streaming cleaning (parity with batch)

**`read-demand-v1` mode** (the live Chimera path, §8.1.1) does not use
the HandleTracker or the open/close update stream:

1. Validate each raw line with `sources.chimera.contract.ChimeraEvent`.
   Invalid lines and `identity_valid=false` are counted per reason and
   charged to coverage as Controller-side loss (§8.1.1).
2. Dedupe on `(pod_uid, producer_epoch, producer_sequence)`. Memory is bounded per
   `(pod_uid, epoch)` by the contiguous high-water mark plus a small
   out-of-order set.
3. Run `TargetMap.observe` (§8.11) on every valid event, including
   `warmer` and failed READs.
4. Keep only successful READs (`outcome=success`, `nfs_status=0`). Others
   are counted.
5. Map each event with `project_chimera_read(event, durable_at=known_at,
   dir_id=None, file_size=…)` to a `ReadDemandEvent`, then apply
   `ReadDemandEngine.update`. The engine excludes `warmer` and `unknown`
   rows itself, and the Controller counts them.

**Parity requirement:**
- `ReadDemandEngine` is the single feature implementation for offline
  (`build_read_demand_feature_rows`) and live paths.
- Replaying a projected dataset through the HTTP path in
  `(known_at, event_id)` order must yield the same features at fixed
  ticks as the offline builder.
- Candidates and anchors follow
  [read-demand-v1 § Co-read table and candidates](../data-prep/read-demand-v1.md#co-read-table-and-candidates)
  (#31).

**Generic mode** (datagen schema-v1 replay and tests). These share code
with data prep:

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

The lifecycle sequence below describes generic ingestion. The experimental
[READ replay runtime](../controller/read-replay-runtime.md) consumes canonical
READ rows directly, without `HandleTracker`, and preserves the READ contract's
`known_at <= t` visibility boundary. Its coverage checkpoints also carry
availability and observed-through times. It does not change generic `< t`
watermark behavior or establish a live Controller.

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

Inputs: scored candidates with their features, `CandidateView`, the
`TargetMap` (§8.11) and cooldown state. Rules apply in order, and every
skip records a reason:

| # | Skip reason | Rule |
| --- | --- | --- |
| 1 | `ineligible` | `view.is_eligible(file)` is false |
| 2 | `recently_read` | READ serving: `secs_since_last_read < recent_skip_s` (300). Generic OPEN replay uses `secs_since_last_open`. This is a recency heuristic, not proof of cache residency. |
| 3 | `cooldown` | Planned within `cooldown_s` (1800) |
| 4 | `below_threshold` | `score < min_score` (chosen on val, §6.3) |
| 5 | `unknown_size` | `file_size` null |
| 6 | `no_target` | `targets.resolve(volume, file, t)` returned None: never seen live with `identity_valid = true`, the entry is too old (`target_max_age_s`), its export isn't allowlisted, or it was marked stale (§8.11). |
| 7 | `budget` | Taken in `(score desc, file_id asc)` order while items < `max_items` and Σ length ≤ `max_bytes`. An item that doesn't fit is skipped and the walk continues. |

- **Range:** `offset 0`, `length = min(file_size, range_cap)` (16 MiB, the
  Warmer cap), further capped by the resolved target's size when known.
  A known-empty file is skipped. Candidate sizes must already be known as
  of the tick; target metadata does not fill an unknown demand feature.
  Choosing ranges other than the prefix is later work (§15).
- **Retry allowance:** 0. Each item's `max_bytes` equals its `length`, and
  the plan's `max_bytes` = Σ item lengths. The dispatcher never retries; a
  retry is a new `task_id` with `supersedes`.
- **Prediction horizon:** `horizon_s` stays unchanged in the active package
  and prediction records (default 300 s). It describes future demand, not
  how long a warming instruction may execute.
- **Target validity:** for each admitted item, `target_valid_until =
  min(t + target_valid_s, seen_at + target_max_age_s, t + 300 s)`.
  `target_valid_s` defaults to 120. The resolver must supply its explicit
  age bound; missing, future-dated or already expired targets are skipped.
  This is a Controller freshness limit, not an NFS handle expiry guarantee.
- **Execution deadline:** `min(t + horizon_s, every admitted item's
  target_valid_until)`. With fresh targets and defaults, predictions look
  ahead five minutes while the plan expires after two minutes. A target
  near its age limit shortens the whole plan further. Never extend target
  validity or shorten the model horizon to make a plan pass validation.
  Because `lead_s = 0` in v1, items may start at once. The existing dispatcher
  requires `created_at < deadline <= target_valid_until <= created_at + 300 s`
  for every valid item; it must not start an item after the deadline.
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

**Warm plan** (`plans/<plan_id>.json`): **schema v2**, the contract the
merged dispatcher accepts. The source of truth is
[docs/dispatcher.md](../dispatcher.md#schema-v2-plan-contract) and the
schema code `predictive_cache.dispatcher.schema`. The planner must
validate with that code in tests and must not re-implement it. Missing or
extra fields reject the whole plan.

```json
{"schema_version": 2, "run_id": "<controller.run_id>", "plan_id": "<controller_id>-20261103T090000Z",
 "model_version": "logreg-v1@3f2a9c1d0b7e4a51", "created_at": "2026-11-03T09:00:00.000000Z",
 "deadline": "2026-11-03T09:02:00.000000Z", "max_bytes": 16777216,
 "items": [{"task_id": "<plan_id>-001", "volume_id": "vol-0001", "file_id": "<64-hex file_id>",
            "export_name": "<export name>", "export_id": "<export id>", "target_fh_b64": "<unpadded base64url>",
            "target_valid_until": "2026-11-03T09:02:00.000000Z", "offset": 0, "length": 16777216,
            "max_bytes": 16777216, "score": 0.82, "reasons": ["coacc_max_conf:+0.83", "dir_opens_15m:+0.41"]}]}
```

- Identifiers: 1–128 characters from `[A-Za-z0-9._:-]`
  (`model_version` also allows `@`). Timestamps are UTC RFC 3339 ending
  in `Z`.
- Per item: `length ≤ 16 MiB` and `max_bytes == length`;
  `target_valid_until ≤ created_at + 300 s`. The optional `operation` may
  only be `"read"`; the optional `supersedes` holds `{run_id, plan_id,
  task_id}` of the item being retried.
- Paths are **not** part of v2. Plans carry opaque NFS file handles
  instead (§8.11).

- **Empty plans** are not written. The tick is counted instead.
- **Publication accounting:** the standalone planner keeps one bounded pending
  publication and advances cooldown/sequence only after successful durable
  publication. A collision or write failure latches publication off for manual
  reconciliation: a rename may have succeeded before directory `fsync` failed.
  An exception is not evidence that no plan became visible. This in-memory
  safeguard does not implement restart recovery or the full tick loop.
- **File handles** appear only in plan files and the in-memory target map
  (and its owner-only checkpoint), never in predictions, logs, metrics,
  datasets or ClearML. Plan and result files are mode `0600`, in `0700`
  directories. The dispatcher keeps them as PoC evidence and doesn't
  auto-prune (docs/dispatcher.md); retention is an operator decision.
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

Implemented in PR #84. **[docs/dispatcher.md](../dispatcher.md) is the
authoritative contract**: claim/state layout, schema v2 plan and task
files, result records, recovery, rate limits and the real-read gate. This
section only summarises what the Controller can rely on:

- **Claim:** each plan is claimed atomically and processed once. Items run
  sequentially (one global Warmer subprocess) in plan order.
- **Skips:** an item with a valid `task_id` but an expired deadline or
  `target_valid_until`, a malformed or unauthorized target, an
  out-of-range request, or no rate-limit capacity before its deadline gets
  a terminal `skipped` result with a precise reason. Later items continue.
  Plan-level schema or identity errors reject the whole plan.
- **Safety gate:** real NFS reads need `real_reads: true` **and** the
  `enable-real-reads` file. Otherwise the dispatcher records
  `skipped/dry_run` with actual bytes 0. Rolling limits default to 60 task
  starts and 64 MiB per minute. Pressure pause is deferred (#63). Live
  pilots still need the README feasibility sign-off.
- **Results:** status, reason, requested and actual bytes, and
  `bytes_read_is_lower_bound`, from the Warmer's terminal record. A
  `completed` read proves execution only, not residency or benefit.
  `stale`-handle errors feed back into the target map (§8.11).
- **No automatic retry.** A restart with an in-flight task yields
  `failed/ambiguous_manual_review`.
- Warmer reads are observed with `origin = warmer` and excluded from demand
  (§8.2). They still refresh the target map.

### 8.11 File identity and the target map (C5)

The model and features identify files by **`(volume_id, file_id)`**; plans
need an **NFS target**. Chimera `nfs4_read_v2` events carry both
(`contracts/nfs4-read-v2.schema.json`):

- `file_id` = SHA-256 over `"chimera-read-v2\0" ‖ mount_id ‖ len ‖
  inner_fh`. It is a stable hash of the server-side file handle, the same
  across export aliases and NFS file-handle key rotation. It is the
  **model identity** everywhere: datagen, cleaning, features, training,
  packages and plans. Datagen's `file-…` IDs are opaque stand-ins for the
  same thing, so datagen and data prep need **no** change.
- `export_id` + `target_fh` is the wrapped, export-specific handle needed
  for a READ. It can change with export alias or key rotation, so it is an
  **execution detail**, never a model identity.

**`TargetMap`** (Controller memory, #59 builds it, #60 uses it):

- **Update:** on every live event with `identity_valid = true`, of any
  origin including `warmer`, store
  `(volume_id, file_id) → {export_id, target_fh_b64, file_size, seen_at}`.
  The latest `seen_at` wins.
- **`resolve(volume_id, file_id, t)`** returns the entry with
  `export_name` from Controller config (`exports: {export_id: name}`, an
  allowlist) if `t − seen_at ≤ target_max_age_s` (default 3600) and the
  entry isn't marked stale. Otherwise it returns None, giving the
  `no_target` skip.
- **Stale feedback:** a dispatcher result with a stale or invalid handle
  error marks the entry stale until a newer event refreshes it.
- **Bounded:** an LRU over files seen within `target_max_age_s`. The map is
  included in the `ControllerState` checkpoint (§8.7), owner-only `0600`.
- **Never persisted elsewhere.** The v1 access-event schema deliberately
  has no handle field, so cleaned and training datasets, features, logs and
  ClearML never contain handles.
- **Replay/testing:** `CatalogTargetMap` derives deterministic fake handles
  from the datagen catalog. Use it only with the dispatcher's dry-run gate.
- **Limitation:** handle persistence across server restart or failover and
  live handle-mode reads are unverified (#40). Stale handles fail safely
  (the Warmer reports an error, and the entry is marked stale).

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

# src/predictive_cache/dispatcher/schema.py  (#63, merged; authoritative for plan/task/result v2)
# Plan.model_validate(raw) / PlanItem / result models: the planner validates with these in tests.

# src/predictive_cache/controller/planner.py  (#60, standalone READ component)
# Public signatures and examples: docs/controller/planner.md; source owns exact API.
@dataclass(frozen=True)
class ScoredCandidate:
    volume_id: str; file_id: str; score: float
    secs_since_last_read: float; file_size: int | None
    candidate_sources: tuple[str, ...] = ()
class TargetResolver(Protocol):                                  # structural seam to §8.11's map
    target_max_age_s: int
    def resolve(self, volume_id: str, file_id: str, t: datetime) -> ResolvedTarget | None: ...
# ResolvedTarget includes volume_id/file_id, export_id:int, export_name,
# target_fh_b64, file_size and seen_at. Convert export_id to str only in plan v2.
# Retain the exact resolved version privately; stale feedback invalidates that
# version only, so an old result cannot invalidate a newer refresh.
class PlannerConfig: ...                                        # lower-only item/byte caps and bounded sinks
class Selection: ...                                            # private plan/bindings plus handle-free predictions
def select_plan(...): ...                                       # pure selection -> Selection
class PlannerPublisher:
    def publish(...): ...                                       # private atomic files, cooldown/sequence on success
# No live tick/FeatureStore wiring or durable restart recovery in this component.
# Catalog-derived fake targets are for dispatcher dry-run fixtures only.

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
pcache-eval freeze (--snapshots DIR | --snapshots-dataset ID) --packages REF ... --out freeze.json [--config FILE] [--no-track]
pcache-eval run (--snapshots DIR | --snapshots-dataset ID) --split val|test --out DIR [--freeze FILE] [--packages REF ...] [--rerun-reason TEXT] [--no-track]
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
    semantic_view: read-demand-v1     # or generic-v1 (datagen replay/tests)
    shippers: []                      # expected Chimera pod_uids (§8.1.1); others get 403
    shipper_heartbeat_max_s: 60
    event_time_skew_s: 30             # allowed producer clock skew before an hour can be complete
    http: {max_body_mib: 4, max_events: 10000, max_concurrent: 8, read_timeout_s: 2,
           token_env: PCACHE_INGEST_TOKEN}
  sinks: {predictions_dir: /var/lib/pcache/out/predictions, plans_dir: /var/lib/pcache/out/plans,
          results_dir: /var/lib/pcache/out/results, log_top_n: 100, retention_days: 2}
  planner_caps: {max_items: null, max_bytes_mib: null}   # optional; can only lower the package policy
  model: {poll_s: 30, retry_s: 300, cache_dir: /var/lib/pcache/models}
  memory: {state_dir: /var/lib/pcache/state, checkpoint_s: 300, max_gap_s: 900, warmup_s: 3600}
dispatcher: {}                       # configured per docs/dispatcher.md (real-read gate, rolling limits, trusted endpoint)
targets:
  exports: {}                        # export_id -> export_name allowlist (no defaults committed)
  target_max_age_s: 3600
  target_valid_s: 120                # <= 300 (dispatcher cap)
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
- **READ mode:** features at fixed ticks from the HTTP path equal
  `build_read_demand_feature_rows` on the same projected events;
- **shipper:** rotation mid-batch (finishes `.1` then continues), a gap
  past `.1` counted as `rotated_unsent`, lines above `last_synced_sequence`
  held back, an epoch change drops unsent old-epoch lines, retry of the
  same batch is deduped by `(pod_uid, producer_epoch, producer_sequence)`,
  and an envelope/event epoch or sequence mismatch gets `400`;
- **coverage:** an hour stays `unknown` until a full barrier scraped after
  `hour_end + event_time_skew_s`; a sequence gap, a missed heartbeat, any
  drop-counter change, Controller overflow, a rejected row, an epoch
  change or a `late_after_final` row marks it `incomplete`, and labels for
  it are unknown;
- untagged (`origin_claim=unknown`) and `pc1:w` reads never change features,
  but do refresh the `TargetMap`;
- **streaming/batch parity** (generic mode) of update sequences on
  fixtures and datagen `tiny`;
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
- planner skip reasons, including `no_target` (target map miss, too old, stale, export not allowlisted);
- plans validate with `predictive_cache.dispatcher.schema` (v2): `max_bytes == length`, `target_valid_until ≤ created_at + 300 s`;
- the target map is updated from live events (any origin), never written to datasets or logs, and marked stale by stale-handle results;
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
- covered by the merged dispatcher test suite (`tests/dispatcher/`): claim
  once, deadline/budget/target/rate-limit skips, stop control, v2 schema;
- remaining gaps tracked in #63 (pressure pause; retention decision).

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
| [#58](https://github.com/rahulgtt/predictive-cache/issues/58) | #50 | `pcache-eval`, freeze, val/test runs, ledger, reports | §7.4–7.6, §11 |
| [#77](https://github.com/rahulgtt/predictive-cache/issues/77) | #50 | `pcache-eval policy` replay (blocked on #60, #28) | §7.7 |
| [#59](https://github.com/rahulgtt/predictive-cache/issues/59) | #51 | Event sources, streaming cleaning, target map updates | §8.1–8.2, §8.11 |
| [#60](https://github.com/rahulgtt/predictive-cache/issues/60) | #51 | Tick loop, planner (v2 plans), sinks | §8.3–8.5, §8.11 |
| [#61](https://github.com/rahulgtt/predictive-cache/issues/61) | #51 | Model pointer and hot swap | §8.6 |
| [#62](https://github.com/rahulgtt/predictive-cache/issues/62) | #51 | Checkpoint, health/metrics, image, manifests | §8.7–8.9 |
| [#63](https://github.com/rahulgtt/predictive-cache/issues/63) | #51 (Warmer lane) | Plan-file dispatcher (merged in PR #84) | §8.10, docs/dispatcher.md |

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
