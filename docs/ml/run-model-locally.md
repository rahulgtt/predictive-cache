# Download a trained model and run it locally

Trained models live in the ClearML model registry on the lab server. Nothing
is served from the server: download the package and score on your machine.

## What is in the registry

`pcache-train` registers one package per model in project
`predictive-cache/train`:

| Field | Value |
| --- | --- |
| Name | model type, for example `logreg-v1` |
| Tags | `candidate`, `data:<kind>`, plus `read-demand-v1` for READ models |
| File | the package zip (ML spec §5): weights, `feature_schema.json`, serving contract, co-read table and policy |

Packages trained on synthetic data are plumbing evidence only. Their metrics
are not real model-quality evidence.

## Download

- **UI:** `http://clearml.team32.internal:8080` → Models → pick the model
  → **Download** (or copy its ID).
- **CLI**, with ClearML SDK credentials (`CLEARML_CONFIG_FILE` or
  `~/clearml.conf`):

  ```bash
  uv sync --extra ml --extra clearml
  uv run pcache-model download <model-id> --out models/
  ```

  `download` verifies the package after copying it.

## Verify and inspect

```bash
uv run pcache-model verify  models/<model-id>.zip
uv run pcache-model inspect models/<model-id>.zip
```

The checks the package needs come from its own manifest. A READ package must
match the `read-demand-v1` semantic view, the `read-demand-fv1` feature schema
and the serving-contract hashes in this checkout. A mismatch means the model
was trained against a different contract version.

## Score rows locally

```bash
# a snapshot split (parquet directory), with labels: prints the top rows and hits
uv run pcache-model score models/<model-id>.zip --rows snapshots/split=test --top-k 20

# your own rows (.parquet, .jsonl or .csv) -> a scored file, highest score first
uv run pcache-model score models/<model-id>.zip --rows rows.parquet --out scores.csv
```

Each row needs `volume_id`, `file_id` and the package's feature columns
(for READ models, the 16 `read-demand-fv1` features in
[read-demand-v1](../data-prep/read-demand-v1.md#feature-and-label-registry)).
Null feature values are allowed.

To get rows:

- **Training snapshots:** download a Dataset from
  `predictive-cache/datasets/snapshots`. Its `snapshots/split=test` is the
  held-out split.
- **Your own READ events:** clean them with `pcache-prep clean
  --semantic-view read-demand-v1`, then build rows with `pcache-prep
  read-snapshots`. For a handful of files, call
  `predictive_cache.ml.read_demand.ReadDemandEngine.features` directly. The
  snapshot builder is parity-tested against it.

## Produce a new model (on the server)

All of this runs as ClearML tasks on the `default` queue. Push your branch
first, because the agent clones the pushed commit.

```bash
# raw datagen Dataset -> READ clean -> snapshots Dataset
uv run --extra clearml --extra ml python src/predictive_cache/clearml/read_snapshots.py \
    --input-dataset <raw-dataset-id> --queue default --tag size:large

# snapshots Dataset -> registered model packages
uv run --extra clearml --extra ml python src/predictive_cache/clearml/train.py \
    --snapshots-dataset <snapshots-dataset-id> --models logreg-v1,baseline-hotset \
    --queue default
```
