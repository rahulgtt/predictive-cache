# ML platform (ClearML)

> **Status:** an internal, temporary hackathon lab. Story
> [#43](https://github.com/rahulgtt/predictive-cache/issues/43).
>
> - **Owner:** @hgandhe-debug. Ask the owner for access, restarts and
>   upgrades.
> - **Infra code is not in this repo** (team decision). This page is the
>   shared reference. It contains no credentials, IP addresses or private
>   paths; keep it that way.

## 1. What runs where

One ML VM on the Strider vSphere lab (Rocky Linux; default size 16 vCPU,
64 GB RAM, 80 GB OS disk, 720 GB `/data` disk). Docker Compose runs:

| Component | Version | Endpoint | Purpose |
| --- | --- | --- | --- |
| ClearML web UI | server 2.4.0 | `http://clearml.team32.internal:8080` | Experiments, datasets, models, pipelines, queues |
| ClearML API | server 2.4.0 | `http://clearml.team32.internal:8008` | SDK / agent API (`/debug.ping` for health) |
| ClearML file server | server 2.4.0 | `http://clearml.team32.internal:8081` | Artifact, dataset and model storage |
| Elasticsearch, MongoDB, Redis | 8.19 / 8.0 / 8.2 | internal only | ClearML backing stores (memory-bounded) |
| Agent `clearml-services` | agent 1.5.0 | queue **`services`** | Pipeline controllers and light service tasks |
| Agent `clearml-worker` | agent 1.5.0 | queue **`default`** | Pipeline steps and training/eval tasks, **one at a time**. Docker mode (`python:3.12-bookworm`), capped at **12 CPU / 32 GB** per task. Dataset cache shared at `/data/clearml-cache`. |

- **Storage:** all server state lives on the VM's `/data` disk.
- **No backups:** losing the VM loses all experiments, datasets and models.
  Copy anything needed as evidence into the issue or `docs/` (sanitized), or
  into approved private storage.
- **No GPU.** Everything we plan (LightGBM, baselines, data prep) is
  CPU-only by design.

## 2. Access

1. **Name resolution:** `clearml.team32.internal` isn't in public DNS. Ask
   the owner for the VM address and add a hosts-file entry on your machine.
   Inside ClearML containers and on the VM, the name already resolves.
2. **Web login:** a single shared lab account with no per-user RBAC. Ask the
   owner. Treat everything on the server as visible to the whole team.
3. **SDK credentials:** in the web UI, go to *Settings → Workspace → Create
   new credentials*. Then either run `clearml-init` (which writes
   `~/clearml.conf`) or export:

   ```bash
   export CLEARML_API_HOST=http://clearml.team32.internal:8008
   export CLEARML_WEB_HOST=http://clearml.team32.internal:8080
   export CLEARML_FILES_HOST=http://clearml.team32.internal:8081
   export CLEARML_API_ACCESS_KEY=<from the UI>
   export CLEARML_API_SECRET_KEY=<from the UI>
   ```

   **Never commit** `clearml.conf`, keys or `.env` files.
4. **Agents and git:** remote tasks run on the VM's agents. The agents
   clone this repository with credentials configured on the VM by the
   owner. A remote run uses **committed code** at the commit recorded by
   the task. Uncommitted diffs are only acceptable for personal sandbox
   runs (§3).

## 3. Conventions

### Code

- All logic lives in the repo package (`src/predictive_cache/...`) behind
  plain CLIs, like `pcache-datagen` and `pcache-prep`. ClearML code is a thin
  wrapper: `Task.init`, connect config, report metrics, upload artifacts.
- Everything must run **without** the server. Set
  `CLEARML_OFFLINE_MODE=1` (or `Task.set_offline(True)`), or skip tracking
  entirely. CI and unit tests never need ClearML.
- Notebooks are for exploration. They are never the source of a reported
  result; port anything that matters into a script.

### Projects and tasks

| ClearML project | Contents |
| --- | --- |
| `predictive-cache/datagen` | Generator runs |
| `predictive-cache/prep` | Cleaning and snapshot runs |
| `predictive-cache/train` | Baseline and model training runs; registered models |
| `predictive-cache/eval` | Frozen-model evaluation runs on held-out data |
| `predictive-cache/pipelines` | Pipeline controllers |
| `predictive-cache/datasets/<stage>` | ClearML Datasets (`synthetic`, `clean`, `snapshots`) |
| `predictive-cache/sandbox/<github-username>` | Personal experiments; nothing here is evidence |

- **Task names:** `<stage>:<short-name>`, e.g. `train:lgbm-ranker`,
  `eval:test-v1`.
- **Tags:**
  - the GitHub issue (`#32`);
  - the data kind (`data:synthetic` / `data:real`);
  - the seed (`seed:42`).
- **Config:** the resolved YAML config is connected as task configuration,
  so a cloned task can be edited and re-run from the UI.

### Datasets

- **Name and immutability:** a ClearML Dataset's name is our manifest
  `dataset_id` (e.g. `synth-small-s42-1a2b3c4d`,
  `clean-synth-small-s42-…`). Datasets are immutable once finalized.
- **Lineage:**
  - The ClearML parent is the parent dataset.
  - `manifest.json` is included unchanged, so lineage is readable without
    ClearML.
- **Truth:** datagen `truth/` is uploaded only as a **separate** dataset
  tagged `truth`. It is used by tests and evaluation reports only, never by
  feature or training tasks.

### Models

- **Registration:** trained models go into `predictive-cache/train`. A model
  package contains:
  - the model file;
  - `feature_schema.json`;
  - the co-access table;
  - calibration (if any);
  - a manifest of dataset and snapshot IDs, code commit and metrics.

  The training/eval spec defines the exact layout.
- **Tags** describe lifecycle: `candidate` (trained) → `frozen` (selected on
  validation, before test) → `evaluated` (scored once on test).
- **Deployment pins an immutable model ID, never a tag.** How the
  Controller selects and refreshes a model is defined in the inference spec
  (proposal: a Kubernetes ConfigMap holding the model ID).

## 4. Data policy

| Data | Allowed on this server? |
| --- | --- |
| Synthetic datagen output, cleaned data, snapshots | Yes |
| Real access events, or anything derived from them | **Only with data-owner approval** of storage, access and retention. The lab server is not approved private storage. No file contents, raw usernames, credentials or identifying paths, in any case (README, AGENTS.md). |
| Secrets, kubeconfigs, credentials | **Never.** ClearML records task environment and config, so never put them in task parameters. |

Results computed on synthetic data are plumbing and pattern-recovery
evidence only, never model-quality claims.

## 5. Operations

| Need | How |
| --- | --- |
| Is it up? | `curl http://clearml.team32.internal:8008/debug.ping` |
| Are workers alive? | Web UI → *Workers & Queues*: `clearml-services` on `services`, `clearml-worker` on `default` |
| A task is stuck in a queue | Check the queue name (`default` for steps, `services` for controllers) and the worker status, then ask the owner |
| Restart, upgrade, resize, disk full | Ask the owner. The VM is managed outside this repo. |

### Datagen task smoke run

The optional `pcache-clearml-datagen` entrypoint in this repo is a single
compute task, not the full pipeline. From a committed checkout with SDK
credentials configured (§2), run:

```bash
uv sync --locked --extra clearml
CLEARML_FILES_HOST=http://clearml.team32.internal:8081 \
  uv run --extra clearml python src/predictive_cache/clearml/datagen.py --preset tiny --queue default
```

Use the same file-server hostname as the worker. The submitting machine
may reach the API by IP, but an IP-based `CLEARML_FILES_HOST` makes the
worker's artifact upload fail with HTTP 401. Add the hosts-file entry in §2
to download artifacts from machines without internal DNS.

The submitting process records the resolved scenario in ClearML before it
queues the task. A worker reuses that scenario, generates into a fresh temporary
directory, validates the complete output, and uploads a synchronous
`synthetic-dataset-test-only` task artifact. That archive includes `truth/`:
it is for compatibility tests only and must never be passed to feature or
training code. The worker also publishes a finalized ClearML Dataset in
`predictive-cache/datasets/synthetic`, named by the generator `dataset_id`.
It contains only `events/`, `catalog/`, `loss/`, `config.resolved.yaml`, and
a projected `manifest.json`; it excludes `truth/`, truth-derived reports,
truth-only counts and planted-pattern details. It retains `known_gaps` as
coverage metadata required by the data-prep contract: cleanup marks those
hours incomplete, and later label builders must not treat missing reads there
as negatives. `known_gaps` is not a model feature. The full generator manifest
stays in the test-only ZIP.
The task's `outputs.synthetic_dataset_id` is the immutable ID to pass to the
cleaning job. Repeated runs with the same scenario can create another Dataset
version with the same name; use the exact ID, not the name, for lineage.
The projected manifest lists exactly the published files; downstream cleaning
verifies their hashes and never opens `truth/`.

Use `--config scenario.yaml` instead of `--preset tiny` for a custom scenario;
`--seed` and `--format` are optional overrides. The YAML is needed on the
submitting machine only. The ClearML configuration can be edited on a cloned
task and rerun. `--local` runs with tracking on the submitting machine;
`--no-track --out /tmp/fresh-dir` runs without ClearML for a local check.
Start with `tiny`; larger preset row and runtime targets are not verified.
Check the task console, validation result, artifact, code revision and resolved
scenario before treating a run as complete.

### Training task

`pcache-train` (ML spec §6.3) trains logistic regression and the baselines
from a snapshot directory, chooses settings on validation data, and writes
model packages. To try it without real data:

```bash
uv sync --locked --extra ml
uv run pcache-model fake-snapshots --out /tmp/pcache-snaps     # data_kind: fixture
uv run pcache-train --snapshots /tmp/pcache-snaps --out /tmp/pcache-train --no-track
```

**With ClearML:**
- configure SDK credentials (§2). Tracking stays off unless a server is
  explicitly configured;
- upload the snapshot directory as a ClearML Dataset;
- run on the `default` worker:

  ```bash
  uv sync --locked --extra ml --extra clearml
  uv run --extra ml --extra clearml python src/predictive_cache/clearml/train.py \
      --snapshots-dataset <dataset-id> --queue default
  ```

The task records `inputs.snapshots_dataset_id` and the `train` config. To
retrain on another dataset, clone the task in the UI, edit the dataset ID
and enqueue it. Each package is registered as an output model tagged
`candidate`; its `package_id` is in the model metadata. Fixture and
synthetic results are plumbing evidence only.

## 6. Not decided yet

- **Pipelines** (datagen → clean → snapshots → train → eval → register):
  to be built once datagen and data prep are done, or close to it.
- **Model package layout, metrics and evaluation protocol:** the
  training/eval spec (to be written).
- **Controller model selection and refresh:** the inference spec. Proposal:
  - a ConfigMap pins the model ID and the expected feature version;
  - the Controller watches the mounted file and verifies the package (hash,
    feature version, smoke score);
  - it swaps between ticks, and falls back to a baseline on failure.
- **Cluster → ClearML reachability:** the Controller would need to reach the
  file server (port 8081) from the Strider cluster, with credentials in a
  Kubernetes Secret. If it can't, the model package ships another way (e.g.
  baked into the Controller image).
