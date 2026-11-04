# Predictive Cache: authoritative two-minute recorded demo plan

**Frozen source:** `origin/main` at `1495af4` on 2026-11-03.  
**Recording target:** 116 seconds of content plus a 4-second edit buffer.  
**Audience:** hackathon judges and engineering stakeholders.  
**Format:** one coherent recorded story; transparent hybrid of validated clips and
clearly labeled fixture handoffs.  
**Claim rule:** never imply that a completed NFS read proves cache residency,
latency benefit, model quality, or foreground safety.

## Objective and success criterion

**Objective:** Show how validated demand metadata can become a bounded, read-only
warming decision through Strider without placing prediction in the foreground
read path.

**Demo success criterion:** In at most two minutes, a viewer can identify the
problem, the two-path architecture, one traceable event-to-result example, what
is implemented versus fixture-only, the current evidence boundary, and the next
controlled experiment needed before claiming benefit.

## Current readiness and evidence boundary

Use these status labels consistently on slides, captions, and spoken narration:

- **IMPLEMENTED / VALIDATED:** present on `origin/main` and supported by tests or
  recorded handoff evidence.
- **FIXTURE-VALIDATED:** implemented and tested, but only against synthetic or
  planted fixture data; it is not real-workload evidence.
- **IN PROGRESS / OFFLINE EVIDENCE:** work exists only in an unmerged,
  uncommitted, or separately reviewed snapshot; it is not a main-branch or live
  cluster capability.
- **HISTORICAL:** a dated observation that must be rechecked before operational
  use.
- **FIXTURE / PROPOSED:** an interface example used to explain the intended
  handoff; it is not execution evidence.
- **UNVERIFIED CLAIM:** a question the next experiment must answer.

| Surface | Recording label | Confirmed fact to show | Do not imply |
|---|---|---|---|
| Synthetic datagen | IMPLEMENTED / VALIDATED | `pcache-datagen` generates, validates, describes, hashes, and reports fictional access records. | Real-user history or cache behavior. |
| Demand cleaning | IMPLEMENTED / VALIDATED | `pcache-prep clean` excludes warmer, synthetic, unknown-origin, failed, invalid, and duplicate records and emits lineage/coverage. | A confirmed live Chimera source or complete observation. |
| Baselines and logistic trainer | FIXTURE-VALIDATED | MRU, frequency, static hot set, co-access, logistic regression, package verification, and fixture training exist on main. | Real-workload model quality or a calibrated probability. |
| Evaluation CLI | IN PROGRESS | PR #78 reports 312 tests and fixture/ClearML evidence; it is not on the frozen main revision. | A merged or real-data evaluation result. |
| Chimera SDK v2 READ correlation | IN PROGRESS / OFFLINE EVIDENCE | Reported offline builds: CUFS `46/46` tests and integrated adapter `3/3` CTests. Event origin remains `unknown`; payload lacks stable file/range identity. | A deployed event, client receipt, trustworthy demand, or live end-to-end correlation. |
| Cluster and NFS export | HISTORICAL | `nasuni-cluster2` was reported healthy on 2026-11-03; a read-only export was reported Ready. State must be rechecked. | Current health, approved client access, fixture readability, or cache residency. |
| Manual Warmer | IN PROGRESS | The supervisor design and guide are tracked; reported source/tests are outside main. | A current executable `pcache-warmer`, live NFS interoperability, or benefit. |
| Planner, dispatcher, Controller | FIXTURE / PROPOSED | Main contains a tested score-and-byte-budget selection primitive and illustrative plan/result contracts. | A live runtime, production queue, or executed plan. |
| Performance benefit | UNVERIFIED CLAIM | No confirmed matched cold/warm trial exists. | Faster reads, fewer cloud bytes, safe cache occupancy, or model superiority. |

The current controlled data-collection gate remains open: there is no confirmed
representative historical dataset, trusted live origin, stable file identity,
complete coverage, or authorized live source suitable for model-quality claims.
Synthetic records prove contracts and plumbing only.

## Exact 116-second timeline, storyboard, and narration

Speak at approximately 145-155 words per minute during narrated beats. The
script is 240 words, approximately 93-99 seconds of speech, with 11-12 seconds
of marked silent reading beats and 17-23 seconds of total non-narrated time
inside the 116-second content timeline. Do
not add unscripted technical detail. Keep the four-second end buffer for edit
variance; do not fill it with another claim.

| Time | Frame / heading | On screen and transition | Exact voiceover |
|---|---|---|---|
| `00:00-00:14` (14s) | **1. Cold reads wait on cloud** | Simple animation: application read enters Chimera, reaches CUFS, and a missing range reaches cloud. Show a clock beside the user. At `00:11`, split the path into foreground and background. | “When project data is cold, a read can wait for bytes from cloud storage. Predictive Cache asks: can likely next reads be prepared in the background without making normal reads depend on prediction?” |
| `00:14-00:36` (22s) | **2. Observe, decide, warm - with hard boundaries** | Architecture frame: solid foreground path `Client -> Chimera -> CUFS -> Cloud`; separate background path `validated events -> Controller -> bounded plan -> Warmer -> NFS -> Chimera`. Add badges: “foreground independent,” “read-only,” “bounded,” “unknown origin excluded.” Hold silently for 3-4 seconds so viewers can trace both paths, then zoom into one event. | “The foreground path stays unchanged. A separate Controller consumes validated metadata, scores candidates, and emits a byte-bounded plan. A separately stoppable Warmer would issue ordinary read-only NFS reads. Warmer, synthetic, and unknown-origin events are excluded, and predictive state never goes into CUFS volume Kvrocks.” |
| `00:36-01:14` (38s) | **3. One traceable example - validated code plus labeled fixtures** | Four quick panels using the assets and commands below. `00:36-00:47`: generated JSONL event and clean report, including a 2-second silent read. `00:47-00:58`: fixture training output with baseline and logreg package IDs, including a 2-second silent read. `00:58-01:06`: Chimera SDK v2 offline test clip beside an identity-gap warning, including a 2-second silent read. `01:06-01:14`: animated fixture plan -> dispatcher -> Warmer READ attempt -> result, with a final 2-second silent hold, all under a persistent amber “FIXTURE HANDOFF - NOT LIVE” banner. | “Here is the honest end-to-end story. Main can generate and clean versioned JSONL, preserving identity and excluding unsafe origins. It can train baseline and logistic scorers on planted fixture signals and package their versions. Offline Chimera-v2 work has bounded READ-event tests, but its event still lacks trusted origin and stable file identity. So the final plan, dispatcher, Warmer read, and result shown here are explicit contract fixtures, not a live success.” |
| `01:14-01:39` (25s) | **4. What the evidence proves - and does not prove** | Evidence ladder with green, amber, and gray rows. Show datagen handoff `161 tests`; cleaning acceptance `693,947 deliveries -> 641,973 kept`, with a visible “synthetic” watermark; Chimera offline `46/46 + 3/3`; bottom row “matched cold/warm benefit: not run.” | “Confirmed evidence covers synthetic generation, cleaning, model-package plumbing, and offline protocol checks. The cleaning run reconciled every synthetic delivery, but synthetic scale is not model quality. We have not shown comparable cold state, durable CUFS residency, lower demand latency, safe foreground impact, or that the learned model beats simple policies on authorized real data.” |
| `01:39-01:56` (17s) | **5. Next experiment: prove mechanism before benefit** | Return to the architecture. Highlight one approved file range and three treatments: no warm, simple baseline, learned plan. End on a stop-sign-shaped “GO only with equivalent cold state + origin + safety evidence.” | “Next, run one controlled matched experiment: verified cold control, equal-byte simple and learned plans, bounded NFS execution, later independent demand, and foreground guardrails under one run ID. Until then, this is a data-and-contract foundation, not a cache-benefit claim.” |
| `01:56-02:00` (4s) | **Edit buffer** | Hold the final frame. No new narration. Optional quiet title: “Predictive Cache - evidence first.” | No narration. |

### Slide headings and transitions

1. **Cold reads wait on cloud** -> split one path into foreground and background.
2. **Observe, decide, warm - with hard boundaries** -> zoom into an event.
3. **One traceable example - validated code plus labeled fixtures** -> wipe from
   terminal clips to the evidence ladder.
4. **What the evidence proves - and does not prove** -> collapse unresolved rows
   into the next-experiment diagram.
5. **Next experiment: prove mechanism before benefit** -> hold final gate.

## Example run: exact commands, assets, outputs, and interpretation

### Recording rule

Do not type commands live during the final two-minute recording. Capture them
ahead of time in a clean terminal at 1440p or higher, trim pauses, and play the
validated clips at normal readable speed. Keep the source revision visible once
at the start of the command sequence:

```bash
git rev-parse --short HEAD
```

Expected frozen value:

```text
1495af4
```

If main changes before capture, update the revision and revalidate every
main-branch command and status statement; do not retain this expected hash.

### A. Implemented main-branch data clip

Run on an approved build/test host using the repository's normal `uv`
environment. Use a fresh private directory and JSONL so one event can be shown:

```bash
export RUN_ROOT=/tmp/pcache-demo-$(date -u +%Y%m%dT%H%M%SZ)
uv sync --locked --extra ml
uv run --locked --extra ml pcache-datagen generate \
  --preset tiny \
  --format jsonl \
  --out "$RUN_ROOT/raw"
uv run --locked --extra ml pcache-datagen validate "$RUN_ROOT/raw"
uv run --locked --extra ml pcache-datagen describe "$RUN_ROOT/raw"
uv run --locked --extra ml pcache-prep clean \
  --input "$RUN_ROOT/raw" \
  --format jsonl \
  --out "$RUN_ROOT/clean"
uv run --locked --extra ml pcache-prep describe "$RUN_ROOT/clean"
```

Expected output shapes:

```text
Generated <dataset_id> at <RUN_ROOT>/raw; synthetic workloads, operations and telemetry faults.
Valid: <artifact_count> artifacts, <event_count> events. Scope: artifact and semantic checks; see report for recovery diagnostics.
<dataset_id>: <event_count> events; synthetic; seed=<seed>
Cleaned <clean_dataset_id> at <RUN_ROOT>/clean
<clean_dataset_id> from <dataset_id>
```

Both `describe` commands continue with the full Markdown summary. In the clip,
show only the one-line header plus a cropped, readable region containing
lineage, counts, exclusions, coverage, and the synthetic evidence disclaimer.
Do not scroll through the entire report during the two-minute edit.

Capture these assets:

1. One sanitized `operation: "read"` line from the generated JSONL. Highlight
   `event_time`, `ingest_time`, opaque `volume_id`/`file_id`, range, result,
   and origin.
2. The cleaned manifest lineage and file hashes.
3. The quality summary's exclusion counts and coverage status.
4. A persistent caption: **SYNTHETIC - CONTRACT/PLUMBING EVIDENCE ONLY**.

What this proves:

- deterministic, versioned generated event artifacts;
- schema/reference/range/loss validation;
- cleaning lineage, exclusion accounting, and coverage reporting;
- the production cleaner does not need `truth/`.

What this does not prove:

- a live Chimera collector;
- trusted foreground attribution on a real cluster;
- stable production identity;
- representative demand, prediction quality, cache state, or latency benefit.

### B. Implemented main-branch model clip

Current main implements training but not the snapshot builder. Use the explicit
fake-snapshot command and retain its fixture label:

```bash
# Reuse the same shell as Section A, or restore the captured run root:
export RUN_ROOT=<captured-run-root>
uv run --locked --extra ml pcache-model fake-snapshots \
  --out "$RUN_ROOT/snapshots" \
  --seed 7 \
  --days 5
uv run --locked --extra ml pcache-train \
  --snapshots "$RUN_ROOT/snapshots" \
  --out "$RUN_ROOT/train" \
  --models baseline-mru,baseline-frequency,baseline-hotset,baseline-coaccess,logreg-v1 \
  --no-track
uv run --locked --extra ml pcache-model verify \
  "$RUN_ROOT/train/packages/<chosen-package>.zip"
uv run --locked --extra ml pcache-model inspect \
  "$RUN_ROOT/train/packages/<chosen-package>.zip"
```

Resolve `<chosen-package>.zip` during preproduction from the generated package
list. Do not hardcode a package ID before capture.

Expected `pcache-model fake-snapshots` output:

```text
<RUN_ROOT>/snapshots
```

Expected `pcache-train` output shape:

```text
Trained 5 models from fixture-snapshots-s7 (fixture) -> <RUN_ROOT>/train
  baseline-mru         recall@bytes_256MiB=<value> min_score=<value> package=<package_id>
  baseline-frequency   recall@bytes_256MiB=<value> min_score=<value> package=<package_id>
  baseline-hotset      recall@bytes_256MiB=<value> min_score=<value> package=<package_id>
  baseline-coaccess    recall@bytes_256MiB=<value> min_score=<value> package=<package_id>
  logreg-v1            recall@bytes_256MiB=<value> min_score=<value> package=<package_id>
```

Expected `pcache-model verify` output:

```text
OK <model_type>@<package_id> (<feature_count> features)
```

Expected `pcache-model inspect` output shape:

```text
model_version:   <model_type>@<package_id>
feature_version: fv1
features:        <ordered feature names>
contract_sha256: <sha256>
serving_policy:  <policy dictionary>
data:            <dictionary containing data_kind: fixture>
created_at:      <UTC timestamp>
```

Show package ID, serving contract hash, serving policy, and
`data_kind: fixture`. Do not read the metric values aloud. The visual point is
that a learned package and simple baselines use the same contract and budget
interface, not that one is better.

What this proves:

- baseline and logistic scorers execute against schema-valid fixture snapshots;
- preprocessing and packaging are versioned and load-time verified;
- selection can enforce score ordering and an equal byte budget.

What this does not prove:

- representative features or labels;
- real-data ranking quality, calibration, or superiority over a baseline;
- a live Controller, planner, dispatcher, or Warmer.

### C. Chimera SDK v2 offline evidence clip

Obtain a sanitized recording and evidence bundle directly from the protocol
owner. The clip may show only the already reported offline checks:

```text
CUFS test_file: 46/46 passed
Integrated adapter CTests: 3/3 passed
```

The accompanying caption must remain visible:

```text
OFFLINE / UNCOMMITTED / NOT DEPLOYED
origin=unknown; no stable file/range identity; not a wire or live-cache test
```

Do not reconstruct a terminal to look live. The evidence bundle must include
the exact source snapshot, patch or archive checksum, build container/image,
host, commands, full logs, and reviewer record. If any of those are missing,
replace this clip with the fallback architecture animation.

What this proves:

- the reported snapshots exercised bounded event queue behavior and compatible
  SDK/adapter/CUFS boundaries in offline tests.

What this does not prove:

- the deployed image contains that code;
- an event was emitted for an approved live read;
- client receipt, trusted origin, stable identity, range attribution, or cache
  residency.

### D. Fixture event-to-plan-to-result assets

Use an amber border and the exact label **FIXTURE HANDOFF - NOT LIVE** on every
asset. Keep opaque IDs; never display a real external path.

#### D1. Chimera-v2-style correlation event

The current offline correlation payload cannot identify the target range.
Represent that limitation explicitly:

```json
{
  "schema_version": "read_correlation_v1",
  "operation_id": "op-demo-001",
  "event_kind": "cufs_completion",
  "status": "ok",
  "bytes": 65536,
  "origin": "unknown"
}
```

On-screen annotation:

```text
Identity, offset, and requested length are absent from this schema.
Cannot enter demand training: origin and target identity are not trusted.
```

#### D2. Sanitized collected JSONL identity fixture

Show the contract the live adapter still needs to produce, not a claim that the
v2 event already contains these fields:

```json
{
  "schema_version": 1,
  "event_id": "ev-demo-001",
  "event_time": "2026-11-03T09:00:00.000000Z",
  "ingest_time": "2026-11-03T09:00:00.010000Z",
  "volume_id": "vol-demo",
  "file_id": "file-a",
  "dir_id": "dir-project",
  "operation": "read",
  "offset": 0,
  "bytes_requested": 65536,
  "bytes_read": 65536,
  "file_size": 1048576,
  "result": "ok",
  "origin": "foreground"
}
```

Transition caption:

```text
Required adapter output after authorization, identity, origin, and coverage gates pass.
```

#### D3. Model/planner fixture

Use the chosen package's real model version if available, but keep the score
fictional unless it was generated by a captured reproducible fixture run:

```json
{
  "model_version": "<model_type>@<package_id>",
  "prediction_time": "2026-11-03T09:00:01.000000Z",
  "volume_id": "vol-demo",
  "file_id": "file-b",
  "horizon_seconds": 300,
  "score": 0.82
}
```

Caption `0.82` as **FIXTURE SCORE - NOT A CALIBRATED PROBABILITY**.

Then show the bounded plan:

```json
{
  "schema_version": 1,
  "run_id": "demo-run-001",
  "plan_id": "plan-001",
  "model_version": "<model_type>@<package_id>",
  "deadline": "2026-11-03T09:05:01.000000Z",
  "max_bytes": 65536,
  "items": [{
    "task_id": "task-001",
    "volume_id": "vol-demo",
    "file_id": "file-b",
    "path": "/approved-fixture/project/file-b",
    "offset": 0,
    "length": 65536
  }]
}
```

On-screen proof boundary:

```text
The tested selector can rank under a byte budget.
The planner, identity resolver, and plan sink are not implemented on main.
```

#### D4. Dispatcher -> Warmer handle READ -> result fixture

Animate these states rather than presenting a fake terminal:

```text
plan-001/inbox
  -> claimed/plan-001
  -> revalidate identity + path + bounds + deadline + budget
  -> bounded read-only NFSv4.1 READ attempt
  -> result/task-001.json
```

Show the illustrative result:

```json
{
  "schema_version": 1,
  "run_id": "demo-run-001",
  "plan_id": "plan-001",
  "task_id": "task-001",
  "status": "completed",
  "bytes_requested": 65536,
  "bytes_read": 65536,
  "started_at": "2026-11-03T09:00:02.000000Z",
  "finished_at": "2026-11-03T09:00:03.000000Z"
}
```

Keep this annotation visible:

```text
“completed” means bytes returned to the reader.
It does not prove durable CUFS residency or saved foreground latency.
```

If the dispatcher and Warmer owners provide a reproducible validated run before
recording, replace only this animation with the real clip. Preserve the same
claim boundary and include its run ID and evidence bundle.

## Primary path and component fallbacks

### Primary recording path: transparent hybrid

1. Pre-record validated main-branch datagen, cleaning, fixture training, and
   package verification clips.
2. Include the protocol owner's sanitized offline Chimera SDK v2 test clip only
   if its source, logs, checksums, and review record are available.
3. Use clearly labeled fixture JSON and animation for collection identity,
   planner, dispatcher, Warmer execution, and result unless a validated,
   reproducible integrated clip is delivered.
4. End with the evidence ladder and next controlled experiment. Do not show a
   benefit chart because no matched benefit result is confirmed.

### Fallback matrix

| Failure at capture time | Pre-recorded or visual fallback | Required narration adjustment |
|---|---|---|
| Cluster unavailable or health changed | Use the dated cluster architecture screenshot with “historical snapshot - not current health.” Do not show addresses. | “The cluster path was previously inventoried; current readiness was not used as evidence in this recording.” |
| Live collection unavailable | Use generated JSONL plus the explicit v2 identity-gap fixture. | “Collection remains gated on trusted origin, stable identity, and measured loss.” |
| Chimera evidence bundle incomplete | Remove the terminal clip; use an architecture card listing the reported offline test counts and evidence limitation. | “Offline checks were reported, but the reviewed source bundle was not available for this recording.” |
| Snapshot builder unavailable | Use `pcache-model fake-snapshots` and retain the fixture watermark. | “The trainer runs on a planted fixture; real snapshots remain a separate data-prep milestone.” |
| Model training fails during capture | Use the last verified capture with command, revision, package ID, and checksum. If none exists, show the package contract only. | “The model interface is implemented; this recording does not present a fresh training run.” |
| Planner unavailable | Use the README-aligned plan JSON fixture and tested `ml.metrics.select` budget explanation. | “Selection primitives exist; runtime planning does not.” |
| Dispatcher unavailable | Use the inbox -> claim -> validate -> execute -> result state animation. | “This is the intended idempotent file handoff, not an executed dispatcher.” |
| Warmer unavailable or NFS denied | Use the result fixture with `status: "not_run"` for a stricter fallback, or omit the result entirely. Never show `completed`. | “No live NFS read was executed; the Warmer interoperability gate remains open.” |
| Grafana unavailable or dashboards unverified | Use a static evidence checklist, not a fabricated graph. | “Metrics correlation is required for the experiment but is not shown as current evidence.” |
| No matched experiment | Show the experiment design with all result cells marked “not run.” | “Benefit, safety, and model superiority remain unresolved claims.” |

### Stop conditions for the primary path

Switch to the fallback immediately if any of these occurs:

- current revision or dependency lock differs from the verified capture without
  revalidation;
- a clip cannot be tied to a command, source revision, run ID, log, or checksum;
- real paths, credentials, raw handles, usernames, kubeconfig data, IPs, or
  private access records appear;
- Chimera origin or identity is inferred rather than observed;
- a fixture watermark is cropped, unreadable, or absent;
- the recording would require saying “faster,” “hit,” “saved,” “accurate,”
  “production-ready,” or “end-to-end” without the required evidence;
- the planned narration exceeds 116 seconds in two consecutive rehearsals.

## Evidence and artifact checklist

Create one private recording bundle outside Git. Store only sanitized,
externally appropriate captures in the submission package.

### Required manifest

- [ ] Recording date/time in UTC.
- [ ] Repository URL and exact Git revision.
- [ ] `uv.lock` SHA-256 and Python/`uv` versions.
- [ ] Command transcript with exit codes.
- [ ] Dataset ID, manifest SHA-256, config hash, seed, and `data_kind`.
- [ ] Clean dataset ID, parent manifest hash, output manifest hash, table hashes,
  exclusion counts, and coverage summary.
- [ ] Model type, package ID, package SHA-256, serving-contract hash, feature
  version, and fixture/real provenance.
- [ ] Protocol snapshot/source/archive checksum, build image/container identity,
  test commands, logs, and reviewer record if the Chimera clip is used.
- [ ] Run ID, plan ID, task ID, source/image versions, and result file hash for
  any real dispatcher/Warmer clip.
- [ ] Explicit list of checks not run and evidence gates still open.

### Required visual assets

- [ ] Foreground/background architecture diagram.
- [ ] One sanitized generated JSONL read.
- [ ] Clean manifest lineage and exclusion/coverage summary.
- [ ] Fixture trainer output and model-package inspection.
- [ ] Offline Chimera test evidence card or fallback architecture card.
- [ ] Fixture prediction, plan, dispatcher state flow, and result.
- [ ] Evidence ladder: confirmed, in progress, fixture, unresolved.
- [ ] Next-experiment diagram with control, simple baseline, learned treatment,
  equal byte budget, independent demand, and foreground guardrails.

### Optional metrics assets

Use only if the owning engineer supplies a reproducible, sanitized run:

- Grafana screenshot with dashboard name, time range, run ID, data source, and
  timezone visible;
- event offered/emitted/dropped/export-failure counters;
- warmer requested/returned bytes and task outcomes;
- foreground latency/errors and unrelated workload guardrails;
- validated fault/remote-byte or residency signal;
- cold-state verification and trial-order record.

Never use an unlabeled screenshot as evidence. A dashboard explains a run; it
does not replace raw data.

### Privacy and sanitization

- No credentials, kubeconfig, tokens, cloud secrets, ClearML credentials, raw
  usernames, raw file handles, private hostnames, identifying paths, or real
  access records.
- Prefer opaque `vol-demo`, `file-a`, `file-b`, and `dir-project` identifiers.
- Crop terminal title bars, shell history, home directories, and unrelated
  notifications.
- Do not display the historical cluster IPs in the external recording.
- Retain full private raw evidence only in the approved evidence location with
  an owner and retention decision.
- Verify that subtitles and generated thumbnails do not expose hidden terminal
  text.

## Evidence slide: exact conclusions

Use this wording verbatim or more conservatively:

### Observed facts

- Main implements synthetic event generation, validation, cleaning, exclusion
  accounting, baseline scorers, logistic-regression training, and versioned
  model packages.
- A retained synthetic cleaning acceptance run processed 693,947 deliveries,
  kept 641,973, reconciled exclusions, and marked two known loss-window hours
  incomplete.
- The datagen handoff reported 161 passing tests; the cleaning handoff reported
  266 passing and one skipped test, plus a separate passing opt-in acceptance
  test.
- Offline Chimera/CUFS work reported 46/46 CUFS tests and 3/3 adapter CTests,
  but it is uncommitted, not deployed, and lacks trusted origin and stable
  file/range identity.
- No confirmed matched cold/warm performance run or authorized representative
  real-data model evaluation is available.

### Unresolved claims

- Whether an approved NFS read causes the intended CUFS range to become and
  remain resident.
- Whether a later independent foreground read avoids remote work or becomes
  materially faster.
- Whether speculative reads avoid unacceptable contention, pollution, or
  foreground regression.
- Whether live events provide trustworthy origin, stable identity, complete
  coverage, and safe path resolution.
- Whether a frozen learned model beats MRU, frequency, or static hot-set
  baselines on authorized real data at the same byte budget.

### Next experiment

Run balanced matched trials with:

1. verified equivalent cold states;
2. no-warmer control, simple baseline, and learned plan at equal issuance
   budgets;
3. frozen dataset/model/policy and predeclared stopping rules;
4. bounded read-only NFS execution and origin exclusion;
5. later independent foreground demand;
6. fault/remote-byte, latency, foreground, and pollution evidence;
7. one run ID across plan, result, telemetry, raw trial data, and analysis;
8. independent reproduction before any success claim.

## Rehearsal and production checklist

### Capture settings

- [ ] Record 16:9 at 1920x1080 or 2560x1440, 30 fps.
- [ ] Use a 20-24 px terminal font, high contrast, no transparency, and no
  animated cursor effects.
- [ ] Record voice at 48 kHz; normalize speech consistently and avoid music
  under technical claims.
- [ ] Burn in concise captions for `IMPLEMENTED`, `FIXTURE-VALIDATED`,
  `OFFLINE`, `HISTORICAL`, `FIXTURE`, and `UNVERIFIED`.
- [ ] Keep every terminal clip readable for at least 2 seconds after the
  relevant output appears.
- [ ] Use hard cuts or short dissolves; avoid decorative transitions that
  consume the timing buffer.
- [ ] Add subtitles and check technical identifiers manually.

### Timing gates

- [ ] Exact narration: 240 words, approximately 93-99 seconds at 145-155 wpm.
- [ ] Marked silent reading beats: 11-12 seconds; no improvised narration.
- [ ] Total non-narrated time, including natural transitions and holds: 17-23
  seconds.
- [ ] Full storyboard playback, including silent beats: 110-116 seconds.
- [ ] First rough cut: at most 118 seconds.
- [ ] Final content: 116 seconds target; final file never exceeds 120 seconds
  including the four-second end buffer.
- [ ] Problem and architecture complete by `00:36`.
- [ ] Example run complete by `01:14`.
- [ ] Evidence limitations begin no later than `01:14`.
- [ ] Final experiment ask begins at `01:39`.
- [ ] No new narration after `01:56`.

If over time, cut visual detail in this order:

1. package-inspection field list;
2. individual exclusion categories;
3. protocol build detail;
4. animation labels already covered by narration.

Never cut the fixture labels, evidence limitations, or next-experiment gate.

### Narrative clarity checks

- [ ] A non-specialist can explain the foreground/background split after one
  viewing.
- [ ] “Prediction” is clearly separated from “planning,” “execution,” and
  “benefit.”
- [ ] The event's missing trusted origin/identity is visible, not hidden.
- [ ] A completed Warmer result is explicitly separated from usefulness and
  incremental benefit.
- [ ] Synthetic/fixture evidence is never placed on the same visual scale as a
  real performance result.
- [ ] The ending asks for one controlled experiment, not broad production
  adoption.

## Ownership and dependencies

| Deliverable | Accountable owner | Dependency before inclusion |
|---|---|---|
| Final narrative, timing, edit, and claim audit | Demo lead / technical lead | This plan, current status review, and final go/no-go. |
| Datagen capture and manifest | Data owner | Clean host, frozen main revision, locked environment, sanitized fixture. |
| Cleaning capture and quality report | Data-prep owner | Generated dataset, manifest/hash reconciliation, coverage report. |
| Model/package capture | ML owner | Locked environment with the `ml` extra, fixture snapshot manifest, package IDs/hashes, no unqualified metric claim. |
| Evaluation evidence | ML/evaluation owner | PR merged or exact PR revision and fixture disclaimer; not required for the primary recording. |
| Chimera SDK v2 clip | Protocol owner | Frozen source bundle, checksums, logs, exact commands, independent review, sanitation. |
| Collection identity statement | Protocol + data owners | Field-availability report and explicit blockers for origin/identity/coverage. |
| Planner fixture | Controller/ML owner | Package version, bounded selector evidence, fixture watermark. |
| Dispatcher fixture or real clip | Dispatcher owner | Exact revision, fake/real mode, plan/result hashes, stop/budget evidence. |
| Warmer fixture or real clip | Warmer owner | Exact revision, approved target, bounded settings, output, and explicit residency limitation. |
| Cluster/Grafana evidence | Cluster + observability owners | Fresh health check, authorization, sanitized run-correlated evidence. |
| Experiment design and result interpretation | Performance owner + independent reproducer | Equivalent cold-state procedure, frozen treatments, raw evidence, safety metrics. |

The demo lead owns omission: if an owner cannot provide complete evidence, use
the specified fallback rather than inferring readiness.

## Go/no-go checklist: 30 minutes before recording

### GO for the transparent hybrid

- [ ] `plan.md` full storyboard plays in 110-116 seconds, with narration and
  marked silent beats both preserved.
- [ ] Every clip has revision/run/checksum provenance.
- [ ] Main-branch commands were captured successfully on an approved host.
- [ ] Fixture and offline badges remain visible at normal playback size.
- [ ] No sensitive data appears in video, audio, subtitles, or thumbnails.
- [ ] Every spoken status matches the final evidence matrix.
- [ ] The final slide states that cache benefit and real-data model quality are
  unverified.
- [ ] A backup render and all source assets exist.

### Additional GO conditions for any live or prerecorded integrated claim

- [ ] Fresh cluster health and target authorization were rechecked.
- [ ] Source/image versions are mapped and recorded.
- [ ] Event origin, identity, range, offered/emitted/dropped counts, and run
  correlation are observed rather than inferred.
- [ ] Plan, dispatcher, and Warmer revisions and result hashes are preserved.
- [ ] The path/range is a known approved fixture; no real user data is shown.
- [ ] A completed read is described only as execution unless cold-state,
  residency, later demand, and matched control evidence also exist.
- [ ] Foreground and pollution guardrails are present before any benefit claim.

### NO-GO for a “live end-to-end benefit” narrative

Declare no-go and retain the hybrid if any of these is missing:

- equivalent verified cold state;
- trustworthy origin and stable target identity;
- approved live source and fixture;
- matched no-warmer control;
- bounded execution and stop behavior;
- later independent foreground read;
- run-correlated raw latency/fault/remote-byte and safety evidence;
- an independently reviewable evidence bundle.

## Recommended recording order

Record in dependency order, not final playback order:

1. freeze revision, scripts, fixture IDs, and narration;
2. capture datagen generation/validation/description;
3. capture cleaning and quality/lineage evidence;
4. capture fake snapshots, training, and package verification;
5. obtain and verify the protocol owner's offline evidence clip;
6. capture any validated dispatcher/Warmer clip, otherwise render fixtures;
7. render architecture, evidence ladder, and next-experiment frames;
8. record voiceover against the locked storyboard;
9. assemble the 116-second cut, add subtitles and status badges;
10. perform technical claim review, privacy review, and one silent visual review;
11. export the final and backup files; preserve the private evidence manifest.

## Optional audience variants

Do not create separate recordings. Use the same two-minute arc.

- **Executive emphasis:** problem, foreground independence, bounded read-only
  mechanism, honest evidence boundary, and the single next experiment.
- **Engineering emphasis:** event origin/identity/coverage, package and plan
  versioning, byte budgets, failure isolation, cold-state equivalence, and
  run-correlated evidence.

The default script already balances both: the visual layer carries engineering
precision while the spoken arc stays decision-oriented.

## Self-review record

### Independent review dispositions

The first frozen draft received an independent read-only review. Material
findings were resolved as follows:

| Finding | Disposition |
|---|---|
| Logistic training omitted the optional `ml` dependency. | Fixed: capture setup now uses `uv sync --locked --extra ml`, and ownership records the dependency. |
| The plan treated 116 seconds of screen time as 116 seconds of speech. | Fixed: the exact 240-word script is separated from 11-12 seconds of marked silent reading beats and additional visual holds. |
| Fixture-validated and historical status labels were undefined. | Fixed: both labels are defined and required in burn-in captions. |
| Expected CLI output was incomplete or grouped under the wrong command. | Fixed: output shapes are separated for fake snapshots, training, verification, inspection, and cropped describe reports. |
| The run-root shell variable, correlation payload, and fixture score were ambiguous. | Fixed: the run root is restored explicitly, absent correlation fields are not fabricated as null, and the score is numeric with a fixture warning. |
| Move the deliverable under `docs/` to follow the normal repository convention. | Not applied: the user explicitly required the primary deliverable to be named `plan.md` in this isolated worktree/session. It remains uncommitted. |

### Narrative clarity

- The story follows status quo -> shift -> mechanism -> evidence -> next
  experiment.
- One example is traceable across event, model package, plan, execution state,
  and result.
- The strongest limitation appears during the example, not only in a final
  disclaimer.

### Technical accuracy

- Current main capabilities are separated from PR, uncommitted, fixture, and
  proposed work.
- The Chimera SDK v2 evidence is described as offline and identity-incomplete.
- A completed NFS read is not treated as residency, usefulness, or benefit.
- Synthetic cleaning counts are labeled as synthetic contract evidence.
- The model metric values are not used as quality claims.
- Foreground independence, origin exclusion, temporal validity, and predictive
  state isolation remain explicit invariants.

### Two-minute feasibility

- The exact script is 240 words, approximately 93-99 seconds of speech, inside
  a 116-second timeline with explicit silent reading beats and visual holds.
- The four-second buffer is reserved for edit variance.
- Five frames stay within the requested 4-6 storyboard range.
- The live example is reduced to readable pre-recorded clips and fixture
  animation; no terminal typing or dashboard exploration competes with the
  narrative.

### Review disposition

The recommended plan is **GO for a transparent hybrid** and **NO-GO for a live
end-to-end benefit claim** until the controlled collection, mechanism, safety,
and matched-experiment gates are supported by reproducible evidence.
