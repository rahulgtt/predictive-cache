# Engineer 1 implementation plan: CUFS access data and cache warming

**Status:** Proposed; feasibility gates and deployed versions are unverified.  
**Owner:** Engineer 1 — protocol/filesystem.  
**Collaborators:** Engineer 2 (ML), Engineer 3 (data/telemetry), Engineer 4 (performance).  
**Target:** CUFS-backed Chimera on the Kind cluster `nasuni-cluster` on `nas-vm`. Use its explicitly resolved kubeconfig, not the host's default context. For this project, do not access `strider-dev` or `strider-dev2`; run builds and tests on `nas-vm` in the respective Chimera or CUFS build container image.  
**Outcome:** Deliver a verified deployed read-path map and trustworthy event-field contract; prove or disprove that a bounded NFS read materializes cold CUFS data for later foreground reads. Implement a minimal event source and warming adapter only after their prerequisites are verified.

## 1. Scope and success criteria

### In scope

1. Identify the running cluster, volume, Chimera/CUFS image and corresponding source revisions; map NFS and SMB reads through CUFS to the cold-chunk fault and retry path.
2. Inventory existing events, tracing, audit logs, fault and residency signals; establish which training fields are available, where, and with what semantics.
3. Agree on a versioned access-event and origin contract with the ML and telemetry engineers. Identify the minimum safe CUFS hook only if existing telemetry is inadequate.
4. With the performance engineer, establish an approved cold-data fixture and a non-warming observation method. Compare a cold control with bounded NFS warming followed by a foreground read.
5. If required and approved, implement disabled-by-default, bounded, best-effort access-event capture and a separately constrained read-only NFS warming adapter. Coordinate transport and Controller ownership with the team.
6. Hand off reproducible evidence, source references, known gaps, and integration contracts.

### Out of scope

- Training/model selection, a full data pipeline, Grafana dashboards, and statistical performance claims: owned by other engineers.
- A generic Chimera VFS warming API, CUFS volume-Kvrocks schema changes, eviction-policy changes, and treating memfs/diskfs as production cache targets.
- A direct CUFS prefetch API for MVP unless the NFS feasibility gate fails and the team agrees to redesign.
- Cluster provisioning, restarts, resets, eviction, fault injection, or deployment changes on shared resources without cluster-owner approval. Use disposable fixtures and approved test infrastructure.

**Definition of done:** the evidence package in §10 is complete; each gate is marked pass, fail, or blocked. A passing warm-mechanism gate requires evidence of the actual CUFS state/fault transition and a subsequent real NFS read; latency alone is insufficient. Report the experiment's observed result even if the hypothesis fails.

## 2. Current code evidence — revalidate against deployment

Local read-only snapshots inspected on 2026-11-03: CUFS `07b78387`; Chimera `c6b02210`. They may differ from the deployed image and can change. Cite repository paths and actual revisions in the final report.

```text
NFS READ -> Chimera NFS read handler -> chimera_vfs_read[_owned]
         -> VFS dispatch/lease mediation -> CUFS cufs_op_read
         -> cufs_ll_read -> File::read -> Dkv::readData
         -> if stubbed: faultChunks -> retry readData -> result
```

- Chimera NFSv3 handler passes credentials, offset and count to `chimera_vfs_read`: `chimera/src/server/nfs/nfs3_proc_read.c:50-85`. NFSv4 and SMB handlers also submit VFS reads: `chimera/src/server/nfs/nfs4_proc_read.c`, `chimera/src/server/smb/smb_proc_read.c`. Verify the protocol/version actually used by the experiment.
- CUFS `cufs_op_read` obtains mount ID, inode, credentials, offset and length, calls `cufs_ll_read`, and handles returned bytes: `cufs/src/vfs_cufs/cufs.c:1366-1465`.
- `File::read` breaks requests into bounded windows; each invokes `Dkv::readData`. Stubbed chunks trigger `faultChunks` and a retry; failures return errors; EOF has a distinct success return: `cufs/src/fsal_cufs/file_impl.cpp:391-478`.
- `ChunkAtimeSet` demonstrates bounded, best-effort bookkeeping with deduplication and drop accounting: `cufs/src/fsal_cufs/chunk_atime_set.cpp:18-83`. It takes a mutex and is intended for atime, not a proven event stream or a lock-free/nonblocking publisher.
- Existing mock-level read/fault tests: `cufs/src/tests/fsal_cufs/test_file_read.cpp`; component-level multi-range faulting example: `cufs/tests/component/faulting/test_faulting_multi_range.py`. These demonstrate useful test seams, not live NFS cache benefit.
- Volume-context setup associates a mount ID and volume GUID: `cufs/src/fsal_cufs/init.cpp:24-72`, `cufs/src/fsal_cufs/volume_context.cpp`. Their stability and visibility at any proposed hook must be confirmed.

**Unknowns:** deployed image/source equivalence; NFS and SMB server/client caches; chunk residency lifetime and eviction; reliable user/session/origin attribution; inode generation/reuse semantics; authorized historical trace; path-to-identity validation; transport and publisher lifecycle.

## 3. Work breakdown and dependencies

| ID | Task | Deliverable | Dependency / exit check |
|---|---|---|---|
| P0 | Locate target via `cluster-manager` using its actual backend; read-only inventory of node, volume, image digest, services and relevant metrics. Ask cluster owner before any modifying experiment. | Environment manifest, permissions and image/source mapping. | Use `nasuni-cluster` on `nas-vm`; recheck live state before experiments. |
| P1 | Trace actual NFS read end to end; map SMB equivalent where relevant. Include offset, length, credentials, mount/volume, inode, actual bytes, error and EOF. | Annotated request/response diagram with code revision and precise call sites. | P0 for deployed equivalence; code-only map can start immediately. |
| P2 | Trace `readData` stub response, chunk ranges, `faultChunks` success/failure/retry, and where the restored data lives and can later be evicted. | State-transition diagram and fault evidence table. | P1; distinguish source observations from runtime assumptions. |
| P3 | Inventory existing audit/OTel/log events and data quality; compare each proposed field to actual producer, coverage and loss visibility. | Field matrix with available / unavailable / unknown and owner. | P1–P2; hand off to ML + telemetry. |
| P4 | Define controlled test fixture, approved reset/seed, cache-observation method and read client. | Written experiment protocol and safe cleanup procedure. | P0, P2; cluster owner + performance engineer approve. |
| P5 | Execute cold-control vs warm-then-foreground pilot, using equivalent independent fixtures or verified reset. | Raw run records, fault/residency evidence, latency and checksums; pass/fail/inconclusive. | P4; no destructive or shared-state changes without approval. |
| P6 | Publish event/origin contract: identity, request granularity, result and fault semantics, ordering, privacy, drop accounting. | Versioned contract and fixtures agreed with Engineers 2 and 3. | P3; unknown fields remain explicitly unknown. |
| P7 | If P3 proves a gap, design and implement a feature-flagged CUFS hook with bounded asynchronous export. | Targeted CUFS change, tests and documented loss/ordering behavior. | P6; separate coding worktree/session; approved destination and origin design. |
| P8 | If P5 passes, help implement a bounded, read-only NFS adapter in the standalone predictive-cache repository. | Adapter contract/implementation, results and safety controls. | P5, path/authorization validation, controller contract. |
| P9 | Integrate with telemetry, ML and performance owners; verify origin filtering and fault/error isolation. | Joint acceptance record and reproducible handoff. | P6–P8 as applicable. |

## 4. Event source: design before implementation

### Proposed field matrix

| Field | Candidate source | Evidence status / required decision |
|---|---|---|
| Volume identity | Mount ID in `cufs_op_read`; volume GUID in volume context | Mapping, lifetime, privacy and cross-mount stability require verification. |
| File identity | `handle->vfs_private` / inode | Generation, reuse and rename semantics are **not verified**. Do not promise `inode + generation` exists. |
| Requested offset/length | `request->read.offset` / `length` | Available at VFS adapter in inspected source. |
| Actual returned bytes and success | `cufs_ll_read` result / `bytes_read`; `File::read` returns success at EOF and normal completion | Define one event per client-visible request, including short reads; avoid double counting internal windows and retries. |
| Stubbed ranges and fault outcome | `result->hasStubbedChunks()`, `faultChunks` result in `File::read` | Range-level facts are internal to per-window retry; define how to aggregate and represent unknown/mixed state. |
| Origin | Protocol/credential/explicit request metadata, if propagated | Unverified. Unknown origin must not become foreground demand. A username or time-window suppression alone is not proof. |
| Path | Not established at `File::read` | Do not derive from inode or persist raw paths without a validated, authorized mapping. ML uses opaque identity; warmer needs an independently validated current target. |
| Event time, session/cohort | Clock and request context, if available | Verify timestamp semantics and approved identity/sessionization; do not invent values. |

**Hook recommendation, conditional:** use `cufs_op_read`/`cufs_ll_read` completion for a single client-visible result and capture range/fault observations at `File::read` only if a narrow, properly typed per-request summary can propagate safely across that boundary. Compare this with an existing producer first. Hooking solely after each `readData` can overcount multiple windows/retries and cannot by itself prove a client-visible success; `File::read` alone does not establish protocol origin or current path. A single hook may not carry all required facts.

### Event semantics to agree before coding

- Include schema version, source/run ID, event ID, event timestamp, opaque volume/file identity, request offset, requested/actual bytes, terminal outcome, origin, fault state and loss indicators. Omit or mark unavailable fields explicitly; never fabricate generation, cohort or path.
- Distinguish `faulted`, `no_fault_observed`, `unknown` and failures; a successful fault is not proof of persistent residency or latency improvement. Define partial/EOF and sparse-file behavior.
- Exclude warmer, replay and administrative origins from demand; treat unknown origin as unknown, not foreground. Confirm how origin survives SMB/NFS to CUFS; if not possible, block live training rather than infer identity.
- Record event offered/emitted/coalesced/dropped and exporter failures; document at-most/at-least-once delivery, duplication and ordering. One missing read is not a negative label when capture is incomplete.
- Avoid content, raw usernames and credentials. Access-controlled path catalog, identity mapping and retention require data-owner approval.

### Safety invariants

Capture defaults off. No synchronous network or storage calls on the foreground path; use bounded enqueue and independent worker lifecycle. Ensure queue contention, allocation, shutdown and failure do not change read outcome or introduce unbounded waiting. `ChunkAtimeSet` is a bounded precedent, not evidence that its locking scheme meets this stronger invariant. Saturation must drop/coalesce with counters; publish failures must be visible. Never publish to CUFS volume Kvrocks.

## 5. Warm-path feasibility protocol

**Purpose:** test that an NFS read results in durable materialization of the intended CUFS range and benefits a later foreground read. This pilot establishes mechanism, not model effectiveness.

1. **Approve environment:** record exact cluster/namespace/volume/export, workload owner, active replicas, image digests, source version, client host and NFS version. Confirm no shared production data or unapproved cache reset.
2. **Prepare equivalent data:** use the existing approved cold/stub fixture or separate equivalent disposable files per treatment. Record size, file identity, checksum and selected range. Establish that fixtures start cold through a validated, non-warming probe. Do not create a “cold” fixture merely by restarting the client.
3. **Cold control:** perform the same defined foreground NFS read with no warm step. Capture monotonic start/end, returned bytes/checksum, service/node routing, fault/stub/remote-byte counters and pre/post state.
4. **Treatment:** restore an equivalent cold state or use a matched independent cold fixture. Perform a bounded read of the same range as the proposed Warmer using an authorized read-only identity. Record actual bytes and errors, and verify any stub-to-materialized transition using the approved probe.
5. **Later foreground read:** after a declared delay, use an independent client/process/cache where feasible, and repeat the same read. Capture the same evidence and verify data/checksum. Control client page cache, NFS attribute/data caching, OS cache and server/node routing; record what cannot be controlled.
6. **Compare:** report cold vs warm latency, actual fault/remote-byte deltas and residency evidence. Repeat matched trials to check basic consistency. Engineer 4 owns the later randomized trial count and statistical analysis. Report failures, retention/eviction and confounders, not only favorable runs.
7. **Origin check:** prove the Warmer operation is classified as non-foreground at the actual collection point. If attribution cannot be proven, do not feed such data to the model.
8. **Cleanup:** use only the approved teardown for the specific disposable fixtures. Leave shared volumes, caches, workloads and routes unchanged unless explicitly approved.

**Decision:** PASS if confirmed cold data materializes and the later read observes the intended CUFS effect in repeatable matched pilots with correct data; FAIL if evidence shows the mechanism does not work; INCONCLUSIVE if residency, cold equivalence or client caching is unobservable. Any larger latency threshold is a proposed team benchmark target, not a Day 1 fact.

## 6. Implementation phases

### Phase A — Investigation and contracts

- Complete P0–P4 and publish the field matrix, sequence/state diagrams and approved fixture protocol.
- Ask Engineer 2 to freeze prediction unit and label horizon; ask Engineer 3 to identify existing collectors and delivery guarantees; ask Engineer 4 to specify measurement units and client-cache controls.
- Gate: no code-change decision until actual event gaps and warm mechanism are known. Fixtures may unblock other lanes but are not model-quality or performance evidence.

### Phase B — CUFS telemetry, conditional

- If existing structured events satisfy the contract, reuse them and do not add a hook.
- Otherwise create a small disabled-by-default CUFS feature in an isolated CUFS coding session. Define which request layer owns the event, how per-window fault state is aggregated, and how mount/volume/origin are passed without ABI or lifetime hazards.
- Use existing configuration, lifecycle and telemetry conventions after verifying current code. Add a bounded producer and separately owned exporter; never add a network dependency to read success.
- Define overflow, coalescing, shutdown, retries and metrics. Keep origin unknown when source metadata is missing.
- Gate: targeted unit/integration tests cover §§7–8; team validates event samples and loss reconciliation against known reads.

### Phase C — Warmer adapter, conditional

- In the predictive-cache repository, create the read-only NFS range executor behind the separately deployed Warmer, using the team's selected client/library after verifying auth and runtime feasibility. No privileged mounts assumed.
- Validate identity and authorized target near execution, byte bounds, deadline, cancellation, short reads and EOF. Bound concurrency, read chunk size, total bytes, rate and in-flight work. Report partial/error results and expose pressure/pause behavior.
- Gate: deterministic adapter tests and a live read through the verified CUFS-backed export; no claim that a completed NFS read implies residency.

### Phase D — Integration and evidence

- Correlate test run, task and events without recording sensitive paths in metrics. Verify Warmer origin exclusion at every training/evaluation input.
- Deliver warm pilot and event samples to Engineers 2–4. Engineer 4 independently runs controlled performance evaluation and determines whether the model improves over baselines under a byte budget.
- If NFS warming fails, stop and document evidence; propose a separate design for a direct, explicitly low-priority CUFS fault API rather than silently changing the MVP.

## 7. Test plan and what each level proves

| Level | Cases | Proves / does not prove |
|---|---|---|
| CUFS unit | hot success; cold fault + retry; failed fault; repeated stub error; EOF; short read; zero-length; multi-window; sparse ranges | Correct aggregation, outcome and event cardinality in mocks; not real cache warming. Extend existing `test_file_read.cpp` patterns. |
| Event failure/pressure | feature disabled; full queue; allocation pressure; exporter down; shutdown while busy; duplicate/out-of-order delivery; unknown and warmer origin | Read results remain unchanged; loss is observable; demand filters are correct. Verify latency impact under load separately. |
| Warmer unit | read-only open; invalid range; short read/EOF; deadline, cancellation, retry, duplicate lease; stale path/identity; max bytes and rate | Adapter enforces contract; not durable residency. |
| Existing CI-integrated integration | CUFS-backed NFS; authorized identity; faulted range; warm-origin propagation; saturated exporter | Cross-component behavior in real paths, without relying on tests that routinely skip in CI. |
| Live mechanism pilot | approved matched cold fixtures, bounded warm and later independent foreground read | Confirms or rejects mechanism for this deployment. Not real-world predictor quality. |
| Performance | randomized or balanced matched trials with unrelated foreground load and pollution checks | Engineer 4 measures effect and uncertainty; thresholds/counts require agreement before final analysis. |

Use `nas-vm` for builds and tests, in the respective CUFS or Chimera build container images; initialize submodules before testing. Do not access `strider-dev` or `strider-dev2` for this project. Discover exact targeted commands from current manifests/CI; do not assume local checkout commands or run builds/tests in the user's primary checkouts. Provision Strider clusters with Kind only unless explicitly requested otherwise.

## 8. Suggested execution schedule — all engineers start together

| When | Engineer 1 | Parallel handoff / gate |
|---|---|---|
| Day 1 | P0–P3: deployed read trace, field availability, candidate hook; draft P4 fixture protocol. | ML and telemetry receive initial schema with unknowns; performance starts probe and trial design. |
| Day 2 | Approve and execute P4–P5; finalize P6 event/origin contract. | Warm mechanism decision and event gaps; do not label fixtures representative. |
| Days 3–4 / spillover | P7 if required; P8 if feasible; targeted tests and failure isolation. | Integrate schema, origin, batch/result contract and run IDs with others. |
| Day 5 / spillover | P9; support controlled trials, document blockers and evidence. | Performance lead owns comparative analysis and claims; team reviews no-go conditions. |

These are planning estimates, not evidence of completion. The session's original November 4 date may not allow the full work; do not waive feasibility, safety or reproducibility gates to meet a date.

## 9. Risks, stop conditions and decisions

| Risk / trigger | Response | Owner |
|---|---|---|
| Deployed code/version differs from inspected snapshots | Re-trace the deployed revision; label mismatched findings. | Protocol/cluster owner |
| Existing telemetry lacks identity, fault or proven origin | Define minimal bounded hook or report blocked; no invented fields. | Protocol + telemetry |
| No representative history | Gather authorized history; synthetic data only for plumbing. | Data + ML |
| Reset/residency not verifiable or client caching dominates | Treat pilot as inconclusive; improve fixture/probe. Do not claim benefit. | Protocol + performance |
| NFS cannot persistently materialize desired range | Stop NFS approach; propose direct CUFS API for team decision. | Protocol + technical lead |
| Queue loss is biased or not measurable | Expose/drop counters, quantify coverage, restrict claims. | Protocol + data |
| Warmer origin indistinguishable | Exclude unknown events and stop live training on contaminated data. | Protocol + ML |
| Warming affects foreground traffic or shared data | Pause, reduce limits, preserve evidence and notify owner; do not proceed on a shared cluster. | Protocol + performance |
| Identity can be reused or path is stale | Validate identity/generation if available; fail closed or classify as unverified. | Protocol + security/data owner |

## 10. Final handoff

Deliver in an approved shared location:

1. Cluster/volume/export and deployed image/source revision manifest with observation timestamp and owners.
2. NFS and SMB read/fault sequence diagrams and a per-field source/availability table.
3. Versioned event contract, sample fixture events, unknown fields, privacy rules, origin proof and loss/ordering semantics.
4. Approved cold-fixture/reset/probe runbook and independent control/treatment records: requested/actual ranges, checksums, timestamps, fault/residency evidence, errors, client-cache controls and raw measurements.
5. Conditional CUFS hook and Warmer adapter change maps, targeted test cases, remote verification evidence and rollout/rollback controls.
6. Explicit mechanism verdict: pass, fail or inconclusive; blockers and named owner for every unresolved gate.
7. Handoff to ML (event semantics/quality), telemetry (producer, counters, run correlation), and performance (fixture/probe and raw evidence).

**No optimization claim is authorized by this plan:** only controlled, independently reproducible, representative performance results can support one.

## 11. P0 inventory: nasuni-cluster on nas-vm

**Observed 2026-11-03; inventory only.** `cluster-manager list --backend kind` and `resolve` identified `nasuni-cluster` on `nas-vm`. Its target-specific kubeconfig showed three Ready Kind nodes, Kubernetes v1.35.1, and three Ready `nasuni/chimera-cufs` pods. `kind get clusters` under the login user's default provider did not list it; use `cluster-manager` to locate the target.

- All three Chimera/CUFS pods report image `registry.strider.nasuni.dev/chimera-cufs:0.1.0-26` and image ID `sha256:2ac4b9977f5e9f0810707dfd9fa9c5e980dc40729b05cfb97ae1008e64bbb27d`. **The corresponding Chimera and CUFS source revisions have not been established.**
- `nasuni/vol-azure-c1b4a8bb` reported `Ready`, `Active`, `Healthy`, with container `vol-azure-c1b4a8bb-guid`. Volume Kvrocks and session Kvrocks pods, OTel collectors and Grafana were running.
- Follow-up inventory found an appliance configuration and an enabled cloud credential referencing the Azure provider; the appliance manager was `2/2` Ready. Actual object-store connectivity was not independently verified.
- `nasuni/chimera-load-balancer` advertised TCP ports 2049 and 445 at `10.89.0.200`. No Kubernetes `NfsExport` or `SmbShare` custom resources were found. This does **not** prove that no export exists through another configuration path.
- The default `kubectl` context on `nas-vm` points to a different RKE2 cluster. Always resolve the intended cluster and use its explicit kubeconfig; never assume the default context targets `nasuni-cluster`.
- The manager's resolve/kubeconfig commands generated temporary kubeconfigs on the host. Both created files were removed after inventory. No cluster resources were changed.

**P0 remains open:** identify the actual NFS export and approved test fixture/owner, map the deployed image digest to both source revisions, and confirm permissions before live reads or modifications. Pod readiness and reported volume health alone do not prove that the NFS data path or warming works.

## 12. P1 read-path investigation

**Status: source-level trace complete; deployed-path verification remains open.** Inspected Chimera `c6b0221067000ca8c24013b8b0bc856b66719b38` and CUFS `07b783872e8b255a6678d228227d96e143140143`; the inspected files had no local modifications. The deployed image's inspected OCI labels identify its Rocky Linux base, not its Chimera or CUFS source revision. Do not equate these local snapshots with the running image until their build provenance is established.

### Mental model

The protocol server translates a network READ into a VFS request. VFS enforces the applicable access rules and coordinates cache claims before dispatching to the CUFS adapter. CUFS uses the selected volume's DKV connection to read the requested range. If DKV reports cold, stubbed chunks, CUFS asks the faulting service to materialize them and retries the read. The result flows back through VFS and is translated into a protocol response.

```text
NFSv3 READ(file handle, offset, count, credentials)
  -> decode handle / open for reading / cap oversized count
  -> chimera_vfs_read
  -> VFS access gate where applicable; allocate buffers; acquire I/O claim
  -> CHIMERA_VFS_OP_READ -> cufs_chimera_dispatch -> cufs_op_read
  -> cufs_ll_read(mount_id, inode, offset, length)
  -> volume context -> File::read -> Dkv::readData
       -> cold/stubbed chunks? faultChunks -> retry readData
  <- bytes or error -> CUFS adapter -> VFS completion -> NFS response
```

An application-level `read()` is not necessarily one server-side READ: client caching, readahead and request splitting can alter or eliminate RPCs. The diagram describes one representative server request, not proof of the actual traffic generated by a particular client.

### Layers and evidence

| Layer | Responsibility and observations in inspected source | Source |
|---|---|---|
| NFSv3 | Maps RPC credentials, decodes the file handle, caps counts above the NFSv3 maximum, opens the handle and submits offset/count to `chimera_vfs_read`. Its completion maps VFS errors to NFS status and sends count/EOF on success. A decode/open failure can occur before CUFS is called. | `chimera/src/server/nfs/nfs3_proc_read.c:15-142` |
| NFSv4 | Submits reads through `chimera_vfs_read` or `chimera_vfs_read_owned`, depending on state/delegation handling, and maps the VFS result to NFSv4 status. The actual client protocol version on this cluster is not yet established. | `chimera/src/server/nfs/nfs4_proc_read.c:15-111` |
| VFS | Applies an access gate where required, allocates a READ request and buffers, copies offset/length and optional ownership metadata, and mediates an I/O claim before dispatch. Completion finalizes buffers and calls the protocol callback. The gate can reject a read before CUFS sees it. | `chimera/src/vfs/vfs_proc_read.c:103-264, 282-469` |
| CUFS adapter | Dispatches `CHIMERA_VFS_OP_READ` to `cufs_op_read`. It derives mount ID and inode from the handle, maps credentials, reads the requested offset/length, invokes `cufs_ll_read` for nonzero reads, then copies returned data to VFS buffers and sets the result. | `cufs/src/vfs_cufs/cufs.c:1366-1499, 1992-2029` |
| Low-level CUFS | Selects a volume context by mount ID, constructs `File` with the volume's DKV and faulting client and returns `File::read`'s result. The mount setup maps a mount ID to a volume GUID, but stable identity and propagation at an event hook still need validation. | `cufs/src/fsal_cufs/file.cpp:158-174`; `cufs/src/fsal_cufs/init.cpp:24-71` |
| File read and fault | Processes the logical read in bounded windows. `readData` returns data or stub information. For stubs, `faultChunks` runs once per window before `readData` is retried. Fault failures or a repeated stub produce an error; EOF and normal completion have separate success returns. | `cufs/src/fsal_cufs/file_impl.cpp:391-479` |
| SMB | Ordinary file reads also submit through VFS, but named-pipe reads take a different path. SMB may turn a successful VFS read into `END_OF_FILE`, for example when fewer bytes than its `MinimumCount` were returned. A CUFS success is therefore not always the final protocol result. | `chimera/src/server/smb/smb_proc_read.c:49-98, 208-220, 319-330` |

### Implications for training events

1. **Identity:** the CUFS adapter has mount ID and inode; `cufs_ll_read` can resolve a volume context. Stable volume identity, inode generation/reuse and an authorized path mapping remain unverified. Do not treat inode alone as globally durable identity.
2. **Range:** the adapter sees the VFS offset and length. For NFSv3, this count can already be capped relative to the original wire request. Internal `File::read` windows and retries are not separate user-demand events.
3. **Outcome:** record the precise layer at which success is observed. `cufs_op_read` can observe a CUFS/VFS completion, but protocol handlers can reject before CUFS or map the result differently afterward. Do not call it a confirmed client-visible success without a verified correlation to the final protocol result.
4. **Fault:** `File::read` sees stubbed chunks and fault responses, but the per-window `fault_attempted` flag resets on each window. A useful logical-read summary must aggregate across all windows and preserve unknown/failure states.
5. **Attribution:** credentials and, for some NFSv4/SMB operations, I/O ownership are present at higher layers; neither has yet been proven to distinguish the Warmer from demand at CUFS. Unknown origin stays excluded from training.
6. **Data completeness:** a CUFS-only hook will miss requests rejected earlier, protocol reads served elsewhere, and application reads served by client cache. That may be acceptable for a clearly defined server-side successful-read dataset, but it is not complete user-access history.

### P1 handoff and next verification

- **Candidate split:** collect the terminal CUFS request result at the adapter and aggregate fault metadata from the file-read path only if safe per-request propagation and final event semantics are designed; first check existing telemetry. Do not implement this hook merely because it is a plausible seam.
- **Next:** map image digest to source builds, identify the actual configured export and client protocol, and inspect live telemetry without initiating unapproved reads or modifying the cluster. P1 is not confirmed for the deployment until those checks establish the source and actual route.

## 13. Deployment provenance and export follow-up

**Observed 2026-11-03; read-only investigation.** Both source revisions remain unverified.

- The running `chimera-cufs:0.1.0-26` image has digest `sha256:2ac4b9977f5e9f0810707dfd9fa9c5e980dc40729b05cfb97ae1008e64bbb27d`. Its init container uses `cufs-base:0.2.0-48`, digest `sha256:2fa381a6d783d0e738bab943ab23621188979780cca4e817d4243f645918ff7c`. The running container contains CUFS RPM `0.2.0-48.el10`, built 2026-10-07.
- Deployment commit `133339c5cf67fc50c0e5846f339389bc16509228` selected those image tags in `deploy/charts/chimera/values.yaml`. This identifies a deployment choice, **not** the Chimera or CUFS source commits that built the image digests. Build records or attestations mapping both digests to source are still needed.
- Each of the three Chimera pods reported an empty list from its read-only `/api/v1/exports` endpoint. `/api/v1/mounts` reported the internal volume mount `vol-azure-c1b4a8bb-guid` at `/`; this is not evidence that an NFS export exists.
- No NFS export or SMB share custom resources, host or Kind-node NFS/CIFS mounts, or directly declared pod/PV NFS volumes were observed. External clients and their protocol versions remain unknown. Identifying them requires an authorized client inventory and its mount metadata.
- No NFS file reads or mounts were performed. The target was explicitly selected rather than using `nas-vm`'s default Kubernetes context.

## 14. READ correlation implementation status

**2026-11-03 — in progress, uncommitted, not deployed.** The current adapter source was located in `nasuni/chimera-build/modules/vfs_cufs/cufs.c`; the older CUFS adapter cited in the source investigation was removed from current CUFS main and does not establish deployed provenance.

- **Chimera:** opt-in NFSv4 READ operation IDs propagate through the asynchronous VFS gate. A bounded best-effort event queue emits protocol start/decision and CUFS completion with queue/log drop counters; origin remains unknown. NFSv3, SMB, S3 GET and `READ_PLUS` are not claimed. A full Debug build with tests enabled succeeded and six targeted tests passed after fixing an NFS test-link regression.
- **CUFS:** additive correlated READ and correlated READ-with-stat APIs preserve existing APIs and idempotency context. A copied ID and caller callback produce one completion per accepted call, including zero-length reads. Existing `test_file` passed 46 tests on `nas-vm`; no per-window fault/retry or remote-worker correlation is claimed.
- **Adapter:** forwards the READ ID and completion callback through normal and zero-length CUFS reads and retains the uncorrelated legacy path. Full module load and bridge CTests passed against staged matching Rocky builds on `nas-vm`; loading against an actual older CUFS library failed on the missing correlated symbol, as intended. The documented updated Chimera image build also succeeded. Strong symbol/SDK compatibility requires a coordinated rollout.
- **Open gates:** validate final source snapshots together, establish the deployed revisions, establish an actual NFS export/client, and obtain approval before live cluster reads or deployment. A protocol-decision event is not proof of final wire delivery, and unknown-origin events are not training demand.

**Confirmed payload limitation:** `read_correlation_v1` currently carries operation ID, event kind/time, status, bytes and `origin=unknown`; it intentionally does not carry stable volume/file identity, offset or requested length. It correlates outcomes but cannot alone identify the range to train on or warm.

**SDK rollout constraint:** SDK v2 gates the server-wide VFS module ABI. Every VFS module loaded by the server, including restore if used, must be rebuilt against the matching SDK version, not just the CUFS adapter. Coordinate these modules with the corresponding Chimera and CUFS builds before any approved rollout.

**ABI-gate evidence:** On `nas-vm`, the actual registration path rejected synthetic v1 module descriptors in a v2 server and synthetic v2 descriptors in a v1 server; matching-version harnesses passed. The mismatches were synthetic version stamps on same-version modules, not independently cross-built module artifacts.

**Combined READ review handoff:** The isolated, uncommitted Chimera SDK v2, CUFS correlation ABI v1 and adapter snapshots were built and reviewed together on `nas-vm`. The documented Chimera image build passed; CUFS reported 46/46 `test_file` cases; integrated adapter CTests passed 3/3, covering module load, mock-backed read cases and real-CUFS validation-error callbacks without a live backend. An actual older CUFS library failed adapter load on the missing correlated symbol. Review covered copied synchronous context, NFSv4-only initiation, bounded queue/drop counters, distinct protocol and CUFS outcomes and queue shutdown ordering. This is not a successful end-to-end NFS/CUFS read or a wire/serialization test. Exact source, patch and local binary hashes are recorded in the overall plan.

**Rollout/rollback planning:** Stage CUFS with the new exported symbol, then atomically pair the v2 server with all matching v2 modules and adapter. Roll back server and modules together, retaining compatible additive CUFS until no new adapter uses it. Verify the effective installed library because the init-container RPM overlay and same-version `dnf` behavior can change the result. Deployment and live cluster operations still require approval.

**Live-proof preflight — read-only, 2026-11-03:** The three-pod DaemonSet serves a shared NFS VIP; its running CUFS library lacks the correlated symbol, correlation is not enabled in the live config, and no NfsExport instances were listed. The internally mounted volume is not automatically an approved fixture location. The exact export authorization, fixture ownership and isolation or service impact remain unresolved; rolling updates are not an atomic cluster-wide transition. Missing events, drop deltas, multiple READs or unrelated traffic make the proposed proof inconclusive, and fixture attribution requires isolation and appropriate wire evidence because events omit file/range identity. No cluster modifications, mounts or live I/O are approved by this preflight.

**Isolation warning:** A second protocol-config-agent on an existing node is not proven safe: the node name keys its field manager and status entry, and the agent reconciles all Volume, NfsExport and SmbShare resources in its control namespace. A separate service label does not isolate those effects. The load balancer's cluster traffic policy and client affinity also mean the server-observed client address and backend cannot be inferred from the client route. An independent control identity/namespace and volume access must be proven safe, or a coordinated existing-DaemonSet rollout must be explicitly approved.

**Export authorization blocker:** Indexed `nasuni/strider-operators` source omits `NfsExport.allowedHosts`, `securityOptions` and `performanceTuning` during translation. The deployed agent image's OCI revision label resolves to `a489316b939211d5fb4d5dcbe779673703979167`, whose mapper also omits these fields. This associates an image label with source, not a reproducibly verified build or a live-export observation. Do not rely on `allowedHosts` or `securityOptions` for isolation or authentication until deployed behavior and independent controls are verified.

**Export source trace, not deployed behavior:** The export uses an owner reference to a Volume in the same control namespace; its internal path derives from the normalized volume GUID and export path. A root export would expose the whole volume. NFSv4-only is a client constraint, not a per-export server enforcement. The user selected `/test` and accepted normal cluster access; directory existence, ownership and permissions remain unverified. Cleanup must refuse preexisting resources, use recorded ownership and UID preconditions, and never delete an existing volume or unrelated data.

**Additional read-only inventory, 2026-11-03:** Existing volume `nasuni/vol-azure-c1b4a8bb` had UID `02157c13-daba-4da9-a78b-371d471d516a` and GUID `vol-azure-c1b4a8bb-guid`; recheck UID before approved creation. No Kubernetes NetworkPolicy objects were listed cluster-wide, which does not establish other network boundaries or policy enforcement. The deployed agent image's OCI label and its source association are recorded in the overall plan; its build is not independently verified.

**Fixture bootstrap and Helm blockers:** No current export or share provides an established client path for creating the test directory. The CUFS owner found no documented packaged live-directory creation CLI; raw CUFS APIs are not a validated operator workflow, and `cufs_ll_init_root` must not be invoked. Obtain an owner-provisioned subtree or approved supported management mechanism. A temporary whole-volume export is not authorized. Directory owner, mode, permissions and collision checks, and an independent network boundary, remain unverified. Cleanup must use exact owned names and inode/ownership checks without recursion; report ambiguous leftovers. The three-node deployment is Helm-managed and both config files use `subPath` mounts, so ConfigMap changes alone do not update running containers. Verify chart rendering and preserve the original release/config securely before proposing an approved coordinated rollout; compatible deployable artifacts are not ready.

**Confirmed Helm configuration blocker:** The deployed chart's ConfigMap template does not reference `nfs4_read_correlation`, and its values expose no correlation key. `helm upgrade --set` alone cannot enable events. A reviewed chart/template or equivalent managed config change and credential-safe rollback are required alongside matching images. Packaging is in progress; nothing has been published or rolled out.

**User-provided directory:** The user reports `/test` already exists relative to the root of `vol-azure-c1b4a8bb`; existence, ownership and permissions are not independently verified. The requested export targets this path, not the volume root. `/test` is pre-existing and must not be deleted during cleanup.

**Candidate export — creation requested:** Use a unique read-only NfsExport owned by the existing Volume and targeting `/test`. A known, non-sensitive 4096-byte file readable by the approved client remains needed for the eventual read. The agents would apply the export on all three servers. Schema-required host/security fields are not a verified boundary, and the user accepted normal cluster access. A writable export and fixture creation would need a separate explicit decision and approval. NFSv4.1 and `sec=sys` are client choices, not server-wide enforcement; event-to-file attribution still requires isolation or appropriate wire evidence.

**User decision:** Use a read-only export for `/test`. After being informed of the possibility of reads by other reachable clients and the unverified host/security restrictions, the user confirmed normal cluster NFS access is acceptable and requested the export. Deployment, mounts, live reads, fixture changes and network-rule changes remain unauthorized.

**Export created, 2026-11-03:** `nasuni/read-correlation-ef3667c7`, UID `d039cb6e-da80-4e67-8023-2953cac207c4`, targets `/test` on the existing Volume with `readOnly: true` and `root_squash`. Overall status is `Ready`; all three nodes reported `Applied`, observed generation 1, reason `ExportPresent`. This is agent-reported reconciliation, not proof of a mount, read, directory existence or permissions. Schema host/security fields are not verified enforcement. The export remains present; do not remove it without further authorization. Exact resource values and ownership are recorded in the overall plan.

**Topology clarification:** The user confirmed that all three Chimera servers applying the export behind the shared NFS address is expected, not a defect. The separate question is which clients can reach and read the export. A network-owner-approved design must account for the VIP, NodePort, pod addresses and node/in-cluster paths, NAT, replacement pods, impact, verification and rollback. Read-only inventory alone proves neither enforcement nor exposure.

**Additional NFSv4 operation events — child session report:** The separate [session](ghapp://sessions/14241c11-79ff-4ee8-b93a-94d53d228182) reports implemented WRITE, GETATTR, SETATTR, LOOKUP, RENAME and REMOVE start/completion events on the READ snapshot. They reuse the bounded queue/drop metrics and retain `origin=unknown`; READ format and CUFS correlation are unchanged. The child reports a full Debug Ninja build and 6/6 targeted tests, including 30 repeated `read_events` runs. The uncommitted incremental patch and dependency are recorded in the overall plan. The report has not yet been independently reviewed or integrated; no live I/O or deployment was reported.

## 15. Proposed contract: events → model → Warmer

**Not implemented or finalized.** The detailed versioned examples and ownership matrix are in §8 of the overall predictive-cache implementation plan.

1. **Model input:** validated, deduplicated foreground-read observations with event time, verified opaque volume/file identity, range, actual bytes and precisely defined outcome. Derive time-bounded features such as access frequency, recency and hour-of-week; use co-access only if session identity is verified. Warmer/replay/unknown-origin events are excluded from demand. Client-cache hits remain unobserved, and capture gaps must not silently become negative labels.
2. **Model output:** ranked `(volume_id, opaque_file_id, offset, length, score)` candidates with model/schema version, prediction time and horizon. A score is not a calibrated probability unless proven. The model does not issue reads or choose paths.
3. **Controller → Warmer:** the Controller validates candidates and enforces byte, rate, deadline, concurrency and foreground-pressure limits. An authorized resolver must find and revalidate a current export/path for each identity. It creates bounded read-only tasks with plan/task IDs, range and deadline; an unresolved or stale target is skipped.
4. **Warmer result → evaluation:** report requested/actual bytes, status, timing and errors; later verified foreground events establish usefulness. A successful warm read does not prove sustained residency or a later hit.

The range bucket, forecast horizon, score interpretation, stable identity, path resolution, origin attribution and acceptable capture-loss limits require owner decisions or evidence. Current READ-correlation events are not yet a trustworthy training dataset.
