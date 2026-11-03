# Engineer 1 implementation plan: CUFS access data and cache warming

**Reviewed source artifact SHA-256:** `ec663e2f33ed180f2433ea765fac438923bc9cbe8c9f1aabd9bdf138dbaa8d47`. This repository copy replaces private access paths and app-session links with stable placeholders; decisions, status, commands and evidence hashes are otherwise preserved.

**Status:** In progress; the historical target cluster and NFSv4.1 mount mechanism are verified, while event instrumentation, Warmer interoperability, cache residency and performance benefit remain open.
**Owner:** Engineer 1 — protocol/filesystem.
**Collaborators:** Engineer 2 (ML), Engineer 3 (data/telemetry), Engineer 4 (performance).
**Active target:** CUFS-backed Chimera on the disposable three-node Kind cluster `nasuni-cluster2` on `nas-vm`, VIP `<cluster-nfs-vip>`, volume `nasuni-cluster2-vol1`. Use `<isolated-kubeconfig>`, not the host's default context; obtain current authorized access through [`docs/nasuni-cluster-access.md`](nasuni-cluster-access.md). The original `nasuni-cluster` is now a protected comparison cluster and must not be modified by this work. For this project, do not access `strider-dev` or `strider-dev2`; run builds and tests on `nas-vm` in the respective Chimera or CUFS build container image.
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
| P0 | Locate target via `cluster-manager` using its actual backend; read-only inventory of node, volume, image digest, services and relevant metrics. Ask cluster owner before any modifying experiment. | Environment manifest, permissions and image/source mapping. | Use disposable `nasuni-cluster2` on `nas-vm`; recheck live state before experiments. Preserve `nasuni-cluster` as a protected comparison. |
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
- `nasuni/chimera-load-balancer` advertised TCP ports 2049 and 445 at `<historical-cluster-nfs-vip>`. No Kubernetes `NfsExport` or `SmbShare` custom resources were found. This does **not** prove that no export exists through another configuration path.
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

**Helm provenance and image pinning:** The deployed embedded templates match the local chart templates and chart version, but deployed values are not known to equal local defaults. Images are rendered as `registry/repository:tag` with pull policy `Always`; digest pinning needs a template change. Preserve and verify original image digests, chart and values for rollback, not just tags.

**Fixture decision:** The user reports no existing test file and authorizes creating a unique non-sensitive fixture. The export is read-only and no supported creation method is confirmed. Mounting, changing the export to writable, unrelated live reads and other cluster changes require separate approval.

**Fixture creation options:** Prefer an owner/operator's supported management path. Otherwise separately approve temporarily switching this export to read-write, converging all nodes, mounting to create a unique mode-0600 4096-byte file, unmounting and restoring read-only. Other reachable clients could write during the window; permissions may prevent creation. Fixture cleanup later requires a separately approved write path. No file has been created.

**User-directed alternative:** The user requested a separate read-write export of `/test` for fixture creation, keeping the read-only export unchanged. Verify same-path exports and independent access modes before creating or mounting it. Reachable clients may also write through the new export while it exists. No fixture has yet been created; removal of the export or fixture requires separate authorization.

**Same-path export investigation:** The deployed agent's labeled source maps distinct names and RO/RW options separately, and reviewed newer Chimera source uses per-export IDs and access checks. Neither establishes that the older running Chimera binary independently enforces the two modes; the image lacks a Chimera source-revision label. The owner is examining its cached artifact. No writable export or mount has been created pending verification.

The newer snapshot includes NFSv4 policy fix `7d286a6f` covering stale handles from deleted exports sharing a mount, cross-export LINK/RENAME and WRITE-stateid/current-filehandle binding. Its presence in the running image is unverified; this is not a finding that the deployed image has a defect.

**Deployed-binary evidence:** The owner reports per-export RO checks, cross-export guards, stale-handle checks and WRITE-stateid/current-filehandle validation in the exact deployed NFS binary; disassembly suggests distinct names on the same path get fresh IDs. Control-agent reconciliation and existing RO stability remain under investigation. RW writes change the same underlying data visible via RO. No second export or fixture has been created.

**Alias limitations:** Static analysis does not prove RO/RW independence across reconciliation or restart. The deployed REST library creates exports read-write by default before applying options, indicating a transient writable window when a read-only export is created or recreated. It does not show that adding an RW alias changes the existing RO record. Reachable clients able to use the RW alias can modify `/test`; the inspected host/security fields do not enforce restrictions.

**User decision:** After being informed of the unverified independence and writable-window limitations, the user explicitly chose to proceed with the separate writable export and fixture. This supersedes the earlier independence-verification gate. Preserve the existing read-only export; deployment, the correlation read and removal remain separately gated.

**Writable export and mount attempt, 2026-11-03:** Created `nasuni/read-correlation-rw-ef3667c7`, UID `eed1f7c6-eb4b-4e52-b7e8-d81a02b3d2b8`, on `/test` with `readOnly: false` and `root_squash`. Both exports report `Applied` generation 1 on all three nodes; the existing RO spec and UID are unchanged. The authorized NFSv4.1 mount failed with `No such file or directory`; cause unknown and under read-only investigation. No fixture was created or data read. The writable export remains present; do not remove it without further authorization.

**Mount diagnosis:** All three servers' REST responses show the intended RO and RW entries mapped to the same `/vol-azure-c1b4a8bb-guid/test` path with respective options and root squash. No matching export-error logs were found, and the attempted mountpoint is not mounted. A backing-path resolution failure and another NFS lookup issue remain possibilities; neither is established.

**Mount safety hold:** The diagnosis session reports that mount-name lookup on the planned Chimera base can invoke CUFS mount-root getattr, which can initialize a missing root. Earlier attempts may have reached that path, but this is unknown. Hold additional mounts, backing LOOKUPs and fixture I/O pending risk reconciliation with the user; the image rollout remains separately authorized. Targeted SECINFO may proceed only if its path avoids root getattr and initialization risk. Details are in the overall plan.

**Alias-only diagnostic:** A pod-local synthetic-root `PUTFH` + `SECINFO` succeeded for the RW alias on the rolled-back worker2 and advertised AUTH_SYS. The inspected server-image path resolves the alias in memory without VFS calls; no backing LOOKUP or GETATTR was issued. This proves that alias resolves on that backend now, not which backend handled earlier VIP requests or whether `/test` exists. A disposable-volume proposal is being prepared; no new resources are authorized yet.

**Disposable-volume proposal, not execution-ready:** A separate Azure-backed volume could be tested with a whole-volume bootstrap alias and `/test` alias. It would create cloud and cluster resources and writable exports, with unknown quota/cost and shared-server exposure. Credential authority, collision-safe cloud creation, backend pinning and permissions remain unresolved. Read-only preflight continues; no creation or cleanup is authorized, and a disposable result would not prove the existing volume's state.

**Root-initialization evidence:** CUFS base `9d55ae03` can initialize root after missing stat and unconditionally reset the inode counter to 11 as part of separate writes. Partial failure and races are possible; the deployed version and effects of earlier mounts remain unverified. A point-in-time metadata check cannot eliminate the race.

**`strider-tests` mount convention:** Its volume-scoped fixture mounts `<vip>:/<volume-name>`; the NFSv4.1 test uses `rw,hard,intr,vers=4.1`. The helper creates a unique mountpoint and invokes `mount -t nfs`. This is a source convention, not evidence that the present cluster exposes that volume path; the observed server mappings are the two explicit aliases. Do not broaden an export without authorization. Exact source references are in the overall plan.

Reviewed NFS lookup can return `NFS4ERR_NOENT` for either an unknown alias or a matched alias whose VFS backing path fails. Repeating the mount alone would not distinguish them. The owners are identifying a targeted metadata-only diagnostic; no additional mount, filesystem access or packet capture has been performed.

**CUFS diagnostic boundary:** No documented operator-safe, side-effect-free independent check for `/test` was found. The user clarified that no management view exists and Chimera pods are the only view into the filesystem. Investigate practical pod-based diagnostics. A custom low-level lookup needs credentials and faulting connectivity and may change cache/backend state; it is not an established operator workflow and requires a reviewed proposal and explicit approval. Raw Kvrocks absence may be a false negative for cold or stubbed entries.

The CUFS owner reports that `lookup(root, "test")` returning ENOENT can also mean missing or invalid root stat, not necessarily a missing `/test`. Root `getattr` can initialize a missing root and must not be treated as a harmless diagnostic. Endpoint, credentials, deployed ABI and lookup identity remain unresolved; no separate-process probe has run.

REST enumeration found no root (`/`) export on any server at inspection time, excluding one possible pseudo-root shadowing configuration at that moment but not proving what happened during the earlier mount. The lookup failure remains unresolved.

All three servers list CUFS mount `vol-azure-c1b4a8bb-guid` at `path: /`; this does not prove backing lookup succeeds. Analysis of the deployed binary confirms unknown alias and backing-path lookup can both yield `NFS4ERR_NOENT`; absence of an `unknown export` log is inconclusive. A narrowly scoped server-side trace is being assessed; none has been run.

**Trace feasibility:** VIP traffic cannot be assumed to reach a chosen backend. GDB is available but can stop the live server or threads and affect clients, and may capture unrelated activity without request/callback correlation. Attach permissions and safe breakpoint conditions are unverified. No validated narrowly scoped trace has been identified or run.

Offline ABI analysis shows a discriminating trace needs a bounded asynchronous alias-to-path-walk correlator and safe backend selection; no real trace program, permissions, routing or cleanup have been validated. Even VFS ENOENT can reflect caching or error translation.

A network-disabled synthetic GDB timeout detached from a sleep process, but this does not verify live-server safety, breakpoint cleanup or asynchronous correlation. Software breakpoints interfere with the live server. No attach or retry is approved.

The user confirmed there is no volume management view and Chimera pods are the only view into the filesystem. Targeted pod-based diagnostics are being investigated; none has been approved or run.

**Pod-targeted diagnostic candidate:** A Kind worker namespace has a route to one Chimera pod. After PID/IP/UID rechecks, a separately approved mount from that namespace might pin the request to that pod without network-rule changes, but is untested, uses a different client source from the VIP attempt and cannot alone identify the failing component. A bounded GDB trace is being explored; attaching would pause a live shared server and is not approved. A direct CUFS probe is also unready: the volume GUID uses a non-UUID XXH3 fallback whose deployed derivation and configuration require verification. No extra mount, trace or filesystem access has occurred.

**Runtime modules:** The running server maps the core VFS library plus cufs, diskfs, io_uring, linux, memfs, memkv, nfs, root, smb and sqlite modules; all loaded modules must match SDK v2. No running Chimera restore deployment was found, but restore jobs are not ruled out. Packaging is in progress.

**Chimera package:** The READ-only snapshot's Rocky 10.2 amd64 Release SDK-v2 server and in-tree modules have been packaged; exact artifact hashes are in the overall plan. The CUFS adapter and matched runtime image remain outstanding, and the package alone is not a runtime-tested deployable image.

**CUFS packages:** The CUFS owner reports uniquely versioned runtime/devel RPMs installed together in a local `cufs-base` image, with symbol checks, on `nas-vm`. Target overlay and runtime compatibility remain unverified; nothing was published or deployed.

**Combined runtime build:** Matching local runtime and CUFS init images were built on `nas-vm`; matching build-stage CTests passed 3/3, and runtime `dlopen(RTLD_NOW)` mapped the expected CUFS library. Exact local image, RPM and binary hashes are in the overall plan. Nothing was published or deployed; live RPC and cluster behavior remain unverified.

**Rollout staging:** Both matching images have been imported to all three Kind nodes with matching CRI IDs, without registry publication. A Helm dry-run reported only intended image/pull-policy, correlation flag and checksum changes; other resources compared equal without displaying credentials. The mount-diagnosis session can continue against replacement pods. Rollout has not yet been reported complete.

**Rollout failure:** One of three replacement pods repeatedly exited 139; the other two old pods remained ready. Helm rollback-on-failure and recovery are in progress but not yet confirmed. Mounts and backing probes remain on hold.

**Rollback in progress:** Interrupting the waiting upgrade triggered its configured rollback to revision 1 as Helm revision 3; no separate manual rollback was started. The failed pod logged `Module cufs symbol vfs_cufs not found after loading` and an abort/backtrace. `io_uring` EPERM was also logged but is not established as causal. Rollback pod readiness and the restored image pair remain unconfirmed.

**Rollback update:** The rollback worker2 pod has the original exact server and init digests; server is ready, protocol agent is not yet ready and Helm revision 3 is pending rollback. The likely startup cause is an omitted CUFS dynamic module path combined with the new daemon/core library lacking the old adapter `DT_NEEDED`. A proposed explicit module-path fix has not yet been validated.

**Rollback readiness investigation:** The protocol-config-agent on worker2 returns HTTP 500 for readiness, leaving the DaemonSet at 2/3 ready and Helm revision 3 pending rollback. Investigation is proceeding without backing-path access.

**Rollback complete:** Helm revision 3 is deployed as rollback to revision 1 and the DaemonSet is 3/3 ready. All pods have the original server/init digests and CUFS runtime; both export CR UIDs and their RO/RW mappings remain present on all nodes. Details and the replacement worker2 pod identity are in the overall plan. No fixture or live READ has occurred, and mount/backing I/O remains on hold.

**`striderctl` deployment direction:** The user selected upgrading existing `nasuni-cluster` to the version in the `striderctl` release manifest. Striderctl cluster deployment (private session evidence) is checking the correct workspace, exact release effects, resource preservation and rollback before execution. The earlier experimental READ-correlation images are not the selected version; no `striderctl` upgrade is reported yet.

**Baseline sequencing and preflight:** The user wants the manifest deployment as a fresh baseline, then local fixes layered on top; existing cluster data and resources are not authorized for deletion. The deployment session reports workspace manifest Strider 1.4.0 and enabled live chart versions match, but rendered values/images are not yet proven identical. Current CUFS pods use `plain` TCP yet attempt and fail to initialize the `io_uring` VFS module.

**Updated user direction:** The user said to ignore `io_uring` provided the CUFS backend initializes. This supersedes the earlier no-initialization-attempt requirement. Continue the manifest baseline after safety preflight; do not change kernel or transport settings solely for that failed attempt. Existing data preservation and mount/backing I/O hold remain.

**TCP configuration correction:** The workspace override, Helm values, ConfigMap and actual mounted config on all three pods show `plain` TCP with CUFS KV; revisions 1 and 2 also record `plain`. The earlier claim that the live deployment selected `io_uring` TCP was unverified and is contradicted by this evidence. Retain the `plain` override because the released chart defaults to `io_uring`. The separate VFS initialization attempt failed; exact old-image-to-new-source identity remains unverified.

**Automatic backend mounting during rollout:** The restored protocol agent automatically mounted the existing CUFS volume after Chimera initialization; a pod restart can repeat that internal mount even without a client NFS mount. Whether this startup path invokes root initialization is not established. The user explicitly allowed automatic backend mounting during the rollout; additional manual backing LOOKUP/root getattr, NFS client mounts and file I/O remain on hold. Full `striderctl --force` phases may also alter multiple releases and MetalLB ownership; the deployment session is investigating the narrowest scope.

**Backend-mount evidence refinement:** The observed agent operation was `cufs_ll_mount_ex`, not an NFS client mount. Its visible direct source path has no explicit root `getattr`/`lookup`, but indirect calls and source-to-RPM identity are unverified. Root initialization was not observed. Manifest-rendered Chimera objects match the seven deployed Helm revision objects; live resources and other releases are not thereby proven identical.

**Manifest rendering comparison:** All eight enabled application-release Helm renders (46 objects) match their installed release manifests; MetalLB dry run reports no difference. Live object state, mutable image tags and data/CRD rollback remain unproven. A previously reported startup suffix is a machine-id prefix, not a source commit. No new `striderctl` deployment has been reported.

**App-stack reconciliation preflight:** The deployment session is proceeding with app-stack-only reconciliation using the retained plain-TCP/CUFS override, not foundations or `cluster-manager upgrade`. Server-side dry-runs report no differences for live rendered resources, nine appliance CRDs, MetalLB objects and alert rules. Existing Helm revisions do not establish data or complete CRD rollback. On failure the session will stop and inspect before rollback; it will verify volume/export/PVC identities afterward without manual mounting or backing I/O. Completion has not yet been reported.

**User-directed `demo` export test:** The user authorized creation of an export named `demo` and a test mount on `nasuni-cluster`. The NFS diagnosis session will wait for confirmed reconciliation completion and assess the backing path and CUFS root-initialization risk before proceeding. No volume creation/deletion or fixture writes were authorized. Export creation and mounting have not yet been reported.

**`demo` mount safety blocker:** Root stat presence on the existing volume is unverified. In reviewed CUFS source, initializing an absent root resets the inode counter and could cause inode collisions or corruption; applicability to the deployed runtime is unverified. No `demo` export or mount has been created. Require explicit acceptance of this risk or a validated safe alternative; a disposable volume needs a separate provisioning proposal and approval.

**CUFS READ ABI warning:** Commit `5970146c6115e5823c3a3b5c1b762bc5f1a59e3c` changed the `cufs_ll_read` buffer argument from `void*` to by-value `cufs_read_iovec_t` without changing the C symbol. Symbol/SONAME matching does not prove compatibility; the exact release devel RPM confirms the legacy pointer signature, while broader replacement compatibility remains unverified.

**Reconciliation completed:** `striderctl deploy-app-stack` succeeded, advancing Chimera Helm revision 3 to 4. All three existing Chimera pods remained Ready without restarting, retaining the same server/CUFS image digests and plain-TCP/CUFS configuration. The existing volume, two exports, 16 pre-existing PVCs and three NFS service endpoints remained Ready/Bound. Resource continuity does not prove data-content integrity; no correlation images were deployed.

**`demo` decision:** The user accepted the potential existing-volume corruption risk from root initialization, but did not explicitly select the proposed `/test` backing path or accept possible transient writable exposure during export creation. The user chose investigation of a way to avoid temporary write access. No `demo` export or mount has been created; network modifications remain unauthorized.

**Deployed READ binary evidence:** Nonzero release adapter READ uses the legacy pointer-buffer `cufs_ll_read` ABI; zero-length READ bypasses it and calls `cufs_ll_getattr`. The apparent success-shaped getattr error is unverified. A faithful port must preserve those behaviors without substituting the newer iovec ABI; source provenance and broader DSO compatibility remain unknown.

**`demo` export exposure confirmed:** Offline disassembly shows the running REST/NFS libraries publish a default writable, no-squash export before applying requested options. A read-only CR or client mount does not close that window. The newer ordering fix is incompatible with the deployed agent's REST schema; no matched fix or full network isolation has been validated. No `demo` export has been created.

**Strider-tests NFS approach:** The user asked to follow the suite's NFSv4 approach: its fixture targets `/<volume-name>` and its mount test uses `rw,hard,intr,vers=4.1`. Its file test writes and deletes a 64 KiB fixture. Default volume fixtures create and tear down a volume unless an existing volume is selected. Diagnosis is checking applicability without running fixtures or making unapproved changes.

**User-directed test:** After the suite flow was explained, the user said "then do this." The diagnosis session was asked to use the existing volume's actual volume-scoped export, if configured, for an NFSv4.1 mount and a unique 64 KiB write/read/digest/delete test, followed by test-mount cleanup. Volume lifecycle changes and export creation through a transient RW/no-squash window remain unauthorized. No test result has yet been reported.

**Backing-path correction:** The user withdrew `/test` and requested the provisioned Kvrocks-backed volume as the export. Diagnosis was redirected to the existing Strider/CUFS volume's root, not the Kvrocks database/PVC itself. The previously accepted root-initialization risk and decision to avoid transient RW/no-squash export creation still apply. No root mapping or test result has yet been confirmed.

**Volume-root mount attempt:** No root export for `vol-azure-c1b4a8bb` appeared in the three live server export tables or NfsExport CRs. A direct NFSv4.1 mount of `<historical-cluster-nfs-vip>:/vol-azure-c1b4a8bb` with the suite's options failed with exit 32, server-reported `No such file or directory`. No mount remained and the temporary mountpoint was removed. No file I/O or cluster resource changes occurred; this was not a pytest run.

**Priority:** Debugging the mount issue is now the user's top priority. Diagnosis is tracing the absent root export, pursuing the safely isolated NFSv4.1 test, and identifying remediation that avoids the transient writable/no-squash creation window.

**Focused pytest blocker:** The isolated builder-container dependency setup failed with `PermissionError` scanning the read-only mounted `strider-utils/src`; pytest did not reach collection or make another mount. The session reports `aikidoctl` unavailable on `nas-vm` and no parseable desktop event JSON, and is respecting the package-protection boundary. The ephemeral container was removed; no cluster resources or volume data changed.

**Mount diagnosis:** Live export tables and NfsExport CRs have no `vol-azure-c1b4a8bb` alias. During the direct test interval, a Chimera pod logged `lookup for unknown export 'vol-azure-c1b4a8bb'` at `nfs4_root.c:124`. This supports failure at export-name resolution before backing-volume lookup, but the log lacks request correlation and cannot conclusively be tied to the mount. Available operator/test source describes explicit export creation; deployed agent source provenance remains unverified.

**Safe export investigation:** REST creation exposes a create-then-set writable/no-squash window. Deployed daemon/library disassembly suggests startup configuration applies export options before NFS listens, but safe failure handling, agent reconciliation, other protocols, and restart/failover remain unverified. This is a potential restart-based alternative, not a proven atomic API. Configuration changes and pod restarts are not yet authorized; offline investigation continues without package-install retries or live changes.

**Startup-path safety limit:** Deployed daemon disassembly indicates startup ignores export create/set return codes. An overlong export name could be truncated on creation, fail option lookup, and leave the default RW/no-squash policy published. This is a static inference, not a reproduced event or evidence the short requested name fails. SELinux category separation blocked exact-image offline runtime testing; the exact deployed agent image is not locally available. No relabeling, protection bypass, or live change occurred.

**Additional startup fail-open evidence:** Exact-release disassembly shows unrecognized static export `options` or `squash` logs an error but continues with RW or no squashing, respectively. Startup configuration is not a fail-closed safety gate as-is. The short requested alias was not tested. Actual agent adoption, restart/failover and multi-node behavior remain unverified; no safe live configuration/restart/rollback sequence is established.

**Historical baseline investigation:** The user asked in another coordinated chat for an earlier compatible `strider-deploy`/Chimera/`chimera-build` set and authorized creating a separate Kind cluster named `nasuni-cluster2` once a suitable path is established. Historical commits, image/source provenance, ABI compatibility, and provisioning are being researched. Preserve existing clusters; no replacement of an existing `nasuni-cluster2`, separate cloud-volume/export creation, image publication, current-cluster changes, or package-protection bypass is authorized.

**Historical export comparison:** The STRID-4889 session's Oct 29 snapshot records NFS `/vol1` mapping to the `nasuni/vol1` volume root, unlike the current cluster's absent root alias. `strider-dev` was reported full with Podman state inaccessible on Nov 3, so that old cluster's current state was not revalidated. This is historical export-configuration evidence, not proof of a release regression.

**Manual Warmer review status:** The Warmer session reports its uncommitted implementation and guide ready for review, with 206 offline tests, Ruff checks, and CLI help passing. Real-server interoperability, live READ correlation, cache residency, and performance benefit remain unverified; no live pilot was performed. Mount diagnosis remains the user's top priority.

**Parallel release preparation:** Chimera, CUFS, adapter and Warmer sessions are preparing the local fixes in parallel with the baseline. The manifest is Strider 1.4.0; exact release source-to-image provenance remains unverified, so the earlier READ patches cannot yet be assumed compatible with the manifest artifacts.

**Release adapter compatibility:** The manifest runtime's inspected adapter uses legacy `cufs_ll_read` and has a different `vfs_cufs` object size from the newer SDK-v2 candidate. The earlier stat-based patch cannot simply be transplanted; establish the release ABI and preserve legacy read, zero-length and error behavior. Exact release source provenance is still unverified.

**Additional compatibility gate:** The source owner reports that the deployed chart omits protocol enable flags and READ correlation, while the new server defaults NFS, SMB, S3 and correlation OFF. Preserve the actually required existing protocols explicitly and validate backend readiness; a module-path fix or a running process alone is insufficient.

**Isolated transport blocker:** A corrected-config full-daemon smoke in a network-isolated container failed because `io_uring` was unavailable and the listen path segfaulted. Container restrictions may differ from the Kind pod. An isolated `plain` TCP comparison is diagnostic only; a live transport change requires explicit user approval.

**Isolated `plain` comparison:** The CUFS adapter loaded and protocol/REST initialization was reached, but CUFS lacked a test DKV and the NFS FH keyring then failed closed. This does not establish backend or listener readiness. `plain` affects shared TCP, not just NFS; no live transport change is approved.

**Transport environment comparison:** A short-lived child in the restored live pod received EPERM from `io_uring_setup`; the isolated container received ENOSYS. Both observed `io_uring_disabled=2`, but their seccomp settings differ. The old daemon continues listening; a new-pod crash remains an inference, not an observed result. The integration session is preparing alternatives without changing live configuration.

**Transport alternatives:** The team identified an unvalidated code fix plus explicit fallback policy, or configuration-level `plain` TCP for shared NFS/SMB/S3/REST/metrics transport. Both require isolated startup and readiness validation; the `plain` performance impact is unmeasured and live use requires a user decision. A synthetic loopback-only Valkey setup is being prepared for isolated backend validation.

**User direction:** The user says `io_uring` should not initialize and the Chimera VFS backend is CUFS. The integration session is determining how to preserve CUFS while satisfying this, including whether selecting `plain` TCP avoids all `io_uring` initialization or only listener use. No live transport change has been approved.

**Plain TCP does not eliminate initialization:** An isolated smoke with loopback-only disposable Valkey loaded CUFS and opened NFS/SMB/S3/REST listener ports, but its wrapper masked the daemon's eventual exit; sustained readiness and live READ behavior were not established. A prior `plain` smoke still attempted to initialize the `io_uring` VFS module. The user later accepted that failed attempt provided CUFS initializes.

**Supported no-`io_uring` candidate:** `-DIO_URING_ENABLED=OFF` reportedly disables the Chimera VFS module and libevpl implementation at build time; the chart default needs a `plain` override, which the actual deployment already has. The isolated rebuild was stopped at the user's request and produced no image. A no-`io_uring` build is not a prerequisite under the user's updated direction.

**Cross-cluster comparison:** At the user's request, Compare test export (private session evidence) is inspecting `strider-dev2` for a comparable separate RO `/test` export, subject to safe volume selection and bounded reconciliation risks. Creation status alone will not prove the backing path exists or mounts.

**Cross-cluster discovery:** `manifest-recursion-fix` is reported as the only Kind cluster on `strider-dev2` with a volume; its `nasuni/vol1-manifest-1d585305` already has a wildcard RW/no-root-squash volume-root export at `/vol1`, not pseudo-root `/`. No new export has been created.

**Cross-cluster safety decision:** Deployed binary disassembly indicates an added RO/root-squash export is initially published RW/no-squash before options are applied. Agent source associated by image label omits host/security fields; that association is not reproducible-build proof. Creation's inspected binary call sequence does not itself perform a backing lookup, but a client could access the published export and deployed CUFS-root behavior is unverified. The user chose not to create the export; no new reconciliation or mount result exists.

**Overlay and rollback evidence:** Network-isolated tests showed the actual old init-overlay RPMs downgrade the new runtime to the exact running old CUFS library, causing the new adapter to fail loading on missing `cufs_ll_restore_file`. The old library lacks additional symbols including `cufs_ll_read_stat_correlated`. New runtime and new overlay must be coordinated; rollback requires the exact old image and overlay. Mixed versions during a rolling update remain untested.

**Rollout/rollback proposal — not approved:** Review an updated chart that renders the correlation flag with a default-off value, compare non-secret rendered resources against the deployed release, then—after separate approval—publish both local images under unique tags and record actual registry digests. Upgrade the server and init image fields together in one Helm revision; verify image IDs, mapped library/hash, symbol, flag, module registration and readiness on all nodes. The rolling transition is not cluster-wide atomic. Roll back to the verified prior Helm revision only after checking old tags resolve to retained original digests; verify the original image, overlay, mapped library and protocol convergence. Helm rollback does not remove the independently created exports or fixture. Exact proposal and gates are recorded in the overall plan; no publication, upgrade or rollback has been performed.

**Local OCI archives:** Integrity-verified runtime and CUFS init OCI archives are preserved temporarily under `<private-evidence-root>/read-correlation-packaging`; exact paths and SHA-256 values are in the overall plan. They are local-only, not registry artifacts, and `/tmp` is not durable. Nothing was published or deployed.

**Origin and data-quality workstream started:** Chimera and CUFS owners are investigating trustworthy foreground/Warmer attribution, drop measurement and reconciliation, stable opaque volume/file identity, offsets/requested lengths, lifetime across rename/restart/faults, and privacy/compatibility boundaries. Unknown-origin events remain excluded from foreground training data. No consequential schema change is approved yet.

**Updated user authorization, 2026-11-03:** The user requested debugging the export/mount, recreating a test export only if missing, deploying the matching images and performing the live READ. This supersedes earlier deployment, mount and live-read approval gates for those actions. It does not authorize overwriting/deleting existing resources, broadening to the volume root, registry publication, network changes, GSS or commits/pushes. Preserve existing exports and fixture unless separately authorized. Authorization does not mean these actions have completed.

**CUFS source findings — no implementation:** CUFS READ receives a caller-supplied mount key, numeric inode, offset and count, but has no authoritative foreground/Warmer signal. Mount key and inode are not proven globally stable or secret-safe identifiers. Same-inode rename preserves inode; unlink/recreate and file restore can change it. Persistent inode counters suggest conditional continuity, but rollback, cloning and volume recreation require a verified incarnation/epoch. Birth time alone is insufficient because restore preserves it. See the overall plan for source references.

CUFS range/count describes a CUFS call, not necessarily an NFS request. Multiwindow reads may retry or short-read; failed calls reset reported bytes to zero even after earlier successful windows. Only a successful checked `[offset, offset + bytes_read)` is a candidate observed range; exclude zero lengths and do not infer no backend work from an error. The current callback is synchronous once per accepted correlated call; invalid/legacy paths and protocol rejects have no CUFS callback, and queue/log loss is external.

**Candidate contract — not implemented or approved:** A separate versioned schema could carry trusted origin/evidence, status and validity, per-call offset/requested/completed counts, and approved volume/file identity with incarnation. HMAC pseudonyms remain linkable and cannot be reversed for warming without an authorized mapping and key policy. Required tests include range/error boundaries, concurrent IDs, callback behavior across retries, rename/recreate/restore identity, and exact queue/export/shutdown loss; restart/failover/rollback claims need isolated fixtures. Existing v1 tests do not establish these guarantees.

**Chimera origin findings — no implementation:** AUTH_SYS UID/GID and machine name are client asserted; IP, hostname and operation ID do not prove origin. A GSS principal is a candidate only with verified authenticated integrity, exclusive credentials and exact-match policy. Reject empty, missing or truncated identities; even a valid principal identifies a credential-holder rather than the cause of each read. A shared client or kernel readahead can blur foreground demand. `not warmer` is not equivalent to foreground; unknown stays excluded. The current `sec=sys` test export does not establish GSS availability.

**Earlier trusted-client investigation:** The user initially requested investigation of authenticated Warmer and demand-client credentials, superseded by the later decision that Kerberos/GSS is out of scope. A demand-client label does not prove each read came directly from an application.

**Trusted-client investigation — findings, not implementation:** The deployed NFS library contains GSS-related dependencies, but GSS configuration, credentials, KDC, enforcement and actual use are unverified. In reviewed source, `krb5i` protects the request body; `krb5` alone does not. Principal truncation/empty names, mechanism and canonical realm validation, UID mapping, ignored GSS initialization failure, privacy-service confidentiality checks, and existing INFO logging of principals/keytab paths all require review before using this for attribution. A versioned exact-match policy could label verified dedicated Warmer or trusted demand credentials, with all uncertainty mapped to `unknown`; it would not prove each demand-client read was foreground. No authentication or schema change is approved.

**Warmer-log proposal:** Investigate the user's suggested join as positive evidence only. A trustworthy shared ID across the Warmer, NFS client and server is unverified, as are split/retry/readahead and log-loss semantics. A missing Warmer log does not prove foreground demand.

**User clarification:** No Warmer implementation exists; it must be built from scratch. Its NFS client and logging can be designed together with attribution. Kernel-mounted reads versus an application-controlled userspace NFS client remain undecided; investigate a connection-scoped RPC transaction/operation join, authentication and retry/cache/loss semantics without assuming feasibility.

**Manual Warmer implementation session:** Manual cache warmer (private session evidence) is working in parallel on a manually operable, bounded read-only NFSv4 Warmer and usage documentation in an isolated `predictive-cache` worktree. It will investigate client support for the selected COMPOUND tag; the Chimera bridge is not yet implemented. Pilot commands and implementation are not yet available.

**Experimental client decision:** The user approved experimental use of `pynfs`, a GPL-2.0-only test library obtained from Git and a candidate for COMPOUND-tag support, subject to confirming safety and deadline controls. Production suitability, distribution licensing obligations and a working pilot remain unverified.

**Warmer client progress:** Pinned `pynfs` source reportedly built and imported in the `nas-vm` CUFS build container; adapter work is underway. No live NFS read or working pilot has been demonstrated.

**Predictive-cache main refresh:** Local `main` and local `origin/main` are both `11ee405f0b084a27a6b185de640c7b5c4fc7d370`; no remote fetch was performed. Main now includes a Python 3.12 datagen foundation and a tracked protocol/filesystem plan, but no Warmer. Datagen is fixture/stub plumbing, not real workload data. The Warmer worktree starts at that commit; default pytest discovery currently targets only `tests/datagen` and must include Warmer tests.

**User scope decision:** Kerberos/GSS is out of scope. The prior GSS findings are historical investigation, not an implementation plan. Operational correlation without another trusted identity mechanism is not cryptographically authenticated provenance.

**Candidate Warmer join — not implemented:** Standard NFSv4 COMPOUND has a client-selected `tag` echoed in the response. A controlled userspace NFS client could generate a bounded random token per compound and log its task mapping; Chimera could record a validated token plus READ ordinal with its generated operation ID, which already links to CUFS. One READ per compound or explicit ordinals, retries/replay, async reuse, loss and privacy all need implementation and tests. The tag is client-controlled, so a match is observational evidence under controlled, non-adversarial assumptions—not authenticated origin. Missing matches remain unknown. Ordinary kernel-mounted reads have no established application API to set the tag and can be split, cached or read ahead.

**Alternative RPC-XID bridge:** Source assigns XIDs per connection, but the public getter returns the next XID, not an atomic per-call result. A correct join would require an actual-send hook, separate connection/process epochs and verified client/server connection association, with reconnect, NAT, wrap, replay and loss handling. A per-COMPOUND tag avoids the separate association but neither option authenticates the sender. A separate controlled demand-generator could provide labelled synthetic NFS traffic, not representative production-user demand. Both client choice and acceptable operational trust assumptions remain undecided.

**Hackathon design decision:** The choices were (1) NFSv4 COMPOUND-tag correlation and (2) RPC transaction IDs plus a connection-level join. The user selected the **COMPOUND-tag approach** as the simpler hackathon candidate; XID correlation requires additional reconnect, NAT, ID-reuse and retry handling. The selected bridge is not yet implemented or validated, does not authenticate origin, and leaves unmatched events `unknown`. GSS is out of scope.

**Recommended path — proposal:** Finish the live READ proof, then establish approved identity/range capture and loss accounting. For an initial experiment, collect a controlled, explicitly scoped workload with the Warmer disabled and verified quiescent; label eligible records as observed demand-client NFS traffic, not guaranteed application demand. Verify exclusivity and capture coverage, account for readahead/background activity and reject inadequate runs. Investigate the COMPOUND-tag bridge for observational Warmer correlation under controlled assumptions; GSS is out of scope. Missing logs do not establish foreground. A protected authorized resolver remains necessary for warming.

**Drop-accounting findings:** Current per-process queue drops include clock failure, contention, full queue and stopping; log drops mean failed local logger admission. Accepted logging is not proof of persistence or collection. Counters publish asynchronously, reset on restart and are aggregate by stage. Zero deltas do not prove zero downstream loss or a complete dataset; no per-kind attempt count or drain/watermark currently exists. A separately designed epoch, per-kind counters, consistent drain and collector acknowledgment would be needed for stronger accounting. Never subtract counters across process restarts.

**Range/identity and decisions:** Protocol offset/count is the original NFS range; CUFS requests or internal windows must be separately labelled. NFS handles, export IDs and non-cryptographic `fh_hash` are not established canonical privacy-safe object IDs; VFS delegates stable identity to the backend. A new schema, authenticated origin policy, volume/file incarnation contract, pseudonym privacy/key policy, authorized Warmer resolver and loss/acknowledgment threshold remain decisions—not implemented behavior. New per-window instrumentation must not be confused with the current one-callback-per-CUFS-call contract.

**User-provided directory:** The user reports `/test` already exists relative to the root of `vol-azure-c1b4a8bb`; existence, ownership and permissions are not independently verified. The requested export targets this path, not the volume root. `/test` is pre-existing and must not be deleted during cleanup.

**Candidate export — creation requested:** Use a unique read-only NfsExport owned by the existing Volume and targeting `/test`. A known, non-sensitive 4096-byte file readable by the approved client remains needed for the eventual read. The agents would apply the export on all three servers. Schema-required host/security fields are not a verified boundary, and the user accepted normal cluster access. A writable export and fixture creation would need a separate explicit decision and approval. NFSv4.1 and `sec=sys` are client choices, not server-wide enforcement; event-to-file attribution still requires isolation or appropriate wire evidence.

**User decision:** Use a read-only export for `/test`. After being informed of the possibility of reads by other reachable clients and the unverified host/security restrictions, the user confirmed normal cluster NFS access is acceptable and requested the export. Deployment, mounts, live reads, fixture changes and network-rule changes remain unauthorized.

**Export created, 2026-11-03:** `nasuni/read-correlation-ef3667c7`, UID `d039cb6e-da80-4e67-8023-2953cac207c4`, targets `/test` on the existing Volume with `readOnly: true` and `root_squash`. Overall status is `Ready`; all three nodes reported `Applied`, observed generation 1, reason `ExportPresent`. This is agent-reported reconciliation, not proof of a mount, read, directory existence or permissions. Schema host/security fields are not verified enforcement. The export remains present; do not remove it without further authorization. Exact resource values and ownership are recorded in the overall plan.

**Topology clarification:** The user confirmed that all three Chimera servers applying the export behind the shared NFS address is expected, not a defect. The separate question is which clients can reach and read the export. A network-owner-approved design must account for the VIP, NodePort, pod addresses and node/in-cluster paths, NAT, replacement pods, impact, verification and rollback. Read-only inventory alone proves neither enforcement nor exposure.

**Additional NFSv4 operation events — child session report:** The separate session (private session evidence) reports implemented WRITE, GETATTR, SETATTR, LOOKUP, RENAME and REMOVE start/completion events on the READ snapshot. They reuse the bounded queue/drop metrics and retain `origin=unknown`; READ format and CUFS correlation are unchanged. The child reports a full Debug Ninja build and 6/6 targeted tests, including 30 repeated `read_events` runs. The uncommitted incremental patch and dependency are recorded in the overall plan. The report has not yet been independently reviewed or integrated; no live I/O or deployment was reported.

## 15. Proposed contract: events → model → Warmer

**Not implemented or finalized.** The detailed versioned examples and ownership matrix are in §8 of the overall predictive-cache implementation plan.

1. **Model input:** validated, deduplicated foreground-read observations with event time, verified opaque volume/file identity, range, actual bytes and precisely defined outcome. Derive time-bounded features such as access frequency, recency and hour-of-week; use co-access only if session identity is verified. Warmer/replay/unknown-origin events are excluded from demand. Client-cache hits remain unobserved, and capture gaps must not silently become negative labels.
2. **Model output:** ranked `(volume_id, opaque_file_id, offset, length, score)` candidates with model/schema version, prediction time and horizon. A score is not a calibrated probability unless proven. The model does not issue reads or choose paths.
3. **Controller → Warmer:** the Controller validates candidates and enforces byte, rate, deadline, concurrency and foreground-pressure limits. An authorized resolver must find and revalidate a current export/path for each identity. It creates bounded read-only tasks with plan/task IDs, range and deadline; an unresolved or stale target is skipped.
4. **Warmer result → evaluation:** report requested/actual bytes, status, timing and errors; later verified foreground events establish usefulness. A successful warm read does not prove sustained residency or a later hit.

The range bucket, forecast horizon, score interpretation, stable identity, path resolution, origin attribution and acceptable capture-loss limits require owner decisions or evidence. Current READ-correlation events are not yet a trustworthy training dataset.

## 16. Historical-cluster mount proof and resumed implementation — 2026-11-03

This section supersedes earlier statements that `nasuni-cluster2` was only proposed, that its storage blocker was unresolved, or that no Warmer implementation existed.

### Historical baseline now provisioned

- `nasuni-cluster2` was provisioned successfully on `nas-vm` by the October 20 cluster-manager as a three-node Kind cluster. All three nodes are Ready.
- Exact selected revisions and components:
  - `strider-deploy`: `9eca2d7039151df271f2853e75e6b28340bbe7a2`
  - historical cluster-manager: `217bd9118c082497c0311637e50ea08b6098dd34`
  - Strider manifest: `1.4.0`
  - Chimera chart: `1.1.18`
  - server image: `chimera-cufs:0.1.0-26`, digest `sha256:2ac4b9977f5e9f0810707dfd9fa9c5e980dc40729b05cfb97ae1008e64bbb27d`
  - CUFS init image: `cufs-base:0.2.0-48`, digest `sha256:2fa381a6d783d0e738bab943ab23621188979780cca4e817d4243f645918ff7c`
  - protocol agent digest: `sha256:e78ff5a0c4c38cb67d86b2ba120f365b42fd1268ff872d9d4d39a644f2633512`
- Chimera is 3/3 Ready, all three nodes report CUFS initialized, and the mounted configuration uses plain TCP. The server image's exact Chimera source revision remains unknown and does not establish that it was built from the October 19 default-branch commit.
- The cluster VIP is `<cluster-nfs-vip>`. Its conflict check was point-in-time on the isolated Kind bridge, not a global reservation.
- Appliance registration completed and the appliance pod reached 2/2 Ready.
- CloudCred `nasuni-cluster2-azure` is Ready and validated. Its credential value must not appear in reports; the key was exposed in chat and should be rotated.
- VolumeKey and Volume `nasuni-cluster2-vol1` are Ready. The Volume is Active and Healthy with container `nasuni-cluster2-vol1-guid`.
- Namespace `vol-nasuni-cluster2-vol1-guid` has initialized/running Kvrocks, nine ready member pods, and nine Bound PVCs.
- The original `nasuni-cluster` remained healthy throughout provisioning; its original volume, two exports and sixteen PVCs were not modified.

### NFSv4.1 mount and bounded integrity result

A disposable root export named `demo` was created for `nasuni-cluster2-vol1`, became Ready and Applied on all three Chimera nodes, and was mounted from `nas-vm` as `<cluster-nfs-vip>:/demo` with NFSv4.1 over TCP.

- Export settings included root path `/`, read-write access, `sec=sys`, `sync`, `no_root_squash`, and the narrow host value supported by the test setup.
- The test generated and fsynced 65,536 bytes, read the file through a fresh process, and obtained matching source/read SHA-256:
  `995eced60546af3be7595cee951e286734fe2bee53f291dd9087ed357f710b25`.
- The file was deleted, the mount was removed, and the `demo` export was deleted. No NFS export or SMB share remained after that test.
- This proves that a published root alias on the historical cluster supports a bounded NFSv4.1 mount and write/read/delete flow. It does not prove remote-object-store service, crash durability, CUFS cache residency, or later-read benefit; client caching could have served the read.
- The earlier ENOENT on the original cluster is therefore consistent with its missing client-facing alias. A Ready Volume/internal CUFS mount does not publish an NFS alias automatically.

### Current `/test` export plan

The user authorized two aliases backed by `/test` on `nasuni-cluster2-vol1`:

| Alias | Backing path | Intended use |
|---|---|---|
| `test-ro` | `/test` | Read-only foreground/Warmer measurements |
| `test-rw` | `/test` | Controlled fixture creation and maintenance |

Because `/test` did not exist on the new volume, it was created through a temporary root bootstrap export; that export was then removed. Both requested aliases are now Ready and Applied on all three nodes with `allowedHosts=[10.89.0.1]`, `root_squash`, `sec=sys` and `sync`. `test-ro` mounted and correctly rejected a create with `EROFS`. `test-rw` mounted and, after Chimera's configured 60-second NFSv4 grace period expired, wrote, fsynced and read back 4,096 bytes with matching SHA-256 `38d1b224d7faef9c56548ed245af8b92d6a095e81c81c7ebcbb446cf34188905`; the validation file was deleted. `/test` is mode `0755`, owner `1002:1002`. The bootstrap export, temporary mounts and validation files are gone; only `test-ro` and `test-rw` remain. A fresh-process read may still have been served from client cache, so this is not CUFS cache-residency evidence.

Client-visible NFS paths are export aliases, not Volume names. The intended read-only mount source is:

```text
<cluster-nfs-vip>:/test-ro
```

It maps to CUFS backing path `/test`; the backing path does not imply a client-visible `:/test` alias.

### `strider-tests` NFS test status

The intended focused selector was:

```text
tests/integration/filesystem/nfs/test_nfs_mount.py::test_nfs_root_mount[FS-TC-NFSMNT-03]
```

It was not executed:

- Existing host environments lacked `pkg_resources`.
- The documented local setup uses pip without a lockfile and includes an HTTP private index plus `--trusted-host`; that insecure setup was not run.
- The repository-documented `strider/test-container:latest` was resolved and pulled at digest `sha256:d7977eaef757f854648b2ef6d9913a83f3ffded060248dab5c02bcf6c61d5b21`, but it lacked `fs`, `pkg_resources`, and installed Strider fixtures.
- A read-only checkout bind was denied by SELinux. No relabeling or security weakening was performed.
- Offline caches were incomplete for the documented dependency set.

The user chose to skip pytest and use the successful disposable `demo` integrity test instead. No test fixture export or mount was left behind.

### Event-source decision: Chimera only for demand

The model's demand stream will use Chimera as the sole per-read event source for the MVP. CUFS evidence is still used for analysis of cache behavior, materialization and benefit, but CUFS is not a second demand-event source and no correlated CUFS READ API or adapter change is in scope.

The boundary is:

```text
Chimera NFSv4 READ event
    -> demand history, range features, later-use labels and model evaluation

Existing CUFS telemetry / controlled cache-state probes
    -> analytical evidence for faults, remote work, materialization, retention
       and later-read behavior where those signals are actually verified
```

CUFS telemetry must not be counted as another user access, used to fabricate missing Chimera events, or treated as a foreground label. Aggregate CUFS counters may show that backend work occurred during a controlled interval but do not attribute that work to a specific READ unless an independent join is proven. Missing CUFS telemetry is not evidence of a cache hit.

The desired Chimera event is one bounded, best-effort NFSv4 READ decision/completion observation containing only verified fields such as schema version, event time, requested range, returned bytes, NFS outcome, a local event/request ID where available, and `origin=unknown`. It must retain queue/log drop accounting and must not claim wire delivery, client receipt, stable file identity, or CUFS hit/fault state.

The Chimera owner is removing the prior VFS SDK/request correlation changes and reworking the patch into a Chimera-only producer. This reduces cross-layer coupling but does not solve deployment compatibility:

- The working snapshot is based on Chimera `3009924e...`.
- Its unmodified VFS ABI still differs from the verified legacy adapter ABI in `nasuni-cluster2`.
- Therefore the newer Chimera binary must not be mixed with the historical server/CUFS/adapter bundle.
- Live validation requires either a provenance-backed port to the exact legacy source, which is not currently available, or a complete mutually compatible newer Chimera/CUFS/adapter bundle loaded into the disposable cluster.
- No custom correlation image has been deployed or published.

### Manual Warmer correction and next gates

PR #41 merged only `docs/manual-warmer.md`. The actual Warmer source, tests, entry point, and updated guide remain uncommitted in the isolated Warmer worktree and are absent from GitHub `main`; a clean checkout cannot run the Warmer yet.

The Warmer target is the read-only export `<cluster-nfs-vip>:/test-ro`. Stable fixture `/test/fixtures/predictive-cache-warm-64k.bin` is 65,536 bytes, mode `0640`, owner `1002:1002`, full SHA-256 `edc2db331ceb6826898e3312192d62b9bbae0e52d6a46446366761b7626e1916`. A 4,096-byte prefix read as UID/GID `1002:1002` was independently validated through `test-ro`.

The one authorized live Warmer attempt succeeded during `2026-11-03T13:05:18.752618356Z`–`13:05:19.415125135Z`: requested/read 4,096 bytes at offset 0, `status=completed`, `reason=complete`, `bytes_read_is_lower_bound=false`, exit 0. The Warmer emitted one READ-attempt tag plus setup/cleanup COMPOUND tags; these are client attempt evidence only because the Chimera tag bridge is not implemented.

Read-only server observation identifies `chimera-cufs-clg7j` on `nasuni-cluster2-worker2` as the likely serving pod: it alone logged in-window NFS session activity and its VFS Read counter increased `3 -> 4`; other pods remained `0 -> 0`. This is not a matched request-level correlation. CUFS `Read`/`ReadData` series reset during the observation window, so subtraction is invalid; no per-file fault, cache-hit or materialization signal was established. The successful Warmer read proves live NFSv4.1 interoperability only, not cache residency or performance benefit.

The resumed sequence is:

1. Finish and validate `test-ro` and `test-rw`.
2. Create a known fixture through `test-rw`; use `test-ro` for measured reads.
3. Finish the ABI-preserving Chimera-only event patch and targeted tests.
4. Establish a deployable fully compatible local image bundle or a provenance-backed legacy port; do not mix ABIs.
5. Deploy only to disposable `nasuni-cluster2` after explicit review of the local-image plan.
6. Run a controlled foreground NFSv4 READ and verify the Chimera event fields and drop accounting.
7. Run the bounded Warmer against the known fixture and keep its events out of foreground-demand training.
8. Use separate CUFS/cache evidence to determine whether the intended range materialized and whether a later foreground read benefited.

**Time constraint:** only 24 hours remain. The critical path is therefore the smallest historical Chimera-only NFSv4 READ event, targeted tests, one compatible local server image, deployment to `nasuni-cluster2`, and a controlled live READ. CUFS API work, adapter-correlation changes, additional protocol operations, generalized provenance reconstruction and nonessential refactoring are deferred.

**Final MVP evidence scope:** validate file and directory data behavior only. Mandatory gates are file create/write/fsync/read/checksum/delete and directory create/list/child read-write/delete/remove, plus the NFSv4 READ event and Warmer read. GETATTR and other metadata operations need only avoid crashing when required by those data operations; birth time, SMB CreationTime and general metadata correctness are explicitly excluded from MVP claims.

## 17. Context map: historical correlation port

**Review gate:** no historical source branch may be edited until this map and its candidate-baseline policy are reviewed. Exact image-to-source provenance remains unavailable; the candidates below are evidence-backed inferences, not verified release commits.

### Baseline evidence

| Component | Deployed evidence | Narrowest current source candidate | Confidence boundary |
|---|---|---|---|
| Chimera core | Server digest `sha256:2ac4b9977f5e9f0810707dfd9fa9c5e980dc40729b05cfb97ae1008e64bbb27d`; deployed DWARF anchors match historical NFS/VFS lines; in-tree CUFS source paths are present | `00f207bb358c3a6fa2e914e3e42b301e1c726b80`; candidate libevpl gitlink `90417684e61fa931e24745fa282d4ee1c2e4e8b8` | Several core anchors match, but the candidate adapter differs from deployed and the actual libevpl pin is unknown. This is not a proven release tree. |
| CUFS | Runtime overlay RPM `0.2.0-48`; runtime library SHA-256 `66109c8514b459a1125faabeb0ae061a49b70304a9dc355329238cbbc99222e7`; devel header SHA-256 `1cfc649d32692c912aed10c7a9f2a5550e33a9fd81652f6f9cb74bb46e99b19e`, Git blob `4e5085a47d9d77aeb2aea5c24186352c59eb1aae`; RPM build time 2026-10-07 19:36:37 UTC | `963aa684501626ad4d85037f1706f39117019e34`, latest first-parent candidate before RPM build time with matching header; submodule `a63ac43bf192772910b52e27cd2b42a6098a0071` | Header equality and chronology narrow the candidate but do not prove the built library, branch or dependency pins. Build number `48` is not a commit identity. |
| In-tree adapter | Legacy server contains an in-tree CUFS adapter, unversioned 64-byte `chimera_vfs_module`, 40-byte mutation op context and pointer-form `cufs_ll_read` call | Closest adapter candidate `ca9292bd8559a2f9f73048ac476d948c18fb2f8f`; best historical blob matched 43/58 DWARF anchors but lacked deployed KV functions | No reviewed candidate is identical to deployed. The adapter may need reconstruction against the selected Chimera/CUFS candidates rather than a claimed exact checkout. |
| Build integration | Historical image uses an in-tree adapter and CUFS init overlay; chart `1.1.18`; server and overlay must be paired | Historical `chimera-build` candidate `d31732a4e2c36de4746d883ed9d7497c4ce50a6f`, which pins different Chimera `ead876cd402280daa2eecf9adf2177dd7e07ff0e` | Candidate recipe is useful reference, not proof it built the deployed image. Exact source RPM/build provenance is still preferred. |

Runtime identity is now verified consistently across all three `nasuni-cluster2` Chimera pods:

- `/usr/sbin/chimera`: SHA-256 `317566c1831267882576cd33a040286f9cd961c7c43121031d82592a28f2d578`, build ID `082c1ac9bf5ad44413850a6b1f64c17182b63657`.
- `/usr/lib/libchimera_vfs_cufs.so`: SHA-256 `205688574f1fd5392cbfa4d91d9a4d7ec6e26c4b3007974b12aa100af873a3e4`, build ID `d5243f0f899c7f73ecb4dab4cde03df141103873`.
- Effective mapped `/usr/lib64/libfsal_cufs.so.0.2.0`: SHA-256 `66109c8514b459a1125faabeb0ae061a49b70304a9dc355329238cbbc99222e7`, build ID `616e7595da93fb386d84d2be90b26db774264f7b`, from CUFS RPM `0.2.0-48.el10`.
- The server image's baked pre-overlay CUFS library differs: SHA-256 `7aa6c8f05731af072a880ea731c9a84ceee09519808515fc0934320d0dd639f6`, build ID `c0deeea5316587bfefeba8c45a97a745fd46a0f1`. The init overlay therefore materially changes the effective runtime and remains part of the inseparable baseline.

These hashes close the runtime-artifact identity gap, not the source-provenance gap.

### Files to modify

| File | Purpose | Changes needed |
|---|---|---|
| Historical Chimera `src/server/nfs/nfs4_proc_compound.c` | NFSv4 compound dispatch and pre-dispatch rejection | Mark an `OP_READ` decision before dispatch/gates using historical rejection helpers; prevent duplicate events on rejection, replay or `REP_TOO_BIG`. |
| Historical Chimera `src/server/nfs/nfs4_procs.h` | Compound completion | Emit exactly one READ decision event before request index/status mutation; derive requested offset/count and terminal status/bytes from historical request/response structures. |
| Historical Chimera `src/server/nfs/nfs_common.h` | Per-request marker and shared event sink | Add only private NFS-layer state; reset markers on pooled allocation/reuse. Do not modify the VFS request/module ABI. |
| New historical Chimera `src/server/nfs/nfs4_read_events.c/.h` | Bounded event producer | Port the bounded nonblocking queue, worker-generated event ID, timestamp, requested/returned range, status, `origin=unknown`, and separate queue/log drop counters to historical APIs. |
| Historical Chimera `src/server/nfs/nfs.c`, `src/server/server.c`, `src/server/server.h`, `src/daemon/daemon.c` | Lifecycle and default-off configuration | Create/destroy the sink safely, expose a default-off option, drain before freeing shared state and preserve all historical defaults. |
| Historical Chimera `src/common/logging.c/.h` | Nonblocking complete-line logging | Adapt the nonblocking INFO admission path and visible rejection/drop behavior without making foreground READ wait on the global logger. |
| Historical CUFS `src/include/cufs/fsal_cufs/fsal_cufs.h` | Public pointer-form additive API | Add a distinctly named/versioned correlated READ API and copied context compatible with the legacy pointer-buffer `cufs_ll_read`; do not reuse newer iovec symbols or calling conventions. |
| Historical CUFS `src/fsal_cufs/file.cpp` | Correlated low-level wrapper | Wrap the exact legacy READ path, invoke one synchronous completion callback for accepted calls, preserve errno/bytes and avoid extra stat/read I/O. |
| Historical CUFS read implementation files | Logical READ/fault semantics | Preserve multi-window, retry, EOF and error behavior. Aggregate only what the selected contract explicitly requires; do not turn internal windows into demand events. |
| Historical in-tree Chimera `src/vfs/cufs/cufs.c` | VFS-to-CUFS bridge | Rework only the positive-length historical pointer READ branch to call the additive correlated API while preserving scratch allocation, scatter, attrs, EOF, errno and completion. Preserve the zero-length getattr bypass and characterize its legacy error behavior instead of silently fixing it. |
| Historical in-tree adapter `CMakeLists.txt`/headers | Compile/link integration | Link the new additive symbol and tests into the actual in-tree target without introducing the newer external-module SDK layout. |
| Historical build `Dockerfile.cufs`, `build_container.sh`, `Jenkinsfile`, `.gitmodules` | Reproducible matched bundle | Pin candidate source/submodules and exact runtime/devel RPM inputs; build Chimera and its in-tree adapter together and pair the server with the matching CUFS init overlay. |
| Historical chart/config templates | Feature flag and paired images | Add a default-off event flag and explicit paired server/init image values only after the historical parser and chart behavior are verified. |

### Dependencies that may need updates

| File or component | Relationship |
|---|---|
| Chimera historical request pool/allocation paths | Must initialize and clear the private READ marker on every reuse; stale state could duplicate or mislabel events. |
| Chimera historical shutdown and thread lifecycle | Event worker and logger admission must drain/stop before shared NFS state is freed. |
| Candidate libevpl submodule | Required to reproduce historical build behavior; deployed pin remains unknown. |
| CUFS public devel RPM/header | Server adapter must compile against the same pointer-form header shipped with the runtime overlay. |
| CUFS runtime library and init overlay | New additive symbol must exist in the effective mapped library on every node; server and overlay are one rollout/rollback unit. |
| Historical module/request ABI | Legacy module is 64 bytes and unversioned; newer SDK-v1/v2 modules must never be mixed with it. |
| NFSv4 replay, undispatched-op and response-size paths | All converge on completion and can otherwise cause duplicate or misleading READ events. |
| Predictive-cache Controller schema | Chimera event is the sole demand record; a CUFS callback is correlated diagnostic/cache evidence and must not be ingested as a second access. |

### Test files

| Test | Coverage |
|---|---|
| Historical Chimera `src/server/nfs/tests/test_read_events.c` (new/adapted) | Queue cardinality, exact schema, unique IDs, disabled mode, contention/full queue, log rejection, counters and shutdown. |
| Historical Chimera `src/server/nfs/tests/test_nfs4_read_decision.c` (new/adapted) | Success, short/zero/error, gate rejection, multi-READ compounds, repeated completion, request reuse, replay, undispatched trailing READ and `REP_TOO_BIG`. |
| Historical `test_nfs4_undispatched.c`, `test_replay_slot.c`, `test_nfs4_close.c`, `test_open_claim_fh.c`, `test_nfs4_export_policy_collect.c` | Reference regressions for historical request, replay, handle and export-policy behavior. |
| Historical CUFS unit tests around `cufs_ll_read` | Pointer-form correlated/uncorrelated data equality, short read, EOF, errors, invalid context, copied ID, callback-once semantics, concurrency and no callback on bypass. |
| Historical CUFS zero-length tests | Preserve adapter getattr bypass and document the legacy outcome; zero-length must not be reported as a CUFS READ completion. |
| Historical in-tree adapter READ tests | Scratch buffer and iovec scatter, attrs/EOF/error propagation, positive-length bridge and zero-length bypass. |
| Matched module/load tests | Load against the exact runtime/devel pair; reject missing/mismatched additive symbol and validate ABI layout/build IDs, not symbol presence alone. |
| Combined container smoke test | Start matched server/init bundle, verify effective mapped library/module hashes, CUFS initialization and plain TCP before cluster import. |
| Live `nasuni-cluster2` test | Controlled `test-ro` READ produces one Chimera demand event and optional correlated CUFS diagnostic completion; validates drop counters and leaves fixtures/exports in the approved state. |

### Reference patterns

| File | Pattern |
|---|---|
| Current uncommitted Chimera `nfs4_read_events` work on base `3009924e...` | Bounded nonblocking queue, event schema, queue/log drops and targeted tests; port semantics, not newer APIs or ABI changes. |
| Historical Chimera `test_nfs4_undispatched.c` and replay tests | Correct historical completion/rejection and replay behavior. |
| Current CUFS correlated READ work | Copied versioned context, synchronous callback-once behavior and validation tests; redesign symbol/signature for the pointer-form baseline. |
| Current adapter `tests/vfs/test_read_correlation*.c` | Callback success/error and worker-concurrency test ideas; rewrite for historical pointer READ and in-tree module. |
| Historical CUFS `ChunkAtimeSet` | Bounded/drop-accounting precedent only; its locking/allocation behavior is not automatically suitable for foreground READ events. |
| `nasuni-cluster2` `test-ro`/`test-rw` validation | Proven NFSv4.1 path and fixture-management surface for eventual live testing; not cache-residency proof. |

### Risk assessment

- [x] Breaking changes to public API: additive CUFS API and server/module ABI must be controlled; existing symbols and layouts must remain unchanged.
- [ ] Database migrations needed: none planned.
- [x] Configuration changes required: default-off Chimera event flag and coordinated server/init image selection.
- [x] Exact source provenance unresolved: candidate baselines require explicit acceptance and must not be described as release-identical.
- [x] Rolling mixed-version risk: server and CUFS overlay are not atomically updated across nodes.
- [x] Same-symbol ABI hazard: historical pointer and newer by-value iovec APIs cannot be inferred from SONAME/symbol names.
- [x] Event duplication/data-quality risk: Chimera demand and CUFS completion must remain separate event types and ingestion roles.
- [x] Foreground-path risk: callbacks, queues and logging must remain bounded and must not change READ outcomes.
- [x] Rollback risk: rollback must restore the exact paired historical server and init overlay and verify the effective mapped library/module on every node.

### Review decisions required before implementation

1. **Accepted for the hackathon experiment:** `00f207bb...` is the experimental Chimera baseline when exact release provenance cannot be established; it must never be described as the verified release source.
2. **Accepted for the hackathon experiment:** `963aa684...` is the experimental CUFS baseline under the same evidence boundary; it must never be described as the verified release source.
3. **Superseded by user decision:** use Chimera-only demand events. Do not port the additive CUFS correlated READ API or change the historical in-tree adapter for per-read correlation. Preserve the deployed CUFS/adapter ABI and use separate existing cache-state evidence or controlled probes.
4. Require the candidate build to reproduce the legacy 64-byte module ABI and pointer-form CUFS interface before any feature code is accepted.
5. Keep all historical-port work uncommitted/unpushed and all deployment local to `nasuni-cluster2` until separate review.

### Historical implementation sessions started

- Historical CUFS correlation (private session evidence): stopped by scope decision; historical CUFS must remain unchanged.
- Historical Chimera correlation (private session evidence): branch `predictive-cache-warming-historical` from candidate `00f207bb...`; implementing only the Chimera NFSv4 event stream, with no VFS SDK or adapter correlation changes.
- Historical matched bundle (private session evidence): branch `predictive-cache-warming-historical` from build candidate `d31732a4...`; preparing a historical Chimera event build paired with the unchanged verified CUFS `0.2.0-48` overlay, ABI gates, tests and local-only rollout/rollback plan.

All source work remains uncommitted and unpushed. A custom historical image has now been built locally on `nas-vm` but has not been approved, imported into Kind, published or deployed.

### Historical Chimera-only patch ready

The historical Chimera session completed the minimal NFSv4 READ decision-event patch on experimental candidate base `00f207bb358c3a6fa2e914e3e42b301e1c726b80`. The work remains uncommitted and unpushed. Stable patch ID: `0703197c05079c30926285ac1c6e92a384a0d397`.

- Config key: `server.nfs4_read_correlation`, default false through zero-initialized historical server configuration.
- Trigger: dispatcher arms only the current NFSv4 `OP_READ`; compound completion clears before emitting, so each reached READ decision emits at most once. Cached replay short-circuits before arming; multiple READs arm independently; repeated completion and later `NFS4ERR_REP_TOO_BIG` rewriting do not duplicate; dispatcher-level rejection emits one error decision.
- Schema: `schema=nfs4_read_v1`, process-local UUID, `origin=unknown`, realtime nanoseconds, requested offset/count, returned bytes, NFS status and `success|error`.
- Overload behavior: fixed 4,096-entry queue, one worker, request-path `pthread_mutex_trylock`, no foreground waiting. Timestamp/lock/contention/full/stopping losses increment queue-drop metrics; nonblocking logger rejection increments log-drop metrics.
- Semantics: protocol decision/completion evidence only; no wire-delivery, client-receipt, stable-file-identity or CUFS hit/fault claim.
- Changed files: historical NFS request/compound/completion, new event service, server/NFS lifecycle/config, nonblocking logger, tests and configuration documentation. `src/vfs`, `src/vfs/sdk`, `ext`, CUFS and the in-tree adapter are unchanged.
- Validation on `nas-vm` used authorized Chimera builder image ID `48c7d8f9b69b` with `IO_URING` and Cairn disabled for the targeted build. Linked Chimera target succeeded; 5/5 targeted tests passed: nonblocking logger, READ event service, READ decision, replay slot and undispatched operation. `git diff --check` passed.

The verified patch handoff and independent review are complete. No material READ-event correctness or safety findings were reported; known full-stack coverage gaps remain cached replay, multi-READ compounds, final-serialization `NFS4ERR_REP_TOO_BIG`, and shutdown racing final queue drain.

The local `nas-vm` runtime image build completed:

```text
localhost/chimera-cufs:read-events-00f207bb-0703197c-nasvm-20261103T1325Z
```

- Current local image ID: `sha256:5acf469fb0b7e52eb5b08bff290064cf014827fab382bf43a0bb03443eec37dc`.
- Current local digest: `sha256:95a2c9c507a599fb5fbaa4167d6e510b8316e6821a218774efb5195aa94b8015`.
- Build inputs: exact patch/base, exact staged CUFS `0.2.0-48` runtime/devel RPMs, and scoped accepted compatibility wrapper.
- Completed gates: patch SHA/stable ID/base/file-set, VFS/SDK/ext unchanged, exact RPM hashes, link/dependency checks, ABI probe (`chimera_vfs_module=64`, request size `6600`, pointer READ interface), 5/5 targeted tests, default-off startup and explicit-enabled startup with CUFS initialization, metrics and clean shutdown.
- The discarded `strider-dev2` image is not authorized for use and is explicitly rejected by rollout automation.

The image is **not yet deployment-approved**. The remaining pre-deployment gate is direct CUFS-backed file read/write, directory and GETATTR no-crash smoke using the correct volume-GUID configuration, followed by rootful image finalization and OCI archive SHA-256/size. Only after those pass may the prepared single rollout load this exact `nas-vm` artifact into `nasuni-cluster2`.

### Accepted PoC build compatibility limitation

The experimental Chimera candidate does not compile directly against the exact deployed CUFS `0.2.0-48` devel header: candidate adapter source refers to `cufs_stat_t` and `st_btim`, while the verified historical header exposes `struct stat` and lacks `st_btim`. The nas-vm build uses a scoped build-only compatibility include:

- `cufs_stat_t` aliases `struct stat`.
- `st_btim` aliases `st_ctim`.

The user explicitly accepted this limitation for the hackathon PoC because its required evidence concerns file/directory data read/write, not birth-time metadata correctness. This does not make the candidate source release-identical. Acceptance is conditional on:

- Verified structure size/layout and no memory-safety mismatch.
- Successful unchanged adapter load with CUFS `0.2.0-48`.
- File read/write and directory-operation validation.
- GETATTR/metadata operations do not crash.
- No claim that birth time is accurate; the experimental build may report change time where candidate code requests birth time.

The compatibility wrapper is build-context-only and must be documented in the artifact provenance. It must not be presented as a CUFS source or runtime change.

Independent review classified the `st_btim -> st_ctim` substitution as a HIGH client-visible semantic defect because SMB marshals Chimera birth time as CreationTime, so later metadata changes can make the reported creation time change. The reviewer independently verified the relevant structure sizes/offsets and found the `cufs_stat_t -> struct stat` alias layout-safe; the finding is semantic rather than a demonstrated overflow or data-path corruption.

After receiving that full explanation, the user explicitly accepted the limitation for this disposable PoC and directed that no birth-time or SMB CreationTime correctness claim be made. This acceptance is limited to `nasuni-cluster2` and does not waive adapter load, memory/layout, file read/write, directory operation, GETATTR no-crash, CUFS initialization, readiness or rollback gates.

## 18. Current repository contracts: Chimera → Controller → Warmer

**Sources refreshed from `predictive-cache` main `64a2cae` (`Resolve README conflict markers`, PR #70).** README conflicts are resolved. The concrete v1 Controller/planner contract is in `docs/ml/spec.md`; the strict generated event contract is in `src/predictive_cache/datagen/schema.py`; cleaning/origin rules are in `docs/data-prep/spec.md`; Warmer execution behavior is documented in `docs/manual-warmer.md`. Controller, planner and dispatcher contracts are planning specifications and are not implemented on `main`.

### 18.1 Chimera raw READ decision event

The historical Chimera patch currently emits a line-oriented raw source event:

```text
schema=nfs4_read_v1
local_id=<process-local UUID>
origin=unknown
event_time_ns=<Unix realtime nanoseconds>
requested_offset=<u64>
requested_count=<u32>
returned_bytes=<u32; zero on error>
nfs_status=<u32>
outcome=success|error
```

Producer guarantees:

- At most one event for each reached NFSv4 READ decision/completion.
- Replay short-circuits before arming; multiple READs in one compound arm independently.
- Bounded 4,096-entry queue and nonblocking logger admission.
- Queue and log losses are counted separately.
- Event export cannot change the foreground READ result.

Producer non-guarantees:

- No wire-delivery or client-receipt proof.
- No stable `volume_id`, `file_id`, `dir_id`, path or file size.
- No authenticated origin; all records are `origin=unknown`.
- No CUFS hit, miss, fault or residency state.
- `local_id` is process-local and not a durable file/request identity.

### 18.2 Normalized Controller access-event v1

The repository's strict event schema requires:

```json
{
  "schema_version": 1,
  "event_id": "opaque globally deduplicatable ID",
  "event_time": "UTC timestamp",
  "ingest_time": "UTC source receipt timestamp",
  "volume_id": "opaque volume identity",
  "file_id": "opaque stable file identity",
  "dir_id": "opaque directory identity",
  "operation": "read",
  "handle_id": null,
  "offset": 0,
  "bytes_requested": 4096,
  "bytes_read": 4096,
  "handle_bytes_read": null,
  "handle_duration_ms": null,
  "file_size": 65536,
  "result": "ok",
  "origin": "foreground",
  "session_id": null,
  "cohort_id": null,
  "client_id": null,
  "app": null,
  "cache_outcome": null,
  "latency_ms": null
}
```

Allowed origins are `foreground`, `warmer`, `synthetic` and `unknown`. Cleaning excludes warmer, synthetic and unknown origins, failures, invalid ranges and duplicates from demand. Therefore the current raw Chimera event **cannot yet become a model-demand event**: it lacks verified volume/file/directory/file-size identity and its origin is `unknown`.

Required source-to-Controller mapping:

| Controller field | Chimera source / decision |
|---|---|
| `schema_version` | Map `nfs4_read_v1` to normalized event schema version 1 through a versioned adapter. |
| `event_id` | Derive from process/boot epoch plus `local_id`; process-local UUID alone needs a documented restart/deduplication boundary. |
| `event_time` | Convert `event_time_ns` to UTC. |
| `ingest_time` / `known_at` | Controller stamps receipt time; never trust source time as availability time. |
| `operation` | Constant `read`. |
| `offset`, `bytes_requested`, `bytes_read` | Directly available. |
| `result` | Map `outcome=success` and `nfs_status` to `ok`; preserve failures explicitly. |
| `volume_id`, `file_id`, `dir_id`, `file_size` | **Missing:** requires a validated identity/enrichment source; do not infer from raw path or unstable hash. |
| `origin` | **Missing/unknown:** requires verified foreground/Warmer attribution. Controlled windows are experimental evidence, not authenticated production origin. |
| optional session/client/app/cache/latency | Null until independently verified. |

The planned Controller live endpoint accepts JSON arrays at `POST /v1/events`, stamps authoritative receipt order, enforces bearer authentication and allowed volumes, and answers `202` without waiting on processing. Chimera must still export asynchronously from its own bounded buffer; Controller limits do not protect foreground I/O by themselves.

### 18.3 Model prediction contract

Model scoring, packaging, evaluation primitives and training are now implemented on main by other engineers; Controller/planner runtime remains under parallel ownership and is not yet visible as merged source. This protocol/filesystem lane does not duplicate that work. Its responsibility is to publish a reviewed, testable event contract and executable task contract, provide fixtures, and validate the cross-component handoff. Contract changes must be coordinated with the model/planner owners before either side freezes an implementation.

The model/scorer produces ranked estimates, not executable reads:

```json
{
  "schema_version": 1,
  "model_version": "logreg-v1@<package_id>",
  "prediction_time": "2026-11-03T09:00:00Z",
  "volume_id": "vol-001",
  "file_id": "file-000451",
  "horizon_seconds": 300,
  "score": 0.82,
  "calibrated": false,
  "candidate_sources": ["recent", "neighbor"],
  "reasons": ["coacc_max_conf:+0.83"],
  "planned": true,
  "skip_reason": null
}
```

`score` is a ranking score unless calibration is proven. Predictions contain opaque identity, not a mutable path. The planner—not the model—applies eligibility, recent-read suppression, cooldown, score threshold, path resolution, byte budget, item cap and deadline.

### 18.4 Controller warm-plan contract

The concrete v1 plan-file contract is:

```json
{
  "schema_version": 1,
  "run_id": "<controller.run_id>",
  "plan_id": "<controller-id>-<tick>",
  "model_version": "logreg-v1@<package_id>",
  "created_at": "2026-11-03T09:00:00.120000Z",
  "deadline": "2026-11-03T09:05:00Z",
  "max_bytes": 4096,
  "items": [{
    "task_id": "<plan_id>-001",
    "volume_id": "nasuni-cluster2-vol1",
    "file_id": "<opaque fixture identity>",
    "path": "/test-ro/fixtures/predictive-cache-warm-64k.bin",
    "offset": 0,
    "length": 4096,
    "score": 0.82,
    "reasons": ["controlled-poc"]
  }]
}
```

Plan invariants:

- JSON file is written atomically, mode `0600`.
- Plan is immutable after dispatch.
- `max_bytes` covers requested bytes across all attempts; v1 retry allowance is zero.
- v1 range is offset 0 and `min(file_size, 16 MiB)`.
- Dispatcher must not start an item after `deadline`.
- Path appears only in the private plan, not predictions/logs/metrics.
- Path must be absolute NFS pseudo-root path with valid UTF-8, no `.`/`..`, bounded total/component length.
- Resolve and revalidate current path, size, permissions and allowed volume close to execution. Path alone is not durable identity.

The plan intentionally does not carry an NFS RPC. Server endpoint and AUTH_SYS identity are deployment configuration for the Warmer/dispatcher, not model output. For the current PoC:

```text
NFS host: <cluster-nfs-vip>
port: 2049
AUTH_SYS UID/GID: 1002/1002
plan item path: /test-ro/fixtures/predictive-cache-warm-64k.bin
```

### 18.5 Dispatcher reconstructs the Warmer request

The planned dispatcher atomically claims a plan, then for each item:

1. Reject if expired or over plan budget.
2. Revalidate path syntax, existence, current size and `offset + length`.
3. Apply configured rate/item limits, one in-flight read per volume and pressure/stop controls.
4. Invoke one bounded `pcache-warmer` process with configured endpoint/identity plus item path/range:

```text
pcache-warmer
  --host <cluster-nfs-vip> --port 2049
  --path /test-ro/fixtures/predictive-cache-warm-64k.bin
  --offset 0 --length 4096 --max-bytes 4096
  --timeout-seconds <remaining bounded deadline>
  --uid 1002 --gid 1002
```

The Warmer establishes NFSv4.1 session state and reconstructs the protocol sequence: EXCHANGE_ID, CREATE_SESSION, RECLAIM_COMPLETE, path LOOKUPs, read-only OPEN, bounded READ COMPOUND(s), CLOSE, DESTROY_SESSION and DESTROY_CLIENTID. Client-selected COMPOUND tags are attempt evidence, not authenticated origin.

### 18.6 Warmer result contract

Per task:

```json
{
  "schema_version": 1,
  "run_id": "<run>",
  "plan_id": "<plan>",
  "task_id": "<task>",
  "status": "completed",
  "reason": "complete",
  "bytes_requested": 4096,
  "bytes_read": 4096,
  "bytes_read_is_lower_bound": false,
  "started_at": "<UTC>",
  "finished_at": "<UTC>"
}
```

Statuses are `completed`, `partial`, `skipped`, `failed` or `cancelled`. Actual returned bytes come from the Warmer terminal record and are never assumed. Completion proves execution only; later verified foreground demand supplies usefulness evidence, and separate CUFS telemetry/probes are needed for materialization or performance claims.

### 18.7 Contract gaps blocking a real model loop

1. Chimera raw events do not satisfy the normalized identity contract.
2. `origin=unknown` means current events are excluded from demand training by repository policy.
3. No implemented raw-event exporter/Controller HTTP adapter exists on `main`.
4. Controller, model, planner and dispatcher specs are concrete but not implemented.
5. Real path resolution from opaque `(volume_id, file_id)` is unresolved; v1 fallback validates path + size only.
6. No authenticated Warmer-origin bridge exists; COMPOUND tags are observational.
7. README conflicts were resolved in main `64a2cae`; contract-changing implementation must still follow the concrete specs and strict schema code rather than illustrative examples alone.

### 18.8 v2 implementation amendment

The v1 Chimera image and rollout are superseded because `nfs4_read_v1` cannot identify the target file/volume or provide executable target information. Helm revision 2 began before the hold arrived; rollback to revision 1 and original-state verification are required before any v2 deployment. The v1 source patch, image and evidence remain preserved but must not be used for model ingestion or rollout.

Rollback completed successfully. Helm revision 3 contains restored revision-1 content; interrupted revision 2 remains recorded as `pending-upgrade` in Helm history but is not deployed. All three pods run the original server digest and verified original server/adapter/CUFS hashes; CUFS initialization/plain TCP, volume, nine Kvrocks pods/PVCs, both exports, fixture checksum and absence of READ-event config/metrics passed. Readiness briefly dropped to 2/3 during rollback; client-visible impact was not measured. The v1 candidate remains only in node caches with rollout approval removed.

The v2 amendment has two implementation tracks:

1. **Chimera `nfs4_read_v2`:** add only NFS-layer fields that can be copied safely without changing VFS/adapter/CUFS ABI. Required feasibility targets are export/backing-volume context, copied opaque current NFS file handle or a stable derived file reference, valid file-size/attribute evidence where available, compound tag/origin evidence, client/session evidence, range/outcome, and producer epoch/sequence for restart and drop reconciliation. Raw handles and paths are private operational data and must never enter metrics or general logs.
2. **Warmer target v2:** accept a private executable target derived from the plan—prefer an opaque NFSv4 file handle plus export/volume/task identity—and reconstruct OPEN `CLAIM_FH`/PUTFH/READ without relying on a mutable path. Structured bounded COMPOUND tags carry task/origin attempt evidence. Endpoint and AUTH_SYS credentials remain deployment configuration, not model-controlled fields.

If historical Chimera cannot expose a safe copied file handle/export identity at READ completion, the fallback is a separate restricted resolver/catalog populated from validated LOOKUP/OPEN evidence; the model still emits opaque identity, and the planner resolves the current executable target immediately before dispatch.

No v2 image may be built or deployed until the exact event/task schemas, privacy boundaries, field lifetimes and tests are reviewed. The preserved nas-vm v1 build context may be reused incrementally after the v2 patch delta is available.

### 18.9 Accepted disposable-MVP v2 contract

The user explicitly accepted logging the wrapped NFS target handle in general Chimera INFO logs for this disposable MVP. It is not treated as sensitive in this experiment. This is a PoC-only decision and must not be generalized to production; retention and access restrictions remain recommended.

**Chimera `nfs4_read_v2` record:**

```json
{
  "schema": "nfs4_read_v2",
  "local_id": "<UUID>",
  "producer_epoch": "<process/boot epoch>",
  "producer_sequence": 123,
  "minorversion": 1,
  "compound_op_index": 3,
  "operation_id": "<sha256 session/slot/sequence/op identity>",
  "event_time_ns": 0,
  "volume_id": "<hex 16-byte VFS mount ID>",
  "file_id": "<sha256(volume_id || inner_vfs_fh)>",
  "export_name": "test-ro",
  "export_id": 7,
  "target_fh": "<base64url wrapped NFSv4 wire handle>",
  "clientid": "<optional>",
  "session_ref": "<optional sha256>",
  "slotid": 0,
  "sequenceid": 12,
  "compound_tag": "pc1:w:<plan-ref>:<task-ref>:<nonce>",
  "compound_tag_original_length": 104,
  "compound_tag_truncated": false,
  "compound_tag_digest": "<sha256>",
  "origin_claim": "warmer",
  "origin_trust": "client_asserted",
  "requested_offset": 0,
  "requested_count": 4096,
  "returned_bytes": 4096,
  "nfs_status": 0,
  "outcome": "success",
  "eof": false,
  "file_size_valid": true,
  "file_size": 65536
}
```

Rules:

- MVP emits only NFSv4.1 session READ events. NFSv4.0 events are out of scope.
- `operation_id` is `SHA256("chimera-read-op-v2\0" || raw_sessionid || BE32(slotid) || BE32(sequenceid) || BE32(compound_op_index))`. Cached replay must not emit a second event.
- Multi-READ compounds use distinct operation indexes. Repeated completion, undispatched operations and final serialization failure must not duplicate events.
- Inner VFS FH is copied at dispatch; wrapped `target_fh` is minted with existing Chimera NFS FH encoding before request reuse.
- Decoded NFSv4 wire handle length must be 1–128 bytes.
- `volume_id` is the stable 16-byte mount ID, not sequential export ID.
- `file_id` is exactly `SHA256("chimera-read-v2\0" || mount_id[16] || BE16(inner_fh_len) || inner_fh)`. Export aliases and re-wrapped/signing-key-rotated handles for the same inner FH share file identity. Publish fixed test vectors.
- Export name is copied while holding the export lock; export ID is a routing/revalidation check, not stable identity.
- Client/session evidence is copied by value for operation identity, then hashed for general logs; raw values are not logged.
- File size is requested through the existing READ attr mask and emitted only when valid.
- Raw pointers, unbounded tags and raw inner FH bytes are never retained.
- Queue/drop/nonblocking behavior remains the v1 design.

**Structured COMPOUND tag contract:**

```text
Warmer:
pc1:w:<plan_ref_32_hex>:<task_ref_32_hex>:<nonce_32_hex>

Controlled demand:
pc1:d:<run_ref_32_hex>:<operation_id_first_32_hex>:<operation_id_last_32_hex>
```

- The private convention is exactly 104 ASCII bytes. NFS protocol tags are not globally limited to 128 bytes; Chimera copies at most 256 bytes for bounded event handling and records original length/truncation.
- `plan_ref`, `task_ref` and `run_ref` are the first 32 hex characters of SHA-256 over their full IDs.
- Warmer nonce is fresh for every COMPOUND.
- Controlled-demand `operation_id` is canonical 64 lowercase hex and is split into two 32-hex components. The manifest's human `run_id` remains unchanged; only the tag uses `run_ref`.
- The tag is client-asserted and spoofable. It supplies join/loop-suppression evidence, not authenticated origin.
- Chimera emits `origin_claim` and `origin_trust=client_asserted`; the Controller applies trusted allowlist/run-manifest policy. It must not silently convert every untagged NFS READ into foreground demand.
- Tags containing controls/non-renderable bytes are encoded/escaped for logs and never rendered with an unbounded string format.

**Executable model/planner task v2:**

```json
{
  "schema_version": 2,
  "run_id": "run-001",
  "plan_id": "plan-001",
  "task_id": "task-001",
  "created_at": "<UTC>",
  "model_version": "logreg-v1@<package-id>",
  "prediction_time": "<UTC>",
  "deadline": "<UTC>",
  "score": 0.82,
  "volume_id": "<mount-id>",
  "file_id": "<opaque sha256 identity>",
  "export_name": "test-ro",
  "export_id": 7,
  "target_fh_b64": "<wrapped NFSv4 handle>",
  "target_valid_until": "<optional UTC>",
  "offset": 0,
  "length": 4096,
  "max_bytes": 4096,
  "origin_role": "w"
}
```

The model ranks `volume_id`/`file_id`; the trusted planner joins the restricted executable target, applies budget/deadline/export checks and writes the private plan. Endpoint, NFS version and AUTH_SYS/security profile remain dispatcher configuration, not model-controlled data.

For the MVP, target TTL is at most 300 seconds. A plan must be atomically written into a dispatcher-owned directory mode `0700`; task files are mode `0600` and ownership is verified. That channel provenance is the accepted attestation—there is no caller-supplied trusted flag. Unknown schema versions, extra/trailing fields or ownership/mode mismatch are rejected.

**Warmer reconstruction:**

- Decode and validate the bounded wire handle.
- Trust the private owner-only dispatcher task as the MVP attestation that `volume_id`, export, `file_id` and `target_fh_b64` belong together. The Warmer cannot independently derive or verify that binding from an opaque handle.
- Verify task schema, deadline/expiry, budget and configured endpoint/security profile.
- Use the dedicated least-privileged AUTH_SYS service identity `1002:1002`; never reuse or impersonate client/session identity from the event.
- Revalidate that the handle refers to a regular file and that current size covers the range; reject directories and special files.
- Issue PUTFH → OPEN(CLAIM_FH) → PUTFH/READ → CLOSE with structured tags.
- Fail closed on BADHANDLE, STALE, FHEXPIRED, WRONGSEC, malformed handle, expired target or task/config mismatch.
- Treat removed export, unavailable volume and revoked permissions as terminal for that task; no indefinite retries.
- Return actual bytes/status/timing; never infer cache residency.

This is an explicitly accepted MVP trust boundary, not cryptographic or server-side target-binding verification. Production use requires a signed/verifiable resolver or a server-side binding check.

**MVP queue ownership decision:** use Option 1. The Warmer is a single-task executor: it opens one explicit validated owner-only task file, executes one bounded request, and writes one result. It does not claim, rename, delete or deduplicate task files and does not implement leases or crash recovery. The trusted dispatcher owns atomic claiming, duplicate suppression, retries, restart recovery, global rate/byte limits and retention. For the PoC, task and result files remain as private evidence; invoking the same task twice is a dispatcher/operator error, not something the Warmer claims to prevent.

**Dispatcher issue 1 resolved — atomic plan claim:** use Linux `renameat2(..., RENAME_NOREPLACE)` within the same owner-only filesystem. Exactly one claimant succeeds; `EEXIST` means another dispatcher already claimed the plan. Fail closed when `renameat2`/`RENAME_NOREPLACE` is unavailable, the source/destination cross filesystems, or the filesystem does not support the guarantee. Do not fall back to overwrite-capable rename, a caller-trusted single-dispatcher assumption, or a lock-file protocol.

**Dispatcher issue 2 resolved — ambiguous crash state:** use durable owner-only files as the MVP source of truth, never CUFS volume Kvrocks. Before starting a Warmer process, atomically write and fsync `state/<plan_id>/<task_id>.json` with `state=running`, attempt/dispatcher identity, start time and process evidence. Terminal records are written atomically under `results/<plan_id>/<task_id>.json`. On restart, `running` without a terminal result becomes `ambiguous` with `automatic_retry_allowed=false`; it is never retried automatically. An operator may explicitly requeue it. Every transition uses temporary file, file fsync, atomic same-directory rename and directory fsync. Session/dedicated Kvrocks may replace this storage later only after its isolation and atomicity are validated.

**Dispatcher issue 3 resolved — stop control:** use a persistent operator-owned stop file. While it exists, the dispatcher starts no new tasks; SIGTERM applies the same stop-new-work behavior. An already-running Warmer may finish or reach its timeout, and all remaining plan items are recorded `cancelled`. The dispatcher never removes the stop file automatically. Only the operator/control plane may remove it to resume work, and the stopped state survives dispatcher restart.

**Dispatcher issue 4 resolved — process lifetime:** launch each Warmer CLI in its own process group with Linux `PR_SET_PDEATHSIG=SIGTERM` set before exec. The child pre-exec hook must explicitly reset inherited `SIGTERM` and `SIGINT` dispositions to `SIG_DFL`; `Popen(restore_signals=True)` does not guarantee this, and inherited `SIG_IGN` could lose a stop before Python installs handlers. After `prctl`, verify the expected parent still exists to close the parent-already-died race. Graceful dispatcher shutdown signals the process group; unexpected dispatcher death signals the CLI, whose internal NFS worker is already parent-bound. The task is still marked `ambiguous` when no terminal result exists because a submitted RPC may already have reached Chimera. Restart recovery must not scan for and kill a reused PID.

**Dispatcher issue 5 resolved — real-read enablement:** the dispatcher uses a two-key gate. Real NFS reads require both configuration `real_reads=true` and an operator-owned enable file. Any other combination uses the fake reader. Real mode also fails closed unless `max_bytes_per_min`, `max_items_per_min`, one in-flight task, task deadline/max-bytes and persistent stop control are configured. The Warmer does not interpret these switches; the dispatcher decides whether to launch it. Removing the enable file prevents new tasks but cannot recall an already submitted RPC.

**Dispatcher issue 6 resolved — retries:** the MVP performs zero automatic retries. Timeout, partial read, stale/expired/bad handle, permission/security failure or server error produces a terminal result. Another attempt requires a new explicit task with a new `task_id`. Plan and global budgets count requested bytes once per task, with no hidden retry allowance. This preserves experiment accounting and avoids accidental duplicate backend work.

**Dispatcher issue 7 resolved — pressure pause deferred:** a separate temporary pressure-pause file and automatic metric-based pausing are out of MVP scope. The bounded MVP already uses one in-flight task, byte/item limits, per-task deadline/budget, two-key real-read enablement and a persistent stop file. Add automated pressure control only after foreground-latency/cache-headroom signals and thresholds are validated.

**Dispatcher issue 8 resolved — bounded crash-preserved Warmer output:** stream Warmer stdout line-by-line into an owner-only per-attempt log, validate/parse incrementally and fsync every accepted line. Limits are 5,000 lines, 16 KiB per line, 16 MiB stdout total and 1 MiB stderr. Exceeding a framing/size limit terminates the Warmer process group. Classify the task `failed` only when no READ could have been admitted; otherwise preserve `ambiguous` when in-flight execution cannot be excluded. Write the terminal task result atomically as a separate record; preserve partial logs across dispatcher crash.

**Dispatcher issue 9 resolved — terminal result agreement:** require exactly one valid terminal JSON result and a matching process exit code. `completed` requires exit 0; `partial`, `error` and `timeout` require exit 1; `cancelled` requires exit 130 or 143; invalid input may exit 2 without a terminal result. Missing, duplicate, malformed or contradictory terminal output becomes `ambiguous` when any READ attempt was admitted, otherwise `failed` with exact zero bytes. The dispatcher never trusts only JSON or only the exit code.

**Dispatcher issue 10 resolved — initial global limits:** configure one in-flight task, `max_items_per_min=60` and `max_bytes_per_min=64 MiB`; retain the Warmer's per-task maximum of 16 MiB and task deadline. These are configurable PoC safety defaults, not measured safe-capacity or performance targets, and must be revisited after controlled pressure tests.

**Dispatcher issue 11 resolved — rolling limit accounting:** enforce global item/byte limits over the previous rolling 60 seconds using durable task-start records. Charge one item and the task's requested `length` immediately before launch. Never refund budget for completed, partial, failed, cancelled or ambiguous outcomes because the speculative operation may already have consumed resources. Do not use fixed wall-clock minute counters or a burst-permitting full-capacity token bucket.

**Dispatcher issue 12 resolved — rate-limit waiting:** preserve plan order and wait interruptibly until rolling-window capacity becomes available. Start the task only if capacity opens before its deadline; otherwise record terminal `skipped` with reason `rate_limit_deadline`. Do not reorder later smaller tasks ahead of it. Waiting must react immediately to the stop file, SIGTERM, enable-file removal and task deadline.

**Dispatcher issue 13 resolved — evidence retention:** perform no automatic deletion during the MVP. Preserve claimed plans, owner-only task files, running/ambiguous state, bounded Warmer stdout/stderr logs and terminal results until the experiment manifest is finalized and reviewed. Cleanup is an explicit, auditable, per-run operator action; the dispatcher does not automatically prune successful or failed evidence.

**Dispatcher issue 14 resolved — fake mode result:** when real reads are disabled, validate and claim the task but do not invoke the Warmer. Write terminal `status=skipped`, `reason=dry_run`, preserve requested bytes, set actual bytes to zero and `bytes_read_is_lower_bound=false`. Never report fake execution as `completed` or include it in completed-byte metrics.

**Dispatcher issue 15 resolved — invalid plan versus invalid item:** reject the entire plan only for plan-level schema/integrity errors such as invalid JSON/version, duplicate task IDs, invalid budget/deadline or untrusted ownership/mode. A stale, expired, malformed, unauthorized or out-of-range individual item receives an explicit terminal `skipped` result with a precise reason, and later valid items continue in plan order. Never silently omit an item.

**Dispatcher issue 16 resolved — durable identity:** use `(run_id, plan_id, task_id)` as the canonical unique identity in state, logs and results. Store evidence under `runs/<run_id>/plans/<plan_id>/...`; require `task_id` unique within a plan and never overwrite existing state/result for the same tuple. Reuse of a plan/task name in another run remains a distinct experiment record.

**Dispatcher issue 17 resolved — plan continuation:** continue later tasks after item-specific terminal outcomes such as stale/expired target, permission denial, partial/EOF or invalid target. Stop the plan and record remaining items `cancelled` after systemic or ambiguous outcomes such as transport/server unavailability, task/result contract mismatch, ambiguous execution, dispatcher/Warmer invariant failure, stop file, enable removal or shutdown signal. Maintain an explicit tested classification table.

**Dispatcher issue 18 resolved — explicit requeue lineage:** never clear or reuse the durable identity of a failed or ambiguous task. An operator/planner retry creates a new `task_id` and includes a `supersedes` reference to the original `(run_id, plan_id, task_id)`. Preserve the original state, logs and result immutably. Reject supersedes cycles, missing referenced identities and reuse/collision of the new tuple.

**Warmer/dispatcher issue 19 resolved — task-load timeout before parsing:** when bounded task-file loading times out or is cancelled before parsing and before any worker/NFS execution starts, the Warmer returns a distinct pre-validation signal/exit rather than a canonical task result with null identity. The dispatcher uses its trusted task metadata to synthesize the canonical terminal result with exact run/plan/task IDs and requested bytes, `bytes_read=0`, `bytes_read_is_lower_bound=false` and `execution_started=false`. If the Warmer cannot prove execution never started, the task remains `ambiguous`.

### V2 rollout result — event delivery gate failed

The corrected v2 image was loaded and Helm revision 4 reached all-three-pod readiness. CUFS/config/binary checks, volume/exports, temporary file/directory CRUD/cleanup, GETATTR and initial zero-drop gates passed. The direct controlled 4 KiB read and checksum also passed, but no matching successful `test-ro` v2 event was found. One v2 pod reported `log` drops 1 and `queue` drops 0; another reported 0/0, and the third had already been replaced.

The root cause was reproduced after rollback: a valid empty NFS COMPOUND tag causes `nfs4_read_events_base64url()` to return encoded length 0, while the worker interprets any zero return as encoding failure, increments the shared `log` drop and skips the event before calling the logger. Existing tests covered nonempty/truncated tags but not `tag.len=0`. Thus this controlled event was deterministically lost in encoding, not proven lost by the general logger. The shared counter still made the live diagnosis ambiguous until reproduction.

Automatic rollback was initiated; no Warmer handle-mode live run is permitted from this rollout. Required fixes are: treat zero-length tags as successful empty encoding, split encoding versus sink failure counters, and add independent tests for empty/nonempty/max tags and forced sink rejection. General nonblocking INFO logging can still reject events for independent reasons, so it remains diagnostic rather than the sole training transport. The active direction is a dedicated owner-only, configurable, off-foreground JSONL sink written by the event worker, with foreground queue remaining nonblocking and explicit encode/sink I/O/error/drop accounting. A corrected sink must pass deterministic retrieval and restart/rotation/retention tests before another deployment.

### Corrected sink rollout and one-event gate passed

The final race-proof reliable-sink image was deployed with per-node HostPath storage. Helm revision 7 is deployed; the Chimera DaemonSet reached 3/3 updated and Ready with zero observed pod restarts at the status check. Runtime/CUFS ABI, volume/export, temporary file/directory CRUD/cleanup, GETATTR and direct aligned 4 KiB checksum gates passed. CUFS attempted io_uring, received EPERM and initialized successfully with plain TCP, which is within the accepted environment boundary.

One controlled empty-tag NFSv4.1 READ produced exactly one parseable authoritative JSONL event. Its producer epoch matched the live epoch; ordered enqueued/synced state, queue depth, worker-in-flight, permanent `delivery_clean`, barrier and zero authoritative queue/encode/sink/rollback/rotation drops all passed before and after the read. No rollback or Warmer run occurred. Private rollout evidence is preserved on nas-vm under `<private-evidence-root>/chimera-v2-rollout`.

The event-transport deployment gate is therefore complete. Remaining collection gates are target-specific fixture/catalog discovery, final harness/provider and importer artifacts, frozen manifest, and combined dry-run/reconciliation.

**Event-sink storage decision:** use a per-node `hostPath` for the disposable MVP, not PVCs. Keep the existing single Chimera DaemonSet. Mount node path `/var/lib/chimera-read-events` into only the Chimera container at `/events`; configure sink `/events/nfs4-read-events.jsonl`. A startup/init step must verify a real directory and set owner/mode `0700`; sink files remain `0600`. The 64 MiB active file plus one backup bounds in-process retention because hostPath has no Kubernetes quota. Data survives pod replacement on the same Kind node but not node deletion/recreation, is node-local inside each Kind node's Podman volume, and requires authorized per-node/pod collection and explicit cleanup. Rollback removes config/mount but does not delete retained event data without separate approval.

**Event-sink live durability gate:** a visible JSONL line is not sufficient because the write occurs before `fdatasync`. After each successful sync, the worker must publish an atomic `last_synced_sequence` watermark and synced-event count; it must also expose queue depth/pending count. A live event is accepted only when its `producer_sequence <= last_synced_sequence`, queue depth is zero, and queue/encode/sink drop counters remain zero. Sync failure must not advance the watermark. These metrics form the explicit live flush/drain barrier; normal teardown draining alone is insufficient.

**Read-only execution decision:** every dispatcher/Warmer task is an NFS READ. The task schema does not accept WRITE or content mutation. A model may eventually predict future write demand, but the planner may convert that only into a separate pre-read when controlled evidence shows that CUFS read-modify-write behavior benefits; the executor still issues READ. NFS WRITE is not “basically a read”: it changes file data/metadata and is outside the warming safety boundary.

Atomic claiming remains required for read-only work. Duplicate reads still consume plan byte/rate budgets, backend bandwidth, cache capacity and client/server resources; they can evict useful data, contaminate demand/telemetry and invalidate experiment accounting. The claim rule protects scheduling and evidence, not merely file contents.

**Parallel ownership:** other engineers own model scoring and planner policy. Engineer 1 owns the Chimera event producer, target semantics, plan-file dispatcher, Warmer execution and live filesystem validation. The shared acceptance test is: a model/planner-produced schema-v2 plan is claimed by Engineer 1's dispatcher, produces the exact owner-only Warmer task contract, and executes unchanged in the Warmer.

Tracked `docs/ml/spec.md` §8.10 is the starting dispatcher design, but the reviewed v2 contract supersedes its older mutable-path target and any confident origin wording. Real reads remain disabled by default and require operator enable plus enable file, global byte/item limits, stop/pressure control and protocol/performance feasibility signoff. V2 uses wrapped-handle targets and client-asserted origin evidence; it does not claim origin authentication.

The current v1 rollout is rejected for model use. V2 implementation and tests are active; no v2 image is yet built or deployed.

### 18.10 Related work visible in the latest repository

Read-only inspection of current `main`/`origin/main` `f193f114f744c625435acf002e8fd1d21d055fcb` found:

| Area | Merged/visible work | Implementation status |
|---|---|---|
| Synthetic datagen | `src/predictive_cache/datagen/` and `tests/datagen/`; recent feature commits from the data engineer | Implemented and merged. This supplies synthetic plumbing data, not a live demand source. |
| Data preparation | `src/predictive_cache/prep/` and `tests/prep/`, merged by PR #80 / commit `1f3a5fe` | Implemented cleaning pipeline: strict schema validation, origin/failure/range exclusion, dedupe, handle reconstruction, open ranges, coverage/loss accounting, reports and `pcache-prep`. Feature/snapshot stages remain separate future work. |
| Model package/scorer | `src/predictive_cache/ml/{scorer,package,dummy,hashing}.py`, merged across PRs #72+ | Implemented: scorer registry/protocol, immutable package manifest/loader, serving-contract hash, serving-policy validation and smoke-score verification. |
| Evaluation/baselines | `src/predictive_cache/ml/{metrics,baselines}.py`, PRs #74/#75 | Implemented: MRU/frequency/hot-set/co-access scorers, byte/top-k selection, recall/precision/coverage/PR-AUC/calibration/bootstrap-related primitives. |
| Training/ClearML | `src/predictive_cache/ml/{logreg,train,train_cli,snapshots,fixtures}.py`, tracking helpers and ClearML adapters, PR #76 plus ClearML datagen work | Implemented: regularized logistic-regression training on train/val only, model/package output, serving-threshold selection, CLI and optional ClearML tracking. |
| Controller/planner runtime | Detailed `docs/ml/spec.md` §8 | No `src/predictive_cache/controller` implementation is present on current main. Planner/Controller work remains a parallel handoff; do not duplicate it. |
| Warmer | `docs/manual-warmer.md` merged by PR #41 | Main contains documentation only. The active implementation exists in the coordinated Warmer worktree/checkpoint, not main. |
| Warmer checkpoint evidence | Copilot checkpoint `d600dbe...` contains `src/predictive_cache/warmer/{cli,execution,limits,nfs41}.py` and four Warmer test files | Substantial unmerged implementation/checkpoint, including task-contract tests; it must not be mistaken for merged main. |
| Dispatcher | Defined in `docs/ml/spec.md` §8.10 | No dispatcher source is merged on main. Engineer 1's implementation/review work remains uncommitted and must integrate against current main. |

Repository `pyproject.toml` now exposes `pcache-datagen`, ClearML datagen, `pcache-model` and `pcache-train`; optional dependencies include `ml` and `clearml`, and pytest discovery covers `tests/datagen` plus `tests/ml`. Warmer/dispatcher integration must preserve these scripts, extras, lockfile entries and test selection.

### 18.11 Reoriented integration plan after merged ML work

The merged model lane changes the critical path:

1. **Do not implement another scorer/trainer/package format.** Consume the existing `LoadedPackage`, `Scorer` and serving-policy contract.
2. **Align task selection with implemented serving policy:** default horizon 300 s, tick 60 s, recent skip 300 s, cooldown 1,800 s, `max_items=20`, plan `max_bytes=256 MiB` and per-item `range_cap<=16 MiB`. Dispatcher safety limits remain stricter at one in-flight, 60 items/min and 64 MiB/min; excess plan work waits or expires.
3. **Feed the existing key contract:** model rows are keyed by `volume_id`, `file_id`, `t`. Chimera v2 canonical `volume_id`/`file_id` must map deterministically into the eventual live feature store.
4. **Preserve event-schema compatibility:** strict datagen schema remains the fixture contract. A live normalization adapter must map Chimera v2 to the shared event schema or a reviewed next version without weakening origin, known-at or range semantics.
5. **Planner handoff, not model duplication:** future Controller/planner uses merged model package/serving policy to produce schema-v2 plans. Engineer 1's dispatcher consumes those plans and creates the owner-only handle task.
6. **Integration tests:** use the merged dummy/model package fixtures to generate representative scores/serving policy, then a contract fixture plan; prove dispatcher/Warmer acceptance without importing model internals into the executor.
7. **Validation environments:** latest-main ML tests requiring scikit-learn/ClearML may skip in the current offline Warmer environment; record skips and use already-provisioned compatible environments only. Do not install or alter registries.
8. **Use merged `pcache-prep` rather than a parallel cleaner:** implement one Chimera JSONL source adapter that outputs the strict datagen-compatible dataset/manifest consumed by `pcache-prep clean`.
9. **Supply required context explicitly:** Chimera READ does not provide parent directory identity, so the controlled run must join a frozen fixture catalog keyed by canonical `volume_id`/`file_id`; missing mapping is a reported exclusion, never fabricated `dir_id`.
10. **Separate request, sink-attempt and durable-availability time:** Chimera v2 `event_time_ns` is request time; `sink_time_ns` is captured before serialization/write/sync and remains diagnostic only. The collector's post-read, barrier-accepted `durable_at` is the authoritative value mapped to dataset `ingest_time` and Controller `known_at`. Do not require `sink_time_ns >= event_time_ns` because wall clocks can adjust.

## 19. Engineer 1 experiment ownership

Engineer 1 also owns:

1. The data-collection experiment that determines whether Chimera v2 events are suitable to train/evaluate the model.
2. The controlled experiment comparing Strider with predictive warming disabled versus enabled.

### 19.1 Framework selection and evidence boundary

Load-bearing methods are experiment design, statistical validity and counter-metric/guardrail design. Product-retention and North-Star frameworks are not load-bearing for this filesystem PoC. Current thresholds have no internal experimental baseline and are therefore `[EVIDENCE-LIMITED: T6]` until pilot variance and real telemetry are available. Overall experiment-design confidence is **M**: the causal structure is clear, but cold-state equivalence, cache telemetry and representative workload access are unresolved.

### 19.2 Experiment A — training-event collection validity

**Question:** Can `nfs4_read_v2` produce a complete-enough, correctly identified and correctly attributed demand dataset for model training?

| Field | Pre-registered design |
|---|---|
| Hypothesis | In a controlled NFSv4.1 workload with known offered operations, Chimera v2 emits one normalized event per first-executed READ, with stable volume/file identity, exact range/outcome and visible loss accounting. **Confidence M; T6 until run.** |
| Unit | One first-executed NFSv4.1 READ operation identified by `{session, slot, sequence, op_index}`. |
| Primary metric | Eligible event capture rate = valid normalized events / known offered first-executed READs. |
| Counter-metric | Foreground READ result/error parity and latency overhead versus event collection disabled. |
| Data-quality guardrails | Duplicate rate, missing identity rate, invalid range rate, unknown-origin share, queue/log drops, out-of-order rate, replay-duplicate rate. |
| Workload | Deterministic controlled files/ranges plus representative authorized workload if available. Include short reads, EOF, failures, multi-READ compounds, replay and node failover. |
| Decision rule | Plumbing passes only if every deterministic offered READ is reconciled or explicitly accounted as a visible drop/exclusion, with no READ-result change. This does not establish representative model quality. |

Goodhart vulnerability: optimizing capture rate could increase blocking or emit duplicates. Pair capture with foreground latency/errors and unique operation reconciliation. Observation-to-action cascade: **capture gap (T1 after run)** → incomplete negatives/labels → protocol owner fixes identity/loss path before model ingestion → confidence **H after deterministic reconciliation** → watch offered/emitted/dropped/duplicate counters.

Required outputs:

- Immutable event/schema/build/run manifest.
- Offered-operation ledger and normalized event ledger.
- Per-stage loss and exclusion reconciliation.
- Identity stability checks across aliases, pods, file-handle remint and allowed failover.
- Origin evidence report separating trusted, client-asserted and unknown.
- Privacy/access/retention record for handles and private plans.

The first five-hour collection is reclassified as a **controlled synthetic pipeline dataset**, not representative production-demand history. The run manifest sets `data_kind=synthetic`; the controlled demand generator may map its exact allowlisted client/tag evidence to event `origin=foreground` for exercising cleaning/training, while untrusted/untagged events remain `unknown`. This follows the repository rule that generated foreground rows can test plumbing but cannot support real-workload model-quality or performance claims.

An owned Chimera event adapter must produce the existing dataset layout:

- strict `events` rows from v2 JSONL;
- hourly `loss_counters` from offered ledger plus synced/drop checkpoints;
- catalog volumes/directories/files/file paths from the frozen fixture catalog;
- deterministic manifest/checksums/source image/schema/run provenance and known gaps.

Dataset finalization fails on unexplained offered-versus-synced discrepancy, identity conflict, missing required catalog mapping or counter epoch ambiguity. The merged `pcache-prep clean` then performs origin/range/result exclusion and generates cleaned events, opens, ranges, coverage and quality reports.

### 19.3 Experiment B — predictive cache versus control

**Preferred design:** paired randomized switchback/matched trials on the same `nasuni-cluster2`, not an unadjusted comparison of two independently configured clusters. Same-cluster switchback reduces hardware, chart, network and CUFS-version confounding. A second cluster can provide reproduction, not the primary causal estimate.

| Field | Pre-registered design |
|---|---|
| Hypothesis | For matched cold file ranges selected under a fixed byte budget, predictive warming reduces client-observed first-range completion time without violating foreground or cache-pollution guardrails. **Confidence L; T6 until cold-state and telemetry gates pass.** |
| Treatments | Control: predictive warming disabled. Treatment: frozen model/planner emits plan, dispatcher executes bounded Warmer task before the same foreground workload. Include static/MRU baseline if available. |
| Randomization unit | Matched file-range trial block; treatment order balanced/randomized within blocks. Avoid concurrent treatment/control on the same range. |
| Primary metric | Client-observed time from foreground READ start to completion of the declared initial byte range. |
| Secondary metrics | First usable byte, later foreground overlap with warmed bytes, completed warm bytes, remote/fault proxy where validated. |
| Guardrails | Foreground p95/p99 latency for unrelated reads, foreground errors, warmer bytes/rate, cache pressure/evictions, wasted warmed bytes, CPU/memory/network, event loss. |
| Counter-metric | Useful warmed-byte ratio paired with total warmed bytes/cache pollution; latency gain paired with unrelated foreground regression. |
| Cold-state gate | Each trial requires equivalent independently verified cold state without warming the measured range. Failed or uncertain resets invalidate the trial. |
| Sample size | Run a small non-decision pilot to estimate paired variance, then compute required N for a pre-declared MDE at α=0.05 and power=0.80. Do not retain the old “30 trials” suggestion as a statistical guarantee. |
| Decision rule | Claim benefit only if the pre-declared primary effect clears its confidence interval/MDE and every guardrail holds. Otherwise report fail or inconclusive. |

Goodhart vulnerability: warming more bytes can improve selected-file latency while polluting cache and harming unrelated users. The experiment therefore fixes a byte budget and reports latency, useful bytes, wasted bytes and unrelated foreground behavior together.

### 19.4 Instrumentation feasibility

| Signal | Current availability | Status |
|---|---|---|
| Chimera v2 operation/range/outcome | Under implementation | Needs work |
| Stable volume/file identity and executable target | Feasible; v2 implementation active | Needs work |
| Trusted origin | Client-asserted tags plus dispatcher evidence only | Limited; do not treat as authenticated |
| Offered/emitted/drop reconciliation | Queue/log counters designed | Needs live validation |
| Client foreground latency | Available from controlled client harness | Ready for fixtures |
| CUFS per-file fault/cache state | Not established | Blocked |
| Aggregate VFS/CUFS counters | Available but can reset and lack attribution | Diagnostic only |
| Equivalent cold reset | Not established | Blocking dependency |
| Unrelated foreground workload | Must be defined and stabilized | Needs work |

### 19.5 Assumption registry

| Assumption | Confidence | Evidence | Invalidation |
|---|---|---|---|
| Wrapped handle and canonical inner-FH identity refer to the same file during task TTL | M | Historical source analysis (T3) | Failover/key/export change breaks handle or maps different object |
| Controlled cold reset produces equivalent treatment/control state | L `[EVIDENCE-LIMITED]` | No verified procedure (T6) | Pre-read probes or backend counters differ across matched trials |
| Client latency improvement represents CUFS benefit rather than client cache | L `[EVIDENCE-LIMITED]` | Prior reads can use client cache (T1 observation) | Independent client still sees no backend/fault difference |
| Planner selections can be replayed under a fixed byte budget | M | Concrete plan specification (T3) | Model/planner output violates task contract or target is stale |

### 19.6 Adversarial self-critique and revision triggers

- **Identity looks stable but is deployment-scoped:** a file-handle key/export change can invalidate targets. Watch BADHANDLE/STALE/FHEXPIRED and revise resolver policy on any unexpected rate.
- **Selected-file speedup can hide system harm:** unrelated foreground regression or cache eviction invalidates success even if primary latency improves.
- **Synthetic/control traffic can overstate model quality:** no model-quality claim until authorized representative demand is collected with adequate origin/coverage.
- **Counter resets can create false deltas:** never subtract counters across reset; require epoch/reset-aware analysis.

Revise the experiment when event identity/origin semantics change, cold reset cannot be reproduced, telemetry lacks required attribution, model/planner task schema changes, guardrails fail, or another engineer cannot reproduce the run from the frozen manifest.

### 19.7 Data-collection start checklist

Collection must not start until every hard gate below passes:

1. **Correct sink sequencing:** assign producer sequence in enqueue order; a synced watermark cannot leap over an unsynced event.
2. **Observable quiescence:** expose `last_enqueued_sequence`, `last_synced_sequence`, queue depth and worker-in-flight. Acceptance requires enqueued equals synced, queue depth 0, in-flight 0 and zero queue/encode/sink/rollback/rotation drops.
3. **Hardened sink review PASS:** regular-file/owner/link checks, no FIFO/device/symlink, partial-write rollback, durable rotation, path validation, empty-tag handling and fault-injection tests.
4. **Rebuilt immutable image:** corrected reviewed patch built on nas-vm with all historical ABI, startup, CUFS and CRUD gates; exact image/archive hashes recorded.
5. **HostPath chart ready:** existing DaemonSet only; per-node `/var/lib/chimera-read-events` preflighted as safe real directories; init secures mode 0700; Chimera writes mode 0600 at `/events/nfs4-read-events.jsonl`; rollback preserves files.
6. **Single-event live durability proof:** one controlled NFSv4.1 READ produces exactly one matching parseable v2 JSONL record; its producer sequence reaches the synced watermark; no duplicate or drop; cluster/read behavior remains healthy.
7. **Collection workload frozen:** duration, offered READ rate/count, file/range distribution, client/session/tag convention, concurrency, workload seed, fixture catalog and exact software/cluster versions are written into an immutable run manifest.
8. **Origin/provenance declared:** Warmer disabled; controlled traffic marked as controlled/synthetic or client-asserted, not authenticated production foreground. Five-hour controlled traffic is plumbing/training-pipeline evidence, not representative model-quality evidence.
9. **Offered-operation ledger ready:** harness records operation ID inputs, UTC/monotonic times, file/range, expected result and client evidence before issuing each READ; ledger writes are bounded and durable.
10. **Checkpoint and stop rules frozen:** every 15 minutes capture per-pod last enqueued/synced, queue/in-flight/drop counters, file sizes/rotation, pod UID/restarts/epoch and cluster health. Stop on any delivery drop, unaccounted restart/epoch, sink fault, storage limit, foreground error/regression or identity mismatch.
11. **Collection/retention path ready:** authorized per-node extraction into owner-only storage, checksums and manifest; sufficient node space; explicit cleanup owner and no secret/file-content collection.

Warmer and dispatcher completion are **not blockers** for the initial Warmer-disabled event collection. They are required later for treatment execution and end-to-end predictive-cache testing.

**Collection identity bridge:** raw Chimera JSONL does not include Kubernetes pod UID. The collector writes a trusted source-file envelope manifest mapping each raw file path/checksum to `pod_name`, `pod_uid`, `node_name` and exact `producer_epoch`. `producer_epoch` is preserved end-to-end as the canonical lowercase UUID string emitted by Chimera; it is never coerced or hashed to an integer. The importer attaches pod identity from that envelope and requires every raw line's epoch to match; the canonical source join is `(local_id, pod_uid, producer_epoch)`. Fixture catalog `volume_id` is exactly 32 lowercase hex, `file_id` exactly 64 lowercase hex, `export_id` a nonnegative integer, and decoded `export_name` must match the run catalog. The catalog join key is `(volume_id, file_id)`; missing or conflicting export/catalog identity fails reconciliation rather than being fabricated.

**Sequence convention:** Chimera accepted-event `producer_sequence` is 1-based; the first event is sequence 1. Within one producer epoch, a contiguous fully synced stream has `synced_count == last_synced_sequence`. The zero-event state uses the exact zero sentinel exported by Chimera and `synced_count=0`. Harness, collector and importer must not apply the earlier zero-based `last+1` rule.

**Collection read-path decision:** do not use ordinary buffered `pread` for the controlled offered workload. Client page cache/readahead can satisfy later non-overlapping reads without a Chimera READ, breaking exact offered-to-event reconciliation. The live harness uses a validated fail-closed direct-I/O executor (`O_DIRECT`, aligned 4 KiB buffers, no buffered fallback) and only the five manifest-declared nonoverlapping 4 KiB offsets per 64 KiB fixture: `0`, `12288`, `24576`, `36864`, `49152`. Any unsupported direct-I/O, alignment error, short read or checksum mismatch stops the run. Before every offered read, a lightweight collector/health probe must confirm current epoch/barrier/drop/storage/cluster/export health; full checkpoints remain at start, every 900 seconds and final.

Direct-I/O preflight passed on healthy `kind-nasuni-cluster2` revision 6: UID/GID `1002:1002`, read-only NFSv4.1 `test-ro` mount, one aligned `O_DIRECT` 4,096-byte `preadv`, expected checksum `e00e153a038f7017f129fbca1d00e69652e39e4befab87f6e0e2e31f233f32d9`, aggregate Chimera VFS Read counter `+1`, no buffered fallback, clean unmount and healthy before/after resources. This validates the selected executor mechanism, not the event sink or full collection.

### 19.8 Dispatcher and Warmer combined handoff

The latest-main dispatcher+Warmer integration is frozen, reviewed and uncommitted/unpushed:

- Base: `f193f114f744c625435acf002e8fd1d21d055fcb`.
- Result tree: `b5c7116e248b17300e144a6caabac0fa7a1398bc`.
- Patch SHA-256: `d1e78b813030506e3a03eede9b8242583be8d93c3ff2107e3a78bcb271f2f617`.
- Stable patch ID: `9803695483d6d4e63020267a73411d8d83400be7`.
- Manifest SHA-256: `271e6b317a4325ee52411af246a55f5e123bcda67de2633d4ce60435ee0dd10a`.
- Thirty changed files; alternate-index apply reproduced the exact tree.
- Owner-only nas-vm handoff: `<private-evidence-root>/dispatcher-warmer-handoff`.

Combined validation passed 943 tests with one expected sklearn skip, Ruff and offline lock validation. The integrated patch preserves latest-main ML/ClearML/datagen work, adds the reviewed Warmer, implements all resolved dispatcher decisions, resets inherited signals before exec, handles `pre_validation_result`, and aligns hashed plan/task references and actual Warmer reason/exit semantics. No live handle execution, commit or push occurred.
