# Contracts index

One page listing every interface between components, where its
authoritative definition lives, and what is still open. Change a contract
by PR to its source document. Update this index in the same PR.

```text
Chimera ─nfs4_read_v2─▶ import / live ingest ─▶ access event v1 ─▶ clean ─▶ feature code ─fv1─▶ model ─▶ planner
datagen ─────────────────────────────────────▶ access event v1 ─▶ clean ─▶ feature code ─fv1─▶ snapshots ─▶ train ─▶ package
planner ─(file_id → target, C5)─▶ plan v2 ─C3─▶ dispatcher ─task v2─▶ Warmer ─NFSv4.1 READ (C4)─▶ Chimera ─▶ results ─▶ Controller
```

| # | Contract | Between | Authoritative source | Status | Open work |
| --- | --- | --- | --- | --- | --- |
| C1 | Access event v1 (+ Chimera raw `nfs4_read_v2`) | Chimera, datagen → cleaning → Controller ingest | [datagen spec §5.1](../datagen/spec.md); raw: [`contracts/nfs4-read-v2.schema.json`](../../contracts/nfs4-read-v2.schema.json), [Chimera adapter](../data-prep/chimera-v2-adapter.md) | Schema defined; both sources produce v1 | Event **shape** differs: #104 decided a versioned READ-only observable view (`read-demand-v1`), with no reads-to-opens coalescing. Live transport: per-pod read shipper tails the Chimera JSONL sink and `POST /v1/events` with an `nfs4_read_v2` envelope ([ML spec §8.1.1](../ml/spec.md)), to be built in #59. Trusted foreground origin still open (#34). |
| C2 | Feature rows (fv1), snapshots, model package | data prep (training) ↔ Controller (inference) | [data-prep spec §5, §8](../data-prep/spec.md); [ML spec §3, §5](../ml/spec.md); `feature_schema.json` in every package | Defined; package format, loader and training/eval implemented | Feature store and snapshot builder not built (#30–#32). READ profile `read-demand-fv1`, READ labels and package/snapshot semantic hashes are defined and implemented (PR #107, [read-demand-v1](../data-prep/read-demand-v1.md)). READ co-read table, candidates and anchor: [read-demand-v1 § Co-read table and candidates](../data-prep/read-demand-v1.md#co-read-table-and-candidates), to be built in #31. READ snapshot builder (#114) implements these for training; models are downloaded and run locally ([run-model-locally](../ml/run-model-locally.md)). |
| C3 | Warm plan / task / result v2 | Controller → dispatcher → Warmer → results | [docs/dispatcher.md](../dispatcher.md) and `predictive_cache.dispatcher.schema`; summary in [ML spec §8.4–§8.5, §8.10](../ml/spec.md) | Dispatcher implemented (PR #84); frozen schedule producer and batch [Pass-2 bridge](../controller/pass2-runtime-bridge.md) implemented | Run the combined file contracts in an approved Pass-2 dry run. Pressure pause and retention remain #63. |
| C4 | Warmer ↔ Chimera | Warmer → Chimera | Standard NFSv4.1 READ; [manual Warmer](../manual-warmer.md) | Path mode verified once live | Handle-mode live read unverified (#40). |
| C5 | File identity / target map | events → planner → plans | [ML spec §8.11](../ml/spec.md) | Live map specified; owner-only batch map consumed by #124 | Build the live map in ingest (#59) and use it in the live planner (#60). The batch bridge accepts only a separately approved owner-only map bound to its frozen schedule. |

## Identity rule (applies to C1–C5)

- **Model identity is `(volume_id, file_id)` everywhere.** Chimera's
  `file_id` is a stable SHA-256 of the server file handle; datagen's IDs
  are opaque stand-ins.
- **Execution identity is `export_id` + `target_fh`.** It lives only in the
  Controller's in-memory target map and in plan/task files (mode `0600`).
  It is never stored in datasets, features, logs or ClearML.
