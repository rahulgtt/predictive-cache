# Model manager

`ModelManager` is the model-control component for #61. It loads and verifies
packages, stages replacements and activates one coherent scorer, co-read table
and policy at an explicit tick boundary. It is not wired into the CLI or a
complete Controller loop yet.

A runtime creates it with a pointer path and, when memory exists, a trusted
`RuntimeContract`. Call `poll()` on a loader thread, `activate()` only between
prediction cycles, and retain `active()` for that whole cycle. The manager never
changes the active bundle while fetching. Call `close()` after stopping both
the loader and prediction loop. Extracted temporary package directories are
owned by the existing loader and cleaned up; caller package files are untouched.

The YAML pointer has exactly `model_uri`, `feature_version` and `fallback`.
Supported URIs are absolute `file:///...` packages and `clearml://<exact-model-id>`.
The latter lazily imports the optional ClearML SDK and uses its existing
credential/configuration chain. URI query strings, credentials and implicit
`latest` selectors are rejected. ConfigMap directory mounts may use symlinks;
`subPath` mounts do not propagate updates and should not be used.

Polling defaults to 30 seconds. An unchanged successful pointer is not fetched
again. Failed unchanged pointers retry after 300 seconds. Pointer changes can
retry at the next poll. Fetch and verification errors expose only fixed reason
codes, never raw exceptions, paths or credentials. A pointer changed during a
fetch cannot stage the old result. The active/staged bundles retain verified
loaded packages. Optional `cache_dir` publishes verified payload directories
by package ID with mode 0700 and files mode 0600; reuse revalidates hashes and
runtime compatibility. One manager owns a cache directory. The default limits
are four packages and 128 MiB. A full cache stops caching new packages without
deleting prior entries or preventing a valid model from staging. Temporary
staging directories are private; cache publication is atomic. Image and
deployment wiring remain later integration work.

The existing `load_package` remains the source of truth for payload hashes,
smoke scores, feature versions and semantic/serving contracts. A supplied
memory contract cannot be silently replaced. A generic first package may
establish a contract before memory exists. READ activation instead requires an
explicit `RuntimeContract.read_demand(...)` capability profile: neither file-size
nor anchor capability is inferred from a package. An incompatible request keeps
the previous active bundle but reports `contract_mismatch` and suppresses plans.
An ordinary failed load retains the previous working bundle without suppressing
plans.

Without an active package, a known compatible contract permits a conservative
fallback: generic frequency for `fv1`, or the existing READ-frequency scorer
for `read-demand-fv1`. The policy caps work at five items and 64 MiB. Unknown
contract means `not_ready`, no scorer and no plans. READ frequency itself refuses
unknown coverage rather than treating missing reads as zero. Fallback is a
ranking policy, not evidence of predictive improvement.

Validate inside the designated remote build container using
`pytest tests/controller/test_models.py tests/ml/test_package.py tests/ml/test_baselines.py`
and Ruff. Tests use fictional packages and injected fetch callbacks; they do
not download a live ClearML model or run filesystem warming.
