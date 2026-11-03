# Reviewed Chimera NFSv4 READ-event patch

This directory preserves the reviewed Chimera source patch that emits bounded,
authoritative `nfs4_read_v2` JSONL events. It is an external source artifact,
not code applied to this repository.

## Provenance

| Item | Value |
| --- | --- |
| Chimera base commit | `00f207bb358c3a6fa2e914e3e42b301e1c726b80` |
| Base qualification | Evidence-backed candidate; not proven to be deployed release source |
| Patch | `chimera-nfs4-read-events-v2.patch` |
| Patch SHA-256 | `ce08c4523b1024e61cebeda45df708882158b81e7e6c71f88d633a65efeab72e` |
| Stable patch ID | `2b75d7775ec6a3301a77baa71327bbb2f8c7503a` |
| Handoff manifest SHA-256 | `bdf754925ec3c60f3f3de3710ab6517fae30fcd3df897a1dbb26bbe9e98b48a8` |
| Rollout note SHA-256 | `ac134a170f133ebd797a59c3a36cd98e3af18bf1dc956b13029bce5e3460e4d4` |
| Builder image ID | `sha256:48c7d8f9b69bf123247e23ed4e55cc711ad07f67c563efc56934e63255b8bc43` |
| Latest sink OCI SHA-256 | `14f37754982567cbe6717694c36c83255a32cbb4681fb22992f3a93acee95315` |
| Earlier v2 OCI SHA-256 | `3bac0bd18efa6cf4d0a2b7294d4186d572e89ded8610f59c71c1e5fab7d5db95` |

The manifest and rollout evidence remain in approved private storage under
their hashes. Raw events, handles, logs, credentials and cluster access files
are intentionally not copied here.

## Apply and build

Apply only to a checkout at the exact base commit:

```bash
git rev-parse HEAD
git apply --check /path/to/chimera-nfs4-read-events-v2.patch
git apply /path/to/chimera-nfs4-read-events-v2.patch
```

Build and test in the authorized Chimera build container. The reviewed build
used Ninja, disabled Cairn and io_uring, and ran the focused common/NFS tests:

```bash
cmake -G Ninja \
  -DIO_URING_ENABLED=OFF \
  -DCAIRN_ENABLED=OFF \
  -DCMAKE_BUILD_TYPE=Debug \
  -S . -B /build
cmake --build /build
ctest --test-dir /build --output-on-failure \
  -R 'chimera/(common/log_try_info_test|server/nfs/(read_events|read_decision|nfs4_undispatched|replay_slot|open_claim_fh))'
```

The reviewed snapshot passed all six focused tests and all 31 common/NFS
tests, with one documentation test skipped. Deployment still requires
authorized access and the current instructions in
[`docs/nasuni-cluster-access.md`](../docs/nasuni-cluster-access.md).

## Boundaries

- The source patch was reviewed but is not applied, committed or deployed by
  this repository.
- A completed event or NFS read does not prove cache residency or benefit.
- Origin authentication, handle validity and matched cold/warm efficacy remain
  separate gates.
- The matched disposable build uses CUFS `0.2.0-48` and accepts a PoC-only
  birth-time compatibility limitation documented in
  [`cufs/README.md`](../cufs/README.md).
