# CUFS compatibility provenance

No CUFS source or runtime feature patch is active for Predictive Cache. The
reviewed Chimera candidate used the existing in-tree CUFS adapter with the
deployed CUFS `0.2.0-48` runtime/development package pair.

## Preserved build-only artifacts

| Item | SHA-256 |
| --- | --- |
| `Dockerfile.poc-compat` | `55b48c7b60732219072ff3f1ad0e924eab02cf1ce726a92474d6543c692d9b0e` |
| `fsal-cufs-poc-compat.h` | `dac238c11ad335e354431c4410c54e33d9d5cc5d4ce10ae0b6e19e5bfce0aed2` |

## Matched CUFS package and ABI record

| Item | Identity | SHA-256 / build ID |
| --- | --- | --- |
| Runtime RPM | `cufs-0.2.0-48.el10.x86_64.rpm` | `ad539181ca82b2136cc9ebba114ce0aba7bf4a072d5788e71effaa2f39c33f9b` |
| Development RPM | `cufs-devel-0.2.0-48.el10.x86_64.rpm` | `0bdb065c3cd6e039bbfe8c36e7f8833d328538362c92e4653df4ca91b2c16ee3` |
| Installed public header | `/usr/include/cufs/fsal_cufs/fsal_cufs.h` | `1cfc649d32692c912aed10c7a9f2a5550e33a9fd81652f6f9cb74bb46e99b19e` |
| Runtime library | `/usr/lib64/libfsal_cufs.so.0.2.0` | `66109c8514b459a1125faabeb0ae061a49b70304a9dc355329238cbbc99222e7` |
| Runtime ELF build ID | `libfsal_cufs.so.0.2.0` | `616e7595da93fb386d84d2be90b26db774264f7b` |

The runtime symlink resolved from `libfsal_cufs.so.1` to
`libfsal_cufs.so.0.2.0`. Both installed RPM identities were
`0.2.0-48.el10.x86_64`.

These files are disposable-PoC build scaffolding, **not CUFS runtime patches**.
Do not merge the wrapper into CUFS or claim production ABI or birth-time
correctness from it.

The wrapper:

- aliases the installed `struct stat` layout as `cufs_stat_t`; and
- maps unavailable `st_btim` access to `st_ctim`.

That mapping is an accepted historical PoC limitation. It does not provide
true file birth time and must not be used as evidence for production identity
or replacement detection.

## Reproduce the matched build

Obtain the exact authorized `cufs-0.2.0-48` and `cufs-devel-0.2.0-48` RPMs,
verify their recorded hashes, and place them in `pcw-rpms/` beside the Chimera
build context. Build with the preserved Dockerfile and authorized local base
images:

```bash
podman build \
  -f /path/to/Dockerfile.poc-compat \
  -t <local-chimera-poc-image> \
  /path/to/chimera-checkout
```

The Dockerfile injects `cufs/` headers and libraries only into the build image,
installs the same runtime RPM in the runtime image, and adds the wrapper include
directory for the Chimera compilation. Local image names in the Dockerfile are
historical build inputs, not public dependencies.

The repository records the identities and hashes but carries no RPMs or CUFS
libraries. Access and deployment details belong in
[`docs/nasuni-cluster-access.md`](../docs/nasuni-cluster-access.md).
