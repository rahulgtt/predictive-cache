# Frozen collection contracts

These repository-safe files are the exact byte sources for controlled
collection manifest hashes.

| File | Scope | SHA-256 |
| --- | --- | --- |
| `cluster-provenance-v1.json` | Scoped collection prerequisites: baseline BOM, Chimera Helm revision 7 manifest, approved component images, sink configuration and reviewed source artifacts. It is not a full-cluster identity. | `5b1045ffaffc4fee3c72dc79ddeb691b4af213d96a84854d2479d5ddae11c02b` |
| `nfs4-read-v2.schema.json` | JSON Schema generated from the strict `ChimeraEvent` importer contract with aliases enabled. | `3c15595d89d5780fc08a6eae667aafa848da85bf0f7f195c933a0fb3c13dbeb7` |

Both files use canonical sorted compact JSON:

```text
UTF-8(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n")
```

The trailing line feed is part of the hashed bytes. The controlled collection
manifest sets:

```text
cluster_manifest_sha256 =
  SHA256(contracts/cluster-provenance-v1.json exact bytes)

event_schema_sha256 =
  SHA256(contracts/nfs4-read-v2.schema.json exact bytes)
```

Regenerate the event schema only from
`predictive_cache.sources.chimera.contract.ChimeraEvent.model_json_schema(by_alias=True)`
using the canonical serialization above. Any contract or provenance change
requires new bytes, hashes, manifest review and provider approval.
