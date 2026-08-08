# Direct historical-correction oracle

## Required source ownership

The caller constructs one `HistoryQuerySource` from one retained
`StorageRead`. Its `forktree_reader` carries the only chronology/state view
identity. Every surface receives that exact identity by value or reference:

| Surface | Required source | Forbidden source |
| --- | --- | --- |
| SQL checkpoint | `query_source.forktree_reader` | local facade, `query_source.store`, fresh read |
| checkpoint creation | transaction’s caller-owned retained ForkTree view | `working_diff_at_head`, BranchHead/TrackedHead, fresh tracked reader |
| filesystem checkpoint working diff | `query_source.forktree_reader` | local facade, `query_source.store`, fresh read |
| ordinary working diff | `query_source.forktree_reader` | BranchHead/TrackedHead accelerator, `TrackedStateContext` fallback |

The view identity is not a cache key and is not persisted. A mismatched view is
a fail-closed error. No surface may refresh the selector, create a second
`ForkTreeReadFacade`, or recover through a legacy reader.

## Historical row contract

The selected historical root is authoritative for row state:

- `Value` rows are materialized only after the authenticated descriptor and
  exactly one matching BlobRef are present.
- The BlobRef must bind the same file identity, canonical BlobId, declared
  payload, and authenticated digest. Missing, malformed, wrong-kind,
  duplicated, substituted, or missing-payload references fail closed.
- `Tombstone` is a deletion event and remains observable as deletion; it is
  not treated as a missing value and does not require a BlobRef.
- `Null` is distinct from both absence and tombstone. A content-bearing file
  row with no valid value/BlobRef contract fails closed.
- Plugin-owned rows require an authenticated plugin owner, registry entry,
  descriptor, BlobRef, and payload. No plugin or ordinary-file fallback may
  turn corruption into empty bytes.

## Read cardinality

Each pure read case must have exactly one retained view/read and zero plans,
prepared writes, commits, selector rotations, legacy fallbacks, or repair
attempts. The model records these counters explicitly.

## Negative source gate

`source_gate.sh` is path-aware. It rejects only the four consumer paths and
the exact historical/file materialization contracts; it does not reject
legitimate `ForkTreeReadFacade` implementation code or unrelated APIs. It
also requires the preserved H–L checks for projection-independent certified
rows, commit identity mismatch, ancestry cycles, missing parents, and strict
directory/path resolution.

## Package-only scope

The candidate diff may contain only this report directory. It may not change
`packages/lix/src`, Cargo manifests, adapters, SQL registration, or benchmark
code. This is an acceptance contract, not runtime qualification or production
approval.
