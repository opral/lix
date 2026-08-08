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

The structural verifier does not accept a field-name mention as proof. For each
provider it parses the `register_*_provider`, `plan_scan`, and exact spec
struct; the chronology receiver must be `self.forktree_reader` or an alias
whose assignment resolves directly to that field. The filesystem working-diff
`load_rows` call is checked separately by balanced argument parsing. Checkpoint
creation is scoped only to `create_checkpoint`; unrelated transaction/context
owners are not globally counted. Its chronology receiver must resolve to the
transaction-owned retained reader/facade, and the function body must contain no
local/fresh/legacy fallback.

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

The source gate anchors these requirements to the production functions
`parse_file_history_descriptors`, `parse_file_history_directories`,
`parse_file_history_blobs`, their observed-row counterparts,
`parse_file_history_plugin_state`, `parse_file_history_plugin_owners`,
`parse_file_history_observed_plugin_owners`,
`prepare_file_history_rows`, `load_file_history_blob_bytes`, and
`validate_file_history_materialization`. It requires the actual persisted-row
fields and rejects the current first-match BlobRef lookup. The
`production_history_fixtures.tsv` cases cover valid file/plugin/directory
values, authenticated tombstones, missing/malformed/substituted identities,
duplicate BlobRefs, missing payloads, and missing plugin registry.

## Read cardinality

Each pure read case must have exactly one retained view/read and zero plans,
prepared writes, commits, selector rotations, legacy fallbacks, or repair
attempts. The model records these counters explicitly.

## Negative source gate

`source_gate.sh` is path-aware. Its caller/materialization decision is made by
the balanced structural verifier, not token co-occurrence. It rejects only the
four consumer functions plus the exact production materialization functions;
it does not reject legitimate `ForkTreeReadFacade` implementation code or
unrelated transaction owners. Source-negative fixtures prove that fake
tokens, distinct views, independent reads, and mismatched call arguments are
rejected. The original `CD91_RED_CALIBRATION.log` is retained unchanged as the
parent calibration; the v2 log calibrates the new verifier separately.

## Package-only scope

The candidate diff may contain only this report directory. It may not change
`packages/lix/src`, Cargo manifests, adapters, SQL registration, or benchmark
code. This is an acceptance contract, not runtime qualification or production
approval.
