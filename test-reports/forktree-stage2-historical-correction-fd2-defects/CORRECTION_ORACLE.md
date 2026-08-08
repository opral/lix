# Correction contract

## A. Descriptor tombstones are deletion events

For a selected authenticated state root, a descriptor row has four semantic
outcomes:

| Row | Logical snapshot | Filesystem diff |
| --- | --- | --- |
| live, correct identity/payload | present | added/modified as appropriate |
| valid tombstone for the selected identity | absent | removed if the prior snapshot contained it |
| missing, malformed, wrong kind, or substituted identity | no result | fail closed |

The tombstone branch is not a malformed descriptor and must not be converted
into a public error. The selected-root loader must still authenticate the row
key, domain, and tombstone identity before returning logical absence.

## B. BlobRef validation precedes projection

For every live content-bearing historical file, the owner must resolve exactly
one authenticated BlobRef for that file identity and authenticate the referenced
payload before applying the requested projection. `metadata_only` changes only
whether authenticated bytes are returned/materialized; it does not change
whether the BlobRef and payload are checked.

The following are fail-closed for live files:

- zero BlobRefs;
- multiple BlobRefs;
- a BlobRef for another file identity;
- a missing, malformed, wrong-domain, or digest-substituted BlobRef; and
- a missing or digest-mismatched payload.

A valid descriptor tombstone is not a live file and therefore does not require a
BlobRef or payload. It remains a deletion event.

## Structural acceptance

`verify_source_contract.sh` extracts Rust functions with a balanced brace
scanner that ignores comments, strings, chars, and raw strings. It then checks
only the affected function scopes:

- `create_checkpoint` retains `branch_ref_reader_on_opening_read`, a single
  `forktree_read_facade`, `checkpoint_history_from_head`, and
  `diff_state_rows_between_commits`;
- the `plan_scan` functions in `working_diff.rs` and
  `filesystem_working_diff.rs` consume the retained historical reader and do
  not create a local facade or invoke the old tracked-state diff path;
- fd2's `scan_descriptors` tombstone error is identified as the expected RED
  defect;
- fd2's `load_file_history_rows` projection-gated materialization validation
  is identified as the expected RED defect; and
- fd2's file-row preparation uses a non-cardinality-checked lookup, which is
  reported as part of the BlobRef correction surface.

The `corrected` mode reverses the two defect assertions and requires explicit
structural markers for tombstone-to-absence handling, pre-projection exact-one
validation, and metadata-only validation. It is intentionally dormant on fd2.

## Preserved chronology and ownership

The package does not replace the accepted fd2 history design. The source gate
requires the migrated checkpoint/working-diff functions to retain one
ForkTree history source and rejects local reader/fallback construction in those
functions. The model separately checks exact checkpoint marker identity,
first-parent chronology, missing-parent failure, and cycle failure. No cache,
compatibility reader, second history authority, or production writer is added.
