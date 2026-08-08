# Correction contract

## A. Descriptor tombstones are deletion events

For a selected authenticated state root, a descriptor row has four semantic
outcomes:

| Row | Logical snapshot | Filesystem diff |
| --- | --- | --- |
| live, exact row/snapshot/descriptor identity | present | added/modified as appropriate |
| valid tombstone for the selected identity | absent | removed if the prior snapshot contained it |
| missing, malformed, wrong kind, or substituted identity | no result | fail closed |

The tombstone branch is not a malformed descriptor and must not become a
public error. The selected-root loader still authenticates row key, snapshot
ID, and descriptor/file identity before returning logical absence.

## B. BlobRef validation precedes projection

Every live content-bearing historical file must resolve exactly one
authenticated BlobRef and validate all of these bindings before projection:

`row_key`, `snapshot_id`, `descriptor_id`, `file_id`, `BlobId`, declared size,
and payload bytes. The BlobId must agree with both the BlobRef and the
authenticated payload, and declared size must equal the payload length.

`metadata_only` changes only whether authenticated bytes are returned or
materialized; it never bypasses BlobRef, size, BlobId, or payload checks. A
valid empty payload is authenticated and returns zero bytes when materialized.
A valid descriptor tombstone is not a live file, remains a deletion event, and
has no live-payload obligation.

The executable negative fixtures cover zero/multiple references, wrong row
key, snapshot, descriptor, file ID, BlobId, declared size, payload identity,
missing payload, missing/malformed/wrong-kind Blob authority, and substituted
authority. These cases execute in the standalone model.

## Structural acceptance

`verify_source_contract.sh` extracts Rust functions with a balanced brace
scanner that ignores comments, strings, chars, and raw strings. It checks only
affected production function scopes for the retained ForkTree history reader,
the fd2 RED defects, and forbidden legacy fallback paths. Its `corrected` mode
also requires explicit field-complete model markers, the valid-empty fixture,
and executable negative-fixture markers. Global token presence is not the
acceptance mechanism.

## Preserved chronology and ownership

The package does not replace the accepted fd2 history design. Checkpoint and
working-diff callers retain one ForkTree history source; exact checkpoint marker
ancestry, missing-parent failure, and cycle failure remain in the model. No
cache, compatibility reader, second history authority, or production writer
is added.
