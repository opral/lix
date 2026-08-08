# FD2 whole-closure correction oracle

## Immutable source binding

- candidate: `b484e20d845aee3f8137bfa3496f9b3cd0e8cd35`
- candidate tree: `4477c83b246bddac09cd972564bd4ccd67f90f7b`
- candidate parent: `fd2be256d763f17e9f127d4c984e36fba191cb82`
- candidate full-index diff: `d36495fc406cc213bb5729babae761916f97bd515221de14c1f3ae114ec22610`
- candidate patch ID: `e90c9dd93db7c343f67887218049406640a77631`
- prior audit report: `83871d2d7c1e8faa0231f77aae75a3f2811debfaeaebd5fb6c18aa83d74d5e96`

Changed production paths are exactly:

```text
packages/lix/src/sql2/providers/file_history.rs
packages/lix/src/sql2/providers/filesystem_working_diff.rs
```

## Nine required source/model seams

The oracle has one named deterministic fixture for each item:

1. absence is not materialized as an empty byte vector;
2. zero BlobRef rows are distinguished from an authenticated BlobRef
   tombstone and fail closed for a live content-bearing descriptor;
3. file descriptors bind `file_id`, EntityPk, and snapshot identity;
4. directory descriptors bind `file_id == NULL`, EntityPk, and snapshot
   identity;
5. descriptor tombstones with payload fail closed;
6. directory tombstones with payload fail closed;
7. plugin-owner tombstones with payload fail closed and owner lookup remains
   registry-authenticated;
8. composite EntityPk values cannot be reduced to their first component for
   file/directory selection;
9. source changes with the same ID collapse only when byte-identical;
   conflicting duplicates fail closed.

The pure model also proves the valid controls:

- one exact live BlobRef materializes nonempty bytes;
- one exact live BlobRef for the content hash of `b""` and size `0`
  materializes `Some([])`;
- an authenticated BlobRef tombstone yields logical absence, not empty bytes;
- an authenticated descriptor tombstone without payload yields removal;
- the already-correct working-diff identity/tombstone rules remain valid.

## Required materialization contract

The corrected provider must carry three outcomes separately:

```text
Absent       authenticated tombstone / logical deletion -> SQL NULL
Present      exactly one live authenticated BlobRef -> bytes, including []
Corrupt      missing, zero, multiple, malformed, wrong-kind, substituted,
             missing-payload, identity, BlobId, or size mismatch -> error
```

`None -> Some(Vec::new())`, `unwrap_or_default`, and projection-dependent
authentication are forbidden. Metadata-only scans must validate the same
authority and payload fields as content scans, materializing only after all
checks pass.

## Source requirements

`file_history.rs` must validate file/directory `file_id` scope in both history
and observed-state parsers. Any observed tombstone carrying payload must be
rejected for descriptor, directory, blob, and plugin-owner rows. BlobRefs must
require exactly one authenticated row; a missing state row cannot be silently
treated as a tombstone. Plugin schema keys come only from the authenticated
registry; explicit empty registry is valid, missing/deleted/malformed/
wrong-kind/substituted registry is not.

`filesystem_working_diff.rs` must retain the caller-owned ForkTree reader,
require exact one-component typed EntityPk selection, preserve file/directory
scope rules, reject payload-bearing tombstones, and fail closed on duplicate
physical rows rather than last-write-win. Its existing identity/tombstone
checks are the positive control in the source gate.

## Calibration

On b484, `source_gate.py` must exit `1` and report all nine RED labels. The
model must exit `0`. This is an intentional pre-correction calibration; a
future direct successor is accepted only if the source gate turns green while
the model controls remain green.
