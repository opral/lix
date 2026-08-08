# ForkTree Stage2 multimedia expected gates

## Profiles

| Profile | Logical bytes | Authenticated edit | Public path |
|---|---:|---:|---|
| `image-64-1` | 67,108,864 | 671,088 bytes (1%) | `/media/fixture.png` |
| `audio-64-1` | 67,108,864 | 671,088 bytes (1%) | `/media/fixture.flac` |
| `archive-512-10` | 536,870,912 | 53,687,091 bytes (10%) | `/media/fixture.tar` |
| `video-512-10` | 536,870,912 | 53,687,091 bytes (10%) | `/media/fixture.mov` |

Payload generation, upload part boundaries, splice offset, replacement bytes,
branch IDs, checkpoint count and operation ordering are deterministic and
identical across adapters and heads. Media extensions classify the lifecycle;
the bytes remain intentionally opaque to Lix.

## Public-semantic correctness

Every cell must prove:

1. multipart upload finalizes and exact full/range hashes match;
2. base checkpoint and branch-without-edit add zero blob manifests, references,
   unique chunks or payload bytes;
3. source edit changes exactly one public `lix_binary_blob_ref` diff entry;
4. merge returns one change and adds zero duplicate blob objects/bytes;
5. post-merge checkpoint adds zero duplicate blob objects/bytes;
6. hot undo returns the base payload and redo returns the edited payload;
7. the source branch is retired, main deletes the file, and a controlled
   retained branch owns the present/absent history interval;
8. 64 non-blob checkpoint advances remove unrelated main recovery retention;
9. full retained-root GC completes while exact diff and hot+cold undo/redo keep
   the edited payload readable;
10. flush/drop/cold reopen preserves roots, history and exact content;
11. deleting the final retained branch followed by full GC reclaims every blob
    manifest, chunk reference, unique chunk and payload byte;
12. a final cold reopen proves branch/file absence and zero blob inventory.

Any dependent corruption or authenticated traversal error must fail closed.
`run_gc_to_completion` is accepted only with
`terminal_status == GcTerminalStatus::Complete`; budget exhaustion and corrupt
progress are errors.

## Exact inventory contract

Every milestone emits the complete `ForkTreeInventory`:

```text
global_selectors, branch_selectors, snapshot_selectors, upload_selectors,
reachable_objects, unreachable_objects, repository_roots, branch_snapshots,
commit_objects, change_objects, state_tree_nodes, catalog_tree_nodes,
blob_manifests, blob_manifest_logical_bytes, blob_chunk_references,
unique_blob_chunks, unique_blob_payload_bytes, receipt_tree_nodes,
upload_parts, gc_maintenance_objects
```

The uploaded state is exactly one manifest whose logical and unique payload
bytes equal the profile size. Finalized upload receipt/part counts are zero.
The edit state is exactly two manifests whose logical bytes equal twice the
profile size. Exact chunk/reference/object counts are recorded from the typed
authenticated inventory and compared relationally across branch, merge and
checkpoint; no fixed 64-row/32-child packing is assumed.

There is deliberately no `presence_rows` field or authority. An authenticated
BlobChunk object is the sole durable payload/presence fact. Adding a presence
table, external object inventory, raw-space scanner, second selector or model
substitution is a hard rejection.

Every GC phase emits exact `ForkTreeGcRunSummary` fields:

```text
steps, marked, validated, reclaimed_objects, reclaimed_object_bytes,
terminal_status, max_page_claims, max_page_deletes
```

## Measurement contract

Setup is excluded. Every named phase emits wall time, process CPU ticks,
allocated bytes/calls, RSS before/after and process HWM, generic adapter read,
scan, put, delete and commit counts/bytes, Slate physical object reads/writes,
and database bytes before/after. Inventory and final settled post-close disk are
also emitted.

The first Stage2 gate is correctness/admission, not an optimization A/B. No
critical phase may regress more than 5% versus the exact-a12 comparator without
attributed unavoidable Stage2 work and coordinator acceptance. Blob duplicate
growth is a zero-tolerance correctness failure.

## Comparator and model binding

Exact-a12 report SHA-256:
`7bd123c1ea7d39bf8ecb315d21dbcb30c41235367df203b386134c02fe63d0de`.

Relevant exact-a12 facts:

- 64 MiB final release reclaimed 64 payload rows / 67,109,312 encoded payload
  bytes and staged 203 deletes on each adapter;
- 512 MiB final release reclaimed 512 payload rows / 536,874,496 encoded
  payload bytes and staged 1,547 deletes on each adapter;
- branch, merge and checkpoints added exactly zero CAS rows/bytes;
- retained-owner GC deleted nothing; history diff and undo/redo survived cold
  reopen; final reopen observed absence.

Those row/encoded-byte counts are comparator facts, not ForkTree packing
requirements.

Accepted ForkTree expectations:

- current/historical/reader/child/upload roots survive;
- abandoned/expired objects reclaim;
- shared objects survive until final reference removal, then reclaim exactly;
- page-streamed authoritative root/object scans only;
- crash/reopen and both GC/publication orders are fenced;
- corruption fails closed;
- total work `O(R + live + G)` and serving memory
  `O(page + depth + root-frontier)`;
- no cache authority, second root/selector, compatibility path or model
  substitution.

Model reference points are 500K retaining 100K and reclaiming 400K / 56.40 MB,
and mixed lifecycle reclaiming 10,002 receipt/part objects after final release.
They are semantic/scaling expectations, not production multimedia counts.
