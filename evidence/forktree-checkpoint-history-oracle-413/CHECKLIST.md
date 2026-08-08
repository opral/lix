# Checkpoint/history reconstruction acceptance checklist

This is a report-only acceptance contract for a future ForkTree migration on
top of 413e08a plus prerequisite 97a7116. It is RED until the exact adapter
gates pass; it authorizes no production edits in this package.

## One retained view/read

For each operation, instrument and assert exactly one caller-owned
`StorageRead`/ForkTree view identity from selector through all commit/root and
state reads. The operation must not acquire another read, instantiate
`TrackedStateStoreReader`/`TrackedStateContext`, use a second graph reader, or
consult a legacy fallback/cache.

## Required cases

1. **Latest checkpoint lookup:** valid marker at a valid commit returns the
   checkpoint; a valid root with no marker returns the explicit baseline result;
   missing/malformed marker, commit catalog, commit object, or root is an
   observable corruption/error.
2. **Chronology/first parent:** checkpoint history is newest-first, follows
   only `CommitObjectV1.parent_commit_ids[0]`, detects missing nodes and cycles,
   and never derives chronology from `CheckpointRecoveryRef`.
3. **Recovery/floor separation:** recovery `{H -> C}` is used only for
   authenticated retention/reopen; the undo floor and merge/history base come
   from canonical graph/control facts. A later rotation must not reinterpret
   the current recovery row as an old H→C relation.
4. **Historical point/scan:** valid commit + valid root + absent key is
   authenticated absence. Missing CommitCatalog/root, wrong-kind substitution,
   or malformed selector/catalog/commit/root fails closed before an empty scan.
   Null, tombstone, and value remain distinct.
5. **65 rotations:** create 65 checkpoints, including empty/no-op rotations;
   reconstruct all retained checkpoint chronology, verify the current recovery
   pair only describes its latest interval, and preserve authenticated queue
   checkpoint roots for older intervals.
6. **Undo/redo:** post-checkpoint commit undoes to the checkpoint floor and no
   further; redo restores the exact commit. Markers are validated from the same
   view and malformed/missing marker payloads fail closed.
7. **Branch from pre-checkpoint:** create a branch from historical H after
   checkpoint C; its first ordinary commit has parents `[H, C]` in that order,
   generation greater than both, and merge base C. No reader consults the
   recovery row as chronology and no permanent C→H parent is introduced.
8. **Cold reopen:** flush/drop/reopen before checkpoint history, undo/redo,
   branch merge, and GC assertions; all view IDs/read counters and errors remain
   equivalent.
9. **GC retention:** while the historical branch lives, H and its complete
   closure remain retained; after branch deletion and queue/CAS completion, H
   and only its unreferenced interval are reclaimable. C/checkpoint roots and
   active branch controls remain retained. Blocked queue/debt must not be
   cleared or spun.

## Negative controls

* No empty-success or canonicalization for missing/malformed/wrong-kind
  CommitCatalog, commit, or state-root objects.
* No `CheckpointRecoveryRef` read in merge-base/history reconstruction.
* No `TrackedStateStoreReader`, legacy tracked-state reader, direct storage
  space access, fallback scan, retry, or cache in the migrated call graph.
* No permanent graph parent added solely to keep compacted intervals live.
* No weakened GC, marker, selector, corruption, undo, or branch assertions.

## Source/deletion gate

The candidate source diff must delete the exact residue listed in `REPORT.md`
and leave no references to the removed reader factories or historical scan
helpers. The ForkTree facade must be the sole checkpoint/history/undo/redo
read owner; semantic undo/redo, parsed file history, and GC retention behavior
remain publically unchanged.
