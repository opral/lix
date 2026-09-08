---
type: minor
---

Support structural JSON row edits, including insertion, deletion, reordering, moves, and scalar/container conversion. Structural batches validate and rebuild the final tree, preserve row identities and unchanged scalar spelling, and stream the replacement file. Existing scalar updates retain their byte-splice fast path.

Container deletion requires removing or moving descendants in the same plugin row batch; invalid trees reject atomically. SQL projects each statement separately. Structural upserts follow normal row last-write-wins behavior, including recreating a previously deleted key.

Repeated row updates honor the final update even when it restores the original value. Renames adapt existing key-formatting hints, and scalar conversions ignore obsolete empty-container whitespace. File-derived row deltas use deterministic key order so stale edits across multiple parents can compose reliably.
