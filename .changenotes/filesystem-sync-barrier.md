---
type: minor
---

Added `FsBackend.syncDiskToLix()` as an awaitable filesystem sync barrier.

The filesystem backend picks up disk edits in the background with debouncing. `backend.syncDiskToLix()` flushes pending on-disk changes into Lix and resolves once they are materialized, so subsequent queries reflect the current disk state.
