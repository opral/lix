---
type: patch
---

A file rename no longer fails when a concurrent commit expires its plugin discovery read.

`UPDATE lix_file SET path` and `lix_file` scans that need plugin rendering run Lix reads inside the query plan. An expired coherent read there was reported as a plain execution error, which hid its `LIX_STORAGE_READ_EXPIRED` code from the session's bounded retry, so a rename during sync churn surfaced "plugin discovery failed" to the caller instead of restarting. The Lix error now stays the cause, code included, and the statement restarts like any other expired read.
