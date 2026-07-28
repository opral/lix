---
type: minor
---

Run Lix workspaces remotely with live, low-latency clients.

`openLix()` can connect to the versioned Lix HTTP protocol for SQL, branches,
atomic batches, binary file operations, and multiplexed live queries. Each
client gets an isolated branch-pinned session, retries writes safely, persists
private local state locally, and sends compact deltas for localized edits.
