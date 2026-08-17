---
type: minor
---

Added `@lix-js/storage-opfs`, a durable SQLite Wasm + OPFS storage provider for browser Lix repositories. It supports multiple Lix workers and tabs attaching to the same repository through one package-owned SQLite connection.

Storage change watches wake `lix.observe()` after commits from another worker or tab, recover missed broadcasts through heartbeat state, and survive owner-tab failover. Lix transparently restarts full read queries when a concurrent commit expires their snapshot.

Auto-commit mutations and mutation batches also restart as a whole when their planning snapshot expires or optimistic commit validation loses a race. The provider is covered for offline reads and writes, reload and abrupt-tab recovery, branch and repository isolation, divergent local engines, and cross-tab convergence.
