---
type: minor
---

Removed the SQLite storage option from `@lix-js/sdk`.

Use the RocksDB-backed `LocalFilesystem` adapter for persistent local development. The standalone Rust SQLite storage adapter remains available for specialized use.
