---
type: major
---

Renamed the Lix backend API to storage across Rust, JavaScript, packages, and documentation.

Pass `storage` to `openLix()` and use the new types such as `Storage`, `SQLite`, and `LocalFilesystem`. The former backend names have been removed without compatibility aliases.

Rust storage implementations are now split into `lix_sqlite_storage`, `lix_rocksdb_storage`, and `lix_slatedb_storage`. Replace `lix_backends` with the individual crates you use, and replace `lix_fs_backend` with `lix_local_filesystem`. The Redb implementation has been removed.
