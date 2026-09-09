# Filesystem storage synchronization

The filesystem adapter runs a background session over its backing storage to import disk edits. For connected replicas, that session inherits the owning repository’s replica write admission, authenticated account, active branch, and sync wakeups. Disk edits enter the same durable local outbox as foreground writes without opening another server connection.

The Rust `Lix::open_storage_session` integration hook supports standalone and connected-replica repositories; it does not grant authority-server write admission. Its backing storage must belong to the same repository. The session intentionally avoids retaining the filesystem wrapper or owning sync runtime, preventing an ownership cycle.

Call `FilesystemStorage::stop_sync().await` before closing the owning connected Lix handle. This stops the filesystem worker and its internal session before server sync shuts down. The JavaScript filesystem binding performs this ordering automatically when its last session closes.
