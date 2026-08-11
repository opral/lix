use crate::storage::{
    BeginScanOptions, CommitResult, GetManyRequest, GetManyResult, Key, KeyRange, PutBatch,
    ReadOptions, ScanCursor, StorageError, StorageSpace, WriteOptions,
};

/// An ordered byte-key entry storage with coherent read views, batched point
/// access, space-scoped scans, and atomic batched writes.
///
/// Storage is organized into spaces: engine-defined namespaces identified by
/// [`SpaceId`]. Every operation addresses exactly one space, keys are logical
/// bytes scoped to that space, and spaces are physically independent (a
/// storage may store them as separate tables, trees, or column families).
/// Spaces come into existence on first write; reading a space that was never
/// written behaves as empty.
///
/// The future-based boundary lets remote implementations yield while waiting
/// for I/O and lets callers overlap independent operations on one read view.
/// Implementations that wrap an asynchronous provider should preserve that
/// behavior instead of synchronously blocking the caller's executor.
pub trait Storage: Send + Sync {
    type Read<'a>: StorageRead + 'a
    where
        Self: 'a;

    type Write<'a>: StorageWrite + 'a
    where
        Self: 'a;

    fn begin_read(
        &self,
        opts: ReadOptions,
    ) -> impl Future<Output = Result<Self::Read<'_>, StorageError>> + Send;

    /// Opens one storage-owned write transaction.
    ///
    /// The storage is the concurrency boundary. Implementations are responsible
    /// for their own persistence and write concurrency semantics. A storage may
    /// publish a commit before its background durability boundary. A storage
    /// that cannot safely support overlapping write transactions must serialize,
    /// use native transactional locking, or reject the second writer with a
    /// deterministic error.
    ///
    /// Lix sessions intentionally do not add a generic per-storage write lock
    /// above this method.
    fn begin_write(
        &self,
        opts: WriteOptions,
    ) -> impl Future<Output = Result<Self::Write<'_>, StorageError>> + Send;
}

/// One coherent read view.
///
/// Read handles must release snapshots and other resources from `Drop`;
/// callers are not required to run asynchronous cleanup when a scope ends.
pub trait StorageRead: Send + Sync {
    /// Stable identity for this immutable read view within one open storage
    /// instance. Adapters may expose this to key process-local derived caches;
    /// `None` disables such caching.
    fn snapshot_cache_key(&self) -> Option<u128> {
        None
    }

    /// Reads one or more space-scoped point batches from this coherent view.
    /// The flat result preserves request order, then key order within each
    /// request, including duplicate keys.
    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send;

    /// Opens one storage-owned iterator on this coherent read view.
    ///
    /// The returned cursor is ephemeral and cannot outlive this read handle.
    /// It advances source state bound to this view and must never acquire a
    /// replacement read view as pages advance.
    fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send;
}

pub trait StorageWrite: Send {
    /// Applies one batch of upserts to one space.
    ///
    /// Batches hold at most one mutation per key. Engine write-set lowering
    /// produces batches sorted ascending by key; other callers may pass
    /// unsorted batches. Point batches are final for their keys: callers must
    /// issue every range deletion before calling `put_many` in the same write
    /// transaction. Exact point deletes remain valid after a put batch.
    fn put_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Deletes the given keys of one space. Batches hold at most one
    /// mutation per key; engine write-set lowering produces sorted keys.
    fn delete_many(
        &mut self,
        space: StorageSpace,
        keys: &[Key],
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Deletes every key of one space within the range. Range deletions must
    /// precede point puts in the same write transaction. An unbounded range
    /// clears the whole space; storage implementations may fast-path that case
    /// (for example by truncating the space's table).
    fn delete_range(
        &mut self,
        space: StorageSpace,
        range: KeyRange,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Applies the write transaction atomically and acknowledges it.
    ///
    /// Atomicity is required: either every staged mutation becomes visible or
    /// none does, and no reader may observe a partial write set.
    ///
    /// **What the acknowledgement means on disk is the adapter's choice**, and
    /// it differs between shipping adapters — see `WriteOptions::await_durable`
    /// for the measured behaviour of each with and without the flag. Returning
    /// `Ok` does not by itself imply the write survives a crash, and which
    /// crash it survives is exactly what varies.
    fn commit(self) -> impl Future<Output = Result<CommitResult, StorageError>> + Send;

    fn rollback(self) -> impl Future<Output = Result<(), StorageError>> + Send;
}
