use crate::backend::{
    BackendError, CommitResult, GetManyResult, GetOptions, Key, KeyRange, PutBatch, ReadOptions,
    ScanChunk, ScanOptions, SpaceId, WriteOptions,
};

/// An ordered byte-key entry backend with coherent read views, batched point
/// access, space-scoped scans, and atomic batched writes.
///
/// Storage is organized into spaces: engine-defined namespaces identified by
/// [`SpaceId`]. Every operation addresses exactly one space, keys are logical
/// bytes scoped to that space, and spaces are physically independent (a
/// backend may store them as separate tables, trees, or column families).
/// Spaces come into existence on first write; reading a space that was never
/// written behaves as empty.
///
/// The future-based boundary lets remote implementations yield while waiting
/// for I/O and lets callers overlap independent operations on one read view.
/// Implementations that wrap an asynchronous provider should preserve that
/// behavior instead of synchronously blocking the caller's executor.
pub trait Backend: Send + Sync {
    type Read<'a>: BackendRead + 'a
    where
        Self: 'a;

    type Write<'a>: BackendWrite + 'a
    where
        Self: 'a;

    fn begin_read(
        &self,
        opts: ReadOptions,
    ) -> impl Future<Output = Result<Self::Read<'_>, BackendError>> + Send;

    /// Opens one backend-owned write transaction.
    ///
    /// The backend is the concurrency boundary. Implementations are responsible
    /// for their own durability and write concurrency semantics. A backend that
    /// cannot safely support overlapping write transactions must serialize,
    /// use native transactional locking, or reject the second writer with a
    /// deterministic error.
    ///
    /// Lix sessions intentionally do not add a generic per-backend write lock
    /// above this method.
    fn begin_write(
        &self,
        opts: WriteOptions,
    ) -> impl Future<Output = Result<Self::Write<'_>, BackendError>> + Send;
}

/// One coherent read view.
///
/// Read handles must release snapshots and other resources from `Drop`;
/// callers are not required to run asynchronous cleanup when a scope ends.
pub trait BackendRead: Send + Sync {
    /// Reads the requested keys of one space. The returned values have one
    /// slot per requested key, in caller order, and preserve duplicates.
    fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions,
    ) -> impl Future<Output = Result<GetManyResult, BackendError>> + Send;

    /// Reads one owned page of a space in ascending logical key order and
    /// reports whether more rows remain. A page contains at most
    /// [`crate::backend::MAX_SCAN_PAGE_ROWS`] rows, even when
    /// `opts.limit_rows` is larger.
    ///
    /// `opts.resume_after` is exclusive and must not widen the range: the
    /// effective lower bound is the maximum of `range.lower` and
    /// `Excluded(resume_after)`. `limit_rows == 0` emits nothing and
    /// reports `has_more: false`.
    fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions,
    ) -> impl Future<Output = Result<ScanChunk, BackendError>> + Send;
}

pub trait BackendWrite: Send {
    /// Applies one batch of upserts to one space.
    ///
    /// Batches hold at most one mutation per key. Engine write-set lowering
    /// produces batches sorted ascending by key; other callers may pass
    /// unsorted batches.
    fn put_many(
        &mut self,
        space: SpaceId,
        entries: PutBatch,
    ) -> impl Future<Output = Result<(), BackendError>> + Send;

    /// Deletes the given keys of one space. Batches hold at most one
    /// mutation per key; engine write-set lowering produces sorted keys.
    fn delete_many(
        &mut self,
        space: SpaceId,
        keys: &[Key],
    ) -> impl Future<Output = Result<(), BackendError>> + Send;

    /// Deletes every key of one space within the range. An unbounded range
    /// clears the whole space; backends may fast-path that case (for
    /// example by truncating the space's table).
    fn delete_range(
        &mut self,
        space: SpaceId,
        range: KeyRange,
    ) -> impl Future<Output = Result<(), BackendError>> + Send;

    fn commit(self) -> impl Future<Output = Result<CommitResult, BackendError>> + Send;

    fn rollback(self) -> impl Future<Output = Result<(), BackendError>> + Send;
}
