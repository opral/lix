use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;

use crate::backend::{
    BackendError, CommitResult, GetManyResult, GetOptions, Key, KeyRange, KeyRef, ProjectedValue,
    ProjectedValueRef, PutBatch, ReadOptions, ScanOptions, ScanResult, SpaceId, WriteOptions,
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
pub trait Backend {
    type Read<'a>: BackendRead + 'a
    where
        Self: 'a;

    type Write<'a>: BackendWrite + 'a
    where
        Self: 'a;

    fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, BackendError>;

    fn mounted_filesystem(&self) -> Option<Arc<dyn BackendMountedFilesystem>> {
        None
    }

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
    fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, BackendError>;
}

#[async_trait]
pub trait BackendMountedFilesystem: Send + Sync {
    /// Returns one best-effort mounted filesystem inventory snapshot.
    ///
    /// Mounted filesystems are external system state. Implementations do not
    /// promise snapshot isolation between inventory and file-data reads.
    async fn inventory(&self) -> Result<MountedFilesystemInventory, BackendError>;

    async fn read_file_data(&self, path: &str) -> Result<Option<Vec<u8>>, BackendError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MountedFilesystemInventory {
    pub directories: BTreeSet<String>,
    pub files: BTreeSet<String>,
    pub revision: u64,
}

pub trait BackendRead {
    /// Visits the requested keys of one space, calling the visitor with each
    /// key's position in `keys`. Visit order is unspecified; consumers must
    /// address results by the visited index, which lets backends return
    /// rows in whatever order their storage produces them.
    fn visit_keys<V>(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized;

    /// Streams up to `opts.limit_rows` rows of one space in ascending key
    /// order to the visitor and reports whether more rows remain. The
    /// visitor observes logical keys.
    ///
    /// `opts.resume_after` is exclusive and must not widen the range: the
    /// effective lower bound is the maximum of `range.lower` and
    /// `Excluded(resume_after)`. `limit_rows == 0` emits nothing and
    /// reports `has_more: false`.
    fn scan<V>(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions<'_>,
        visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized;

    fn close(self) -> Result<(), BackendError>
    where
        Self: Sized,
    {
        Ok(())
    }
}

pub trait ScanVisitor {
    fn visit(&mut self, key: KeyRef<'_>, value: ProjectedValueRef<'_>) -> Result<(), BackendError>;
}

pub trait BackendWrite {
    /// Applies one batch of upserts to one space.
    ///
    /// Batches hold at most one mutation per key. Engine write-set lowering
    /// produces batches sorted ascending by key; other callers may pass
    /// unsorted batches.
    fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), BackendError>;

    /// Deletes the given keys of one space. Batches hold at most one
    /// mutation per key; engine write-set lowering produces sorted keys.
    fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), BackendError>;

    /// Deletes every key of one space within the range. An unbounded range
    /// clears the whole space; backends may fast-path that case (for
    /// example by truncating the space's table).
    fn delete_range(&mut self, space: SpaceId, range: KeyRange) -> Result<(), BackendError>;

    fn commit(self) -> Result<CommitResult, BackendError>;

    fn rollback(self) -> Result<(), BackendError>;
}

pub trait PointVisitor {
    fn visit(
        &mut self,
        index: usize,
        key: &Key,
        value: Option<ProjectedValueRef<'_>>,
    ) -> Result<(), BackendError>;
}

pub fn get_many<R>(
    read: &R,
    space: SpaceId,
    keys: &[Key],
    opts: GetOptions<'_>,
) -> Result<GetManyResult, BackendError>
where
    R: BackendRead + ?Sized,
{
    struct MaterializingPointVisitor<'a> {
        values: &'a mut [Option<ProjectedValue>],
    }

    impl PointVisitor for MaterializingPointVisitor<'_> {
        fn visit(
            &mut self,
            index: usize,
            _key: &Key,
            value: Option<ProjectedValueRef<'_>>,
        ) -> Result<(), BackendError> {
            if let Some(slot) = self.values.get_mut(index) {
                *slot = value.map(ProjectedValueRef::to_owned);
            }
            Ok(())
        }
    }

    let mut values = vec![None::<ProjectedValue>; keys.len()];
    read.visit_keys(
        space,
        keys,
        opts,
        &mut MaterializingPointVisitor {
            values: values.as_mut_slice(),
        },
    )?;
    Ok(GetManyResult::new(values))
}

impl<F> ScanVisitor for F
where
    F: for<'a> FnMut(KeyRef<'a>, ProjectedValueRef<'a>) -> Result<(), BackendError>,
{
    fn visit(&mut self, key: KeyRef<'_>, value: ProjectedValueRef<'_>) -> Result<(), BackendError> {
        self(key, value)
    }
}

impl<F> PointVisitor for F
where
    F: for<'a> FnMut(usize, &Key, Option<ProjectedValueRef<'a>>) -> Result<(), BackendError>,
{
    fn visit(
        &mut self,
        index: usize,
        key: &Key,
        value: Option<ProjectedValueRef<'_>>,
    ) -> Result<(), BackendError> {
        self(index, key, value)
    }
}
