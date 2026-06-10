use crate::backend::{
    BackendError, CommitResult, GetManyResult, GetOptions, Key, KeyRange, KeyRef, ProjectedValue,
    ProjectedValueRef, PutBatch, ReadEntry, ReadOptions, ScanOptions, ScanResult, WriteOptions,
};

pub trait Backend {
    type Read<'a>: BackendRead + 'a
    where
        Self: 'a;

    type Write<'a>: BackendWrite + 'a
    where
        Self: 'a;

    fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, BackendError>;

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

pub trait BackendRead {
    type RangeScan<'cursor>: BackendRangeScan;

    /// Visits the requested keys, calling the visitor with each key's
    /// position in `keys`. Visit order is unspecified; consumers must
    /// address results by the visited index, which lets backends return
    /// rows in whatever order their storage produces them.
    fn visit_keys<V>(
        &self,
        keys: &[Key],
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized;

    fn with_range_scan<T, F>(
        &self,
        range: KeyRange,
        opts: ScanOptions<'_>,
        f: F,
    ) -> Result<T, BackendError>
    where
        F: FnOnce(&mut Self::RangeScan<'_>) -> Result<T, BackendError>;

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

pub trait BackendRangeScan {
    fn visit_next<V>(
        &mut self,
        limit_rows: usize,
        visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized;
}

#[derive(Clone, Debug, Default)]
pub struct BufferedRangeScan {
    rows: Vec<ReadEntry>,
    position: usize,
}

impl BufferedRangeScan {
    pub fn new(rows: Vec<ReadEntry>) -> Self {
        Self { rows, position: 0 }
    }
}

impl BackendRangeScan for BufferedRangeScan {
    fn visit_next<V>(
        &mut self,
        limit_rows: usize,
        visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized,
    {
        if limit_rows == 0 {
            return Ok(ScanResult::default());
        }

        let mut emitted = 0usize;
        while emitted < limit_rows {
            let Some(entry) = self.rows.get(self.position) else {
                break;
            };
            visitor.visit(entry.key.as_ref(), entry.value.as_ref())?;
            self.position += 1;
            emitted += 1;
        }

        Ok(ScanResult {
            emitted,
            has_more: self.position < self.rows.len(),
        })
    }
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
        keys,
        opts,
        &mut MaterializingPointVisitor {
            values: values.as_mut_slice(),
        },
    )?;
    Ok(GetManyResult::new(values))
}

pub fn visit_range<R>(
    read: &R,
    range: KeyRange,
    opts: ScanOptions<'_>,
    visitor: &mut dyn ScanVisitor,
) -> Result<ScanResult, BackendError>
where
    R: BackendRead,
{
    let limit_rows = opts.limit_rows;
    read.with_range_scan(range, opts, |cursor| cursor.visit_next(limit_rows, visitor))
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

pub trait BackendWrite {
    /// Stages the batch's entries for the transaction.
    ///
    /// Batches hold at most one mutation per key (the engine's write-set
    /// validation enforces this before lowering), so backends may reorder
    /// entries freely. The engine's write-set lowering additionally produces
    /// batches sorted by key ascending, so backends that want key order pay
    /// at most a linear verification pass on engine-produced batches. Other
    /// callers (e.g. the conformance suite) may pass unsorted batches.
    fn put_many(&mut self, entries: PutBatch) -> Result<(), BackendError>;

    /// Deletes the given keys.
    ///
    /// Like put batches, key sets hold at most one mutation per key
    /// (write-set validation rejects duplicates), and the engine's write-set
    /// lowering produces them sorted ascending.
    fn delete_many(&mut self, keys: &[Key]) -> Result<(), BackendError>;

    fn delete_range(&mut self, range: KeyRange) -> Result<(), BackendError>;

    fn commit(self) -> Result<CommitResult, BackendError>
    where
        Self: Sized;

    fn rollback(self) -> Result<(), BackendError>
    where
        Self: Sized;
}
