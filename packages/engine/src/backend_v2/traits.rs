use crate::backend_v2::{
    BackendCapabilities, BackendError, CommitResult, GetManyResult, GetOptions, Key, KeyRange,
    KeyRef, ProjectedValue, ProjectedValueRef, PutBatch, ReadEntry, ReadOptions, ScanOptions,
    ScanResult, WriteOptions,
};

pub trait Backend {
    type Read<'a>: BackendRead + 'a
    where
        Self: 'a;

    type Write<'a>: BackendWrite + 'a
    where
        Self: 'a;

    fn capabilities(&self) -> BackendCapabilities;

    fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, BackendError>;

    fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, BackendError>;
}

pub trait BackendRead {
    type ScanCursor<'a>: BackendScanCursor + 'a
    where
        Self: 'a;

    fn visit_many<V>(
        &self,
        keys: &[Key],
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized;

    fn open_scan_cursor(
        &self,
        range: KeyRange,
        opts: ScanOptions<'_>,
    ) -> Result<Self::ScanCursor<'_>, BackendError>;

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

pub trait BackendScanCursor {
    fn visit_next<V>(
        &mut self,
        limit_rows: usize,
        visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized;
}

#[derive(Clone, Debug, Default)]
pub struct BufferedScanCursor {
    rows: Vec<ReadEntry>,
    position: usize,
}

impl BufferedScanCursor {
    pub fn new(rows: Vec<ReadEntry>) -> Self {
        Self { rows, position: 0 }
    }
}

impl BackendScanCursor for BufferedScanCursor {
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
                *slot = value.map(|value| value.to_owned());
            }
            Ok(())
        }
    }

    let mut values = vec![None::<ProjectedValue>; keys.len()];
    read.visit_many(
        keys,
        opts,
        &mut MaterializingPointVisitor {
            values: values.as_mut_slice(),
        },
    )?;
    Ok(GetManyResult::new(values))
}

pub fn visit_range<R, V>(
    read: &R,
    range: KeyRange,
    opts: ScanOptions<'_>,
    visitor: &mut V,
) -> Result<ScanResult, BackendError>
where
    R: BackendRead + ?Sized,
    V: ScanVisitor + ?Sized,
{
    let limit_rows = opts.limit_rows;
    let mut cursor = read.open_scan_cursor(range, opts)?;
    cursor.visit_next(limit_rows, visitor)
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
    fn put_many(&mut self, entries: PutBatch) -> Result<(), BackendError>;

    fn delete_many(&mut self, keys: &[Key]) -> Result<(), BackendError>;

    fn delete_range(&mut self, range: KeyRange) -> Result<(), BackendError>;

    fn commit(self) -> Result<CommitResult, BackendError>
    where
        Self: Sized;

    fn rollback(self) -> Result<(), BackendError>
    where
        Self: Sized;
}
