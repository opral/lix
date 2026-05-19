use crate::backend::{
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
    type RangeScan<'cursor>: BackendRangeScan;

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
        keys: &'a [Key],
        values: &'a mut [Option<ProjectedValue>],
        visited: &'a mut [bool],
        visited_count: usize,
    }

    impl PointVisitor for MaterializingPointVisitor<'_> {
        fn visit(
            &mut self,
            index: usize,
            key: &Key,
            value: Option<ProjectedValueRef<'_>>,
        ) -> Result<(), BackendError> {
            let Some(expected_key) = self.keys.get(index) else {
                return Err(BackendError::Corruption(format!(
                    "backend point read visited out-of-range key index {index} for {} requested keys",
                    self.keys.len()
                )));
            };
            if expected_key != key {
                return Err(BackendError::Corruption(
                    "backend point read visited key that does not match requested index"
                        .to_string(),
                ));
            }
            let Some(slot) = self.values.get_mut(index) else {
                return Err(BackendError::Corruption(format!(
                    "backend point read collector has no value slot for key index {index}"
                )));
            };
            let Some(visited) = self.visited.get_mut(index) else {
                return Err(BackendError::Corruption(format!(
                    "backend point read collector has no visit slot for key index {index}"
                )));
            };
            if *visited {
                return Err(BackendError::Corruption(format!(
                    "backend point read visited key index {index} more than once"
                )));
            }
            *visited = true;
            self.visited_count += 1;
            *slot = value.map(|value| value.to_owned());
            Ok(())
        }
    }

    let mut values = vec![None::<ProjectedValue>; keys.len()];
    let mut visited = vec![false; keys.len()];
    let mut visitor = MaterializingPointVisitor {
        keys,
        values: values.as_mut_slice(),
        visited: visited.as_mut_slice(),
        visited_count: 0,
    };
    read.visit_keys(keys, opts, &mut visitor)?;
    if visitor.visited_count != keys.len() {
        let index = visitor
            .visited
            .iter()
            .position(|visited| !visited)
            .unwrap_or(visitor.visited.len());
        return Err(BackendError::Corruption(format!(
            "backend point read did not visit requested key index {index}"
        )));
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{BufferedRangeScan, KeyRange, ScanOptions};

    enum BrokenPointReadMode {
        Skip,
        Duplicate,
        OutOfRange,
        WrongKey,
    }

    struct BrokenPointRead {
        mode: BrokenPointReadMode,
    }

    impl BackendRead for BrokenPointRead {
        type RangeScan<'cursor> = BufferedRangeScan;

        fn visit_keys<V>(
            &self,
            keys: &[Key],
            _opts: GetOptions<'_>,
            visitor: &mut V,
        ) -> Result<(), BackendError>
        where
            V: PointVisitor + ?Sized,
        {
            match self.mode {
                BrokenPointReadMode::Skip => Ok(()),
                BrokenPointReadMode::Duplicate => {
                    visitor.visit(0, &keys[0], None)?;
                    visitor.visit(0, &keys[0], None)
                }
                BrokenPointReadMode::OutOfRange => visitor.visit(keys.len(), &keys[0], None),
                BrokenPointReadMode::WrongKey => {
                    let wrong_key = Key(bytes::Bytes::from_static(b"wrong-key"));
                    visitor.visit(0, &wrong_key, None)
                }
            }
        }

        fn with_range_scan<T, F>(
            &self,
            _range: KeyRange,
            _opts: ScanOptions<'_>,
            f: F,
        ) -> Result<T, BackendError>
        where
            F: FnOnce(&mut Self::RangeScan<'_>) -> Result<T, BackendError>,
        {
            f(&mut BufferedRangeScan::default())
        }
    }

    fn get_many_error(mode: BrokenPointReadMode) -> BackendError {
        let read = BrokenPointRead { mode };
        let keys = vec![Key(bytes::Bytes::from_static(b"key-1"))];
        get_many(&read, &keys, GetOptions::default())
            .expect_err("broken backend point-read visitor contract should be rejected")
    }

    #[test]
    fn backend_get_many_rejects_missing_backend_visit() {
        let error = get_many_error(BrokenPointReadMode::Skip);
        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("did not visit requested key index 0")
        ));
    }

    #[test]
    fn backend_get_many_rejects_duplicate_backend_visit() {
        let error = get_many_error(BrokenPointReadMode::Duplicate);
        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("visited key index 0 more than once")
        ));
    }

    #[test]
    fn backend_get_many_rejects_out_of_range_backend_visit() {
        let error = get_many_error(BrokenPointReadMode::OutOfRange);
        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("visited out-of-range key index 1")
        ));
    }

    #[test]
    fn backend_get_many_rejects_wrong_key_for_backend_visit_index() {
        let error = get_many_error(BrokenPointReadMode::WrongKey);
        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("visited key that does not match requested index")
        ));
    }
}
