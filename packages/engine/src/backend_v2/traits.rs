use crate::backend_v2::{
    BackendCapabilities, BackendError, CommitResult, GetManyResult, GetOptions, Key, KeyRange,
    ProjectedValueRef, PutBatch, ReadOptions, ScanOptions, ScanResult, SpaceId, WriteOptions,
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
    fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions<'_>,
    ) -> Result<GetManyResult, BackendError>;

    fn visit_range<V>(
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
    fn visit(&mut self, key: &Key, value: ProjectedValueRef<'_>) -> Result<(), BackendError>;
}

impl<F> ScanVisitor for F
where
    F: for<'a> FnMut(&Key, ProjectedValueRef<'a>) -> Result<(), BackendError>,
{
    fn visit(&mut self, key: &Key, value: ProjectedValueRef<'_>) -> Result<(), BackendError> {
        self(key, value)
    }
}

pub trait BackendWrite {
    fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), BackendError>;

    fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), BackendError>;

    fn commit(self) -> Result<CommitResult, BackendError>
    where
        Self: Sized;

    fn rollback(self) -> Result<(), BackendError>
    where
        Self: Sized;
}
