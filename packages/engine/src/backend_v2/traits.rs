use crate::backend_v2::{
    BackendCapabilities, BackendError, Capability, CommitResult, GetManyResult, GetOptions,
    Key, KeyRange, Precondition, PreconditionSupportReport, Prefix, PutBatch, ReadOptions,
    ScanOptions, ScanPage, SpaceId, WriteOptions,
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

    fn scan_range(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions<'_>,
    ) -> Result<ScanPage, BackendError>;

    fn scan_prefix(
        &self,
        space: SpaceId,
        prefix: Prefix,
        opts: ScanOptions<'_>,
    ) -> Result<ScanPage, BackendError> {
        self.scan_range(space, prefix.to_range()?, opts)
    }

    fn close(self) -> Result<(), BackendError>
    where
        Self: Sized,
    {
        Ok(())
    }
}

pub trait BackendWrite: BackendRead {
    fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), BackendError>;

    fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), BackendError>;

    fn delete_range(&mut self, space: SpaceId, range: KeyRange) -> Result<(), BackendError> {
        let _ = (space, range);
        Err(BackendError::Unsupported(Capability::DeleteRange))
    }

    fn require(
        &mut self,
        preconditions: &[Precondition],
    ) -> Result<PreconditionSupportReport, BackendError> {
        let _ = preconditions;
        Err(BackendError::Unsupported(Capability::Preconditions))
    }

    fn commit(self) -> Result<CommitResult, BackendError>
    where
        Self: Sized;

    fn rollback(self) -> Result<(), BackendError>
    where
        Self: Sized;
}
