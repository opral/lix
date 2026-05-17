use crate::backend_v2::{BackendError, BackendRead, KeyRange, Prefix, ScanOptions};
use crate::storage_v2::{with_prefix_scan, with_range_scan, StorageRangeScan, StorageSpace};

pub struct StorageReadScope<R> {
    read: R,
}

impl<R> StorageReadScope<R> {
    pub fn new(read: R) -> Self {
        Self { read }
    }

    pub(crate) fn backend_read(&self) -> &R {
        &self.read
    }
}

impl<R> StorageReadScope<R>
where
    R: BackendRead,
{
    pub fn close(self) -> Result<(), BackendError> {
        self.read.close()
    }

    pub fn with_range_scan<T, F>(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: ScanOptions<'_>,
        f: F,
    ) -> Result<T, BackendError>
    where
        F: FnOnce(&mut StorageRangeScan<'_, R::RangeScan<'_>>) -> Result<T, BackendError>,
    {
        with_range_scan(&self.read, space.id, range, opts, f)
    }

    pub fn with_prefix_scan<T, F>(
        &self,
        space: StorageSpace,
        prefix: Prefix,
        opts: ScanOptions<'_>,
        f: F,
    ) -> Result<T, BackendError>
    where
        F: FnOnce(&mut StorageRangeScan<'_, R::RangeScan<'_>>) -> Result<T, BackendError>,
    {
        with_prefix_scan(&self.read, space.id, prefix, opts, f)
    }
}
