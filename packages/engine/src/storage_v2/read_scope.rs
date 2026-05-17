use crate::backend_v2::{BackendError, BackendRead, KeyRange, Prefix, ScanOptions};
use crate::storage_v2::{
    open_scan_prefix_cursor, open_scan_range_cursor, StorageScanCursor, StorageSpace,
};

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

    pub fn open_scan_range_cursor(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: ScanOptions<'_>,
    ) -> Result<StorageScanCursor<R::ScanCursor<'_>>, BackendError> {
        open_scan_range_cursor(&self.read, space.id, range, opts)
    }

    pub fn open_scan_prefix_cursor(
        &self,
        space: StorageSpace,
        prefix: Prefix,
        opts: ScanOptions<'_>,
    ) -> Result<StorageScanCursor<R::ScanCursor<'_>>, BackendError> {
        open_scan_prefix_cursor(&self.read, space.id, prefix, opts)
    }
}
