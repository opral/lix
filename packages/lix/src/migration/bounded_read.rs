use crate::LixError;
use crate::storage_adapter::{
    StorageAdapterRead, StorageBeginScanOptions, StorageError, StorageGetManyRequest,
    StorageGetManyResult, StorageKeyRange, StorageProjectedValue, StorageScanCursor, StorageSpace,
};
use std::sync::Mutex;

pub(crate) struct BoundedRead<'a, R: ?Sized> {
    read: &'a R,
    used: Mutex<(usize, usize)>,
    limits: (usize, usize),
}

impl<'a, R: ?Sized> BoundedRead<'a, R> {
    pub(crate) fn new(read: &'a R, entries: usize, bytes: usize) -> Self {
        Self {
            read,
            used: Mutex::new((0, 0)),
            limits: (entries, bytes),
        }
    }

    pub(crate) fn charge(&self, entries: usize, bytes: usize) -> Result<(), StorageError> {
        let mut used = self.used.lock().expect("migration budget lock");
        used.0 = used.0.saturating_add(entries);
        used.1 = used.1.saturating_add(bytes);
        if used.0 > self.limits.0 || used.1 > self.limits.1 {
            return Err(StorageError::Io("migration budget exceeded".into()));
        }
        Ok(())
    }

    pub(crate) fn usage(&self) -> Result<(usize, usize), LixError> {
        let used = *self.used.lock().expect("migration budget lock");
        if used.0 > self.limits.0 || used.1 > self.limits.1 {
            return Err(LixError::new(
                "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
                "migration exceeds preflight limits",
            ));
        }
        Ok(used)
    }
}

impl<R: StorageAdapterRead + ?Sized> StorageAdapterRead for BoundedRead<'_, R> {
    async fn get_many(
        &self,
        requests: &[StorageGetManyRequest<'_>],
    ) -> Result<StorageGetManyResult, StorageError> {
        self.charge(
            requests.iter().map(|request| request.keys.len()).sum(),
            requests
                .iter()
                .flat_map(|request| request.keys)
                .map(|key| key.0.len())
                .sum(),
        )?;
        let result = self.read.get_many(requests).await?;
        self.charge(
            0,
            result
                .values
                .iter()
                .flatten()
                .map(|value| match value {
                    StorageProjectedValue::FullValue(bytes) => bytes.len(),
                    StorageProjectedValue::KeyOnly => 0,
                })
                .sum(),
        )?;
        Ok(result)
    }

    async fn begin_scan(
        &self,
        _space: StorageSpace,
        _range: StorageKeyRange,
        _opts: StorageBeginScanOptions,
    ) -> Result<StorageScanCursor<'_>, StorageError> {
        Err(StorageError::Io(
            "migration bounded reader requires point reads".into(),
        ))
    }
}
