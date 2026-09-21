use std::sync::Arc;

use crate::storage::{
    BeginScanOptions, GetManyRequest, GetManyResult, KeyRange, ScanCursor, StorageError,
    StorageRead, StorageSpace,
};

use super::epoch::EpochRouting;

/// The async read capability consumed by engine stores.
///
/// Implementations preserve one coherent storage read view while allowing
/// independent point and scan requests to overlap.
pub trait StorageAdapterRead: Send + Sync {
    /// Require callers that can satisfy a read from a decoded/global cache to
    /// perform the underlying storage read as well. Read discovery uses this
    /// to make every immutable native input observable in its dependency
    /// closure; ordinary readers keep the cache-friendly default.
    fn requires_physical_reads(&self) -> bool {
        false
    }

    fn snapshot_cache_key(&self) -> Option<u128> {
        None
    }

    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send;

    fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send;
}

#[derive(Debug)]
pub struct StorageAdapterReadScope<R> {
    read: R,
    routing: EpochRouting,
}

impl<R> StorageAdapterReadScope<R> {
    pub fn new(read: R) -> Self {
        Self {
            read,
            routing: EpochRouting::legacy(),
        }
    }

    pub(super) fn with_routing(read: R, routing: EpochRouting) -> Self {
        Self { read, routing }
    }
}

/// Cloneable SQL/DataFusion bridge for one execution-scoped storage read.
///
/// Clones share the read handle directly. Concurrency and synchronization are
/// storage responsibilities; this layer never serializes requests.
pub(crate) struct SharedStorageAdapterRead<R>
where
    R: StorageRead,
{
    read: Arc<StorageAdapterReadScope<R>>,
    hydrated: Option<Arc<StorageAdapterReadScope<R>>>,
    hydrated_previous: Option<Arc<SharedStorageAdapterRead<R>>>,
    hydrated_keys: Option<Arc<std::collections::BTreeSet<(StorageSpace, crate::storage::Key)>>>,
    hydrated_prefixes: Option<Arc<Vec<(StorageSpace, bytes::Bytes)>>>,
}

impl<R> SharedStorageAdapterRead<R>
where
    R: StorageRead,
{
    pub(crate) fn new(read: StorageAdapterReadScope<R>) -> Self {
        Self {
            read: Arc::new(read),
            hydrated: None,
            hydrated_previous: None,
            hydrated_keys: None,
            hydrated_prefixes: None,
        }
    }

    /// Add only exact immutable inputs already selected by the pinned SQL
    /// snapshot. Mutable coordinates and all scans stay on the original read.
    pub(crate) fn with_hydrated_keys(
        &self,
        read: StorageAdapterReadScope<R>,
        keys: impl IntoIterator<Item = (StorageSpace, crate::storage::Key)>,
    ) -> Self {
        let mut hydrated_keys = self
            .hydrated_keys
            .as_ref()
            .map(|keys| keys.as_ref().clone())
            .unwrap_or_default();
        hydrated_keys.extend(keys);
        Self {
            read: self.read.clone(),
            hydrated: Some(Arc::new(read)),
            hydrated_previous: self.hydrated.as_ref().map(|_| Arc::new(self.clone())),
            hydrated_keys: Some(Arc::new(hydrated_keys)),
            hydrated_prefixes: self.hydrated_prefixes.clone(),
        }
    }

    pub(crate) fn with_hydrated_prefix(
        mut self,
        space: StorageSpace,
        prefix: bytes::Bytes,
    ) -> Self {
        Arc::make_mut(self.hydrated_prefixes.get_or_insert_with(Default::default))
            .push((space, prefix));
        self
    }

    pub(crate) fn finish(self) -> Result<(), StorageError> {
        // Prior hydrated scopes retain the opening read for exact-key fallback.
        // Release our own chain before checking for external borrowers.
        drop(self.hydrated_previous);
        drop(self.hydrated);
        let read = Arc::try_unwrap(self.read).map_err(|read| {
            StorageError::Io(format!(
                "shared storage read still has {} active handles",
                Arc::strong_count(&read) - 1
            ))
        })?;
        drop(read);
        Ok(())
    }
}

impl<R> Clone for SharedStorageAdapterRead<R>
where
    R: StorageRead,
{
    fn clone(&self) -> Self {
        Self {
            read: Arc::clone(&self.read),
            hydrated: self.hydrated.clone(),
            hydrated_previous: self.hydrated_previous.clone(),
            hydrated_keys: self.hydrated_keys.clone(),
            hydrated_prefixes: self.hydrated_prefixes.clone(),
        }
    }
}

/// Charges one adapter point-read batch to the active plan-load phase.
///
/// This is the single boundary every engine point read crosses, so counting
/// here answers "how many physical reads does one plan load issue" without
/// trusting a hand audit of the call tree.
#[cfg(feature = "root-replay-trace")]
async fn traced_get_many<F>(
    requests: &[GetManyRequest<'_>],
    future: F,
) -> Result<GetManyResult, StorageError>
where
    F: Future<Output = Result<GetManyResult, StorageError>>,
{
    let keys = requests
        .iter()
        .map(|request| request.keys.len() as u64)
        .sum::<u64>();
    let start = std::time::Instant::now();
    let result = future.await;
    let nanos = start.elapsed().as_nanos() as u64;
    let (hits, bytes) = match result.as_ref() {
        Ok(result) => result
            .values
            .iter()
            .flatten()
            .fold((0u64, 0u64), |(hits, bytes), value| match value {
                crate::storage::ProjectedValue::KeyOnly => (hits + 1, bytes),
                crate::storage::ProjectedValue::FullValue(payload) => {
                    (hits + 1, bytes + payload.len() as u64)
                }
            }),
        Err(_) => (0, 0),
    };
    crate::storage_bench::record_plan_load_io(nanos, requests.len() as u64, keys, hits, bytes);
    result
}

impl<R> StorageAdapterRead for StorageAdapterReadScope<R>
where
    R: StorageRead,
{
    fn snapshot_cache_key(&self) -> Option<u128> {
        self.routing
            .mix_snapshot_cache_key(self.read.snapshot_cache_key())
    }

    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_checkpoint_point_read(
            requests.len(),
            requests.iter().map(|request| request.keys.len()).sum(),
        );
        async move {
            let requests = requests
                .iter()
                .map(|request| GetManyRequest {
                    space: self.routing.map_space(request.space),
                    keys: request.keys,
                    opts: request.opts,
                })
                .collect::<Vec<_>>();
            #[cfg(feature = "root-replay-trace")]
            {
                traced_get_many(&requests, self.read.get_many(&requests)).await
            }
            #[cfg(not(feature = "root-replay-trace"))]
            {
                self.read.get_many(&requests).await
            }
        }
    }

    fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send {
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_checkpoint_scan_start();
        self.read
            .begin_scan(self.routing.map_space(space), range, opts)
    }
}

impl<R> StorageAdapterRead for SharedStorageAdapterRead<R>
where
    R: StorageRead,
{
    fn snapshot_cache_key(&self) -> Option<u128> {
        // A mixed read must never share caches with the fresh mutable
        // snapshot used to fetch its immutable inputs.
        if self.hydrated.is_some() {
            None
        } else {
            StorageAdapterRead::snapshot_cache_key(self.read.as_ref())
        }
    }

    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        if self.hydrated.is_none() {
            return futures_util::future::Either::Left(
                StorageAdapterRead::get_many(self.read.as_ref(), requests));
        }
        // Keep the ordinary read future small and allocation-free. The larger
        // pinned-hydration state machine must not inflate every SQL/commit poll.
        futures_util::future::Either::Right(Box::pin(async move {
            let mut result = StorageAdapterRead::get_many(self.read.as_ref(), requests).await?;
            let Some(hydrated) = &self.hydrated else {
                return Ok(result);
            };
            let mut indices = Vec::new();
            let mut missing = Vec::new();
            let mut offset = 0;
            for request in requests {
                for (index, key) in request.keys.iter().enumerate() {
                    if result.values[offset + index].is_none()
                        && self
                            .hydrated_keys
                            .as_ref()
                            .is_some_and(|keys| keys.contains(&(request.space, key.clone())))
                    {
                        indices.push(offset + index);
                        missing.push(GetManyRequest {
                            space: request.space,
                            keys: std::slice::from_ref(key),
                            opts: request.opts,
                        });
                    }
                }
                offset += request.keys.len();
            }
            if !missing.is_empty() {
                let fetched = StorageAdapterRead::get_many(hydrated.as_ref(), &missing).await?;
                let prior = if let Some(previous) = &self.hydrated_previous {
                    Some(Box::pin(StorageAdapterRead::get_many(previous.as_ref(), &missing)).await?)
                } else {
                    None
                };
                for (position, (index, value)) in
                    indices.into_iter().zip(fetched.values).enumerate()
                {
                    result.values[index] = value.or_else(|| {
                        prior
                            .as_ref()
                            .and_then(|prior| prior.values[position].clone())
                    });
                }
            }
            Ok(result)
        }))
    }

    fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send {
        if self.hydrated.is_none() {
            return futures_util::future::Either::Left(
                StorageAdapterRead::begin_scan(self.read.as_ref(), space, range, opts));
        }
        futures_util::future::Either::Right(Box::pin(async move {
            // A prior manifest scan keeps its original immutable rows even if
            // a later hydration snapshot observes local cache reclamation.
            if let Some(previous) = &self.hydrated_previous {
                if previous
                    .hydrated_prefixes
                    .iter()
                    .flat_map(|prefixes| prefixes.iter())
                    .any(|(allowed_space, prefix)| {
                        space == *allowed_space && range_within_prefix(&range, prefix)
                    })
                {
                    return Box::pin(StorageAdapterRead::begin_scan(
                        previous.as_ref(),
                        space,
                        range,
                        opts,
                    ))
                    .await;
                }
            }
            if let Some(hydrated) = &self.hydrated {
                for (allowed_space, prefix) in self
                    .hydrated_prefixes
                    .iter()
                    .flat_map(|prefixes| prefixes.iter())
                {
                    if space == *allowed_space && range_within_prefix(&range, prefix) {
                        return StorageAdapterRead::begin_scan(
                            hydrated.as_ref(),
                            space,
                            range,
                            opts,
                        )
                        .await;
                    }
                }
            }
            StorageAdapterRead::begin_scan(self.read.as_ref(), space, range, opts).await
        }))
    }
}

fn range_within_prefix(range: &KeyRange, prefix: &bytes::Bytes) -> bool {
    use std::ops::Bound;
    let lower = match &range.lower {
        Bound::Included(key) | Bound::Excluded(key) => key.0.starts_with(prefix),
        Bound::Unbounded => false,
    };
    let upper = match &range.upper {
        Bound::Included(key) => key.0.starts_with(prefix),
        Bound::Excluded(key) => {
            key.0.starts_with(prefix)
                || crate::storage::Prefix {
                    bytes: prefix.clone(),
                }
                .to_range()
                .ok()
                .is_some_and(|allowed| allowed.upper == range.upper)
        }
        Bound::Unbounded => false,
    };
    lower && upper
}

impl<T> StorageAdapterRead for &T
where
    T: StorageAdapterRead + ?Sized,
{
    fn requires_physical_reads(&self) -> bool {
        (*self).requires_physical_reads()
    }

    fn snapshot_cache_key(&self) -> Option<u128> {
        (*self).snapshot_cache_key()
    }

    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        (*self).get_many(requests)
    }

    fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send {
        (*self).begin_scan(space, range, opts)
    }
}

impl<T> StorageAdapterRead for &mut T
where
    T: StorageAdapterRead + ?Sized,
{
    fn requires_physical_reads(&self) -> bool {
        (**self).requires_physical_reads()
    }

    fn snapshot_cache_key(&self) -> Option<u128> {
        (**self).snapshot_cache_key()
    }

    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        (**self).get_many(requests)
    }

    fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send {
        (**self).begin_scan(space, range, opts)
    }
}

impl<T> StorageAdapterRead for Arc<T>
where
    T: StorageAdapterRead + ?Sized,
{
    fn requires_physical_reads(&self) -> bool {
        self.as_ref().requires_physical_reads()
    }

    fn snapshot_cache_key(&self) -> Option<u128> {
        self.as_ref().snapshot_cache_key()
    }

    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        self.as_ref().get_many(requests)
    }

    fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send {
        self.as_ref().begin_scan(space, range, opts)
    }
}

#[cfg(test)]
mod hydration_tests {
    use super::*;
    use crate::storage::{Key, ProjectedValue};
    use crate::storage_adapter::{Memory, StorageAdapter, StorageWriteOptions};
    use bytes::Bytes;

    #[tokio::test]
    async fn hydrated_keys_preserve_existing_values_and_hide_unrequested_additions() {
        let storage = StorageAdapter::new(Memory::new());
        let space = crate::changelog::COMMIT_SPACE;
        let keys = ["existing", "requested", "unrequested"].map(|key| Key(Bytes::from(key)));
        let mut writes = storage.new_write_set();
        writes.put(space, keys[0].clone(), b"old".as_slice());
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let pinned =
            SharedStorageAdapterRead::new(storage.begin_read(Default::default()).await.unwrap());
        let mut writes = storage.new_write_set();
        for key in &keys {
            writes.put(space, key.clone(), b"new".as_slice());
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let hydrated = pinned.with_hydrated_keys(
            storage.begin_read(Default::default()).await.unwrap(),
            [(space, keys[0].clone()), (space, keys[1].clone())],
        );
        let result = hydrated
            .get_many(&[GetManyRequest {
                space,
                keys: &keys,
                opts: Default::default(),
            }])
            .await
            .unwrap();
        assert_eq!(
            result.values,
            vec![
                Some(ProjectedValue::FullValue(Bytes::from_static(b"old"))),
                Some(ProjectedValue::FullValue(Bytes::from_static(b"new"))),
                None,
            ]
        );
        assert_eq!(
            hydrated.snapshot_cache_key(),
            None,
            "mixed reads cannot poison a fresh snapshot's cache"
        );
        assert!(
            pinned
                .get_many(&[GetManyRequest {
                    space,
                    keys: &keys[1..2],
                    opts: Default::default()
                }])
                .await
                .unwrap()
                .values[0]
                .is_none()
        );
        let refreshed =
            hydrated.with_hydrated_keys(storage.begin_read(Default::default()).await.unwrap(), []);
        drop(hydrated);
        drop(pinned);
        refreshed
            .finish()
            .expect("owned hydration history is not an external read borrower");
    }
}
