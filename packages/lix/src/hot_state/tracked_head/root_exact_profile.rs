//! Test-only local I/O attribution for root-backed exact reads. The profile
//! gate uses a current-thread runtime; no production cache/read behavior changes.
use crate::storage_adapter::StorageAdapterRead;
use crate::storage_adapter::{
    StorageBeginScanOptions as BeginScanOptions, StorageError,
    StorageGetManyRequest as GetManyRequest, StorageGetManyResult as GetManyResult,
    StorageKeyRange as KeyRange, StorageScanCursor as ScanCursor, StorageSpace,
};
use std::cell::RefCell;
use std::time::Instant;

#[derive(Clone, Debug, Default, serde::Serialize)]
pub(crate) struct SpaceProfile {
    pub batches: u64,
    pub keys: u64,
    pub hits: u64,
    pub bytes: u64,
}
#[derive(Clone, Debug, Default, serde::Serialize)]
pub(crate) struct Profile {
    pub calls: u64,
    pub keys: u64,
    pub elapsed_ns: u64,
    pub get_calls: u64,
    pub get_keys: u64,
    pub get_hits: u64,
    pub get_bytes: u64,
    pub get_elapsed_ns: u64,
    pub scans: u64,
    pub spaces: std::collections::BTreeMap<String, SpaceProfile>,
}
impl Profile {
    pub(crate) fn add(&mut self, other: Self) {
        self.calls += other.calls;
        self.keys += other.keys;
        self.elapsed_ns += other.elapsed_ns;
        self.get_calls += other.get_calls;
        self.get_keys += other.get_keys;
        self.get_hits += other.get_hits;
        self.get_bytes += other.get_bytes;
        self.get_elapsed_ns += other.get_elapsed_ns;
        self.scans += other.scans;
        for (name, other) in other.spaces {
            let space = self.spaces.entry(name).or_default();
            space.batches += other.batches;
            space.keys += other.keys;
            space.hits += other.hits;
            space.bytes += other.bytes;
        }
    }
}
thread_local! { static ACTIVE: RefCell<Option<Profile>> = const { RefCell::new(None) }; }
pub(crate) fn begin() {
    ACTIVE.with(|active| *active.borrow_mut() = Some(Profile::default()));
}
pub(crate) fn take() -> Profile {
    ACTIVE.with(|active| active.borrow_mut().take().unwrap_or_default())
}
fn record(f: impl FnOnce(&mut Profile)) {
    ACTIVE.with(|active| {
        if let Some(profile) = active.borrow_mut().as_mut() {
            f(profile);
        }
    });
}
pub(super) struct Read<'a, R: ?Sized> {
    inner: &'a R,
    started: Instant,
}
impl<'a, R: ?Sized> Read<'a, R> {
    pub(super) fn new(inner: &'a R, keys: usize) -> Self {
        record(|profile| {
            profile.calls += 1;
            profile.keys += keys as u64;
        });
        Self {
            inner,
            started: Instant::now(),
        }
    }
}
impl<R: ?Sized> Drop for Read<'_, R> {
    fn drop(&mut self) {
        record(|profile| profile.elapsed_ns += self.started.elapsed().as_nanos() as u64);
    }
}
impl<R: StorageAdapterRead + ?Sized> StorageAdapterRead for Read<'_, R> {
    fn snapshot_cache_key(&self) -> Option<u128> {
        self.inner.snapshot_cache_key()
    }
    async fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        let started = Instant::now();
        let result = self.inner.get_many(requests).await;
        record(|profile| {
            profile.get_calls += 1;
            profile.get_keys += requests
                .iter()
                .map(|request| request.keys.len() as u64)
                .sum::<u64>();
            profile.get_elapsed_ns += started.elapsed().as_nanos() as u64;
            let mut offset = 0;
            for request in requests {
                let space = profile
                    .spaces
                    .entry(request.space.name.to_owned())
                    .or_default();
                space.batches += 1;
                space.keys += request.keys.len() as u64;
                if let Ok(result) = &result {
                    for value in result
                        .values
                        .iter()
                        .skip(offset)
                        .take(request.keys.len())
                        .flatten()
                    {
                        space.hits += 1;
                        if let crate::storage_adapter::StorageProjectedValue::FullValue(bytes) =
                            value
                        {
                            space.bytes += bytes.len() as u64;
                        }
                    }
                }
                offset += request.keys.len();
            }
            if let Ok(result) = &result {
                for value in result.values.iter().flatten() {
                    profile.get_hits += 1;
                    if let crate::storage_adapter::StorageProjectedValue::FullValue(bytes) = value {
                        profile.get_bytes += bytes.len() as u64;
                    }
                }
            }
        });
        result
    }
    async fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> Result<ScanCursor<'_>, StorageError> {
        record(|profile| profile.scans += 1);
        self.inner.begin_scan(space, range, opts).await
    }
}
