use std::future::Future;
use std::sync::{Arc, Mutex};

use lix::storage::{
    BeginScanOptions, CommitResult, GetManyRequest, GetManyResult, Key, KeyRange, ProjectedValue,
    PutBatch, ReadOptions, ScanChunk, ScanCursor, Storage, StorageError, StorageRead,
    StorageScanSource, StorageWrite, WriteOptions,
};

pub(crate) use lix_storage_rocksdb::RocksDB;
#[cfg(feature = "slatedb")]
pub(crate) use lix_storage_slatedb::SlateDB;
use tempfile::TempDir;

#[derive(Clone, Debug, Default)]
pub(crate) struct IoStats {
    pub(crate) get_calls: u64,
    pub(crate) get_keys: u64,
    pub(crate) get_values: u64,
    pub(crate) get_value_bytes: u64,
    pub(crate) scan_calls: u64,
    pub(crate) scan_entries: u64,
    pub(crate) scan_value_bytes: u64,
    pub(crate) write_batches: u64,
    pub(crate) write_bytes: u64,
}

#[derive(Clone)]
pub(crate) struct CountingStorage<S> {
    inner: S,
    stats: Arc<Mutex<IoStats>>,
}

pub(crate) struct CountingRead<R> {
    inner: R,
    stats: Arc<Mutex<IoStats>>,
}

pub(crate) struct CountingWrite<W> {
    inner: W,
    stats: Arc<Mutex<IoStats>>,
}

impl<S> CountingStorage<S> {
    pub(crate) fn new(inner: S) -> (Self, Arc<Mutex<IoStats>>) {
        let stats = Arc::new(Mutex::new(IoStats::default()));
        (
            Self {
                inner,
                stats: Arc::clone(&stats),
            },
            stats,
        )
    }
}

impl<S: Clone> CountingStorage<S> {
    pub(crate) fn raw_clone(&self) -> S {
        self.inner.clone()
    }
}

impl<S> Storage for CountingStorage<S>
where
    S: Storage,
{
    type Read<'a>
        = CountingRead<S::Read<'a>>
    where
        Self: 'a;
    type Write<'a>
        = CountingWrite<S::Write<'a>>
    where
        Self: 'a;

    async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        Ok(CountingRead {
            inner: self.inner.begin_read(options).await?,
            stats: Arc::clone(&self.stats),
        })
    }

    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        Ok(CountingWrite {
            inner: self.inner.begin_write(options).await?,
            stats: Arc::clone(&self.stats),
        })
    }
}

impl<R> StorageRead for CountingRead<R>
where
    R: StorageRead,
{
    async fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        {
            let mut stats = self.stats.lock().expect("OLAP I/O stats mutex");
            stats.get_calls += 1;
            stats.get_keys += requests
                .iter()
                .map(|request| request.keys.len() as u64)
                .sum::<u64>();
        }
        let result = self.inner.get_many(requests).await?;
        {
            let mut stats = self.stats.lock().expect("OLAP I/O stats mutex");
            for value in result.values.iter().flatten() {
                stats.get_values += 1;
                stats.get_value_bytes += projected_value_len(value) as u64;
            }
        }
        Ok(result)
    }

    async fn begin_scan(
        &self,
        space: lix::storage::StorageSpace,
        range: KeyRange,
        options: BeginScanOptions,
    ) -> Result<ScanCursor<'_>, StorageError> {
        let order = options.order;
        self.stats
            .lock()
            .expect("OLAP I/O stats mutex")
            .scan_calls += 1;
        let inner = self.inner.begin_scan(space, range.clone(), options).await?;
        ScanCursor::from_source(
            range,
            order,
            CountingScanSource {
                inner,
                stats: Arc::clone(&self.stats),
            },
        )
    }
}

struct CountingScanSource<'a> {
    inner: ScanCursor<'a>,
    stats: Arc<Mutex<IoStats>>,
}

impl StorageScanSource for CountingScanSource<'_> {
    fn next_page(
        &mut self,
        limit_rows: usize,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<ScanChunk, StorageError>> + Send + '_>> {
        Box::pin(async move {
            let (chunk, chunk_has_more) = self.inner.next_page(limit_rows).await?.into_parts();
            let mut stats = self.stats.lock().expect("OLAP I/O stats mutex");
            stats.scan_entries += chunk.len() as u64;
            stats.scan_value_bytes += chunk
                .iter()
                .map(|entry| projected_value_len(&entry.value) as u64)
                .sum::<u64>();
            drop(stats);
            Ok(ScanChunk::new(chunk, chunk_has_more))
        })
    }
}

impl<W> StorageWrite for CountingWrite<W>
where
    W: StorageWrite,
{
    async fn put_many(
        &mut self,
        space: lix::storage::StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        {
            let mut stats = self.stats.lock().expect("OLAP I/O stats mutex");
            stats.write_batches += 1;
            stats.write_bytes += entries
                .entries
                .iter()
                .map(|entry| (entry.key.0.len() + entry.value.bytes.len()) as u64)
                .sum::<u64>();
        }
        self.inner.put_many(space, entries).await
    }

    async fn delete_many(
        &mut self,
        space: lix::storage::StorageSpace,
        keys: &[Key],
    ) -> Result<(), StorageError> {
        {
            let mut stats = self.stats.lock().expect("OLAP I/O stats mutex");
            stats.write_batches += 1;
            stats.write_bytes += keys.iter().map(|key| key.0.len() as u64).sum::<u64>();
        }
        self.inner.delete_many(space, keys).await
    }

    async fn delete_range(
        &mut self,
        space: lix::storage::StorageSpace,
        range: KeyRange,
    ) -> Result<(), StorageError> {
        self.stats
            .lock()
            .expect("OLAP I/O stats mutex")
            .write_batches += 1;
        self.inner.delete_range(space, range).await
    }

    async fn commit(self) -> Result<CommitResult, StorageError>
    where
        Self: Sized,
    {
        self.inner.commit().await
    }

    async fn rollback(self) -> Result<(), StorageError>
    where
        Self: Sized,
    {
        self.inner.rollback().await
    }
}

#[derive(Clone, Copy)]
pub(crate) enum StorageProfile {
    RocksDB,
    #[cfg(feature = "slatedb")]
    SlateDB,
    #[cfg(feature = "slatedb")]
    SlateDBRemoteObjectStore,
}

pub(crate) const KV_STORAGE_PROFILES: &[StorageProfile] = &[StorageProfile::RocksDB];

#[cfg(not(feature = "slatedb"))]
pub(crate) const STORAGE_PROFILES: &[StorageProfile] = &[StorageProfile::RocksDB];
#[cfg(feature = "slatedb")]
pub(crate) const STORAGE_PROFILES: &[StorageProfile] =
    &[StorageProfile::RocksDB, StorageProfile::SlateDB];

impl StorageProfile {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::RocksDB => "lix_rocksdb",
            #[cfg(feature = "slatedb")]
            Self::SlateDB => "lix_slatedb",
            #[cfg(feature = "slatedb")]
            Self::SlateDBRemoteObjectStore => "lix_slatedb_remote_object_store",
        }
    }
}

pub(crate) enum ProfileStorage {
    RocksDB {
        storage: RocksDB,
        _dir: TempDir,
    },
    #[cfg(feature = "slatedb")]
    SlateDB {
        storage: SlateDB,
        _dir: TempDir,
    },
}

pub(crate) enum CountedProfileStorage {
    RocksDB {
        storage: CountingStorage<RocksDB>,
        stats: Arc<Mutex<IoStats>>,
        _dir: TempDir,
    },
    #[cfg(feature = "slatedb")]
    SlateDB {
        storage: CountingStorage<SlateDB>,
        stats: Arc<Mutex<IoStats>>,
        _dir: TempDir,
    },
}

impl StorageProfile {
    pub(crate) fn storage(self) -> ProfileStorage {
        match self {
            Self::RocksDB => {
                let dir = TempDir::new().expect("create rocksdb bench tempdir");
                let storage = RocksDB::open(dir.path().join("bench.rocksdb"))
                    .expect("open rocksdb bench storage");
                ProfileStorage::RocksDB { storage, _dir: dir }
            }
            #[cfg(feature = "slatedb")]
            Self::SlateDB => {
                let dir = TempDir::new().expect("create slatedb bench tempdir");
                let storage =
                    SlateDB::open(dir.path().join("bench.slatedb")).expect("open slatedb storage");
                ProfileStorage::SlateDB { storage, _dir: dir }
            }
            #[cfg(feature = "slatedb")]
            Self::SlateDBRemoteObjectStore => {
                use object_store::memory::InMemory;
                use object_store::throttle::{ThrottleConfig, ThrottledStore};
                use std::sync::Arc;
                use std::time::Duration;

                let latency_ms = std::env::var("LIX_TRACKED_STATE_CRUD_REMOTE_LATENCY_MS")
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(5);
                let latency = Duration::from_millis(latency_ms);
                let object_store = Arc::new(ThrottledStore::new(
                    InMemory::new(),
                    ThrottleConfig {
                        wait_delete_per_call: latency,
                        wait_get_per_call: latency,
                        wait_list_per_call: latency,
                        wait_list_with_delimiter_per_call: latency,
                        wait_put_per_call: latency,
                        ..ThrottleConfig::default()
                    },
                ));
                let dir = TempDir::new().expect("create remote SlateDB bench tempdir");
                let db_path = format!("tracked-state-crud-{}", ulid::Ulid::new());
                let storage = SlateDB::open_object_store_with_options(
                    db_path,
                    object_store,
                    lix_storage_slatedb::SlateDBObjectStoreOptions::default(),
                )
                .expect("open remote-path SlateDB object store");
                ProfileStorage::SlateDB { storage, _dir: dir }
            }
        }
    }

    pub(crate) fn counted_storage(self) -> CountedProfileStorage {
        match self {
            Self::RocksDB => {
                let dir = TempDir::new().expect("create counted rocksdb bench tempdir");
                let raw = RocksDB::open(dir.path().join("bench.rocksdb"))
                    .expect("open counted rocksdb bench storage");
                let (storage, stats) = CountingStorage::new(raw);
                CountedProfileStorage::RocksDB {
                    storage,
                    stats,
                    _dir: dir,
                }
            }
            #[cfg(feature = "slatedb")]
            Self::SlateDB => {
                let dir = TempDir::new().expect("create counted slatedb bench tempdir");
                let raw = SlateDB::open(dir.path().join("bench.slatedb"))
                    .expect("open counted slatedb bench storage");
                let (storage, stats) = CountingStorage::new(raw);
                CountedProfileStorage::SlateDB {
                    storage,
                    stats,
                    _dir: dir,
                }
            }
            #[cfg(feature = "slatedb")]
            Self::SlateDBRemoteObjectStore => {
                panic!("counted remote SlateDB is outside the OLAP baseline")
            }
        }
    }
}

fn projected_value_len(value: &ProjectedValue) -> usize {
    match value {
        ProjectedValue::KeyOnly => 0,
        ProjectedValue::FullValue(value) => value.len(),
    }
}
