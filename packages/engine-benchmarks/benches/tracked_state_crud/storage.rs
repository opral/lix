pub(crate) use lix_storage_rocksdb::RocksDB;
#[cfg(feature = "slatedb")]
pub(crate) use lix_storage_slatedb::{SlateDB, SlateDBIoCounters};
use tempfile::TempDir;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct BackendIoSnapshot {
    pub(crate) read_objects: u64,
    pub(crate) read_bytes: u64,
    pub(crate) write_objects: u64,
    pub(crate) write_bytes: u64,
}

impl BackendIoSnapshot {
    pub(crate) fn saturating_sub(self, earlier: Self) -> Self {
        Self {
            read_objects: self.read_objects.saturating_sub(earlier.read_objects),
            read_bytes: self.read_bytes.saturating_sub(earlier.read_bytes),
            write_objects: self.write_objects.saturating_sub(earlier.write_objects),
            write_bytes: self.write_bytes.saturating_sub(earlier.write_bytes),
        }
    }
}

#[derive(Clone, Default)]
pub(crate) enum BackendIoCounters {
    #[default]
    Unavailable,
    #[cfg(feature = "slatedb")]
    SlateDB(SlateDBIoCounters),
}

impl BackendIoCounters {
    pub(crate) fn snapshot(&self) -> Option<BackendIoSnapshot> {
        match self {
            Self::Unavailable => None,
            #[cfg(feature = "slatedb")]
            Self::SlateDB(counters) => {
                let snapshot = counters.snapshot();
                Some(BackendIoSnapshot {
                    read_objects: snapshot.read_objects,
                    read_bytes: snapshot.read_bytes,
                    write_objects: snapshot.write_objects,
                    write_bytes: snapshot.write_bytes,
                })
            }
        }
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

    pub(crate) fn durability_mode(self) -> &'static str {
        match self {
            Self::RocksDB => "rocksdb-default-options+explicit-flush",
            #[cfg(feature = "slatedb")]
            Self::SlateDB => "slatedb-default-options+explicit-flush",
            #[cfg(feature = "slatedb")]
            Self::SlateDBRemoteObjectStore => "slatedb-default-object-store-options+explicit-flush",
        }
    }
}

pub(crate) enum ProfileStorage {
    RocksDB {
        storage: RocksDB,
        _dir: TempDir,
        backend_counters: BackendIoCounters,
    },
    #[cfg(feature = "slatedb")]
    SlateDB {
        storage: SlateDB,
        _dir: TempDir,
        backend_counters: BackendIoCounters,
    },
}

impl StorageProfile {
    pub(crate) fn storage(self) -> ProfileStorage {
        match self {
            Self::RocksDB => {
                let dir = TempDir::new().expect("create rocksdb bench tempdir");
                let storage = RocksDB::open(dir.path().join("bench.rocksdb"))
                    .expect("open rocksdb bench storage");
                ProfileStorage::RocksDB {
                    storage,
                    _dir: dir,
                    backend_counters: BackendIoCounters::Unavailable,
                }
            }
            #[cfg(feature = "slatedb")]
            Self::SlateDB => {
                let dir = TempDir::new().expect("create slatedb bench tempdir");
                let counters = SlateDBIoCounters::default();
                let storage = SlateDB::open_with_io_counters(
                    dir.path().join("bench.slatedb"),
                    counters.clone(),
                )
                .expect("open slatedb bench storage");
                ProfileStorage::SlateDB {
                    storage,
                    _dir: dir,
                    backend_counters: BackendIoCounters::SlateDB(counters),
                }
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
                let counters = SlateDBIoCounters::default();
                let storage = SlateDB::open_object_store_with_options_and_io_counters(
                    db_path,
                    object_store,
                    lix_storage_slatedb::SlateDBObjectStoreOptions::default(),
                    counters.clone(),
                )
                .expect("open remote-path SlateDB object store");
                ProfileStorage::SlateDB {
                    storage,
                    _dir: dir,
                    backend_counters: BackendIoCounters::SlateDB(counters),
                }
            }
        }
    }
}
