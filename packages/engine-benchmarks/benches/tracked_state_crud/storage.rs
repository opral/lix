pub(crate) use lix_rocksdb_storage::RocksDB;
#[cfg(feature = "slatedb")]
pub(crate) use lix_slatedb_storage::SlateDB;
pub(crate) use lix_sqlite_storage::SQLite;
use tempfile::TempDir;

#[derive(Clone, Copy)]
pub(crate) enum StorageProfile {
    SQLite,
    RocksDB,
    #[cfg(feature = "slatedb")]
    SlateDB,
    #[cfg(feature = "slatedb")]
    SlateDBRemoteObjectStore,
}

pub(crate) const KV_STORAGE_PROFILES: &[StorageProfile] =
    &[StorageProfile::SQLite, StorageProfile::RocksDB];

#[cfg(not(feature = "slatedb"))]
pub(crate) const STORAGE_PROFILES: &[StorageProfile] =
    &[StorageProfile::SQLite, StorageProfile::RocksDB];
#[cfg(feature = "slatedb")]
pub(crate) const STORAGE_PROFILES: &[StorageProfile] = &[
    StorageProfile::SQLite,
    StorageProfile::RocksDB,
    StorageProfile::SlateDB,
];

impl StorageProfile {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::SQLite => "lix_sqlite",
            Self::RocksDB => "lix_rocksdb",
            #[cfg(feature = "slatedb")]
            Self::SlateDB => "lix_slatedb",
            #[cfg(feature = "slatedb")]
            Self::SlateDBRemoteObjectStore => "lix_slatedb_remote_object_store",
        }
    }
}

pub(crate) enum ProfileStorage {
    SQLite {
        storage: SQLite,
        _dir: TempDir,
    },
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

impl StorageProfile {
    pub(crate) fn storage(self) -> ProfileStorage {
        match self {
            Self::SQLite => {
                let dir = TempDir::new().expect("create sqlite bench tempdir");
                let storage = SQLite::open(dir.path().join("bench.sqlite"))
                    .expect("open sqlite bench storage");
                ProfileStorage::SQLite { storage, _dir: dir }
            }
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
                    lix_slatedb_storage::SlateDBObjectStoreOptions::default(),
                )
                .expect("open remote-path SlateDB object store");
                ProfileStorage::SlateDB { storage, _dir: dir }
            }
        }
    }
}
