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
}

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
        }
    }
}
