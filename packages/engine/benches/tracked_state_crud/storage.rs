pub(crate) use lix_rocksdb_storage::RocksDB;
pub(crate) use lix_sqlite_storage::SQLite;
use tempfile::TempDir;

#[derive(Clone, Copy)]
pub(crate) enum StorageProfile {
    SQLite,
    RocksDB,
}

pub(crate) const STORAGE_PROFILES: [StorageProfile; 2] =
    [StorageProfile::SQLite, StorageProfile::RocksDB];

impl StorageProfile {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::SQLite => "lix_sqlite",
            Self::RocksDB => "lix_rocksdb",
        }
    }
}

pub(crate) enum ProfileStorage {
    SQLite(SQLite),
    RocksDB(RocksDB),
}

impl StorageProfile {
    pub(crate) fn storage(self) -> ProfileStorage {
        match self {
            Self::SQLite => {
                let dir = TempDir::new().expect("create sqlite bench tempdir");
                ProfileStorage::SQLite(
                    SQLite::open(dir.keep().join("bench.sqlite"))
                        .expect("open sqlite bench storage"),
                )
            }
            Self::RocksDB => {
                let dir = TempDir::new().expect("create rocksdb bench tempdir");
                ProfileStorage::RocksDB(
                    RocksDB::open(dir.keep().join("bench.rocksdb"))
                        .expect("open rocksdb bench storage"),
                )
            }
        }
    }
}
