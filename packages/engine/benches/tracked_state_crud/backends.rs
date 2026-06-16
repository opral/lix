use tempfile::TempDir;

#[expect(dead_code)]
#[path = "../../tests/backend/support/redb_backend.rs"]
mod redb_backend;
#[expect(dead_code)]
#[path = "../../tests/backend/support/rocksdb_backend.rs"]
mod rocksdb_backend;
#[expect(dead_code)]
#[path = "../../tests/backend/support/sqlite_backend.rs"]
mod sqlite_backend;

pub(crate) use redb_backend::RedbBackend;
pub(crate) use rocksdb_backend::RocksDbBackend;
pub(crate) use sqlite_backend::SqliteBackend;

#[derive(Clone, Copy)]
pub(crate) enum BackendProfile {
    Sqlite,
    RocksDb,
    Redb,
}

pub(crate) const BACKEND_PROFILES: [BackendProfile; 3] = [
    BackendProfile::Sqlite,
    BackendProfile::RocksDb,
    BackendProfile::Redb,
];

impl BackendProfile {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Sqlite => "lix_sqlite",
            Self::RocksDb => "lix_rocksdb",
            Self::Redb => "lix_redb",
        }
    }
}

pub(crate) enum ProfileBackend {
    Sqlite(SqliteBackend),
    RocksDb(RocksDbBackend),
    Redb(RedbBackend),
}

impl BackendProfile {
    pub(crate) fn backend(self) -> ProfileBackend {
        match self {
            Self::Sqlite => {
                let dir = TempDir::new().expect("create sqlite bench tempdir");
                ProfileBackend::Sqlite(
                    SqliteBackend::open(dir.keep().join("bench.sqlite"))
                        .expect("open sqlite bench backend"),
                )
            }
            Self::RocksDb => {
                let dir = TempDir::new().expect("create rocksdb bench tempdir");
                ProfileBackend::RocksDb(
                    RocksDbBackend::open(dir.keep().join("bench.rocksdb"))
                        .expect("open rocksdb bench backend"),
                )
            }
            Self::Redb => {
                let dir = TempDir::new().expect("create redb bench tempdir");
                ProfileBackend::Redb(
                    RedbBackend::open(dir.keep().join("bench.redb"))
                        .expect("open redb bench backend"),
                )
            }
        }
    }
}
