use std::sync::Arc;

use lix_engine::Backend;

#[path = "redb_backend.rs"]
mod redb_backend;
#[path = "rocksdb_backend.rs"]
mod rocksdb_backend;
#[path = "sqlite_backend.rs"]
mod sqlite_backend;

use redb_backend::RedbBenchBackend;
use rocksdb_backend::RocksDbBenchBackend;
use sqlite_backend::SqliteBenchBackend;

#[derive(Clone, Copy)]
pub(crate) enum LixBackendProfile {
    Sqlite,
    RocksDb,
    Redb,
}

pub(crate) const LIX_BACKEND_PROFILES: [LixBackendProfile; 3] = [
    LixBackendProfile::Sqlite,
    LixBackendProfile::RocksDb,
    LixBackendProfile::Redb,
];

impl LixBackendProfile {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Sqlite => "lix_sqlite",
            Self::RocksDb => "lix_rocksdb",
            Self::Redb => "lix_redb",
        }
    }

    pub(crate) fn backend(self) -> Arc<dyn Backend + Send + Sync> {
        match self {
            Self::Sqlite => Arc::new(
                SqliteBenchBackend::tempfile().expect("create sqlite untracked-state backend"),
            ),
            Self::RocksDb => Arc::new(
                RocksDbBenchBackend::new().expect("create rocksdb untracked-state backend"),
            ),
            Self::Redb => {
                Arc::new(RedbBenchBackend::new().expect("create redb untracked-state backend"))
            }
        }
    }
}
