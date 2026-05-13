use std::sync::Arc;

use lix_engine::Backend;

mod rocksdb;
mod sqlite;
mod unit;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChangelogBenchBackend {
    Unit,
    SqliteTempfile,
    RocksDbTempdir,
}

impl ChangelogBenchBackend {
    pub(crate) const CI: [Self; 3] = [Self::Unit, Self::SqliteTempfile, Self::RocksDbTempdir];

    #[allow(dead_code)]
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Unit => "mem_unit",
            Self::SqliteTempfile => "sqlite_tempfile",
            Self::RocksDbTempdir => "rocksdb_tempdir",
        }
    }

    pub(crate) fn create(self) -> Arc<dyn Backend + Send + Sync> {
        match self {
            Self::Unit => Arc::new(unit::UnitChangelogBenchBackend::new()),
            Self::SqliteTempfile => Arc::new(
                sqlite::SqliteChangelogBenchBackend::tempfile()
                    .expect("create sqlite changelog bench backend"),
            ),
            Self::RocksDbTempdir => Arc::new(
                rocksdb::RocksDbChangelogBenchBackend::new()
                    .expect("create rocksdb changelog bench backend"),
            ),
        }
    }
}
