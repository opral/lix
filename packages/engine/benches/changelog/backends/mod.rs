use lix_engine::backend::InMemoryBackend;

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

    pub(crate) fn create(self) -> InMemoryBackend {
        let _ = self;
        InMemoryBackend::new()
    }
}
