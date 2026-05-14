#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StorageReadStats {
    pub requested_keys: u64,
    pub unique_backend_keys: u64,
    pub backend_calls: u64,
    pub prefix_lowered: u64,
}

impl StorageReadStats {
    pub fn add(&mut self, other: StorageReadStats) {
        self.requested_keys += other.requested_keys;
        self.unique_backend_keys += other.unique_backend_keys;
        self.backend_calls += other.backend_calls;
        self.prefix_lowered += other.prefix_lowered;
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageReadResult<T> {
    pub value: T,
    pub stats: StorageReadStats,
}

impl<T> StorageReadResult<T> {
    pub fn new(value: T, stats: StorageReadStats) -> Self {
        Self { value, stats }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StorageWriteSetStats {
    pub staged_puts: u64,
    pub staged_deletes: u64,
    pub touched_spaces: u64,
    pub put_batches: u64,
    pub delete_batches: u64,
    pub backend_calls: u64,
    pub written_bytes: u64,
}
