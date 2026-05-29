#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageReadStats {
    pub requested_keys: u64,
    pub unique_backend_keys: u64,
    pub backend_calls: u64,
    pub prefix_lowered: u64,
    pub range_scan_chunks: u64,
    pub prefix_scan_chunks: u64,
    pub scan_key_only_chunks: u64,
    pub scan_full_value_chunks: u64,
    pub scan_rows: u64,
    pub scan_has_more: u64,
    pub scan_resume_after: u64,
    pub scan_limit_rows_total: u64,
    pub scan_limit_rows_max: u64,
}

impl StorageReadStats {
    pub fn add(&mut self, other: Self) {
        self.requested_keys += other.requested_keys;
        self.unique_backend_keys += other.unique_backend_keys;
        self.backend_calls += other.backend_calls;
        self.prefix_lowered += other.prefix_lowered;
        self.range_scan_chunks += other.range_scan_chunks;
        self.prefix_scan_chunks += other.prefix_scan_chunks;
        self.scan_key_only_chunks += other.scan_key_only_chunks;
        self.scan_full_value_chunks += other.scan_full_value_chunks;
        self.scan_rows += other.scan_rows;
        self.scan_has_more += other.scan_has_more;
        self.scan_resume_after += other.scan_resume_after;
        self.scan_limit_rows_total += other.scan_limit_rows_total;
        self.scan_limit_rows_max = self.scan_limit_rows_max.max(other.scan_limit_rows_max);
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageReadStatsCollector {
    stats: StorageReadStats,
}

impl StorageReadStatsCollector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&mut self, stats: StorageReadStats) {
        self.stats.add(stats);
    }

    pub fn snapshot(&self) -> StorageReadStats {
        self.stats
    }

    pub fn reset(&mut self) {
        self.stats = StorageReadStats::default();
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageWriteSetStats {
    pub staged_puts: u64,
    pub staged_deletes: u64,
    pub touched_spaces: u64,
    pub put_batches: u64,
    pub delete_batches: u64,
    pub backend_calls: u64,
    pub written_bytes: u64,
}
