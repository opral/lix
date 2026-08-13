//! RocksDB storage implementation for the Lix engine storage API.

mod rocksdb;

pub use rocksdb::{RocksDB, RocksDBFactory, RocksDBFixture, RocksDBRead, RocksDBWrite};

/// Counts of the block fetches RocksDB performed on the calling thread.
///
/// `block_cache_hits + block_reads` is the number of times the table reader had
/// to fetch a block: on RocksDB a hit is a cache lookup and a read is a file
/// read, but on a remote object-store LSM every one of these is a range GET.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlockFetchCounters {
    pub block_cache_hits: u64,
    pub block_reads: u64,
    pub block_read_bytes: u64,
    pub user_key_comparisons: u64,
    pub internal_keys_skipped: u64,
}

impl BlockFetchCounters {
    #[must_use]
    pub fn block_fetches(&self) -> u64 {
        self.block_cache_hits + self.block_reads
    }
}

/// A handle on the calling thread's RocksDB perf context.
///
/// RocksDB's perf context is thread-local and cumulative, so the caller must
/// create, reset and read this on one thread. Drive the measurement on a
/// `current_thread` runtime; a multi-thread runtime may resume the future on a
/// different worker and read a context that never saw the work.
pub struct PerfProbe {
    context: ::rocksdb::perf::PerfContext,
}

impl Default for PerfProbe {
    fn default() -> Self {
        Self::new()
    }
}

impl PerfProbe {
    #[must_use]
    pub fn new() -> Self {
        ::rocksdb::perf::set_perf_stats(::rocksdb::perf::PerfStatsLevel::EnableCount);
        Self {
            context: ::rocksdb::perf::PerfContext::default(),
        }
    }

    pub fn reset(&mut self) {
        self.context.reset();
    }

    #[must_use]
    pub fn read(&self) -> BlockFetchCounters {
        use ::rocksdb::perf::PerfMetric;
        BlockFetchCounters {
            block_cache_hits: self.context.metric(PerfMetric::BlockCacheHitCount),
            block_reads: self.context.metric(PerfMetric::BlockReadCount),
            block_read_bytes: self.context.metric(PerfMetric::BlockReadByte),
            user_key_comparisons: self.context.metric(PerfMetric::UserKeyComparisonCount),
            internal_keys_skipped: self.context.metric(PerfMetric::InternalKeySkippedCount),
        }
    }
}
