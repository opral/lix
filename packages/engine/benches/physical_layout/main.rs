use criterion::{criterion_group, criterion_main, Criterion};
use lix_engine::storage_bench::{
    StorageBenchConfig, StorageBenchKeyPattern, StorageBenchSelectivity, StorageBenchUpdateFraction,
};

#[path = "../storage/rocksdb_backend.rs"]
mod rocksdb_backend;
#[path = "../storage/sqlite_backend.rs"]
mod sqlite_backend;

mod backend_kv;
mod changelog;
mod json_store;
mod tracked_state;
mod workflow;

use rocksdb_backend::RocksDbBenchBackend;
use sqlite_backend::SqliteBenchBackend;

const BENCH_ROWS: usize = 10_000;
const BENCH_BLOB_BYTES: usize = 1024;
const BENCH_STATE_PAYLOAD_BYTES: usize = 256;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Args {
    pub(crate) rows: usize,
    pub(crate) blob_bytes: usize,
    pub(crate) state_payload_bytes: usize,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            rows: BENCH_ROWS,
            blob_bytes: BENCH_BLOB_BYTES,
            state_payload_bytes: BENCH_STATE_PAYLOAD_BYTES,
        }
    }
}

impl Args {
    pub(crate) fn config(self) -> StorageBenchConfig {
        StorageBenchConfig {
            rows: self.rows,
            blob_bytes: self.blob_bytes,
            state_payload_bytes: self.state_payload_bytes,
            key_pattern: StorageBenchKeyPattern::Sequential,
            selectivity: StorageBenchSelectivity::Percent100,
            update_fraction: StorageBenchUpdateFraction::Percent100,
        }
    }
}

fn physical_layout_benches(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create tokio runtime for physical layout benchmarks");
    let args = Args::default();

    backend_kv::bench(c, &runtime, args);
    changelog::bench(c, &runtime, args);
    tracked_state::bench(c, &runtime, args);
    json_store::bench(c, &runtime, args);
    workflow::bench(c, &runtime, args);
}

criterion_group!(benches, physical_layout_benches);
criterion_main!(benches);
