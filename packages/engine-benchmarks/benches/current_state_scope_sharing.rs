use std::hint::black_box;
use std::time::{Duration, Instant};

use lix_engine::storage_adapter::StorageAdapter;
use lix_engine::tracked_state::bench::{
    BenchCurrentStatePointFixture, BenchCurrentStatePointMode, BenchCurrentStatePointTarget,
    BenchCurrentStateSparseShape, seed_current_state_point_fixture,
};
use lix_rocksdb_storage::RocksDB;
use lix_slatedb_storage::SlateDB;

fn main() {
    let rows = env_usize("LIX_CURRENT_STATE_ROWS", 1_000_000);
    let sparse_commits = env_usize("LIX_CURRENT_STATE_SPARSE_COMMITS", 32);
    let scopes = env_usize("LIX_CURRENT_STATE_SCOPES", 256);
    let warmups = env_usize("LIX_CURRENT_STATE_WARMUPS", 10);
    let samples = env_usize("LIX_CURRENT_STATE_SAMPLES", 101).max(1);
    let backend_filter = std::env::var("LIX_CURRENT_STATE_BACKEND").unwrap_or_default();
    let sparse_shape = match std::env::var("LIX_CURRENT_STATE_SPARSE_SHAPE").as_deref() {
        Ok("unrelated") => BenchCurrentStateSparseShape::UnrelatedScopes,
        Ok("touched") | Err(_) => BenchCurrentStateSparseShape::TouchedScope,
        Ok(other) => panic!(
            "unknown LIX_CURRENT_STATE_SPARSE_SHAPE '{other}'; expected touched or unrelated"
        ),
    };
    let point_target = match std::env::var("LIX_CURRENT_STATE_TARGET").as_deref() {
        Ok("hot") => BenchCurrentStatePointTarget::HotMutated,
        Ok("cold") | Err(_) => BenchCurrentStatePointTarget::ColdUntouched,
        Ok(other) => panic!("unknown LIX_CURRENT_STATE_TARGET '{other}'; expected hot or cold"),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create current-state benchmark runtime");
    runtime.block_on(async {
        if backend_filter.is_empty() || backend_filter == "rocksdb" {
            let rocks_dir = tempfile::tempdir().expect("create RocksDB directory");
            let rocks = StorageAdapter::new(
                RocksDB::open(rocks_dir.path()).expect("open current-state RocksDB"),
            );
            run_backend(
                "rocksdb",
                rocks,
                rows,
                sparse_commits,
                scopes,
                warmups,
                samples,
                sparse_shape,
                point_target,
            )
            .await;
        }
        if backend_filter.is_empty() || backend_filter == "slatedb" {
            let slate_dir = tempfile::tempdir().expect("create SlateDB directory");
            let slate = StorageAdapter::new(
                SlateDB::open(slate_dir.path()).expect("open current-state SlateDB"),
            );
            run_backend(
                "slatedb",
                slate,
                rows,
                sparse_commits,
                scopes,
                warmups,
                samples,
                sparse_shape,
                point_target,
            )
            .await;
        }
    });
}

async fn run_backend<S>(
    backend: &str,
    storage: StorageAdapter<S>,
    rows: usize,
    sparse_commits: usize,
    scopes: usize,
    warmups: usize,
    samples: usize,
    sparse_shape: BenchCurrentStateSparseShape,
    point_target: BenchCurrentStatePointTarget,
) where
    S: lix_engine::storage::Storage,
{
    let setup = Instant::now();
    let fixture = seed_current_state_point_fixture(
        storage,
        rows,
        sparse_commits,
        scopes,
        sparse_shape,
        point_target,
    )
    .await;
    let setup = setup.elapsed();
    let measured = measure(
        &fixture,
        BenchCurrentStatePointMode::PersistentCatalog,
        warmups,
        samples,
    )
    .await;
    println!(
        "current_state_scope_sharing,backend={backend},mode=arrow_native,shape={sparse_shape:?},target={point_target:?},rows={rows},scopes={},sparse_commits={sparse_commits},setup_ms={:.3},catalog_staged_encoded_bytes={},directory_staged_encoded_bytes={},sparse_staged_puts={},sparse_written_bytes={},first_sparse_ms={:.3},first_sparse_staged_puts={},first_sparse_written_bytes={},catalog_manifest_bytes={},p50_us={:.3},p95_us={:.3}",
        fixture.catalog_entry_count(),
        millis(setup),
        fixture.catalog_staged_encoded_bytes(),
        fixture.directory_staged_encoded_bytes(),
        fixture.sparse_staged_puts(),
        fixture.sparse_written_bytes(),
        fixture.first_sparse_elapsed_nanos() as f64 / 1_000_000.0,
        fixture.first_sparse_staged_puts(),
        fixture.first_sparse_written_bytes(),
        fixture.catalog_manifest_bytes(),
        micros(measured.0),
        micros(measured.1),
    );
}

async fn measure<S>(
    fixture: &BenchCurrentStatePointFixture<S>,
    mode: BenchCurrentStatePointMode,
    warmups: usize,
    samples: usize,
) -> (Duration, Duration)
where
    S: lix_engine::storage::Storage,
{
    for _ in 0..warmups {
        black_box(fixture.read_point(mode).await);
    }
    let mut timings = Vec::with_capacity(samples);
    for _ in 0..samples {
        let started = Instant::now();
        assert_eq!(black_box(fixture.read_point(mode).await), 1);
        timings.push(started.elapsed());
    }
    timings.sort_unstable();
    let p50 = timings[timings.len() / 2];
    let p95 = timings[(timings.len() - 1) * 95 / 100];
    (p50, p95)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}
