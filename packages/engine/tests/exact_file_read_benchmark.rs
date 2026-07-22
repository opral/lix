//! Manual benchmark for warmed exact `lix_file` reads through the public session API.
//!
//! Run with:
//! `cargo test -p lix_engine --release --test exact_file_read_benchmark -- --ignored --nocapture`

use std::hint::black_box;
use std::time::{Duration, Instant};

use lix_engine::{Engine, Memory, Storage, Value};
use lix_rocksdb_storage::RocksDB;
use lix_slatedb_storage::SlateDB;
use tempfile::TempDir;

const WARMUPS: usize = 30;
const ROUNDS: usize = 300;

#[tokio::test(flavor = "current_thread")]
#[ignore = "manual performance probe; run with --ignored --nocapture"]
async fn exact_file_read_benchmark_probe() {
    run_backend("memory", Memory::new()).await;

    let rocks_dir = TempDir::new().expect("create RocksDB benchmark directory");
    run_backend(
        "rocksdb",
        RocksDB::open(rocks_dir.path().join("rocksdb")).expect("open RocksDB benchmark storage"),
    )
    .await;

    let slate_dir = TempDir::new().expect("create SlateDB benchmark directory");
    run_backend(
        "slatedb",
        SlateDB::open(slate_dir.path().join("slatedb")).expect("open SlateDB benchmark storage"),
    )
    .await;
}

async fn run_backend<S>(backend: &str, storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    Engine::initialize(storage.clone())
        .await
        .expect("initialize benchmark storage");
    let engine = Engine::new(storage).await.expect("open benchmark engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("open benchmark session");

    for (file_id, path, bytes) in [
        ("exact-read-4k", "/exact-read-4k.bin", vec![0x41; 4 * 1024]),
        (
            "exact-read-1m",
            "/exact-read-1m.bin",
            vec![0x42; 1024 * 1024],
        ),
    ] {
        session
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES ($1, $2, $3)",
                &[
                    Value::Text(file_id.to_string()),
                    Value::Text(path.to_string()),
                    Value::Blob(bytes),
                ],
            )
            .await
            .expect("seed benchmark file");
    }

    // Warm the filesystem path index and backend caches before every timed shape.
    for (shape, sql, parameter) in [
        ("scalar_text", "SELECT $1 AS value", "control"),
        (
            "id_by_id_4k",
            "SELECT id FROM lix_file WHERE id = $1",
            "exact-read-4k",
        ),
        (
            "data_by_id_4k",
            "SELECT data FROM lix_file WHERE id = $1",
            "exact-read-4k",
        ),
        (
            "data_by_path_4k",
            "SELECT data FROM lix_file WHERE path = $1",
            "/exact-read-4k.bin",
        ),
        (
            "change_id_by_id_4k",
            "SELECT lixcol_change_id FROM lix_file WHERE id = $1",
            "exact-read-4k",
        ),
        (
            "data_by_id_1m",
            "SELECT data FROM lix_file WHERE id = $1",
            "exact-read-1m",
        ),
        (
            "data_by_path_1m",
            "SELECT data FROM lix_file WHERE path = $1",
            "/exact-read-1m.bin",
        ),
    ] {
        let params = [Value::Text(parameter.to_string())];
        for _ in 0..WARMUPS {
            black_box(
                session
                    .execute(sql, &params)
                    .await
                    .expect("warm exact read"),
            );
        }

        let mut samples = Vec::with_capacity(ROUNDS);
        for _ in 0..ROUNDS {
            let started = Instant::now();
            let result = session
                .execute(sql, &params)
                .await
                .expect("execute exact read");
            black_box(&result);
            samples.push(started.elapsed());
        }
        samples.sort_unstable();
        let mean_ns = samples.iter().map(Duration::as_nanos).sum::<u128>()
            / u128::try_from(samples.len()).expect("sample count fits u128");
        println!(
            "exact_file_read backend={backend} shape={shape} rounds={ROUNDS} p50_ns={} p95_ns={} mean_ns={mean_ns}",
            percentile(&samples, 50).as_nanos(),
            percentile(&samples, 95).as_nanos(),
        );
    }
}

fn percentile(sorted: &[Duration], percentile: usize) -> Duration {
    let rank = sorted.len().saturating_mul(percentile).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}
