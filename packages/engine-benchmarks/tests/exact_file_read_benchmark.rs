//! Manual benchmark for warmed exact `lix_file` reads through the public session API.
//!
//! Run with:
//! `cargo test -p lix_benchmarks --release --test exact_file_read_benchmark -- --ignored --nocapture`
//!
//! Set `LIX_EXACT_FILE_READ_BENCH_FILE_COUNT=10000` to verify that a
//! `WHERE id = $1` read remains a point lookup as the file corpus grows.

use std::fmt::Write as _;
use std::hint::black_box;
use std::time::{Duration, Instant};

use lix::integration::Engine;
use lix::storage::Storage;
use lix::{Memory, Value};
use lix_storage_rocksdb::RocksDB;
#[cfg(feature = "slatedb")]
use lix_storage_slatedb::SlateDB;
use tempfile::TempDir;

const WARMUPS: usize = 30;
const ROUNDS: usize = 300;
const DEFAULT_FILE_COUNT: usize = 2;
const CORPUS_INSERT_CHUNK_SIZE: usize = 500;
const CORPUS_FILE_DATA: &str = "CCCCCCCCCCCCCCCC";

fn benchmark_file_id(index: usize) -> String {
    format!("01920000-0000-7000-8000-{index:012x}")
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "manual performance probe; run with --ignored --nocapture"]
async fn exact_file_read_benchmark_probe() {
    run_exact_file_read_benchmark_probe().await;
}

/// Matches the multithreaded executor used by the service, which lets the
/// SlateDB adapter keep request reads on the request runtime instead of
/// crossing into its lifecycle manager runtime.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual performance probe; run with --ignored --nocapture"]
async fn exact_file_read_multithread_benchmark_probe() {
    run_exact_file_read_benchmark_probe().await;
}

async fn run_exact_file_read_benchmark_probe() {
    let file_count = file_count_from_env();
    run_backend("memory", Memory::new(), file_count).await;

    let rocks_dir = TempDir::new().expect("create RocksDB benchmark directory");
    run_backend(
        "rocksdb",
        RocksDB::open(rocks_dir.path().join("rocksdb")).expect("open RocksDB benchmark storage"),
        file_count,
    )
    .await;

    #[cfg(feature = "slatedb")]
    {
        let slate_dir = TempDir::new().expect("create SlateDB benchmark directory");
        run_backend(
            "slatedb",
            SlateDB::open(slate_dir.path().join("slatedb"))
                .expect("open SlateDB benchmark storage"),
            file_count,
        )
        .await;
    }
}

async fn run_backend<S>(backend: &str, storage: S, file_count: usize)
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

    let mut transaction = session
        .begin_transaction()
        .await
        .expect("begin exact-file benchmark seed transaction");
    let mut inserted = 0_u64;
    for (file_id, path, bytes) in [
        (
            benchmark_file_id(0),
            "/exact-read-4k.bin",
            vec![0x41; 4 * 1024],
        ),
        (
            benchmark_file_id(1),
            "/exact-read-1m.bin",
            vec![0x42; 1024 * 1024],
        ),
    ] {
        inserted += transaction
            .execute(
                "INSERT INTO lix_file (id, path, content) VALUES ($1, $2, $3)",
                &[
                    Value::Text(file_id),
                    Value::Text(path.to_string()),
                    Value::Blob(bytes.into()),
                ],
            )
            .await
            .expect("seed benchmark special file")
            .rows_affected();
    }
    for first_index in (2..file_count).step_by(CORPUS_INSERT_CHUNK_SIZE) {
        let last_index = (first_index + CORPUS_INSERT_CHUNK_SIZE).min(file_count);
        let mut sql = String::from("INSERT INTO lix_file (id, path, content) VALUES ");
        for index in first_index..last_index {
            if index != first_index {
                sql.push(',');
            }
            write!(
                &mut sql,
                "('{}','/exact-read-corpus/{index:05}.bin',CAST('{CORPUS_FILE_DATA}' AS BYTEA))",
                benchmark_file_id(index)
            )
            .expect("format corpus file insert");
        }
        inserted += transaction
            .execute(&sql, &[])
            .await
            .expect("seed benchmark corpus files")
            .rows_affected();
    }
    transaction
        .commit()
        .await
        .expect("commit exact-file benchmark seed transaction");
    assert_eq!(
        inserted,
        u64::try_from(file_count).expect("file count fits u64"),
        "seeded every benchmark file"
    );

    let mut shapes = vec![
        ("scalar_text", "SELECT $1 AS value", "control".to_string()),
        (
            "id_by_id_4k",
            "SELECT id FROM lix_file WHERE id = $1",
            benchmark_file_id(0),
        ),
        (
            "data_by_id_4k",
            "SELECT content FROM lix_file WHERE id = $1",
            benchmark_file_id(0),
        ),
        (
            "data_by_path_4k",
            "SELECT content FROM lix_file WHERE path = $1",
            "/exact-read-4k.bin".to_string(),
        ),
        (
            "change_id_by_id_4k",
            "SELECT lixcol_change_id FROM lix_file WHERE id = $1",
            benchmark_file_id(0),
        ),
        (
            "data_by_id_1m",
            "SELECT content FROM lix_file WHERE id = $1",
            benchmark_file_id(1),
        ),
        (
            "data_by_path_1m",
            "SELECT content FROM lix_file WHERE path = $1",
            "/exact-read-1m.bin".to_string(),
        ),
    ];
    if file_count > DEFAULT_FILE_COUNT {
        let target = file_count - 1;
        shapes.push((
            "id_by_id_corpus_tail",
            "SELECT id FROM lix_file WHERE id = $1",
            benchmark_file_id(target),
        ));
        shapes.push((
            "data_by_id_corpus_tail",
            "SELECT content FROM lix_file WHERE id = $1",
            benchmark_file_id(target),
        ));
        shapes.push((
            "change_id_by_id_corpus_tail",
            "SELECT lixcol_change_id FROM lix_file WHERE id = $1",
            benchmark_file_id(target),
        ));
    }

    // Warm the filesystem path index and backend caches before every timed shape.
    for (shape, sql, parameter) in shapes {
        let params = [Value::Text(parameter)];
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
            "exact_file_read backend={backend} files={file_count} shape={shape} rounds={ROUNDS} p50_ns={} p95_ns={} mean_ns={mean_ns}",
            percentile(&samples, 50).as_nanos(),
            percentile(&samples, 95).as_nanos(),
        );
    }

    let root_listing_sql = "SELECT id, path, name, lixcol_metadata, lixcol_updated_at \
         FROM lix_file WHERE directory_id IS NULL ORDER BY name";
    for _ in 0..WARMUPS {
        black_box(
            session
                .execute(root_listing_sql, &[])
                .await
                .expect("warm root listing"),
        );
    }
    let mut samples = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let started = Instant::now();
        let result = session
            .execute(root_listing_sql, &[])
            .await
            .expect("execute root listing");
        black_box(&result);
        samples.push(started.elapsed());
    }
    samples.sort_unstable();
    let mean_ns = samples.iter().map(Duration::as_nanos).sum::<u128>()
        / u128::try_from(samples.len()).expect("sample count fits u128");
    println!(
        "exact_file_read backend={backend} files={file_count} shape=root_listing rounds={ROUNDS} p50_ns={} p95_ns={} mean_ns={mean_ns}",
        percentile(&samples, 50).as_nanos(),
        percentile(&samples, 95).as_nanos(),
    );

    let root_directory_sql = "SELECT id, path, name, lixcol_updated_at \
         FROM lix_directory WHERE parent_id IS NULL ORDER BY name";
    for _ in 0..WARMUPS {
        black_box(
            session
                .execute(root_directory_sql, &[])
                .await
                .expect("warm root directories"),
        );
        black_box(
            session
                .execute(root_listing_sql, &[])
                .await
                .expect("warm root files"),
        );
    }
    let mut samples = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let started = Instant::now();
        let directories = session
            .execute(root_directory_sql, &[])
            .await
            .expect("execute root directories");
        let files = session
            .execute(root_listing_sql, &[])
            .await
            .expect("execute root files");
        black_box((&directories, &files));
        samples.push(started.elapsed());
    }
    samples.sort_unstable();
    let mean_ns = samples.iter().map(Duration::as_nanos).sum::<u128>()
        / u128::try_from(samples.len()).expect("sample count fits u128");
    println!(
        "exact_file_read backend={backend} files={file_count} shape=root_directory_listing rounds={ROUNDS} p50_ns={} p95_ns={} mean_ns={mean_ns}",
        percentile(&samples, 50).as_nanos(),
        percentile(&samples, 95).as_nanos(),
    );
}

fn file_count_from_env() -> usize {
    let file_count = std::env::var("LIX_EXACT_FILE_READ_BENCH_FILE_COUNT")
        .ok()
        .map(|value| {
            value.parse::<usize>().unwrap_or_else(|_| {
                panic!(
                    "LIX_EXACT_FILE_READ_BENCH_FILE_COUNT must be an integer at least {DEFAULT_FILE_COUNT}, got '{value}'"
                )
            })
        })
        .unwrap_or(DEFAULT_FILE_COUNT);
    assert!(
        file_count >= DEFAULT_FILE_COUNT,
        "LIX_EXACT_FILE_READ_BENCH_FILE_COUNT must be at least {DEFAULT_FILE_COUNT}"
    );
    file_count
}

fn percentile(sorted: &[Duration], percentile: usize) -> Duration {
    let rank = sorted.len().saturating_mul(percentile).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}
