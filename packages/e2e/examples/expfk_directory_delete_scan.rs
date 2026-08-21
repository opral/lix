#![recursion_limit = "256"]

//! Cost of the foreign-key delete restriction on a directory delete, as a
//! function of how many files the branch holds.
//!
//! `lix_file_descriptor./directory_id -> lix_directory_descriptor./id` is a
//! declared foreign key whose committed delete restriction was unreachable:
//! the deleted directory row sits at file scope `Exact(None)` and every
//! file-descriptor row sits at `Exact(Some(own id))`, so the restriction scan
//! filtered `file_id IS NULL` and saw nothing. Making it reachable means
//! scanning `lix_file_descriptor` across every file scope in the branch.
//!
//! That turns an O(1) check into an O(files-in-branch) scan on the write path,
//! shared across a transaction by `NormalDeleteRestrictionBatchKey`. This
//! example measures the resulting per-transaction cost directly: it seeds
//! `files` file descriptors, then times a `DELETE FROM lix_directory` of an
//! **empty** directory. An empty directory isolates the added scan, because the
//! recursive planner stages exactly one tombstone and no child work.
//!
//! Run the identical binary on both arms and compare. Usage:
//! `expfk_directory_delete_scan [files] [samples] [seed_batch]`
//! (defaults: 10000 files, 25 samples, 500 inserts per seeding transaction).

use std::time::{Duration, Instant};

use lix::Value;
use lix::storage::Storage;
use lix::{Lix, open_lix};
use lix_storage_rocksdb::RocksDB;

const SEED_DIRECTORIES: usize = 100;

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let files = args
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10_000);
    let samples = args
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(25);
    let seed_batch = args
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(500);
    assert!(samples >= 3, "need at least three samples");

    // Seeding is O(files^2 / batch) because every seeding transaction
    // re-validates the filesystem namespace over the whole domain, so at large
    // collection sizes it dwarfs the thing being measured. `LIX_FK_SEED_DIR`
    // seeds one reusable fixture; `LIX_FK_FIXTURE` measures against a copy of
    // it. Both arms then run over byte-identical state, which is also better
    // science than reseeding per arm.
    let seed_dir = std::env::var("LIX_FK_SEED_DIR").ok();
    let fixture_dir = std::env::var("LIX_FK_FIXTURE").ok();

    let scratch = tempfile::tempdir().expect("create RocksDB directory");
    let (path, seeding) = match (&seed_dir, &fixture_dir) {
        (Some(dir), _) => (std::path::PathBuf::from(dir), true),
        (None, Some(dir)) => (std::path::PathBuf::from(dir), false),
        (None, None) => (scratch.path().to_path_buf(), true),
    };

    if seeding {
        std::fs::create_dir_all(&path).expect("create fixture directory");
    }
    let storage = RocksDB::open(&path).expect("open RocksDB");
    if seeding {
        open_lix()
            .with_storage(storage.clone())
            .await
            .expect("initialize repository");
    }
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open lix");
    let session = lix.open_another_session().await.expect("open workspace");

    let seed_started = Instant::now();
    if seeding {
        seed_files(&session, files, seed_batch).await;
        seed_victims(&session, samples, seed_batch).await;
    }
    let seed_elapsed = seed_started.elapsed();

    let observed = count_files(&session).await;
    assert_eq!(
        observed, files as i64,
        "seeded file count must match the requested collection size"
    );

    if seed_dir.is_some() {
        println!(
            "expfk_directory_delete_scan SEEDED files={files} seed_s={:.1} path={}",
            seed_elapsed.as_secs_f64(),
            path.display()
        );
        return;
    }

    // One untimed delete so plan caches and page caches are warm.
    delete_directory(&session, "/empty/warmup").await;

    let mut timings = Vec::with_capacity(samples);
    for index in 0..samples {
        let path = format!("/empty/e{index:06}");
        let started = Instant::now();
        delete_directory(&session, &path).await;
        timings.push(started.elapsed());
    }
    timings.sort_unstable();

    let median = timings[timings.len() / 2];
    let p95 = timings[(timings.len() * 95) / 100];
    println!(
        "expfk_directory_delete_scan files={files} samples={samples} \
         seed_s={:.1} median_us={:.1} p95_us={:.1} min_us={:.1} max_us={:.1}",
        seed_elapsed.as_secs_f64(),
        micros(median),
        micros(p95),
        micros(timings[0]),
        micros(timings[timings.len() - 1]),
    );
    print!("raw_us=");
    for (index, timing) in timings.iter().enumerate() {
        if index > 0 {
            print!(",");
        }
        print!("{:.1}", micros(*timing));
    }
    println!();
}

fn micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}

async fn seed_files<S>(session: &Lix<S>, files: usize, seed_batch: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mut written = 0usize;
    while written < files {
        let batch = seed_batch.min(files - written);
        let mut transaction = session.begin_transaction().await.expect("begin seed");
        for index in 0..batch {
            let ordinal = written + index;
            let bucket = ordinal % SEED_DIRECTORIES;
            transaction
                .execute(
                    "INSERT INTO lix_file (path, content) VALUES ($1, CAST($2 AS BYTEA))",
                    &[
                        Value::Text(format!("/data/d{bucket:04}/f{ordinal:09}.txt")),
                        Value::Text(format!("v{ordinal}")),
                    ],
                )
                .await
                .expect("seed file insert");
        }
        transaction.commit().await.expect("commit seed batch");
        written += batch;
    }
}

/// Empty directories, one per timed sample plus a warmup. These carry no file
/// children, so the timed transaction stages exactly one directory tombstone.
async fn seed_victims<S>(session: &Lix<S>, samples: usize, seed_batch: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let paths = std::iter::once("/empty/warmup".to_string())
        .chain((0..samples).map(|index| format!("/empty/e{index:06}")))
        .collect::<Vec<_>>();
    for chunk in paths.chunks(seed_batch.max(1)) {
        let mut transaction = session.begin_transaction().await.expect("begin victims");
        for path in chunk {
            transaction
                .execute(
                    "INSERT INTO lix_directory (path) VALUES ($1)",
                    &[Value::Text(path.clone())],
                )
                .await
                .expect("seed empty directory");
        }
        transaction.commit().await.expect("commit victim batch");
    }
}

async fn delete_directory<S>(session: &Lix<S>, path: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let deleted = session
        .execute(
            "DELETE FROM lix_directory WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await
        .expect("empty directory delete should succeed");
    assert_eq!(
        deleted.rows_affected(),
        1,
        "each victim directory is deleted exactly once"
    );
}

async fn count_files<S>(session: &Lix<S>) -> i64
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = session
        .execute(
            "SELECT count(*) AS files FROM lix_file WHERE path LIKE '/data/%'",
            &[],
        )
        .await
        .expect("count seeded files");
    result.rows()[0]
        .get::<i64>("files")
        .expect("count column should be an integer")
}
