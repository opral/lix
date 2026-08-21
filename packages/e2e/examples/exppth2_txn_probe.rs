#![recursion_limit = "256"]

//! EXPPTH2: does batching content updates into ONE explicit transaction
//! collapse the per-statement O(repository size) term?
//!
//! Arms, all doing the SAME N content updates against the SAME fixture:
//!
//! 1. `by_id_per_stmt`  -- `UPDATE ... WHERE id = $2`, one implicit transaction
//!                         per statement. This is the arm that showed a linear
//!                         term (~0.23 us per file in the branch).
//! 2. `by_id_one_txn`   -- the identical statements inside ONE explicit
//!                         transaction, committed once at the end.
//! 3. `by_path_per_stmt`-- `UPDATE ... WHERE path = $2` (DataFusion), the arm
//!                         measured flat in repository size. In-run control.
//!
//! A fourth arm, `native_per_stmt`, drove the native fast-write path
//! (`Lix::upsert_file_content`) as a no-SQL reference. It was removed when the
//! Rust SDK public API hard cut (#1438/#1442) made that method `pub(crate)`:
//! **the native reference is no longer obtainable from an external test crate**
//! such as `lix_e2e`, and re-routing it through SQL would just duplicate
//! `by_path_per_stmt`. Do not go looking for it. The three-path comparison it
//! existed for is already recorded -- native 251 us, sql_by_id 322,
//! sql_by_path 444 -- as is the by-id slope it anchored, which collapsed from
//! 0.2025 to 0.00207 us/file after the fix. The surviving arms carry the
//! residual slope work.
//!
//! Discriminating prediction: if the cause is a revision bump per COMMIT
//! busting a revision-keyed cache, arm 2 is flat in file count while arm 1
//! grows. If arm 2 grows too, the cause is elsewhere.
//!
//! Usage: `exppth2_txn_probe [files] [updates] [reps] [payload_bytes]`

use std::time::Instant;

use lix::{Value, open_lix};
use lix_storage_rocksdb::RocksDB;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let files: usize = args.get(1).and_then(|v| v.parse().ok()).unwrap_or(2000);
    let updates: usize = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(200);
    let reps: usize = args.get(3).and_then(|v| v.parse().ok()).unwrap_or(3);
    let payload: usize = args.get(4).and_then(|v| v.parse().ok()).unwrap_or(4096);

    if let Ok(loops) = std::env::var("EXPPTH2_PROFILE_LOOPS") {
        let loops: usize = loops.parse().unwrap_or(1);
        run_profile(files, updates, payload, loops).await;
        return;
    }

    let all_arms = ["by_id_per_stmt", "by_id_one_txn", "by_path_per_stmt"];
    // Only arms named here run, so a control can be re-measured without paying
    // for every fixture again.
    let selected = std::env::var("EXPPTH2_ARMS").unwrap_or_default();
    let selected = if selected.is_empty() {
        all_arms.to_vec()
    } else {
        selected
            .split(',')
            .map(str::trim)
            .filter(|arm| !arm.is_empty())
            .map(|arm| {
                all_arms
                    .into_iter()
                    .find(|candidate| *candidate == arm)
                    .unwrap_or_else(|| panic!("unknown arm {arm}"))
            })
            .collect::<Vec<_>>()
    };
    let orders = (0..selected.len().max(1))
        .map(|offset| {
            let mut rotated = selected.clone();
            rotated.rotate_left(offset % selected.len().max(1));
            rotated
        })
        .collect::<Vec<_>>();

    let mut samples: Vec<(String, f64)> = Vec::new();
    for rep in 0..reps {
        for arm in orders[rep % orders.len()].iter().copied() {
            let ms = run_arm(arm, files, updates, payload).await;
            println!(
                "EXPPTH2 files={files} rep={rep} arm={arm} total_ms={ms:.3} per_op_us={:.2}",
                ms * 1000.0 / updates as f64
            );
            samples.push((arm.to_string(), ms));
        }
    }

    println!();
    for arm in selected.iter().copied() {
        let mut arm_samples: Vec<f64> = samples
            .iter()
            .filter(|(name, _)| name == arm)
            .map(|(_, ms)| *ms)
            .collect();
        arm_samples.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
        let median = arm_samples[arm_samples.len() / 2];
        println!(
            "EXPPTH2_SUMMARY files={files} arm={arm} median_total_ms={median:.3} median_per_op_us={:.2} raw={:?}",
            median * 1000.0 / updates as f64,
            arm_samples
                .iter()
                .map(|v| format!("{v:.2}"))
                .collect::<Vec<_>>()
        );
    }
}

/// Single-arm `by_id_per_stmt` driver with the fixture build gated OUT of the
/// profiling window, and the update phase repeated so a short measured region
/// still yields enough samples. Per-loop timings are printed so drift caused by
/// the growing commit history can be ruled out rather than assumed away.
async fn run_profile(files: usize, updates: usize, payload: usize, loops: usize) {
    use std::io::Write as _;

    let root = tempfile::Builder::new()
        .prefix("expPTH2p-")
        .tempdir()
        .expect("create dir");
    let storage = RocksDB::open(&root.path().join("db")).expect("open RocksDB");
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open Lix");

    let seed: Vec<u8> = vec![b'a'; payload];
    let mut transaction = lix.begin_transaction().await.expect("begin seed");
    for index in 0..files {
        transaction
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                &[
                    Value::Text(format!("/exppth2-{index:06}.bin")),
                    Value::Blob(seed.clone().into()),
                ],
            )
            .await
            .expect("seed insert");
    }
    transaction.commit().await.expect("commit seed");

    let rows = lix
        .execute("SELECT id FROM lix_file ORDER BY path", &[])
        .await
        .expect("read ids back");
    let ids = rows
        .rows()
        .iter()
        .map(|row| match rows.get(row, "id").expect("file id") {
            Value::Text(text) => text.clone(),
            other => panic!("unexpected id value {other:?}"),
        })
        .collect::<Vec<_>>();
    assert!(ids.len() >= updates, "fixture must cover every update");

    let gate = std::env::var("EXPPTH2_GATE").unwrap_or_default();
    println!(
        "EXPPTH2_READY pid={} files={files} updates={updates} loops={loops}",
        std::process::id()
    );
    std::io::stdout().flush().ok();
    if !gate.is_empty() {
        while !std::path::Path::new(&gate).exists() {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }

    for loop_index in 0..loops {
        let payload_byte = if loop_index % 2 == 0 { b'b' } else { b'c' };
        let updated: Vec<u8> = vec![payload_byte; payload];
        let started = Instant::now();
        for id in ids.iter().take(updates) {
            lix.execute(
                "UPDATE lix_file SET content = $1 WHERE id = $2",
                &[Value::Blob(updated.clone().into()), Value::Text(id.clone())],
            )
            .await
            .expect("update by id");
        }
        let ms = started.elapsed().as_secs_f64() * 1000.0;
        let (calls, point_batch, file_prefix, fallback, decoded, matched) =
            lix::storage_bench::take_hot_blob_ref_scan_accounting();
        println!(
            "EXPPTH2_LOOP files={files} loop={loop_index} total_ms={ms:.3} per_op_us={:.2} \
blob_ref_calls={calls} point_batch={point_batch} file_prefix={file_prefix} fallback={fallback} \
entries_decoded={decoded} entries_matched={matched} decoded_per_update={:.1}",
            ms * 1000.0 / updates as f64,
            decoded as f64 / updates as f64
        );
        std::io::stdout().flush().ok();
    }
    storage.flush().ok();
    println!("EXPPTH2_PROFILE_DONE");
}

async fn run_arm(arm: &str, files: usize, updates: usize, payload: usize) -> f64 {
    let root = tempfile::Builder::new()
        .prefix("expPTH2-")
        .tempdir()
        .expect("create dir");
    let storage = RocksDB::open(&root.path().join("db")).expect("open RocksDB");
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open Lix");

    // Identical fixture for every arm. Seeded in one transaction so fixture
    // build time does not dominate at large file counts.
    let seed: Vec<u8> = vec![b'a'; payload];
    let mut transaction = lix.begin_transaction().await.expect("begin seed");
    for index in 0..files {
        transaction
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                &[
                    Value::Text(format!("/exppth2-{index:06}.bin")),
                    Value::Blob(seed.clone().into()),
                ],
            )
            .await
            .expect("seed insert");
    }
    transaction.commit().await.expect("commit seed");

    let rows = lix
        .execute("SELECT id FROM lix_file ORDER BY path", &[])
        .await
        .expect("read ids back");
    let ids = rows
        .rows()
        .iter()
        .map(|row| match rows.get(row, "id").expect("file id") {
            Value::Text(text) => text.clone(),
            other => panic!("unexpected id value {other:?}"),
        })
        .collect::<Vec<_>>();
    assert!(ids.len() >= updates, "fixture must cover every update");

    let updated: Vec<u8> = vec![b'b'; payload];
    let started = Instant::now();
    match arm {
        "by_id_per_stmt" => {
            for id in ids.iter().take(updates) {
                lix.execute(
                    "UPDATE lix_file SET content = $1 WHERE id = $2",
                    &[Value::Blob(updated.clone().into()), Value::Text(id.clone())],
                )
                .await
                .expect("update by id");
            }
        }
        "by_id_one_txn" => {
            let mut transaction = lix.begin_transaction().await.expect("begin updates");
            for id in ids.iter().take(updates) {
                transaction
                    .execute(
                        "UPDATE lix_file SET content = $1 WHERE id = $2",
                        &[Value::Blob(updated.clone().into()), Value::Text(id.clone())],
                    )
                    .await
                    .expect("update by id in transaction");
            }
            transaction.commit().await.expect("commit updates");
        }
        "by_path_per_stmt" => {
            for index in 0..updates {
                lix.execute(
                    "UPDATE lix_file SET content = $1 WHERE path = $2",
                    &[
                        Value::Blob(updated.clone().into()),
                        Value::Text(format!("/exppth2-{index:06}.bin")),
                    ],
                )
                .await
                .expect("update by path");
            }
        }
        other => panic!("unknown arm {other}"),
    }
    let elapsed = started.elapsed().as_secs_f64() * 1000.0;
    storage.flush().ok();
    elapsed
}
