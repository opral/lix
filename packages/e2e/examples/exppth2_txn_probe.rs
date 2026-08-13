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

    let orders = [
        ["by_id_per_stmt", "by_id_one_txn", "by_path_per_stmt"],
        ["by_id_one_txn", "by_path_per_stmt", "by_id_per_stmt"],
        ["by_path_per_stmt", "by_id_per_stmt", "by_id_one_txn"],
    ];

    let mut samples: Vec<(String, f64)> = Vec::new();
    for rep in 0..reps {
        for arm in orders[rep % orders.len()] {
            let ms = run_arm(arm, files, updates, payload).await;
            println!(
                "EXPPTH2 files={files} rep={rep} arm={arm} total_ms={ms:.3} per_op_us={:.2}",
                ms * 1000.0 / updates as f64
            );
            samples.push((arm.to_string(), ms));
        }
    }

    println!();
    for arm in ["by_id_per_stmt", "by_id_one_txn", "by_path_per_stmt"] {
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
                    &[
                        Value::Blob(updated.clone().into()),
                        Value::Text(id.clone()),
                    ],
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
                        &[
                            Value::Blob(updated.clone().into()),
                            Value::Text(id.clone()),
                        ],
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
