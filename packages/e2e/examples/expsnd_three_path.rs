//! EXPSND: the same file content update, driven three ways.
//!
//! 1. `native`     -- `Lix::upsert_file_content`, the path the file benchmarks measure.
//! 2. `sql_by_id`  -- `UPDATE lix_file SET content = $1 WHERE id = $2`, the fast path.
//! 3. `sql_by_path`-- `UPDATE lix_file SET content = $1 WHERE path = $2`, DataFusion,
//!                    and the form the JS SDK and CLI actually emit.
//!
//! Arms 2 and 3 differ by one column name. Routing is confirmed positively by the
//! `WriteExecutorPath` census (set `EXPSND_CENSUS=1`), not assumed.
//!
//! Usage: `expSND_three_path_file_update [files] [reps] [payload_bytes]`

use std::time::Instant;

use lix::{Value, open_lix};
use lix_storage_rocksdb::RocksDB;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let files: usize = args.get(1).and_then(|v| v.parse().ok()).unwrap_or(200);
    let reps: usize = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(5);
    let payload: usize = args.get(3).and_then(|v| v.parse().ok()).unwrap_or(4096);

    // Rotate arm order across reps so warmup and thermal drift cancel.
    let orders = [
        ["native", "sql_by_id", "sql_by_path"],
        ["sql_by_id", "sql_by_path", "native"],
        ["sql_by_path", "native", "sql_by_id"],
    ];

    let mut samples: Vec<(String, f64)> = Vec::new();
    for rep in 0..reps {
        for arm in orders[rep % orders.len()] {
            let ms = run_arm(arm, files, payload).await;
            println!("EXPSND_3PATH rep={rep} arm={arm} total_ms={ms:.3} per_op_us={:.2}", ms * 1000.0 / files as f64);
            samples.push((arm.to_string(), ms));
        }
    }

    println!();
    for arm in ["native", "sql_by_id", "sql_by_path"] {
        let mut arm_samples: Vec<f64> = samples
            .iter()
            .filter(|(name, _)| name == arm)
            .map(|(_, ms)| *ms)
            .collect();
        arm_samples.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
        let median = arm_samples[arm_samples.len() / 2];
        println!(
            "EXPSND_3PATH_SUMMARY arm={arm} median_total_ms={median:.3} median_per_op_us={:.2} raw={:?}",
            median * 1000.0 / files as f64,
            arm_samples
                .iter()
                .map(|v| format!("{v:.2}"))
                .collect::<Vec<_>>()
        );
    }
}

async fn run_arm(arm: &str, files: usize, payload: usize) -> f64 {
    let root = tempfile::Builder::new()
        .prefix("expSND-3path-")
        .tempdir()
        .expect("create dir");
    let storage = RocksDB::open(&root.path().join("db")).expect("open RocksDB");
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open Lix");

    // Seed identical files for every arm.
    let seed: Vec<u8> = vec![b'a'; payload];
    let mut ids = Vec::with_capacity(files);
    for index in 0..files {
        let path = format!("/expsnd-{index:06}.bin");
        lix.execute(
            "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
            &[
                Value::Text(path.clone()),
                Value::Blob(seed.clone().into()),
            ],
        )
        .await
        .expect("seed insert");
        let rows = lix
            .execute("SELECT id FROM lix_file WHERE path = $1", &[Value::Text(path)])
            .await
            .expect("read id back");
        let row = rows.rows().first().expect("file row");
        let id = match rows.get(row, "id").expect("file id") {
            Value::Text(text) => text.clone(),
            other => panic!("unexpected id value {other:?}"),
        };
        ids.push(id);
    }

    let updated: Vec<u8> = vec![b'b'; payload];
    let started = Instant::now();
    for index in 0..files {
        let path = format!("/expsnd-{index:06}.bin");
        match arm {
            "native" => {
                lix.upsert_file_content(path, lix::Blob::from(updated.clone()))
                    .await
                    .expect("native upsert");
            }
            "sql_by_id" => {
                lix.execute(
                    "UPDATE lix_file SET content = $1 WHERE id = $2",
                    &[
                        Value::Blob(updated.clone().into()),
                        Value::Text(ids[index].clone()),
                    ],
                )
                .await
                .expect("sql update by id");
            }
            "sql_by_path" => {
                lix.execute(
                    "UPDATE lix_file SET content = $1 WHERE path = $2",
                    &[Value::Blob(updated.clone().into()), Value::Text(path)],
                )
                .await
                .expect("sql update by path");
            }
            other => panic!("unknown arm {other}"),
        }
    }
    let elapsed = started.elapsed().as_secs_f64() * 1000.0;
    storage.flush().ok();
    elapsed
}
