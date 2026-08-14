//! E24 — binary CAS read-path throughput probe.
//!
//! Reproduces the claim-2 `checkout` shape in isolation so the per-byte cost of
//! the binary CAS read path can be attributed and profiled:
//!
//!   phase `open`   — cold `open_lix` against an already-populated store
//!   phase `select` — `SELECT path, content FROM lix_file`, every payload
//!                    materialized (this is where the CAS read lives)
//!   phase `write`  — `fs::write` of every payload to a fresh directory
//!
//! `checkout` in the claim-2 harness is exactly `open + select + write`.
//!
//! Usage:
//!   e24_cas_read_probe <asset_kib> <asset_count> <reps> <dir> [import_batch]
//!
//! The payload is incompressible pseudo-random data, matching the `bigmedia`
//! corpus (gif/mp4) rather than text.

use std::fs;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use lix::{Lix, Value, open_lix};
use lix_storage_rocksdb::RocksDB;

fn incompressible(len: usize, seed: u64) -> Vec<u8> {
    // xorshift64* — fast, and its output does not compress.
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        let word = state.wrapping_mul(0x2545_F491_4F6C_DD1D);
        out.extend_from_slice(&word.to_le_bytes());
    }
    out.truncate(len);
    out
}

async fn open_at(dir: &Path) -> Lix<RocksDB> {
    let storage = RocksDB::open(dir.join(".lix")).expect("open RocksDB storage");
    open_lix()
        .with_storage(storage)
        .await
        .expect("open lix workspace")
}

fn stats(label: &str, size_bytes: u64, samples: &[Duration]) {
    let mut sorted = samples.to_vec();
    sorted.sort();
    let median = sorted[sorted.len() / 2];
    let p95 = sorted[(sorted.len() * 95 / 100).min(sorted.len() - 1)];
    let raw = samples
        .iter()
        .map(|d| format!("{:.3}", d.as_secs_f64() * 1000.0))
        .collect::<Vec<_>>()
        .join(",");
    let mib = size_bytes as f64 / (1024.0 * 1024.0);
    println!(
        "e24 phase={label} p50_ms={:.3} p95_ms={:.3} mib={mib:.1} \
         p50_mibs={:.1} raw_ms=[{raw}]",
        median.as_secs_f64() * 1000.0,
        p95.as_secs_f64() * 1000.0,
        mib / median.as_secs_f64(),
    );
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let mut args = std::env::args().skip(1);
    let asset_kib: usize = args
        .next()
        .and_then(|v| v.parse().ok())
        .expect("usage: e24_cas_read_probe <asset_kib> <asset_count> <reps> <dir> [batch]");
    let asset_count: usize = args
        .next()
        .and_then(|v| v.parse().ok())
        .expect("asset_count");
    let reps: usize = args.next().and_then(|v| v.parse().ok()).expect("reps");
    let dir = PathBuf::from(args.next().expect("dir"));
    let import_batch: usize = args
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50)
        .max(1);
    let skip_write = std::env::var("E24_SKIP_WRITE").is_ok();

    fs::create_dir_all(&dir).expect("create probe dir");
    let asset_bytes = asset_kib * 1024;
    let total_bytes = (asset_bytes * asset_count) as u64;
    println!(
        "e24 corpus asset_kib={asset_kib} assets={asset_count} total_mib={:.1} reps={reps} \
         batch={import_batch} threads={}",
        total_bytes as f64 / (1024.0 * 1024.0),
        std::thread::available_parallelism().map_or(0, |n| n.get()),
    );

    // ---- seed (untimed) -------------------------------------------------
    // `E24_SKIP_SEED=1` reuses an already-populated directory so a profile can
    // be taken over the read path alone, with no write-path samples in it.
    if !std::env::var("E24_SKIP_SEED").is_ok_and(|v| v == "1") {
        let lix = open_at(&dir).await;
        let seed_started = Instant::now();
        let mut cursor = 0usize;
        while cursor < asset_count {
            let n = import_batch.min(asset_count - cursor);
            let mut sql = String::from("INSERT INTO lix_file (path, content) VALUES ");
            let mut params: Vec<Value> = Vec::with_capacity(n * 2);
            for k in 0..n {
                if k > 0 {
                    sql.push(',');
                }
                sql.push_str(&format!("(${}, ${})", 2 * k + 1, 2 * k + 2));
                params.push(Value::Text(format!("/assets/a{:06}.bin", cursor + k)));
                params.push(Value::Blob(
                    incompressible(asset_bytes, (cursor + k) as u64 + 1).into(),
                ));
            }
            sql.push_str(" ON CONFLICT (path) DO UPDATE SET content = excluded.content");
            lix.execute(&sql, &params).await.expect("seed import");
            cursor += n;
        }
        println!(
            "e24 seed_ms={:.1}",
            seed_started.elapsed().as_secs_f64() * 1000.0
        );
        lix.close().await.expect("close seed lix");
    }

    // ---- measured phases ------------------------------------------------
    let mut opens = Vec::new();
    let mut selects = Vec::new();
    let mut writes = Vec::new();
    let mut checkouts = Vec::new();

    for rep in 0..reps {
        let dest = dir.join(format!("co-{rep}"));
        let checkout_started = Instant::now();

        let t = Instant::now();
        let lix = open_at(&dir).await;
        opens.push(t.elapsed());

        let t = Instant::now();
        let rows = lix
            .execute("SELECT path, content FROM lix_file", &[])
            .await
            .expect("full-tree read");
        let mut payloads: Vec<(String, Vec<u8>)> = Vec::with_capacity(asset_count);
        let mut seen = 0u64;
        for row in rows.rows() {
            let p: String = row.get("path").expect("path text");
            let c: Vec<u8> = row.get("content").expect("content bytes");
            seen += c.len() as u64;
            payloads.push((p, c));
        }
        selects.push(t.elapsed());
        assert_eq!(seen, total_bytes, "read back the whole corpus");
        black_box(&payloads);

        if !skip_write {
            let t = Instant::now();
            for (p, c) in &payloads {
                let out = dest.join(p.trim_start_matches('/'));
                if let Some(parent) = out.parent() {
                    fs::create_dir_all(parent).expect("checkout parent");
                }
                fs::write(&out, c).expect("checkout write");
            }
            writes.push(t.elapsed());
        }

        drop(payloads);
        checkouts.push(checkout_started.elapsed());
        lix.close().await.expect("close probe lix");
        fs::remove_dir_all(&dest).ok();
    }

    stats("open", total_bytes, &opens);
    stats("select", total_bytes, &selects);
    if !skip_write {
        stats("write", total_bytes, &writes);
    }
    stats("checkout", total_bytes, &checkouts);
}
