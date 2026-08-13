//! The authoritative lix scan baseline.
//!
//! The campaign's headline scan number has existed in three incompatible
//! forms because nobody stated the axes it was measured under. This states all
//! five -- layer, backend, row count, checkpoint cadence, host class -- and
//! reports ns per SCANNED row and ns per RETURNED row separately. Conflating
//! those two denominators is the direct cause of the 375x-versus-110x
//! confusion.
//!
//! `full_scan` returns every row, so scanned == returned.
//! `filtered_one` returns one row while scanning the collection.
//!
//! The scanned count is MEASURED, not assumed: it comes from the route census
//! in `hot_scan_entries`' per-entry decode loop, which counts each storage key
//! it decodes before `matches_filter` rejects anything. A count taken above
//! that loop is post-filter and reports the surviving rows in both cases.
//!
//! The fixture is seeded in sub-512 chunks on purpose. At >= 512 rows per
//! commit a packed current base is published, the rows leave the HOT row
//! space, and the decode-loop census can no longer see them -- the scanned
//! count would silently become unobservable.
//!
//! Usage: `expb_scan_baseline [rows] [reps]`  (defaults: 10000 rows, 9 reps)

use std::time::Instant;

use lix::Value;
use lix::integration::{Engine, SessionContext};
use lix::storage::Storage;
use lix::storage_bench::{CRUD_PHASE_COUNT, take_hot_scan_route_census};
use lix_storage_rocksdb::RocksDB;

#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

const SEED_CHUNK: usize = 256;

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let rows = args
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10_000);
    let reps = args
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(9);

    let host = std::process::Command::new("hostname")
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|value| value.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    println!(
        "expb_scan_baseline rows={rows} reps={reps} host={host} \
layer=sql_session backend=rocksdb allocator=mimalloc \
cadence=seeded_in_{SEED_CHUNK}_row_commits_no_packed_base"
    );

    let directory = tempfile::tempdir().expect("create RocksDB directory");
    let storage = RocksDB::open(directory.path()).expect("open RocksDB");
    Engine::initialize(storage.clone())
        .await
        .expect("initialize repository");
    let engine = Engine::new(storage.clone()).await.expect("open engine");
    let session = engine.open_session().await.expect("open workspace");

    let mut start = 0;
    while start < rows {
        let end = (start + SEED_CHUNK).min(rows);
        let values = (start..end)
            .map(|index| format!("('seed-{index:08}', '\"v-{index:08}\"')"))
            .collect::<Vec<_>>()
            .join(", ");
        session
            .execute(
                &format!("INSERT INTO lix_key_value (key, value) VALUES {values}"),
                &[],
            )
            .await
            .expect("seed chunk should commit");
        start = end;
    }

    let marker = format!("\"v-{:08}\"", rows / 2);

    measure(
        &session,
        "full_scan",
        "SELECT key, value FROM lix_key_value",
        None,
        reps,
    )
    .await;
    measure(
        &session,
        "filtered_one",
        //  is a JSON column; a bare text comparison is rejected.
        "SELECT key, value FROM lix_key_value WHERE value = lix_json($1)",
        Some(marker),
        reps,
    )
    .await;
}

async fn measure<S>(
    session: &SessionContext<S>,
    label: &str,
    sql: &str,
    param: Option<String>,
    reps: usize,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let params: Vec<Value> = param.map(Value::Text).into_iter().collect();

    // Warm once so the first timed rep is not a cache fill.
    let returned = run(session, sql, &params).await;
    let _ = take_hot_scan_route_census();

    let mut samples = Vec::with_capacity(reps);
    for _ in 0..reps {
        let started = Instant::now();
        let count = run(session, sql, &params).await;
        samples.push(started.elapsed().as_secs_f64() * 1e9);
        assert_eq!(count, returned, "row count must be stable across reps");
    }
    let census = take_hot_scan_route_census();

    // Scanned rows, measured at the decode loop, summed across phases.
    let decoded: u64 = (0..CRUD_PHASE_COUNT)
        .map(|phase| census[phase].fallback_entries_decoded)
        .sum();
    let calls: u64 = (0..CRUD_PHASE_COUNT).map(|phase| census[phase].calls).sum();
    let scanned_per_rep = decoded as f64 / reps as f64;

    // A vacuous query would make every ns/row figure meaningless.
    assert!(returned > 0, "`{label}` returned no rows");

    let mut sorted = samples.clone();
    sorted.sort_by(|left, right| left.partial_cmp(right).expect("finite"));
    let median = sorted[sorted.len() / 2];
    let p95 = sorted[(sorted.len() * 95 / 100).min(sorted.len() - 1)];

    println!();
    println!(
        "LIX {label:<14} returned={returned:<7} scanned_measured_per_rep={scanned_per_rep:<9.0} \
scan_calls_per_rep={:.1}",
        calls as f64 / reps as f64
    );
    println!(
        "  median_total_us={:.1} p95_total_us={:.1} ns_per_returned_row={:.2} \
ns_per_scanned_row={}",
        median / 1e3,
        p95 / 1e3,
        median / returned as f64,
        if scanned_per_rep >= 1.0 {
            format!("{:.2}", median / scanned_per_rep)
        } else {
            "n/a (decode loop never ran; see note)".to_string()
        }
    );
    println!(
        "  raw total_us: {}",
        samples
            .iter()
            .map(|value| format!("{:.1}", value / 1e3))
            .collect::<Vec<_>>()
            .join(" ")
    );
}

async fn run<S>(session: &SessionContext<S>, sql: &str, params: &[Value]) -> usize
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute(sql, params)
        .await
        .expect("query should execute")
        .rows()
        .len()
}
