//! SQLite scan baseline, for comparison against lix's scan on the same host.
//!
//! Deliberately mirrors `expb_scan_baseline` in `packages/e2e/examples`:
//! same row count, same two query shapes, same rep count, same allocator.
//! Only the engine differs.
//!
//! `full_scan` returns every row, so scanned == returned.
//! `filtered_one` returns exactly one row while scanning all of them, because
//! `value` carries no index. Those two denominators are reported separately:
//! conflating them is what produced this campaign's 375x-versus-110x
//! confusion.
//!
//! Usage: `cargo run --release -- [rows] [reps]`  (defaults: 10000 rows, 9 reps)

use std::time::Instant;

use rusqlite::Connection;

#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    let mut args = std::env::args().skip(1);
    let rows: usize = args
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10_000);
    let reps: usize = args
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or(9);

    let host = std::process::Command::new("hostname")
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|value| value.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    println!(
        "sqlite_scan_baseline rows={rows} reps={reps} host={host} \
engine=sqlite/{} allocator=mimalloc",
        rusqlite::version()
    );

    let directory = tempfile::tempdir().expect("create sqlite directory");
    let path = directory.path().join("baseline.db");
    let connection = Connection::open(&path).expect("open sqlite");

    // Durability comparable to a committed on-disk store, not an in-memory toy.
    connection
        .execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
        .expect("set pragmas");
    connection
        .execute_batch("CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT NOT NULL);")
        .expect("create table");

    // No index on `value`, so `filtered_one` must scan.
    let transaction = connection.unchecked_transaction().expect("begin seed");
    {
        let mut statement = connection
            .prepare("INSERT INTO kv (key, value) VALUES (?1, ?2)")
            .expect("prepare insert");
        for index in 0..rows {
            statement
                .execute(rusqlite::params![
                    format!("seed-{index:08}"),
                    format!("\"v-{index:08}\"")
                ])
                .expect("seed row");
        }
    }
    transaction.commit().expect("commit seed");

    // Prove the filtered query really scans rather than seeking.
    let plan: String = connection
        .query_row(
            "EXPLAIN QUERY PLAN SELECT key, value FROM kv WHERE value = ?1",
            rusqlite::params![marker(rows)],
            |row| row.get(3),
        )
        .expect("explain filtered query");
    println!("PLAN filtered_one: {plan}");
    assert!(
        plan.contains("SCAN"),
        "filtered_one must scan, not seek; plan was `{plan}`"
    );

    let full = measure(&connection, "SELECT key, value FROM kv", None, reps);
    let filtered = measure(
        &connection,
        "SELECT key, value FROM kv WHERE value = ?1",
        Some(marker(rows)),
        reps,
    );

    report("full_scan", rows, full.0, full.1, rows, &full.2);
    report(
        "filtered_one",
        rows,
        filtered.0,
        filtered.1,
        rows,
        &filtered.2,
    );
}

fn marker(rows: usize) -> String {
    format!("\"v-{:08}\"", rows / 2)
}

/// Returns (returned_rows, scanned_rows_by_construction, per-rep nanos).
fn measure(
    connection: &Connection,
    sql: &str,
    param: Option<String>,
    reps: usize,
) -> (usize, usize, Vec<f64>) {
    let mut statement = connection.prepare(sql).expect("prepare query");
    let mut returned = 0;
    let mut samples = Vec::with_capacity(reps);

    // Warm once so the first timed rep is not a cache fill.
    returned = run(&mut statement, param.as_deref()).max(returned);

    for _ in 0..reps {
        let started = Instant::now();
        let count = run(&mut statement, param.as_deref());
        samples.push(started.elapsed().as_secs_f64() * 1e9);
        returned = count;
    }
    (returned, 0, samples)
}

fn run(statement: &mut rusqlite::Statement<'_>, param: Option<&str>) -> usize {
    let mut count = 0;
    let mut rows = match param {
        Some(value) => statement.query(rusqlite::params![value]).expect("query"),
        None => statement.query([]).expect("query"),
    };
    while let Some(row) = rows.next().expect("row") {
        let _: String = row.get(0).expect("key");
        let _: String = row.get(1).expect("value");
        count += 1;
    }
    count
}

fn report(label: &str, _rows: usize, returned: usize, _unused: usize, scanned: usize, samples: &[f64]) {
    let mut sorted = samples.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).expect("finite"));
    let median = sorted[sorted.len() / 2];
    let p95 = sorted[(sorted.len() * 95 / 100).min(sorted.len() - 1)];
    println!(
        "SQLITE {label:<14} returned={returned:<7} scanned={scanned:<7} \
median_total_us={:.1} ns_per_scanned_row={:.2} ns_per_returned_row={:.2} p95_total_us={:.1}",
        median / 1e3,
        median / scanned.max(1) as f64,
        median / returned.max(1) as f64,
        p95 / 1e3,
    );
    println!(
        "  raw total_us {label}: {}",
        samples
            .iter()
            .map(|value| format!("{:.1}", value / 1e3))
            .collect::<Vec<_>>()
            .join(" ")
    );
}
