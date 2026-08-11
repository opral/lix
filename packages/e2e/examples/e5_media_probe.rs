//! E5 probe: large-binary read (checkout) throughput and commit-latency tail.
//!
//! Two questions, one binary so a single build serves both:
//!
//! * `read` — how does whole-corpus materialization
//!   (`SELECT path, content FROM lix_file`, the checkout shape) scale with mean
//!   asset size at a fixed total byte count? A flat MB/s curve means the cost is
//!   a per-byte constant; a falling curve means a per-chunk or per-file term.
//! * `commit` — the per-commit latency *distribution* for a 3-file agent edit on
//!   a large-asset corpus. Medians hide the LSM flush/compaction tail, so every
//!   round's wall time and start timestamp are printed for correlation against
//!   the RocksDB `LOG`.
//!
//! Usage:
//!   e5_media_probe read   <asset_kib> <files> <reps> [db_dir]
//!   e5_media_probe commit <asset_kib> <files> <rounds> [db_dir]
//!
//! Payload bytes are deterministic xorshift output, which is incompressible and
//! therefore behaves like the compressed media (gif/mp4/png) the claim-2 corpora
//! are made of.

use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use lix::{Lix, Value, open_lix};
use lix_storage_rocksdb::RocksDB;

fn fill_pseudo_random(buffer: &mut [u8], seed: u64) {
    let mut state = seed | 1;
    for chunk in buffer.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let bytes = state.to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
}

fn asset(index: usize, bytes: usize) -> Vec<u8> {
    let mut buffer = vec![0_u8; bytes];
    fill_pseudo_random(&mut buffer, 0x9E37_79B9_7F4A_7C15 ^ (index as u64 + 1));
    buffer
}

fn path_of(index: usize) -> String {
    format!("/assets/{index:05}.bin")
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis()
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (p / 100.0 * (sorted.len() - 1) as f64).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

async fn open_session(dir: &Path, initialize: bool) -> (RocksDB, Lix<RocksDB>) {
    let storage = RocksDB::open(dir).expect("open RocksDB storage");
    if initialize {
        open_lix()
            .with_storage(storage.clone())
            .await
            .expect("initialize repository");
    }
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open lix");
    let session = lix.open_another_session().await.expect("open session");
    (storage, session)
}

/// Seeds `files` assets of `asset_bytes` each, batching statements so a batch
/// carries at most `MAX_BATCH_BYTES` of payload. One transaction, one commit —
/// the claim-2 harness's corrected import granularity.
const MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;

async fn seed(session: &Lix<RocksDB>, files: usize, asset_bytes: usize) -> f64 {
    let started = Instant::now();
    let mut transaction = session
        .begin_transaction()
        .await
        .expect("begin seed transaction");
    let mut batch_bytes = 0_usize;
    let mut sql = String::new();
    let mut params: Vec<Value> = Vec::new();
    let mut flush_at: Vec<usize> = Vec::new();
    let mut pending = 0_usize;

    for index in 0..files {
        if pending > 0 && batch_bytes + asset_bytes > MAX_BATCH_BYTES {
            flush_at.push(index);
            batch_bytes = 0;
            pending = 0;
        }
        batch_bytes += asset_bytes;
        pending += 1;
    }
    flush_at.push(files);

    let mut start = 0_usize;
    for end in flush_at {
        sql.clear();
        params.clear();
        sql.push_str("INSERT INTO lix_file (path, content) VALUES ");
        for index in start..end {
            if index != start {
                sql.push(',');
            }
            let slot = (index - start) * 2;
            sql.push_str(&format!("(${},${})", slot + 1, slot + 2));
            params.push(Value::Text(path_of(index)));
            params.push(Value::Blob(asset(index, asset_bytes).into()));
        }
        transaction
            .execute(&sql, &params)
            .await
            .expect("seed asset batch");
        start = end;
    }
    transaction.commit().await.expect("commit seed");
    started.elapsed().as_secs_f64() * 1000.0
}

/// Whole-corpus materialization: the checkout shape.
async fn read_all(session: &Lix<RocksDB>) -> (f64, u64, u64) {
    let started = Instant::now();
    let result = session
        .execute("SELECT path, content FROM lix_file", &[])
        .await
        .expect("read every file");
    let mut bytes = 0_u64;
    let mut rows = 0_u64;
    for row in result.rows() {
        rows += 1;
        match &row.values()[1] {
            Value::Blob(blob) => bytes += blob.len() as u64,
            other => panic!("content should be a blob, got {other:?}"),
        }
    }
    (started.elapsed().as_secs_f64() * 1000.0, rows, bytes)
}

async fn mode_read(dir: PathBuf, asset_bytes: usize, files: usize, reps: usize) {
    let total = (asset_bytes * files) as f64;
    let seed_ms = {
        let (_storage, session) = open_session(&dir, true).await;
        let ms = seed(&session, files, asset_bytes).await;
        drop(session);
        ms
    };
    println!(
        "e5_read setup asset_bytes={asset_bytes} files={files} total_bytes={} seed_ms={seed_ms:.2}",
        asset_bytes * files
    );

    // Cold arm: a fresh process-level open of the same store, exactly like a
    // checkout into a new directory.
    let mut samples = Vec::new();
    for rep in 0..reps {
        let (_storage, session) = open_session(&dir, false).await;
        let (ms, rows, bytes) = read_all(&session).await;
        let mb_per_s = (bytes as f64 / (1024.0 * 1024.0)) / (ms / 1000.0);
        println!(
            "e5_read cold rep={rep} asset_bytes={asset_bytes} files={files} \
             ms={ms:.3} rows={rows} bytes={bytes} mib_per_s={mb_per_s:.1}"
        );
        samples.push(ms);
        // Warm arm: same open, second read — separates backend I/O from
        // per-byte lix work.
        let (warm_ms, _, warm_bytes) = read_all(&session).await;
        let warm_mb = (warm_bytes as f64 / (1024.0 * 1024.0)) / (warm_ms / 1000.0);
        println!(
            "e5_read warm rep={rep} asset_bytes={asset_bytes} files={files} \
             ms={warm_ms:.3} mib_per_s={warm_mb:.1}"
        );
        drop(session);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
    let median = percentile(&samples, 50.0);
    println!(
        "e5_read summary asset_bytes={asset_bytes} files={files} total_bytes={} \
         cold_median_ms={median:.3} cold_median_mib_per_s={:.1}",
        asset_bytes * files,
        (total / (1024.0 * 1024.0)) / (median / 1000.0)
    );
}

async fn mode_commit(dir: PathBuf, asset_bytes: usize, files: usize, rounds: usize) {
    let (_storage, session) = open_session(&dir, true).await;
    let seed_ms = seed(&session, files, asset_bytes).await;
    println!(
        "e5_commit setup asset_bytes={asset_bytes} files={files} total_bytes={} \
         seed_ms={seed_ms:.2} rounds={rounds}",
        asset_bytes * files
    );

    let mut samples = Vec::new();
    for round in 0..rounds {
        // The claim-2 agent edit: three files, one byte flipped near the middle
        // of each, published as a single commit.
        let mut payloads = Vec::with_capacity(3);
        for slot in 0..3 {
            let index = (round * 3 + slot) % files;
            let mut bytes = asset(index, asset_bytes);
            let middle = bytes.len() / 2;
            bytes[middle] = bytes[middle].wrapping_add(round as u8 + 1);
            payloads.push((path_of(index), bytes));
        }

        let started_at = now_millis();
        let started = Instant::now();
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("begin agent commit");
        let mut sql = String::from("INSERT INTO lix_file (path, content) VALUES ");
        let mut params: Vec<Value> = Vec::new();
        for (slot, (path, bytes)) in payloads.into_iter().enumerate() {
            if slot != 0 {
                sql.push(',');
            }
            sql.push_str(&format!("(${},${})", slot * 2 + 1, slot * 2 + 2));
            params.push(Value::Text(path));
            params.push(Value::Blob(bytes.into()));
        }
        sql.push_str(" ON CONFLICT (path) DO UPDATE SET content = excluded.content");
        transaction
            .execute(&sql, &params)
            .await
            .expect("agent edit");
        transaction.commit().await.expect("commit agent edit");
        let ms = started.elapsed().as_secs_f64() * 1000.0;
        println!("e5_commit round={round} ms={ms:.3} start_unix_ms={started_at}");
        samples.push(ms);
    }

    let mut sorted = samples.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
    println!(
        "e5_commit summary asset_bytes={asset_bytes} files={files} rounds={rounds} \
         p50={:.3} p90={:.3} p95={:.3} p99={:.3} max={:.3} mean={:.3}",
        percentile(&sorted, 50.0),
        percentile(&sorted, 90.0),
        percentile(&sorted, 95.0),
        percentile(&sorted, 99.0),
        sorted.last().copied().unwrap_or(0.0),
        samples.iter().sum::<f64>() / samples.len() as f64,
    );
    let p50 = percentile(&sorted, 50.0);
    let over = samples
        .iter()
        .enumerate()
        .filter(|(_, ms)| **ms > p50 * 5.0)
        .map(|(round, ms)| format!("{round}:{ms:.1}"))
        .collect::<Vec<_>>();
    println!(
        "e5_commit stalls_over_5x_median count={} {}",
        over.len(),
        over.join(" ")
    );
}

/// Replicates the claim-2 agent harness shape exactly, because the 565.9 ms
/// outlier it reported has to be reproduced under *its* conditions before it
/// can be attributed to anything: the public `Lix` handle rather than a raw
/// `SessionContext`, an import split into `batch`-row commits rather than one,
/// and an `lix_active_branch_commit_id()` read plus a `lix_diff` count between
/// every pair of commits.
async fn mode_agent(dir: PathBuf, asset_bytes: usize, files: usize, rounds: usize, batch: usize) {
    let storage = RocksDB::open(&dir).expect("open RocksDB storage");
    let lix: Lix<RocksDB> = open_lix()
        .with_storage(storage)
        .await
        .expect("open lix workspace");

    let import_started = Instant::now();
    let mut cursor = 0_usize;
    while cursor < files {
        let n = batch.min(files - cursor);
        let mut sql = String::from("INSERT INTO lix_file (path, content) VALUES ");
        let mut params: Vec<Value> = Vec::with_capacity(n * 2);
        for k in 0..n {
            if k > 0 {
                sql.push(',');
            }
            sql.push_str(&format!("(${},${})", 2 * k + 1, 2 * k + 2));
            params.push(Value::Text(path_of(cursor + k)));
            params.push(Value::Blob(asset(cursor + k, asset_bytes).into()));
        }
        sql.push_str(" ON CONFLICT (path) DO UPDATE SET content = excluded.content");
        lix.execute(&sql, &params).await.expect("import batch");
        cursor += n;
    }
    let import_ms = import_started.elapsed().as_secs_f64() * 1000.0;
    println!(
        "e5_agent setup asset_bytes={asset_bytes} files={files} import_batch={batch} \
         import_ms={import_ms:.2} rounds={rounds}"
    );

    let mut samples = Vec::new();
    for round in 0..rounds {
        let before = active_commit(&lix).await;
        let mut sql = String::from("INSERT INTO lix_file (path, content) VALUES ");
        let mut params: Vec<Value> = Vec::new();
        for slot in 0..3 {
            let index = (round * 3 + slot) % files;
            let mut bytes = asset(index, asset_bytes);
            // The claim-2 mutation: one byte near the middle, walked by round.
            let at = (bytes.len() / 2 + round * 7) % bytes.len();
            bytes[at] = bytes[at].wrapping_add(1 + (round as u8 % 7));
            if slot != 0 {
                sql.push(',');
            }
            sql.push_str(&format!("(${},${})", slot * 2 + 1, slot * 2 + 2));
            params.push(Value::Text(path_of(index)));
            params.push(Value::Blob(bytes.into()));
        }
        sql.push_str(" ON CONFLICT (path) DO UPDATE SET content = excluded.content");

        let started_at = now_millis();
        let started = Instant::now();
        lix.execute(&sql, &params).await.expect("agent commit");
        let ms = started.elapsed().as_secs_f64() * 1000.0;
        println!("e5_agent round={round} ms={ms:.3} start_unix_ms={started_at}");
        samples.push(ms);

        let after = active_commit(&lix).await;
        lix.execute(
            &format!("SELECT COUNT(*) FROM lix_diff('{before}', '{after}')"),
            &[],
        )
        .await
        .expect("diff versus parent");
    }

    let mut sorted = samples.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
    let p50 = percentile(&sorted, 50.0);
    println!(
        "e5_agent summary asset_bytes={asset_bytes} files={files} rounds={rounds} \
         p50={p50:.3} p90={:.3} p95={:.3} p99={:.3} max={:.3} mean={:.3}",
        percentile(&sorted, 90.0),
        percentile(&sorted, 95.0),
        percentile(&sorted, 99.0),
        sorted.last().copied().unwrap_or(0.0),
        samples.iter().sum::<f64>() / samples.len() as f64,
    );
    let over = samples
        .iter()
        .enumerate()
        .filter(|(_, ms)| **ms > p50 * 5.0)
        .map(|(round, ms)| format!("{round}:{ms:.1}"))
        .collect::<Vec<_>>();
    println!(
        "e5_agent stalls_over_5x_median count={} {}",
        over.len(),
        over.join(" ")
    );
}

async fn active_commit(lix: &Lix<RocksDB>) -> String {
    let result = lix
        .execute("SELECT lix_active_branch_commit_id()", &[])
        .await
        .expect("read active commit id");
    match &result.rows()[0].values()[0] {
        Value::Text(id) => id.clone(),
        other => panic!("active commit id should be text, got {other:?}"),
    }
}

/// `E5_PERF_SPANS=1` turns on Lix's own `lix_perf` spans and prints one
/// line per span close, so a single outlier commit can be attributed to a
/// materialization stage instead of guessed at.
fn install_perf_spans() {
    if std::env::var("E5_PERF_SPANS").ok().as_deref() != Some("1") {
        return;
    }
    use tracing_subscriber::fmt::format::FmtSpan;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("lix_perf=debug")),
        )
        .with_span_events(FmtSpan::CLOSE)
        .with_target(true)
        .init();
}

#[tokio::main]
async fn main() {
    install_perf_spans();
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "read".to_string());
    let asset_kib = args
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1024);
    let files = args
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(64);
    let count = args
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(3);
    let explicit_dir = args.next().map(PathBuf::from);
    let import_batch = args
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(50)
        .max(1);

    let temp = tempfile::tempdir().expect("create probe directory");
    let dir = explicit_dir.unwrap_or_else(|| temp.path().join("rocksdb"));
    std::fs::create_dir_all(&dir).expect("create store directory");
    println!("e5_probe mode={mode} dir={}", dir.display());

    match mode.as_str() {
        "read" => mode_read(dir, asset_kib * 1024, files, count).await,
        "commit" => mode_commit(dir, asset_kib * 1024, files, count).await,
        "agent" => mode_agent(dir, asset_kib * 1024, files, count, import_batch).await,
        other => panic!("unknown mode {other}; expected read|commit|agent"),
    }
}
