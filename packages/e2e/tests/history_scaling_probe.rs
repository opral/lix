//! The standard scaling instrument for `lix_file_history()`.
//!
//! **Use this rather than writing a fresh history probe, and keep the two
//! sweeps separate.** History cost has two independent terms - the number of
//! files in the workspace and the number of commits the query traverses - and
//! the obvious way to seed a probe couples them. A probe that inserts files in
//! fixed-size batches makes `commits = files / batch + edits`, so "cost grew
//! 9x when I added 100x the files" silently also means "and 5.7x the commits".
//! That conflation sent one optimization round at the wrong variable; the
//! measurement that corrected it is exactly the pair of tests below.
//!
//! * [`history_files_at_fixed_commits`] varies file count with the commit count
//!   pinned, by seeding every file in a single statement. Growth here is a real
//!   `O(files)` term.
//! * [`history_commits_at_fixed_files`] varies commit count with both the file
//!   count and the answer size pinned, by committing edits to a file the query
//!   never asks about. Growth here is a real `O(commits traversed)` term. Its
//!   `depth0` lane asks for a single row via `lixcol_depth = 0`, so any growth
//!   there is work that a depth bound failed to prune. Its `null_control_kv`
//!   lane reads a *different* history surface over the same commit graph, so it
//!   cannot execute any `lix_file_history` routing change: whatever spread that
//!   lane shows between two arms is the harness's noise floor.
//!
//! Both are `#[ignore]`d; run them by name with `--release`. Every knob is an
//! environment variable so a curve can be widened without editing this file.
//!
//! MEASUREMENT ONLY - this file drives the public SQL surface and
//! changes no engine code.

use lix::storage::Storage;
use lix::{Lix, Row, Value, open_lix};
use lix_storage_rocksdb::RocksDB;
use std::collections::BTreeSet;
use std::path::Path;
use std::time::{Duration, Instant};

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_list(name: &str, default: &str) -> Vec<usize> {
    std::env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .split(',')
        .filter_map(|v| v.trim().parse().ok())
        .collect()
}

fn emit(label: &str, samples: &[Duration]) {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let raw: Vec<String> = samples
        .iter()
        .map(|d| format!("{:.3}", d.as_secs_f64() * 1000.0))
        .collect();
    let idx = (sorted.len() / 2).min(sorted.len().saturating_sub(1));
    eprintln!(
        "history_scale {label} samples={} p50_ms={:.3} raw_ms=[{}]",
        sorted.len(),
        sorted[idx].as_secs_f64() * 1000.0,
        raw.join(","),
    );
}

async fn open_at(dir: &Path) -> Lix<RocksDB> {
    let storage = RocksDB::open(dir.join(".lix")).expect("open RocksDB");
    open_lix()
        .with_storage(storage)
        .await
        .expect("open lix workspace")
}

async fn active_commit<S>(lix: &Lix<S>) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let r = lix
        .execute("SELECT lix_active_branch_commit_id()", &[])
        .await
        .expect("active commit");
    match &r.rows()[0].values()[0] {
        Value::Text(t) => t.clone(),
        other => panic!("expected text, got {other:?}"),
    }
}

fn payload_for(index: usize, bytes: usize) -> Vec<u8> {
    let mut v = vec![0u8; bytes];
    for (j, b) in v.iter_mut().enumerate() {
        *b = ((index * 31 + j * 17) % 251) as u8;
    }
    v
}

/// Seeds `files` files using `rows_per_stmt` rows per statement (one statement
/// is one commit), then applies `edits` further commits to `edit_path`.
async fn seed(
    lix: &Lix<RocksDB>,
    files: usize,
    file_bytes: usize,
    rows_per_stmt: usize,
    edits: usize,
    edit_path: &str,
) {
    let mut i = 0;
    while i < files {
        let n = rows_per_stmt.min(files - i);
        let mut sql = String::from("INSERT INTO lix_file (path, content) VALUES ");
        let mut params: Vec<Value> = Vec::new();
        for k in 0..n {
            if k > 0 {
                sql.push(',');
            }
            sql.push_str(&format!("(${}, ${})", 2 * k + 1, 2 * k + 2));
            params.push(Value::Text(format!("/probe/f{:05}.bin", i + k)));
            params.push(Value::Blob(payload_for(i + k, file_bytes).into()));
        }
        sql.push_str(" ON CONFLICT (path) DO UPDATE SET content = excluded.content");
        lix.execute(&sql, &params).await.expect("probe insert");
        i += n;
    }

    let mut body = payload_for(0, file_bytes);
    for r in 0..edits {
        let slot = r % body.len();
        body[slot] = body[slot].wrapping_add(1);
        lix.execute(
            "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
             ON CONFLICT (path) DO UPDATE SET content = excluded.content",
            &[
                Value::Text(edit_path.to_string()),
                Value::Blob(body.clone().into()),
            ],
        )
        .await
        .expect("probe edit");
    }
}

fn row_path(row: &Row) -> Option<String> {
    match row.value("path").expect("path column") {
        Value::Text(value) => Some(value.clone()),
        _ => None,
    }
}

fn row_key(row: &Row) -> String {
    let id: String = row.get("id").expect("id");
    let depth: i64 = row.get("lixcol_depth").expect("depth");
    let observed: String = row
        .get("lixcol_observed_commit_id")
        .expect("observed commit");
    format!(
        "{id}|{depth}|{observed}|{}",
        row_path(row).unwrap_or_default()
    )
}

async fn timed(
    lix: &Lix<RocksDB>,
    sql: &str,
    params: &[Value],
    samples: usize,
) -> (Vec<Duration>, usize) {
    let mut d = Vec::new();
    let mut rows = 0;
    for _ in 0..samples {
        let t = Instant::now();
        let result = lix.execute(sql, params).await.expect("probe query");
        d.push(t.elapsed());
        rows = result.rows().len();
    }
    (d, rows)
}

/// Vary file count with the commit count held constant.
///
/// All files land in ONE statement, i.e. one commit, so reachable commits stay
/// at `1 + edits` regardless of how many files exist. Growth here is a genuine
/// `O(files)` term.
#[tokio::test]
#[ignore = "manual history-scaling probe"]
async fn history_files_at_fixed_commits() {
    let file_bytes = env_usize("LIX_HISTORY_SCALE_FILE_BYTES", 4 * 1024);
    let edits = env_usize("LIX_HISTORY_SCALE_EDITS", 20);
    let samples = env_usize("LIX_HISTORY_SCALE_SAMPLES", 3);
    let depth = env_usize("LIX_HISTORY_SCALE_DEPTH", 20);

    for files in env_list("LIX_HISTORY_SCALE_FILES", "50,500,5000") {
        let dir = tempfile::tempdir().expect("probe tempdir");
        let lix = open_at(dir.path()).await;
        let probe_path = "/probe/f00000.bin".to_string();
        // rows_per_stmt = files => the whole corpus is a single commit.
        seed(&lix, files, file_bytes, files, edits, &probe_path).await;

        let head = active_commit(&lix).await;
        let file_id: String = lix
            .execute(
                "SELECT id FROM lix_file WHERE path = $1",
                &[Value::Text(probe_path.clone())],
            )
            .await
            .expect("probe file id")
            .rows()[0]
            .get("id")
            .expect("id text");

        let by_path = format!(
            "SELECT lixcol_depth FROM lix_file_history($1) WHERE path = $2 \
             ORDER BY lixcol_depth LIMIT {depth}"
        );
        let by_id = format!(
            "SELECT lixcol_depth FROM lix_file_history($1) WHERE id = $2 \
             ORDER BY lixcol_depth LIMIT {depth}"
        );

        for (label, sql, arg) in [
            ("by_path", &by_path, probe_path.clone()),
            ("by_id", &by_id, file_id.clone()),
        ] {
            let (d, rows) = timed(
                &lix,
                sql,
                &[Value::Text(head.clone()), Value::Text(arg)],
                samples,
            )
            .await;
            emit(
                &format!("fixed_commits {label} files={files} commits={} rows={rows}", edits + 1),
                &d,
            );
        }
        lix.close().await.expect("close probe");
    }
}

/// Vary the number of reachable commits with file count AND answer size held
/// constant.
///
/// The extra commits touch a file the query never asks about, so the answer
/// stays at `edits + 1` rows. Growth here is an `O(commits traversed)` term.
#[tokio::test]
#[ignore = "manual history-scaling probe"]
async fn history_commits_at_fixed_files() {
    let files = env_usize("LIX_HISTORY_SCALE_FIXED_FILES", 200);
    let file_bytes = env_usize("LIX_HISTORY_SCALE_FILE_BYTES", 4 * 1024);
    let edits = env_usize("LIX_HISTORY_SCALE_EDITS", 20);
    let samples = env_usize("LIX_HISTORY_SCALE_SAMPLES", 3);
    let depth = env_usize("LIX_HISTORY_SCALE_DEPTH", 20);

    for noise_commits in env_list("LIX_HISTORY_SCALE_COMMITS", "20,200,2000") {
        let dir = tempfile::tempdir().expect("probe tempdir");
        let lix = open_at(dir.path()).await;
        let probe_path = "/probe/f00000.bin".to_string();
        seed(&lix, files, file_bytes, files, edits, &probe_path).await;

        // Commits that never touch the queried file.
        let noise_path = format!("/probe/f{:05}.bin", files - 1);
        let mut noise = payload_for(files - 1, file_bytes);
        for r in 0..noise_commits {
            let slot = r % noise.len();
            noise[slot] = noise[slot].wrapping_add(1);
            lix.execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
                 ON CONFLICT (path) DO UPDATE SET content = excluded.content",
                &[
                    Value::Text(noise_path.clone()),
                    Value::Blob(noise.clone().into()),
                ],
            )
            .await
            .expect("noise commit");
        }

        let head = active_commit(&lix).await;
        let file_id: String = lix
            .execute(
                "SELECT id FROM lix_file WHERE path = $1",
                &[Value::Text(probe_path.clone())],
            )
            .await
            .expect("probe file id")
            .rows()[0]
            .get("id")
            .expect("id text");

        let by_path = format!(
            "SELECT lixcol_depth FROM lix_file_history($1) WHERE path = $2 \
             ORDER BY lixcol_depth LIMIT {depth}"
        );
        let by_id = format!(
            "SELECT lixcol_depth FROM lix_file_history($1) WHERE id = $2 \
             ORDER BY lixcol_depth LIMIT {depth}"
        );
        // A depth-bounded shape: the bounded-history-traversal work should make
        // this cheap regardless of how many commits exist.
        let by_path_depth0 = "SELECT lixcol_depth FROM lix_file_history($1) \
                              WHERE path = $2 AND lixcol_depth = 0"
            .to_string();
        // Null control. This walks the same commit graph over the same fixture
        // through the same `load_history_entries` traversal and the same
        // touched-scope digest, but it is a different history surface, so it
        // cannot reach `lix_file_history`'s descriptor/blob route at all. Any
        // arm-to-arm movement it shows is this harness's noise floor: a
        // `lix_file_history` delta smaller than it is unresolvable.
        let null_control_kv = "SELECT lixcol_depth FROM lix_key_value_history($1) \
                               ORDER BY lixcol_depth DESC"
            .to_string();

        for (label, sql, params) in [
            (
                "by_path",
                &by_path,
                vec![Value::Text(head.clone()), Value::Text(probe_path.clone())],
            ),
            (
                "by_id",
                &by_id,
                vec![Value::Text(head.clone()), Value::Text(file_id.clone())],
            ),
            (
                "by_path_depth0",
                &by_path_depth0,
                vec![Value::Text(head.clone()), Value::Text(noise_path.clone())],
            ),
            (
                "null_control_kv",
                &null_control_kv,
                vec![Value::Text(head.clone())],
            ),
        ] {
            let (d, rows) = timed(&lix, sql, &params, samples).await;
            emit(
                &format!(
                    "fixed_files {label} files={files} noise_commits={noise_commits} rows={rows}"
                ),
                &d,
            );
        }

        if std::env::var("LIX_HISTORY_SCALE_VERIFY").is_ok() {
            let projection = "SELECT id, path, lixcol_depth, lixcol_observed_commit_id \
                              FROM lix_file_history($1)";
            let all = lix
                .execute(projection, &[Value::Text(head.clone())])
                .await
                .expect("unfiltered history");
            for target in [&probe_path, &noise_path] {
                let expected: BTreeSet<String> = all
                    .rows()
                    .iter()
                    .filter(|row| row_path(row).as_deref() == Some(target.as_str()))
                    .map(row_key)
                    .collect();
                let actual: BTreeSet<String> = lix
                    .execute(
                        &format!("{projection} WHERE path = $2"),
                        &[Value::Text(head.clone()), Value::Text(target.clone())],
                    )
                    .await
                    .expect("filtered history")
                    .rows()
                    .iter()
                    .map(row_key)
                    .collect();
                assert_eq!(expected, actual, "path pushdown changed the answer for {target}");
            }
            eprintln!("history_scale verify noise_commits={noise_commits} ok");
        }

        lix.close().await.expect("close probe");
    }
}
