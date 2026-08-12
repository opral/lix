//! MEASUREMENT ONLY - NOT FOR MERGE.
//!
//! Supplementary lane for experiment `expfr`. The in-tree
//! `history_scaling_probe` has exactly one depth-bounded lane and it carries an
//! exact `path` predicate, which makes `load_file_history_rows` resolve the
//! path against the *unbounded* anchor route before it loads either entry set.
//! That pre-pass already materializes the unbounded reachable-node set, so the
//! path lane cannot observe a change to the order in which
//! `load_file_history_entry_sets` asks for its two routes.
//!
//! This file adds the depth-bounded shapes that have no such pre-pass:
//! `id = ? AND lixcol_depth = 0` and a bare `lixcol_depth = 0`. Seeding is
//! copied from `history_commits_at_fixed_files` so the two instruments are
//! directly comparable. It is applied IDENTICALLY to both A/B arms.

use lix::storage::Storage;
use lix::{Lix, Value, open_lix};
use lix_storage_rocksdb::RocksDB;
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
        "depth_scale {label} samples={} p50_ms={:.3} raw_ms=[{}]",
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

#[tokio::test]
#[ignore = "manual depth-bounded history probe"]
async fn depth_bounded_shapes_at_fixed_files() {
    let files = env_usize("LIX_HISTORY_SCALE_FIXED_FILES", 200);
    let file_bytes = env_usize("LIX_HISTORY_SCALE_FILE_BYTES", 4 * 1024);
    let edits = env_usize("LIX_HISTORY_SCALE_EDITS", 20);
    let samples = env_usize("LIX_HISTORY_SCALE_SAMPLES", 3);

    for noise_commits in env_list("LIX_HISTORY_SCALE_COMMITS", "20,200,2000") {
        let dir = tempfile::tempdir().expect("probe tempdir");
        let lix = open_at(dir.path()).await;
        let probe_path = "/probe/f00000.bin".to_string();
        seed(&lix, files, file_bytes, files, edits, &probe_path).await;

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
        let noise_id: String = lix
            .execute(
                "SELECT id FROM lix_file WHERE path = $1",
                &[Value::Text(noise_path.clone())],
            )
            .await
            .expect("probe file id")
            .rows()[0]
            .get("id")
            .expect("id text");

        // id + depth: no exact-path predicate, so nothing pre-loads the
        // unbounded anchor route. THIS is the shape the ordering changes.
        let id_depth0 = "SELECT lixcol_depth FROM lix_file_history($1) \
                         WHERE id = $2 AND lixcol_depth = 0";
        // path + depth: the in-tree probe's lane. Control - the path pre-pass
        // already loads the unbounded route on both arms.
        let path_depth0 = "SELECT lixcol_depth FROM lix_file_history($1) \
                           WHERE path = $2 AND lixcol_depth = 0";
        // id, no depth: event route == context route, so the reordered `if`
        // short-circuits identically on both arms. NULL CONTROL.
        let id_nodepth = "SELECT lixcol_depth FROM lix_file_history($1) \
                          WHERE id = $2 ORDER BY lixcol_depth LIMIT 20";

        for (label, sql, arg) in [
            ("id_depth0", id_depth0, noise_id.clone()),
            ("path_depth0", path_depth0, noise_path.clone()),
            ("null_control_id_nodepth", id_nodepth, noise_id.clone()),
        ] {
            let (d, rows) = timed(
                &lix,
                sql,
                &[Value::Text(head.clone()), Value::Text(arg)],
                samples,
            )
            .await;
            emit(
                &format!("{label} files={files} noise_commits={noise_commits} rows={rows}"),
                &d,
            );
        }
        lix.close().await.expect("close probe");
    }

    // A bare depth-bounded query over the whole workspace: also has no
    // exact-path pre-pass. Kept out of the sweep above because its answer size
    // grows with `files`, which is a different term.
    let dir = tempfile::tempdir().expect("probe tempdir");
    let lix = open_at(dir.path()).await;
    seed(&lix, files, file_bytes, files, edits, "/probe/f00000.bin").await;
    let head = active_commit(&lix).await;
    let (d, rows) = timed(
        &lix,
        "SELECT lixcol_depth FROM lix_file_history($1) WHERE lixcol_depth = 0",
        &[Value::Text(head)],
        samples,
    )
    .await;
    emit(&format!("unfiltered_depth0 files={files} rows={rows}"), &d);
    lix.close().await.expect("close probe");
}
