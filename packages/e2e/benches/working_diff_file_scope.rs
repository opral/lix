#![allow(clippy::large_futures)]

//! Scaling harness for **file-scoped** working-diff reads.
//!
//! `docs/diff-commands.md` documents `SELECT ... FROM lix_working_diff WHERE
//! file_id = $1` as the headline shape, but no benchmark covered it. This
//! harness holds the *returned* row count fixed and grows the *total* dirty
//! set since the checkpoint, so the slope answers one question: does a
//! narrow file-scoped working-diff read cost O(rows returned) or O(all dirty
//! rows in the branch)?
//!
//! Five query shapes are timed against the same fixture:
//!   * `full`        — unfiltered `lix_working_diff` (the existing bench shape)
//!   * `file`        — `WHERE file_id = $1`, the shape `docs/diff-commands.md`
//!                     shows first
//!   * `file_schema` — `WHERE file_id = $1 AND schema_key = $2`, the shape of
//!                     the one file-scoped working-diff test in the tree
//!   * `point`       — `WHERE schema_key = $1 AND entity_pk = lix_json($2) AND
//!                     file_id = $3`, which takes the finite-filter bypass in
//!                     `hot_working_diff_entries` and therefore never reads the
//!                     HOT_DIFF index or verifies its coverage proof. This is
//!                     the control: it isolates the proof as the cost.
//!   * `entity_scan` — the same file read through the ordinary entity surface,
//!                     which does not push `lixcol_file_id` down at all.
//!
//! ```text
//! cargo bench -p lix_benchmarks --features storage-benches,slatedb \
//!   --bench working_diff_file_scope -- rocksdb 8 40 100 9 1000 20000 100000
//! ```
//! Positionals: `<rocksdb|slatedb> <min-files> <rows-per-file> <changes-per-commit> <reps>`
//! then a list of total dirty-row targets, e.g. `1000 10000 100000`. Rows per
//! file is fixed; the file count grows to hold the requested dirty set.

use std::fmt::Write as _;
use std::time::{Duration, Instant};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

use lix::integration::{Engine, SessionContext};
use lix::storage::Storage;
use lix::{ExecuteBatchStatement, Value};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;

const SCHEMA_KEY: &str = "wd_file_row";
const INSERT_BATCH_SIZE: usize = 500;
/// Rows dirtied inside the probe file. Held constant across every dirty-set
/// size so the file-scoped query always returns the same number of rows.
const PROBE_DIRTY_ROWS: usize = 4;

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let backend = args.get(1).map(String::as_str).unwrap_or("rocksdb");
    let files = parse_usize(args.get(2), 64);
    let rows_per_file = parse_usize(args.get(3), 40);
    let changes_per_commit = parse_usize(args.get(4), 100);
    let reps = parse_usize(args.get(5), 9);
    let targets = args
        .get(6..)
        .map(|rest| {
            rest.iter()
                .filter(|value| value.parse::<usize>().is_ok())
                .map(|value| value.parse::<usize>().expect("checked"))
                .collect::<Vec<_>>()
        })
        .filter(|targets: &Vec<usize>| !targets.is_empty())
        .unwrap_or_else(|| vec![1_000, 10_000, 100_000]);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create working-diff file-scope runtime");

    for target in targets {
        let dir = tempdir(backend, target);
        runtime.block_on(async {
            match backend {
                "rocksdb" => {
                    let storage = RocksDB::open(&dir).expect("open working-diff file-scope RocksDB");
                    run(
                        storage.clone(),
                        backend,
                        files,
                        rows_per_file,
                        changes_per_commit,
                        target,
                        reps,
                    )
                    .await;
                    storage.flush().expect("flush RocksDB");
                }
                "slatedb" => {
                    let storage = SlateDB::open(&dir).expect("open working-diff file-scope SlateDB");
                    run(
                        storage.clone(),
                        backend,
                        files,
                        rows_per_file,
                        changes_per_commit,
                        target,
                        reps,
                    )
                    .await;
                    storage.flush().await.expect("flush SlateDB");
                }
                other => panic!("backend must be 'rocksdb' or 'slatedb', got '{other}'"),
            }
        });
        let _ = std::fs::remove_dir_all(&dir);
    }
}

fn tempdir(backend: &str, target: usize) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("/tmp/lix-expS-wdfs-{backend}-{target}-{nanos}")
}

fn parse_usize(value: Option<&String>, default: usize) -> usize {
    value.map_or(default, |value| {
        value.parse::<usize>().unwrap_or_else(|_| default)
    })
}

async fn run<StorageImpl>(
    storage: StorageImpl,
    backend: &str,
    files: usize,
    rows_per_file: usize,
    changes_per_commit: usize,
    dirty_target: usize,
    reps: usize,
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    Engine::initialize(storage.clone())
        .await
        .expect("initialize working-diff file-scope storage");
    let engine = Engine::new(storage.clone())
        .await
        .expect("open working-diff file-scope engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("open working-diff file-scope session");

    register_schema(&session).await;
    // Rows per file stays FIXED and the file count grows with the dirty
    // target. Holding the file width constant is what makes the probe file's
    // cost independent of the dirty-set size, which is the whole question.
    let bulk_rows_needed = dirty_target.saturating_sub(PROBE_DIRTY_ROWS);
    let files = files.max(1 + bulk_rows_needed.div_ceil(rows_per_file));
    let file_ids = (0..files).map(file_id).collect::<Vec<_>>();
    seed_files(&session, &file_ids).await;
    // Every dirty row must exist before the checkpoint so the working diff is
    // a pure "modified" set with a stable returned-row count.
    seed_rows(&session, &file_ids, rows_per_file).await;

    session
        .create_checkpoint()
        .await
        .expect("create working-diff file-scope checkpoint");

    // Dirty exactly PROBE_DIRTY_ROWS rows inside file 0 …
    dirty_rows(
        &session,
        &[(0usize, PROBE_DIRTY_ROWS)],
        changes_per_commit,
        rows_per_file,
    )
    .await;
    // … and spread the remainder over files 1..files, never touching file 0.
    let mut plan = Vec::new();
    let mut remaining = bulk_rows_needed;
    let mut file_index = 1usize;
    while remaining > 0 && file_index < files.max(2) {
        let take = remaining.min(rows_per_file);
        plan.push((file_index, take));
        remaining -= take;
        file_index += 1;
        if file_index >= files {
            break;
        }
    }
    assert_eq!(
        remaining, 0,
        "fixture cannot hold {dirty_target} dirty rows in {files} files \
         at {rows_per_file} rows/file"
    );
    dirty_rows(&session, &plan, changes_per_commit, rows_per_file).await;

    let probe_file = file_ids[0].clone();
    let file_sql = "SELECT diff_id, entity_pk, schema_key, file_id, diff_type \
                    FROM lix_working_diff WHERE file_id = $1";
    let full_sql = "SELECT diff_id, entity_pk, schema_key, file_id, diff_type \
                    FROM lix_working_diff";
    // `schema_key + file_id`, the shape of the only file-scoped working-diff
    // test in the tree. Still a full HOT_DIFF scan today.
    let file_schema_sql = "SELECT diff_id, entity_pk, schema_key, file_id, diff_type \
                           FROM lix_working_diff WHERE file_id = $1 AND schema_key = $2";
    // Finite identity + file id: takes the existing bypass and resolves as a
    // point read. This is the floor a seekable file-scoped read aims at.
    let point_sql = "SELECT diff_id, entity_pk, schema_key, file_id, diff_type \
                     FROM lix_working_diff \
                     WHERE schema_key = $1 AND entity_pk = lix_json($2) AND file_id = $3";
    // Live-state read of the same file. HOT_ROW is keyed
    // `schema_key ++ file_id ++ entity_pk` and `hot_scan_entries` owns a
    // file-first prefix route; the entity surface pushes `lixcol_file_id` into
    // `HotStateFilter::file_ids`, so this reaches that prefix seek and costs
    // O(rows in the file). `entity_scan_rows` is fixed at `rows_per_file`, so
    // a flat latency across dirty-set sizes is the pass condition and a rising
    // one means the pushdown stopped reaching the seek.
    let entity_scan_sql = format!("SELECT id FROM {SCHEMA_KEY} WHERE lixcol_file_id = $1");
    let file_params = vec![Value::Text(probe_file.clone())];
    let file_schema_params = vec![
        Value::Text(probe_file.clone()),
        Value::Text(SCHEMA_KEY.to_string()),
    ];
    let point_params = vec![
        Value::Text(SCHEMA_KEY.to_string()),
        Value::Text(format!("[\"{}\"]", row_id(0, 0))),
        Value::Text(probe_file.clone()),
    ];

    // Warm provider construction and the block cache outside the samples.
    let file_rows = rows(&session, file_sql, &file_params).await;
    let full_rows = rows(&session, full_sql, &[]).await;
    let file_schema_rows = rows(&session, file_schema_sql, &file_schema_params).await;
    let point_rows = rows(&session, point_sql, &point_params).await;
    let entity_scan_rows_count = rows(&session, &entity_scan_sql, &file_params).await;

    let file = sample(&session, file_sql, &file_params, reps).await;
    let full = sample(&session, full_sql, &[], reps).await;
    let file_schema = sample(&session, file_schema_sql, &file_schema_params, reps).await;
    let point = sample(&session, point_sql, &point_params, reps).await;
    let entity_scan = sample(&session, &entity_scan_sql, &file_params, reps).await;

    println!(
        "working_diff_file_scope backend={backend} files={files} total_dirty={full_rows} \
         file_rows={file_rows} full_rows={full_rows} file_schema_rows={file_schema_rows} \
         point_rows={point_rows} entity_scan_rows={entity_scan_rows_count} reps={reps} \
         file_p50_ms={:.3} file_min_ms={:.3} file_max_ms={:.3} \
         full_p50_ms={:.3} full_min_ms={:.3} full_max_ms={:.3} \
         file_schema_p50_ms={:.3} file_schema_min_ms={:.3} file_schema_max_ms={:.3} \
         point_p50_ms={:.3} point_min_ms={:.3} point_max_ms={:.3} \
         entity_scan_p50_ms={:.3} entity_scan_min_ms={:.3} entity_scan_max_ms={:.3}",
        millis(file.0),
        millis(file.1),
        millis(file.2),
        millis(full.0),
        millis(full.1),
        millis(full.2),
        millis(file_schema.0),
        millis(file_schema.1),
        millis(file_schema.2),
        millis(point.0),
        millis(point.1),
        millis(point.2),
        millis(entity_scan.0),
        millis(entity_scan.1),
        millis(entity_scan.2),
    );
    drop(session);
    drop(engine);
}

async fn sample<StorageImpl>(
    session: &SessionContext<StorageImpl>,
    sql: &str,
    params: &[Value],
    reps: usize,
) -> (Duration, Duration, Duration)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let mut latencies = Vec::with_capacity(reps);
    for _ in 0..reps {
        let start = Instant::now();
        let _ = rows(session, sql, params).await;
        latencies.push(start.elapsed());
    }
    latencies.sort_unstable();
    (
        latencies[latencies.len() / 2],
        latencies[0],
        *latencies.last().expect("samples are non-empty"),
    )
}

async fn rows<StorageImpl>(
    session: &SessionContext<StorageImpl>,
    sql: &str,
    params: &[Value],
) -> usize
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute(sql, params)
        .await
        .expect("working-diff query should succeed")
        .len()
}

async fn register_schema<StorageImpl>(session: &SessionContext<StorageImpl>)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let schema = serde_json::json!({
        "x-lix-key": SCHEMA_KEY,
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "required": ["id", "value"],
        "properties": {
            "id": { "type": "string" },
            "value": { "type": "string" }
        },
        "additionalProperties": false
    });
    session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (lix_json($1), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register working-diff file-scope schema");
}

async fn seed_files<StorageImpl>(session: &SessionContext<StorageImpl>, file_ids: &[String])
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let mut transaction = session
        .begin_transaction()
        .await
        .expect("begin file seed transaction");
    for chunk in file_ids.chunks(100) {
        let mut sql = String::from("INSERT INTO lix_file (id, path, content) VALUES ");
        let mut params = Vec::with_capacity(chunk.len() * 3);
        for (offset, id) in chunk.iter().enumerate() {
            if offset > 0 {
                sql.push(',');
            }
            let parameter = offset * 3;
            write!(
                sql,
                "(${}, ${}, CAST(${} AS BYTEA))",
                parameter + 1,
                parameter + 2,
                parameter + 3
            )
            .expect("write file insert parameters");
            params.push(Value::Text(id.clone()));
            params.push(Value::Text(format!("/f-{id}.txt")));
            params.push(Value::Text("seed".to_string()));
        }
        transaction
            .execute(&sql, &params)
            .await
            .expect("seed working-diff file-scope files");
    }
    transaction
        .commit()
        .await
        .expect("commit file seed transaction");
}

async fn seed_rows<StorageImpl>(
    session: &SessionContext<StorageImpl>,
    file_ids: &[String],
    rows_per_file: usize,
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    for (file_index, file) in file_ids.iter().enumerate() {
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("begin row seed transaction");
        for start in (0..rows_per_file).step_by(INSERT_BATCH_SIZE) {
            let end = (start + INSERT_BATCH_SIZE).min(rows_per_file);
            let mut sql =
                format!("INSERT INTO {SCHEMA_KEY} (id, value, lixcol_file_id) VALUES ");
            let mut params = Vec::with_capacity((end - start) * 3);
            for (offset, row_index) in (start..end).enumerate() {
                if offset > 0 {
                    sql.push(',');
                }
                let parameter = offset * 3;
                write!(
                    sql,
                    "(${}, ${}, ${})",
                    parameter + 1,
                    parameter + 2,
                    parameter + 3
                )
                .expect("write row insert parameters");
                params.push(Value::Text(row_id(file_index, row_index)));
                params.push(Value::Text("baseline".to_string()));
                params.push(Value::Text(file.clone()));
            }
            transaction
                .execute(&sql, &params)
                .await
                .expect("seed working-diff file-scope rows");
        }
        transaction
            .commit()
            .await
            .expect("commit row seed transaction");
    }
}

/// Applies `count` updates inside each listed file, batching `changes_per_commit`
/// statements into one commit so the HOT_DIFF packing threshold is exercised
/// the same way an ordinary bulk edit would exercise it.
async fn dirty_rows<StorageImpl>(
    session: &SessionContext<StorageImpl>,
    plan: &[(usize, usize)],
    changes_per_commit: usize,
    rows_per_file: usize,
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let mut pending: Vec<ExecuteBatchStatement> = Vec::with_capacity(changes_per_commit);
    for (file_index, count) in plan {
        assert!(
            *count <= rows_per_file,
            "cannot dirty {count} rows in a file seeded with {rows_per_file}"
        );
        for row_index in 0..*count {
            pending.push(ExecuteBatchStatement {
                label: None,
                sql: format!("UPDATE {SCHEMA_KEY} SET value = $1 WHERE id = $2"),
                params: vec![
                    Value::Text("dirty".to_string()),
                    Value::Text(row_id(*file_index, row_index)),
                ],
            });
            if pending.len() >= changes_per_commit {
                flush(session, &mut pending).await;
            }
        }
    }
    flush(session, &mut pending).await;
}

async fn flush<StorageImpl>(
    session: &SessionContext<StorageImpl>,
    pending: &mut Vec<ExecuteBatchStatement>,
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    if pending.is_empty() {
        return;
    }
    let results = session
        .execute_batch(pending)
        .await
        .expect("dirty working-diff rows");
    assert!(
        results.iter().all(|result| result.rows_affected() == 1),
        "every dirtying update must affect exactly one row"
    );
    pending.clear();
}

fn file_id(index: usize) -> String {
    format!("66696c65-{:04x}-8000-8000-{:012x}", index & 0xffff, index)
}

fn row_id(file_index: usize, row_index: usize) -> String {
    format!("f{file_index:04}-r{row_index:06}")
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}
