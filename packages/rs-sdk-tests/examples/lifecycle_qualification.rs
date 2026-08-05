//! One public-Lix lifecycle qualification harness for RocksDB and SlateDB.
//!
//! The same worker exercises full and incremental checkpoints, startup
//! restore, undo/redo, branch advance/switch/delete, retention, checkpoint GC,
//! sparse or dense merge, corruption fail-closed behavior, and storage growth.
//! It emits one JSON object so a worker built from `main` can be compared with
//! a worker built from `one-layout` without changing the workload.
//!
//! Examples:
//!
//! ```text
//! cargo run --release -p lix_sdk_tests --example lifecycle_qualification -- \
//!   worker rocksdb 10000 sparse 70
//! cargo run --release -p lix_sdk_tests --example lifecycle_qualification -- \
//!   worker slatedb 1000000 dense 70
//! ```

#![recursion_limit = "512"]

use async_trait::async_trait;
use lix_rocksdb_storage::RocksDB;
use lix_sdk::{
    CreateBranchOptions, ExecuteBatchStatement, Lix, MergeBranchOptions, MergeBranchPreviewOptions,
    Storage, SwitchBranchOptions, Value, open_lix_with_storage,
};
use lix_slatedb_storage::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};
use serde_json::{Value as JsonValue, json};
use sha2::{Digest as _, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const FILE_BATCH: usize = 5_000;
const FILE_BYTES: usize = 96;
const CORRUPTION_SENTINEL_BYTES: usize = 2 * 1024 * 1024;
const SOURCE_BRANCH_ID: &str = "01920000-0000-7000-8000-00000000b101";
const CHECKPOINT_KEY: &str = "lifecycle-checkpoint-key";
const UNTRACKED_KEY: &str = "lifecycle-untracked-key";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MergeShape {
    Sparse,
    Dense,
}

impl MergeShape {
    fn parse(value: &str) -> Self {
        match value {
            "sparse" => Self::Sparse,
            "dense" => Self::Dense,
            other => panic!("merge shape must be sparse or dense, got {other:?}"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Sparse => "sparse",
            Self::Dense => "dense",
        }
    }
}

#[async_trait]
trait LifecycleStorage: Storage + Clone + Send + Sync + 'static {
    const NAME: &'static str;

    fn open_for_lifecycle(
        path: &Path,
        counters: Option<&SlateDBIoCounters>,
    ) -> Result<Self, String>
    where
        Self: Sized;

    async fn flush_for_lifecycle(&self);
}

#[async_trait]
impl LifecycleStorage for RocksDB {
    const NAME: &'static str = "rocksdb";

    fn open_for_lifecycle(
        path: &Path,
        _counters: Option<&SlateDBIoCounters>,
    ) -> Result<Self, String> {
        Self::open(path).map_err(|error| error.to_string())
    }

    async fn flush_for_lifecycle(&self) {
        self.flush().expect("flush RocksDB lifecycle fixture");
    }
}

#[async_trait]
impl LifecycleStorage for SlateDB {
    const NAME: &'static str = "slatedb";

    fn open_for_lifecycle(
        path: &Path,
        counters: Option<&SlateDBIoCounters>,
    ) -> Result<Self, String> {
        let counters = counters.cloned().unwrap_or_default();
        Self::open_with_io_counters(path, counters).map_err(|error| error.to_string())
    }

    async fn flush_for_lifecycle(&self) {
        self.flush().await.expect("flush SlateDB lifecycle fixture");
    }
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let Some(mode) = args.get(1).map(String::as_str) else {
        usage();
        return;
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("lifecycle qualification runtime should build");
    match mode {
        "worker" => {
            let backend = args.get(2).map(String::as_str).unwrap_or("rocksdb");
            let rows = parse_usize(args.get(3), 10_000, "row count");
            let shape = MergeShape::parse(args.get(4).map(String::as_str).unwrap_or("sparse"));
            let checkpoints = parse_usize(args.get(5), 70, "checkpoint count");
            let output = match backend {
                "rocksdb" => runtime.block_on(run::<RocksDB>(rows, shape, checkpoints)),
                "slatedb" => runtime.block_on(run::<SlateDB>(rows, shape, checkpoints)),
                other => panic!("backend must be rocksdb or slatedb, got {other:?}"),
            };
            println!(
                "{}",
                serde_json::to_string(&output).expect("serialize lifecycle result")
            );
        }
        _ => usage(),
    }
}

fn usage() {
    eprintln!(
        "usage: lifecycle_qualification worker <rocksdb|slatedb> [rows] [sparse|dense] [checkpoints]"
    );
}

fn parse_usize(value: Option<&String>, default: usize, label: &str) -> usize {
    value.map_or(default, |value| {
        value
            .parse::<usize>()
            .unwrap_or_else(|_| panic!("{label} must be a positive integer"))
    })
}

struct WorkloadPaths {
    root: tempfile::TempDir,
    database: PathBuf,
}

impl WorkloadPaths {
    fn new() -> Self {
        let root = tempfile::Builder::new()
            .prefix("lix-lifecycle-")
            .tempdir()
            .expect("create lifecycle temporary directory");
        let database = root.path().join("database");
        Self { root, database }
    }

    fn corruption_copy(&self) -> PathBuf {
        self.root.path().join("corruption-copy")
    }
}

async fn run<S>(rows: usize, shape: MergeShape, checkpoint_count: usize) -> JsonValue
where
    S: LifecycleStorage,
{
    assert!(rows > 3, "row count must be greater than three");
    assert!(checkpoint_count > 0, "checkpoint count must be positive");
    let paths = WorkloadPaths::new();
    let counters = (S::NAME == "slatedb").then(SlateDBIoCounters::default);
    let storage = S::open_for_lifecycle(&paths.database, counters.as_ref())
        .unwrap_or_else(|error| panic!("open {} lifecycle storage: {error}", S::NAME));
    let lix = open_lix_with_storage(storage.clone())
        .await
        .expect("open lifecycle Lix");
    seed_files(&lix, rows).await;

    let storage_before_checkpoint = directory_bytes(&paths.database);
    let full_checkpoint_start = Instant::now();
    lix.create_checkpoint()
        .await
        .expect("create full lifecycle checkpoint");
    let full_checkpoint_ms = millis(full_checkpoint_start.elapsed());
    let storage_after_full_checkpoint = directory_bytes(&paths.database);

    let incremental_changes = rows.saturating_sub(1).min(16);
    update_files(&lix, 1, incremental_changes, "incremental").await;
    let incremental_checkpoint_start = Instant::now();
    lix.create_checkpoint()
        .await
        .expect("create incremental lifecycle checkpoint");
    let incremental_checkpoint_ms = millis(incremental_checkpoint_start.elapsed());

    let retention_ok = exercise_retention(&lix).await;
    let (undo_ms, redo_ms, undo_redo_ok) = exercise_undo_redo(&lix).await;
    let (branch_ms, merge_preview_ms, merge_ms, branch_delete_ms, merge_ok, branch_details) =
        exercise_branches_and_merge(&lix, rows, shape, counters.as_ref()).await;

    let gc_start = Instant::now();
    let gc_checkpoint_start = lix
        .execute("SELECT count(*) AS count FROM lix_checkpoint", &[])
        .await
        .expect("read checkpoint count before GC workload")
        .rows()
        .first()
        .and_then(|row| row.get::<i64>("count").ok())
        .unwrap_or_default();
    for index in 0..checkpoint_count {
        let file_index = rows - 1 - (index % (rows - 1));
        update_files(&lix, file_index, 1, "retention-gc").await;
        lix.create_checkpoint()
            .await
            .unwrap_or_else(|error| panic!("create GC checkpoint {index}: {error}"));
    }
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    let gc_workload_ms = millis(gc_start.elapsed());
    let gc_checkpoint_end = lix
        .execute("SELECT count(*) AS count FROM lix_checkpoint", &[])
        .await
        .expect("read checkpoint count after GC workload")
        .rows()
        .first()
        .and_then(|row| row.get::<i64>("count").ok())
        .unwrap_or_default();

    let final_count = lix
        .execute("SELECT count(*) AS count FROM lix_file", &[])
        .await
        .expect("read final file count")
        .rows()
        .first()
        .and_then(|row| row.get::<i64>("count").ok())
        .unwrap_or_default();
    assert_eq!(final_count, rows as i64, "lifecycle file count changed");

    lix.close().await.expect("close lifecycle Lix");
    drop(lix);
    S::flush_for_lifecycle(&storage).await;
    drop(storage);

    let reopen_start = Instant::now();
    let reopened_storage = S::open_for_lifecycle(&paths.database, counters.as_ref())
        .unwrap_or_else(|error| panic!("reopen {} lifecycle storage: {error}", S::NAME));
    let reopened = open_lix_with_storage(reopened_storage.clone())
        .await
        .expect("reopen lifecycle Lix");
    let reopen_ms = millis(reopen_start.elapsed());
    let restore_query_start = Instant::now();
    let restored_count = reopened
        .execute("SELECT count(*) AS count FROM lix_file", &[])
        .await
        .expect("query restored lifecycle state")
        .rows()
        .first()
        .and_then(|row| row.get::<i64>("count").ok())
        .unwrap_or_default();
    let restore_query_ms = millis(restore_query_start.elapsed());
    assert_eq!(restored_count, rows as i64, "reopen did not restore files");
    reopened
        .close()
        .await
        .expect("close reopened lifecycle Lix");
    drop(reopened);
    S::flush_for_lifecycle(&reopened_storage).await;
    drop(reopened_storage);

    let corruption_start = Instant::now();
    let corruption_copy = paths.corruption_copy();
    copy_tree(&paths.database, &corruption_copy);
    let corruption_target = select_corruption_target(&corruption_copy)
        .unwrap_or_else(|| panic!("{} fixture has no mutable corruption target", S::NAME));
    corrupt_file(&corruption_target);
    let corruption_error = match S::open_for_lifecycle(&corruption_copy, counters.as_ref()) {
        Err(error) => Some(error),
        Ok(corrupt_storage) => match open_lix_with_storage(corrupt_storage).await {
            Err(error) => Some(error.to_string()),
            Ok(corrupt_lix) => match corrupt_lix
                .execute("SELECT id, content FROM lix_file ORDER BY id", &[])
                .await
            {
                Err(error) => Some(error.to_string()),
                Ok(_) => None,
            },
        },
    };
    let corruption_ms = millis(corruption_start.elapsed());
    assert!(
        corruption_error.is_some(),
        "{} accepted a corrupted physical fixture at {}",
        S::NAME,
        corruption_target.display()
    );

    let storage_after_reopen = directory_bytes(&paths.database);
    let io_delta = counters
        .as_ref()
        .map(SlateDBIoCounters::snapshot)
        .map(snapshot_json)
        .unwrap_or(JsonValue::Null);
    json!({
        "schema_version": 1,
        "provenance": provenance(),
        "backend": S::NAME,
        "rows": rows,
        "merge_shape": shape.as_str(),
        "checkpoint_count_requested": checkpoint_count,
        "checkpoint_count_before_gc": gc_checkpoint_start,
        "checkpoint_count_after_gc": gc_checkpoint_end,
        "phases_ms": {
            "checkpoint_full": full_checkpoint_ms,
            "checkpoint_incremental": incremental_checkpoint_ms,
            "undo": undo_ms,
            "redo": redo_ms,
            "branch_create_advance_switch": branch_ms,
            "branch_detail": branch_details,
            "merge_preview": merge_preview_ms,
            "merge_commit": merge_ms,
            "branch_delete": branch_delete_ms,
            "retention_gc_workload": gc_workload_ms,
            "reopen_restore": reopen_ms,
            "restore_query": restore_query_ms,
            "corruption_probe": corruption_ms,
        },
        "correctness": {
            "retention": retention_ok,
            "undo_redo": undo_redo_ok,
            "merge": merge_ok,
            "restore": restored_count == rows as i64,
            "corruption_fail_closed": corruption_error.is_some(),
            "rebuild_boundary": "startup_restore_via_public_open",
        },
        "storage": {
            "before_checkpoint": storage_before_checkpoint,
            "after_full_checkpoint": storage_after_full_checkpoint,
            "after_reopen": storage_after_reopen,
            "growth_through_full_checkpoint": signed_delta(storage_after_full_checkpoint, storage_before_checkpoint),
            "growth_through_reopen": signed_delta(storage_after_reopen, storage_before_checkpoint),
        },
        "slatedb_io_delta": io_delta,
        "corruption_target": corruption_target.strip_prefix(&corruption_copy).unwrap_or(&corruption_target),
    })
}

async fn seed_files<S>(lix: &Lix<S>, rows: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    for start in (0..rows).step_by(FILE_BATCH) {
        let end = (start + FILE_BATCH).min(rows);
        let mut sql = String::from("INSERT INTO lix_file (id, path, content) VALUES ");
        let mut params = Vec::with_capacity((end - start) * 3);
        for (offset, index) in (start..end).enumerate() {
            if offset > 0 {
                sql.push(',');
            }
            let parameter = offset * 3;
            sql.push_str(&format!(
                "(${}, ${}, ${})",
                parameter + 1,
                parameter + 2,
                parameter + 3
            ));
            params.push(Value::Text(file_id(index)));
            params.push(Value::Text(format!("/lifecycle/{index:012}.bin")));
            params.push(Value::Blob(file_payload("seed", index).into()));
        }
        lix.execute(&sql, &params)
            .await
            .unwrap_or_else(|error| panic!("seed files {start}..{end}: {error}"));
    }
}

async fn update_files<S>(lix: &Lix<S>, start: usize, count: usize, tag: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let statements = (start..start + count)
        .map(|index| ExecuteBatchStatement {
            sql: "UPDATE lix_file SET content = $1 WHERE id = $2".to_owned(),
            params: vec![
                Value::Blob(file_payload(tag, index).into()),
                Value::Text(file_id(index)),
            ],
        })
        .collect::<Vec<_>>();
    lix.execute_batch(&statements)
        .await
        .unwrap_or_else(|error| panic!("update files {start}..{}: {error}", start + count));
}

async fn exercise_retention<S>(lix: &Lix<S>) -> bool
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
        &[
            Value::Text(CHECKPOINT_KEY.to_owned()),
            Value::Text("tracked-value".to_owned()),
        ],
    )
    .await
    .expect("insert tracked retention row");
    lix.execute(
        "INSERT INTO lix_key_value (key, value, lixcol_untracked) VALUES ($1, $2, true)",
        &[
            Value::Text(UNTRACKED_KEY.to_owned()),
            Value::Text("untracked-value".to_owned()),
        ],
    )
    .await
    .expect("insert untracked retention row");
    let result = lix
        .execute(
            "SELECT key, value, lixcol_untracked FROM lix_key_value \
             WHERE key IN ($1, $2) ORDER BY key",
            &[
                Value::Text(CHECKPOINT_KEY.to_owned()),
                Value::Text(UNTRACKED_KEY.to_owned()),
            ],
        )
        .await
        .expect("read retention rows");
    let rows = result.rows();
    rows.len() == 2
        && rows[0].get::<String>("key").ok().as_deref() == Some(CHECKPOINT_KEY)
        && rows[1].get::<String>("key").ok().as_deref() == Some(UNTRACKED_KEY)
        && rows[0].get::<bool>("lixcol_untracked").ok() == Some(false)
        && rows[1].get::<bool>("lixcol_untracked").ok() == Some(true)
}

async fn exercise_undo_redo<S>(lix: &Lix<S>) -> (f64, f64, bool)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
        &[
            Value::Text("lifecycle-undo-key".to_owned()),
            Value::Text("undo-value".to_owned()),
        ],
    )
    .await
    .expect("create undoable commit");
    let undo_start = Instant::now();
    lix.undo().await.expect("undo lifecycle commit");
    let undo_ms = millis(undo_start.elapsed());
    let after_undo = scalar_count(
        lix,
        "SELECT count(*) AS count FROM lix_key_value WHERE key = 'lifecycle-undo-key'",
    )
    .await;
    let redo_start = Instant::now();
    lix.redo().await.expect("redo lifecycle commit");
    let redo_ms = millis(redo_start.elapsed());
    let after_redo = scalar_count(
        lix,
        "SELECT count(*) AS count FROM lix_key_value WHERE key = 'lifecycle-undo-key'",
    )
    .await;
    (undo_ms, redo_ms, after_undo == 0 && after_redo == 1)
}

async fn exercise_branches_and_merge<S>(
    lix: &Lix<S>,
    rows: usize,
    shape: MergeShape,
    counters: Option<&SlateDBIoCounters>,
) -> (f64, f64, f64, f64, bool, JsonValue)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let active_branch_start = Instant::now();
    let main_branch_id = lix
        .active_branch_id()
        .await
        .expect("read lifecycle main branch");
    let active_branch_ms = millis(active_branch_start.elapsed());
    let branch_start = Instant::now();
    let create_io_before = counters.map(SlateDBIoCounters::snapshot);
    let branch_create_start = Instant::now();
    let branch = lix
        .create_branch(CreateBranchOptions {
            id: Some(SOURCE_BRANCH_ID.to_owned()),
            name: "lifecycle-source".to_owned(),
            from_commit_id: None,
        })
        .await
        .expect("create lifecycle source branch");
    let branch_create_ms = millis(branch_create_start.elapsed());
    let create_io = io_delta_json(counters, create_io_before);
    let switch_to_source_io_before = counters.map(SlateDBIoCounters::snapshot);
    let switch_to_source_start = Instant::now();
    lix.switch_branch(SwitchBranchOptions {
        branch_id: branch.id.clone(),
    })
    .await
    .expect("switch to lifecycle source branch");
    let switch_to_source_ms = millis(switch_to_source_start.elapsed());
    let switch_to_source_io = io_delta_json(counters, switch_to_source_io_before);
    let changes = merge_change_count(rows, shape);
    let source_update_io_before = counters.map(SlateDBIoCounters::snapshot);
    let source_update_start = Instant::now();
    update_files(lix, rows / 2, changes, "source-merge").await;
    let source_update_ms = millis(source_update_start.elapsed());
    let source_update_io = io_delta_json(counters, source_update_io_before);
    let switch_to_target_io_before = counters.map(SlateDBIoCounters::snapshot);
    let switch_to_target_start = Instant::now();
    lix.switch_branch(SwitchBranchOptions {
        branch_id: main_branch_id,
    })
    .await
    .expect("switch to lifecycle target branch");
    let switch_to_target_ms = millis(switch_to_target_start.elapsed());
    let switch_to_target_io = io_delta_json(counters, switch_to_target_io_before);
    let target_update_io_before = counters.map(SlateDBIoCounters::snapshot);
    let target_update_start = Instant::now();
    update_files(lix, 1, changes, "target-merge").await;
    let target_update_ms = millis(target_update_start.elapsed());
    let target_update_io = io_delta_json(counters, target_update_io_before);
    let branch_ms = millis(branch_start.elapsed());

    let preview_start = Instant::now();
    let preview = lix
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: SOURCE_BRANCH_ID.to_owned(),
        })
        .await
        .expect("preview lifecycle merge");
    let merge_preview_ms = millis(preview_start.elapsed());
    let merge_start = Instant::now();
    let receipt = lix
        .merge_branch(MergeBranchOptions {
            source_branch_id: SOURCE_BRANCH_ID.to_owned(),
        })
        .await
        .expect("commit lifecycle merge");
    let merge_ms = millis(merge_start.elapsed());
    let merge_ok = preview.change_stats == receipt.change_stats
        && preview.conflicts.is_empty()
        && scalar_count(lix, "SELECT count(*) AS count FROM lix_file").await == rows as i64;

    let delete_start = Instant::now();
    lix.execute(
        "DELETE FROM lix_branch WHERE id = $1",
        &[Value::Text(SOURCE_BRANCH_ID.to_owned())],
    )
    .await
    .expect("delete lifecycle source branch");
    let branch_delete_ms = millis(delete_start.elapsed());
    (
        branch_ms,
        merge_preview_ms,
        merge_ms,
        branch_delete_ms,
        merge_ok,
        json!({
            "active_branch_id": active_branch_ms,
            "create": branch_create_ms,
            "create_io": create_io,
            "switch_to_source": switch_to_source_ms,
            "switch_to_source_io": switch_to_source_io,
            "source_update": source_update_ms,
            "source_update_io": source_update_io,
            "switch_to_target": switch_to_target_ms,
            "switch_to_target_io": switch_to_target_io,
            "target_update": target_update_ms,
            "target_update_io": target_update_io,
        }),
    )
}

async fn scalar_count<S>(lix: &Lix<S>, sql: &str) -> i64
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(sql, &[])
        .await
        .expect("execute lifecycle count")
        .rows()
        .first()
        .and_then(|row| row.get::<i64>("count").ok())
        .unwrap_or_default()
}

fn merge_change_count(rows: usize, shape: MergeShape) -> usize {
    let requested = match shape {
        MergeShape::Sparse => rows.div_ceil(10_000),
        MergeShape::Dense => rows.div_ceil(1_000),
    };
    requested.max(1).min(rows / 2)
}

fn file_id(index: usize) -> String {
    format!("00000000-0000-0000-0000-{index:012x}")
}

fn file_payload(tag: &str, index: usize) -> Vec<u8> {
    let mut payload = format!("tag={tag};index={index:012};").into_bytes();
    payload.resize(
        if index == 0 {
            CORRUPTION_SENTINEL_BYTES
        } else {
            FILE_BYTES
        },
        b'.',
    );
    payload
}

fn provenance() -> JsonValue {
    let executable = std::env::current_exe().expect("resolve lifecycle executable");
    let bytes = fs::read(&executable).expect("read lifecycle executable");
    json!({
        "commit_sha": option_env!("LIX_BENCH_COMMIT_SHA").unwrap_or("unrecorded"),
        "binary_sha256": format!("{:x}", Sha256::digest(bytes)),
        "rustc_version": option_env!("LIX_BENCH_RUSTC_VERSION").unwrap_or("unrecorded"),
        "cargo_profile": if cfg!(debug_assertions) { "debug" } else { "release" },
        "target_arch": std::env::consts::ARCH,
        "target_os": std::env::consts::OS,
    })
}

fn snapshot_json(snapshot: SlateDBIoSnapshot) -> JsonValue {
    json!({
        "read_objects": snapshot.read_objects,
        "read_bytes": snapshot.read_bytes,
        "write_objects": snapshot.write_objects,
        "write_bytes": snapshot.write_bytes,
        "list_operations": snapshot.list_operations,
        "listed_objects": snapshot.listed_objects,
        "deleted_objects": snapshot.deleted_objects,
        "copied_objects": snapshot.copied_objects,
        "immutable_locator_rows": snapshot.immutable_locator_rows,
        "cache_filesystem_reads": snapshot.cache_filesystem_reads,
        "cache_filesystem_writes": snapshot.cache_filesystem_writes,
        "cache_filesystem_removes": snapshot.cache_filesystem_removes,
        "writer_gate_acquisitions": snapshot.writer_gate_acquisitions,
        "writer_gate_wait_nanos": snapshot.writer_gate_wait_nanos,
        "wal_read_objects": snapshot.wal.read_objects,
        "wal_read_bytes": snapshot.wal.read_bytes,
        "wal_write_objects": snapshot.wal.write_objects,
        "wal_write_bytes": snapshot.wal.write_bytes,
        "compacted_read_objects": snapshot.compacted.read_objects,
        "compacted_read_bytes": snapshot.compacted.read_bytes,
        "compacted_write_objects": snapshot.compacted.write_objects,
        "compacted_write_bytes": snapshot.compacted.write_bytes,
        "manifest_read_objects": snapshot.manifest.read_objects,
        "manifest_read_bytes": snapshot.manifest.read_bytes,
        "manifest_write_objects": snapshot.manifest.write_objects,
        "manifest_write_bytes": snapshot.manifest.write_bytes,
        "compaction_read_objects": snapshot.compactions.read_objects,
        "compaction_read_bytes": snapshot.compactions.read_bytes,
        "compaction_write_objects": snapshot.compactions.write_objects,
        "compaction_write_bytes": snapshot.compactions.write_bytes,
        "other_read_objects": snapshot.other.read_objects,
        "other_read_bytes": snapshot.other.read_bytes,
        "other_write_objects": snapshot.other.write_objects,
        "other_write_bytes": snapshot.other.write_bytes,
        "main_read_requests": snapshot.main.read_requests,
        "main_write_requests": snapshot.main.write_requests,
        "reader_read_requests": snapshot.reader.read_requests,
        "reader_write_requests": snapshot.reader.write_requests,
        "compactor_read_requests": snapshot.compactor.read_requests,
        "compactor_write_requests": snapshot.compactor.write_requests,
        "gc_read_requests": snapshot.gc.read_requests,
        "gc_write_requests": snapshot.gc.write_requests,
    })
}

fn io_delta_json(
    counters: Option<&SlateDBIoCounters>,
    before: Option<SlateDBIoSnapshot>,
) -> JsonValue {
    match (counters, before) {
        (Some(counters), Some(before)) => snapshot_json(counters.snapshot().saturating_sub(before)),
        _ => JsonValue::Null,
    }
}

fn signed_delta(after: u64, before: u64) -> i64 {
    if after >= before {
        i64::try_from(after - before).unwrap_or(i64::MAX)
    } else {
        -i64::try_from(before - after).unwrap_or(i64::MAX)
    }
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn directory_bytes(path: &Path) -> u64 {
    let Ok(entries) = fs::read_dir(path) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| {
            let path = entry.path();
            match entry.metadata() {
                Ok(metadata) if metadata.is_dir() => directory_bytes(&path),
                Ok(metadata) => metadata.len(),
                Err(_) => 0,
            }
        })
        .sum()
}

fn copy_tree(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).expect("create corruption copy");
    for entry in fs::read_dir(source).expect("read lifecycle storage for copy") {
        let entry = entry.expect("read lifecycle storage entry");
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if entry
            .file_type()
            .expect("read lifecycle entry type")
            .is_dir()
        {
            copy_tree(&source_path, &destination_path);
        } else {
            fs::copy(&source_path, &destination_path).unwrap_or_else(|error| {
                panic!(
                    "copy lifecycle corruption fixture {} -> {}: {error}",
                    source_path.display(),
                    destination_path.display()
                )
            });
        }
    }
}

fn select_corruption_target(root: &Path) -> Option<PathBuf> {
    let mut files = Vec::new();
    collect_files(root, &mut files);
    files.sort_by_key(|path| {
        let text = path.to_string_lossy().to_ascii_lowercase();
        if text.contains("immutable") {
            0
        } else if text.contains("manifest") {
            1
        } else if text.ends_with(".sst") {
            2
        } else if text.ends_with(".log") || text.ends_with(".wal") {
            3
        } else {
            4
        }
    });
    files.into_iter().find(|path| {
        path.file_name()
            .is_some_and(|name| name != "LOCK" && name != "CURRENT")
            && fs::metadata(path).is_ok_and(|metadata| metadata.len() > 8)
    })
}

fn collect_files(root: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            collect_files(&path, files);
        } else {
            files.push(path);
        }
    }
}

fn corrupt_file(path: &Path) {
    if path
        .to_string_lossy()
        .to_ascii_lowercase()
        .contains("immutable")
    {
        let store = path.parent().expect("immutable target store directory");
        for entry in fs::read_dir(store).expect("read immutable corruption store") {
            let entry = entry.expect("read immutable corruption entry");
            if entry
                .file_type()
                .expect("read immutable corruption type")
                .is_file()
            {
                fs::remove_file(entry.path()).unwrap_or_else(|error| {
                    panic!(
                        "remove corruption target {}: {error}",
                        entry.path().display()
                    )
                });
            }
        }
        return;
    }
    let mut bytes = fs::read(path)
        .unwrap_or_else(|error| panic!("read corruption target {}: {error}", path.display()));
    let index = bytes.len() / 2;
    bytes[index] ^= 0x5a;
    fs::write(path, bytes)
        .unwrap_or_else(|error| panic!("write corruption target {}: {error}", path.display()));
}
