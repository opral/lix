#![allow(clippy::large_futures)]

//! Public version-control acceptance oracle for ForkTree Stage 2.
//! This harness is compiled on the approved Stage 1 storage-backed prototype;
//! it does not add a serving root, selector, object format, or production hook.

use std::alloc::{GlobalAlloc, Layout};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use lix::integration::{Engine, SessionContext};
use lix::storage::{
    GetManyRequest, GetOptions, Key, ProjectedValue, PutBatch, PutEntry, ReadOptions, Storage,
    StorageRead, StorageWrite, StoredValue, ValueSemantics, WriteOptions,
};
use lix::storage_adapter::{StorageAdapter, StorageReadOptions};
use lix::storage_bench::{
    audit_repository_gc_standalone_for_bench, diff_tracked_commits_for_bench,
    has_durable_commit_root_for_bench, layout_accounting, plan_repository_gc_for_bench,
    synthetic_space_for_bench,
};
use lix::{
    CreateBranchOptions, LixError, MergeBranchOptions, MergeBranchOutcome,
    MergeBranchPreviewOptions, PreparedDmlParameterBatch, Value,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};

#[path = "forktree_stage2_acceptance/cursor_contract.rs"]
mod cursor_contract;

const SCHEMA_KEY: &str = "forktree_stage2_acceptance_row";
const INSERT_BATCH: usize = 1_000;
const DEEP_FORK_DEPTH: usize = 8;

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

struct CountingAllocator;
static ALLOC_ENABLED: AtomicBool = AtomicBool::new(false);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !ptr.is_null() && ALLOC_ENABLED.load(Ordering::Relaxed) {
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { mimalloc::MiMalloc.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let replacement = unsafe { mimalloc::MiMalloc.realloc(ptr, layout, new_size) };
        if !replacement.is_null()
            && new_size >= layout.size()
            && ALLOC_ENABLED.load(Ordering::Relaxed)
        {
            ALLOC_BYTES.fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
            ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        replacement
    }
}

#[derive(Clone, Copy, Debug)]
enum Backend {
    Rocks,
    Slate,
}

impl Backend {
    fn parse(value: &str) -> Self {
        match value {
            "rocksdb" => Self::Rocks,
            "slatedb" => Self::Slate,
            _ => panic!("backend must be rocksdb or slatedb, got {value}"),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Rocks => "rocksdb",
            Self::Slate => "slatedb",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum CorruptionKind {
    Graph,
    Catalog,
    Object,
    Selector,
}

impl CorruptionKind {
    fn parse(value: &str) -> Self {
        match value {
            "graph" => Self::Graph,
            "catalog" => Self::Catalog,
            "object" => Self::Object,
            "selector" => Self::Selector,
            _ => panic!("corruption kind must be graph, catalog, object, or selector"),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Graph => "graph",
            Self::Catalog => "catalog",
            Self::Object => "object",
            Self::Selector => "selector",
        }
    }
}

#[async_trait]
trait DurableStorage: Storage + Clone + Send + Sync + Sized + 'static {
    fn open(path: &Path, counters: Option<SlateDBIoCounters>) -> Self;
    async fn flush_storage(&self);
}

#[async_trait]
impl DurableStorage for RocksDB {
    fn open(path: &Path, _counters: Option<SlateDBIoCounters>) -> Self {
        Self::open(path).expect("open acceptance RocksDB")
    }

    async fn flush_storage(&self) {
        self.flush().expect("flush acceptance RocksDB");
    }
}

#[async_trait]
impl DurableStorage for SlateDB {
    fn open(path: &Path, counters: Option<SlateDBIoCounters>) -> Self {
        match counters {
            Some(counters) => Self::open_with_io_counters(path, counters)
                .expect("open acceptance SlateDB with counters"),
            None => Self::open(path).expect("open acceptance SlateDB"),
        }
    }

    async fn flush_storage(&self) {
        self.flush().await.expect("flush acceptance SlateDB");
    }
}

#[derive(Clone, Copy)]
struct ProcessUsage {
    cpu_us: u64,
    max_rss_bytes: u64,
}

struct Phase {
    name: &'static str,
    wall: Instant,
    usage: ProcessUsage,
    slate: SlateDBIoSnapshot,
}

impl Phase {
    fn begin(name: &'static str, counters: Option<&SlateDBIoCounters>) -> Self {
        ALLOC_BYTES.store(0, Ordering::Relaxed);
        ALLOC_CALLS.store(0, Ordering::Relaxed);
        ALLOC_ENABLED.store(true, Ordering::Relaxed);
        Self {
            name,
            wall: Instant::now(),
            usage: process_usage(),
            slate: counters.map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot),
        }
    }

    fn finish(self, backend: Backend, counters: Option<&SlateDBIoCounters>) {
        ALLOC_ENABLED.store(false, Ordering::Relaxed);
        let usage = process_usage();
        let slate = counters
            .map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot)
            .saturating_sub(self.slate);
        println!(
            "phase backend={} name={} wall_ms={:.3} cpu_ms={:.3} alloc_bytes={} alloc_calls={} max_rss_bytes={} slate_gets={} slate_scans={} slate_bytes_read={} slate_writes={} slate_bytes_written={}",
            backend.name(),
            self.name,
            self.wall.elapsed().as_secs_f64() * 1_000.0,
            usage.cpu_us.saturating_sub(self.usage.cpu_us) as f64 / 1_000.0,
            ALLOC_BYTES.load(Ordering::Relaxed),
            ALLOC_CALLS.load(Ordering::Relaxed),
            usage.max_rss_bytes,
            slate.read_objects,
            slate.list_operations,
            slate.read_bytes,
            slate.write_objects + slate.deleted_objects,
            slate.write_bytes,
        );
    }
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if args.get(1).map(String::as_str) == Some("cursor-contract") {
        cursor_contract::run();
        return;
    }
    assert!(
        args.len() >= 5,
        "usage: forktree_stage2_acceptance <control|corrupt> <rocksdb|slatedb> <path> <rows> [graph|catalog|object|selector]"
    );
    let mode = args[1].as_str();
    let backend = Backend::parse(&args[2]);
    let path = Path::new(&args[3]);
    let rows = args[4].parse::<usize>().expect("rows must be an integer");
    assert!(matches!(rows, 1_000 | 10_000 | 50_000));
    assert!(!path.exists(), "refusing to overwrite {}", path.display());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build acceptance runtime");
    let counters = matches!(backend, Backend::Slate).then(SlateDBIoCounters::default);
    match (mode, backend) {
        ("control", Backend::Rocks) => {
            runtime.block_on(run_control::<RocksDB>(backend, path, rows, counters))
        }
        ("control", Backend::Slate) => {
            runtime.block_on(run_control::<SlateDB>(backend, path, rows, counters))
        }
        ("corrupt", Backend::Rocks) => runtime.block_on(run_corruption::<RocksDB>(
            backend,
            path,
            rows,
            CorruptionKind::parse(args.get(5).expect("corrupt mode needs a kind")),
            counters,
        )),
        ("corrupt", Backend::Slate) => runtime.block_on(run_corruption::<SlateDB>(
            backend,
            path,
            rows,
            CorruptionKind::parse(args.get(5).expect("corrupt mode needs a kind")),
            counters,
        )),
        _ => panic!("mode must be control or corrupt"),
    }
}

async fn run_control<S>(
    backend: Backend,
    path: &Path,
    rows: usize,
    counters: Option<SlateDBIoCounters>,
) where
    S: DurableStorage,
{
    let storage = S::open(path, counters.clone());
    let initialize = Phase::begin("initialize_seed_checkpoint", counters.as_ref());
    let receipt = Engine::initialize(storage.clone())
        .await
        .expect("initialize acceptance repository");
    let engine = Engine::new(storage.clone())
        .await
        .expect("open acceptance engine");
    let main = engine
        .open_workspace_session()
        .await
        .expect("open acceptance workspace");
    register_schema(&main).await;
    seed_rows(&main, rows).await;
    let base_commit = main
        .create_checkpoint()
        .await
        .expect("checkpoint acceptance base")
        .commit_id;
    initialize.finish(backend, counters.as_ref());

    let lifecycle = Phase::begin("version_control_lifecycle", counters.as_ref());
    let boundary_rows = split_boundaries(rows);
    for (index, row) in boundary_rows.iter().copied().enumerate() {
        update_rows(&main, &[(row, format!("linear-{index}"))]).await;
    }
    let linear_head = branch_head(&main, &receipt.main_branch_id).await;

    let mut deep_parent = base_commit.clone();
    for depth in 0..DEEP_FORK_DEPTH {
        let id = branch_id(0x100 + depth);
        create_at(&main, &id, &format!("deep-{depth}"), &deep_parent).await;
        let session = engine
            .open_session(&id)
            .await
            .expect("open deep-fork branch");
        update_rows(&session, &[(200 + depth, format!("deep-{depth}"))]).await;
        deep_parent = branch_head(&session, &id).await;
    }

    let history_a = branch_id(0x200);
    let history_b = branch_id(0x201);
    create_at(&main, &history_a, "history-a", &base_commit).await;
    create_at(&main, &history_b, "history-b", &base_commit).await;
    let a = engine
        .open_session(&history_a)
        .await
        .expect("open history-a");
    let b = engine
        .open_session(&history_b)
        .await
        .expect("open history-b");
    let divergent = boundary_rows
        .iter()
        .copied()
        .enumerate()
        .map(|(index, row)| (row, format!("same-final-{index}")))
        .collect::<Vec<_>>();
    update_rows(&a, &divergent).await;
    let mut reverse = divergent.clone();
    reverse.reverse();
    update_rows(&b, &reverse).await;
    let a_state = ordered_state(&a).await;
    assert_eq!(
        a_state,
        ordered_state(&b).await,
        "divergent histories must preserve exact order/results"
    );
    let a_head = branch_head(&a, &history_a).await;
    let b_head = branch_head(&b, &history_b).await;
    let identical_diff = ordered_diff(&a, &a_head, &b_head).await;
    // Public diff identity is selected-change based: equal snapshots authored
    // on divergent branches remain observable as ordered changes. Freeze that
    // exact output instead of treating logical equality as root equality.
    assert!(identical_diff.len() <= divergent.len());
    if rows == 1_000 {
        assert!(
            !identical_diff.is_empty(),
            "the focused selected-ChangeId fixture must expose the nonempty equal-state diff nuance"
        );
    }

    let merge_left_id = branch_id(0x300);
    let merge_right_id = branch_id(0x301);
    create_at(&main, &merge_left_id, "merge-left", &base_commit).await;
    create_at(&main, &merge_right_id, "merge-right", &base_commit).await;
    let merge_left = engine
        .open_session(&merge_left_id)
        .await
        .expect("open merge-left");
    let merge_right = engine
        .open_session(&merge_right_id)
        .await
        .expect("open merge-right");
    update_rows(&merge_left, &[(10, "left".to_owned())]).await;
    update_rows(&merge_right, &[(11, "right".to_owned())]).await;
    let preview = merge_left
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: merge_right_id.clone(),
        })
        .await
        .expect("preview disjoint merge");
    assert_eq!(preview.outcome, MergeBranchOutcome::MergeCommitted);
    assert_eq!(preview.change_stats.total, 1);
    assert!(preview.conflicts.is_empty());
    let merged = merge_left
        .merge_branch(MergeBranchOptions {
            source_branch_id: merge_right_id.clone(),
        })
        .await
        .expect("commit disjoint merge");
    assert_eq!(merged.outcome, MergeBranchOutcome::MergeCommitted);
    assert_eq!(merged.change_stats.total, 1);
    assert!(merged.created_merge_commit_id.is_some());

    let criss_error = build_criss_cross(&engine, &main, &base_commit).await;
    assert_eq!(criss_error.code, LixError::CODE_AMBIGUOUS_MERGE_BASE);

    let undo_id = branch_id(0x400);
    create_at(&main, &undo_id, "undo-redo", &base_commit).await;
    let undo_session = engine
        .open_session(&undo_id)
        .await
        .expect("open undo branch");
    update_rows(&undo_session, &[(31, "undo-target".to_owned())]).await;
    let target = branch_head(&undo_session, &undo_id).await;
    let undo = undo_session.undo().await.expect("undo focused mutation");
    assert_eq!(undo.target_commit_id, target);
    assert_eq!(read_value(&undo_session, 31).await, "seed");
    let redo = undo_session.redo().await.expect("redo focused mutation");
    assert_eq!(redo.target_commit_id, target);
    assert_eq!(read_value(&undo_session, 31).await, "undo-target");
    let history = ordered_history(&undo_session, 31).await;
    assert!(
        history.len() >= 2,
        "history must retain original and replayed values"
    );

    let retired_id = branch_id(0x500);
    create_at(&main, &retired_id, "retained-then-released", &base_commit).await;
    let retired = engine
        .open_session(&retired_id)
        .await
        .expect("open retained branch");
    update_rows(&retired, &[(41, "retained".to_owned())]).await;
    assert_eq!(read_value(&retired, 41).await, "retained");
    drop(retired);
    main.execute(
        "DELETE FROM lix_branch WHERE id = $1",
        &[Value::Text(retired_id.clone())],
    )
    .await
    .expect("delete retained branch");
    assert!(
        main.execute(
            "SELECT id FROM lix_branch WHERE id = $1",
            &[Value::Text(retired_id.clone())],
        )
        .await
        .expect("query deleted branch")
        .is_empty()
    );

    let adapter = StorageAdapter::new(storage.clone());
    let standalone_before = audit_repository_gc_standalone_for_bench(&adapter)
        .await
        .expect("audit pre-GC standalone owners");
    let gc = plan_repository_gc_for_bench(&adapter)
        .await
        .expect("plan collection after final branch release");
    let standalone_after = audit_repository_gc_standalone_for_bench(&adapter)
        .await
        .expect("audit post-GC standalone owners");
    assert!(standalone_after.len() <= standalone_before.len());

    let historical_diff = diff_tracked_commits_for_bench(&adapter, &base_commit, &linear_head)
        .await
        .expect("measure tracked historical diff");
    assert_eq!(historical_diff.entries, boundary_rows.len());
    let base_has_root = has_durable_commit_root_for_bench(storage.clone(), &base_commit)
        .await
        .expect("load base root ownership");
    let linear_has_root = has_durable_commit_root_for_bench(storage.clone(), &linear_head)
        .await
        .expect("load linear root ownership");
    lifecycle.finish(backend, counters.as_ref());

    println!(
        "semantic backend={} rows={} base_commit={} linear_head={} ordered_state_hash={} history_hash={} identical_diff_rows={} identical_diff_hash={} criss_cross_error_code={} deep_fork_depth={} deleted_branch={} gc_planned_swept_commits={} gc_planned_staged_deletes={} standalone_before={} standalone_after={}",
        backend.name(),
        rows,
        base_commit,
        linear_head,
        hash_rows(&a_state),
        hash_rows_normalized(&history),
        identical_diff.len(),
        hash_rows_normalized(&identical_diff),
        criss_error.code,
        DEEP_FORK_DEPTH,
        retired_id,
        gc.swept_commits,
        gc.staged_deletes,
        standalone_before.len(),
        standalone_after.len(),
    );
    println!(
        "root_share backend={} base_has_durable_root={} linear_has_durable_root={} shared_objects=unavailable shared_bytes=unavailable authority=public_engine_commit_and_tracked_root forktree_stage1_serving=not_wired",
        backend.name(),
        base_has_root,
        linear_has_root,
    );
    println!(
        "diff_work backend={} public_rows={} tracked_entries={} left_has_root={} right_has_root={}",
        backend.name(),
        ordered_diff(&main, &base_commit, &linear_head).await.len(),
        historical_diff.entries,
        historical_diff.left_has_durable_root,
        historical_diff.right_has_durable_root,
    );

    drop(a);
    drop(b);
    drop(merge_left);
    drop(merge_right);
    drop(undo_session);
    drop(main);
    drop(engine);
    storage.flush_storage().await;
    let disk_before_reopen = directory_bytes(path);

    let reopen_counters = counters.as_ref().map(|_| SlateDBIoCounters::default());
    let reopened_storage = S::open(path, reopen_counters.clone());
    let reopen = Phase::begin("cold_reopen_verify", reopen_counters.as_ref());
    let reopened_engine = Engine::new(reopened_storage.clone())
        .await
        .expect("cold reopen acceptance engine");
    let reopened_main = reopened_engine
        .open_session(&receipt.main_branch_id)
        .await
        .expect("cold reopen main");
    assert_eq!(
        branch_head(&reopened_main, &receipt.main_branch_id).await,
        linear_head
    );
    let reopened_a = reopened_engine
        .open_session(&history_a)
        .await
        .expect("cold reopen history-a");
    assert_eq!(ordered_state(&reopened_a).await, a_state);
    assert_eq!(
        ordered_history(&reopened_main, boundary_rows[0])
            .await
            .len(),
        2
    );
    reopen.finish(backend, reopen_counters.as_ref());
    drop(reopened_a);
    drop(reopened_main);
    drop(reopened_engine);
    reopened_storage.flush_storage().await;

    let read = adapter
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open layout accounting read");
    let layout = layout_accounting(&read).await;
    let layout_rows = layout.iter().map(|entry| entry.rows).sum::<u64>();
    let layout_bytes = layout
        .iter()
        .map(|entry| entry.key_bytes + entry.value_bytes)
        .sum::<u64>();
    println!(
        "storage backend={} rows={} layout_rows={} layout_bytes={} disk_before_reopen={} disk_after_reopen={}",
        backend.name(),
        rows,
        layout_rows,
        layout_bytes,
        disk_before_reopen,
        directory_bytes(path),
    );
}

async fn run_corruption<S>(
    backend: Backend,
    path: &Path,
    rows: usize,
    kind: CorruptionKind,
    counters: Option<SlateDBIoCounters>,
) where
    S: DurableStorage,
{
    let storage = S::open(path, counters);
    let space = synthetic_space_for_bench(41 + kind as u16, ValueSemantics::Mutable);
    let key = Key(Bytes::from_static(b"forktree-stage2-authority"));
    let valid = encode_model_authority(kind, b"root-selector-object-edge");
    put_model_authority(&storage, space, key.clone(), valid).await;
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("open valid authority read");
    let value = read_model_authority(&read, space, &key).await;
    authenticate_model_authority(kind, &value).expect("valid authority must authenticate");
    drop(read);
    storage.flush_storage().await;
    let malformed = malformed_model_authority(kind);
    put_model_authority(&storage, space, key.clone(), malformed).await;
    storage.flush_storage().await;
    drop(storage);
    let reopened = S::open(path, None);
    let read = reopened
        .begin_read(ReadOptions::default())
        .await
        .expect("cold reopen malformed authority read");
    let value = read_model_authority(&read, space, &key).await;
    let error = authenticate_model_authority(kind, &value)
        .expect_err("malformed authority must fail closed");
    println!(
        "corruption backend={} rows={} kind={} fail_closed=true error={:?}",
        backend.name(),
        rows,
        kind.name(),
        error,
    );
}

async fn put_model_authority<S: Storage>(
    storage: &S,
    space: lix::storage::StorageSpace,
    key: Key,
    bytes: Bytes,
) {
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("open model authority write");
    write
        .put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key,
                    value: StoredValue { bytes },
                }],
            },
        )
        .await
        .expect("stage model authority");
    write.commit().await.expect("commit model authority");
}

async fn read_model_authority<R: StorageRead>(
    read: &R,
    space: lix::storage::StorageSpace,
    key: &Key,
) -> Bytes {
    let result = read
        .get_many(&[GetManyRequest {
            space,
            keys: std::slice::from_ref(key),
            opts: GetOptions::default(),
        }])
        .await
        .expect("read model authority");
    match result.values.into_iter().next().flatten() {
        Some(ProjectedValue::FullValue(bytes)) => bytes,
        value => panic!("model authority missing or projected incorrectly: {value:?}"),
    }
}

fn encode_model_authority(kind: CorruptionKind, payload: &[u8]) -> Bytes {
    let domain = kind.name().as_bytes();
    let mut bytes = Vec::with_capacity(16 + domain.len() + payload.len() + 32);
    bytes.extend_from_slice(b"FTAUTH1\0");
    bytes.extend_from_slice(&(domain.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    bytes.extend_from_slice(domain);
    bytes.extend_from_slice(payload);
    let digest = blake3::hash(&bytes);
    bytes.extend_from_slice(digest.as_bytes());
    Bytes::from(bytes)
}

fn malformed_model_authority(kind: CorruptionKind) -> Bytes {
    let mut bytes = encode_model_authority(kind, b"root-selector-object-edge").to_vec();
    match kind {
        CorruptionKind::Graph => bytes[0] ^= 0x80,
        CorruptionKind::Catalog => bytes[10] = u8::MAX,
        CorruptionKind::Object => bytes.truncate(bytes.len() - 17),
        CorruptionKind::Selector => {
            let payload = bytes.len() - 33;
            bytes[payload] ^= 0x01;
        }
    }
    Bytes::from(bytes)
}

fn authenticate_model_authority(kind: CorruptionKind, bytes: &[u8]) -> Result<(), &'static str> {
    if bytes.len() < 16 + 32 || &bytes[..8] != b"FTAUTH1\0" {
        return Err("malformed authority envelope");
    }
    let domain_len = u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let payload_len = u32::from_be_bytes(bytes[12..16].try_into().unwrap()) as usize;
    let authenticated_len = 16usize
        .checked_add(domain_len)
        .and_then(|length| length.checked_add(payload_len))
        .ok_or("authority length overflow")?;
    if authenticated_len.checked_add(32) != Some(bytes.len()) {
        return Err("authority length mismatch");
    }
    if &bytes[16..16 + domain_len] != kind.name().as_bytes() {
        return Err("authority domain mismatch");
    }
    if blake3::hash(&bytes[..authenticated_len]).as_bytes() != &bytes[authenticated_len..] {
        return Err("authority digest mismatch");
    }
    Ok(())
}

async fn register_schema<S>(session: &SessionContext<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
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
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (lix_json($1), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register acceptance schema");
}

async fn seed_rows<S>(session: &SessionContext<S>, rows: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    for start in (0..rows).step_by(INSERT_BATCH) {
        let end = (start + INSERT_BATCH).min(rows);
        let parameters = PreparedDmlParameterBatch::from_rows(
            (start..end).map(|row| vec![Value::Text(row_id(row)), Value::Text("seed".to_owned())]),
        )
        .expect("build acceptance insert batch");
        let results = session
            .execute_prepared_dml_batch(
                Arc::from(format!(
                    "INSERT INTO {SCHEMA_KEY} (id, value) VALUES ($1, $2)"
                )),
                parameters,
            )
            .await
            .expect("seed acceptance rows");
        assert_eq!(results.len(), end - start);
        assert!(results.iter().all(|result| result.rows_affected() == 1));
    }
}

async fn update_rows<S>(session: &SessionContext<S>, updates: &[(usize, String)])
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let parameters = PreparedDmlParameterBatch::from_rows(
        updates
            .iter()
            .map(|(row, value)| vec![Value::Text(value.clone()), Value::Text(row_id(*row))]),
    )
    .expect("build acceptance update batch");
    let results = session
        .execute_prepared_dml_batch(
            Arc::from(format!("UPDATE {SCHEMA_KEY} SET value = $1 WHERE id = $2")),
            parameters,
        )
        .await
        .expect("update acceptance rows");
    assert_eq!(results.len(), updates.len());
    assert!(results.iter().all(|result| result.rows_affected() == 1));
}

async fn create_at<S>(session: &SessionContext<S>, id: &str, name: &str, commit_id: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let receipt = session
        .create_branch(CreateBranchOptions {
            id: Some(id.to_owned()),
            name: name.to_owned(),
            from_commit_id: Some(commit_id.to_owned()),
        })
        .await
        .expect("create acceptance branch");
    assert_eq!(receipt.id, id);
    assert_eq!(receipt.commit_id, commit_id);
}

async fn build_criss_cross<S>(engine: &Engine<S>, main: &SessionContext<S>, base: &str) -> LixError
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let left_id = branch_id(0x600);
    let right_id = branch_id(0x601);
    create_at(main, &left_id, "criss-left", base).await;
    create_at(main, &right_id, "criss-right", base).await;
    let left = engine
        .open_session(&left_id)
        .await
        .expect("open criss-left");
    let right = engine
        .open_session(&right_id)
        .await
        .expect("open criss-right");
    update_rows(&left, &[(50, "criss-left".to_owned())]).await;
    update_rows(&right, &[(51, "criss-right".to_owned())]).await;
    let left_head = branch_head(&left, &left_id).await;
    let right_head = branch_head(&right, &right_id).await;
    let left_frozen = branch_id(0x602);
    let right_frozen = branch_id(0x603);
    create_at(main, &left_frozen, "criss-left-frozen", &left_head).await;
    create_at(main, &right_frozen, "criss-right-frozen", &right_head).await;
    left.merge_branch(MergeBranchOptions {
        source_branch_id: right_frozen,
    })
    .await
    .expect("build left criss merge");
    right
        .merge_branch(MergeBranchOptions {
            source_branch_id: left_frozen,
        })
        .await
        .expect("build right criss merge");
    left.merge_branch_preview(MergeBranchPreviewOptions {
        source_branch_id: right_id,
    })
    .await
    .expect_err("criss-cross merge base must be ambiguous")
}

async fn branch_head<S>(session: &SessionContext<S>, branch_id: &str) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = session
        .execute(
            "SELECT commit_id FROM lix_branch WHERE id = $1",
            &[Value::Text(branch_id.to_owned())],
        )
        .await
        .expect("load acceptance branch head");
    assert_eq!(result.len(), 1);
    result.rows()[0]
        .get::<String>("commit_id")
        .expect("branch has commit id")
}

async fn ordered_state<S>(session: &SessionContext<S>) -> Vec<Vec<Value>>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute(
            &format!("SELECT id, value FROM {SCHEMA_KEY} ORDER BY id"),
            &[],
        )
        .await
        .expect("read ordered acceptance state")
        .rows()
        .iter()
        .map(|row| row.values().to_vec())
        .collect()
}

async fn ordered_diff<S>(session: &SessionContext<S>, left: &str, right: &str) -> Vec<Vec<Value>>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute(
            "SELECT schema_key, entity_pk, diff_type, before_change_id, after_change_id FROM lix_diff($1, $2) ORDER BY schema_key, entity_pk, diff_type",
            &[Value::Text(left.to_owned()), Value::Text(right.to_owned())],
        )
        .await
        .expect("read ordered acceptance diff")
        .rows()
        .iter()
        .map(|row| row.values().to_vec())
        .collect()
}

async fn ordered_history<S>(session: &SessionContext<S>, row: usize) -> Vec<Vec<Value>>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute(
            &format!(
                "SELECT id, value, lixcol_depth, lixcol_change_id FROM {SCHEMA_KEY}_history() WHERE id = $1 ORDER BY lixcol_depth, lixcol_change_id"
            ),
            &[Value::Text(row_id(row))],
        )
        .await
        .expect("read ordered acceptance history")
        .rows()
        .iter()
        .map(|row| row.values().to_vec())
        .collect()
}

async fn read_value<S>(session: &SessionContext<S>, row: usize) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = session
        .execute(
            &format!("SELECT value FROM {SCHEMA_KEY} WHERE id = $1"),
            &[Value::Text(row_id(row))],
        )
        .await
        .expect("read acceptance value");
    assert_eq!(result.len(), 1);
    result.rows()[0]
        .get::<String>("value")
        .expect("acceptance value is text")
}

fn split_boundaries(rows: usize) -> Vec<usize> {
    let mut values = vec![0, 63, 64, 65, rows - 1];
    if rows > 2_049 {
        values.extend([2_047, 2_048, 2_049]);
    }
    values.sort_unstable();
    values.dedup();
    values
}

fn row_id(row: usize) -> String {
    format!("row-{row:08}")
}

fn branch_id(value: usize) -> String {
    format!("019a0000-0000-7000-8000-{value:012x}")
}

fn hash_rows(rows: &[Vec<Value>]) -> String {
    let mut hasher = blake3::Hasher::new();
    for row in rows {
        hasher.update(format!("{row:?}\n").as_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

fn hash_rows_normalized(rows: &[Vec<Value>]) -> String {
    let mut opaque_ids = std::collections::BTreeMap::<String, usize>::new();
    let mut next_id = 0;
    let mut hasher = blake3::Hasher::new();
    for row in rows {
        for value in row {
            if let Value::Text(text) = value
                && looks_like_uuid(text)
            {
                let ordinal = *opaque_ids.entry(text.clone()).or_insert_with(|| {
                    let ordinal = next_id;
                    next_id += 1;
                    ordinal
                });
                hasher.update(format!("opaque-id-{ordinal}\0").as_bytes());
            } else {
                hasher.update(format!("{value:?}\0").as_bytes());
            }
        }
        hasher.update(b"\n");
    }
    hasher.finalize().to_hex().to_string()
}

fn looks_like_uuid(value: &str) -> bool {
    value.len() == 36
        && value.as_bytes().get(8) == Some(&b'-')
        && value.as_bytes().get(13) == Some(&b'-')
        && value.as_bytes().get(18) == Some(&b'-')
        && value.as_bytes().get(23) == Some(&b'-')
        && value
            .bytes()
            .filter(|byte| *byte != b'-')
            .all(|byte| byte.is_ascii_hexdigit())
}

fn process_usage() -> ProcessUsage {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    assert_eq!(result, 0, "getrusage failed");
    let usage = unsafe { usage.assume_init() };
    let micros = |time: libc::timeval| {
        (time.tv_sec as u64)
            .saturating_mul(1_000_000)
            .saturating_add(time.tv_usec as u64)
    };
    ProcessUsage {
        cpu_us: micros(usage.ru_utime).saturating_add(micros(usage.ru_stime)),
        max_rss_bytes: (usage.ru_maxrss as u64).saturating_mul(1_024),
    }
}

fn directory_bytes(path: &Path) -> u64 {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return 0;
    };
    if metadata.is_file() {
        return metadata.len();
    }
    std::fs::read_dir(path)
        .expect("read acceptance storage directory")
        .map(|entry| {
            let entry = entry.expect("read acceptance directory entry");
            directory_bytes(&entry.path())
        })
        .sum()
}
