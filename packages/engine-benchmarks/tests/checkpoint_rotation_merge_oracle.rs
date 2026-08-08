use std::future::Future;
use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use lix::storage::Storage;
use lix::storage_adapter::{
    StorageAdapter, StorageKey, StorageSpace, StorageSpaceId, StorageWriteOptions,
};
use lix::storage_bench::merge_base_for_bench;
use lix::{
    CreateBranchOptions, Lix, LixError, MergeBranchOptions, MergeBranchOutcome,
    SwitchBranchOptions, Value, open_lix,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;
use serde_json::json;

const SOURCE_BRANCH_ID: &str = "01920000-0000-7000-8000-00000000c001";
const MAIN_KEYS: [&str; 3] = ["history", "source", "target"];
const CHECKPOINT_GC_INTERVAL: usize = 64;
const COMMIT_SPACE: StorageSpace =
    StorageSpace::mutable(StorageSpaceId(0x0006_0001), "changelog.commit");

#[async_trait]
trait DurableBackend: Storage + Clone + Send + Sync + Sized + 'static {
    fn open(path: &Path) -> Self;
    async fn flush_all(&self);
}

#[async_trait]
impl DurableBackend for RocksDB {
    fn open(path: &Path) -> Self {
        Self::open(path).expect("open checkpoint-rotation RocksDB fixture")
    }

    async fn flush_all(&self) {
        self.flush()
            .expect("flush checkpoint-rotation RocksDB fixture");
    }
}

#[async_trait]
impl DurableBackend for SlateDB {
    fn open(path: &Path) -> Self {
        Self::open(path).expect("open checkpoint-rotation SlateDB fixture")
    }

    async fn flush_all(&self) {
        self.flush_memtable_for_diagnostics()
            .await
            .expect("flush checkpoint-rotation SlateDB fixture");
    }
}

#[test]
fn recovered_head_disjoint_merge_rocksdb() {
    run_on_large_stack(|| recovered_head_disjoint_merge::<RocksDB>());
}

#[test]
fn recovered_head_disjoint_merge_slatedb() {
    run_on_large_stack(|| recovered_head_disjoint_merge::<SlateDB>());
}

#[test]
fn recovered_head_true_conflict_rocksdb() {
    run_on_large_stack(|| recovered_head_true_conflict::<RocksDB>());
}

#[test]
fn recovered_head_true_conflict_slatedb() {
    run_on_large_stack(|| recovered_head_true_conflict::<SlateDB>());
}

#[test]
fn missing_recovered_ancestry_fails_closed_rocksdb() {
    run_on_large_stack(|| missing_recovered_ancestry_fails_closed::<RocksDB>());
}

#[test]
fn missing_recovered_ancestry_fails_closed_slatedb() {
    run_on_large_stack(|| missing_recovered_ancestry_fails_closed::<SlateDB>());
}

fn run_on_large_stack<F, Fut>(make_future: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + 'static,
{
    std::thread::Builder::new()
        .name("checkpoint-rotation-oracle".to_owned())
        .stack_size(16 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build checkpoint-rotation runtime")
                .block_on(make_future());
        })
        .expect("spawn checkpoint-rotation thread")
        .join()
        .expect("checkpoint-rotation thread should not panic");
}

#[derive(Debug)]
struct DivergedHeads {
    initial_head: String,
    history_head: String,
    checkpoint_head: String,
    source_head: String,
    target_head: String,
    main_branch_id: String,
}

async fn recovered_head_disjoint_merge<B: DurableBackend>() {
    let directory = tempfile::tempdir().expect("create disjoint checkpoint fixture");
    let path = directory.path().join("database");
    let storage = B::open(&path);
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("initialize disjoint checkpoint fixture");
    let heads = seed_checkpoint_divergence(&lix, false).await;

    assert_graph_edge(&lix, &heads.source_head, &heads.history_head, 0).await;
    assert_graph_edge(&lix, &heads.target_head, &heads.checkpoint_head, 0).await;
    assert_graph_edge(&lix, &heads.checkpoint_head, &heads.initial_head, 0).await;

    let actual_base = merge_base_for_bench(
        &StorageAdapter::new(storage.clone()),
        &heads.target_head,
        &heads.source_head,
    )
    .await
    .expect("authenticated merge-base traversal should succeed");
    let receipt = lix
        .merge_branch(MergeBranchOptions {
            source_branch_id: SOURCE_BRANCH_ID.to_owned(),
        })
        .await
        .unwrap_or_else(|error| {
            panic!(
                "disjoint recovered-head merge failed: history_head={}, actual_graph_base={}, checkpoint_head={}, target_head={}, source_head={}, error={error:?}",
                heads.history_head,
                actual_base,
                heads.checkpoint_head,
                heads.target_head,
                heads.source_head,
            )
        });
    assert_eq!(
        actual_base, heads.history_head,
        "merge base must be recovered head"
    );
    assert_eq!(receipt.base_commit_id, heads.history_head);
    assert_eq!(receipt.outcome, MergeBranchOutcome::MergeCommitted);
    assert_eq!(receipt.target_head_before_commit_id, heads.target_head);
    assert_eq!(receipt.source_head_before_commit_id, heads.source_head);
    assert_eq!(
        (receipt.change_stats.added, receipt.change_stats.modified),
        (0, 1)
    );
    assert_value(&lix, "history", "history-1").await;
    assert_value(&lix, "source", "source-1").await;
    assert_value(&lix, "target", "target-1").await;

    let merged_head = active_head(&lix).await;
    assert_graph_edge(&lix, &merged_head, &heads.target_head, 0).await;
    assert_graph_edge(&lix, &merged_head, &heads.source_head, 1).await;

    lix.undo().await.expect("undo disjoint merge");
    assert_value(&lix, "source", "seed-source").await;
    assert_value(&lix, "target", "target-1").await;
    lix.redo().await.expect("redo disjoint merge");
    assert_value(&lix, "source", "source-1").await;
    assert_value(&lix, "target", "target-1").await;

    for _ in 0..CHECKPOINT_GC_INTERVAL {
        lix.create_checkpoint()
            .await
            .expect("advance checkpoint recovery/GC cadence");
    }
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_commit_present(&lix, &merged_head).await;
    assert_history_present(&lix, "source").await;
    lix.close()
        .await
        .expect("close disjoint checkpoint fixture");
    storage.flush_all().await;
    drop(storage);

    let storage = B::open(&path);
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("cold reopen disjoint checkpoint fixture");
    assert_value(&lix, "history", "history-1").await;
    assert_value(&lix, "source", "source-1").await;
    assert_value(&lix, "target", "target-1").await;
    assert_history_present(&lix, "source").await;
    lix.switch_branch(SwitchBranchOptions {
        branch_id: SOURCE_BRANCH_ID.to_owned(),
    })
    .await
    .expect("cold reopen retained source branch");
    assert_value(&lix, "source", "source-1").await;
    assert_value(&lix, "target", "seed-target").await;
    lix.switch_branch(SwitchBranchOptions {
        branch_id: heads.main_branch_id,
    })
    .await
    .expect("return to cold reopened main branch");
    lix.close().await.expect("close cold reopened fixture");
    storage.flush_all().await;
}

async fn recovered_head_true_conflict<B: DurableBackend>() {
    let directory = tempfile::tempdir().expect("create true-conflict checkpoint fixture");
    let storage = B::open(&directory.path().join("database"));
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("initialize true-conflict checkpoint fixture");
    let heads = seed_checkpoint_divergence(&lix, true).await;
    let error = lix
        .merge_branch(MergeBranchOptions {
            source_branch_id: SOURCE_BRANCH_ID.to_owned(),
        })
        .await
        .expect_err("same-identity edits must remain a conflict");
    assert_eq!(error.code, LixError::CODE_MERGE_CONFLICT);
    let details = error.details.expect("merge conflict details");
    let conflicts = details["conflicts"]
        .as_array()
        .expect("merge conflict array");
    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0]["entityPk"], json!(["source"]));
    assert_eq!(conflicts[0]["kind"], json!("sameEntityChanged"));
    assert_eq!(active_head(&lix).await, heads.target_head);
    assert_value(&lix, "source", "target-source-1").await;
    lix.switch_branch(SwitchBranchOptions {
        branch_id: SOURCE_BRANCH_ID.to_owned(),
    })
    .await
    .expect("switch to conflicting source branch");
    assert_value(&lix, "source", "source-1").await;
    lix.close().await.expect("close true-conflict fixture");
    storage.flush_all().await;
}

async fn missing_recovered_ancestry_fails_closed<B: DurableBackend>() {
    let directory = tempfile::tempdir().expect("create corrupt checkpoint fixture");
    let path = directory.path().join("database");
    let (history_head, source_head, target_head) = {
        let storage = B::open(&path);
        let lix = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("initialize corrupt checkpoint fixture");
        let heads = seed_checkpoint_divergence(&lix, false).await;
        lix.close().await.expect("close corrupt checkpoint fixture");
        storage.flush_all().await;
        (heads.history_head, heads.source_head, heads.target_head)
    };

    let storage = B::open(&path);
    let adapter = StorageAdapter::new(storage.clone());
    let mut writes = adapter.new_write_set();
    let history_uuid = uuid::Uuid::parse_str(&history_head).expect("history head is UUID");
    writes.delete(
        COMMIT_SPACE,
        StorageKey(Bytes::copy_from_slice(history_uuid.as_bytes())),
    );
    adapter
        .commit_write_set(writes, StorageWriteOptions::default())
        .await
        .expect("delete recovered ancestry commit corruption fixture");
    storage.flush_all().await;
    drop(adapter);
    drop(storage);

    let storage = B::open(&path);
    let error = merge_base_for_bench(
        &StorageAdapter::new(storage.clone()),
        &target_head,
        &source_head,
    )
    .await
    .expect_err("missing recovered ancestry must fail closed");
    let rendered = error.to_string();
    assert!(
        rendered.contains(&history_head) || rendered.contains("missing"),
        "unexpected missing-ancestry error: {error:?}"
    );
    storage.flush_all().await;
}

async fn seed_checkpoint_divergence<S>(lix: &Lix<S>, same_identity: bool) -> DivergedHeads
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let initial_head = active_head(lix).await;
    let mut seed = lix
        .begin_transaction()
        .await
        .expect("begin three-row seed transaction");
    for (key, value) in MAIN_KEYS
        .into_iter()
        .zip(["seed-history", "seed-source", "seed-target"])
    {
        seed.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
            &[Value::Text(key.to_owned()), Value::Text(value.to_owned())],
        )
        .await
        .expect("insert three-row checkpoint fixture");
    }
    seed.commit().await.expect("commit three-row seed");
    lix.execute(
        "UPDATE lix_key_value SET value = 'history-1' WHERE key = 'history'",
        &[],
    )
    .await
    .expect("publish one ordinary history commit");
    let history_head = active_head(lix).await;
    let checkpoint_head = lix
        .create_checkpoint()
        .await
        .expect("publish one compacting checkpoint")
        .commit_id;
    let main_branch_id = lix.active_branch_id().await.expect("load main branch id");
    let branch = lix
        .create_branch(CreateBranchOptions {
            id: Some(SOURCE_BRANCH_ID.to_owned()),
            name: "checkpoint recovered-head source".to_owned(),
            from_commit_id: Some(history_head.clone()),
        })
        .await
        .expect("create source from pre-checkpoint recovered head");
    assert_eq!(branch.commit_id, history_head);
    lix.switch_branch(SwitchBranchOptions {
        branch_id: SOURCE_BRANCH_ID.to_owned(),
    })
    .await
    .expect("switch to recovered-head source");
    lix.execute(
        "UPDATE lix_key_value SET value = 'source-1' WHERE key = 'source'",
        &[],
    )
    .await
    .expect("publish source edit");
    let source_head = active_head(lix).await;
    lix.switch_branch(SwitchBranchOptions {
        branch_id: main_branch_id.clone(),
    })
    .await
    .expect("switch to checkpointed target");
    let (target_key, target_value) = if same_identity {
        ("source", "target-source-1")
    } else {
        ("target", "target-1")
    };
    lix.execute(
        "UPDATE lix_key_value SET value = $1 WHERE key = $2",
        &[
            Value::Text(target_value.to_owned()),
            Value::Text(target_key.to_owned()),
        ],
    )
    .await
    .expect("publish target edit");
    let target_head = active_head(lix).await;
    DivergedHeads {
        initial_head,
        history_head,
        checkpoint_head,
        source_head,
        target_head,
        main_branch_id,
    }
}

async fn active_head<S>(lix: &Lix<S>) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let branch_id = lix.active_branch_id().await.expect("load active branch id");
    lix.execute(
        "SELECT commit_id FROM lix_branch WHERE id = $1",
        &[Value::Text(branch_id)],
    )
    .await
    .expect("load active branch head")
    .rows()[0]
        .get::<String>("commit_id")
        .expect("active branch head is text")
}

async fn assert_graph_edge<S>(lix: &Lix<S>, child: &str, parent: &str, order: i64)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let rows = lix
        .execute(
            "SELECT parent_id FROM lix_commit_edge WHERE child_id = $1 AND parent_order = $2",
            &[Value::Text(child.to_owned()), Value::Integer(order)],
        )
        .await
        .expect("load authenticated commit edge");
    assert_eq!(rows.len(), 1, "expected one parent edge for {child}");
    assert_eq!(rows.rows()[0].get::<String>("parent_id").unwrap(), parent);
}

async fn assert_value<S>(lix: &Lix<S>, key: &str, expected: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let rows = lix
        .execute(
            "SELECT value FROM lix_key_value WHERE key = $1",
            &[Value::Text(key.to_owned())],
        )
        .await
        .expect("load checkpoint oracle value");
    assert_eq!(rows.len(), 1, "expected one value for {key}");
    assert_eq!(
        rows.rows()[0].get::<Value>("value").unwrap(),
        Value::Json(json!(expected))
    );
}

async fn assert_history_present<S>(lix: &Lix<S>, key: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let rows = lix
        .execute(
            "SELECT COUNT(*) AS entries FROM lix_key_value_history() WHERE key = $1",
            &[Value::Text(key.to_owned())],
        )
        .await
        .expect("load checkpoint oracle history");
    assert!(rows.rows()[0].get::<i64>("entries").unwrap() > 0);
}

async fn assert_commit_present<S>(lix: &Lix<S>, commit_id: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let rows = lix
        .execute(
            "SELECT id FROM lix_commit WHERE id = $1",
            &[Value::Text(commit_id.to_owned())],
        )
        .await
        .expect("load retained merge commit");
    assert_eq!(rows.len(), 1, "retained merge commit must survive GC");
}
