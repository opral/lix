//! Public checkpoint/merge acceptance oracle for the ForkTree Stage2 cut.
//!
//! This source intentionally compiles red before Stage2 installs the closed
//! acceptance-only physical-layout selector. It uses no current-layout space,
//! queue, codec, object ID, storage adapter, or maintenance implementation.

use std::future::Future;
use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use lix::integration::AcceptancePhysicalLayout;
use lix::storage::Storage;
use lix::{
    CreateBranchOptions, Lix, LixError, MergeBranchOptions, MergeBranchOutcome,
    MergeBranchPreviewOptions, MergeConflictKind, SwitchBranchOptions, Value, open_lix,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;
use serde_json::json;

const SOURCE_BRANCH_ID: &str = "01920000-0000-7000-8000-00000000c201";
const CONFLICT_BRANCH_ID: &str = "01920000-0000-7000-8000-00000000c202";
const RELEASE_BRANCH_ID: &str = "01920000-0000-7000-8000-00000000c203";
const MISSING_PARENT_BRANCH_ID: &str = "01920000-0000-7000-8000-00000000c204";
const MISSING_COMMIT_ID: &str = "01920000-0000-7000-8000-00000000dead";
const GC_ROTATIONS: usize = 64;

#[async_trait]
trait AcceptanceBackend {
    type Storage: Storage + Clone + Send + Sync + 'static;

    fn open(path: &Path) -> Self::Storage;
    async fn flush(storage: &Self::Storage);
}

struct RocksBackend;

#[async_trait]
impl AcceptanceBackend for RocksBackend {
    type Storage = RocksDB;

    fn open(path: &Path) -> Self::Storage {
        RocksDB::open(path.join(".lix")).expect("open Stage2 checkpoint RocksDB")
    }

    async fn flush(storage: &Self::Storage) {
        storage.flush().expect("flush Stage2 checkpoint RocksDB");
    }
}

struct SlateBackend;

#[async_trait]
impl AcceptanceBackend for SlateBackend {
    type Storage = SlateDB;

    fn open(path: &Path) -> Self::Storage {
        SlateDB::open(path.join(".lix")).expect("open Stage2 checkpoint SlateDB")
    }

    async fn flush(storage: &Self::Storage) {
        storage
            .flush()
            .await
            .expect("flush Stage2 checkpoint SlateDB");
    }
}

async fn open_with_layout<B: AcceptanceBackend>(
    path: &Path,
    layout: AcceptancePhysicalLayout,
) -> (Lix<B::Storage>, B::Storage) {
    let storage = B::open(path);
    let lix = open_lix()
        .with_storage(storage.clone())
        .with_acceptance_physical_layout(layout)
        .await
        .expect("open repository with selected physical owner");
    (lix, storage)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OracleArtifact {
    final_values: Vec<String>,
    disjoint_outcome: String,
    true_conflict_kind: String,
    missing_parent_code: String,
    recovered_history_rows: i64,
    final_release_reclaimed: bool,
}

#[derive(Debug)]
struct CheckpointHeads {
    initial: String,
    seed: String,
    recovered: String,
    checkpoint: String,
    main_branch: String,
}

async fn seed_three_rows_and_rotate<S>(lix: &Lix<S>) -> CheckpointHeads
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let initial = active_head(lix).await;
    let mut seed = lix
        .begin_transaction()
        .await
        .expect("begin exact three-row seed");
    for (key, value) in [
        ("history", "seed-history"),
        ("source", "seed-source"),
        ("target", "seed-target"),
    ] {
        seed.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
            &[Value::Text(key.to_owned()), Value::Text(value.to_owned())],
        )
        .await
        .expect("insert exact checkpoint row");
    }
    seed.commit().await.expect("commit exact three-row seed");
    let seed = active_head(lix).await;
    lix.execute(
        "UPDATE lix_key_value SET value = 'history-1' WHERE key = 'history'",
        &[],
    )
    .await
    .expect("publish exactly one history commit");
    let recovered = active_head(lix).await;
    let checkpoint = lix
        .create_checkpoint()
        .await
        .expect("publish compacting checkpoint")
        .commit_id;
    for _ in 1..GC_ROTATIONS {
        lix.create_checkpoint()
            .await
            .expect("rotate checkpoint recovery/GC state");
    }
    let main_branch = lix.active_branch_id().await.expect("load main branch");
    CheckpointHeads {
        initial,
        seed,
        recovered,
        checkpoint,
        main_branch,
    }
}

async fn run_trace<B: AcceptanceBackend>(layout: AcceptancePhysicalLayout) -> OracleArtifact {
    let directory = tempfile::tempdir().expect("create Stage2 checkpoint repository");
    let (lix, storage) = open_with_layout::<B>(directory.path(), layout).await;
    let heads = seed_three_rows_and_rotate(&lix).await;

    // Checkpoint compaction must remain bounded: C cannot permanently parent H.
    assert_graph_edge(&lix, &heads.checkpoint, &heads.initial, 0).await;
    assert_no_graph_edge(&lix, &heads.checkpoint, &heads.recovered).await;

    let source = lix
        .create_branch(CreateBranchOptions {
            id: Some(SOURCE_BRANCH_ID.to_owned()),
            name: "Stage2 recovered-head source".to_owned(),
            from_commit_id: Some(heads.recovered.clone()),
        })
        .await
        .expect("create source from pre-checkpoint recovered head after 64 rotations");
    assert_eq!(source.commit_id, heads.recovered);
    lix.switch_branch(SwitchBranchOptions {
        branch_id: SOURCE_BRANCH_ID.to_owned(),
    })
    .await
    .expect("switch to recovered-head source");
    update_value(&lix, "source", "source-1").await;
    let source_head = active_head(&lix).await;
    assert_graph_edge(&lix, &source_head, &heads.recovered, 0).await;

    lix.switch_branch(SwitchBranchOptions {
        branch_id: heads.main_branch.clone(),
    })
    .await
    .expect("switch to checkpointed target");
    update_value(&lix, "target", "target-1").await;
    let target_head = active_head(&lix).await;

    let preview = lix
        .preview_merge_branch(MergeBranchPreviewOptions {
            source_branch_id: SOURCE_BRANCH_ID.to_owned(),
        })
        .await
        .expect("preview disjoint recovered-head merge");
    assert_eq!(preview.base_commit_id, heads.recovered);
    assert!(preview.conflicts.is_empty());
    let receipt = lix
        .merge_branch(MergeBranchOptions {
            source_branch_id: SOURCE_BRANCH_ID.to_owned(),
        })
        .await
        .expect("merge disjoint recovered-head edits without added/added conflict");
    assert_eq!(receipt.base_commit_id, heads.recovered);
    assert_eq!(receipt.target_head_before_commit_id, target_head);
    assert_eq!(receipt.source_head_before_commit_id, source_head);
    assert_eq!(receipt.outcome, MergeBranchOutcome::MergeCommitted);
    assert_eq!(
        (receipt.change_stats.added, receipt.change_stats.modified),
        (0, 1)
    );
    let merged_head = active_head(&lix).await;
    assert_graph_edge(&lix, &merged_head, &target_head, 0).await;
    assert_graph_edge(&lix, &merged_head, &source_head, 1).await;

    lix.undo().await.expect("undo recovered-head merge");
    assert_value(&lix, "source", "seed-source").await;
    assert_value(&lix, "target", "target-1").await;
    lix.redo().await.expect("redo recovered-head merge");
    assert_value(&lix, "source", "source-1").await;
    assert_value(&lix, "target", "target-1").await;

    let conflict = lix
        .create_branch(CreateBranchOptions {
            id: Some(CONFLICT_BRANCH_ID.to_owned()),
            name: "Stage2 true-conflict source".to_owned(),
            from_commit_id: Some(active_head(&lix).await),
        })
        .await
        .expect("create true-conflict source");
    lix.switch_branch(SwitchBranchOptions {
        branch_id: conflict.id,
    })
    .await
    .expect("switch to true-conflict source");
    update_value(&lix, "source", "conflict-source").await;
    lix.switch_branch(SwitchBranchOptions {
        branch_id: heads.main_branch.clone(),
    })
    .await
    .expect("switch to true-conflict target");
    update_value(&lix, "source", "conflict-target").await;
    let conflict_preview = lix
        .preview_merge_branch(MergeBranchPreviewOptions {
            source_branch_id: CONFLICT_BRANCH_ID.to_owned(),
        })
        .await
        .expect("preview true same-identity conflict");
    assert_eq!(conflict_preview.conflicts.len(), 1);
    assert_eq!(
        conflict_preview.conflicts[0].kind,
        MergeConflictKind::SameEntityChanged
    );
    let conflict_error = lix
        .merge_branch(MergeBranchOptions {
            source_branch_id: CONFLICT_BRANCH_ID.to_owned(),
        })
        .await
        .expect_err("true same-identity edit must conflict");
    assert_eq!(conflict_error.code, LixError::CODE_MERGE_CONFLICT);

    let missing_error = lix
        .create_branch(CreateBranchOptions {
            id: Some(MISSING_PARENT_BRANCH_ID.to_owned()),
            name: "Stage2 missing-parent rejection".to_owned(),
            from_commit_id: Some(MISSING_COMMIT_ID.to_owned()),
        })
        .await
        .expect_err("missing authenticated parent must fail closed");
    assert!(
        missing_error.to_string().contains(MISSING_COMMIT_ID)
            || missing_error.to_string().contains("missing")
    );
    assert_branch_absent(&lix, MISSING_PARENT_BRANCH_ID).await;

    let recovered_history_rows = history_count(&lix, &heads.recovered).await;
    assert_eq!(recovered_history_rows, 3);
    B::flush(&storage).await;
    lix.close()
        .await
        .expect("close Stage2 checkpoint repository");
    drop(lix);
    drop(storage);

    let (reopened, reopened_storage) = open_with_layout::<B>(directory.path(), layout).await;
    assert_value(&reopened, "history", "history-1").await;
    assert_value(&reopened, "source", "conflict-target").await;
    assert_value(&reopened, "target", "target-1").await;
    assert_eq!(history_count(&reopened, &heads.recovered).await, 3);
    let final_values = values(&reopened).await;
    B::flush(&reopened_storage).await;
    reopened
        .close()
        .await
        .expect("close cold-reopened checkpoint repository");
    drop(reopened);
    drop(reopened_storage);

    qualify_final_release::<B>(layout).await;
    OracleArtifact {
        final_values,
        disjoint_outcome: "mergeCommitted".to_owned(),
        true_conflict_kind: "sameEntityChanged".to_owned(),
        missing_parent_code: missing_error.code,
        recovered_history_rows,
        final_release_reclaimed: true,
    }
}

async fn qualify_final_release<B: AcceptanceBackend>(layout: AcceptancePhysicalLayout) {
    let directory = tempfile::tempdir().expect("create Stage2 final-release repository");
    let (lix, storage) = open_with_layout::<B>(directory.path(), layout).await;
    let heads = seed_three_rows_and_rotate(&lix).await;
    // T is deliberately later than semantically-equivalent C. Publishing a
    // branch from H must never bind its bridge to T.
    update_value(&lix, "target", "later-target").await;
    let later_target = active_head(&lix).await;
    let branch = lix
        .create_branch(CreateBranchOptions {
            id: Some(RELEASE_BRANCH_ID.to_owned()),
            name: "Stage2 final recovered-head release".to_owned(),
            from_commit_id: Some(heads.recovered.clone()),
        })
        .await
        .expect("publish retained recovered-head branch");
    assert_eq!(branch.commit_id, heads.recovered);
    assert_ne!(branch.commit_id, heads.checkpoint);
    assert_ne!(branch.commit_id, later_target);
    lix.execute(
        "DELETE FROM lix_branch WHERE id = $1 RETURNING id",
        &[Value::Text(RELEASE_BRANCH_ID.to_owned())],
    )
    .await
    .expect("release historical branch root");
    for _ in 0..GC_ROTATIONS {
        lix.create_checkpoint()
            .await
            .expect("advance final-reference checkpoint/GC rotation");
    }
    wait_for_commit_count(&lix, &heads.recovered, 0).await;
    wait_for_commit_count(&lix, &heads.seed, 0).await;
    assert_eq!(commit_count(&lix, &heads.checkpoint).await, 1);
    assert_value(&lix, "history", "history-1").await;
    assert_value(&lix, "target", "later-target").await;
    assert_branch_absent(&lix, RELEASE_BRANCH_ID).await;
    B::flush(&storage).await;
    lix.close().await.expect("close final-release repository");
    drop(lix);
    drop(storage);

    let (reopened, reopened_storage) = open_with_layout::<B>(directory.path(), layout).await;
    assert_eq!(commit_count(&reopened, &heads.recovered).await, 0);
    assert_eq!(commit_count(&reopened, &heads.checkpoint).await, 1);
    assert_value(&reopened, "target", "later-target").await;
    B::flush(&reopened_storage).await;
    reopened
        .close()
        .await
        .expect("close reopened final-release repository");
}

async fn update_value<S>(lix: &Lix<S>, key: &str, value: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = lix
        .execute(
            "UPDATE lix_key_value SET value = $1 WHERE key = $2 RETURNING key",
            &[Value::Text(value.to_owned()), Value::Text(key.to_owned())],
        )
        .await
        .expect("update checkpoint oracle value");
    assert_eq!(result.rows_affected(), 1);
}

async fn active_head<S>(lix: &Lix<S>) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let branch = lix.active_branch_id().await.expect("load active branch");
    lix.execute(
        "SELECT commit_id FROM lix_branch WHERE id = $1",
        &[Value::Text(branch)],
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
    assert_eq!(rows.len(), 1);
    assert_eq!(rows.rows()[0].get::<String>("parent_id").unwrap(), parent);
}

async fn assert_no_graph_edge<S>(lix: &Lix<S>, child: &str, forbidden_parent: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let rows = lix
        .execute(
            "SELECT parent_id FROM lix_commit_edge WHERE child_id = $1 AND parent_id = $2",
            &[
                Value::Text(child.to_owned()),
                Value::Text(forbidden_parent.to_owned()),
            ],
        )
        .await
        .expect("reject permanent checkpoint-to-recovered-head edge");
    assert!(rows.is_empty());
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
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows.rows()[0].get::<Value>("value").unwrap(),
        Value::Json(json!(expected))
    );
}

async fn values<S>(lix: &Lix<S>) -> Vec<String>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute("SELECT value FROM lix_key_value ORDER BY key", &[])
        .await
        .expect("load final checkpoint values")
        .rows()
        .iter()
        .map(|row| row.get::<serde_json::Value>("value").unwrap().to_string())
        .collect()
}

async fn history_count<S>(lix: &Lix<S>, commit_id: &str) -> i64
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "SELECT COUNT(*) AS count FROM lix_key_value_history($1) WHERE lixcol_is_deleted = false",
        &[Value::Text(commit_id.to_owned())],
    )
    .await
    .expect("load exact checkpoint history")
    .rows()[0]
        .get::<i64>("count")
        .expect("history count is integer")
}

async fn commit_count<S>(lix: &Lix<S>, commit_id: &str) -> i64
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "SELECT COUNT(*) AS count FROM lix_commit WHERE id = $1",
        &[Value::Text(commit_id.to_owned())],
    )
    .await
    .expect("load commit retention count")
    .rows()[0]
        .get::<i64>("count")
        .expect("commit count is integer")
}

async fn wait_for_commit_count<S>(lix: &Lix<S>, commit_id: &str, expected: i64)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    for _ in 0..500 {
        if commit_count(lix, commit_id).await == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(commit_count(lix, commit_id).await, expected);
}

async fn assert_branch_absent<S>(lix: &Lix<S>, branch_id: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let rows = lix
        .execute(
            "SELECT id FROM lix_branch WHERE id = $1",
            &[Value::Text(branch_id.to_owned())],
        )
        .await
        .expect("load rejected/released branch");
    assert!(rows.is_empty());
}

async fn qualify_backend<B: AcceptanceBackend>() {
    let current = run_trace::<B>(AcceptancePhysicalLayout::Current).await;
    let forktree = run_trace::<B>(AcceptancePhysicalLayout::ForkTree).await;
    assert_eq!(
        forktree, current,
        "physical owner changed checkpoint semantics"
    );
}

#[test]
fn forktree_stage2_checkpoint_rotation_rocksdb() {
    run_on_large_stack(|| qualify_backend::<RocksBackend>());
}

#[test]
fn forktree_stage2_checkpoint_rotation_slatedb() {
    run_on_large_stack(|| qualify_backend::<SlateBackend>());
}

fn run_on_large_stack<F, Fut>(make_future: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + 'static,
{
    std::thread::Builder::new()
        .name("forktree-stage2-checkpoint".to_owned())
        .stack_size(16 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build Stage2 checkpoint runtime")
                .block_on(make_future());
        })
        .expect("spawn Stage2 checkpoint thread")
        .join()
        .expect("Stage2 checkpoint thread should not panic");
}
