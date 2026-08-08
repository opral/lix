//! Public version-control acceptance oracle for the ForkTree Stage2 cut.
//!
//! The frozen source intentionally compiles red on `a12b76c8` until Stage2
//! wires the closed, cfg-only physical-layout selector used by the SQL oracle.

use async_trait::async_trait;
use lix::integration::AcceptancePhysicalLayout;
use lix::storage::Storage;
use lix::storage_adapter::StorageAdapter;
use lix::storage_bench::collect_repository_gc_for_bench;
use lix::{
    CreateBranchOptions, Lix, LixError, MergeBranchOptions, MergeBranchOutcome,
    MergeBranchPreviewOptions, MergeConflictKind, SwitchBranchOptions, Value, open_lix,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;
use serde_json::{Value as JsonValue, json};
use sha2::{Digest as _, Sha256};
use std::fmt::Write as _;
use std::path::Path;

const ROW_COUNT: usize = 1_000;
const SOURCE_BRANCH_ID: &str = "01920000-0000-7000-8000-00000000d201";
const CONFLICT_BRANCH_ID: &str = "01920000-0000-7000-8000-00000000d202";
const RETENTION_BRANCH_ID: &str = "01920000-0000-7000-8000-00000000d203";
const DISPOSABLE_BRANCH_ID: &str = "01920000-0000-7000-8000-00000000d204";
const FINAL_RELEASE_BRANCH_ID: &str = "01920000-0000-7000-8000-00000000d205";
const EXPECTED_SEMANTIC_DIGEST: &str =
    "98f32ba6e147d8c2f8bb88c691aa92cfc5de149e2d23fb439ee79fec4fdeb791";
const EXPECTED_FINAL_DIGEST: &str =
    "fda3c0c062441132e70e594714fb00bb274dab48838afe268e5349e3e68d0839";

#[derive(Debug, Clone, PartialEq, Eq)]
struct OracleArtifact {
    semantic_digest: String,
    final_digest: String,
}

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
        RocksDB::open(path.join(".lix")).expect("open version-control RocksDB")
    }

    async fn flush(storage: &Self::Storage) {
        storage.flush().expect("flush version-control RocksDB");
    }
}

struct SlateBackend;

#[async_trait]
impl AcceptanceBackend for SlateBackend {
    type Storage = SlateDB;

    fn open(path: &Path) -> Self::Storage {
        SlateDB::open(path.join(".lix")).expect("open version-control SlateDB")
    }

    async fn flush(storage: &Self::Storage) {
        storage
            .flush()
            .await
            .expect("flush version-control SlateDB");
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

fn sha256_json(value: &JsonValue) -> String {
    format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(value).expect("serialize oracle evidence"))
    )
}

async fn branch_head<S>(lix: &Lix<S>, branch_id: &str) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "SELECT commit_id FROM lix_branch WHERE id = $1",
        &[Value::Text(branch_id.to_owned())],
    )
    .await
    .expect("load branch head")
    .rows()[0]
        .get::<String>("commit_id")
        .expect("branch head is text")
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
    .expect("query public commit fact")
    .rows()[0]
        .get::<i64>("count")
        .expect("commit count is integer")
}

async fn register_schema_and_seed<S>(lix: &Lix<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let schema = r#"{"x-lix-key":"vc_stage2_row","x-lix-primary-key":["/id"],"type":"object","properties":{"id":{"type":"string"},"value":{"type":"string"}},"required":["id","value"],"additionalProperties":false}"#;
    lix.execute(
        "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
        &[Value::Text(schema.to_owned())],
    )
    .await
    .expect("register version-control schema");

    let mut sql = String::from("INSERT INTO vc_stage2_row (id, value) VALUES ");
    for index in 0..ROW_COUNT {
        if index != 0 {
            sql.push(',');
        }
        write!(sql, "('{index:04}','base-{index:04}')").unwrap();
    }
    lix.execute(&sql, &[])
        .await
        .expect("seed exact 1K version-control rows");
}

async fn update_row<S>(lix: &Lix<S>, id: &str, value: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = lix
        .execute(
            "UPDATE vc_stage2_row SET value = $1 WHERE id = $2 RETURNING id, value",
            &[Value::Text(value.to_owned()), Value::Text(id.to_owned())],
        )
        .await
        .expect("update version-control row");
    assert_eq!(result.rows_affected(), 1);
    assert_eq!(result.rows()[0].get::<String>("value").unwrap(), value);
}

async fn row_value<S>(lix: &Lix<S>, id: &str) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "SELECT value FROM vc_stage2_row WHERE id = $1",
        &[Value::Text(id.to_owned())],
    )
    .await
    .expect("read version-control row")
    .rows()[0]
        .get::<String>("value")
        .expect("row value is text")
}

async fn history_count<S>(lix: &Lix<S>, commit_id: &str) -> i64
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "SELECT COUNT(*) AS count FROM vc_stage2_row_history($1) WHERE lixcol_is_deleted = false",
        &[Value::Text(commit_id.to_owned())],
    )
    .await
    .expect("query exact typed history")
    .rows()[0]
        .get::<i64>("count")
        .expect("history count is integer")
}

async fn qualify_final_release<B: AcceptanceBackend>(layout: AcceptancePhysicalLayout) {
    let directory = tempfile::tempdir().expect("create final-release repository");
    let (lix, storage) = open_with_layout::<B>(directory.path(), layout).await;
    register_schema_and_seed(&lix).await;
    let main_branch_id = lix.active_branch_id().await.unwrap();
    lix.create_branch(CreateBranchOptions {
        id: Some(FINAL_RELEASE_BRANCH_ID.to_owned()),
        name: "Stage2 final-release sibling".to_owned(),
        from_commit_id: Some(branch_head(&lix, &main_branch_id).await),
    })
    .await
    .expect("create final-release sibling");
    lix.switch_branch(SwitchBranchOptions {
        branch_id: FINAL_RELEASE_BRANCH_ID.to_owned(),
    })
    .await
    .expect("switch to final-release sibling");
    update_row(&lix, "0006", "final-release").await;
    let released_commit = branch_head(&lix, FINAL_RELEASE_BRANCH_ID).await;
    lix.switch_branch(SwitchBranchOptions {
        branch_id: main_branch_id,
    })
    .await
    .expect("return from final-release sibling");
    lix.execute(
        "DELETE FROM lix_branch WHERE id = $1 RETURNING id",
        &[Value::Text(FINAL_RELEASE_BRANCH_ID.to_owned())],
    )
    .await
    .expect("release final sibling reference");
    let gc = collect_repository_gc_for_bench(&StorageAdapter::new(storage.clone()))
        .await
        .expect("collect final released sibling");
    assert!(gc.swept_commits > 0);
    assert_eq!(commit_count(&lix, &released_commit).await, 0);
    assert!(
        lix.execute(
            "SELECT id FROM lix_branch WHERE id = $1",
            &[Value::Text(FINAL_RELEASE_BRANCH_ID.to_owned())],
        )
        .await
        .unwrap()
        .is_empty()
    );
    B::flush(&storage).await;
    lix.close().await.expect("close final-release repository");
    drop(lix);
    drop(storage);
}

async fn run_trace<B: AcceptanceBackend>(layout: AcceptancePhysicalLayout) -> OracleArtifact {
    let directory = tempfile::tempdir().expect("create version-control oracle directory");
    let (lix, storage) = open_with_layout::<B>(directory.path(), layout).await;
    register_schema_and_seed(&lix).await;

    let main_branch_id = lix.active_branch_id().await.expect("load main branch id");
    let seed_head = branch_head(&lix, &main_branch_id).await;
    assert_eq!(history_count(&lix, &seed_head).await, ROW_COUNT as i64);
    lix.create_checkpoint()
        .await
        .expect("checkpoint 1K seed state");
    let merge_base = branch_head(&lix, &main_branch_id).await;

    let source = lix
        .create_branch(CreateBranchOptions {
            id: Some(SOURCE_BRANCH_ID.to_owned()),
            name: "Stage2 disjoint source".to_owned(),
            from_commit_id: Some(merge_base.clone()),
        })
        .await
        .expect("create disjoint source branch");
    update_row(&lix, "0001", "target-disjoint").await;
    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .expect("switch to disjoint source");
    update_row(&lix, "0002", "source-disjoint").await;
    lix.switch_branch(SwitchBranchOptions {
        branch_id: main_branch_id.clone(),
    })
    .await
    .expect("return to target branch");

    let disjoint_preview = lix
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: source.id.clone(),
        })
        .await
        .expect("preview disjoint merge");
    assert_eq!(disjoint_preview.outcome, MergeBranchOutcome::MergeCommitted);
    assert!(disjoint_preview.conflicts.is_empty());
    assert_eq!(disjoint_preview.change_stats.total, 1);
    let merge = lix
        .merge_branch(MergeBranchOptions {
            source_branch_id: source.id.clone(),
        })
        .await
        .expect("publish disjoint merge");
    assert_eq!(merge.outcome, MergeBranchOutcome::MergeCommitted);
    assert_eq!(row_value(&lix, "0001").await, "target-disjoint");
    assert_eq!(row_value(&lix, "0002").await, "source-disjoint");
    let merged_head = branch_head(&lix, &main_branch_id).await;

    let conflict = lix
        .create_branch(CreateBranchOptions {
            id: Some(CONFLICT_BRANCH_ID.to_owned()),
            name: "Stage2 conflict source".to_owned(),
            from_commit_id: Some(merged_head.clone()),
        })
        .await
        .expect("create conflict branch");
    update_row(&lix, "0003", "target-conflict").await;
    lix.switch_branch(SwitchBranchOptions {
        branch_id: conflict.id.clone(),
    })
    .await
    .expect("switch to conflict source");
    update_row(&lix, "0003", "source-conflict").await;
    lix.switch_branch(SwitchBranchOptions {
        branch_id: main_branch_id.clone(),
    })
    .await
    .expect("return to conflict target");

    let conflict_preview = lix
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: conflict.id.clone(),
        })
        .await
        .expect("preview true same-identity conflict");
    assert_eq!(conflict_preview.conflicts.len(), 1);
    assert_eq!(
        conflict_preview.conflicts[0].kind,
        MergeConflictKind::SameEntityChanged
    );
    assert_eq!(conflict_preview.conflicts[0].schema_key, "vc_stage2_row");
    let merge_error = lix
        .merge_branch(MergeBranchOptions {
            source_branch_id: conflict.id.clone(),
        })
        .await
        .expect_err("same-identity merge must fail closed");
    assert_eq!(merge_error.code, LixError::CODE_MERGE_CONFLICT);
    assert_eq!(row_value(&lix, "0003").await, "target-conflict");

    let before_undo = row_value(&lix, "0004").await;
    update_row(&lix, "0004", "undo-redo-value").await;
    lix.undo().await.expect("undo tracked update");
    assert_eq!(row_value(&lix, "0004").await, before_undo);
    lix.redo().await.expect("redo tracked update");
    assert_eq!(row_value(&lix, "0004").await, "undo-redo-value");

    let winner = lix
        .open_workspace_session()
        .await
        .expect("open stale winner session");
    let mut stale = lix
        .begin_transaction()
        .await
        .expect("begin stale transaction");
    stale
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('vc-stage2-stale', 'stale')",
            &[],
        )
        .await
        .expect("stage same-owner stale write");
    winner
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('vc-stage2-stale', 'winner')",
            &[],
        )
        .await
        .expect("publish same-owner winner");
    let stale_error = stale
        .commit()
        .await
        .expect_err("same-owner stale commit must reject");
    assert_eq!(stale_error.code, LixError::CODE_UNIQUE);
    winner.close().await.expect("close stale winner session");

    let left_session = lix.open_workspace_session().await.unwrap();
    let right_session = lix.open_workspace_session().await.unwrap();
    let mut left = left_session.begin_transaction().await.unwrap();
    let mut right = right_session.begin_transaction().await.unwrap();
    left.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('vc-stage2-left', 'left')",
        &[],
    )
    .await
    .expect("stage unrelated left owner");
    right
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('vc-stage2-right', 'right')",
            &[],
        )
        .await
        .expect("stage unrelated right owner");
    left.commit().await.expect("commit unrelated left owner");
    right
        .commit()
        .await
        .expect("commit stale unrelated right owner");
    left_session.close().await.unwrap();
    right_session.close().await.unwrap();

    lix.execute(
        "DELETE FROM lix_branch WHERE id IN ($1, $2) RETURNING id",
        &[
            Value::Text(source.id.clone()),
            Value::Text(conflict.id.clone()),
        ],
    )
    .await
    .expect("delete merged and conflicted branches");

    lix.create_checkpoint()
        .await
        .expect("checkpoint merged version-control state");
    let head_before_reopen = branch_head(&lix, &main_branch_id).await;
    B::flush(&storage).await;
    lix.close().await.expect("close before cold recovery");
    drop(lix);
    drop(storage);

    let (reopened, reopened_storage) = open_with_layout::<B>(directory.path(), layout).await;
    assert_eq!(reopened.active_branch_id().await.unwrap(), main_branch_id);
    assert_eq!(
        branch_head(&reopened, &main_branch_id).await,
        head_before_reopen
    );
    let cold_diff = reopened
        .execute(
            "SELECT entity_pk, diff_type FROM lix_diff($1, $2) WHERE schema_key = 'vc_stage2_row' ORDER BY entity_pk",
            &[
                Value::Text(merge_base.clone()),
                Value::Text(merged_head.clone()),
            ],
        )
        .await
        .expect("cold exact diff");
    assert_eq!(cold_diff.len(), 2);
    assert!(
        cold_diff
            .rows()
            .iter()
            .all(|row| row.get::<String>("diff_type").unwrap() == "modified")
    );
    let merged_history_rows = history_count(&reopened, &merged_head).await;
    assert_eq!(merged_history_rows, ROW_COUNT as i64 + 2);

    let gc_branch_base = branch_head(&reopened, &main_branch_id).await;
    reopened
        .create_branch(CreateBranchOptions {
            id: Some(DISPOSABLE_BRANCH_ID.to_owned()),
            name: "Stage2 disposable GC sibling".to_owned(),
            from_commit_id: Some(gc_branch_base),
        })
        .await
        .expect("create disposable sibling branch");
    reopened
        .switch_branch(SwitchBranchOptions {
            branch_id: DISPOSABLE_BRANCH_ID.to_owned(),
        })
        .await
        .expect("switch to disposable sibling");
    update_row(&reopened, "0005", "gc-disposable").await;
    let disposable_commit = branch_head(&reopened, DISPOSABLE_BRANCH_ID).await;
    reopened
        .switch_branch(SwitchBranchOptions {
            branch_id: main_branch_id.clone(),
        })
        .await
        .expect("return from disposable sibling");
    reopened
        .execute(
            "DELETE FROM lix_branch WHERE id = $1 RETURNING id",
            &[Value::Text(DISPOSABLE_BRANCH_ID.to_owned())],
        )
        .await
        .expect("release disposable sibling branch");
    reopened
        .create_branch(CreateBranchOptions {
            id: Some(RETENTION_BRANCH_ID.to_owned()),
            name: "Stage2 GC retention root".to_owned(),
            from_commit_id: Some(branch_head(&reopened, &main_branch_id).await),
        })
        .await
        .expect("create explicit history retention branch");
    reopened
        .switch_branch(SwitchBranchOptions {
            branch_id: RETENTION_BRANCH_ID.to_owned(),
        })
        .await
        .expect("switch to retained sibling");
    update_row(&reopened, "0005", "gc-retained").await;
    let retained_commit = branch_head(&reopened, RETENTION_BRANCH_ID).await;
    let retained_history_rows = history_count(&reopened, &retained_commit).await;
    reopened
        .switch_branch(SwitchBranchOptions {
            branch_id: main_branch_id.clone(),
        })
        .await
        .expect("return from retained sibling");

    let retained_gc =
        collect_repository_gc_for_bench(&StorageAdapter::new(reopened_storage.clone()))
            .await
            .expect("complete retained-root production GC pass");
    assert!(retained_gc.swept_commits > 0);
    assert_eq!(commit_count(&reopened, &disposable_commit).await, 0);
    assert_eq!(commit_count(&reopened, &retained_commit).await, 1);
    let retained = reopened
        .open_session(RETENTION_BRANCH_ID)
        .await
        .expect("open retained historical branch after GC");
    assert_eq!(row_value(&retained, "0005").await, "gc-retained");
    assert_eq!(
        history_count(&retained, &retained_commit).await,
        retained_history_rows
    );
    retained.close().await.unwrap();

    reopened
        .execute(
            "DELETE FROM lix_branch WHERE id = $1 RETURNING id",
            &[Value::Text(RETENTION_BRANCH_ID.to_owned())],
        )
        .await
        .expect("release final retained branch root");
    assert!(
        reopened
            .execute(
                "SELECT id FROM lix_branch WHERE id IN ($1, $2, $3, $4)",
                &[
                    Value::Text(SOURCE_BRANCH_ID.to_owned()),
                    Value::Text(CONFLICT_BRANCH_ID.to_owned()),
                    Value::Text(RETENTION_BRANCH_ID.to_owned()),
                    Value::Text(DISPOSABLE_BRANCH_ID.to_owned()),
                ],
            )
            .await
            .unwrap()
            .is_empty()
    );
    qualify_final_release::<B>(layout).await;

    let semantic_evidence = json!({
        "rows": ROW_COUNT,
        "seedHistoryRows": ROW_COUNT,
        "disjointPreview": {
            "outcome": "mergeCommitted",
            "conflicts": disjoint_preview.conflicts.len(),
            "total": disjoint_preview.change_stats.total,
            "modified": disjoint_preview.change_stats.modified,
        },
        "merge": {
            "outcome": "mergeCommitted",
            "total": merge.change_stats.total,
            "modified": merge.change_stats.modified,
        },
        "conflict": {
            "count": conflict_preview.conflicts.len(),
            "kind": "sameEntityChanged",
            "schema": conflict_preview.conflicts[0].schema_key,
            "error": merge_error.code,
        },
        "coldDiff": cold_diff.rows().iter().map(|row| row.values().to_vec()).collect::<Vec<_>>(),
        "coldHistoryRows": merged_history_rows,
        "undoRedo": [before_undo, "undo-redo-value".to_owned()],
        "staleError": stale_error.code,
        "unrelatedOwners": ["left", "right"],
        "gc": {
            "disposableReleased": true,
            "retainedThroughFirstSweep": true,
            "retainedHistoryRows": retained_history_rows,
            "finalReferenceReleased": true,
        },
    });
    let semantic_digest = sha256_json(&semantic_evidence);

    let final_rows = reopened
        .execute(
            "SELECT id, value FROM vc_stage2_row WHERE id IN ('0001','0002','0003','0004','0005','0999') ORDER BY id",
            &[],
        )
        .await
        .expect("read final logical rows");
    let final_kv = reopened
        .execute(
            "SELECT key, value FROM lix_key_value WHERE key LIKE 'vc-stage2-%' ORDER BY key",
            &[],
        )
        .await
        .expect("read final stale/disjoint owner rows");
    let final_digest = sha256_json(&json!({
        "rows": final_rows.rows().iter().map(|row| row.values().to_vec()).collect::<Vec<_>>(),
        "keyValue": final_kv.rows().iter().map(|row| row.values().to_vec()).collect::<Vec<_>>(),
    }));

    B::flush(&reopened_storage).await;
    reopened
        .close()
        .await
        .expect("close final recovered repository");
    drop(reopened);
    drop(reopened_storage);

    OracleArtifact {
        semantic_digest,
        final_digest,
    }
}

async fn qualify_backend<B: AcceptanceBackend>() {
    let current = run_trace::<B>(AcceptancePhysicalLayout::Current).await;
    let forktree = run_trace::<B>(AcceptancePhysicalLayout::ForkTree).await;
    assert_eq!(
        forktree, current,
        "physical layout changed version-control semantics"
    );
    assert_eq!(current.semantic_digest, EXPECTED_SEMANTIC_DIGEST);
    assert_eq!(current.final_digest, EXPECTED_FINAL_DIGEST);
}

#[tokio::test]
async fn forktree_stage2_version_control_rocksdb() {
    qualify_backend::<RocksBackend>().await;
}

#[tokio::test]
async fn forktree_stage2_version_control_slatedb() {
    qualify_backend::<SlateBackend>().await;
}
