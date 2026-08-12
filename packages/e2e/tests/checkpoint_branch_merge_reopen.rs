use std::path::Path;

use async_trait::async_trait;
use lix::storage::Storage;
use lix::{
    CreateBranchOptions, Lix, MergeBranchOptions, MergeBranchOutcome, MergeBranchPreviewOptions,
    MergeChangeStats, Value, open_lix,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;

const SOURCE_BRANCH_ID: &str = "01990000-0000-7000-8000-0000000000c1";
const SOURCE_KEY: &str = "checkpoint-fork-source";
const TARGET_KEY: &str = "checkpoint-fork-target";

#[async_trait]
trait ReopenStorage: Storage + Clone + Send + Sync + Sized + 'static {
    fn open(path: &Path) -> Self;
    async fn flush(&self);
}

#[async_trait]
impl ReopenStorage for RocksDB {
    fn open(path: &Path) -> Self {
        Self::open(path).expect("open RocksDB checkpoint-merge fixture")
    }

    async fn flush(&self) {
        self.flush()
            .expect("flush RocksDB checkpoint-merge fixture");
    }
}

#[async_trait]
impl ReopenStorage for SlateDB {
    fn open(path: &Path) -> Self {
        Self::open(path).expect("open SlateDB checkpoint-merge fixture")
    }

    async fn flush(&self) {
        self.flush()
            .await
            .expect("flush SlateDB checkpoint-merge fixture");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rocksdb_checkpoint_preserves_branch_merge_base_after_reopen() {
    checkpoint_preserves_branch_merge_base_after_reopen::<RocksDB>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slatedb_checkpoint_preserves_branch_merge_base_after_reopen() {
    checkpoint_preserves_branch_merge_base_after_reopen::<SlateDB>().await;
}

async fn checkpoint_preserves_branch_merge_base_after_reopen<S: ReopenStorage>() {
    let temp = tempfile::tempdir().expect("create checkpoint-merge fixture");
    let path = temp.path().join("database");
    let initial_commit_id;
    let fork_commit_id;
    let checkpoint_commit_id;
    let source_commit_id;

    {
        let storage = S::open(&path);
        let main = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("initialize durable checkpoint-merge Lix");
        initial_commit_id = active_commit_id(&main).await;
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES \
             ($1, 'shared-source'), ($2, 'shared-target')",
            &[
                Value::Text(SOURCE_KEY.to_owned()),
                Value::Text(TARGET_KEY.to_owned()),
            ],
        )
        .await
        .expect("insert shared rows");
        fork_commit_id = active_commit_id(&main).await;
        checkpoint_commit_id = main
            .create_checkpoint()
            .await
            .expect("checkpoint target branch")
            .commit_id;
        assert_eq!(
            commit_parent_edges(&main, &checkpoint_commit_id).await,
            vec![(initial_commit_id.clone(), 0)],
        );

        let branch = main
            .create_branch(CreateBranchOptions {
                id: Some(SOURCE_BRANCH_ID.to_owned()),
                name: "Checkpoint merge source".to_owned(),
                from_commit_id: Some(fork_commit_id.clone()),
            })
            .await
            .expect("create source branch at pre-checkpoint head");
        assert_eq!(branch.commit_id, fork_commit_id);

        let source = main
            .open_session_at(SOURCE_BRANCH_ID)
            .await
            .expect("open source branch");
        source
            .execute(
                "UPDATE lix_key_value SET value = 'source' WHERE key = $1",
                &[Value::Text(SOURCE_KEY.to_owned())],
            )
            .await
            .expect("publish disjoint source edit");
        source_commit_id = active_commit_id(&source).await;
        assert_eq!(
            commit_parent_edges(&main, &source_commit_id).await,
            vec![
                (fork_commit_id.clone(), 0),
                (checkpoint_commit_id.clone(), 1),
            ],
        );
        main.execute(
            "UPDATE lix_key_value SET value = 'target' WHERE key = $1",
            &[Value::Text(TARGET_KEY.to_owned())],
        )
        .await
        .expect("publish disjoint target edit");

        source.close().await.expect("close source session");
        main.close().await.expect("close main session");
        drop(source);
        drop(main);
        storage.flush().await;
    }

    let storage = S::open(&path);
    let main = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("cold reopen durable checkpoint-merge Lix");
    assert_eq!(
        commit_parent_edges(&main, &checkpoint_commit_id).await,
        vec![(initial_commit_id, 0)],
    );
    assert_eq!(
        commit_parent_edges(&main, &source_commit_id).await,
        vec![
            (fork_commit_id.clone(), 0),
            (checkpoint_commit_id.clone(), 1),
        ],
    );
    let preview = main
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: SOURCE_BRANCH_ID.to_owned(),
        })
        .await
        .expect("disjoint merge preview after cold reopen");
    assert_eq!(preview.base_commit_id, checkpoint_commit_id);
    assert_eq!(preview.outcome, MergeBranchOutcome::MergeCommitted);
    assert!(preview.conflicts.is_empty());
    assert_eq!(
        preview.change_stats,
        MergeChangeStats {
            total: 1,
            added: 0,
            modified: 1,
            removed: 0,
        }
    );

    let receipt = main
        .merge_branch(MergeBranchOptions {
            source_branch_id: SOURCE_BRANCH_ID.to_owned(),
        })
        .await
        .expect("merge disjoint source after cold reopen");
    assert_eq!(receipt.base_commit_id, preview.base_commit_id);
    assert_value(&main, SOURCE_KEY, "source").await;
    assert_value(&main, TARGET_KEY, "target").await;
    main.close().await.expect("close reopened main session");
    drop(main);
    storage.flush().await;
}

async fn active_commit_id<S>(lix: &Lix<S>) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let branch_id = lix
        .active_branch_id()
        .await
        .expect("active branch resolves");
    let result = lix
        .execute(
            "SELECT commit_id FROM lix_branch WHERE id = $1",
            &[Value::Text(branch_id)],
        )
        .await
        .expect("active commit reads");
    result.rows()[0]
        .get::<String>("commit_id")
        .expect("active commit id decodes")
}

async fn commit_parent_edges<S>(lix: &Lix<S>, commit_id: &str) -> Vec<(String, i64)>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "SELECT parent_id, parent_order FROM lix_commit_edge \
         WHERE child_id = $1 ORDER BY parent_order",
        &[Value::Text(commit_id.to_owned())],
    )
    .await
    .expect("checkpoint parent edges read")
    .rows()
    .iter()
    .map(|row| {
        (
            row.get::<String>("parent_id").expect("parent id decodes"),
            row.get::<i64>("parent_order")
                .expect("parent order decodes"),
        )
    })
    .collect()
}

async fn assert_value<S>(lix: &Lix<S>, key: &str, expected: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = lix
        .execute(
            "SELECT value FROM lix_key_value WHERE key = $1",
            &[Value::Text(key.to_owned())],
        )
        .await
        .expect("merged value reads");
    assert_eq!(
        result.rows()[0]
            .get::<serde_json::Value>("value")
            .expect("merged value decodes"),
        serde_json::Value::String(expected.to_owned()),
    );
}
