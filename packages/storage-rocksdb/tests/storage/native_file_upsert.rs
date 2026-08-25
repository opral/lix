//! RocksDB coverage for public SQL file writes and atomic batches.

use lix::storage::Storage;
use lix::{ExecuteBatchStatement, GLOBAL_BRANCH_ID, Lix, Value, open_lix};
use lix_storage_rocksdb::RocksDB;

#[tokio::test]
async fn public_file_upsert_works_with_rocksdb() {
    let temp_dir = tempfile::tempdir().expect("create RocksDB temp directory");
    let storage = RocksDB::open(temp_dir.path().join("6e617469-7665-8d66-896c-652d75707300"))
        .expect("open RocksDB storage");
    let lix = open_lix().with_storage(storage).await.expect("open Lix");

    upsert_file(&lix, "/native/deep/payload.bin", b"first").await;
    assert_file_content(&lix, "/native/deep/payload.bin", b"first").await;
    upsert_file(&lix, "/native/deep/payload.bin", b"second").await;
    assert_file_content(&lix, "/native/deep/payload.bin", b"second").await;
    upsert_file(&lix, "/native/deep/payload.bin", b"").await;
    assert_file_content(&lix, "/native/deep/payload.bin", b"").await;

    let batch_parent = active_branch_commit_id(&lix).await;
    execute_file_batch(
        &lix,
        &[
            ("/native/deep/payload.bin", b"batch-update"),
            ("/native/batch/one.bin", b"one"),
            ("/native/batch/two.bin", b"two"),
            ("/native/batch/empty.bin", b""),
        ],
    )
    .await
    .expect("execute public SQL file batch");
    assert_active_branch_head_parent(&lix, &batch_parent).await;
    assert_file_content(&lix, "/native/deep/payload.bin", b"batch-update").await;
    assert_file_content(&lix, "/native/batch/one.bin", b"one").await;
    assert_file_content(&lix, "/native/batch/two.bin", b"two").await;
    assert_file_content(&lix, "/native/batch/empty.bin", b"").await;

    let head_before_error = active_branch_commit_id(&lix).await;
    let error = execute_file_batch(
        &lix,
        &[
            ("/native/batch/must-not-write.bin", b"first"),
            ("relative.bin", b"invalid"),
        ],
    )
    .await
    .expect_err("invalid path rejects the complete SQL batch");
    assert_eq!(error.code, "LIX_INVALID_PARAM");
    assert_file_missing(&lix, "/native/batch/must-not-write.bin").await;
    assert_eq!(active_branch_commit_id(&lix).await, head_before_error);

    // SQL upserts target the active overlay while preserving the global row.
    let global = lix
        .open_another_session()
        .with_branch(GLOBAL_BRANCH_ID)
        .await
        .expect("open global session");
    global
        .execute(
            "INSERT INTO lix_file (id, path, content, lixcol_global) \
         VALUES ($1, $2, $3, true)",
            &[
                Value::Text("abc1de5c-4b72-748d-84df-8fc7b1beedda".to_owned()),
                Value::Text("/native/overlap.bin".to_owned()),
                Value::Blob(b"g".to_vec().into()),
            ],
        )
        .await
        .expect("insert global overlap fixture");
    lix.execute(
        "INSERT INTO lix_file (id, path, content) VALUES ($1, $2, $3)",
        &[
            Value::Text("abc1de5c-4b72-748d-84df-8fc7b1beedda".to_owned()),
            Value::Text("/native/overlap.bin".to_owned()),
            Value::Blob(b"l".to_vec().into()),
        ],
    )
    .await
    .expect("insert active overlap fixture");
    execute_file_batch(
        &lix,
        &[
            ("/native/overlap.bin", b"updated"),
            ("/native/batch/overlay-companion.bin", b"companion"),
        ],
    )
    .await
    .expect("update active overlay through SQL batch");
    assert_file_content_by_session(&lix, "abc1de5c-4b72-748d-84df-8fc7b1beedda", b"updated").await;
    assert_file_content_by_session(&global, "abc1de5c-4b72-748d-84df-8fc7b1beedda", b"g").await;
}

const UPSERT_SQL: &str = "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
    ON CONFLICT (path) DO UPDATE SET content = excluded.content";

async fn upsert_file<S>(lix: &Lix<S>, path: &str, content: &[u8])
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        UPSERT_SQL,
        &[
            Value::Text(path.to_owned()),
            Value::Blob(content.to_vec().into()),
        ],
    )
    .await
    .expect("upsert file through SQL");
}

async fn execute_file_batch<S>(
    lix: &Lix<S>,
    files: &[(&str, &[u8])],
) -> Result<Vec<lix::ExecuteResult>, lix::LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let statements = files
        .iter()
        .map(|(path, content)| ExecuteBatchStatement {
            sql: UPSERT_SQL.to_owned(),
            params: vec![
                Value::Text((*path).to_owned()),
                Value::Blob((*content).to_vec().into()),
            ],
            label: None,
        })
        .collect::<Vec<_>>();
    lix.execute_batch(&statements).await
}

async fn active_branch_commit_id<S>(lix: &Lix<S>) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await
        .expect("read active branch commit id")
        .rows()[0]
        .get::<String>("commit_id")
        .expect("commit id decodes")
}

async fn assert_active_branch_head_parent<S>(lix: &Lix<S>, expected_parent: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let head = active_branch_commit_id(lix).await;
    let result = lix
        .execute(
            "SELECT parent_commit_ids ->> 0 AS parent_id FROM lix_commit WHERE id = $1",
            &[Value::Text(head)],
        )
        .await
        .expect("read first commit parent");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0]
            .get::<String>("parent_id")
            .expect("parent id decodes"),
        expected_parent
    );
}

async fn assert_file_missing<S>(lix: &Lix<S>, path: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = lix
        .execute(
            "SELECT content FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_owned())],
        )
        .await
        .expect("read file absence");
    assert!(result.rows().is_empty());
}

async fn assert_file_content<S>(lix: &Lix<S>, path: &str, expected: &[u8])
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = lix
        .execute(
            "SELECT content FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_owned())],
        )
        .await
        .expect("read file");
    assert_eq!(
        result.rows()[0]
            .get::<Vec<u8>>("content")
            .expect("file content decodes"),
        expected
    );
}

async fn assert_file_content_by_session<S>(lix: &Lix<S>, id: &str, expected: &[u8])
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = lix
        .execute(
            "SELECT content FROM lix_file WHERE id = $1",
            &[Value::Text(id.to_owned())],
        )
        .await
        .expect("read branch file");
    assert_eq!(
        result.rows()[0]
            .get::<Vec<u8>>("content")
            .expect("branch file content decodes"),
        expected
    );
}
