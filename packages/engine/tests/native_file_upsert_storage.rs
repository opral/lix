//! Storage-backend coverage for the structured native file-write surface.

use lix_engine::{Blob, Engine, LixError, SessionContext, Storage, Value};
use lix_rocksdb_storage::RocksDB;
use lix_slatedb_storage::SlateDB;

#[tokio::test]
async fn native_file_upsert_works_with_rocksdb() {
    let temp_dir = tempfile::tempdir().expect("create RocksDB temp directory");
    let storage =
        RocksDB::open(temp_dir.path().join("native-file-upsert")).expect("open RocksDB storage");
    assert_native_file_upsert(storage).await;
}

#[tokio::test]
async fn native_file_upsert_works_with_slatedb() {
    let temp_dir = tempfile::tempdir().expect("create SlateDB temp directory");
    let storage =
        SlateDB::open(temp_dir.path().join("native-file-upsert")).expect("open SlateDB storage");
    assert_native_file_upsert(storage).await;
}

async fn assert_native_file_upsert<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    Engine::initialize(storage.clone())
        .await
        .expect("initialize storage");
    let engine = Engine::new(storage).await.expect("open engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("open workspace session");

    assert_eq!(
        session
            .upsert_file_data(
                "/native/deep/payload.bin".to_string(),
                b"first".to_vec().into()
            )
            .await
            .expect("create through native file upsert"),
        1
    );
    assert_file_data(&session, "/native/deep/payload.bin", b"first").await;

    assert_eq!(
        session
            .upsert_file_data(
                "/native/deep/payload.bin".to_string(),
                b"second".to_vec().into()
            )
            .await
            .expect("update through native file upsert"),
        1
    );
    assert_file_data(&session, "/native/deep/payload.bin", b"second").await;

    // Empty content is a present empty file. For an existing blob-backed file,
    // the fast helper also stages the matching blob-reference tombstone.
    assert_eq!(
        session
            .upsert_file_data("/native/deep/payload.bin".to_string(), Vec::new().into())
            .await
            .expect("empty native file upsert"),
        1
    );
    assert_file_data(&session, "/native/deep/payload.bin", b"").await;

    let fast_batch_parent = active_branch_commit_id(&session).await;
    assert_eq!(
        session
            .upsert_file_data_batch(file_writes(&[
                ("/native/deep/payload.bin", b"batch-update"),
                ("/native/batch/one.bin", b"one"),
                ("/native/batch/two.bin", b"two"),
                ("/native/batch/empty.bin", b""),
            ]))
            .await
            .expect("create a native file batch"),
        4
    );
    assert_active_branch_head_parent(&session, &fast_batch_parent).await;
    assert_file_data(&session, "/native/deep/payload.bin", b"batch-update").await;
    assert_file_data(&session, "/native/batch/one.bin", b"one").await;
    assert_file_data(&session, "/native/batch/two.bin", b"two").await;
    assert_file_data(&session, "/native/batch/empty.bin", b"").await;

    let fast_batch_update_parent = active_branch_commit_id(&session).await;
    assert_eq!(
        session
            .upsert_file_data_batch(file_writes(&[
                ("/native/batch/one.bin", b"updated-one"),
                ("/native/batch/two.bin", b""),
                ("/native/batch/empty.bin", b"updated-empty"),
            ]))
            .await
            .expect("update a native file batch"),
        3
    );
    assert_active_branch_head_parent(&session, &fast_batch_update_parent).await;
    assert_file_data(&session, "/native/batch/one.bin", b"updated-one").await;
    assert_file_data(&session, "/native/batch/two.bin", b"").await;
    assert_file_data(&session, "/native/batch/empty.bin", b"updated-empty").await;

    let head_before_prevalidation_errors = active_branch_commit_id(&session).await;
    let empty_error = session
        .upsert_file_data_batch(Vec::new())
        .await
        .expect_err("empty native file batches should be rejected");
    assert_eq!(empty_error.code, LixError::CODE_INVALID_PARAM);

    let duplicate_error = session
        .upsert_file_data_batch(file_writes(&[
            ("/native/batch/duplicate.bin", b"first"),
            ("/native/batch/duplicate.bin", b"second"),
        ]))
        .await
        .expect_err("duplicate paths should be rejected before the batch writes");
    assert_eq!(duplicate_error.code, LixError::CODE_INVALID_PARAM);
    assert_file_missing(&session, "/native/batch/duplicate.bin").await;

    let path_error = session
        .upsert_file_data_batch(file_writes(&[
            ("/native/batch/must-not-write.bin", b"first"),
            ("relative.bin", b"invalid"),
        ]))
        .await
        .expect_err("an invalid path should reject the complete batch before it writes");
    assert_eq!(path_error.code, "LIX_ERROR_PATH_MISSING_LEADING_SLASH");
    assert_file_missing(&session, "/native/batch/must-not-write.bin").await;
    assert_eq!(
        active_branch_commit_id(&session).await,
        head_before_prevalidation_errors,
        "prevalidation failures must not create a partial batch commit"
    );

    // A global file can have a branch-local overlay at the same logical path.
    // The exact path index selects the active overlay directly, without a
    // general SQL fallback.
    let active_branch_id = session
        .active_branch_id()
        .await
        .expect("active branch should resolve");
    session
        .execute(
            "INSERT INTO lix_file_by_branch \
             (id, path, data, lixcol_global, lixcol_branch_id) \
             VALUES ($1, $2, $3, true, 'global')",
            &[
                Value::Text("native-overlap".to_string()),
                Value::Text("/native/overlap.bin".to_string()),
                Value::Blob(b"g".to_vec().into()),
            ],
        )
        .await
        .expect("global overlap fixture should insert");
    session
        .execute(
            "INSERT INTO lix_file_by_branch \
             (id, path, data, lixcol_branch_id) \
             VALUES ($1, $2, $3, $4)",
            &[
                Value::Text("native-overlap".to_string()),
                Value::Text("/native/overlap.bin".to_string()),
                Value::Blob(b"l".to_vec().into()),
                Value::Text(active_branch_id.clone()),
            ],
        )
        .await
        .expect("active overlap fixture should insert");

    let overlay_batch_parent = active_branch_commit_id(&session).await;
    assert_eq!(
        session
            .upsert_file_data_batch(file_writes(&[
                ("/native/overlap.bin", b"updated"),
                ("/native/batch/overlay-companion.bin", b"companion"),
            ]))
            .await
            .expect("direct batch should update the active overlay"),
        2
    );
    assert_active_branch_head_parent(&session, &overlay_batch_parent).await;
    assert_file_data_by_branch(&session, "native-overlap", &active_branch_id, b"updated").await;
    assert_file_data_by_branch(&session, "native-overlap", "global", b"g").await;
    assert_file_data(
        &session,
        "/native/batch/overlay-companion.bin",
        b"companion",
    )
    .await;
}

fn file_writes(files: &[(&str, &[u8])]) -> Vec<(String, Blob)> {
    files
        .iter()
        .map(|(path, data)| ((*path).to_string(), (*data).to_vec().into()))
        .collect()
}

async fn active_branch_commit_id<S>(session: &SessionContext<S>) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await
        .expect("read active branch commit id")
        .rows()
        .first()
        .expect("active branch commit id should have one row")
        .get::<String>("commit_id")
        .expect("active branch commit id should decode")
}

async fn assert_active_branch_head_parent<S>(session: &SessionContext<S>, expected_parent: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let head = active_branch_commit_id(session).await;
    let result = session
        .execute(
            "SELECT parent_id FROM lix_commit_edge WHERE child_id = $1",
            &[Value::Text(head)],
        )
        .await
        .expect("read active branch commit edge");
    assert_eq!(
        result.rows().len(),
        1,
        "a native file batch should create exactly one single-parent commit"
    );
    assert_eq!(
        result.rows()[0]
            .get::<String>("parent_id")
            .expect("active branch parent id should decode"),
        expected_parent
    );
}

async fn assert_file_missing<S>(session: &SessionContext<S>, path: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = session
        .execute(
            "SELECT data FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await
        .expect("read native file absence");
    assert!(
        result.rows().is_empty(),
        "file '{path}' should not be visible after an atomic batch failure"
    );
}

async fn assert_file_data<S>(session: &SessionContext<S>, path: &str, expected: &[u8])
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = session
        .execute(
            "SELECT data FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await
        .expect("read native upserted file");
    let actual = result
        .rows()
        .first()
        .expect("native upserted file should remain visible")
        .get::<Vec<u8>>("data")
        .expect("file data should decode");
    assert_eq!(actual, expected);
}

async fn assert_file_data_by_branch<S>(
    session: &SessionContext<S>,
    id: &str,
    branch_id: &str,
    expected: &[u8],
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = session
        .execute(
            "SELECT data FROM lix_file_by_branch \
             WHERE id = $1 AND lixcol_branch_id = $2",
            &[
                Value::Text(id.to_string()),
                Value::Text(branch_id.to_string()),
            ],
        )
        .await
        .expect("read native upserted file by branch");
    let actual = result
        .rows()
        .first()
        .expect("native upserted file should remain visible by branch")
        .get::<Vec<u8>>("data")
        .expect("file data should decode");
    assert_eq!(actual, expected);
}
