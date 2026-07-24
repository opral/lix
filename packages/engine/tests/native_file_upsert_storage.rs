//! Storage-backend coverage for the structured native file-write surface.

use lix_engine::{Engine, SessionContext, Storage, Value};
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

    // A global file can have a branch-local overlay at the same logical path.
    // The direct filesystem index deliberately declines that topology so SQL
    // can select the visible active-branch row. The native surface must make
    // the same selection instead of rejecting a valid existing workspace.
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

    assert_eq!(
        session
            .upsert_file_data(
                "/native/overlap.bin".to_string(),
                b"updated".to_vec().into()
            )
            .await
            .expect("native upsert should fall back for global/local overlap"),
        1
    );
    assert_file_data_by_branch(&session, "native-overlap", &active_branch_id, b"updated").await;
    assert_file_data_by_branch(&session, "native-overlap", "global", b"g").await;
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
