//! Storage-backend coverage for the structured native file-read surface.

use lix_engine::{Engine, SessionContext, Storage, Value};
use lix_rocksdb_storage::RocksDB;
use lix_slatedb_storage::SlateDB;

#[tokio::test]
async fn native_file_read_works_with_rocksdb() {
    let temp_dir = tempfile::tempdir().expect("create RocksDB temp directory");
    let storage =
        RocksDB::open(temp_dir.path().join("native-file-read")).expect("open RocksDB storage");
    assert_native_file_read(storage).await;
}

#[tokio::test]
async fn native_file_read_works_with_slatedb() {
    let temp_dir = tempfile::tempdir().expect("create SlateDB temp directory");
    let storage =
        SlateDB::open(temp_dir.path().join("native-file-read")).expect("open SlateDB storage");
    assert_native_file_read(storage).await;
}

async fn assert_native_file_read<S>(storage: S)
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
            .read_file_data("/native/missing.bin".to_string())
            .await
            .expect("read missing file"),
        None
    );

    session
        .upsert_file_data(
            "/native/deep/payload.bin".to_string(),
            b"payload".to_vec().into(),
        )
        .await
        .expect("create native file");
    assert_file_data(&session, "/native/deep/payload.bin", Some(b"payload")).await;

    session
        .upsert_file_data("/native/empty.bin".to_string(), Vec::new().into())
        .await
        .expect("create empty native file");
    assert_file_data(&session, "/native/empty.bin", Some(b"")).await;

    // Exact native reads must keep the established active-branch precedence
    // when a global file is overlaid by an active branch-local version.
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
                Value::Text("native-read-overlap".to_string()),
                Value::Text("/native/overlap.bin".to_string()),
                Value::Blob(b"global".to_vec().into()),
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
                Value::Text("native-read-overlap".to_string()),
                Value::Text("/native/overlap.bin".to_string()),
                Value::Blob(b"local".to_vec().into()),
                Value::Text(active_branch_id),
            ],
        )
        .await
        .expect("local overlap fixture should insert");
    assert_file_data(&session, "/native/overlap.bin", Some(b"local")).await;
}

async fn assert_file_data<S>(session: &SessionContext<S>, path: &str, expected: Option<&[u8]>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let actual = session
        .read_file_data(path.to_string())
        .await
        .expect("read native file")
        .map(|data| data.to_vec());
    assert_eq!(actual.as_deref(), expected);
}
