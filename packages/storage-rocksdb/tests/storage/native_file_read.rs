//! Storage-backend coverage for the structured native file-read surface.

use lix::Value;
use lix::integration::{Engine, SessionContext};
use lix::storage::Storage;
use lix_storage_rocksdb::RocksDB;

#[tokio::test]
async fn native_file_read_works_with_rocksdb() {
    let temp_dir = tempfile::tempdir().expect("create RocksDB temp directory");
    let storage = RocksDB::open(temp_dir.path().join("6e617469-7665-8d66-896c-652d72656100"))
        .expect("open RocksDB storage");
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
            .read_file_content("/native/missing.bin".to_string(), None)
            .await
            .expect("read missing file"),
        None
    );

    session
        .upsert_file_content(
            "/native/deep/payload.bin".to_string(),
            b"payload".to_vec().into(),
        )
        .await
        .expect("create native file");
    assert_file_content(&session, "/native/deep/payload.bin", Some(b"payload")).await;
    let range = session
        .read_file_content("/native/deep/payload.bin".to_string(), Some(1..5))
        .await
        .expect("read native file range")
        .expect("ranged native file should exist");
    assert_eq!(range.content().as_ref(), b"aylo");
    assert_eq!(range.range(), 1..5);
    assert_eq!(range.total_size(), 7);

    session
        .upsert_file_content("/native/empty.bin".to_string(), Vec::new().into())
        .await
        .expect("create empty native file");
    assert_file_content(&session, "/native/empty.bin", Some(b"")).await;

    // Exact native reads must keep the established active-branch precedence
    // when a global file is overlaid by an active branch-local version.
    let active_branch_id = session
        .active_branch_id()
        .await
        .expect("active branch should resolve");
    session
        .execute(
            "INSERT INTO lix_file_by_branch \
             (id, path, content, lixcol_global, lixcol_branch_id) \
             VALUES ($1, $2, $3, true, 'ffffffff-ffff-7fff-bfff-ffffffffffff')",
            &[
                Value::Text("630c8282-b934-7fd8-89df-6b093f08f3e3".to_string()),
                Value::Text("/native/overlap.bin".to_string()),
                Value::Blob(b"global".to_vec().into()),
            ],
        )
        .await
        .expect("global overlap fixture should insert");
    session
        .execute(
            "INSERT INTO lix_file_by_branch \
             (id, path, content, lixcol_branch_id) \
             VALUES ($1, $2, $3, $4)",
            &[
                Value::Text("630c8282-b934-7fd8-89df-6b093f08f3e3".to_string()),
                Value::Text("/native/overlap.bin".to_string()),
                Value::Blob(b"local".to_vec().into()),
                Value::Text(active_branch_id),
            ],
        )
        .await
        .expect("local overlap fixture should insert");
    assert_file_content(&session, "/native/overlap.bin", Some(b"local")).await;
}

async fn assert_file_content<S>(session: &SessionContext<S>, path: &str, expected: Option<&[u8]>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let actual = session
        .read_file_content(path.to_string(), None)
        .await
        .expect("read native file")
        .map(|read| read.into_content().to_vec());
    assert_eq!(actual.as_deref(), expected);
}
