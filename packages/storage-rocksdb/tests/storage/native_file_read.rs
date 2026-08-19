//! RocksDB coverage for the public SQL file-read surface.

use lix::storage::Storage;
use lix::{GLOBAL_BRANCH_ID, Lix, Value, open_lix};
use lix_storage_rocksdb::RocksDB;

#[tokio::test]
async fn public_file_read_works_with_rocksdb() {
    let temp_dir = tempfile::tempdir().expect("create RocksDB temp directory");
    let storage = RocksDB::open(temp_dir.path().join("6e617469-7665-8d66-896c-652d72656100"))
        .expect("open RocksDB storage");
    let lix = open_lix().with_storage(storage).await.expect("open Lix");

    assert_file_content(&lix, "/native/missing.bin", None).await;
    upsert_file(&lix, "/native/deep/payload.bin", b"payload").await;
    assert_file_content(&lix, "/native/deep/payload.bin", Some(b"payload")).await;
    upsert_file(&lix, "/native/empty.bin", b"").await;
    assert_file_content(&lix, "/native/empty.bin", Some(b"")).await;

    // Public reads retain active-branch precedence when a global file has an
    // active branch-local overlay at the same logical path.
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
                Value::Text("630c8282-b934-7fd8-89df-6b093f08f3e3".to_owned()),
                Value::Text("/native/overlap.bin".to_owned()),
                Value::Blob(b"global".to_vec().into()),
            ],
        )
        .await
        .expect("insert global overlap fixture");
    lix.execute(
        "INSERT INTO lix_file (id, path, content) VALUES ($1, $2, $3)",
        &[
            Value::Text("630c8282-b934-7fd8-89df-6b093f08f3e3".to_owned()),
            Value::Text("/native/overlap.bin".to_owned()),
            Value::Blob(b"local".to_vec().into()),
        ],
    )
    .await
    .expect("insert local overlap fixture");
    assert_file_content(&lix, "/native/overlap.bin", Some(b"local")).await;
}

async fn upsert_file<S>(lix: &Lix<S>, path: &str, content: &[u8])
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET content = excluded.content",
        &[
            Value::Text(path.to_owned()),
            Value::Blob(content.to_vec().into()),
        ],
    )
    .await
    .expect("upsert file through SQL");
}

async fn assert_file_content<S>(lix: &Lix<S>, path: &str, expected: Option<&[u8]>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = lix
        .execute(
            "SELECT content FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_owned())],
        )
        .await
        .expect("read file through SQL");
    let actual = result
        .rows()
        .first()
        .map(|row| row.get::<Vec<u8>>("content").expect("file content decodes"));
    assert_eq!(actual.as_deref(), expected);
}
