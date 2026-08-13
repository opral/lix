use lix::{LixError, Value, open_lix};
use lix_storage_rocksdb::RocksDB;

#[tokio::test]
async fn file_sql_bytea_hard_cut_roundtrips_after_rocksdb_reopen() {
    let temp_dir = tempfile::tempdir().expect("create RocksDB temp directory");
    let path = temp_dir.path().join("file-sql.rocksdb");
    let storage = RocksDB::open(&path).expect("open RocksDB storage");
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open repository");

    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, CAST($2 AS BYTEA))",
        &[
            Value::Text("/adapter.bin".to_string()),
            Value::Text("aé—".to_string()),
        ],
    )
    .await
    .expect("insert text through an explicit BYTEA cast");
    let lengths = lix
        .execute(
            "SELECT length(content) AS characters, OCTET_LENGTH(content) AS octets \
             FROM lix_file WHERE path = $1",
            &[Value::Text("/adapter.bin".to_string())],
        )
        .await
        .expect("read character and byte lengths");
    assert_eq!(
        lengths.rows()[0]
            .get::<i64>("characters")
            .expect("character length should decode"),
        3
    );
    assert_eq!(
        lengths.rows()[0]
            .get::<i64>("octets")
            .expect("byte length should decode"),
        6
    );

    lix.execute(
        "UPDATE lix_file SET content = $2 WHERE path = $1",
        &[
            Value::Text("/adapter.bin".to_string()),
            Value::Blob(vec![0xff, 0x00, 0x61].into()),
        ],
    )
    .await
    .expect("update with a direct binary parameter");
    let error = lix
        .execute("SELECT X'41'", &[])
        .await
        .expect_err("legacy SQL hex literals should be rejected");
    assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);

    drop(lix);
    storage.flush().expect("flush RocksDB storage");
    drop(storage);

    let reopened = RocksDB::open(&path).expect("reopen RocksDB storage");
    let lix = open_lix()
        .with_storage(reopened)
        .await
        .expect("reopen repository");
    let result = lix
        .execute(
            "SELECT content, OCTET_LENGTH(content) AS octets FROM lix_file WHERE path = $1",
            &[Value::Text("/adapter.bin".to_string())],
        )
        .await
        .expect("read binary content after reopen");
    assert_eq!(
        result.rows()[0]
            .get::<Vec<u8>>("content")
            .expect("content should decode"),
        vec![0xff, 0x00, 0x61]
    );
    assert_eq!(
        result.rows()[0]
            .get::<i64>("octets")
            .expect("byte length should decode"),
        3
    );
}
