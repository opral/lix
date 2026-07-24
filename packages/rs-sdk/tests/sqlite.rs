#![cfg(feature = "sqlite")]
use lix_engine::run_storage_conformance;
use lix_sdk::{
    SQLITE_FORMAT_VERSION, SQLite, SQLiteFactory, Value, open_lix, open_lix_with_storage,
};
use rusqlite::Connection;

#[tokio::test]
async fn sqlite_passes_storage_conformance() {
    let factory = SQLiteFactory::new();

    run_storage_conformance(&factory).await.assert_no_failures();
}

#[test]
fn sqlite_initializes_file_format_and_open_pragmas() {
    let tempdir = tempfile::tempdir().expect("tempdir should create");
    let path = tempdir.path().join("workspace.lix");

    let storage = SQLite::open(&path).expect("sqlite storage opens");

    assert_eq!(
        storage
            .format_version()
            .expect("format version should read"),
        SQLITE_FORMAT_VERSION,
        "empty database should initialize to the current format version"
    );
    assert_eq!(
        sqlite_journal_mode(&path),
        "wal",
        "sqlite storage should use WAL journal mode"
    );
    assert_eq!(
        storage.busy_timeout_ms().expect("busy timeout should read"),
        5000,
        "sqlite storage should set a 5s busy timeout on opened connections"
    );

    drop(storage);
}

#[test]
fn sqlite_refuses_future_file_format_version() {
    let tempdir = tempfile::tempdir().expect("tempdir should create");
    let path = tempdir.path().join("workspace.lix");
    let conn = Connection::open(&path).expect("sqlite file should create");
    conn.pragma_update(None, "user_version", 999)
        .expect("future user_version should write");
    drop(conn);

    let Err(error) = SQLite::open(&path) else {
        panic!("future file format version should be refused");
    };

    assert!(
        error.to_string().contains("newer than supported version"),
        "error should explain future format version: {error}"
    );
}

#[tokio::test]
async fn sqlite_persists_lix_data_across_reopen() {
    let tempdir = tempfile::tempdir().expect("tempdir should create");
    let path = tempdir.path().join("workspace.lix");

    {
        let lix = open_lix_with_storage(SQLite::open(&path).expect("sqlite storage opens"))
            .await
            .expect("lix opens on sqlite storage");
        lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('sqlite-key', 'sqlite-value')",
            &[],
        )
        .await
        .expect("write succeeds");
        lix.close().await.expect("lix closes");
    }

    let lix = open_lix_with_storage(SQLite::open(&path).expect("sqlite storage reopens"))
        .await
        .expect("lix reopens on sqlite storage");
    let result = lix
        .execute(
            "SELECT key FROM lix_key_value WHERE key = 'sqlite-key' AND value = lix_json('\"sqlite-value\"')",
            &[],
        )
        .await
        .expect("read succeeds");

    assert_eq!(result.len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Text("sqlite-key".to_string())]
    );
    lix.close().await.expect("lix closes");
}

fn sqlite_journal_mode(path: &std::path::Path) -> String {
    let conn = Connection::open(path).expect("sqlite file should open");
    conn.pragma_query_value(None, "journal_mode", |row| row.get(0))
        .expect("journal_mode should read")
}

#[tokio::test]
async fn sqlite_scans_with_usize_max_limit() {
    // The engine drives unbounded scans as one visit_next(usize::MAX) call;
    // a wrapping lookahead limit returned zero rows in release builds.
    use lix_sdk::{
        CoreProjection, KeyRange, PutBatch, ReadOptions, ScanOptions, SpaceId, Storage,
        StorageRead, StorageWrite, WriteOptions,
    };
    const TEST_SPACE: SpaceId = SpaceId(0x0001_0001);
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = SQLite::open(dir.path().join("max.lix")).expect("open");
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("write");
    write
        .put_many(
            TEST_SPACE,
            PutBatch {
                entries: (0..10u32)
                    .map(|index| lix_engine::storage::PutEntry {
                        key: lix_sdk::Key(bytes::Bytes::from(format!("k{index:04}"))),
                        value: lix_sdk::StoredValue {
                            bytes: bytes::Bytes::from(vec![index.to_le_bytes()[0]; 8]),
                        },
                    })
                    .collect(),
            },
        )
        .await
        .expect("put");
    write.commit().await.expect("commit");

    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("read");
    let result = read
        .scan(
            TEST_SPACE,
            KeyRange {
                lower: std::ops::Bound::Unbounded,
                upper: std::ops::Bound::Unbounded,
            },
            ScanOptions {
                projection: CoreProjection::FullValue,
                limit_rows: usize::MAX,
                resume_after: None,
            },
        )
        .await
        .expect("scan");
    assert_eq!(result.entries.len(), 10);
    assert!(!result.has_more);
}

#[tokio::test]
async fn sqlite_put_many_handles_multi_chunk_batches() {
    use bytes::Bytes;
    use lix_engine::storage::PutEntry;
    use lix_sdk::{
        CoreProjection, GetOptions, Key, ProjectedValue, PutBatch, ReadOptions, SpaceId, Storage,
        StorageRead, StorageWrite, StoredValue, WriteOptions,
    };
    const TEST_SPACE: SpaceId = SpaceId(0x0001_0001);

    // 300 entries: two full 128-row upsert chunks plus a 44-row remainder.
    const ROWS: usize = 300;

    let tempdir = tempfile::tempdir().expect("tempdir should create");
    let storage = SQLite::open(tempdir.path().join("chunked.lix")).expect("sqlite storage opens");

    let key = |index: usize| Key(Bytes::from(format!("chunked/{index:03}")));
    let batch = |tag: u8| PutBatch {
        entries: (0..ROWS)
            // Reverse insertion order so put_many's internal key sort is
            // exercised against out-of-order input.
            .rev()
            .map(|index| PutEntry {
                key: key(index),
                value: StoredValue {
                    bytes: Bytes::from(vec![tag, index.to_le_bytes()[0]]),
                },
            })
            .collect(),
    };

    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("begin insert write");
    write
        .put_many(TEST_SPACE, batch(1))
        .await
        .expect("insert all rows");
    let insert_stats = write.commit().await.expect("commit inserts").stats;
    assert_eq!(insert_stats.put_entries, ROWS as u64);
    assert_eq!(insert_stats.written_bytes, (ROWS * 2) as u64);

    // Overwrite every row so both the chunked and remainder paths take the
    // upsert conflict branch.
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("begin overwrite write");
    write
        .put_many(TEST_SPACE, batch(2))
        .await
        .expect("overwrite all rows");
    write.commit().await.expect("commit overwrites");

    let keys = (0..ROWS).map(key).collect::<Vec<_>>();
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("begin read");
    let result = read
        .get_many(
            TEST_SPACE,
            &keys,
            GetOptions {
                projection: CoreProjection::FullValue,
            },
        )
        .await
        .expect("read keys");
    drop(read);

    for (index, value) in result.values.iter().enumerate() {
        assert_eq!(
            value.as_ref().map(|value| match value {
                ProjectedValue::FullValue(bytes) => bytes.as_ref(),
                ProjectedValue::KeyOnly => &[][..],
            }),
            Some([2u8, index.to_le_bytes()[0]].as_slice()),
            "row {index} should hold the overwritten value"
        );
    }
}
