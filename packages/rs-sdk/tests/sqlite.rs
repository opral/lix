#![cfg(feature = "sqlite")]
use lix_engine::run_storage_conformance;
use lix_sdk::{
    Lix, LixError, OpenLixOptions, SQLITE_FORMAT_VERSION, SQLite, SQLiteFactory, Storage, Value,
    WasmComponentInstance, WasmLimits, WasmPluginDetectedChange, WasmPluginEntityState,
    WasmPluginFile, WasmRuntime, open_lix, open_lix_with_storage,
};
use rusqlite::Connection;
use std::io::{Cursor, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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

#[tokio::test]
async fn sqlite_open_lix_options_supplies_plugin_wasm_runtime() {
    let tempdir = tempfile::tempdir().expect("tempdir should create");
    let path = tempdir.path().join("workspace.lix");
    let runtime = Arc::new(RecordingWasmRuntime::default());
    let wasm_runtime: Arc<dyn WasmRuntime> = runtime.clone();

    let lix = open_lix(OpenLixOptions {
        storage: SQLite::open(&path).expect("sqlite storage opens"),
        wasm_runtime: Some(wasm_runtime),
    })
    .await
    .expect("lix opens on sqlite storage with wasm runtime");

    Box::pin(install_plugin(
        &lix,
        "plugin_runtime_test",
        &build_runtime_test_plugin_archive(),
    ))
    .await
    .expect("plugin archive installs");
    write_file(&lix, "/custom.runtime", b"source bytes".to_vec())
        .await
        .expect("matching plugin file write uses the supplied wasm runtime");

    assert_eq!(runtime.init_calls.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.detect_calls.load(Ordering::SeqCst), 1);

    let rendered = read_file(&lix, "/custom.runtime")
        .await
        .expect("plugin file reads");
    assert_eq!(
        rendered.as_deref(),
        Some(b"rendered by custom runtime".as_slice())
    );
    assert_eq!(runtime.render_calls.load(Ordering::SeqCst), 1);

    lix.close().await.expect("lix closes");
}

fn sqlite_journal_mode(path: &std::path::Path) -> String {
    let conn = Connection::open(path).expect("sqlite file should open");
    conn.pragma_query_value(None, "journal_mode", |row| row.get(0))
        .expect("journal_mode should read")
}

async fn install_plugin<StorageImpl>(
    lix: &Lix<StorageImpl>,
    key: &str,
    archive: &[u8],
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    write_file(
        lix,
        &format!("/.lix/plugins/{key}.lixplugin"),
        archive.to_vec(),
    )
    .await
}

async fn write_file<StorageImpl>(
    lix: &Lix<StorageImpl>,
    path: &str,
    data: Vec<u8>,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_file (path, data) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET data = excluded.data",
        &[Value::Text(path.to_string()), Value::Blob(data)],
    )
    .await?;
    Ok(())
}

async fn read_file<StorageImpl>(
    lix: &Lix<StorageImpl>,
    path: &str,
) -> Result<Option<Vec<u8>>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let result = lix
        .execute(
            "SELECT data FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await?;
    result
        .rows()
        .first()
        .map(|row| row.get::<Vec<u8>>("data"))
        .transpose()
}

#[derive(Default)]
struct RecordingWasmRuntime {
    init_calls: Arc<AtomicUsize>,
    detect_calls: Arc<AtomicUsize>,
    render_calls: Arc<AtomicUsize>,
}

struct RecordingWasmComponent {
    detect_calls: Arc<AtomicUsize>,
    render_calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl WasmRuntime for RecordingWasmRuntime {
    async fn init_component(
        &self,
        bytes: Vec<u8>,
        _limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
        assert!(bytes.starts_with(b"\0asm"));
        self.init_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(RecordingWasmComponent {
            detect_calls: self.detect_calls.clone(),
            render_calls: self.render_calls.clone(),
        }))
    }
}

#[async_trait::async_trait]
impl WasmComponentInstance for RecordingWasmComponent {
    async fn detect_changes(
        &self,
        _state: Vec<WasmPluginEntityState>,
        _file: WasmPluginFile,
    ) -> Result<Vec<WasmPluginDetectedChange>, LixError> {
        self.detect_calls.fetch_add(1, Ordering::SeqCst);
        Ok(vec![WasmPluginDetectedChange {
            entity_pk: vec!["doc".to_string()],
            schema_key: "test_plugin_doc".to_string(),
            snapshot_content: Some("{\"id\":\"doc\",\"content\":\"from runtime\"}".to_string()),
            metadata: None,
        }])
    }

    async fn render(&self, _state: Vec<WasmPluginEntityState>) -> Result<Vec<u8>, LixError> {
        self.render_calls.fetch_add(1, Ordering::SeqCst);
        Ok(b"rendered by custom runtime".to_vec())
    }
}

fn build_runtime_test_plugin_archive() -> Vec<u8> {
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        ("manifest.json", RUNTIME_TEST_PLUGIN_MANIFEST.as_bytes()),
        (
            "schema/test_plugin_doc.json",
            RUNTIME_TEST_PLUGIN_SCHEMA.as_bytes(),
        ),
        ("plugin.wasm", b"\0asm\x01\0\0\0".as_slice()),
    ] {
        writer.start_file(path, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

const RUNTIME_TEST_PLUGIN_MANIFEST: &str = r#"{
  "key": "plugin_runtime_test",
  "runtime": "wasm-component-v1",
  "api_version": "0.1.0",
  "match": {
    "path_glob": "*.runtime",
    "content_type": "text"
  },
  "entry": "plugin.wasm",
  "schemas": [
    "schema/test_plugin_doc.json"
  ]
}"#;

const RUNTIME_TEST_PLUGIN_SCHEMA: &str = r#"{
  "x-lix-key": "test_plugin_doc",
  "x-lix-primary-key": [
    "/id"
  ],
  "type": "object",
  "required": [
    "id",
    "content"
  ],
  "properties": {
    "id": {
      "type": "string"
    },
    "content": {
      "type": "string"
    }
  },
  "additionalProperties": false
}"#;

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
