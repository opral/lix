#![cfg(feature = "sqlite")]
use lix_engine::run_backend_conformance;
use lix_sdk::{
    Backend, FsBackend, Lix, LixError, OpenLixOptions, SQLITE_FORMAT_VERSION, SqliteBackend,
    SqliteBackendFactory, Value, WasmComponentInstance, WasmLimits, WasmPluginDetectedChange,
    WasmPluginEntityState, WasmPluginFile, WasmRuntime, open_lix, open_lix_with_backend,
};
use rusqlite::Connection;
use std::io::{Cursor, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

#[test]
fn sqlite_backend_passes_backend_conformance() {
    let factory = SqliteBackendFactory::new();

    run_backend_conformance(&factory).assert_no_failures();
}

#[test]
fn sqlite_backend_initializes_file_format_and_open_pragmas() {
    let tempdir = tempfile::tempdir().expect("tempdir should create");
    let path = tempdir.path().join("workspace.lix");

    let backend = SqliteBackend::open(&path).expect("sqlite backend opens");

    assert_eq!(
        backend
            .format_version()
            .expect("format version should read"),
        SQLITE_FORMAT_VERSION,
        "empty database should initialize to the current format version"
    );
    assert_eq!(
        sqlite_journal_mode(&path),
        "wal",
        "sqlite backend should use WAL journal mode"
    );
    assert_eq!(
        backend.busy_timeout_ms().expect("busy timeout should read"),
        5000,
        "sqlite backend should set a 5s busy timeout on opened connections"
    );

    drop(backend);
}

#[test]
fn sqlite_backend_refuses_future_file_format_version() {
    let tempdir = tempfile::tempdir().expect("tempdir should create");
    let path = tempdir.path().join("workspace.lix");
    let conn = Connection::open(&path).expect("sqlite file should create");
    conn.pragma_update(None, "user_version", 999)
        .expect("future user_version should write");
    drop(conn);

    let Err(error) = SqliteBackend::open(&path) else {
        panic!("future file format version should be refused");
    };

    assert!(
        error.to_string().contains("newer than supported version"),
        "error should explain future format version: {error}"
    );
}

#[tokio::test]
async fn sqlite_backend_persists_lix_data_across_reopen() {
    let tempdir = tempfile::tempdir().expect("tempdir should create");
    let path = tempdir.path().join("workspace.lix");

    {
        let lix = open_lix_with_backend(SqliteBackend::open(&path).expect("sqlite backend opens"))
            .await
            .expect("lix opens on sqlite backend");
        lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('sqlite-key', 'sqlite-value')",
            &[],
        )
        .await
        .expect("write succeeds");
        lix.close().await.expect("lix closes");
    }

    let lix = open_lix_with_backend(SqliteBackend::open(&path).expect("sqlite backend reopens"))
        .await
        .expect("lix reopens on sqlite backend");
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
async fn fs_backend_uses_dir_lix_and_materializes_across_reopen() {
    let tempdir = tempfile::tempdir().expect("tempdir should create");
    let filesystem_path = tempdir.path().join("filesystem");

    {
        let backend = FsBackend::open(&filesystem_path)
            .await
            .expect("fs backend opens");
        let lix = open_lix_with_backend(backend)
            .await
            .expect("lix opens on fs backend");
        let sqlite_dir = filesystem_path.join(".lix/.internal");
        let sqlite_path = sqlite_dir.join("db.sqlite");
        assert!(
            sqlite_dir.is_dir(),
            "fs backend should store SQLite in dir/.lix/.internal"
        );
        assert!(
            sqlite_path.is_file(),
            "fs backend should store SQLite at dir/.lix/.internal/db.sqlite"
        );
        assert_eq!(
            read_file(&lix, "/.lix")
                .await
                .expect(".lix file path reads"),
            None,
            "dir/.lix should not be imported into Lix as a file"
        );
        assert_eq!(
            readdir(&lix, "/.lix/")
                .await
                .expect(".lix directory path reads"),
            Some(Vec::new()),
            "dir/.lix should be visible but private children should not"
        );
        assert_eq!(
            readdir(&lix, "/.lix/.internal/")
                .await
                .expect(".lix/.internal directory path reads"),
            None,
            "dir/.lix/.internal should not be visible as a synced Lix directory"
        );
        write_file(&lix, "/persisted.txt", b"persisted".to_vec())
            .await
            .expect("file write succeeds");
        wait_for_disk_file(
            &filesystem_path.join("persisted.txt"),
            Some(b"persisted".as_slice()),
        );
        lix.close().await.expect("lix closes");
    }

    {
        let plain = open_lix_with_backend(
            SqliteBackend::open(filesystem_path.join(".lix/.internal").join("db.sqlite"))
                .expect("sqlite backend reopens"),
        )
        .await
        .expect("plain lix opens on sqlite backend");
        assert_eq!(
            read_file(&plain, "/persisted.txt")
                .await
                .expect("persisted file reads")
                .as_deref(),
            Some(b"persisted".as_slice())
        );
        plain.close().await.expect("plain lix closes");
    }

    let backend = FsBackend::open(&filesystem_path)
        .await
        .expect("fs backend reopens");
    let lix = open_lix_with_backend(backend)
        .await
        .expect("lix reopens on fs backend");
    assert_eq!(
        read_file(&lix, "/persisted.txt")
            .await
            .expect("persisted file reads after reopen")
            .as_deref(),
        Some(b"persisted".as_slice())
    );
    write_file(&lix, "/second.txt", b"second".to_vec())
        .await
        .expect("second file write succeeds");
    wait_for_disk_file(
        &filesystem_path.join("second.txt"),
        Some(b"second".as_slice()),
    );
    lix.close().await.expect("lix closes");
}

#[tokio::test]
async fn fs_backend_moves_legacy_root_sqlite_metadata_into_internal_dir() {
    let tempdir = tempfile::tempdir().expect("tempdir should create");
    let filesystem_path = tempdir.path().join("filesystem");
    let legacy_sqlite_dir = filesystem_path.join(".lix");
    let legacy_sqlite_path = legacy_sqlite_dir.join("db.sqlite");
    std::fs::create_dir_all(&legacy_sqlite_dir).expect("legacy sqlite dir creates");

    {
        let lix = open_lix_with_backend(
            SqliteBackend::open(&legacy_sqlite_path).expect("legacy sqlite backend opens"),
        )
        .await
        .expect("legacy lix opens");
        lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('legacy-key', 'legacy-value')",
            &[],
        )
        .await
        .expect("legacy key value write succeeds");
        lix.close().await.expect("legacy lix closes");
    }
    for name in ["db.sqlite-wal", "db.sqlite-shm", "db.sqlite-journal"] {
        std::fs::write(legacy_sqlite_dir.join(name), b"legacy-sidecar")
            .expect("legacy sidecar writes");
    }

    let lix = open_lix_with_backend(
        FsBackend::open(&filesystem_path)
            .await
            .expect("fs backend opens"),
    )
    .await
    .expect("lix opens on fs backend");

    assert!(
        !legacy_sqlite_path.exists(),
        "legacy SQLite path should move"
    );
    for name in ["db.sqlite-wal", "db.sqlite-shm", "db.sqlite-journal"] {
        assert!(
            !legacy_sqlite_dir.join(name).exists(),
            "legacy {name} should move"
        );
    }
    for name in ["db.sqlite-wal", "db.sqlite-shm"] {
        assert!(
            filesystem_path.join(".lix/.internal").join(name).is_file(),
            "{name} should move into .lix/.internal"
        );
    }
    assert!(
        filesystem_path.join(".lix/.internal/db.sqlite").is_file(),
        "SQLite file should move into .lix/.internal"
    );
    let rows = lix
        .execute(
            "SELECT key FROM lix_key_value WHERE key = 'legacy-key' AND value = lix_json('\"legacy-value\"')",
            &[],
        )
        .await
        .expect("legacy key value reads");
    assert_eq!(rows.len(), 1, "moved SQLite database should be reused");
    assert_eq!(
        read_file(&lix, "/.lix/db.sqlite")
            .await
            .expect("legacy metadata file path reads"),
        None,
        "legacy SQLite file should not be imported as a Lix file"
    );

    lix.close().await.expect("lix closes");
}

#[tokio::test]
async fn fs_backend_legacy_sqlite_metadata_conflict_does_not_partially_move() {
    let tempdir = tempfile::tempdir().expect("tempdir should create");
    let filesystem_path = tempdir.path().join("filesystem");
    let legacy_sqlite_dir = filesystem_path.join(".lix");
    let internal_sqlite_dir = filesystem_path.join(".lix/.internal");
    std::fs::create_dir_all(&legacy_sqlite_dir).expect("legacy sqlite dir creates");
    std::fs::create_dir_all(&internal_sqlite_dir).expect("internal sqlite dir creates");
    std::fs::write(legacy_sqlite_dir.join("db.sqlite"), b"legacy").expect("legacy sqlite writes");
    std::fs::write(legacy_sqlite_dir.join("db.sqlite-wal"), b"legacy-wal")
        .expect("legacy wal writes");
    std::fs::write(internal_sqlite_dir.join("db.sqlite-wal"), b"target-wal")
        .expect("target wal writes");

    let Err(error) = FsBackend::open(&filesystem_path).await else {
        panic!("conflicting legacy metadata should fail");
    };

    assert!(
        error.message.contains("target already exists"),
        "error should explain metadata conflict: {error:?}"
    );
    assert!(
        legacy_sqlite_dir.join("db.sqlite").is_file(),
        "legacy db.sqlite should remain in place after preflight failure"
    );
    assert!(
        legacy_sqlite_dir.join("db.sqlite-wal").is_file(),
        "legacy db.sqlite-wal should remain in place after preflight failure"
    );
}

#[tokio::test]
async fn fs_backend_ignores_sqlite_sidecar_paths() {
    let tempdir = tempfile::tempdir().expect("tempdir should create");
    let filesystem_path = tempdir.path().join("filesystem");
    std::fs::create_dir_all(filesystem_path.join(".lix")).expect("legacy .lix dir creates");
    for name in ["db.sqlite-wal", "db.sqlite-shm", "db.sqlite-journal"] {
        std::fs::write(filesystem_path.join(".lix").join(name), b"legacy")
            .expect("legacy sqlite sidecar writes");
    }
    let lix = open_lix_with_backend(
        FsBackend::open(&filesystem_path)
            .await
            .expect("fs backend opens"),
    )
    .await
    .expect("lix opens on fs backend");

    for path in [
        "/.lix/.internal",
        "/.lix/.internal/db.sqlite",
        "/.lix/.internal/db.sqlite-wal",
        "/.lix/.internal/db.sqlite-shm",
        "/.lix/.internal/db.sqlite-journal",
        "/.lix/db.sqlite",
        "/.lix/db.sqlite-wal",
        "/.lix/db.sqlite-shm",
        "/.lix/db.sqlite-journal",
    ] {
        assert_eq!(
            read_file(&lix, path).await.expect("metadata path reads"),
            None,
            "{path} should not be visible as a synced Lix file"
        );
    }
    assert_eq!(
        readdir(&lix, "/.lix/.internal/")
            .await
            .expect("internal directory reads"),
        None,
        "/.lix/.internal should not be visible as a synced Lix directory"
    );
    assert_eq!(
        readdir(&lix, "/.lix/").await.expect(".lix directory reads"),
        Some(Vec::new()),
        "legacy root SQLite sidecars should not appear in /.lix/"
    );

    lix.close().await.expect("lix closes");
}

#[tokio::test]
async fn sqlite_backend_open_lix_options_supplies_plugin_wasm_runtime() {
    let tempdir = tempfile::tempdir().expect("tempdir should create");
    let path = tempdir.path().join("workspace.lix");
    let runtime = Arc::new(RecordingWasmRuntime::default());
    let wasm_runtime: Arc<dyn WasmRuntime> = runtime.clone();

    let lix = open_lix(OpenLixOptions {
        backend: SqliteBackend::open(&path).expect("sqlite backend opens"),
        wasm_runtime: Some(wasm_runtime),
    })
    .await
    .expect("lix opens on sqlite backend with wasm runtime");

    install_plugin(
        &lix,
        "plugin_runtime_test",
        &build_runtime_test_plugin_archive(),
    )
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

    let hash_rows = lix
        .execute(
            "SELECT lixcol_data_hash FROM lix_file WHERE path = $1",
            &[Value::Text("/custom.runtime".to_string())],
        )
        .await
        .expect("plugin file data hash reads");
    let data_hash = hash_rows.rows()[0]
        .get::<String>("lixcol_data_hash")
        .expect("data hash should decode");
    assert!(!data_hash.is_empty());
    assert!(!data_hash.starts_with("unresolved:"));
    assert_eq!(runtime.render_calls.load(Ordering::SeqCst), 2);

    lix.close().await.expect("lix closes");
}

fn sqlite_journal_mode(path: &std::path::Path) -> String {
    let conn = Connection::open(path).expect("sqlite file should open");
    conn.pragma_query_value(None, "journal_mode", |row| row.get(0))
        .expect("journal_mode should read")
}

fn wait_for_disk_file(path: &std::path::Path, expected: Option<&[u8]>) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let path_display = path.display();
    loop {
        let actual = std::fs::read(path).ok();
        if actual.as_deref() == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for disk file {path_display} to be {expected:?}, got {actual:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

async fn install_plugin<B>(lix: &Lix<B>, key: &str, archive: &[u8]) -> Result<(), LixError>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    write_file(
        lix,
        &format!("/.lix/plugins/{key}.lixplugin"),
        archive.to_vec(),
    )
    .await
}

async fn write_file<B>(lix: &Lix<B>, path: &str, data: Vec<u8>) -> Result<(), LixError>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    lix.execute(
        "INSERT INTO lix_file (path, data) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET data = excluded.data",
        &[Value::Text(path.to_string()), Value::Blob(data)],
    )
    .await?;
    Ok(())
}

async fn read_file<B>(lix: &Lix<B>, path: &str) -> Result<Option<Vec<u8>>, LixError>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
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

async fn readdir<B>(lix: &Lix<B>, path: &str) -> Result<Option<Vec<String>>, LixError>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    let directory = lix
        .execute(
            "SELECT id FROM lix_directory WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await?;
    let Some(directory) = directory.rows().first() else {
        return Ok(None);
    };
    let directory_id = directory.get::<String>("id")?;
    let entries = lix
        .execute(
            "SELECT name FROM lix_directory WHERE parent_id = $1 \
             UNION ALL \
             SELECT name FROM lix_file WHERE directory_id = $1 \
             ORDER BY name",
            &[Value::Text(directory_id)],
        )
        .await?;
    Ok(Some(
        entries
            .rows()
            .iter()
            .map(|row| row.get::<String>("name"))
            .collect::<Result<Vec<_>, _>>()?,
    ))
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

#[test]
fn sqlite_backend_scans_with_usize_max_limit() {
    // The engine drives unbounded scans as one visit_next(usize::MAX) call;
    // a wrapping lookahead limit returned zero rows in release builds.
    use lix_sdk::{
        Backend, BackendRead, BackendWrite, CoreProjection, KeyRange, ProjectedValueRef, PutBatch,
        ReadOptions, ScanOptions, ScanVisitor, SpaceId, WriteOptions,
    };
    const TEST_SPACE: SpaceId = SpaceId(0x0001_0001);
    struct Counter(usize);
    impl ScanVisitor for Counter {
        fn visit(
            &mut self,
            _key: lix_engine::backend::KeyRef<'_>,
            _value: ProjectedValueRef<'_>,
        ) -> Result<(), lix_sdk::BackendError> {
            self.0 += 1;
            Ok(())
        }
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = SqliteBackend::open(dir.path().join("max.lix")).expect("open");
    let mut write = backend.begin_write(WriteOptions::default()).expect("write");
    write
        .put_many(
            TEST_SPACE,
            PutBatch {
                entries: (0..10u32)
                    .map(|index| lix_engine::backend::PutEntry {
                        key: lix_sdk::Key(bytes::Bytes::from(format!("k{index:04}"))),
                        value: lix_sdk::StoredValue {
                            bytes: bytes::Bytes::from(vec![index.to_le_bytes()[0]; 8]),
                        },
                    })
                    .collect(),
            },
        )
        .expect("put");
    write.commit().expect("commit");

    let read = backend.begin_read(ReadOptions::default()).expect("read");
    let mut counter = Counter(0);
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
            &mut counter,
        )
        .expect("scan");
    assert_eq!(counter.0, 10);
    assert!(!result.has_more);
}

#[test]
fn sqlite_backend_put_many_handles_multi_chunk_batches() {
    use bytes::Bytes;
    use lix_engine::backend::PutEntry;
    use lix_sdk::{
        Backend, BackendRead, BackendWrite, CoreProjection, GetOptions, Key, PointVisitor,
        ProjectedValueRef, PutBatch, ReadOptions, SpaceId, StoredValue, WriteOptions,
    };
    const TEST_SPACE: SpaceId = SpaceId(0x0001_0001);

    // 300 entries: two full 128-row upsert chunks plus a 44-row remainder.
    const ROWS: usize = 300;

    struct CollectingVisitor {
        values: Vec<Option<Vec<u8>>>,
    }
    impl PointVisitor for CollectingVisitor {
        fn visit(
            &mut self,
            index: usize,
            _key: &Key,
            value: Option<ProjectedValueRef<'_>>,
        ) -> Result<(), lix_sdk::BackendError> {
            self.values[index] = match value {
                Some(ProjectedValueRef::FullValue(bytes)) => Some(bytes.to_vec()),
                Some(ProjectedValueRef::KeyOnly) => Some(Vec::new()),
                None => None,
            };
            Ok(())
        }
    }

    let tempdir = tempfile::tempdir().expect("tempdir should create");
    let backend =
        SqliteBackend::open(tempdir.path().join("chunked.lix")).expect("sqlite backend opens");

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

    let mut write = backend
        .begin_write(WriteOptions::default())
        .expect("begin insert write");
    write
        .put_many(TEST_SPACE, batch(1))
        .expect("insert all rows");
    let insert_stats = write.commit().expect("commit inserts").stats;
    assert_eq!(insert_stats.put_entries, ROWS as u64);
    assert_eq!(insert_stats.written_bytes, (ROWS * 2) as u64);

    // Overwrite every row so both the chunked and remainder paths take the
    // upsert conflict branch.
    let mut write = backend
        .begin_write(WriteOptions::default())
        .expect("begin overwrite write");
    write
        .put_many(TEST_SPACE, batch(2))
        .expect("overwrite all rows");
    write.commit().expect("commit overwrites");

    let keys = (0..ROWS).map(key).collect::<Vec<_>>();
    let read = backend
        .begin_read(ReadOptions::default())
        .expect("begin read");
    let mut visitor = CollectingVisitor {
        values: vec![None; ROWS],
    };
    read.visit_keys(
        TEST_SPACE,
        &keys,
        GetOptions {
            projection: CoreProjection::FullValue,
            _reserved: std::marker::PhantomData,
        },
        &mut visitor,
    )
    .expect("visit keys");
    read.close().expect("close read");

    for (index, value) in visitor.values.iter().enumerate() {
        assert_eq!(
            value.as_deref(),
            Some([2u8, index.to_le_bytes()[0]].as_slice()),
            "row {index} should hold the overwritten value"
        );
    }
}
