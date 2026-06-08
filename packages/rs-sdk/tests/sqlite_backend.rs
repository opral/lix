#![cfg(feature = "sqlite")]
use lix_engine::run_backend_conformance;
use lix_sdk::{
    FsWriteOptions, OpenLixOptions, SQLITE_FORMAT_VERSION, SqliteBackend, SqliteBackendFactory,
    Value, WasmComponentInstance, WasmLimits, WasmPluginDetectedChange, WasmPluginEntityState,
    WasmPluginFile, WasmRuntime, WorktreeBackend, open_lix, open_lix_with_backend,
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
async fn worktree_backend_wraps_sqlite_and_materializes_across_reopen() {
    let tempdir = tempfile::tempdir().expect("tempdir should create");
    let sqlite_path = tempdir.path().join("workspace.lix");
    let worktree_path = tempdir.path().join("worktree");

    {
        let backend = WorktreeBackend::open(
            SqliteBackend::open(&sqlite_path).expect("sqlite backend opens"),
            &worktree_path,
        )
        .await
        .expect("worktree backend opens");
        let lix = open_lix_with_backend(backend)
            .await
            .expect("lix opens on worktree sqlite backend");
        lix.write_file(
            "/persisted.txt",
            b"persisted".to_vec(),
            FsWriteOptions::default(),
        )
        .await
        .expect("file write succeeds");
        wait_for_disk_file(
            &worktree_path.join("persisted.txt"),
            Some(b"persisted".as_slice()),
        );
        lix.close().await.expect("lix closes");
    }

    {
        let plain = open_lix_with_backend(
            SqliteBackend::open(&sqlite_path).expect("sqlite backend reopens"),
        )
        .await
        .expect("plain lix opens on sqlite backend");
        assert_eq!(
            plain
                .read_file("/persisted.txt")
                .await
                .expect("persisted file reads")
                .as_deref(),
            Some(b"persisted".as_slice())
        );
        plain.close().await.expect("plain lix closes");
    }

    let backend = WorktreeBackend::open(
        SqliteBackend::open(&sqlite_path).expect("sqlite backend reopens"),
        &worktree_path,
    )
    .await
    .expect("worktree backend reopens");
    let lix = open_lix_with_backend(backend)
        .await
        .expect("lix reopens on worktree sqlite backend");
    assert_eq!(
        lix.read_file("/persisted.txt")
            .await
            .expect("persisted file reads after reopen")
            .as_deref(),
        Some(b"persisted".as_slice())
    );
    lix.write_file("/second.txt", b"second".to_vec(), FsWriteOptions::default())
        .await
        .expect("second file write succeeds");
    wait_for_disk_file(
        &worktree_path.join("second.txt"),
        Some(b"second".as_slice()),
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

    lix.install_plugin_archive(&build_runtime_test_plugin_archive())
        .await
        .expect("plugin archive installs");
    lix.write_file(
        "/custom.runtime",
        b"source bytes".to_vec(),
        FsWriteOptions::default(),
    )
    .await
    .expect("matching plugin file write uses the supplied wasm runtime");

    assert_eq!(runtime.init_calls.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.detect_calls.load(Ordering::SeqCst), 1);

    let rendered = lix
        .read_file("/custom.runtime")
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
    ) -> Result<Arc<dyn WasmComponentInstance>, lix_sdk::LixError> {
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
    ) -> Result<Vec<WasmPluginDetectedChange>, lix_sdk::LixError> {
        self.detect_calls.fetch_add(1, Ordering::SeqCst);
        Ok(vec![WasmPluginDetectedChange {
            entity_pk: vec!["doc".to_string()],
            schema_key: "test_plugin_doc".to_string(),
            snapshot_content: Some("{\"id\":\"doc\",\"content\":\"from runtime\"}".to_string()),
            metadata: None,
        }])
    }

    async fn render(
        &self,
        _state: Vec<WasmPluginEntityState>,
    ) -> Result<Vec<u8>, lix_sdk::LixError> {
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
