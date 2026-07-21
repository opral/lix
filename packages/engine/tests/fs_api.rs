#[macro_use]
mod support;

use std::io::{Cursor, Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lix_engine::wasm::{
    WasmComponentInstance, WasmLimits, WasmPluginDetectedChange, WasmPluginEntityState,
    WasmPluginFile, WasmRuntime,
};
use lix_engine::{Engine, ExecuteResult, LixError, Memory, SessionContext, Value};
use serde_json::json;

simulation_test!(
    sql_file_write_read_and_readdir_roundtrip,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );

        write_file(&session, "/docs/readme.txt", b"hello".to_vec())
            .await
            .expect("file upsert should create parents and data");

        assert_eq!(
            read_file(&session, "/docs/readme.txt")
                .await
                .expect("file read should succeed"),
            Some(b"hello".to_vec())
        );
        assert_eq!(
            read_file(&session, "/docs/missing.txt")
                .await
                .expect("missing file read should succeed"),
            None
        );

        let entries = readdir(&session, "/docs/")
            .await
            .expect("directory read should succeed")
            .expect("directory should exist");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "readme.txt");
        assert_eq!(entries[0].path, "/docs/readme.txt");
        assert_eq!(entries[0].kind, DirEntryKind::File);
    }
);

simulation_test!(
    sql_path_only_file_reads_as_empty_without_blob_ref,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );

        session
            .execute("INSERT INTO lix_file (path) VALUES ('/empty.txt')", &[])
            .await
            .expect("path-only file insert should succeed");

        assert_eq!(
            read_file(&session, "/empty.txt")
                .await
                .expect("file read should succeed"),
            Some(Vec::new())
        );

        let file_result = session
            .execute("SELECT id FROM lix_file WHERE path = '/empty.txt'", &[])
            .await
            .expect("file id read should succeed");
        let [Value::Text(file_id)] = file_result.rows()[0].values() else {
            panic!(
                "expected file id row, got {:?}",
                file_result.rows()[0].values()
            );
        };

        let blob_ref_result = session
            .execute(
                &format!(
                    "SELECT entity_pk \
                     FROM lix_state \
                     WHERE schema_key = 'lix_binary_blob_ref' \
                       AND entity_pk = lix_json('[\"{file_id}\"]')"
                ),
                &[],
            )
            .await
            .expect("blob ref state read should succeed");
        assert_eq!(blob_ref_result.len(), 0);
    }
);

#[tokio::test]
async fn sql_plugin_archive_upsert_installs_and_updates_plugin() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");
    let archive = sentinel_plugin_archive();

    install_plugin(&session, "plugin_sentinel", &archive)
        .await
        .expect("plugin archive upsert should install plugin");
    install_plugin(&session, "plugin_sentinel", &archive)
        .await
        .expect("plugin archive upsert should update plugin");

    let plugins = list_installed_plugins(&session)
        .await
        .expect("installed plugins should list");
    assert_eq!(plugins.len(), 1);
    assert_eq!(plugins[0].key, "plugin_sentinel");
    assert_eq!(plugins[0].schema_keys, vec!["plugin_note".to_string()]);
    assert_eq!(
        read_file(&session, "/.lix/plugins/plugin_sentinel.lixplugin")
            .await
            .expect("archive read should succeed")
            .as_deref(),
        Some(archive.as_slice())
    );

    let schemas = session
        .execute(
            "SELECT value FROM lix_registered_schema \
             WHERE lixcol_entity_pk = lix_json('[\"plugin_note\"]')",
            &[],
        )
        .await
        .expect("plugin schema should be queryable");
    assert_eq!(schemas.len(), 1);

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn sql_plugin_archive_plain_insert_reuses_deterministic_file_id() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");
    let archive = sentinel_plugin_archive();
    let path = "/.lix/plugins/plugin_sentinel.lixplugin";

    install_plugin(&session, "plugin_sentinel", &archive)
        .await
        .expect("plugin archive upsert should install plugin");

    let error = session
        .execute(
            "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
            &[Value::Text(path.to_string()), Value::Blob(archive.clone())],
        )
        .await
        .expect_err("plain plugin archive insert should reject duplicate archive id");
    assert_eq!(error.code, LixError::CODE_UNIQUE);
    assert!(
        error
            .message
            .contains("lix_plugin_archive::plugin_sentinel")
    );

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn sql_plugin_archive_path_must_match_manifest_key() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    let error = install_plugin(&session, "plugin_other", &sentinel_plugin_archive())
        .await
        .expect_err("mismatched plugin archive key should fail");

    assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
    assert!(error.message.contains("does not match manifest key"));

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn sql_update_path_to_plugin_storage_rejects_plugin_archive_rename() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    write_file(&session, "/normal.txt", b"normal".to_vec())
        .await
        .expect("normal file insert should succeed");

    let error = session
        .execute(
            "UPDATE lix_file SET path = $1, data = $2 WHERE path = $3",
            &[
                Value::Text("/.lix/plugins/plugin_sentinel.lixplugin".to_string()),
                Value::Blob(sentinel_plugin_archive()),
                Value::Text("/normal.txt".to_string()),
            ],
        )
        .await
        .expect_err("path update into plugin storage should fail");

    assert!(error.message.contains("plugin archive paths"));

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn sql_update_rejects_invalid_installed_plugin_storage_archive_data() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    install_plugin(&session, "plugin_sentinel", &sentinel_plugin_archive())
        .await
        .expect("plugin install should succeed");

    let error = session
        .execute(
            "UPDATE lix_file \
             SET data = X'626164' \
             WHERE path = '/.lix/plugins/plugin_sentinel.lixplugin'",
            &[],
        )
        .await
        .expect_err("SQL update should reject invalid plugin archive data");

    assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
    assert!(error.message.contains("ZIP"));

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn sql_invalid_plugin_manifest_writes_are_atomic() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");
    let path = "/.lix/plugins/plugin_sentinel.lixplugin";
    let invalid = invalid_glob_sentinel_plugin_archive();

    let error = install_plugin(&session, "plugin_sentinel", &invalid)
        .await
        .expect_err("a fresh install with an invalid glob must fail");
    assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
    assert!(error.message.contains("path_glob"));
    assert_eq!(
        read_file(&session, path)
            .await
            .expect("failed install archive lookup should succeed"),
        None
    );
    assert_eq!(registered_plugin_note_schema_count(&session).await, 0);

    let original = sentinel_plugin_archive();
    install_plugin(&session, "plugin_sentinel", &original)
        .await
        .expect("valid plugin install should succeed");
    let error = install_plugin(&session, "plugin_sentinel", &invalid)
        .await
        .expect_err("an update with an invalid glob must fail");
    assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
    assert_eq!(
        read_file(&session, path)
            .await
            .expect("installed archive lookup should succeed")
            .as_deref(),
        Some(original.as_slice())
    );
    assert_eq!(registered_plugin_note_schema_count(&session).await, 1);

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn sql_delete_rejects_installed_plugin_storage() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    install_plugin(&session, "plugin_sentinel", &sentinel_plugin_archive())
        .await
        .expect("plugin install should succeed");

    let archive_error = session
        .execute(
            "DELETE FROM lix_file \
             WHERE path = '/.lix/plugins/plugin_sentinel.lixplugin'",
            &[],
        )
        .await
        .expect_err("SQL delete should reject installed plugin archive tombstones");
    assert_eq!(archive_error.code, LixError::CODE_CONSTRAINT_VIOLATION);
    assert!(
        archive_error
            .message
            .contains("reserved plugin storage path")
    );

    let directory_error = session
        .execute(
            "DELETE FROM lix_directory WHERE path = '/.lix/plugins/'",
            &[],
        )
        .await
        .expect_err("SQL delete should reject plugin storage directory tombstones");
    assert_eq!(directory_error.code, LixError::CODE_CONSTRAINT_VIOLATION);
    assert!(
        directory_error
            .message
            .contains("reserved plugin storage path")
    );

    assert_eq!(
        list_installed_plugins(&session)
            .await
            .expect("plugin should still be installed")
            .len(),
        1
    );

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn empty_regular_file_does_not_render_through_later_installed_plugin() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let runtime = Arc::new(SentinelPluginRuntime::default());
    let engine = Engine::new_with_wasm_runtime(storage, runtime.clone())
        .await
        .expect("engine should open with plugin runtime");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    write_file(&session, "/raw.sentinel", Vec::new())
        .await
        .expect("empty file write should succeed");
    install_plugin(&session, "plugin_sentinel", &sentinel_plugin_archive())
        .await
        .expect("plugin install should succeed");

    assert_eq!(
        read_file(&session, "/raw.sentinel")
            .await
            .expect("file read should succeed"),
        Some(Vec::new())
    );

    let files = session
        .execute(
            "SELECT data FROM lix_file WHERE path = '/raw.sentinel'",
            &[],
        )
        .await
        .expect("lix_file data read should succeed");
    assert_eq!(files.len(), 1);
    assert_eq!(files.rows()[0].values(), &[Value::Blob(Vec::new())]);
    assert_eq!(runtime.render_calls.load(Ordering::SeqCst), 0);

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn plugin_detect_changes_receives_descriptor_filename() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let runtime = Arc::new(SentinelPluginRuntime::default());
    let engine = Engine::new_with_wasm_runtime(storage, runtime.clone())
        .await
        .expect("engine should open with plugin runtime");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    install_plugin(&session, "plugin_sentinel", &sentinel_plugin_archive())
        .await
        .expect("plugin install should succeed");
    write_file(&session, "/nested/raw.sentinel", b"hello".to_vec())
        .await
        .expect("plugin file write should succeed");

    let filenames = runtime
        .detect_filenames
        .lock()
        .expect("detect filename lock should not be poisoned")
        .clone();
    assert_eq!(filenames, vec![Some("raw.sentinel".to_string())]);

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn multi_value_file_upsert_reconciles_each_plugin_file() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let runtime = Arc::new(SentinelPluginRuntime::default());
    let engine = Engine::new_with_wasm_runtime(storage, runtime.clone())
        .await
        .expect("engine should open with plugin runtime");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    install_plugin(&session, "plugin_sentinel", &sentinel_plugin_archive())
        .await
        .expect("plugin install should succeed");
    let sql = "INSERT INTO lix_file (path, data, lixcol_metadata) \
               VALUES ($1, $2, $3), ($4, $5, $6) \
               ON CONFLICT (path) DO UPDATE SET \
                 data = excluded.data, lixcol_metadata = excluded.lixcol_metadata";
    let params = [
        Value::Text("/nested/first.sentinel".to_string()),
        Value::Blob(b"first".to_vec()),
        Value::Json(json!({"size": 5})),
        Value::Text("/nested/second.sentinel".to_string()),
        Value::Blob(b"second".to_vec()),
        Value::Json(json!({"size": 6})),
    ];
    session
        .execute(sql, &params)
        .await
        .expect("multi-value plugin file upsert should succeed");
    session
        .execute(sql, &params)
        .await
        .expect("multi-value plugin file overwrite should succeed");

    let filenames = runtime
        .detect_filenames
        .lock()
        .expect("detect filename lock should not be poisoned")
        .clone();
    assert_eq!(
        filenames,
        vec![
            Some("first.sentinel".to_string()),
            Some("second.sentinel".to_string()),
            Some("first.sentinel".to_string()),
            Some("second.sentinel".to_string()),
        ]
    );

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn empty_write_to_binary_plugin_file_clears_plugin_state() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let runtime = Arc::new(SentinelPluginRuntime::default());
    let engine = Engine::new_with_wasm_runtime(storage, runtime)
        .await
        .expect("engine should open with plugin runtime");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    install_plugin(
        &session,
        "plugin_binary_sentinel",
        &binary_sentinel_plugin_archive(),
    )
    .await
    .expect("plugin install should succeed");

    write_file(&session, "/owned.binary-sentinel", vec![0xff])
        .await
        .expect("binary plugin write should succeed");

    assert_eq!(
        read_file(&session, "/owned.binary-sentinel")
            .await
            .expect("file read should render plugin state"),
        Some(b"plugin-rendered".to_vec())
    );

    let file_id_rows = session
        .execute(
            "SELECT id FROM lix_file WHERE path = '/owned.binary-sentinel'",
            &[],
        )
        .await
        .expect("file id read should succeed");
    let [Value::Text(file_id)] = file_id_rows.rows()[0].values() else {
        panic!(
            "expected file id row, got {:?}",
            file_id_rows.rows()[0].values()
        );
    };

    let plugin_rows = session
        .execute(
            &format!(
                "SELECT entity_pk \
                 FROM lix_state \
                 WHERE schema_key = 'plugin_note' \
                   AND file_id = '{file_id}'"
            ),
            &[],
        )
        .await
        .expect("plugin state read should succeed");
    assert_eq!(plugin_rows.len(), 1);

    write_file(&session, "/owned.binary-sentinel", Vec::new())
        .await
        .expect("empty plugin write should succeed");

    assert_eq!(
        read_file(&session, "/owned.binary-sentinel")
            .await
            .expect("file read should succeed"),
        Some(Vec::new())
    );

    let plugin_rows = session
        .execute(
            &format!(
                "SELECT entity_pk \
                 FROM lix_state \
                 WHERE schema_key = 'plugin_note' \
                   AND file_id = '{file_id}'"
            ),
            &[],
        )
        .await
        .expect("plugin state read should succeed");
    assert_eq!(plugin_rows.len(), 0);

    session.close().await.expect("session should close");
}

simulation_test!(
    sql_mkdir_is_idempotent_and_readdir_distinguishes_missing,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );

        mkdir(&session, "/empty/nested/")
            .await
            .expect("mkdir should create parents");
        mkdir(&session, "/empty/nested/")
            .await
            .expect("mkdir should be idempotent");

        assert_eq!(
            readdir(&session, "/empty/nested/")
                .await
                .expect("directory read should succeed"),
            Some(Vec::new())
        );
        assert_eq!(
            readdir(&session, "/missing/")
                .await
                .expect("missing directory read should succeed"),
            None
        );
    }
);

simulation_test!(sql_write_file_upserts_existing_data, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("workspace session should open"),
        &engine,
    );

    write_file(&session, "/orders.xlsx", b"old".to_vec())
        .await
        .expect("initial write should succeed");
    write_file(&session, "/orders.xlsx", b"new".to_vec())
        .await
        .expect("second write should upsert");

    assert_eq!(
        read_file(&session, "/orders.xlsx")
            .await
            .expect("read should succeed"),
        Some(b"new".to_vec())
    );

    let rows = session
        .execute("SELECT id FROM lix_file WHERE path = '/orders.xlsx'", &[])
        .await
        .expect("query should succeed");
    assert_eq!(rows.len(), 1, "file upsert should not duplicate descriptor");

    write_file(&session, "/orders.xlsx", Vec::new())
        .await
        .expect("empty overwrite should succeed");
    assert_eq!(
        read_file(&session, "/orders.xlsx")
            .await
            .expect("read should succeed"),
        Some(Vec::new())
    );

    let [Value::Text(file_id)] = rows.rows()[0].values() else {
        panic!("expected file id row, got {:?}", rows.rows()[0].values());
    };
    let blob_ref_rows = session
        .execute(
            &format!(
                "SELECT entity_pk \
                 FROM lix_state \
                 WHERE schema_key = 'lix_binary_blob_ref' \
                   AND entity_pk = lix_json('[\"{file_id}\"]')"
            ),
            &[],
        )
        .await
        .expect("blob ref query should succeed");
    assert_eq!(blob_ref_rows.len(), 0);
});

simulation_test!(sql_rm_file_and_recursive_directory, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("workspace session should open"),
        &engine,
    );

    write_file(&session, "/tmp/a.txt", b"a".to_vec())
        .await
        .expect("write should succeed");
    write_file(&session, "/tmp/nested/b.txt", b"b".to_vec())
        .await
        .expect("nested write should succeed");

    rm_path(&session, "/tmp/")
        .await
        .expect("recursive directory delete should remove tree");

    assert_eq!(
        read_file(&session, "/tmp/a.txt")
            .await
            .expect("read should succeed"),
        None
    );
    assert_eq!(
        readdir(&session, "/tmp/")
            .await
            .expect("directory read should succeed"),
        None
    );

    rm_path(&session, "/tmp/missing.txt")
        .await
        .expect("missing delete should be a no-op");
});

simulation_test!(sql_file_directory_path_constraints, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("workspace session should open"),
        &engine,
    );

    mkdir(&session, "/docs/")
        .await
        .expect("mkdir should succeed");
    write_file(&session, "/file.txt", b"file".to_vec())
        .await
        .expect("write should succeed");

    write_file(&session, "/docs", b"nope".to_vec())
        .await
        .expect_err("file write over directory should fail");
    mkdir(&session, "/file.txt/")
        .await
        .expect_err("directory create over file should fail");
});

#[async_trait(?Send)]
trait TestSession {
    async fn execute_sql(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError>;
}

#[async_trait(?Send)]
impl TestSession for SessionContext {
    async fn execute_sql(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        self.execute(sql, params).await
    }
}

#[async_trait(?Send)]
impl TestSession for support::simulation_test::engine::SimSession {
    async fn execute_sql(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        self.execute(sql, params).await
    }
}

async fn install_plugin<S>(session: &S, key: &str, archive: &[u8]) -> Result<(), LixError>
where
    S: TestSession + Sync + ?Sized,
{
    write_file(
        session,
        &format!("/.lix/plugins/{key}.lixplugin"),
        archive.to_vec(),
    )
    .await
}

async fn write_file<S>(session: &S, path: &str, data: Vec<u8>) -> Result<(), LixError>
where
    S: TestSession + Sync + ?Sized,
{
    session
        .execute_sql(
            "INSERT INTO lix_file (path, data) VALUES ($1, $2) \
             ON CONFLICT (path) DO UPDATE SET data = excluded.data",
            &[Value::Text(path.to_string()), Value::Blob(data)],
        )
        .await?;
    Ok(())
}

async fn read_file<S>(session: &S, path: &str) -> Result<Option<Vec<u8>>, LixError>
where
    S: TestSession + Sync + ?Sized,
{
    let result = session
        .execute_sql(
            "SELECT data FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await?;
    let Some(row) = result.rows().first() else {
        return Ok(None);
    };
    match row.values() {
        [Value::Blob(data)] => Ok(Some(data.clone())),
        [Value::Null] => Ok(Some(Vec::new())),
        other => panic!("expected one blob data column, got {other:?}"),
    }
}

async fn registered_plugin_note_schema_count<S>(session: &S) -> usize
where
    S: TestSession + Sync + ?Sized,
{
    session
        .execute_sql(
            "SELECT value FROM lix_registered_schema \
             WHERE lixcol_entity_pk = lix_json('[\"plugin_note\"]')",
            &[],
        )
        .await
        .expect("plugin schema lookup should succeed")
        .len()
}

async fn mkdir<S>(session: &S, path: &str) -> Result<(), LixError>
where
    S: TestSession + Sync + ?Sized,
{
    session
        .execute_sql(
            "INSERT INTO lix_directory (path) VALUES ($1) \
             ON CONFLICT (path) DO NOTHING",
            &[Value::Text(path.to_string())],
        )
        .await?;
    Ok(())
}

async fn rm_path<S>(session: &S, path: &str) -> Result<(), LixError>
where
    S: TestSession + Sync + ?Sized,
{
    session
        .execute_sql(
            "DELETE FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await?;
    session
        .execute_sql(
            "DELETE FROM lix_directory WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await?;
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct DirEntry {
    name: String,
    path: String,
    kind: DirEntryKind,
}

#[derive(Debug, PartialEq, Eq)]
enum DirEntryKind {
    File,
    Directory,
}

async fn readdir<S>(session: &S, path: &str) -> Result<Option<Vec<DirEntry>>, LixError>
where
    S: TestSession + Sync + ?Sized,
{
    let exists = session
        .execute_sql(
            "SELECT path FROM lix_directory WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await?;
    let children = session
        .execute_sql(
            "SELECT path, 'file' AS kind FROM lix_file WHERE path LIKE $1 \
             UNION ALL \
             SELECT path, 'directory' AS kind FROM lix_directory WHERE path LIKE $1 AND path != $2 \
             ORDER BY path",
            &[
                Value::Text(format!("{path}%")),
                Value::Text(path.to_string()),
            ],
        )
        .await?;
    let mut entries = Vec::new();
    for row in children.rows() {
        let [Value::Text(child_path), Value::Text(kind)] = row.values() else {
            panic!("expected path/kind row, got {:?}", row.values());
        };
        let Some(name) = direct_child_name(path, child_path) else {
            continue;
        };
        entries.push(DirEntry {
            name,
            path: child_path.clone(),
            kind: match kind.as_str() {
                "file" => DirEntryKind::File,
                "directory" => DirEntryKind::Directory,
                other => panic!("unexpected directory entry kind {other}"),
            },
        });
    }
    if entries.is_empty() && exists.is_empty() {
        Ok(None)
    } else {
        Ok(Some(entries))
    }
}

fn direct_child_name(parent: &str, child: &str) -> Option<String> {
    let remainder = child.strip_prefix(parent)?;
    if remainder.is_empty() {
        return None;
    }
    let trimmed = remainder.trim_end_matches('/');
    if trimmed.is_empty() || trimmed.contains('/') {
        return None;
    }
    Some(trimmed.to_string())
}

#[derive(Debug, PartialEq, Eq)]
struct InstalledPluginInfo {
    key: String,
    schema_keys: Vec<String>,
}

async fn list_installed_plugins<S>(session: &S) -> Result<Vec<InstalledPluginInfo>, LixError>
where
    S: TestSession + Sync + ?Sized,
{
    let result = session
        .execute_sql(
            "SELECT data FROM lix_file \
             WHERE path LIKE '/.lix/plugins/%.lixplugin' \
             ORDER BY path",
            &[],
        )
        .await?;
    let mut plugins = Vec::new();
    for row in result.rows() {
        let [Value::Blob(data)] = row.values() else {
            panic!("expected plugin archive data row, got {:?}", row.values());
        };
        plugins.push(plugin_info_from_archive(data)?);
    }
    Ok(plugins)
}

fn plugin_info_from_archive(archive: &[u8]) -> Result<InstalledPluginInfo, LixError> {
    let mut zip = zip::ZipArchive::new(Cursor::new(archive)).map_err(test_parse_error)?;
    let mut manifest_json = String::new();
    zip.by_name("manifest.json")
        .map_err(test_parse_error)?
        .read_to_string(&mut manifest_json)
        .map_err(test_parse_error)?;
    let manifest: serde_json::Value =
        serde_json::from_str(&manifest_json).map_err(test_parse_error)?;
    let key = manifest
        .get("key")
        .and_then(|value| value.as_str())
        .expect("test plugin manifest should include key")
        .to_string();
    let schema_paths = manifest
        .get("schemas")
        .and_then(|value| value.as_array())
        .expect("test plugin manifest should include schemas");
    let mut schema_keys = Vec::new();
    for schema_path in schema_paths {
        let schema_path = schema_path
            .as_str()
            .expect("test plugin schema path should be a string");
        let mut schema_json = String::new();
        zip.by_name(schema_path)
            .map_err(test_parse_error)?
            .read_to_string(&mut schema_json)
            .map_err(test_parse_error)?;
        let schema: serde_json::Value =
            serde_json::from_str(&schema_json).map_err(test_parse_error)?;
        schema_keys.push(
            schema
                .get("x-lix-key")
                .and_then(|value| value.as_str())
                .expect("test plugin schema should include x-lix-key")
                .to_string(),
        );
    }
    Ok(InstalledPluginInfo { key, schema_keys })
}

fn test_parse_error(error: impl std::fmt::Display) -> LixError {
    LixError::new(LixError::CODE_INTERNAL_ERROR, error.to_string())
}

#[derive(Default)]
struct SentinelPluginRuntime {
    render_calls: Arc<AtomicUsize>,
    detect_filenames: Arc<Mutex<Vec<Option<String>>>>,
}

struct SentinelPluginComponent {
    render_calls: Arc<AtomicUsize>,
    detect_filenames: Arc<Mutex<Vec<Option<String>>>>,
}

#[async_trait]
impl WasmRuntime for SentinelPluginRuntime {
    async fn init_component(
        &self,
        _bytes: Vec<u8>,
        _limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
        Ok(Arc::new(SentinelPluginComponent {
            render_calls: Arc::clone(&self.render_calls),
            detect_filenames: Arc::clone(&self.detect_filenames),
        }))
    }
}

#[async_trait]
impl WasmComponentInstance for SentinelPluginComponent {
    async fn detect_changes(
        &self,
        _state: Vec<WasmPluginEntityState>,
        file: WasmPluginFile,
    ) -> Result<Vec<WasmPluginDetectedChange>, LixError> {
        self.detect_filenames
            .lock()
            .expect("detect filename lock should not be poisoned")
            .push(file.filename.clone());
        if file.data.is_empty() {
            Ok(vec![WasmPluginDetectedChange {
                entity_pk: vec!["note".to_string()],
                schema_key: "plugin_note".to_string(),
                snapshot_content: None,
                metadata: None,
            }])
        } else {
            Ok(vec![WasmPluginDetectedChange {
                entity_pk: vec!["note".to_string()],
                schema_key: "plugin_note".to_string(),
                snapshot_content: Some("{\"id\":\"note\",\"value\":\"detected\"}".to_string()),
                metadata: None,
            }])
        }
    }

    async fn render(&self, _state: Vec<WasmPluginEntityState>) -> Result<Vec<u8>, LixError> {
        self.render_calls.fetch_add(1, Ordering::SeqCst);
        Ok(b"plugin-rendered".to_vec())
    }
}

fn sentinel_plugin_archive() -> Vec<u8> {
    const MANIFEST_JSON: &[u8] = br#"{
        "key": "plugin_sentinel",
        "runtime": "wasm-component-v1",
        "api_version": "0.1.0",
        "match": { "path_glob": "*.sentinel" },
        "entry": "plugin.wasm",
        "schemas": ["schema/plugin_note.json"]
    }"#;
    plugin_archive(MANIFEST_JSON)
}

fn invalid_glob_sentinel_plugin_archive() -> Vec<u8> {
    const MANIFEST_JSON: &[u8] = br#"{
        "key": "plugin_sentinel",
        "runtime": "wasm-component-v1",
        "api_version": "0.1.0",
        "match": { "path_glob": "*.{sentinel" },
        "entry": "plugin.wasm",
        "schemas": ["schema/plugin_note.json"]
    }"#;
    plugin_archive(MANIFEST_JSON)
}

fn binary_sentinel_plugin_archive() -> Vec<u8> {
    const MANIFEST_JSON: &[u8] = br#"{
        "key": "plugin_binary_sentinel",
        "runtime": "wasm-component-v1",
        "api_version": "0.1.0",
        "match": { "path_glob": "*.binary-sentinel", "content_type": "binary" },
        "entry": "plugin.wasm",
        "schemas": ["schema/plugin_note.json"]
    }"#;
    plugin_archive(MANIFEST_JSON)
}

fn plugin_archive(manifest_json: &[u8]) -> Vec<u8> {
    const SCHEMA_JSON: &[u8] = br#"{
        "x-lix-key": "plugin_note",
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "properties": {
            "id": { "type": "string" },
            "value": { "type": "string" }
        },
        "required": ["id", "value"],
        "additionalProperties": false
    }"#;
    const WASM_HEADER: &[u8] = b"\0asm\x01\0\0\0";

    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        ("manifest.json", manifest_json),
        ("schema/plugin_note.json", SCHEMA_JSON),
        ("plugin.wasm", WASM_HEADER),
    ] {
        writer.start_file(path, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}
