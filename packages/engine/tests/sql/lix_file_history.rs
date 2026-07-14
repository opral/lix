use std::io::{Cursor, Write};
use std::sync::Arc;

use async_trait::async_trait;
use lix_engine::Value;
use lix_engine::wasm::{
    WasmComponentInstance, WasmLimits, WasmPluginDetectedChange, WasmPluginEntityState,
    WasmPluginFile, WasmRuntime,
};
use lix_engine::{Engine, LixError, Memory};
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(
    lix_file_history_reads_path_and_data_from_commit_graph,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('history-file', '/docs/guides/readme.md', X'68656C6C6F')",
                &[],
            )
            .await
            .expect("file insert should succeed");
        let first_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("first file commit head should load")
            .expect("first file commit head should exist");

        session
            .execute(
                "UPDATE lix_file \
                 SET path = '/docs/readme-renamed.md' \
                 WHERE id = 'history-file'",
                &[],
            )
            .await
            .expect("file path update should succeed");
        let second_commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("second file commit head should load")
            .expect("second file commit head should exist");

        assert_ne!(first_commit_id, second_commit_id);

        let result = session
            .execute(
                &format!(
                    "SELECT id, path, name, data, lixcol_start_commit_id, lixcol_depth \
                     FROM lix_file_history \
                     WHERE lixcol_start_commit_id = '{second_commit_id}' \
                       AND id = 'history-file' \
                       AND path LIKE '/docs/%' \
                     ORDER BY lixcol_depth"
                ),
                &[],
            )
            .await
            .expect("file history read should succeed");

        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("history-file".to_string()),
                    Value::Text("/docs/readme-renamed.md".to_string()),
                    Value::Text("readme-renamed.md".to_string()),
                    Value::Blob(b"hello".to_vec()),
                    Value::Text(second_commit_id.clone()),
                    Value::Integer(0),
                ],
                vec![
                    Value::Text("history-file".to_string()),
                    Value::Text("/docs/guides/readme.md".to_string()),
                    Value::Text("readme.md".to_string()),
                    Value::Blob(b"hello".to_vec()),
                    Value::Text(second_commit_id.clone()),
                    Value::Integer(1),
                ],
            ],
        );

        let snapshot_result = session
            .execute(
                &format!(
                    "SELECT lixcol_snapshot_content \
                     FROM lix_file_history \
                     WHERE lixcol_start_commit_id = '{second_commit_id}' \
                       AND id = 'history-file' \
                       AND lixcol_depth = 0"
                ),
                &[],
            )
            .await
            .expect("file history descriptor snapshot should be selectable");
        let snapshot = snapshot_result.rows()[0]
            .get::<Value>("lixcol_snapshot_content")
            .expect("snapshot_content should be present");
        let Value::Json(snapshot) = snapshot else {
            panic!("snapshot_content should be semantic JSON, got {snapshot:?}");
        };
        assert_eq!(snapshot["name"], json!("readme-renamed.md"));

        let result = session
            .execute(
                &format!(
                    "SELECT id \
                     FROM lix_file_history \
                     WHERE lixcol_start_commit_id = '{first_commit_id}' \
                       AND path LIKE '/missing/%'"
                ),
                &[],
            )
            .await
            .expect("file history should route start commit and leave path LIKE as residual");
        assert_rows_eq(result, Vec::<Vec<Value>>::new());
    }
);

simulation_test!(
    lix_file_history_treats_path_only_file_as_empty,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (path) VALUES ('/empty-history.txt')",
                &[],
            )
            .await
            .expect("path-only file insert should succeed");
        let commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("file commit head should load")
            .expect("file commit head should exist");

        let result = session
            .execute(
                &format!(
                    "SELECT path, data \
                     FROM lix_file_history \
                     WHERE lixcol_start_commit_id = '{commit_id}' \
                       AND path = '/empty-history.txt' \
                       AND lixcol_depth = 0"
                ),
                &[],
            )
            .await
            .expect("file history read should succeed");

        assert_rows_eq(
            result,
            vec![vec![
                Value::Text("/empty-history.txt".to_string()),
                Value::Blob(Vec::new()),
            ]],
        );
    }
);

simulation_test!(
    lix_file_history_limit_applies_after_sql_ordering,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('aaa-older-history-file', '/older.txt', X'6F6C646572')",
                &[],
            )
            .await
            .expect("older file insert should succeed");
        session
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('zzz-newer-history-file', '/newer.txt', X'6E65776572')",
                &[],
            )
            .await
            .expect("newer file insert should succeed");
        let commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("file commit head should load")
            .expect("file commit head should exist");

        let result = session
            .execute(
                &format!(
                    "SELECT id, path, lixcol_depth \
                     FROM lix_file_history \
                     WHERE lixcol_start_commit_id = '{commit_id}' \
                     ORDER BY lixcol_depth \
                     LIMIT 1"
                ),
                &[],
            )
            .await
            .expect("file history read should succeed");

        assert_rows_eq(
            result,
            vec![vec![
                Value::Text("zzz-newer-history-file".to_string()),
                Value::Text("/newer.txt".to_string()),
                Value::Integer(0),
            ]],
        );
    }
);

simulation_test!(
    lix_file_history_limit_applies_after_residual_path_filters,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES \
                    ('aaa-history-noise-1', '/noise/one.txt', X'6F6E65'), \
                    ('aaa-history-noise-2', '/noise/two.txt', X'74776F'), \
                    ('zzz-history-target', '/target/three.txt', X'7468726565')",
                &[],
            )
            .await
            .expect("file inserts should succeed");
        let commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("file commit head should load")
            .expect("file commit head should exist");

        let result = session
            .execute(
                &format!(
                    "SELECT id, path, data \
                     FROM lix_file_history \
                     WHERE lixcol_start_commit_id = '{commit_id}' \
                       AND path LIKE '/target/%' \
                     LIMIT 1"
                ),
                &[],
            )
            .await
            .expect("file history read should succeed");

        assert_rows_eq(
            result,
            vec![vec![
                Value::Text("zzz-history-target".to_string()),
                Value::Text("/target/three.txt".to_string()),
                Value::Blob(b"three".to_vec()),
            ]],
        );
    }
);

#[tokio::test]
async fn lix_file_history_renders_plugin_state_at_each_depth() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new_with_wasm_runtime(storage, Arc::new(HistoryRenderPluginRuntime))
        .await
        .expect("engine should open with plugin runtime");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    session
        .execute(
            "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
            &[
                Value::Text("/.lix/plugins/plugin_history_render.lixplugin".to_string()),
                Value::Blob(history_render_plugin_archive()),
            ],
        )
        .await
        .expect("plugin archive write should install plugin");
    session
        .execute(
            "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
            &[
                Value::Text("/note.history-render".to_string()),
                Value::Blob(b"first".to_vec()),
            ],
        )
        .await
        .expect("plugin file write should succeed");
    session
        .execute(
            "UPDATE lix_file SET data = $1 WHERE path = $2",
            &[
                Value::Blob(b"second".to_vec()),
                Value::Text("/note.history-render".to_string()),
            ],
        )
        .await
        .expect("plugin file update should succeed");
    session
        .execute(
            "INSERT INTO lix_key_value (key, value) \
             VALUES ('history-render-sidecar', 'newer non-file commit')",
            &[],
        )
        .await
        .expect("non-file commit should succeed");

    let commit_id_rows = session
        .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await
        .expect("active branch commit id should load");
    let [Value::Text(commit_id)] = commit_id_rows.rows()[0].values() else {
        panic!(
            "expected active branch commit id row, got {:?}",
            commit_id_rows.rows()[0].values()
        );
    };
    let file_id_rows = session
        .execute(
            "SELECT id FROM lix_file WHERE path = '/note.history-render'",
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

    let result = session
        .execute(
            &format!(
                "SELECT path, data, lixcol_depth \
                 FROM lix_file_history \
                 WHERE lixcol_start_commit_id = '{commit_id}' \
                   AND id = '{file_id}' \
                 ORDER BY lixcol_depth \
                 LIMIT 2"
            ),
            &[],
        )
        .await
        .expect("plugin file history read should succeed");

    assert_rows_eq(
        result,
        vec![
            vec![
                Value::Text("/note.history-render".to_string()),
                Value::Blob(b"rendered:second-a|second-b".to_vec()),
                Value::Integer(1),
            ],
            vec![
                Value::Text("/note.history-render".to_string()),
                Value::Blob(b"rendered:first-a|first-b".to_vec()),
                Value::Integer(2),
            ],
        ],
    );

    session.close().await.expect("session should close");
}

simulation_test!(
    lix_file_history_requires_start_commit_id,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let error = session
            .execute("SELECT id FROM lix_file_history", &[])
            .await
            .expect_err("file history queries must provide start commit");

        assert!(
            error
                .to_string()
                .contains("requires a lixcol_start_commit_id filter"),
            "unexpected error: {error}"
        );
        assert!(
            error
                .hint()
                .is_some_and(|hint| hint.contains("WHERE lixcol_start_commit_id")),
            "unexpected error: {error}"
        );
    }
);

simulation_test!(
    lix_file_history_ignores_unrelated_file_scoped_state_events,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('ordinary-history-file', '/ordinary-history.txt', X'68656C6C6F')",
                &[],
            )
            .await
            .expect("file insert should succeed");
        session
            .execute(
                "INSERT INTO lix_state (entity_pk, schema_key, file_id, snapshot_content) \
                 VALUES (lix_json('[\"ordinary-sidecar\"]'), 'lix_key_value', \
                         'ordinary-history-file', \
                         lix_json('{\"key\":\"ordinary-sidecar\",\"value\":\"noise\"}'))",
                &[],
            )
            .await
            .expect("unrelated file-scoped state insert should succeed");
        let commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("head commit should load")
            .expect("head commit should exist");

        let result = session
            .execute(
                &format!(
                    "SELECT path, data, lixcol_depth \
                     FROM lix_file_history \
                     WHERE lixcol_start_commit_id = '{commit_id}' \
                       AND id = 'ordinary-history-file' \
                     ORDER BY lixcol_depth"
                ),
                &[],
            )
            .await
            .expect("file history read should succeed");

        assert_rows_eq(
            result,
            vec![vec![
                Value::Text("/ordinary-history.txt".to_string()),
                Value::Blob(b"hello".to_vec()),
                Value::Integer(1),
            ]],
        );
    }
);

struct HistoryRenderPluginRuntime;

struct HistoryRenderPluginComponent;

#[async_trait]
impl WasmRuntime for HistoryRenderPluginRuntime {
    async fn init_component(
        &self,
        _bytes: Vec<u8>,
        _limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
        Ok(Arc::new(HistoryRenderPluginComponent))
    }
}

#[async_trait]
impl WasmComponentInstance for HistoryRenderPluginComponent {
    async fn detect_changes(
        &self,
        _state: Vec<WasmPluginEntityState>,
        file: WasmPluginFile,
    ) -> Result<Vec<WasmPluginDetectedChange>, LixError> {
        let value = String::from_utf8(file.data).map_err(|error| {
            LixError::unknown(format!("plugin test data was not UTF-8: {error}"))
        })?;
        Ok(["a", "b"]
            .into_iter()
            .map(|suffix| WasmPluginDetectedChange {
                entity_pk: vec![format!("note-{suffix}")],
                schema_key: "plugin_history_note".to_string(),
                snapshot_content: Some(format!(
                    "{{\"id\":\"note-{suffix}\",\"value\":{}}}",
                    serde_json::to_string(&format!("{value}-{suffix}"))
                        .expect("test value should serialize")
                )),
                metadata: None,
            })
            .collect())
    }

    async fn render(&self, state: Vec<WasmPluginEntityState>) -> Result<Vec<u8>, LixError> {
        let mut values = state
            .iter()
            .filter(|row| row.schema_key == "plugin_history_note")
            .filter_map(|row| serde_json::from_str::<serde_json::Value>(&row.snapshot_content).ok())
            .filter_map(|snapshot| {
                snapshot
                    .get("value")
                    .and_then(|value| value.as_str())
                    .map(ToOwned::to_owned)
            })
            .collect::<Vec<_>>();
        values.sort();
        Ok(format!("rendered:{}", values.join("|")).into_bytes())
    }
}

fn history_render_plugin_archive() -> Vec<u8> {
    const MANIFEST_JSON: &[u8] = br#"{
        "key": "plugin_history_render",
        "runtime": "wasm-component-v1",
        "api_version": "0.1.0",
        "match": { "path_glob": "*.history-render" },
        "entry": "plugin.wasm",
        "schemas": ["schema/plugin_history_note.json"]
    }"#;
    const SCHEMA_JSON: &[u8] = br#"{
        "x-lix-key": "plugin_history_note",
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
        ("manifest.json", MANIFEST_JSON),
        ("schema/plugin_history_note.json", SCHEMA_JSON),
        ("plugin.wasm", WASM_HEADER),
    ] {
        writer.start_file(path, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

simulation_test!(
    lix_file_history_exposes_file_descriptor_schema_key,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('history-file-blob-filter', '/docs/blob-filter.txt', X'626C6F62')",
                &[],
            )
            .await
            .expect("file insert should succeed");
        session
            .execute(
                "UPDATE lix_file SET data = X'626C6F6232' \
                 WHERE id = 'history-file-blob-filter'",
                &[],
            )
            .await
            .expect("file data update should succeed");
        let commit_id = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("file commit head should load")
            .expect("file commit head should exist");

        let result = session
            .execute(
                &format!(
                    "SELECT id, path, data, lixcol_schema_key \
                     FROM lix_file_history \
                     WHERE lixcol_start_commit_id = '{commit_id}' \
                       AND lixcol_schema_key = 'lix_file_descriptor' \
                       AND id = 'history-file-blob-filter' \
                       AND lixcol_depth = 0"
                ),
                &[],
            )
            .await
            .expect("file-descriptor-filtered file history read should succeed");

        assert_rows_eq(
            result,
            vec![vec![
                Value::Text("history-file-blob-filter".to_string()),
                Value::Text("/docs/blob-filter.txt".to_string()),
                Value::Blob(b"blob2".to_vec()),
                Value::Text("lix_file_descriptor".to_string()),
            ]],
        );

        let blob_schema_result = session
            .execute(
                &format!(
                    "SELECT id \
                     FROM lix_file_history \
                     WHERE lixcol_start_commit_id = '{commit_id}' \
                       AND lixcol_schema_key = 'lix_binary_blob_ref' \
                       AND id = 'history-file-blob-filter'"
                ),
                &[],
            )
            .await
            .expect("blob-ref-filtered file history read should succeed");
        assert_rows_eq(blob_schema_result, Vec::<Vec<Value>>::new());
    }
);
