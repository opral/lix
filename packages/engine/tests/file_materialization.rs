mod support;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, OnceLock};

use lix_engine::{
    MaterializationDebugMode, MaterializationRequest, MaterializationScope, Value, WasmRuntime,
};
use serde_json::Value as JsonValue;

const TEST_PLUGIN_MANIFEST: &str = r#"{
  "key": "json",
  "runtime": "wasm-component-v1",
  "api_version": "0.1.0",
  "detect_changes_glob": "*.json",
  "entry": "plugin.wasm"
}"#;

fn plugin_json_v2_manifest_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../plugin-json-v2")
        .join("Cargo.toml")
}

fn plugin_json_v2_wasm_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../plugin-json-v2")
        .join("target/wasm32-wasip2/debug/plugin_json_v2.wasm")
}

fn ensure_wasm32_wasip2_target() -> Result<(), String> {
    let status = Command::new("rustup")
        .arg("target")
        .arg("add")
        .arg("wasm32-wasip2")
        .status()
        .map_err(|error| format!("failed to run rustup target add wasm32-wasip2: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "rustup target add wasm32-wasip2 failed with status {status}"
        ))
    }
}

fn build_plugin_json_v2_wasm(manifest_path: &Path) -> Result<(), String> {
    let output = Command::new("cargo")
        .arg("build")
        .arg("--manifest-path")
        .arg(manifest_path)
        .arg("--target")
        .arg("wasm32-wasip2")
        .output()
        .map_err(|error| format!("failed to run cargo build for plugin_json_v2: {error}"))?;

    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if stderr.contains("wasm32-wasip2")
        && (stderr.contains("target may not be installed")
            || stderr.contains("can't find crate for `core`"))
    {
        ensure_wasm32_wasip2_target()?;

        let retry = Command::new("cargo")
            .arg("build")
            .arg("--manifest-path")
            .arg(manifest_path)
            .arg("--target")
            .arg("wasm32-wasip2")
            .output()
            .map_err(|error| format!("failed to rerun cargo build for plugin_json_v2: {error}"))?;

        if retry.status.success() {
            return Ok(());
        }

        let retry_stderr = String::from_utf8_lossy(&retry.stderr);
        return Err(format!(
            "cargo build for plugin_json_v2 failed after installing target:\n{retry_stderr}"
        ));
    }

    Err(format!("cargo build for plugin_json_v2 failed:\n{stderr}"))
}

fn plugin_json_v2_wasm_bytes() -> Vec<u8> {
    static WASM_BYTES: OnceLock<Vec<u8>> = OnceLock::new();
    WASM_BYTES
        .get_or_init(|| {
            let manifest_path = plugin_json_v2_manifest_path();
            let wasm_path = plugin_json_v2_wasm_path();

            if !wasm_path.exists() {
                build_plugin_json_v2_wasm(&manifest_path).unwrap_or_else(|error| panic!("{error}"));
            }

            fs::read(&wasm_path).unwrap_or_else(|error| {
                panic!(
                    "failed to read plugin_json_v2 wasm at {}: {error}",
                    wasm_path.display()
                )
            })
        })
        .clone()
}

async fn register_plugin_schema(engine: &support::simulation_test::SimulationEngine) {
    engine
        .execute(
            "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES (\
             'lix_stored_schema',\
             '{\"value\":{\"x-lix-key\":\"json_pointer\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"path\":{\"type\":\"string\"},\"value\":{}},\"required\":[\"path\",\"value\"],\"additionalProperties\":false}}'\
             )",
            &[],
        )
        .await
        .expect("register plugin schema should succeed");
}

async fn main_version_id(engine: &support::simulation_test::SimulationEngine) -> String {
    let rows = engine
        .execute(
            "SELECT id FROM lix_version WHERE name = 'main' LIMIT 1",
            &[],
        )
        .await
        .expect("main version query should succeed");
    assert_eq!(rows.rows.len(), 1);
    match &rows.rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected main version id text, got {other:?}"),
    }
}

async fn boot_engine_with_json_plugin(
    sim: &support::simulation_test::SimulationArgs,
) -> (support::simulation_test::SimulationEngine, String) {
    let runtime = Arc::new(
        support::wasmtime_runtime::TestWasmtimeRuntime::new()
            .expect("test wasmtime runtime should initialize"),
    ) as Arc<dyn WasmRuntime>;

    let engine = sim
        .boot_simulated_engine(Some(support::simulation_test::SimulationBootArgs {
            key_values: Vec::new(),
            active_account: None,
            wasm_runtime: Some(runtime),
        }))
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.expect("engine init should succeed");
    register_plugin_schema(&engine).await;
    let main_version_id = main_version_id(&engine).await;
    let plugin_wasm = plugin_json_v2_wasm_bytes();
    engine
        .install_plugin(TEST_PLUGIN_MANIFEST, &plugin_wasm)
        .await
        .expect("install_plugin should succeed");
    (engine, main_version_id)
}

async fn materialize_full(engine: &support::simulation_test::SimulationEngine) {
    engine
        .materialize(&MaterializationRequest {
            scope: MaterializationScope::Full,
            debug: MaterializationDebugMode::Off,
            debug_row_limit: 1,
        })
        .await
        .expect("materialize should succeed");
}

async fn detected_json_pointer_entities(
    engine: &support::simulation_test::SimulationEngine,
    file_id: &str,
    version_id: &str,
) -> Vec<String> {
    let rows = engine
        .execute(
            &format!(
                "SELECT entity_id \
                 FROM lix_state_by_version \
                 WHERE file_id = '{}' \
                   AND version_id = '{}' \
                   AND schema_key = 'json_pointer' \
                 ORDER BY entity_id",
                file_id, version_id
            ),
            &[],
        )
        .await
        .expect("detected json_pointer query should succeed");
    rows.rows
        .iter()
        .map(|row| match &row[0] {
            Value::Text(value) => value.clone(),
            other => panic!("expected entity_id text, got {other:?}"),
        })
        .collect::<Vec<_>>()
}

fn assert_blob_json_eq(value: &Value, expected: JsonValue) {
    let bytes = match value {
        Value::Blob(bytes) => bytes,
        other => panic!("expected blob value, got {other:?}"),
    };
    let actual: JsonValue = serde_json::from_slice(bytes).expect("blob should contain valid JSON");
    assert_eq!(actual, expected);
}

fn value_as_i64(value: &Value) -> i64 {
    match value {
        Value::Integer(v) => *v,
        other => panic!("expected integer value, got {other:?}"),
    }
}

async fn file_cache_row_count(
    engine: &support::simulation_test::SimulationEngine,
    file_id: &str,
    version_id: &str,
) -> i64 {
    let rows = engine
        .execute(
            &format!(
                "SELECT COUNT(*) \
                 FROM lix_internal_file_data_cache \
                 WHERE file_id = '{}' AND version_id = '{}'",
                file_id, version_id
            ),
            &[],
        )
        .await
        .expect("file_data_cache count query should succeed");
    assert_eq!(rows.rows.len(), 1);
    value_as_i64(&rows.rows[0][0])
}

async fn file_descriptor_tombstone_count(
    engine: &support::simulation_test::SimulationEngine,
    file_id: &str,
    version_id: &str,
) -> i64 {
    let rows = engine
        .execute(
            &format!(
                "SELECT COUNT(*) \
                 FROM lix_internal_state_materialized_v1_lix_file_descriptor \
                 WHERE entity_id = '{}' \
                   AND version_id = '{}' \
                   AND is_tombstone = 1",
                file_id, version_id
            ),
            &[],
        )
        .await
        .expect("file descriptor tombstone count query should succeed");
    assert_eq!(rows.rows.len(), 1);
    value_as_i64(&rows.rows[0][0])
}

simulation_test!(
    materialize_applies_real_json_plugin_and_persists_file_data_cache,
    simulations = [sqlite, postgres],
    |sim| async move {
        let runtime = Arc::new(
            support::wasmtime_runtime::TestWasmtimeRuntime::new()
                .expect("test wasmtime runtime should initialize"),
        ) as Arc<dyn WasmRuntime>;

        let engine = sim
            .boot_simulated_engine(Some(support::simulation_test::SimulationBootArgs {
                key_values: Vec::new(),
                active_account: None,
                wasm_runtime: Some(runtime),
            }))
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.expect("engine init should succeed");
        register_plugin_schema(&engine).await;
        let main_version_id = main_version_id(&engine).await;
        let plugin_wasm = plugin_json_v2_wasm_bytes();

        engine
            .install_plugin(TEST_PLUGIN_MANIFEST, &plugin_wasm)
            .await
            .expect("install_plugin should succeed");

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_state_by_version (\
                     entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                     ) VALUES (\
                     'file-1', 'lix_file_descriptor', 'lix', '{}', 'lix', '{{\"id\":\"file-1\",\"directory_id\":null,\"name\":\"config\",\"extension\":\"json\",\"metadata\":null,\"hidden\":false}}', '1'\
                     )",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("insert file descriptor should succeed");

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_state_by_version (\
                     entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                     ) VALUES \
                     ('', 'json_pointer', 'file-1', '{}', 'json', '{{\"path\":\"\",\"value\":{{}}}}', '1'), \
                     ('/value', 'json_pointer', 'file-1', '{}', 'json', '{{\"path\":\"/value\",\"value\":\"A\"}}', '1')",
                    main_version_id, main_version_id
                ),
                &[],
            )
            .await
            .expect("insert plugin state should succeed");

        engine
            .materialize(&MaterializationRequest {
                scope: MaterializationScope::Full,
                debug: MaterializationDebugMode::Off,
                debug_row_limit: 1,
            })
            .await
            .expect("materialize should succeed");

        let cache_rows = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_internal_file_data_cache \
                     WHERE file_id = 'file-1' AND version_id = '{}' \
                     LIMIT 1",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("file_data_cache query should succeed");
        assert_eq!(cache_rows.rows.len(), 1);
        assert_blob_json_eq(&cache_rows.rows[0][0], serde_json::json!({"value":"A"}));

        let file_rows = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_file_by_version \
                     WHERE id = 'file-1' AND lixcol_version_id = '{}' \
                     LIMIT 1",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("lix_file_by_version query should succeed");
        assert_eq!(file_rows.rows.len(), 1);
        assert_blob_json_eq(&file_rows.rows[0][0], serde_json::json!({"value":"A"}));
    }
);

simulation_test!(
    file_insert_json_detects_changes_with_real_plugin_and_materializes,
    simulations = [sqlite, postgres],
    |sim| async move {
        let runtime = Arc::new(
            support::wasmtime_runtime::TestWasmtimeRuntime::new()
                .expect("test wasmtime runtime should initialize"),
        ) as Arc<dyn WasmRuntime>;

        let engine = sim
            .boot_simulated_engine(Some(support::simulation_test::SimulationBootArgs {
                key_values: Vec::new(),
                active_account: None,
                wasm_runtime: Some(runtime),
            }))
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.expect("engine init should succeed");
        register_plugin_schema(&engine).await;
        let main_version_id = main_version_id(&engine).await;
        let plugin_wasm = plugin_json_v2_wasm_bytes();

        engine
            .install_plugin(TEST_PLUGIN_MANIFEST, &plugin_wasm)
            .await
            .expect("install_plugin should succeed");

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json', '/config.json', '{\"hello\":\"from-write\"}')",
                &[],
            )
            .await
            .expect("file insert should succeed");

        let detected_changes = engine
            .execute(
                &format!(
                    "SELECT entity_id, schema_key, plugin_key \
                     FROM lix_state_by_version \
                     WHERE file_id = 'file-json' \
                       AND version_id = '{}' \
                       AND schema_key = 'json_pointer' \
                     ORDER BY entity_id",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("detected plugin changes query should succeed");
        assert_eq!(detected_changes.rows.len(), 2);
        assert_eq!(detected_changes.rows[0][0], Value::Text("".to_string()));
        assert_eq!(
            detected_changes.rows[0][1],
            Value::Text("json_pointer".to_string())
        );
        assert_eq!(detected_changes.rows[0][2], Value::Text("json".to_string()));
        assert_eq!(
            detected_changes.rows[1][0],
            Value::Text("/hello".to_string())
        );
        assert_eq!(
            detected_changes.rows[1][1],
            Value::Text("json_pointer".to_string())
        );
        assert_eq!(detected_changes.rows[1][2], Value::Text("json".to_string()));

        engine
            .materialize(&MaterializationRequest {
                scope: MaterializationScope::Full,
                debug: MaterializationDebugMode::Off,
                debug_row_limit: 1,
            })
            .await
            .expect("materialize should succeed");

        let file_rows = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'file-json' LIMIT 1",
                &[],
            )
            .await
            .expect("lix_file query should succeed");
        assert_eq!(file_rows.rows.len(), 1);
        assert_blob_json_eq(
            &file_rows.rows[0][0],
            serde_json::json!({"hello":"from-write"}),
        );
    }
);

simulation_test!(
    file_update_json_detects_tombstones_and_refreshes_materialized_data,
    simulations = [sqlite, postgres],
    |sim| async move {
        let runtime = Arc::new(
            support::wasmtime_runtime::TestWasmtimeRuntime::new()
                .expect("test wasmtime runtime should initialize"),
        ) as Arc<dyn WasmRuntime>;

        let engine = sim
            .boot_simulated_engine(Some(support::simulation_test::SimulationBootArgs {
                key_values: Vec::new(),
                active_account: None,
                wasm_runtime: Some(runtime),
            }))
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.expect("engine init should succeed");
        register_plugin_schema(&engine).await;
        let main_version_id = main_version_id(&engine).await;
        let plugin_wasm = plugin_json_v2_wasm_bytes();

        engine
            .install_plugin(TEST_PLUGIN_MANIFEST, &plugin_wasm)
            .await
            .expect("install_plugin should succeed");

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json-update', '/config.json', '{\"hello\":\"before\",\"remove\":\"soon-gone\"}')",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

        engine
            .execute(
                "UPDATE lix_file \
                 SET data = '{\"hello\":\"after\",\"add\":\"new-value\"}' \
                 WHERE id = 'file-json-update'",
                &[],
            )
            .await
            .expect("file update should succeed");

        let detected_changes = engine
            .execute(
                &format!(
                    "SELECT entity_id, snapshot_content \
                     FROM lix_state_by_version \
                     WHERE file_id = 'file-json-update' \
                       AND version_id = '{}' \
                       AND schema_key = 'json_pointer' \
                     ORDER BY entity_id",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("detected plugin changes query should succeed");
        let detected_entities = detected_changes
            .rows
            .iter()
            .map(|row| match &row[0] {
                Value::Text(value) => value.clone(),
                other => panic!("expected entity_id text, got {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            detected_entities,
            vec!["".to_string(), "/add".to_string(), "/hello".to_string()]
        );

        engine
            .materialize(&MaterializationRequest {
                scope: MaterializationScope::Full,
                debug: MaterializationDebugMode::Off,
                debug_row_limit: 1,
            })
            .await
            .expect("materialize should succeed");

        let cache_rows = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_internal_file_data_cache \
                     WHERE file_id = 'file-json-update' AND version_id = '{}' \
                     LIMIT 1",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("file_data_cache query should succeed");
        assert_eq!(cache_rows.rows.len(), 1);
        assert_blob_json_eq(
            &cache_rows.rows[0][0],
            serde_json::json!({"hello":"after","add":"new-value"}),
        );
    }
);

simulation_test!(
    file_update_json_parameterized_detects_and_materializes,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json-param', '/config.json', '{\"hello\":\"before\",\"drop\":\"gone\"}')",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

        engine
            .execute(
                "UPDATE lix_file SET data = $1 WHERE id = $2",
                &[
                    Value::Text("{\"hello\":\"after-param\",\"new\":1}".to_string()),
                    Value::Text("file-json-param".to_string()),
                ],
            )
            .await
            .expect("parameterized file update should succeed");

        let detected =
            detected_json_pointer_entities(&engine, "file-json-param", &main_version_id).await;
        assert_eq!(
            detected,
            vec!["".to_string(), "/hello".to_string(), "/new".to_string()]
        );

        materialize_full(&engine).await;

        let cache_rows = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_internal_file_data_cache \
                     WHERE file_id = 'file-json-param' AND version_id = '{}' \
                     LIMIT 1",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("file_data_cache query should succeed");
        assert_eq!(cache_rows.rows.len(), 1);
        assert_blob_json_eq(
            &cache_rows.rows[0][0],
            serde_json::json!({"hello":"after-param","new":1}),
        );
    }
);

simulation_test!(
    direct_state_insert_refreshes_file_data_cache,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json-state-insert-cache', '/state-insert-cache.json', '{\"content\":\"Start\"}')",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

        materialize_full(&engine).await;

        let before_rows = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'file-json-state-insert-cache' LIMIT 1",
                &[],
            )
            .await
            .expect("file read before state insert should succeed");
        assert_eq!(before_rows.rows.len(), 1);
        assert_blob_json_eq(
            &before_rows.rows[0][0],
            serde_json::json!({"content":"Start"}),
        );

        engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, file_id, schema_key, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 '/extra', 'file-json-state-insert-cache', 'json_pointer', 'json', '1', '{\"path\":\"/extra\",\"value\":\"Add\"}'\
                 )",
                &[],
            )
            .await
            .expect("direct state insert should succeed");

        let after_rows = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'file-json-state-insert-cache' LIMIT 1",
                &[],
            )
            .await
            .expect("file read after state insert should succeed");
        assert_eq!(after_rows.rows.len(), 1);
        assert_blob_json_eq(
            &after_rows.rows[0][0],
            serde_json::json!({"content":"Start","extra":"Add"}),
        );

        let cache_rows = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_internal_file_data_cache \
                     WHERE file_id = 'file-json-state-insert-cache' AND version_id = '{}' \
                     LIMIT 1",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("file_data_cache query should succeed");
        assert_eq!(cache_rows.rows.len(), 1);
        assert_blob_json_eq(
            &cache_rows.rows[0][0],
            serde_json::json!({"content":"Start","extra":"Add"}),
        );
    }
);

simulation_test!(
    direct_state_update_refreshes_file_data_cache,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json-state-cache', '/state-cache.json', '{\"content\":\"Start\"}')",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

        materialize_full(&engine).await;

        let before_rows = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'file-json-state-cache' LIMIT 1",
                &[],
            )
            .await
            .expect("file read before state update should succeed");
        assert_eq!(before_rows.rows.len(), 1);
        assert_blob_json_eq(
            &before_rows.rows[0][0],
            serde_json::json!({"content":"Start"}),
        );

        engine
            .execute(
                "UPDATE lix_state \
                 SET snapshot_content = '{\"path\":\"/content\",\"value\":\"New\"}' \
                 WHERE file_id = 'file-json-state-cache' \
                   AND schema_key = 'json_pointer' \
                   AND entity_id = '/content'",
                &[],
            )
            .await
            .expect("direct state update should succeed");

        let after_rows = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'file-json-state-cache' LIMIT 1",
                &[],
            )
            .await
            .expect("file read after state update should succeed");
        assert_eq!(after_rows.rows.len(), 1);
        assert_blob_json_eq(&after_rows.rows[0][0], serde_json::json!({"content":"New"}));

        let cache_rows = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_internal_file_data_cache \
                     WHERE file_id = 'file-json-state-cache' AND version_id = '{}' \
                     LIMIT 1",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("file_data_cache query should succeed");
        assert_eq!(cache_rows.rows.len(), 1);
        assert_blob_json_eq(&cache_rows.rows[0][0], serde_json::json!({"content":"New"}));
    }
);

simulation_test!(
    direct_state_delete_refreshes_file_data_cache,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json-state-delete-cache', '/state-delete-cache.json', '{\"content\":\"Start\"}')",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

        materialize_full(&engine).await;

        let before_rows = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'file-json-state-delete-cache' LIMIT 1",
                &[],
            )
            .await
            .expect("file read before state delete should succeed");
        assert_eq!(before_rows.rows.len(), 1);
        assert_blob_json_eq(
            &before_rows.rows[0][0],
            serde_json::json!({"content":"Start"}),
        );

        engine
            .execute(
                "DELETE FROM lix_state \
                 WHERE file_id = 'file-json-state-delete-cache' \
                   AND schema_key = 'json_pointer' \
                   AND entity_id = '/content'",
                &[],
            )
            .await
            .expect("direct state delete should succeed");

        let after_rows = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'file-json-state-delete-cache' LIMIT 1",
                &[],
            )
            .await
            .expect("file read after state delete should succeed");
        assert_eq!(after_rows.rows.len(), 1);
        assert_blob_json_eq(&after_rows.rows[0][0], serde_json::json!({}));

        let cache_rows = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_internal_file_data_cache \
                     WHERE file_id = 'file-json-state-delete-cache' AND version_id = '{}' \
                     LIMIT 1",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("file_data_cache query should succeed");
        assert_eq!(cache_rows.rows.len(), 1);
        assert_blob_json_eq(&cache_rows.rows[0][0], serde_json::json!({}));
    }
);

simulation_test!(
    direct_state_by_version_insert_refreshes_file_data_cache,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json-state-by-version-insert-cache', '/state-by-version-insert-cache.json', '{\"content\":\"Start\"}')",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

        materialize_full(&engine).await;

        let before_rows = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_file_by_version \
                     WHERE id = 'file-json-state-by-version-insert-cache' \
                       AND lixcol_version_id = '{}' \
                     LIMIT 1",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("file_by_version read before state insert should succeed");
        assert_eq!(before_rows.rows.len(), 1);
        assert_blob_json_eq(
            &before_rows.rows[0][0],
            serde_json::json!({"content":"Start"}),
        );

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_state_by_version (\
                     entity_id, file_id, version_id, schema_key, plugin_key, schema_version, snapshot_content\
                     ) VALUES (\
                     '/extra', 'file-json-state-by-version-insert-cache', '{}', 'json_pointer', 'json', '1', '{{\"path\":\"/extra\",\"value\":\"Add\"}}'\
                     )",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("direct state_by_version insert should succeed");

        let after_rows = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_file_by_version \
                     WHERE id = 'file-json-state-by-version-insert-cache' \
                       AND lixcol_version_id = '{}' \
                     LIMIT 1",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("file_by_version read after state insert should succeed");
        assert_eq!(after_rows.rows.len(), 1);
        assert_blob_json_eq(
            &after_rows.rows[0][0],
            serde_json::json!({"content":"Start","extra":"Add"}),
        );

        let cache_rows = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_internal_file_data_cache \
                     WHERE file_id = 'file-json-state-by-version-insert-cache' \
                       AND version_id = '{}' \
                     LIMIT 1",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("file_data_cache query should succeed");
        assert_eq!(cache_rows.rows.len(), 1);
        assert_blob_json_eq(
            &cache_rows.rows[0][0],
            serde_json::json!({"content":"Start","extra":"Add"}),
        );
    }
);

simulation_test!(
    direct_state_by_version_update_refreshes_file_data_cache,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json-state-by-version-cache', '/state-by-version-cache.json', '{\"content\":\"Start\"}')",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

        materialize_full(&engine).await;

        let before_rows = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_file_by_version \
                     WHERE id = 'file-json-state-by-version-cache' \
                       AND lixcol_version_id = '{}' \
                     LIMIT 1",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("file_by_version read before state update should succeed");
        assert_eq!(before_rows.rows.len(), 1);
        assert_blob_json_eq(
            &before_rows.rows[0][0],
            serde_json::json!({"content":"Start"}),
        );

        engine
            .execute(
                &format!(
                    "UPDATE lix_state_by_version \
                     SET snapshot_content = '{{\"path\":\"/content\",\"value\":\"New\"}}' \
                     WHERE file_id = 'file-json-state-by-version-cache' \
                       AND version_id = '{}' \
                       AND schema_key = 'json_pointer' \
                       AND entity_id = '/content'",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("direct state_by_version update should succeed");

        let after_rows = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_file_by_version \
                     WHERE id = 'file-json-state-by-version-cache' \
                       AND lixcol_version_id = '{}' \
                     LIMIT 1",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("file_by_version read after state update should succeed");
        assert_eq!(after_rows.rows.len(), 1);
        assert_blob_json_eq(&after_rows.rows[0][0], serde_json::json!({"content":"New"}));

        let cache_rows = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_internal_file_data_cache \
                     WHERE file_id = 'file-json-state-by-version-cache' \
                       AND version_id = '{}' \
                     LIMIT 1",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("file_data_cache query should succeed");
        assert_eq!(cache_rows.rows.len(), 1);
        assert_blob_json_eq(&cache_rows.rows[0][0], serde_json::json!({"content":"New"}));
    }
);

simulation_test!(
    direct_state_by_version_delete_refreshes_file_data_cache,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json-state-by-version-delete-cache', '/state-by-version-delete-cache.json', '{\"content\":\"Start\"}')",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

        materialize_full(&engine).await;

        let before_rows = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_file_by_version \
                     WHERE id = 'file-json-state-by-version-delete-cache' \
                       AND lixcol_version_id = '{}' \
                     LIMIT 1",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("file_by_version read before state delete should succeed");
        assert_eq!(before_rows.rows.len(), 1);
        assert_blob_json_eq(
            &before_rows.rows[0][0],
            serde_json::json!({"content":"Start"}),
        );

        engine
            .execute(
                &format!(
                    "DELETE FROM lix_state_by_version \
                     WHERE file_id = 'file-json-state-by-version-delete-cache' \
                       AND version_id = '{}' \
                       AND schema_key = 'json_pointer' \
                       AND entity_id = '/content'",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("direct state_by_version delete should succeed");

        let after_rows = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_file_by_version \
                     WHERE id = 'file-json-state-by-version-delete-cache' \
                       AND lixcol_version_id = '{}' \
                     LIMIT 1",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("file_by_version read after state delete should succeed");
        assert_eq!(after_rows.rows.len(), 1);
        assert_blob_json_eq(&after_rows.rows[0][0], serde_json::json!({}));

        let cache_rows = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_internal_file_data_cache \
                     WHERE file_id = 'file-json-state-by-version-delete-cache' \
                       AND version_id = '{}' \
                     LIMIT 1",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("file_data_cache query should succeed");
        assert_eq!(cache_rows.rows.len(), 1);
        assert_blob_json_eq(&cache_rows.rows[0][0], serde_json::json!({}));
    }
);

simulation_test!(
    file_update_json_with_path_and_data_detects_and_materializes,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json-path', '/before.json', '{\"hello\":\"before\",\"remove\":true}')",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

        engine
            .execute(
                "UPDATE lix_file \
                 SET path = '/after.json', data = '{\"hello\":\"after-path\"}' \
                 WHERE id = 'file-json-path'",
                &[],
            )
            .await
            .expect("path+data update should succeed");

        let detected =
            detected_json_pointer_entities(&engine, "file-json-path", &main_version_id).await;
        assert_eq!(detected, vec!["".to_string(), "/hello".to_string()]);

        let file_rows = engine
            .execute(
                "SELECT path FROM lix_file WHERE id = 'file-json-path' LIMIT 1",
                &[],
            )
            .await
            .expect("file path query should succeed");
        assert_eq!(file_rows.rows.len(), 1);
        assert_eq!(file_rows.rows[0][0], Value::Text("/after.json".to_string()));

        materialize_full(&engine).await;

        let cache_rows = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_internal_file_data_cache \
                     WHERE file_id = 'file-json-path' AND version_id = '{}' \
                     LIMIT 1",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("file_data_cache query should succeed");
        assert_eq!(cache_rows.rows.len(), 1);
        assert_blob_json_eq(
            &cache_rows.rows[0][0],
            serde_json::json!({"hello":"after-path"}),
        );
    }
);

simulation_test!(
    file_update_json_by_version_detects_and_materializes,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
                     VALUES ('file-json-by-version', '/config.json', '{{\"hello\":\"before\",\"remove\":\"gone\"}}', '{}')",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("initial by-version file insert should succeed");

        engine
            .execute(
                &format!(
                    "UPDATE lix_file_by_version \
                     SET data = '{{\"hello\":\"after-by-version\",\"add\":\"v\"}}' \
                     WHERE id = 'file-json-by-version' \
                       AND lixcol_version_id = '{}'",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("by-version file update should succeed");

        let detected =
            detected_json_pointer_entities(&engine, "file-json-by-version", &main_version_id).await;
        assert_eq!(
            detected,
            vec!["".to_string(), "/add".to_string(), "/hello".to_string()]
        );

        materialize_full(&engine).await;

        let cache_rows = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_internal_file_data_cache \
                     WHERE file_id = 'file-json-by-version' AND version_id = '{}' \
                     LIMIT 1",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("file_data_cache query should succeed");
        assert_eq!(cache_rows.rows.len(), 1);
        assert_blob_json_eq(
            &cache_rows.rows[0][0],
            serde_json::json!({"hello":"after-by-version","add":"v"}),
        );
    }
);

simulation_test!(
    file_update_json_multi_row_update_detects_each_file,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES \
                 ('file-json-bulk-1', '/bulk-1.json', '{\"old\":1}'), \
                 ('file-json-bulk-2', '/bulk-2.json', '{\"old\":2}')",
                &[],
            )
            .await
            .expect("bulk insert should succeed");

        engine
            .execute(
                "UPDATE lix_file \
                 SET data = '{\"common\":\"updated\"}' \
                 WHERE id IN ('file-json-bulk-1', 'file-json-bulk-2')",
                &[],
            )
            .await
            .expect("bulk update should succeed");

        let detected_one =
            detected_json_pointer_entities(&engine, "file-json-bulk-1", &main_version_id).await;
        let detected_two =
            detected_json_pointer_entities(&engine, "file-json-bulk-2", &main_version_id).await;
        assert_eq!(detected_one, vec!["".to_string(), "/common".to_string()]);
        assert_eq!(detected_two, vec!["".to_string(), "/common".to_string()]);

        materialize_full(&engine).await;

        let cache_one = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_internal_file_data_cache \
                     WHERE file_id = 'file-json-bulk-1' AND version_id = '{}' \
                     LIMIT 1",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("file_data_cache query should succeed");
        let cache_two = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_internal_file_data_cache \
                     WHERE file_id = 'file-json-bulk-2' AND version_id = '{}' \
                     LIMIT 1",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("file_data_cache query should succeed");
        assert_eq!(cache_one.rows.len(), 1);
        assert_eq!(cache_two.rows.len(), 1);
        assert_blob_json_eq(
            &cache_one.rows[0][0],
            serde_json::json!({"common":"updated"}),
        );
        assert_blob_json_eq(
            &cache_two.rows[0][0],
            serde_json::json!({"common":"updated"}),
        );
    }
);

simulation_test!(
    file_update_json_multi_statement_uses_sequential_before_data,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json-seq', '/seq.json', '{\"a\":1}')",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

        engine
            .execute(
                "UPDATE lix_file SET data = '{\"a\":2,\"b\":true}' WHERE id = 'file-json-seq'; \
                 UPDATE lix_file SET data = '{\"a\":4}' WHERE id = 'file-json-seq'",
                &[],
            )
            .await
            .expect("multi-statement update should succeed");

        let detected =
            detected_json_pointer_entities(&engine, "file-json-seq", &main_version_id).await;
        assert_eq!(detected, vec!["".to_string(), "/a".to_string()]);

        materialize_full(&engine).await;

        let cache_rows = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_internal_file_data_cache \
                     WHERE file_id = 'file-json-seq' AND version_id = '{}' \
                     LIMIT 1",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("file_data_cache query should succeed");
        assert_eq!(cache_rows.rows.len(), 1);
        assert_blob_json_eq(&cache_rows.rows[0][0], serde_json::json!({"a":4}));
    }
);

simulation_test!(
    file_update_json_multi_statement_placeholders_preserve_parameter_order,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json-seq-param', '/seq-param.json', '{\"seed\":0}')",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

        engine
            .execute(
                "UPDATE lix_file SET data = $1 WHERE id = 'file-json-seq-param'; \
                 UPDATE lix_file SET data = $2 WHERE id = 'file-json-seq-param'",
                &[
                    Value::Text("{\"step\":1,\"keep\":true}".to_string()),
                    Value::Text("{\"final\":2}".to_string()),
                ],
            )
            .await
            .expect("multi-statement parameterized update should succeed");

        let detected =
            detected_json_pointer_entities(&engine, "file-json-seq-param", &main_version_id).await;
        assert_eq!(detected, vec!["".to_string(), "/final".to_string()]);

        materialize_full(&engine).await;

        let cache_rows = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_internal_file_data_cache \
                     WHERE file_id = 'file-json-seq-param' AND version_id = '{}' \
                     LIMIT 1",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("file_data_cache query should succeed");
        assert_eq!(cache_rows.rows.len(), 1);
        assert_blob_json_eq(&cache_rows.rows[0][0], serde_json::json!({"final":2}));
    }
);

simulation_test!(
    file_delete_json_tombstones_detected_rows_and_clears_cache,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json-delete', '/delete.json', '{\"hello\":\"before\",\"drop\":\"x\"}')",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

        materialize_full(&engine).await;
        assert_eq!(
            file_cache_row_count(&engine, "file-json-delete", &main_version_id).await,
            1
        );

        engine
            .execute("DELETE FROM lix_file WHERE id = 'file-json-delete'", &[])
            .await
            .expect("file delete should succeed");

        materialize_full(&engine).await;

        assert_eq!(
            file_descriptor_tombstone_count(&engine, "file-json-delete", &main_version_id).await,
            1
        );
        assert_eq!(
            file_cache_row_count(&engine, "file-json-delete", &main_version_id).await,
            0
        );
    }
);

simulation_test!(
    file_multi_statement_insert_update_delete_keeps_tombstones_and_cache_consistent,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json-mixed-drop', '/drop.json', '{\"a\":1}'); \
                 UPDATE lix_file \
                 SET data = '{\"a\":2,\"b\":true}' \
                 WHERE id = 'file-json-mixed-drop'; \
                 DELETE FROM lix_file WHERE id = 'file-json-mixed-drop'; \
                 INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json-mixed-keep', '/keep.json', '{\"keep\":\"yes\"}')",
                &[],
            )
            .await
            .expect("mixed insert/update/delete execute should succeed");

        materialize_full(&engine).await;

        assert_eq!(
            file_descriptor_tombstone_count(&engine, "file-json-mixed-drop", &main_version_id)
                .await,
            1
        );
        assert_eq!(
            file_cache_row_count(&engine, "file-json-mixed-drop", &main_version_id).await,
            0
        );

        let kept_visible =
            detected_json_pointer_entities(&engine, "file-json-mixed-keep", &main_version_id).await;
        assert_eq!(kept_visible, vec!["".to_string(), "/keep".to_string()]);

        let kept_cache = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_internal_file_data_cache \
                     WHERE file_id = 'file-json-mixed-keep' AND version_id = '{}' \
                     LIMIT 1",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("kept file_data_cache query should succeed");
        assert_eq!(kept_cache.rows.len(), 1);
        assert_blob_json_eq(&kept_cache.rows[0][0], serde_json::json!({"keep":"yes"}));
    }
);
