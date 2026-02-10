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

fn assert_blob_json_eq(value: &Value, expected: JsonValue) {
    let bytes = match value {
        Value::Blob(bytes) => bytes,
        other => panic!("expected blob value, got {other:?}"),
    };
    let actual: JsonValue = serde_json::from_slice(bytes).expect("blob should contain valid JSON");
    assert_eq!(actual, expected);
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
