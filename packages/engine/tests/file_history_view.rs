mod support;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, OnceLock};

use lix_engine::{Value, WasmRuntime};
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

async fn active_version_commit_id(engine: &support::simulation_test::SimulationEngine) -> String {
    let rows = engine
        .execute(
            "SELECT v.commit_id \
             FROM lix_version v \
             JOIN lix_active_version av ON av.version_id = v.id \
             ORDER BY av.id \
             LIMIT 1",
            &[],
        )
        .await
        .expect("active version commit query should succeed");
    assert_eq!(rows.rows.len(), 1);
    match &rows.rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected active version commit id text, got {other:?}"),
    }
}

async fn boot_engine_with_json_plugin(
    sim: &support::simulation_test::SimulationArgs,
) -> support::simulation_test::SimulationEngine {
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
    let plugin_wasm = plugin_json_v2_wasm_bytes();
    engine
        .install_plugin(TEST_PLUGIN_MANIFEST, &plugin_wasm)
        .await
        .expect("install_plugin should succeed");
    engine
}

async fn file_history_cache_row_count(
    engine: &support::simulation_test::SimulationEngine,
    file_id: &str,
    root_commit_id: &str,
) -> i64 {
    let rows = engine
        .execute(
            &format!(
                "SELECT COUNT(*) \
                 FROM lix_internal_file_history_data_cache \
                 WHERE file_id = '{}' AND root_commit_id = '{}'",
                file_id, root_commit_id
            ),
            &[],
        )
        .await
        .expect("file history data cache count query should succeed");
    assert_eq!(rows.rows.len(), 1);
    value_as_i64(&rows.rows[0][0])
}

async fn file_history_cache_row_count_at_depth(
    engine: &support::simulation_test::SimulationEngine,
    file_id: &str,
    root_commit_id: &str,
    depth: i64,
) -> i64 {
    let rows = engine
        .execute(
            &format!(
                "SELECT COUNT(*) \
                 FROM lix_internal_file_history_data_cache \
                 WHERE file_id = '{}' \
                   AND root_commit_id = '{}' \
                   AND depth = {}",
                file_id, root_commit_id, depth
            ),
            &[],
        )
        .await
        .expect("file history data cache depth count query should succeed");
    assert_eq!(rows.rows.len(), 1);
    value_as_i64(&rows.rows[0][0])
}

simulation_test!(
    file_history_view_materializes_data_per_root_commit_and_depth,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('history-data', '/history-data.json', '{\"value\":\"v0\"}')",
                &[],
            )
            .await
            .expect("initial file insert should succeed");
        let insert_commit_id = active_version_commit_id(&engine).await;

        engine
            .execute(
                "UPDATE lix_file \
                 SET data = '{\"value\":\"v1\"}' \
                 WHERE id = 'history-data'",
                &[],
            )
            .await
            .expect("file update should succeed");
        let update_commit_id = active_version_commit_id(&engine).await;

        let after_row = engine
            .execute(
                &format!(
                    "SELECT data \
                     FROM lix_file_history \
                     WHERE id = 'history-data' \
                       AND lixcol_root_commit_id = '{}' \
                       AND lixcol_depth = 0",
                    update_commit_id
                ),
                &[],
            )
            .await
            .expect("updated-root depth-0 file history read should succeed");
        assert_eq!(after_row.rows.len(), 1);
        assert_blob_json_eq(&after_row.rows[0][0], serde_json::json!({"value":"v1"}));

        let before_row = engine
            .execute(
                &format!(
                    "SELECT data \
                     FROM lix_file_history \
                     WHERE id = 'history-data' \
                       AND lixcol_root_commit_id = '{}' \
                       AND lixcol_depth = 0",
                    insert_commit_id
                ),
                &[],
            )
            .await
            .expect("insert-root depth-0 file history read should succeed");
        assert_eq!(before_row.rows.len(), 1);
        assert_blob_json_eq(&before_row.rows[0][0], serde_json::json!({"value":"v0"}));
    }
);

simulation_test!(
    file_history_view_read_materializes_history_cache_on_demand,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('history-cache', '/history-cache.json', '{\"value\":\"before\"}')",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

        engine
            .execute(
                "UPDATE lix_file \
                 SET data = '{\"value\":\"after\"}' \
                 WHERE id = 'history-cache'",
                &[],
            )
            .await
            .expect("file update should succeed");
        let update_commit_id = active_version_commit_id(&engine).await;

        assert_eq!(
            file_history_cache_row_count(&engine, "history-cache", &update_commit_id).await,
            0
        );

        let rows = engine
            .execute(
                &format!(
                    "SELECT data \
                     FROM lix_file_history \
                     WHERE id = 'history-cache' \
                       AND lixcol_root_commit_id = '{}' \
                       AND lixcol_depth = 0",
                    update_commit_id
                ),
                &[],
            )
            .await
            .expect("file history read should succeed");
        assert_eq!(rows.rows.len(), 1);
        assert_blob_json_eq(&rows.rows[0][0], serde_json::json!({"value":"after"}));

        assert_eq!(
            file_history_cache_row_count_at_depth(&engine, "history-cache", &update_commit_id, 0)
                .await,
            1
        );
    }
);

simulation_test!(
    file_history_view_read_does_not_override_live_file_data_cache,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('history-live', '/history-live.json', '{\"value\":\"old\"}')",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

        engine
            .execute(
                "UPDATE lix_file \
                 SET data = '{\"value\":\"new\"}' \
                 WHERE id = 'history-live'",
                &[],
            )
            .await
            .expect("file update should succeed");
        let insert_commit_id = active_version_commit_id(&engine).await;
        engine
            .execute(
                "UPDATE lix_file \
                 SET data = '{\"value\":\"newer\"}' \
                 WHERE id = 'history-live'",
                &[],
            )
            .await
            .expect("second file update should succeed");
        let update_commit_id = active_version_commit_id(&engine).await;

        let historical_before = engine
            .execute(
                &format!(
                    "SELECT data \
                     FROM lix_file_history \
                     WHERE id = 'history-live' \
                       AND lixcol_root_commit_id = '{}' \
                       AND lixcol_depth = 0",
                    insert_commit_id
                ),
                &[],
            )
            .await
            .expect("historical before-root read should succeed");
        assert_eq!(historical_before.rows.len(), 1);
        assert_blob_json_eq(&historical_before.rows[0][0], serde_json::json!({"value":"new"}));

        let historical_after = engine
            .execute(
                &format!(
                    "SELECT data \
                     FROM lix_file_history \
                     WHERE id = 'history-live' \
                       AND lixcol_root_commit_id = '{}' \
                       AND lixcol_depth = 0",
                    update_commit_id
                ),
                &[],
            )
            .await
            .expect("historical after-root read should succeed");
        assert_eq!(historical_after.rows.len(), 1);
        assert_blob_json_eq(&historical_after.rows[0][0], serde_json::json!({"value":"newer"}));

        let live = engine
            .execute("SELECT data FROM lix_file WHERE id = 'history-live'", &[])
            .await
            .expect("live file read should succeed");
        assert_eq!(live.rows.len(), 1);
        assert_blob_json_eq(&live.rows[0][0], serde_json::json!({"value":"newer"}));
    }
);

simulation_test!(
    file_history_view_content_only_partial_update_reconstructs_full_document,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('history-partial', '/history-partial.json', '{\"name\":\"test-item\",\"value\":100}')",
                &[],
            )
            .await
            .expect("initial file insert should succeed");
        let before_commit_id = active_version_commit_id(&engine).await;

        engine
            .execute(
                "UPDATE lix_file \
                 SET data = '{\"name\":\"test-item\",\"value\":105}' \
                 WHERE id = 'history-partial'",
                &[],
            )
            .await
            .expect("partial file update should succeed");
        let after_commit_id = active_version_commit_id(&engine).await;

        let before = engine
            .execute(
                &format!(
                    "SELECT data \
                     FROM lix_file_history \
                     WHERE id = 'history-partial' \
                       AND lixcol_root_commit_id = '{}' \
                       AND lixcol_depth = 0",
                    before_commit_id
                ),
                &[],
            )
            .await
            .expect("before-root partial history read should succeed");
        assert_eq!(before.rows.len(), 1);
        assert_blob_json_eq(
            &before.rows[0][0],
            serde_json::json!({"name":"test-item","value":100}),
        );

        let after = engine
            .execute(
                &format!(
                    "SELECT data \
                     FROM lix_file_history \
                     WHERE id = 'history-partial' \
                       AND lixcol_root_commit_id = '{}' \
                       AND lixcol_depth = 0",
                    after_commit_id
                ),
                &[],
            )
            .await
            .expect("after-root partial history read should succeed");
        assert_eq!(after.rows.len(), 1);
        assert_blob_json_eq(
            &after.rows[0][0],
            serde_json::json!({"name":"test-item","value":105}),
        );
    }
);
