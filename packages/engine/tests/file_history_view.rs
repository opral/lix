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

fn assert_text(value: &Value, expected: &str) {
    match value {
        Value::Text(actual) => assert_eq!(actual, expected),
        other => panic!("expected text value '{expected}', got {other:?}"),
    }
}

fn assert_integer(value: &Value, expected: i64) {
    match value {
        Value::Integer(actual) => assert_eq!(*actual, expected),
        other => panic!("expected integer value {expected}, got {other:?}"),
    }
}

fn assert_not_null(value: &Value, label: &str) {
    assert!(
        !matches!(value, Value::Null),
        "expected non-null value for {label}, got Null"
    );
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
                    "SELECT id, path, data, lixcol_commit_id, lixcol_root_commit_id, lixcol_depth \
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
        assert_text(&after_row.rows[0][0], "history-data");
        assert_text(&after_row.rows[0][1], "/history-data.json");
        assert_blob_json_eq(&after_row.rows[0][2], serde_json::json!({"value":"v1"}));
        assert_text(&after_row.rows[0][3], &update_commit_id);
        assert_text(&after_row.rows[0][4], &update_commit_id);
        assert_integer(&after_row.rows[0][5], 0);

        let before_row = engine
            .execute(
                &format!(
                    "SELECT id, path, data, lixcol_commit_id, lixcol_root_commit_id, lixcol_depth \
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
        assert_text(&before_row.rows[0][0], "history-data");
        assert_text(&before_row.rows[0][1], "/history-data.json");
        assert_blob_json_eq(&before_row.rows[0][2], serde_json::json!({"value":"v0"}));
        assert_text(&before_row.rows[0][3], &insert_commit_id);
        assert_text(&before_row.rows[0][4], &insert_commit_id);
        assert_integer(&before_row.rows[0][5], 0);
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
                    "SELECT data, lixcol_commit_id, lixcol_root_commit_id, lixcol_depth \
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
        assert_text(&rows.rows[0][1], &update_commit_id);
        assert_text(&rows.rows[0][2], &update_commit_id);
        assert_integer(&rows.rows[0][3], 0);

        assert_eq!(
            file_history_cache_row_count_at_depth(&engine, "history-cache", &update_commit_id, 0)
                .await,
            1
        );
        assert_eq!(
            file_history_cache_row_count(&engine, "history-cache", &update_commit_id).await,
            2
        );
    }
);

simulation_test!(
    file_history_view_rename_and_move_reconstructs_path_history_per_root_commit,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('history-docs-dir', '/docs/')",
                &[],
            )
            .await
            .expect("docs directory insert should succeed");

        engine
            .execute(
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('history-archive-dir', '/archive/')",
                &[],
            )
            .await
            .expect("archive directory insert should succeed");

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('history-path', '/docs/a.json', '{\"value\":\"v0\"}')",
                &[],
            )
            .await
            .expect("initial file insert should succeed");
        let insert_commit_id = active_version_commit_id(&engine).await;

        engine
            .execute(
                "UPDATE lix_file \
                 SET path = '/docs/a-renamed.json' \
                 WHERE id = 'history-path'",
                &[],
            )
            .await
            .expect("file rename should succeed");
        let rename_commit_id = active_version_commit_id(&engine).await;

        engine
            .execute(
                "UPDATE lix_file \
                 SET path = '/archive/a-renamed.json' \
                 WHERE id = 'history-path'",
                &[],
            )
            .await
            .expect("file move should succeed");
        let move_commit_id = active_version_commit_id(&engine).await;

        for (depth, expected_commit_id, expected_path) in [
            (0_i64, move_commit_id.as_str(), "/archive/a-renamed.json"),
            (1_i64, rename_commit_id.as_str(), "/docs/a-renamed.json"),
            (2_i64, insert_commit_id.as_str(), "/docs/a.json"),
        ] {
            let rows = engine
                .execute(
                    &format!(
                        "SELECT path, lixcol_commit_id, lixcol_root_commit_id, lixcol_depth \
                         FROM lix_file_history \
                         WHERE id = 'history-path' \
                           AND lixcol_root_commit_id = '{}' \
                           AND lixcol_depth = {}",
                        move_commit_id, depth
                    ),
                    &[],
                )
                .await
                .expect("latest-root depth-scoped file history read should succeed");
            assert_eq!(rows.rows.len(), 1);
            assert_text(&rows.rows[0][0], expected_path);
            assert_text(&rows.rows[0][1], expected_commit_id);
            assert_text(&rows.rows[0][2], &move_commit_id);
            assert_integer(&rows.rows[0][3], depth);
        }

        let latest_root_count = engine
            .execute(
                &format!(
                    "SELECT COUNT(*) \
                     FROM lix_file_history \
                     WHERE id = 'history-path' \
                       AND lixcol_root_commit_id = '{}'",
                    move_commit_id
                ),
                &[],
            )
            .await
            .expect("latest-root history count should succeed");
        assert_eq!(latest_root_count.rows.len(), 1);
        assert_eq!(value_as_i64(&latest_root_count.rows[0][0]), 3);

        assert_ne!(insert_commit_id, rename_commit_id);
        assert_ne!(rename_commit_id, move_commit_id);
    }
);

simulation_test!(
    file_history_view_directory_rename_propagates_into_file_history_paths,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('history-dir-rename-file', '/docs/readme.json', '{\"content\":\"hello\"}')",
                &[],
            )
            .await
            .expect("initial file insert should succeed");
        let before_rename_commit_id = active_version_commit_id(&engine).await;
        let docs_directory = engine
            .execute(
                "SELECT id \
                 FROM lix_directory \
                 WHERE path = '/docs/' \
                 LIMIT 1",
                &[],
            )
            .await
            .expect("docs directory lookup should succeed");
        assert_eq!(docs_directory.rows.len(), 1);
        let docs_directory_id = match &docs_directory.rows[0][0] {
            Value::Text(value) => value.clone(),
            other => panic!("expected docs directory id text, got {other:?}"),
        };

        engine
            .execute(
                &format!(
                    "UPDATE lix_directory \
                     SET name = 'guides' \
                     WHERE id = '{}'",
                    docs_directory_id
                ),
                &[],
            )
            .await
            .expect("directory rename should succeed");

        engine
            .execute(
                "UPDATE lix_file \
                 SET metadata = '{\"stage\":\"after-rename\"}' \
                 WHERE id = 'history-dir-rename-file'",
                &[],
            )
            .await
            .expect("file metadata touch after directory rename should succeed");
        let after_rename_commit_id = active_version_commit_id(&engine).await;

        let before_rename_row = engine
            .execute(
                &format!(
                    "SELECT path, lixcol_commit_id, lixcol_root_commit_id, lixcol_depth \
                     FROM lix_file_history \
                     WHERE id = 'history-dir-rename-file' \
                       AND lixcol_root_commit_id = '{}' \
                       AND lixcol_depth = 0",
                    before_rename_commit_id
                ),
                &[],
            )
            .await
            .expect("before-rename history row query should succeed");
        assert_eq!(before_rename_row.rows.len(), 1);
        assert_text(&before_rename_row.rows[0][0], "/docs/readme.json");
        assert_text(&before_rename_row.rows[0][1], &before_rename_commit_id);
        assert_text(&before_rename_row.rows[0][2], &before_rename_commit_id);
        assert_integer(&before_rename_row.rows[0][3], 0);

        let after_rename_row = engine
            .execute(
                &format!(
                    "SELECT path, lixcol_commit_id, lixcol_root_commit_id, lixcol_depth \
                     FROM lix_file_history \
                     WHERE id = 'history-dir-rename-file' \
                       AND lixcol_root_commit_id = '{}' \
                       AND lixcol_depth = 0",
                    after_rename_commit_id
                ),
                &[],
            )
            .await
            .expect("after-rename history row query should succeed");
        assert_eq!(after_rename_row.rows.len(), 1);
        assert_text(&after_rename_row.rows[0][0], "/guides/readme.json");
        assert_text(&after_rename_row.rows[0][1], &after_rename_commit_id);
        assert_text(&after_rename_row.rows[0][2], &after_rename_commit_id);
        assert_integer(&after_rename_row.rows[0][3], 0);

        let historical_paths = engine
            .execute(
                &format!(
                    "SELECT path, lixcol_depth \
                     FROM lix_file_history \
                     WHERE id = 'history-dir-rename-file' \
                       AND lixcol_root_commit_id = '{}' \
                     ORDER BY lixcol_depth ASC",
                    after_rename_commit_id
                ),
                &[],
            )
            .await
            .expect("after-rename depth scan should succeed");
        assert!(
            historical_paths.rows.iter().any(|row| {
                matches!(row.get(0), Some(Value::Text(path)) if path == "/docs/readme.json")
                    && matches!(row.get(1), Some(Value::Integer(depth)) if *depth > 0)
            }),
            "expected old path to remain in deeper history after directory rename"
        );
    }
);

simulation_test!(
    file_history_view_directory_move_propagates_into_file_history_paths,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_directory (id, path) \
                 VALUES ('history-dir-move-articles', '/articles/')",
                &[],
            )
            .await
            .expect("articles directory insert should succeed");

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('history-dir-move-file', '/docs/guides/intro.json', '{\"note\":\"move\"}')",
                &[],
            )
            .await
            .expect("initial nested file insert should succeed");
        let before_move_commit_id = active_version_commit_id(&engine).await;

        let docs_directory = engine
            .execute(
                "SELECT id \
                 FROM lix_directory \
                 WHERE name = 'docs' \
                 LIMIT 1",
                &[],
            )
            .await
            .expect("docs directory lookup should succeed");
        assert_eq!(docs_directory.rows.len(), 1);
        let docs_directory_id = match &docs_directory.rows[0][0] {
            Value::Text(value) => value.clone(),
            other => panic!("expected docs directory id text, got {other:?}"),
        };

        let guides_directory = engine
            .execute(
                &format!(
                    "SELECT id \
                     FROM lix_directory \
                     WHERE name = 'guides' \
                       AND parent_id = '{}' \
                     LIMIT 1",
                    docs_directory_id
                ),
                &[],
            )
            .await
            .expect("guides directory lookup should succeed");
        assert_eq!(guides_directory.rows.len(), 1);
        let guides_directory_id = match &guides_directory.rows[0][0] {
            Value::Text(value) => value.clone(),
            other => panic!("expected guides directory id text, got {other:?}"),
        };

        engine
            .execute(
                &format!(
                    "UPDATE lix_directory \
                     SET parent_id = 'history-dir-move-articles' \
                     WHERE id = '{}'",
                    guides_directory_id
                ),
                &[],
            )
            .await
            .expect("directory move should succeed");

        engine
            .execute(
                "UPDATE lix_file \
                 SET metadata = '{\"stage\":\"after-move\"}' \
                 WHERE id = 'history-dir-move-file'",
                &[],
            )
            .await
            .expect("file metadata touch after directory move should succeed");
        let after_move_commit_id = active_version_commit_id(&engine).await;

        let before_move_row = engine
            .execute(
                &format!(
                    "SELECT path, lixcol_commit_id, lixcol_root_commit_id, lixcol_depth \
                     FROM lix_file_history \
                     WHERE id = 'history-dir-move-file' \
                       AND lixcol_root_commit_id = '{}' \
                       AND lixcol_depth = 0",
                    before_move_commit_id
                ),
                &[],
            )
            .await
            .expect("before-move history row query should succeed");
        assert_eq!(before_move_row.rows.len(), 1);
        assert_text(&before_move_row.rows[0][0], "/docs/guides/intro.json");
        assert_text(&before_move_row.rows[0][1], &before_move_commit_id);
        assert_text(&before_move_row.rows[0][2], &before_move_commit_id);
        assert_integer(&before_move_row.rows[0][3], 0);

        let after_move_row = engine
            .execute(
                &format!(
                    "SELECT path, lixcol_commit_id, lixcol_root_commit_id, lixcol_depth \
                     FROM lix_file_history \
                     WHERE id = 'history-dir-move-file' \
                       AND lixcol_root_commit_id = '{}' \
                       AND lixcol_depth = 0",
                    after_move_commit_id
                ),
                &[],
            )
            .await
            .expect("after-move history row query should succeed");
        assert_eq!(after_move_row.rows.len(), 1);
        assert_text(&after_move_row.rows[0][0], "/articles/guides/intro.json");
        assert_text(&after_move_row.rows[0][1], &after_move_commit_id);
        assert_text(&after_move_row.rows[0][2], &after_move_commit_id);
        assert_integer(&after_move_row.rows[0][3], 0);

        let historical_paths = engine
            .execute(
                &format!(
                    "SELECT path, lixcol_depth \
                     FROM lix_file_history \
                     WHERE id = 'history-dir-move-file' \
                       AND lixcol_root_commit_id = '{}' \
                     ORDER BY lixcol_depth ASC",
                    after_move_commit_id
                ),
                &[],
            )
            .await
            .expect("after-move depth scan should succeed");
        assert!(
            historical_paths.rows.iter().any(|row| {
                matches!(row.get(0), Some(Value::Text(path)) if path == "/docs/guides/intro.json")
                    && matches!(row.get(1), Some(Value::Integer(depth)) if *depth > 0)
            }),
            "expected old path to remain in deeper history after directory move"
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
                    "SELECT data, lixcol_commit_id, lixcol_root_commit_id, lixcol_depth \
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
        assert_blob_json_eq(
            &historical_before.rows[0][0],
            serde_json::json!({"value":"new"}),
        );
        assert_text(&historical_before.rows[0][1], &insert_commit_id);
        assert_text(&historical_before.rows[0][2], &insert_commit_id);
        assert_integer(&historical_before.rows[0][3], 0);

        let historical_after = engine
            .execute(
                &format!(
                    "SELECT data, lixcol_commit_id, lixcol_root_commit_id, lixcol_depth \
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
        assert_blob_json_eq(
            &historical_after.rows[0][0],
            serde_json::json!({"value":"newer"}),
        );
        assert_text(&historical_after.rows[0][1], &update_commit_id);
        assert_text(&historical_after.rows[0][2], &update_commit_id);
        assert_integer(&historical_after.rows[0][3], 0);

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
        let expected_after_content_change_id_rows = engine
            .execute(
                &format!(
                    "SELECT change_id \
                     FROM lix_state_history \
                     WHERE file_id = 'history-partial' \
                       AND root_commit_id = '{}' \
                       AND depth = 0 \
                       AND schema_key != 'lix_file_descriptor' \
                       AND snapshot_content IS NOT NULL \
                     ORDER BY commit_id ASC, change_id ASC \
                     LIMIT 1",
                    after_commit_id
                ),
                &[],
            )
            .await
            .expect("content-root change id lookup should succeed");
        assert_eq!(expected_after_content_change_id_rows.rows.len(), 1);
        let expected_after_content_change_id = match &expected_after_content_change_id_rows.rows[0]
            [0]
        {
            Value::Text(value) => value.clone(),
            other => panic!("expected content-root change id text, got {other:?}"),
        };

        let before = engine
            .execute(
                &format!(
                    "SELECT data, lixcol_commit_id, lixcol_root_commit_id, lixcol_depth \
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
        assert_text(&before.rows[0][1], &before_commit_id);
        assert_text(&before.rows[0][2], &before_commit_id);
        assert_integer(&before.rows[0][3], 0);

        let after = engine
            .execute(
                &format!(
                    "SELECT \
                        data, \
                        lixcol_schema_key, \
                        lixcol_file_id, \
                        lixcol_plugin_key, \
                        lixcol_schema_version, \
                        lixcol_change_id, \
                        lixcol_commit_id, \
                        lixcol_root_commit_id, \
                        lixcol_depth \
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
        assert_text(&after.rows[0][1], "lix_file_descriptor");
        assert_text(&after.rows[0][2], "lix");
        assert_text(&after.rows[0][3], "lix");
        assert_text(&after.rows[0][4], "1");
        assert_text(&after.rows[0][5], &expected_after_content_change_id);
        assert_text(&after.rows[0][6], &after_commit_id);
        assert_text(&after.rows[0][7], &after_commit_id);
        assert_integer(&after.rows[0][8], 0);
    }
);

simulation_test!(
    file_history_view_exposes_lixcol_surface_parity_with_key_value_history,
    simulations = [sqlite, postgres],
    |sim| async move {
        let engine = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_key_value (\
                 key, value, lixcol_file_id, lixcol_plugin_key, lixcol_schema_version\
                 ) VALUES (\
                 'history-lixcol-kv', 'value-0', 'lix', 'lix', '1'\
                 )",
                &[],
            )
            .await
            .expect("key_value insert should succeed");
        let key_value_root_commit_id = active_version_commit_id(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('history-lixcol-file', '/history-lixcol-file.json', '{\"value\":\"x\"}')",
                &[],
            )
            .await
            .expect("file insert should succeed");
        let file_root_commit_id = active_version_commit_id(&engine).await;

        let file_history = engine
            .execute(
                &format!(
                    "SELECT \
                        lixcol_entity_id, \
                        lixcol_schema_key, \
                        lixcol_file_id, \
                        lixcol_version_id, \
                        lixcol_plugin_key, \
                        lixcol_schema_version, \
                        lixcol_change_id, \
                        lixcol_metadata, \
                        lixcol_commit_id, \
                        lixcol_root_commit_id, \
                        lixcol_depth \
                     FROM lix_file_history \
                     WHERE id = 'history-lixcol-file' \
                       AND lixcol_root_commit_id = '{}' \
                       AND lixcol_depth = 0",
                    file_root_commit_id
                ),
                &[],
            )
            .await
            .expect("file_history lixcol surface query should succeed");
        assert_eq!(file_history.rows.len(), 1);
        assert_eq!(file_history.rows[0].len(), 11);
        assert_text(&file_history.rows[0][0], "history-lixcol-file");
        assert_text(&file_history.rows[0][1], "lix_file_descriptor");
        assert_text(&file_history.rows[0][2], "lix");
        assert_not_null(&file_history.rows[0][3], "file_history.lixcol_version_id");
        assert_text(&file_history.rows[0][4], "lix");
        assert_text(&file_history.rows[0][5], "1");
        assert_not_null(&file_history.rows[0][6], "file_history.lixcol_change_id");
        assert_text(&file_history.rows[0][8], &file_root_commit_id);
        assert_text(&file_history.rows[0][9], &file_root_commit_id);
        assert_integer(&file_history.rows[0][10], 0);

        let key_value_history = engine
            .execute(
                &format!(
                    "SELECT \
                        lixcol_entity_id, \
                        lixcol_schema_key, \
                        lixcol_file_id, \
                        lixcol_version_id, \
                        lixcol_plugin_key, \
                        lixcol_schema_version, \
                        lixcol_change_id, \
                        lixcol_metadata, \
                        lixcol_commit_id, \
                        lixcol_root_commit_id, \
                        lixcol_depth \
                     FROM lix_key_value_history \
                     WHERE key = 'history-lixcol-kv' \
                       AND lixcol_root_commit_id = '{}' \
                       AND lixcol_depth = 0",
                    key_value_root_commit_id
                ),
                &[],
            )
            .await
            .expect("key_value_history lixcol surface query should succeed");
        assert_eq!(key_value_history.rows.len(), 1);
        assert_eq!(key_value_history.rows[0].len(), 11);
        assert_text(&key_value_history.rows[0][0], "history-lixcol-kv");
        assert_text(&key_value_history.rows[0][1], "lix_key_value");
        assert_text(&key_value_history.rows[0][2], "lix");
        assert_not_null(
            &key_value_history.rows[0][3],
            "key_value_history.lixcol_version_id",
        );
        assert_text(&key_value_history.rows[0][4], "lix");
        assert_text(&key_value_history.rows[0][5], "1");
        assert_not_null(
            &key_value_history.rows[0][6],
            "key_value_history.lixcol_change_id",
        );
        assert_text(&key_value_history.rows[0][8], &key_value_root_commit_id);
        assert_text(&key_value_history.rows[0][9], &key_value_root_commit_id);
        assert_integer(&key_value_history.rows[0][10], 0);

        // Keep parity with legacy intent: the relevant lixcol surface should be selectable on both views.
        assert_eq!(file_history.rows[0].len(), key_value_history.rows[0].len());
    }
);
