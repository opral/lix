use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use lix_engine::{Engine, RegisterPluginOptions, Value};
use serde_json::json;
use tempfile::TempDir;
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

use crate::support::kv_backend::InMemoryKvBackend;
use crate::wasmtime_runtime::WasmtimeWasmRuntime;

#[test]
fn file_write_runs_installed_plugin_detect_changes() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime should build");
    runtime.block_on(async {
        let wasm = build_detect_changes_component();
        let backend = InMemoryKvBackend::new();
        Engine::initialize(Box::new(backend.clone()))
            .await
            .expect("backend should initialize");
        let engine =
            Engine::new_with_wasm_runtime(Box::new(backend), Arc::new(WasmtimeWasmRuntime::new()))
                .await
                .expect("engine should boot with test wasm runtime");
        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");

        session
            .register_plugin(RegisterPluginOptions {
                bytes: test_plugin_archive("test_plugin_detect_changes", &wasm),
            })
            .await
            .expect("plugin should register");

        session
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES ('file-json', '/foo.json', $1)",
                &[Value::Blob(br#"{"hello":"world"}"#.to_vec())],
            )
            .await
            .expect("file write should run detect_changes");

        let rows = session
            .execute(
                "SELECT id, value, lixcol_file_id \
                 FROM test_json_entity \
                 WHERE id = 'entity-1'",
                &[],
            )
            .await
            .expect("semantic row should be readable");

        assert_eq!(rows.len(), 1);
        let insert_value = json!({
            "active_state_entity_id": null,
            "active_state_file_id": null,
            "active_state_len": 0,
            "active_state_plugin_key": null,
            "active_state_schema": null,
            "active_state_snapshot_content": null,
            "active_state_version_id": null,
            "after": r#"{"hello":"world"}"#,
            "after_path": "/foo.json",
            "before": null,
            "before_path": null,
        })
        .to_string();
        assert_eq!(
            rows.rows()[0].values(),
            &[
                Value::Text("entity-1".to_string()),
                Value::Text(insert_value.clone()),
                Value::Text("file-json".to_string()),
            ]
        );

        let state_rows = session
            .execute(
                "SELECT schema_key, entity_id, file_id, snapshot_content, global, untracked \
                 FROM lix_state \
                 WHERE schema_key = 'test_json_entity' \
                   AND file_id = 'file-json'",
                &[],
            )
            .await
            .expect("plugin changes should be visible through lix_state");

        assert_eq!(state_rows.len(), 1);
        assert_eq!(
            state_rows.rows()[0].values(),
            &[
                Value::Text("test_json_entity".to_string()),
                Value::Json(json!(["entity-1"])),
                Value::Text("file-json".to_string()),
                Value::Json(json!({
                    "id": "entity-1",
                    "value": insert_value.clone(),
                })),
                Value::Boolean(false),
                Value::Boolean(false),
            ]
        );

        session
            .execute(
                "UPDATE lix_file \
                 SET path = '/bar.json', data = $1 \
                 WHERE id = 'file-json'",
                &[Value::Blob(br#"{"hello":"mars"}"#.to_vec())],
            )
            .await
            .expect("file update should pass before and active-state context");

        let rows = session
            .execute(
                "SELECT value, lixcol_file_id \
                 FROM test_json_entity \
                 WHERE id = 'entity-1'",
                &[],
            )
            .await
            .expect("semantic row should be readable after update");

        let values = rows.rows()[0].values();
        let Value::Text(update_value) = &values[0] else {
            panic!("plugin value should be text, got {:?}", values[0]);
        };
        assert_eq!(values[1], Value::Text("file-json".to_string()));
        let update_value: serde_json::Value =
            serde_json::from_str(update_value).expect("plugin value should be JSON");
        assert_eq!(update_value["active_state_entity_id"], json!("entity-1"));
        assert_eq!(update_value["active_state_file_id"], json!("file-json"));
        assert_eq!(update_value["active_state_len"], json!(1));
        assert_eq!(
            update_value["active_state_plugin_key"],
            json!("test_plugin_detect_changes")
        );
        assert_eq!(
            update_value["active_state_schema"],
            json!("test_json_entity")
        );
        assert_eq!(update_value["active_state_version_id"], json!(null));
        assert_eq!(update_value["after"], json!(r#"{"hello":"mars"}"#));
        assert_eq!(update_value["after_path"], json!("/bar.json"));
        assert_eq!(update_value["before"], json!(r#"{"hello":"world"}"#));
        assert_eq!(update_value["before_path"], json!("/foo.json"));
        let active_snapshot: serde_json::Value = serde_json::from_str(
            update_value["active_state_snapshot_content"]
                .as_str()
                .expect("snapshot content should be present"),
        )
        .expect("active snapshot should be JSON");
        assert_eq!(active_snapshot["id"], json!("entity-1"));
        assert_eq!(active_snapshot["value"], json!(insert_value));

        session
            .execute("DELETE FROM lix_file WHERE id = 'file-json'", &[])
            .await
            .expect("file delete should tombstone plugin-owned rows");

        let rows = session
            .execute(
                "SELECT COUNT(*) FROM test_json_entity WHERE lixcol_file_id = 'file-json'",
                &[],
            )
            .await
            .expect("plugin-owned row should disappear after file delete");
        assert_eq!(rows.rows()[0].values(), &[Value::Integer(0)]);
    });
}

fn build_detect_changes_component() -> Vec<u8> {
    let fixture_dir = fixture_dir();
    let manifest_path = fixture_dir.join("Cargo.toml");
    let target_dir = TempDir::new().expect("fixture target dir should be created");
    let status = Command::new("cargo")
        .args([
            "component",
            "build",
            "--locked",
            "--manifest-path",
            manifest_path
                .to_str()
                .expect("fixture manifest path should be UTF-8"),
            "--target",
            "wasm32-wasip2",
            "--target-dir",
            target_dir
                .path()
                .to_str()
                .expect("fixture target dir should be UTF-8"),
        ])
        .status()
        .expect("cargo component should run");
    assert!(
        status.success(),
        "fixture plugin component build should succeed"
    );

    let wasm_path = component_wasm_path(target_dir.path());
    std::fs::read(&wasm_path).unwrap_or_else(|error| {
        panic!(
            "fixture plugin wasm should be readable at {}: {error}",
            wasm_path.display()
        )
    })
}

fn component_wasm_path(target_dir: &Path) -> PathBuf {
    [
        target_dir.join("wasm32-wasip1/debug/engine_test_plugin_detect_changes.wasm"),
        target_dir.join("wasm32-wasip2/debug/engine_test_plugin_detect_changes.wasm"),
    ]
    .into_iter()
    .find(|path| path.exists())
    .expect("cargo component should emit the fixture wasm")
}

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/plugin/fixtures/test-plugin-detect-changes")
}

fn test_plugin_archive(plugin_key: &str, wasm: &[u8]) -> Vec<u8> {
    let mut cursor = Cursor::new(Vec::new());
    {
        let mut zip = ZipWriter::new(&mut cursor);
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);

        zip.start_file("manifest.json", options)
            .expect("manifest zip entry should start");
        zip.write_all(test_manifest(plugin_key).to_string().as_bytes())
            .expect("manifest should write");

        zip.start_file("plugin.wasm", options)
            .expect("wasm zip entry should start");
        zip.write_all(wasm).expect("wasm should write");

        zip.start_file("schema/test_json_entity.json", options)
            .expect("schema zip entry should start");
        zip.write_all(test_schema().to_string().as_bytes())
            .expect("schema should write");

        zip.finish().expect("zip should finish");
    }
    cursor.into_inner()
}

fn test_manifest(plugin_key: &str) -> serde_json::Value {
    json!({
        "key": plugin_key,
        "runtime": "wasm-component-v1",
        "api_version": "0.1.0",
        "match": {
            "path_glob": "*.json"
        },
        "detect_changes": {
            "state_context": {
                "include_active_state": true,
                "columns": [
                    "entity_id",
                    "schema_key",
                    "snapshot_content",
                    "file_id",
                    "plugin_key"
                ]
            }
        },
        "entry": "plugin.wasm",
        "schemas": ["schema/test_json_entity.json"]
    })
}

fn test_schema() -> serde_json::Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "x-lix-key": "test_json_entity",
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "properties": {
            "id": { "type": "string" },
            "value": { "type": "string" }
        },
        "required": ["id", "value"],
        "additionalProperties": false
    })
}
