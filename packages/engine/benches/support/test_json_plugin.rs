use async_trait::async_trait;
use lix_engine::{LixError, WasmComponentInstance, WasmLimits, WasmRuntime};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::io::{Cursor, Write};
use std::sync::Arc;
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

pub const TEST_PLUGIN_MANIFEST_JSON: &str = r#"{
  "key": "test_json_plugin",
  "runtime": "wasm-component-v1",
  "api_version": "0.1.0",
  "match": {"path_glob": "*.json"},
  "entry": "plugin.wasm",
  "schemas": ["schema/test_json_pointer.json"]
}"#;

pub const TEST_JSON_POINTER_SCHEMA_DEFINITION: &str = r#"{"value":{"x-lix-key":"test_json_pointer","x-lix-version":"1","type":"object","properties":{"path":{"type":"string"},"value":{}},"required":["path","value"],"additionalProperties":false}}"#;
pub const TEST_JSON_POINTER_SCHEMA_JSON: &str = r#"{"x-lix-key":"test_json_pointer","x-lix-version":"1","type":"object","properties":{"path":{"type":"string"},"value":{}},"required":["path","value"],"additionalProperties":false}"#;

pub fn dummy_wasm_header() -> [u8; 8] {
    [0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]
}

pub fn build_test_plugin_archive() -> Result<Vec<u8>, LixError> {
    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));

    writer
        .start_file("manifest.json", options)
        .map_err(|error| LixError {
            message: format!("failed to start manifest.json for benchmark plugin archive: {error}"),
        })?;
    writer
        .write_all(TEST_PLUGIN_MANIFEST_JSON.as_bytes())
        .map_err(|error| LixError {
            message: format!("failed to write manifest.json for benchmark plugin archive: {error}"),
        })?;

    writer
        .start_file("plugin.wasm", options)
        .map_err(|error| LixError {
            message: format!("failed to start plugin.wasm for benchmark plugin archive: {error}"),
        })?;
    writer
        .write_all(&dummy_wasm_header())
        .map_err(|error| LixError {
            message: format!("failed to write plugin.wasm for benchmark plugin archive: {error}"),
        })?;

    writer
        .start_file("schema/test_json_pointer.json", options)
        .map_err(|error| LixError {
            message: format!(
                "failed to start schema/test_json_pointer.json for benchmark plugin archive: {error}"
            ),
        })?;
    writer
        .write_all(TEST_JSON_POINTER_SCHEMA_JSON.as_bytes())
        .map_err(|error| LixError {
            message: format!(
                "failed to write schema/test_json_pointer.json for benchmark plugin archive: {error}"
            ),
        })?;

    writer
        .finish()
        .map(|cursor| cursor.into_inner())
        .map_err(|error| LixError {
            message: format!("failed to finalize benchmark plugin archive: {error}"),
        })
}

#[derive(Default)]
pub struct BenchJsonPluginRuntime;

#[derive(Default)]
struct BenchJsonPluginInstance;

#[derive(Debug, Deserialize)]
struct WirePluginFile {
    data: Vec<u8>,
}

#[derive(Debug, Deserialize)]
struct WireDetectChangesRequest {
    #[allow(dead_code)]
    before: Option<WirePluginFile>,
    after: WirePluginFile,
}

#[derive(Debug, Deserialize)]
struct WireApplyChangesRequest {
    file: WirePluginFile,
}

#[derive(Debug, Serialize)]
struct WirePluginEntityChange {
    entity_id: String,
    schema_key: String,
    schema_version: String,
    snapshot_content: Option<String>,
}

#[async_trait(?Send)]
impl WasmRuntime for BenchJsonPluginRuntime {
    async fn init_component(
        &self,
        bytes: Vec<u8>,
        _limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
        if bytes.is_empty() {
            return Err(LixError {
                message: "benchmark runtime received empty wasm bytes".to_string(),
            });
        }
        Ok(Arc::new(BenchJsonPluginInstance))
    }
}

#[async_trait(?Send)]
impl WasmComponentInstance for BenchJsonPluginInstance {
    async fn call(&self, export: &str, input: &[u8]) -> Result<Vec<u8>, LixError> {
        match export {
            "detect-changes" | "api#detect-changes" => detect_changes(input),
            "apply-changes" | "api#apply-changes" => apply_changes(input),
            other => Err(LixError {
                message: format!("unsupported benchmark export: {other}"),
            }),
        }
    }
}

fn detect_changes(input: &[u8]) -> Result<Vec<u8>, LixError> {
    let request: WireDetectChangesRequest =
        serde_json::from_slice(input).map_err(|error| LixError {
            message: format!("benchmark runtime: failed to decode detect-changes payload: {error}"),
        })?;
    let value: JsonValue =
        serde_json::from_slice(&request.after.data).map_err(|error| LixError {
            message: format!("benchmark runtime: after.data is invalid JSON: {error}"),
        })?;

    let mut changes = Vec::new();
    collect_nodes_as_changes("", &value, &mut changes)?;
    serde_json::to_vec(&changes).map_err(|error| LixError {
        message: format!("benchmark runtime: failed to encode detect-changes output: {error}"),
    })
}

fn apply_changes(input: &[u8]) -> Result<Vec<u8>, LixError> {
    let request: WireApplyChangesRequest =
        serde_json::from_slice(input).map_err(|error| LixError {
            message: format!("benchmark runtime: failed to decode apply-changes payload: {error}"),
        })?;
    Ok(request.file.data)
}

fn collect_nodes_as_changes(
    path: &str,
    value: &JsonValue,
    out: &mut Vec<WirePluginEntityChange>,
) -> Result<(), LixError> {
    out.push(WirePluginEntityChange {
        entity_id: path.to_string(),
        schema_key: "test_json_pointer".to_string(),
        schema_version: "1".to_string(),
        snapshot_content: Some(json!({ "path": path, "value": value }).to_string()),
    });

    match value {
        JsonValue::Object(object) => {
            for (key, child) in object {
                let child_path = format!("{}/{}", path, escape_json_pointer_token(key));
                collect_nodes_as_changes(&child_path, child, out)?;
            }
        }
        JsonValue::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                let child_path = format!("{}/{}", path, index);
                collect_nodes_as_changes(&child_path, child, out)?;
            }
        }
        _ => {}
    }

    Ok(())
}

fn escape_json_pointer_token(token: &str) -> String {
    token.replace('~', "~0").replace('/', "~1")
}
