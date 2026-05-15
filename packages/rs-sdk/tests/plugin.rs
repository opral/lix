use std::io::{Cursor, Write};
use std::sync::Arc;

use async_trait::async_trait;
use lix_rs_sdk::{
    open_lix, LixError, OpenLixOptions, RegisterPluginOptions, Value, WasmComponentInstance,
    WasmLimits, WasmRuntime,
};
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

#[tokio::test]
async fn rs_sdk_installs_mock_plugin_and_detects_file_changes() {
    let lix = open_lix(OpenLixOptions {
        backend: None,
        wasm_runtime: Some(Arc::new(MockPluginRuntime)),
    })
    .await
    .unwrap();

    let receipt = lix
        .register_plugin(RegisterPluginOptions {
            bytes: mock_plugin_archive(),
        })
        .await
        .expect("mock plugin should install through rs-sdk");
    assert_eq!(receipt.plugin_key, "rs_sdk_mock_plugin");

    lix.execute(
        "INSERT INTO lix_file (id, path, data) VALUES ('rs-sdk-json', '/rs-sdk.json', $1)",
        &[Value::Blob(br#"{"from":"rs-sdk"}"#.to_vec())],
    )
    .await
    .expect("file write should invoke mock plugin detect_changes");

    let rows = lix
        .execute(
            "SELECT id, value, lixcol_file_id FROM rs_sdk_mock_entity WHERE id = 'entity-1'",
            &[],
        )
        .await
        .expect("mock plugin semantic row should be queryable");

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows.rows()[0].values(),
        &[
            Value::Text("entity-1".to_string()),
            Value::Text(r#"{"from":"rs-sdk"}"#.to_string()),
            Value::Text("rs-sdk-json".to_string()),
        ]
    );

    lix.close().await.unwrap();
}

#[derive(Debug, Clone, Copy)]
struct MockPluginRuntime;

#[async_trait]
impl WasmRuntime for MockPluginRuntime {
    async fn init_component(
        &self,
        _bytes: Vec<u8>,
        _limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
        Ok(Arc::new(MockPluginInstance))
    }
}

#[derive(Debug, Clone, Copy)]
struct MockPluginInstance;

#[async_trait]
impl WasmComponentInstance for MockPluginInstance {
    async fn call(&self, export: &str, input: &[u8]) -> Result<Vec<u8>, LixError> {
        if !matches!(export, "detect-changes" | "api#detect-changes") {
            return Err(LixError::unknown(format!(
                "mock plugin received unexpected export '{export}'"
            )));
        }
        let input: serde_json::Value = serde_json::from_slice(input)
            .map_err(|error| LixError::unknown(format!("mock input must be JSON: {error}")))?;
        let data = input
            .get("after")
            .and_then(|after| after.get("data"))
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| LixError::unknown("mock input is missing after.data"))?
            .iter()
            .map(|byte| {
                byte.as_u64()
                    .and_then(|value| u8::try_from(value).ok())
                    .ok_or_else(|| LixError::unknown("mock after.data must be bytes"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let value = String::from_utf8(data).map_err(|error| {
            LixError::unknown(format!("mock after.data must be UTF-8: {error}"))
        })?;
        let output = serde_json::json!([
            {
                "entity_id": "entity-1",
                "schema_key": "rs_sdk_mock_entity",
                "snapshot_content": serde_json::json!({
                    "id": "entity-1",
                    "value": value,
                }).to_string(),
            }
        ]);
        serde_json::to_vec(&output).map_err(|error| {
            LixError::unknown(format!("mock output serialization failed: {error}"))
        })
    }
}

fn mock_plugin_archive() -> Vec<u8> {
    let mut cursor = Cursor::new(Vec::new());
    {
        let mut zip = ZipWriter::new(&mut cursor);
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
        zip.start_file("manifest.json", options)
            .expect("manifest entry should start");
        zip.write_all(mock_plugin_manifest().to_string().as_bytes())
            .expect("manifest should write");
        zip.start_file("plugin.wasm", options)
            .expect("wasm entry should start");
        zip.write_all(b"\0asm\x01\0\0\0")
            .expect("wasm should write");
        zip.start_file("schema/rs_sdk_mock_entity.json", options)
            .expect("schema entry should start");
        zip.write_all(mock_plugin_schema().to_string().as_bytes())
            .expect("schema should write");
        zip.finish().expect("zip should finish");
    }
    cursor.into_inner()
}

fn mock_plugin_manifest() -> serde_json::Value {
    serde_json::json!({
        "key": "rs_sdk_mock_plugin",
        "runtime": "wasm-component-v1",
        "api_version": "0.1.0",
        "match": {
            "path_glob": "*.json"
        },
        "entry": "plugin.wasm",
        "schemas": ["schema/rs_sdk_mock_entity.json"]
    })
}

fn mock_plugin_schema() -> serde_json::Value {
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "x-lix-key": "rs_sdk_mock_entity",
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
