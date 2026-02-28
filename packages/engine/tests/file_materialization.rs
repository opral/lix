mod support;

use async_trait::async_trait;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, OnceLock};

use lix_engine::{
    LixError, MaterializationDebugMode, MaterializationRequest, MaterializationScope, Value,
    WasmComponentInstance, WasmLimits, WasmRuntime,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

const TEST_PLUGIN_MANIFEST: &str = r#"{
  "key": "json",
  "runtime": "wasm-component-v1",
  "api_version": "0.1.0",
  "match": {"path_glob": "*.json"},
  "entry": "plugin.wasm"
}"#;

const TEST_TXT_PLUGIN_MANIFEST: &str = r#"{
  "key": "txt_noop",
  "runtime": "wasm-component-v1",
  "api_version": "0.1.0",
  "match": {"path_glob": "*.txt"},
  "entry": "plugin.wasm"
}"#;

const TEST_TXT_NOOP_PLUGIN_WASM_BYTES: &[u8] = b"\0asm\x01\0\0\0lix-test-txt-noop-plugin-v1";

#[derive(Debug, Deserialize)]
struct WirePluginFile {
    id: String,
    path: String,
    data: Vec<u8>,
}

#[derive(Debug, Deserialize)]
struct WirePluginEntityChange {
    entity_id: String,
    schema_key: String,
    schema_version: String,
    snapshot_content: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WireDetectChangesRequest {
    before: Option<WirePluginFile>,
    after: WirePluginFile,
}

#[derive(Debug, Deserialize)]
struct WireApplyChangesRequest {
    file: WirePluginFile,
    changes: Vec<WirePluginEntityChange>,
}

#[derive(Debug, Serialize)]
struct WirePluginEntityChangeOutput {
    entity_id: String,
    schema_key: String,
    schema_version: String,
    snapshot_content: Option<String>,
}

#[derive(Debug, Default)]
struct PathEchoRuntime;

#[derive(Debug, Default)]
struct PathEchoInstance;

#[derive(Debug, Default)]
struct BeforeAwareRuntime;

#[derive(Debug, Default)]
struct BeforeAwareInstance;

struct JsonWithTxtNoopRuntime {
    inner: support::wasmtime_runtime::TestWasmtimeRuntime,
}

#[derive(Debug, Default)]
struct TxtNoopInstance;

impl JsonWithTxtNoopRuntime {
    fn new() -> Self {
        Self {
            inner: support::wasmtime_runtime::TestWasmtimeRuntime::new()
                .expect("test wasmtime runtime should initialize"),
        }
    }
}

#[async_trait(?Send)]
impl WasmRuntime for PathEchoRuntime {
    async fn init_component(
        &self,
        _bytes: Vec<u8>,
        _limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
        Ok(Arc::new(PathEchoInstance))
    }
}

#[async_trait(?Send)]
impl WasmComponentInstance for PathEchoInstance {
    async fn call(&self, export: &str, input: &[u8]) -> Result<Vec<u8>, LixError> {
        match export {
            "detect-changes" | "api#detect-changes" => {
                let request: WireDetectChangesRequest =
                    serde_json::from_slice(input).map_err(|error| LixError {
                        message: format!("failed to decode detect-changes payload: {error}"),
                    })?;
                let _ = (
                    request
                        .before
                        .as_ref()
                        .map(|file| (&file.id, &file.path, &file.data)),
                    (&request.after.id, &request.after.path, &request.after.data),
                );
                serde_json::to_vec(&vec![WirePluginEntityChangeOutput {
                    entity_id: "".to_string(),
                    schema_key: "json_pointer".to_string(),
                    schema_version: "1".to_string(),
                    snapshot_content: Some(r#"{"path":"","value":{}}"#.to_string()),
                }])
                .map_err(|error| LixError {
                    message: format!("failed to encode detect-changes response: {error}"),
                })
            }
            "apply-changes" | "api#apply-changes" => {
                let request: WireApplyChangesRequest =
                    serde_json::from_slice(input).map_err(|error| LixError {
                        message: format!("failed to decode apply-changes payload: {error}"),
                    })?;
                let _ = request.changes.iter().all(|change| {
                    !change.entity_id.is_empty()
                        || !change.schema_key.is_empty()
                        || !change.schema_version.is_empty()
                        || change.snapshot_content.is_some()
                });
                Ok(request.file.path.into_bytes())
            }
            other => Err(LixError {
                message: format!("unsupported test export: {other}"),
            }),
        }
    }
}

#[async_trait(?Send)]
impl WasmRuntime for BeforeAwareRuntime {
    async fn init_component(
        &self,
        _bytes: Vec<u8>,
        _limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
        Ok(Arc::new(BeforeAwareInstance))
    }
}

#[async_trait(?Send)]
impl WasmComponentInstance for BeforeAwareInstance {
    async fn call(&self, export: &str, input: &[u8]) -> Result<Vec<u8>, LixError> {
        match export {
            "detect-changes" | "api#detect-changes" => {
                let request: WireDetectChangesRequest =
                    serde_json::from_slice(input).map_err(|error| LixError {
                        message: format!("failed to decode detect-changes payload: {error}"),
                    })?;
                let marker = match request.before {
                    None => "none",
                    Some(file) if file.data.is_empty() => "empty",
                    Some(_) => "non-empty",
                };
                let snapshot_content =
                    serde_json::json!({"path":"/before","value":marker}).to_string();
                serde_json::to_vec(&vec![WirePluginEntityChangeOutput {
                    entity_id: "/before".to_string(),
                    schema_key: "json_pointer".to_string(),
                    schema_version: "1".to_string(),
                    snapshot_content: Some(snapshot_content),
                }])
                .map_err(|error| LixError {
                    message: format!("failed to encode detect-changes response: {error}"),
                })
            }
            "apply-changes" | "api#apply-changes" => {
                let request: WireApplyChangesRequest =
                    serde_json::from_slice(input).map_err(|error| LixError {
                        message: format!("failed to decode apply-changes payload: {error}"),
                    })?;
                let _ = request;
                Ok(b"reconstructed-before".to_vec())
            }
            other => Err(LixError {
                message: format!("unsupported test export: {other}"),
            }),
        }
    }
}

#[async_trait(?Send)]
impl WasmRuntime for JsonWithTxtNoopRuntime {
    async fn init_component(
        &self,
        bytes: Vec<u8>,
        limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
        if bytes.as_slice() == TEST_TXT_NOOP_PLUGIN_WASM_BYTES {
            return Ok(Arc::new(TxtNoopInstance));
        }
        self.inner.init_component(bytes, limits).await
    }
}

#[async_trait(?Send)]
impl WasmComponentInstance for TxtNoopInstance {
    async fn call(&self, export: &str, input: &[u8]) -> Result<Vec<u8>, LixError> {
        match export {
            "detect-changes" | "api#detect-changes" => Ok(b"[]".to_vec()),
            "apply-changes" | "api#apply-changes" => {
                let request: WireApplyChangesRequest =
                    serde_json::from_slice(input).map_err(|error| LixError {
                        message: format!("failed to decode apply-changes payload: {error}"),
                    })?;
                Ok(request.file.data)
            }
            other => Err(LixError {
                message: format!("unsupported test export: {other}"),
            }),
        }
    }
}

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

fn plugin_txt_noop_wasm_bytes() -> Vec<u8> {
    TEST_TXT_NOOP_PLUGIN_WASM_BYTES.to_vec()
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

async fn active_version_id(engine: &support::simulation_test::SimulationEngine) -> String {
    let rows = engine
        .execute(
            "SELECT version_id FROM lix_active_version ORDER BY id LIMIT 1",
            &[],
        )
        .await
        .expect("active version query should succeed");
    assert_eq!(rows.rows.len(), 1);
    match &rows.rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected active version id text, got {other:?}"),
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
            wasm_runtime: runtime,
            access_to_internal: true,
        }))
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.expect("engine init should succeed");
    register_plugin_schema(&engine).await;
    let main_version_id = main_version_id(&engine).await;
    let plugin_wasm = plugin_json_v2_wasm_bytes();
    let plugin_archive =
        support::simulation_test::build_test_plugin_archive(TEST_PLUGIN_MANIFEST, &plugin_wasm)
            .expect("build test plugin archive should succeed");
    engine
        .install_plugin(&plugin_archive)
        .await
        .expect("install_plugin should succeed");
    (engine, main_version_id)
}

async fn boot_engine_with_path_echo_plugin(
    sim: &support::simulation_test::SimulationArgs,
) -> (support::simulation_test::SimulationEngine, String) {
    let runtime = Arc::new(PathEchoRuntime) as Arc<dyn WasmRuntime>;

    let engine = sim
        .boot_simulated_engine(Some(support::simulation_test::SimulationBootArgs {
            key_values: Vec::new(),
            active_account: None,
            wasm_runtime: runtime,
            access_to_internal: true,
        }))
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.expect("engine init should succeed");
    register_plugin_schema(&engine).await;
    let main_version_id = main_version_id(&engine).await;
    let plugin_wasm = plugin_json_v2_wasm_bytes();
    let plugin_archive =
        support::simulation_test::build_test_plugin_archive(TEST_PLUGIN_MANIFEST, &plugin_wasm)
            .expect("build test plugin archive should succeed");
    engine
        .install_plugin(&plugin_archive)
        .await
        .expect("install_plugin should succeed");
    (engine, main_version_id)
}

async fn boot_engine_with_before_aware_plugin(
    sim: &support::simulation_test::SimulationArgs,
) -> (support::simulation_test::SimulationEngine, String) {
    let runtime = Arc::new(BeforeAwareRuntime) as Arc<dyn WasmRuntime>;

    let engine = sim
        .boot_simulated_engine(Some(support::simulation_test::SimulationBootArgs {
            key_values: Vec::new(),
            active_account: None,
            wasm_runtime: runtime,
            access_to_internal: true,
        }))
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.expect("engine init should succeed");
    register_plugin_schema(&engine).await;
    let main_version_id = main_version_id(&engine).await;
    let plugin_wasm = plugin_json_v2_wasm_bytes();
    let plugin_archive =
        support::simulation_test::build_test_plugin_archive(TEST_PLUGIN_MANIFEST, &plugin_wasm)
            .expect("build test plugin archive should succeed");
    engine
        .install_plugin(&plugin_archive)
        .await
        .expect("install_plugin should succeed");
    (engine, main_version_id)
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

async fn json_pointer_change_count(
    engine: &support::simulation_test::SimulationEngine,
    file_id: &str,
) -> i64 {
    let rows = engine
        .execute(
            &format!(
                "SELECT COUNT(*) \
                 FROM lix_internal_change \
                 WHERE file_id = '{}' \
                   AND schema_key = 'json_pointer'",
                file_id
            ),
            &[],
        )
        .await
        .expect("json_pointer change count query should succeed");
    assert_eq!(rows.rows.len(), 1);
    value_as_i64(&rows.rows[0][0])
}

fn assert_blob_json_eq(value: &Value, expected: JsonValue) {
    let bytes = match value {
        Value::Blob(bytes) => bytes,
        other => panic!("expected blob value, got {other:?}"),
    };
    let actual: JsonValue = serde_json::from_slice(bytes).expect("blob should contain valid JSON");
    assert_eq!(actual, expected);
}

fn assert_blob_bytes_eq(value: &Value, expected: &[u8]) {
    let bytes = match value {
        Value::Blob(bytes) => bytes,
        other => panic!("expected blob value, got {other:?}"),
    };
    assert_eq!(bytes.as_slice(), expected);
}

fn value_as_i64(value: &Value) -> i64 {
    match value {
        Value::Integer(v) => *v,
        other => panic!("expected integer value, got {other:?}"),
    }
}

fn value_as_text(value: &Value) -> String {
    match value {
        Value::Text(v) => v.clone(),
        other => panic!("expected text value, got {other:?}"),
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

async fn total_file_cache_row_count_for_prefix(
    engine: &support::simulation_test::SimulationEngine,
    file_id_prefix: &str,
) -> i64 {
    let rows = engine
        .execute(
            &format!(
                "SELECT COUNT(*) \
                 FROM lix_internal_file_data_cache \
                 WHERE file_id LIKE '{}%'",
                file_id_prefix
            ),
            &[],
        )
        .await
        .expect("total file_data_cache count query should succeed");
    assert_eq!(rows.rows.len(), 1);
    value_as_i64(&rows.rows[0][0])
}

async fn orphan_file_cache_row_count_for_prefix(
    engine: &support::simulation_test::SimulationEngine,
    file_id_prefix: &str,
) -> i64 {
    let rows = engine
        .execute(
            &format!(
                "SELECT COUNT(*) \
                 FROM lix_internal_file_data_cache c \
                 LEFT JOIN lix_state_by_version d \
                   ON d.schema_key = 'lix_file_descriptor' \
                  AND d.entity_id = c.file_id \
                  AND d.version_id = c.version_id \
                  AND d.snapshot_content IS NOT NULL \
                 WHERE c.file_id LIKE '{}%' \
                   AND d.entity_id IS NULL",
                file_id_prefix
            ),
            &[],
        )
        .await
        .expect("orphan file_data_cache count query should succeed");
    assert_eq!(rows.rows.len(), 1);
    value_as_i64(&rows.rows[0][0])
}

async fn binary_blob_hash_for_file_version(
    engine: &support::simulation_test::SimulationEngine,
    file_id: &str,
    version_id: &str,
) -> Option<String> {
    let rows = engine
        .execute(
            &format!(
                "SELECT blob_hash \
                 FROM lix_internal_binary_file_version_ref \
                 WHERE file_id = '{}' AND version_id = '{}' \
                 LIMIT 1",
                file_id, version_id
            ),
            &[],
        )
        .await
        .expect("binary file_version_ref lookup should succeed");
    rows.rows.first().map(|row| value_as_text(&row[0]))
}

async fn binary_chunk_hash_for_blob(
    engine: &support::simulation_test::SimulationEngine,
    blob_hash: &str,
) -> Option<String> {
    let rows = engine
        .execute(
            &format!(
                "SELECT chunk_hash \
                 FROM lix_internal_binary_blob_manifest_chunk \
                 WHERE blob_hash = '{}' \
                 ORDER BY chunk_index \
                 LIMIT 1",
                blob_hash
            ),
            &[],
        )
        .await
        .expect("binary manifest chunk lookup should succeed");
    rows.rows.first().map(|row| value_as_text(&row[0]))
}

async fn binary_manifest_row_count_by_hash(
    engine: &support::simulation_test::SimulationEngine,
    blob_hash: &str,
) -> i64 {
    let rows = engine
        .execute(
            &format!(
                "SELECT COUNT(*) \
                 FROM lix_internal_binary_blob_manifest \
                 WHERE blob_hash = '{}'",
                blob_hash
            ),
            &[],
        )
        .await
        .expect("binary manifest row count query should succeed");
    assert_eq!(rows.rows.len(), 1);
    value_as_i64(&rows.rows[0][0])
}

async fn orphan_binary_chunk_row_count(engine: &support::simulation_test::SimulationEngine) -> i64 {
    let rows = engine
        .execute(
            "SELECT COUNT(*) \
             FROM lix_internal_binary_chunk_store c \
             LEFT JOIN lix_internal_binary_blob_manifest_chunk mc \
               ON mc.chunk_hash = c.chunk_hash \
             WHERE mc.chunk_hash IS NULL",
            &[],
        )
        .await
        .expect("orphan binary chunk count query should succeed");
    assert_eq!(rows.rows.len(), 1);
    value_as_i64(&rows.rows[0][0])
}

async fn orphan_binary_manifest_chunk_row_count(
    engine: &support::simulation_test::SimulationEngine,
) -> i64 {
    let rows = engine
        .execute(
            "SELECT COUNT(*) \
             FROM lix_internal_binary_blob_manifest_chunk mc \
             LEFT JOIN lix_internal_binary_blob_manifest m \
               ON m.blob_hash = mc.blob_hash \
             WHERE m.blob_hash IS NULL",
            &[],
        )
        .await
        .expect("orphan binary manifest chunk count query should succeed");
    assert_eq!(rows.rows.len(), 1);
    value_as_i64(&rows.rows[0][0])
}

async fn binary_codec_counts_for_blob(
    engine: &support::simulation_test::SimulationEngine,
    blob_hash: &str,
) -> (i64, i64, i64) {
    let rows = engine
        .execute(
            &format!(
                "SELECT \
                     COALESCE(SUM(CASE WHEN cs.codec = 'raw' THEN 1 ELSE 0 END), 0), \
                     COALESCE(SUM(CASE WHEN cs.codec = 'zstd' THEN 1 ELSE 0 END), 0), \
                     COALESCE(SUM(CASE WHEN cs.codec = 'legacy' OR cs.codec IS NULL THEN 1 ELSE 0 END), 0) \
                 FROM lix_internal_binary_blob_manifest_chunk mc \
                 JOIN lix_internal_binary_chunk_store cs ON cs.chunk_hash = mc.chunk_hash \
                 WHERE mc.blob_hash = '{}'",
                blob_hash
            ),
            &[],
        )
        .await
        .expect("binary codec counts query should succeed");
    assert_eq!(rows.rows.len(), 1);
    (
        value_as_i64(&rows.rows[0][0]),
        value_as_i64(&rows.rows[0][1]),
        value_as_i64(&rows.rows[0][2]),
    )
}

async fn binary_prefixed_chunk_payload_count_for_blob(
    engine: &support::simulation_test::SimulationEngine,
    blob_hash: &str,
) -> i64 {
    let sqlite_style_query = format!(
        "SELECT COUNT(*) \
         FROM lix_internal_binary_blob_manifest_chunk mc \
         JOIN lix_internal_binary_chunk_store cs ON cs.chunk_hash = mc.chunk_hash \
         WHERE mc.blob_hash = '{}' \
           AND hex(substr(cs.data, 1, 8)) IN ('4C49585241573031', '4C49585A53544431')",
        blob_hash
    );
    let postgres_style_query = format!(
        "SELECT COUNT(*) \
         FROM lix_internal_binary_blob_manifest_chunk mc \
         JOIN lix_internal_binary_chunk_store cs ON cs.chunk_hash = mc.chunk_hash \
         WHERE mc.blob_hash = '{}' \
           AND encode(substring(cs.data from 1 for 8), 'hex') IN ('4c49585241573031', '4c49585a53544431')",
        blob_hash
    );

    let rows = match engine.execute(&sqlite_style_query, &[]).await {
        Ok(rows) => rows,
        Err(_) => engine
            .execute(&postgres_style_query, &[])
            .await
            .expect("prefixed binary chunk payload count query should succeed"),
    };
    assert_eq!(rows.rows.len(), 1);
    value_as_i64(&rows.rows[0][0])
}

async fn boot_engine_with_json_plugin_and_txt_noop_runtime(
    sim: &support::simulation_test::SimulationArgs,
) -> (support::simulation_test::SimulationEngine, String) {
    let runtime = Arc::new(JsonWithTxtNoopRuntime::new()) as Arc<dyn WasmRuntime>;

    let engine = sim
        .boot_simulated_engine(Some(support::simulation_test::SimulationBootArgs {
            key_values: Vec::new(),
            active_account: None,
            wasm_runtime: runtime,
            access_to_internal: true,
        }))
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.expect("engine init should succeed");
    register_plugin_schema(&engine).await;
    let main_version_id = main_version_id(&engine).await;
    let plugin_wasm = plugin_json_v2_wasm_bytes();
    let plugin_archive =
        support::simulation_test::build_test_plugin_archive(TEST_PLUGIN_MANIFEST, &plugin_wasm)
            .expect("build test plugin archive should succeed");
    engine
        .install_plugin(&plugin_archive)
        .await
        .expect("install_plugin should succeed");
    (engine, main_version_id)
}

simulation_test!(
    file_write_uses_builtin_binary_fallback_when_no_plugin_matches_file_type,
    simulations = [sqlite],
    |sim| async move {
        let runtime = Arc::new(PathEchoRuntime) as Arc<dyn WasmRuntime>;
        let engine = sim
            .boot_simulated_engine(Some(support::simulation_test::SimulationBootArgs {
                key_values: Vec::new(),
                active_account: None,
                wasm_runtime: runtime,
                access_to_internal: true,
            }))
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.expect("engine init should succeed");

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-no-plugin', '/assets/video.mp4', lix_text_encode('ignored'))",
                &[],
            )
            .await
            .expect("write should use builtin binary fallback");

        let rows = engine
            .execute(
                "SELECT snapshot_content \
                 FROM lix_state_by_version \
                 WHERE file_id = 'file-no-plugin' \
                   AND schema_key = 'lix_binary_blob_ref' \
                   AND plugin_key = 'lix_builtin_binary_fallback' \
                   AND snapshot_content IS NOT NULL \
                 LIMIT 1",
                &[],
            )
            .await
            .expect("builtin fallback state row query should succeed");
        assert_eq!(rows.rows.len(), 1);
        let snapshot = match &rows.rows[0][0] {
            Value::Text(value) => value.clone(),
            other => panic!("expected snapshot text, got {other:?}"),
        };
        let parsed: JsonValue =
            serde_json::from_str(&snapshot).expect("fallback snapshot should be valid JSON");
        assert_eq!(
            parsed
                .get("id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default(),
            "file-no-plugin"
        );
        assert_eq!(
            parsed
                .get("size_bytes")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or_default(),
            7
        );
    }
);

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
                wasm_runtime: runtime,
                access_to_internal: true,
            }))
            .await
            .expect("boot_simulated_engine should succeed");

        engine.init().await.expect("engine init should succeed");
        register_plugin_schema(&engine).await;
        let main_version_id = main_version_id(&engine).await;
        let plugin_wasm = plugin_json_v2_wasm_bytes();

        let plugin_archive =
            support::simulation_test::build_test_plugin_archive(TEST_PLUGIN_MANIFEST, &plugin_wasm)
                .expect("build test plugin archive should succeed");
        engine
            .install_plugin(&plugin_archive)
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
                wasm_runtime: runtime,
                access_to_internal: true,
            }))
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.expect("engine init should succeed");
        register_plugin_schema(&engine).await;
        let main_version_id = main_version_id(&engine).await;
        let plugin_wasm = plugin_json_v2_wasm_bytes();

        let plugin_archive =
            support::simulation_test::build_test_plugin_archive(TEST_PLUGIN_MANIFEST, &plugin_wasm)
                .expect("build test plugin archive should succeed");
        engine
            .install_plugin(&plugin_archive)
            .await
            .expect("install_plugin should succeed");

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json', '/config.json', lix_text_encode('{\"hello\":\"from-write\"}'))",
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
                wasm_runtime: runtime,
                access_to_internal: true,
            }))
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.expect("engine init should succeed");
        register_plugin_schema(&engine).await;
        let main_version_id = main_version_id(&engine).await;
        let plugin_wasm = plugin_json_v2_wasm_bytes();

        let plugin_archive =
            support::simulation_test::build_test_plugin_archive(TEST_PLUGIN_MANIFEST, &plugin_wasm)
                .expect("build test plugin archive should succeed");
        engine
            .install_plugin(&plugin_archive)
            .await
            .expect("install_plugin should succeed");

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json-update', '/config.json', lix_text_encode('{\"hello\":\"before\",\"remove\":\"soon-gone\"}'))",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

        engine
            .execute(
                "UPDATE lix_file \
                 SET data = lix_text_encode('{\"hello\":\"after\",\"add\":\"new-value\"}') \
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

        let file_rows = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'file-json-update' LIMIT 1",
                &[],
            )
            .await
            .expect("lix_file query should succeed");
        assert_eq!(file_rows.rows.len(), 1);
        assert_blob_json_eq(
            &file_rows.rows[0][0],
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
                 VALUES ('file-json-param', '/config.json', lix_text_encode('{\"hello\":\"before\",\"drop\":\"gone\"}'))",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

        engine
            .execute(
                "UPDATE lix_file SET data = $1 WHERE id = $2",
                &[
                    Value::Blob(b"{\"hello\":\"after-param\",\"new\":1}".to_vec()),
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

        let file_rows = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'file-json-param' LIMIT 1",
                &[],
            )
            .await
            .expect("lix_file query should succeed");
        assert_eq!(file_rows.rows.len(), 1);
        assert_blob_json_eq(
            &file_rows.rows[0][0],
            serde_json::json!({"hello":"after-param","new":1}),
        );
    }
);

simulation_test!(
    file_update_path_and_data_uses_single_commit_for_descriptor_and_plugin_changes,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json-combined', '/combined.json', lix_text_encode('{\"hello\":\"before\"}'))",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

        let before_commit_id = active_version_commit_id(&engine).await;

        engine
            .execute(
                "UPDATE lix_file \
                 SET path = '/combined-renamed.json', \
                     data = lix_text_encode('{\"hello\":\"after\",\"added\":true}') \
                 WHERE id = 'file-json-combined'",
                &[],
            )
            .await
            .expect("combined file update should succeed");

        let after_commit_id = active_version_commit_id(&engine).await;
        assert_ne!(after_commit_id, before_commit_id);

        let edge_entity_id = format!("{}~{}", before_commit_id, after_commit_id);
        let edge_rows = engine
            .execute(
                &format!(
                    "SELECT COUNT(*) \
                     FROM lix_internal_state_vtable \
                     WHERE schema_key = 'lix_commit_edge' \
                       AND entity_id = '{}' \
                       AND snapshot_content IS NOT NULL",
                    edge_entity_id
                ),
                &[],
            )
            .await
            .expect("commit edge query should succeed");
        assert_eq!(edge_rows.rows.len(), 1);
        assert_eq!(value_as_i64(&edge_rows.rows[0][0]), 1);

        let detected =
            detected_json_pointer_entities(&engine, "file-json-combined", &main_version_id).await;
        assert_eq!(
            detected,
            vec!["".to_string(), "/added".to_string(), "/hello".to_string()]
        );

        let file_rows = engine
            .execute(
                "SELECT path, data \
                 FROM lix_file \
                 WHERE id = 'file-json-combined' \
                 LIMIT 1",
                &[],
            )
            .await
            .expect("combined file read should succeed");
        assert_eq!(file_rows.rows.len(), 1);
        match &file_rows.rows[0][0] {
            Value::Text(value) => assert_eq!(value, "/combined-renamed.json"),
            other => panic!("expected path text, got {other:?}"),
        }
        assert_blob_json_eq(
            &file_rows.rows[0][1],
            serde_json::json!({"hello":"after","added":true}),
        );
    }
);

simulation_test!(
    direct_state_insert_refreshes_file_data_cache,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, _main_version_id) = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json-state-insert-cache', '/state-insert-cache.json', lix_text_encode('{\"content\":\"Start\"}'))",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

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
    }
);

simulation_test!(
    direct_state_update_refreshes_file_data_cache,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, _main_version_id) = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json-state-cache', '/state-cache.json', lix_text_encode('{\"content\":\"Start\"}'))",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

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
    }
);

simulation_test!(
    direct_state_delete_refreshes_file_data_cache,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, _main_version_id) = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json-state-delete-cache', '/state-delete-cache.json', lix_text_encode('{\"content\":\"Start\"}'))",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

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
                 VALUES ('file-json-state-by-version-insert-cache', '/state-by-version-insert-cache.json', lix_text_encode('{\"content\":\"Start\"}'))",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

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
                 VALUES ('file-json-state-by-version-cache', '/state-by-version-cache.json', lix_text_encode('{\"content\":\"Start\"}'))",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

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
                 VALUES ('file-json-state-by-version-delete-cache', '/state-by-version-delete-cache.json', lix_text_encode('{\"content\":\"Start\"}'))",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

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
                 VALUES ('file-json-path', '/before.json', lix_text_encode('{\"hello\":\"before\",\"remove\":true}'))",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

        engine
            .execute(
                "UPDATE lix_file \
                 SET path = '/after.json', data = lix_text_encode('{\"hello\":\"after-path\"}') \
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

        let file_rows = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'file-json-path' LIMIT 1",
                &[],
            )
            .await
            .expect("lix_file query should succeed");
        assert_eq!(file_rows.rows.len(), 1);
        assert_blob_json_eq(
            &file_rows.rows[0][0],
            serde_json::json!({"hello":"after-path"}),
        );
    }
);

simulation_test!(
    file_update_path_only_plugin_switch_tombstones_previous_plugin_entities,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) =
            boot_engine_with_json_plugin_and_txt_noop_runtime(&sim).await;
        let plugin_wasm = plugin_txt_noop_wasm_bytes();
        let plugin_archive = support::simulation_test::build_test_plugin_archive(
            TEST_TXT_PLUGIN_MANIFEST,
            &plugin_wasm,
        )
        .expect("build test plugin archive should succeed");
        engine
            .install_plugin(&plugin_archive)
            .await
            .expect("install txt plugin should succeed");

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json-path-only-switch', '/switch.json', lix_text_encode('{\"hello\":\"before\"}'))",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

        let before_entities =
            detected_json_pointer_entities(&engine, "file-json-path-only-switch", &main_version_id)
                .await;
        assert_eq!(before_entities, vec!["".to_string(), "/hello".to_string()]);

        engine
            .execute(
                "UPDATE lix_file \
                 SET path = '/switch.txt' \
                 WHERE id = 'file-json-path-only-switch'",
                &[],
            )
            .await
            .expect("path-only file update should succeed");

        let after_json_entities = engine
            .execute(
                &format!(
                    "SELECT entity_id \
                     FROM lix_state_by_version \
                     WHERE file_id = 'file-json-path-only-switch' \
                       AND version_id = '{}' \
                       AND schema_key = 'json_pointer' \
                       AND plugin_key = 'json' \
                       AND snapshot_content IS NOT NULL \
                     ORDER BY entity_id",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("post-switch json plugin entities query should succeed");
        assert!(
            after_json_entities.rows.is_empty(),
            "json plugin should have no live entities after switch"
        );

        let tombstone_rows = engine
            .execute(
                &format!(
                    "SELECT COUNT(*) \
                     FROM lix_internal_state_vtable \
                     WHERE file_id = 'file-json-path-only-switch' \
                       AND version_id = '{}' \
                       AND schema_key = 'json_pointer' \
                       AND plugin_key = 'json' \
                       AND snapshot_content IS NULL",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("json_pointer tombstone count query should succeed");
        assert_eq!(tombstone_rows.rows.len(), 1);
        assert_eq!(value_as_i64(&tombstone_rows.rows[0][0]), 2);

        let file_rows = engine
            .execute(
                "SELECT path \
                 FROM lix_file \
                 WHERE id = 'file-json-path-only-switch' \
                 LIMIT 1",
                &[],
            )
            .await
            .expect("path-only updated file read should succeed");
        assert_eq!(file_rows.rows.len(), 1);
        assert_eq!(file_rows.rows[0][0], Value::Text("/switch.txt".to_string()));
    }
);

simulation_test!(
    file_update_path_only_plugin_switch_does_not_write_non_authoritative_cache_data,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) =
            boot_engine_with_json_plugin_and_txt_noop_runtime(&sim).await;
        let plugin_wasm = plugin_txt_noop_wasm_bytes();
        let plugin_archive = support::simulation_test::build_test_plugin_archive(
            TEST_TXT_PLUGIN_MANIFEST,
            &plugin_wasm,
        )
        .expect("build test plugin archive should succeed");
        engine
            .install_plugin(&plugin_archive)
            .await
            .expect("install txt plugin should succeed");

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json-path-only-cache-guard', '/cache-guard.json', lix_text_encode('{\"hello\":\"before\"}'))",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

        assert_eq!(
            file_cache_row_count(&engine, "file-json-path-only-cache-guard", &main_version_id)
                .await,
            0
        );

        engine
            .execute(
                &format!(
                    "DELETE FROM lix_internal_file_data_cache \
                     WHERE file_id = 'file-json-path-only-cache-guard' \
                       AND version_id = '{}'",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("cache delete should succeed");
        assert_eq!(
            file_cache_row_count(&engine, "file-json-path-only-cache-guard", &main_version_id)
                .await,
            0
        );

        engine
            .execute(
                "UPDATE lix_file \
                 SET path = '/cache-guard.txt' \
                 WHERE id = 'file-json-path-only-cache-guard'",
                &[],
            )
            .await
            .expect("path-only file update should succeed");

        assert_eq!(
            file_cache_row_count(&engine, "file-json-path-only-cache-guard", &main_version_id)
                .await,
            0
        );
    }
);

simulation_test!(
    file_update_cache_miss_uses_reconstructed_before_data_for_detect_stage,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, _main_version_id) = boot_engine_with_before_aware_plugin(&sim).await;
        let active_version_id = active_version_id(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-before-aware', '/before-aware.json', lix_text_encode('{\"hello\":\"before\"}'))",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

        engine
            .execute(
                &format!(
                    "DELETE FROM lix_internal_file_data_cache \
                     WHERE file_id = 'file-before-aware' AND version_id = '{}'",
                    active_version_id
                ),
                &[],
            )
            .await
            .expect("cache delete should succeed");
        assert_eq!(
            file_cache_row_count(&engine, "file-before-aware", &active_version_id).await,
            0
        );

        engine
            .execute(
                "UPDATE lix_file \
                 SET data = lix_text_encode('{\"hello\":\"after\"}') \
                 WHERE id = 'file-before-aware'",
                &[],
            )
            .await
            .expect("file update should succeed");

        let rows = engine
            .execute(
                &format!(
                    "SELECT snapshot_content \
                     FROM lix_state_by_version \
                     WHERE file_id = 'file-before-aware' \
                       AND version_id = '{}' \
                       AND schema_key = 'json_pointer' \
                       AND entity_id = '/before' \
                     LIMIT 1",
                    active_version_id
                ),
                &[],
            )
            .await
            .expect("before marker query should succeed");
        assert_eq!(rows.rows.len(), 1);
        let snapshot = match &rows.rows[0][0] {
            Value::Text(value) => value,
            other => panic!("expected snapshot_content text, got {other:?}"),
        };
        assert!(
            snapshot.contains("\"value\":\"non-empty\""),
            "expected non-empty before marker, got: {}",
            snapshot
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
                     VALUES ('file-json-by-version', '/config.json', lix_text_encode('{{\"hello\":\"before\",\"remove\":\"gone\"}}'), '{}')",
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
                     SET data = lix_text_encode('{{\"hello\":\"after-by-version\",\"add\":\"v\"}}') \
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

        let file_rows = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_file_by_version \
                     WHERE id = 'file-json-by-version' \
                       AND lixcol_version_id = '{}' \
                     LIMIT 1",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("lix_file_by_version query should succeed");
        assert_eq!(file_rows.rows.len(), 1);
        assert_blob_json_eq(
            &file_rows.rows[0][0],
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
                 ('file-json-bulk-1', '/bulk-1.json', lix_text_encode('{\"old\":1}')), \
                 ('file-json-bulk-2', '/bulk-2.json', lix_text_encode('{\"old\":2}'))",
                &[],
            )
            .await
            .expect("bulk insert should succeed");

        engine
            .execute(
                "UPDATE lix_file \
                 SET data = lix_text_encode('{\"common\":\"updated\"}') \
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

        let file_one = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'file-json-bulk-1' LIMIT 1",
                &[],
            )
            .await
            .expect("lix_file query should succeed");
        let file_two = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'file-json-bulk-2' LIMIT 1",
                &[],
            )
            .await
            .expect("lix_file query should succeed");
        assert_eq!(file_one.rows.len(), 1);
        assert_eq!(file_two.rows.len(), 1);
        assert_blob_json_eq(
            &file_one.rows[0][0],
            serde_json::json!({"common":"updated"}),
        );
        assert_blob_json_eq(
            &file_two.rows[0][0],
            serde_json::json!({"common":"updated"}),
        );
    }
);

simulation_test!(
    file_insert_json_multi_statement_does_not_replay_detected_changes,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json-multi-insert-1', '/multi-insert-1.json', lix_text_encode('{\"first\":1}')); \
                 INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json-multi-insert-2', '/multi-insert-2.json', lix_text_encode('{\"second\":2}'))",
                &[],
            )
            .await
            .expect("multi-statement insert should succeed");

        let detected_one =
            detected_json_pointer_entities(&engine, "file-json-multi-insert-1", &main_version_id)
                .await;
        let detected_two =
            detected_json_pointer_entities(&engine, "file-json-multi-insert-2", &main_version_id)
                .await;
        assert_eq!(detected_one, vec!["".to_string(), "/first".to_string()]);
        assert_eq!(detected_two, vec!["".to_string(), "/second".to_string()]);

        assert_eq!(
            json_pointer_change_count(&engine, "file-json-multi-insert-1").await,
            2
        );
        assert_eq!(
            json_pointer_change_count(&engine, "file-json-multi-insert-2").await,
            2
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
                 VALUES ('file-json-seq', '/seq.json', lix_text_encode('{\"a\":1}'))",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

        engine
            .execute(
                "UPDATE lix_file SET data = lix_text_encode('{\"a\":2,\"b\":true}') WHERE id = 'file-json-seq'; \
                 UPDATE lix_file SET data = lix_text_encode('{\"a\":4}') WHERE id = 'file-json-seq'",
                &[],
            )
            .await
            .expect("multi-statement update should succeed");

        let detected =
            detected_json_pointer_entities(&engine, "file-json-seq", &main_version_id).await;
        assert_eq!(detected, vec!["".to_string(), "/a".to_string()]);

        let file_rows = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'file-json-seq' LIMIT 1",
                &[],
            )
            .await
            .expect("lix_file query should succeed");
        assert_eq!(file_rows.rows.len(), 1);
        assert_blob_json_eq(&file_rows.rows[0][0], serde_json::json!({"a":4}));
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
                 VALUES ('file-json-seq-param', '/seq-param.json', lix_text_encode('{\"seed\":0}'))",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

        engine
            .execute(
                "UPDATE lix_file SET data = $1 WHERE id = 'file-json-seq-param'; \
                 UPDATE lix_file SET data = $2 WHERE id = 'file-json-seq-param'",
                &[
                    Value::Blob(b"{\"step\":1,\"keep\":true}".to_vec()),
                    Value::Blob(b"{\"final\":2}".to_vec()),
                ],
            )
            .await
            .expect("multi-statement parameterized update should succeed");

        let detected =
            detected_json_pointer_entities(&engine, "file-json-seq-param", &main_version_id).await;
        assert_eq!(detected, vec!["".to_string(), "/final".to_string()]);

        let file_rows = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'file-json-seq-param' LIMIT 1",
                &[],
            )
            .await
            .expect("lix_file query should succeed");
        assert_eq!(file_rows.rows.len(), 1);
        assert_blob_json_eq(&file_rows.rows[0][0], serde_json::json!({"final":2}));
    }
);

simulation_test!(
    file_update_after_active_version_switch_in_same_batch_uses_new_active_scope,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) = boot_engine_with_json_plugin(&sim).await;
        let version_b = "file-active-switch-version-b";

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_version (\
                     id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
                     ) VALUES (\
                     '{version_b}', '{version_b}', '{main_version}', false, 'commit-{version_b}', 'working-{version_b}'\
                     )",
                    version_b = version_b,
                    main_version = main_version_id
                ),
                &[],
            )
            .await
            .expect("version insert should succeed");

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
                     VALUES ('file-active-switch', '/active-switch.json', lix_text_encode('{{\"hello\":\"before\"}}'), '{}')",
                    version_b
                ),
                &[],
            )
            .await
            .expect("file_by_version insert should succeed");

        engine
            .execute(
                &format!(
                    "UPDATE lix_active_version SET version_id = '{}'; \
                     UPDATE lix_file SET data = lix_text_encode('{{\"hello\":\"after\"}}') WHERE id = 'file-active-switch'",
                    version_b
                ),
                &[],
            )
            .await
            .expect("active-version switch + file update should succeed");

        assert_eq!(active_version_id(&engine).await, version_b);

        let rows = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'file-active-switch' LIMIT 1",
                &[],
            )
            .await
            .expect("lix_file read after active switch update should succeed");
        assert_eq!(rows.rows.len(), 1);
        assert_blob_json_eq(&rows.rows[0][0], serde_json::json!({"hello":"after"}));

        let pointer_rows = engine
            .execute(
                &format!(
                    "SELECT snapshot_content \
                     FROM lix_state_by_version \
                     WHERE file_id = 'file-active-switch' \
                       AND version_id = '{}' \
                       AND schema_key = 'json_pointer' \
                       AND entity_id = '/hello' \
                     LIMIT 1",
                    version_b
                ),
                &[],
            )
            .await
            .expect("json pointer row read should succeed");
        assert_eq!(pointer_rows.rows.len(), 1);
        let snapshot = match &pointer_rows.rows[0][0] {
            Value::Text(text) => serde_json::from_str::<JsonValue>(text)
                .expect("json pointer snapshot_content should be valid JSON"),
            other => panic!("expected snapshot_content text, got {other:?}"),
        };
        assert_eq!(snapshot["value"], serde_json::json!("after"));
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
                 VALUES ('file-json-delete', '/delete.json', lix_text_encode('{\"hello\":\"before\",\"drop\":\"x\"}'))",
                &[],
            )
            .await
            .expect("initial file insert should succeed");

        let before_rows = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'file-json-delete' LIMIT 1",
                &[],
            )
            .await
            .expect("file read before delete should succeed");
        assert_eq!(before_rows.rows.len(), 1);
        assert_blob_json_eq(
            &before_rows.rows[0][0],
            serde_json::json!({"hello":"before","drop":"x"}),
        );
        assert_eq!(
            file_cache_row_count(&engine, "file-json-delete", &main_version_id).await,
            1
        );

        engine
            .execute("DELETE FROM lix_file WHERE id = 'file-json-delete'", &[])
            .await
            .expect("file delete should succeed");

        assert_eq!(
            file_descriptor_tombstone_count(&engine, "file-json-delete", &main_version_id).await,
            1
        );
        let after_rows = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'file-json-delete' LIMIT 1",
                &[],
            )
            .await
            .expect("file read after delete should succeed");
        assert_eq!(after_rows.rows.len(), 0);
        assert_eq!(
            file_cache_row_count(&engine, "file-json-delete", &main_version_id).await,
            0
        );
    }
);

simulation_test!(
    file_by_version_delete_evicts_materialized_cache_row,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) = boot_engine_with_json_plugin(&sim).await;
        let version_b = "cache-delete-version-b";

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_version (\
                     id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
                     ) VALUES (\
                     '{version_b}', '{version_b}', '{main_version}', false, 'commit-{version_b}', 'working-{version_b}'\
                     )",
                    version_b = version_b,
                    main_version = main_version_id
                ),
                &[],
            )
            .await
            .expect("version insert should succeed");

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
                     VALUES ('file-delete-by-version', '/delete-by-version.json', lix_text_encode('{{\"hello\":\"by-version\"}}'), '{}')",
                    version_b
                ),
                &[],
            )
            .await
            .expect("file_by_version insert should succeed");

        assert_eq!(
            file_cache_row_count(&engine, "file-delete-by-version", version_b).await,
            0
        );

        let before_rows = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_file_by_version \
                     WHERE id = 'file-delete-by-version' \
                       AND lixcol_version_id = '{}' \
                     LIMIT 1",
                    version_b
                ),
                &[],
            )
            .await
            .expect("file_by_version read before delete should succeed");
        assert_eq!(before_rows.rows.len(), 1);
        assert_blob_json_eq(
            &before_rows.rows[0][0],
            serde_json::json!({"hello":"by-version"}),
        );
        assert_eq!(
            file_cache_row_count(&engine, "file-delete-by-version", version_b).await,
            1
        );

        engine
            .execute(
                &format!(
                    "DELETE FROM lix_file_by_version \
                     WHERE id = 'file-delete-by-version' \
                       AND lixcol_version_id = '{}'",
                    version_b
                ),
                &[],
            )
            .await
            .expect("file_by_version delete should succeed");

        let after_rows = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_file_by_version \
                     WHERE id = 'file-delete-by-version' \
                       AND lixcol_version_id = '{}' \
                     LIMIT 1",
                    version_b
                ),
                &[],
            )
            .await
            .expect("file_by_version read after delete should succeed");
        assert_eq!(after_rows.rows.len(), 0);
        assert_eq!(
            file_cache_row_count(&engine, "file-delete-by-version", version_b).await,
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
                 VALUES ('file-json-mixed-drop', '/drop.json', lix_text_encode('{\"a\":1}')); \
                 UPDATE lix_file \
                 SET data = lix_text_encode('{\"a\":2,\"b\":true}') \
                 WHERE id = 'file-json-mixed-drop'; \
                 DELETE FROM lix_file WHERE id = 'file-json-mixed-drop'; \
                 INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-json-mixed-keep', '/keep.json', lix_text_encode('{\"keep\":\"yes\"}'))",
                &[],
            )
            .await
            .expect("mixed insert/update/delete execute should succeed");

        assert_eq!(
            file_descriptor_tombstone_count(&engine, "file-json-mixed-drop", &main_version_id)
                .await,
            1
        );
        let kept_visible =
            detected_json_pointer_entities(&engine, "file-json-mixed-keep", &main_version_id).await;
        assert_eq!(kept_visible, vec!["".to_string(), "/keep".to_string()]);

        let kept_rows = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'file-json-mixed-keep' LIMIT 1",
                &[],
            )
            .await
            .expect("kept file query should succeed");
        assert_eq!(kept_rows.rows.len(), 1);
        assert_blob_json_eq(&kept_rows.rows[0][0], serde_json::json!({"keep":"yes"}));
        assert_eq!(
            file_cache_row_count(&engine, "file-json-mixed-drop", &main_version_id).await,
            0
        );
    }
);

simulation_test!(
    file_multi_statement_delete_with_metadata_predicate_works_with_overlay_prefetch,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data, metadata) \
                 VALUES ('file-json-overlay-meta-delete', '/overlay-meta-delete.json', lix_text_encode('{\"v\":1}'), '{\"tag\":\"x\"}'); \
                 DELETE FROM lix_file \
                 WHERE metadata IS NOT NULL \
                   AND id = 'file-json-overlay-meta-delete'",
                &[],
            )
            .await
            .expect("metadata-predicate delete should succeed");

        let rows = engine
            .execute(
                "SELECT id FROM lix_file \
                 WHERE id = 'file-json-overlay-meta-delete' \
                 LIMIT 1",
                &[],
            )
            .await
            .expect("post-delete read should succeed");
        assert!(rows.rows.is_empty());
        assert_eq!(
            file_cache_row_count(&engine, "file-json-overlay-meta-delete", &main_version_id).await,
            0
        );
    }
);

simulation_test!(
    file_read_materializes_cache_miss_without_explicit_materialize,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-read-miss', '/read-miss.json', lix_text_encode('{\"hello\":\"from-read\"}'))",
                &[],
            )
            .await
            .expect("file insert should succeed");

        assert_eq!(
            file_cache_row_count(&engine, "file-read-miss", &main_version_id).await,
            0
        );

        engine
            .execute(
                &format!(
                    "DELETE FROM lix_internal_file_data_cache \
                     WHERE file_id = 'file-read-miss' AND version_id = '{}'",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("cache delete should succeed");

        assert_eq!(
            file_cache_row_count(&engine, "file-read-miss", &main_version_id).await,
            0
        );

        let rows = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'file-read-miss' LIMIT 1",
                &[],
            )
            .await
            .expect("lix_file read should succeed");
        assert_eq!(rows.rows.len(), 1);
        assert_blob_json_eq(&rows.rows[0][0], serde_json::json!({"hello":"from-read"}));

        assert_eq!(
            file_cache_row_count(&engine, "file-read-miss", &main_version_id).await,
            1
        );
    }
);

simulation_test!(
    file_by_version_read_materializes_cache_miss_for_non_active_version,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) = boot_engine_with_json_plugin(&sim).await;
        let version_b = "file-read-miss-version-b";

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_version (\
                     id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
                     ) VALUES (\
                     '{version_b}', '{version_b}', '{main_version}', false, 'commit-{version_b}', 'working-{version_b}'\
                     )",
                    version_b = version_b,
                    main_version = main_version_id
                ),
                &[],
            )
            .await
            .expect("version insert should succeed");

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
                     VALUES ('file-read-miss-by-version', '/read-miss-by-version.json', lix_text_encode('{{\"hello\":\"by-version\"}}'), '{}')",
                    version_b
                ),
                &[],
            )
            .await
            .expect("file_by_version insert should succeed");

        assert_eq!(
            file_cache_row_count(&engine, "file-read-miss-by-version", version_b).await,
            0
        );

        engine
            .execute(
                &format!(
                    "DELETE FROM lix_internal_file_data_cache \
                     WHERE file_id = 'file-read-miss-by-version' \
                       AND version_id = '{}'",
                    version_b
                ),
                &[],
            )
            .await
            .expect("cache delete should succeed");

        assert_eq!(
            file_cache_row_count(&engine, "file-read-miss-by-version", version_b).await,
            0
        );

        let rows = engine
            .execute(
                &format!(
                    "SELECT data FROM lix_file_by_version \
                     WHERE id = 'file-read-miss-by-version' \
                       AND lixcol_version_id = '{}' \
                     LIMIT 1",
                    version_b
                ),
                &[],
            )
            .await
            .expect("lix_file_by_version read should succeed");
        assert_eq!(rows.rows.len(), 1);
        assert_blob_json_eq(&rows.rows[0][0], serde_json::json!({"hello":"by-version"}));

        assert_eq!(
            file_cache_row_count(&engine, "file-read-miss-by-version", version_b).await,
            1
        );
    }
);

simulation_test!(
    file_insert_select_from_lix_file_materializes_cache_miss_before_source_read,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-read-insert-select', '/insert-select.json', lix_text_encode('{\"hello\":\"from-insert-select\"}'))",
                &[],
            )
            .await
            .expect("file insert should succeed");

        assert_eq!(
            file_cache_row_count(&engine, "file-read-insert-select", &main_version_id).await,
            0
        );

        engine
            .execute(
                &format!(
                    "DELETE FROM lix_internal_file_data_cache \
                     WHERE file_id = 'file-read-insert-select' \
                       AND version_id = '{}'",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("cache delete should succeed");

        assert_eq!(
            file_cache_row_count(&engine, "file-read-insert-select", &main_version_id).await,
            0
        );

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_internal_state_vtable (\
                     entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                     ) \
                     SELECT \
                     'insert-select-probe-main', \
                     'json_pointer', \
                     'file-read-insert-select', \
                     '{}', \
                     'json', \
                     '{{\"path\":\"probe-main\",\"value\":{{\"ok\":true}}}}', \
                     '1' \
                     FROM lix_file \
                     WHERE id = 'file-read-insert-select' \
                     LIMIT 1",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("insert-select from lix_file should succeed");

        assert_eq!(
            file_cache_row_count(&engine, "file-read-insert-select", &main_version_id).await,
            1
        );
    }
);

simulation_test!(
    file_insert_select_from_lix_file_by_version_materializes_non_active_cache_miss,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) = boot_engine_with_json_plugin(&sim).await;
        let version_b = "file-read-insert-select-version-b";

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_version (\
                     id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
                     ) VALUES (\
                     '{version_b}', '{version_b}', '{main_version}', false, 'commit-{version_b}', 'working-{version_b}'\
                     )",
                    version_b = version_b,
                    main_version = main_version_id
                ),
                &[],
            )
            .await
            .expect("version insert should succeed");

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
                     VALUES ('file-read-insert-select-by-version', '/insert-select-by-version.json', lix_text_encode('{{\"hello\":\"from-version-b\"}}'), '{}')",
                    version_b
                ),
                &[],
            )
            .await
            .expect("file_by_version insert should succeed");

        assert_eq!(
            file_cache_row_count(&engine, "file-read-insert-select-by-version", version_b).await,
            0
        );

        engine
            .execute(
                &format!(
                    "DELETE FROM lix_internal_file_data_cache \
                     WHERE file_id = 'file-read-insert-select-by-version' \
                       AND version_id = '{}'",
                    version_b
                ),
                &[],
            )
            .await
            .expect("cache delete should succeed");

        assert_eq!(
            file_cache_row_count(&engine, "file-read-insert-select-by-version", version_b).await,
            0
        );

        engine
            .execute(
                &format!(
                    "INSERT INTO lix_internal_state_vtable (\
                     entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                     ) \
                     SELECT \
                     'insert-select-probe-version-b', \
                     'json_pointer', \
                     'file-read-insert-select-by-version', \
                     '{}', \
                     'json', \
                     '{{\"path\":\"probe-version\",\"value\":{{\"ok\":true}}}}', \
                     '1' \
                     FROM lix_file_by_version \
                     WHERE id = 'file-read-insert-select-by-version' \
                       AND lixcol_version_id = '{}' \
                     LIMIT 1",
                    main_version_id, version_b
                ),
                &[],
            )
            .await
            .expect("insert-select from lix_file_by_version should succeed");

        assert_eq!(
            file_cache_row_count(&engine, "file-read-insert-select-by-version", version_b).await,
            1
        );
    }
);

simulation_test!(
    on_demand_plugin_materialization_uses_full_file_path,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, main_version_id) = boot_engine_with_path_echo_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('file-path-echo', '/docs/readme.json', lix_text_encode('{\"hello\":\"world\"}'))",
                &[],
            )
            .await
            .expect("file insert should succeed");

        engine
            .execute(
                &format!(
                    "DELETE FROM lix_internal_file_data_cache \
                     WHERE file_id = 'file-path-echo' AND version_id = '{}'",
                    main_version_id
                ),
                &[],
            )
            .await
            .expect("cache delete should succeed");
        assert_eq!(
            file_cache_row_count(&engine, "file-path-echo", &main_version_id).await,
            0
        );

        let rows = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'file-path-echo' LIMIT 1",
                &[],
            )
            .await
            .expect("lix_file read should succeed");
        assert_eq!(rows.rows.len(), 1);
        assert_blob_bytes_eq(&rows.rows[0][0], b"/docs/readme.json");

        assert_eq!(
            file_cache_row_count(&engine, "file-path-echo", &main_version_id).await,
            1
        );
    }
);

simulation_test!(
    file_cache_churn_insert_read_delete_leaves_no_rows,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, _main_version_id) = boot_engine_with_json_plugin(&sim).await;

        for i in 0..40 {
            let file_id = format!("cache-churn-{i}");
            let path = format!("/{file_id}.json");
            let insert_sql = format!(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('{}', '{}', lix_text_encode('{{\"i\":{}}}'))",
                file_id, path, i
            );
            engine
                .execute(&insert_sql, &[])
                .await
                .expect("churn insert should succeed");

            let read_sql = format!("SELECT data FROM lix_file WHERE id = '{}' LIMIT 1", file_id);
            let read_rows = engine
                .execute(&read_sql, &[])
                .await
                .expect("churn read should succeed");
            assert_eq!(read_rows.rows.len(), 1);

            let delete_sql = format!("DELETE FROM lix_file WHERE id = '{}'", file_id);
            engine
                .execute(&delete_sql, &[])
                .await
                .expect("churn delete should succeed");
        }

        assert_eq!(
            total_file_cache_row_count_for_prefix(&engine, "cache-churn-").await,
            0
        );
    }
);

simulation_test!(
    file_cache_has_no_orphan_rows_after_mixed_lifecycle,
    simulations = [sqlite, postgres],
    |sim| async move {
        let (engine, _main_version_id) = boot_engine_with_json_plugin(&sim).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES \
                 ('cache-orphan-keep', '/cache-orphan-keep.json', lix_text_encode('{\"keep\":true}')), \
                 ('cache-orphan-drop', '/cache-orphan-drop.json', lix_text_encode('{\"drop\":true}'))",
                &[],
            )
            .await
            .expect("mixed lifecycle insert should succeed");

        let keep_rows = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'cache-orphan-keep' LIMIT 1",
                &[],
            )
            .await
            .expect("keep read should succeed");
        assert_eq!(keep_rows.rows.len(), 1);
        let drop_rows = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'cache-orphan-drop' LIMIT 1",
                &[],
            )
            .await
            .expect("drop read should succeed");
        assert_eq!(drop_rows.rows.len(), 1);

        engine
            .execute("DELETE FROM lix_file WHERE id = 'cache-orphan-drop'", &[])
            .await
            .expect("drop delete should succeed");

        assert_eq!(
            orphan_file_cache_row_count_for_prefix(&engine, "cache-orphan-").await,
            0
        );
        assert_eq!(
            total_file_cache_row_count_for_prefix(&engine, "cache-orphan-").await,
            1
        );
    }
);

simulation_test!(
    binary_cas_gc_keeps_history_referenced_rows_after_overwrite,
    simulations = [sqlite, postgres],
    |sim| async move {
        let runtime = Arc::new(PathEchoRuntime) as Arc<dyn WasmRuntime>;
        let engine = sim
            .boot_simulated_engine(Some(support::simulation_test::SimulationBootArgs {
                key_values: Vec::new(),
                active_account: None,
                wasm_runtime: runtime,
                access_to_internal: true,
            }))
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.expect("engine init should succeed");
        let main_version_id = main_version_id(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('binary-gc-overwrite', '/assets/video.mp4', lix_text_encode('AAAA-AAAA-AAAA-AAAA'))",
                &[],
            )
            .await
            .expect("initial binary write should succeed");
        let old_blob_hash =
            binary_blob_hash_for_file_version(&engine, "binary-gc-overwrite", &main_version_id)
                .await
                .expect("old blob hash should exist");

        engine
            .execute(
                "UPDATE lix_file \
                 SET data = lix_text_encode('BBBB-BBBB-BBBB-BBBB-BBBB-BBBB') \
                 WHERE id = 'binary-gc-overwrite'",
                &[],
            )
            .await
            .expect("binary overwrite should succeed");
        let update_commit_id = active_version_commit_id(&engine).await;
        let new_blob_hash =
            binary_blob_hash_for_file_version(&engine, "binary-gc-overwrite", &main_version_id)
                .await
                .expect("new blob hash should exist");
        assert_ne!(old_blob_hash, new_blob_hash);

        assert_eq!(
            binary_manifest_row_count_by_hash(&engine, &old_blob_hash).await,
            1
        );

        let history_rows = engine
            .execute(
                &format!(
                    "SELECT data \
                 FROM lix_file_history \
                 WHERE id = 'binary-gc-overwrite' \
                   AND lixcol_root_commit_id = '{}' \
                   AND lixcol_depth = 1 \
                 LIMIT 1",
                    update_commit_id
                ),
                &[],
            )
            .await
            .expect("history read should succeed");
        assert_eq!(history_rows.rows.len(), 1);
        assert_blob_bytes_eq(&history_rows.rows[0][0], b"AAAA-AAAA-AAAA-AAAA");

        assert_eq!(orphan_binary_manifest_chunk_row_count(&engine).await, 0);
        assert_eq!(orphan_binary_chunk_row_count(&engine).await, 0);
    }
);

simulation_test!(
    binary_cas_foreign_keys_restrict_live_parent_deletes,
    simulations = [sqlite, postgres],
    |sim| async move {
        let runtime = Arc::new(PathEchoRuntime) as Arc<dyn WasmRuntime>;
        let engine = sim
            .boot_simulated_engine(Some(support::simulation_test::SimulationBootArgs {
                key_values: Vec::new(),
                active_account: None,
                wasm_runtime: runtime,
                access_to_internal: true,
            }))
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.expect("engine init should succeed");
        let main_version_id = main_version_id(&engine).await;

        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('binary-fk-guard', '/assets/blob.mp4', lix_text_encode('FK-GUARD-DATA-123456'))",
                &[],
            )
            .await
            .expect("binary write should succeed");

        let blob_hash =
            binary_blob_hash_for_file_version(&engine, "binary-fk-guard", &main_version_id)
                .await
                .expect("blob hash should exist");
        let chunk_hash = binary_chunk_hash_for_blob(&engine, &blob_hash)
            .await
            .expect("chunk hash should exist");

        let delete_manifest = engine
            .execute(
                &format!(
                    "DELETE FROM lix_internal_binary_blob_manifest \
                     WHERE blob_hash = '{}'",
                    blob_hash
                ),
                &[],
            )
            .await;
        assert!(
            delete_manifest.is_err(),
            "manifest delete should be rejected while file_version_ref still points to it"
        );

        let delete_chunk = engine
            .execute(
                &format!(
                    "DELETE FROM lix_internal_binary_chunk_store \
                     WHERE chunk_hash = '{}'",
                    chunk_hash
                ),
                &[],
            )
            .await;
        assert!(
            delete_chunk.is_err(),
            "chunk delete should be rejected while manifest_chunk still points to it"
        );
    }
);

simulation_test!(
    binary_chunk_codec_metadata_is_explicit_and_payload_unframed,
    simulations = [sqlite, postgres],
    |sim| async move {
        let runtime = Arc::new(PathEchoRuntime) as Arc<dyn WasmRuntime>;
        let engine = sim
            .boot_simulated_engine(Some(support::simulation_test::SimulationBootArgs {
                key_values: Vec::new(),
                active_account: None,
                wasm_runtime: runtime,
                access_to_internal: true,
            }))
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.expect("engine init should succeed");
        let main_version_id = main_version_id(&engine).await;

        let payload = vec![0u8; 300_000];
        engine
            .execute(
                "INSERT INTO lix_file (id, path, data) \
                 VALUES ('binary-codec-meta', '/assets/video.mp4', $1)",
                &[Value::Blob(payload.clone())],
            )
            .await
            .expect("binary write should succeed");

        let blob_hash =
            binary_blob_hash_for_file_version(&engine, "binary-codec-meta", &main_version_id)
                .await
                .expect("blob hash should exist");

        let (raw_count, zstd_count, legacy_count) =
            binary_codec_counts_for_blob(&engine, &blob_hash).await;
        assert!(raw_count + zstd_count > 0);
        assert_eq!(legacy_count, 0);
        assert!(
            zstd_count >= 1,
            "compressible payload should produce zstd chunks"
        );
        assert_eq!(
            binary_prefixed_chunk_payload_count_for_blob(&engine, &blob_hash).await,
            0
        );

        let rows = engine
            .execute(
                "SELECT data FROM lix_file WHERE id = 'binary-codec-meta' LIMIT 1",
                &[],
            )
            .await
            .expect("binary file read should succeed");
        assert_eq!(rows.rows.len(), 1);
        assert_blob_bytes_eq(&rows.rows[0][0], &payload);
    }
);
