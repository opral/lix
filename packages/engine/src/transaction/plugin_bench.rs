use std::io::{Cursor, Write};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;

use crate::plugin::bench_stats::{plugin_bench_stats, reset_plugin_bench_stats};
use crate::storage_adapter::Memory;
use crate::wasm::{
    WasmComponentInstance, WasmLimits, WasmPluginDetectedChange, WasmPluginEntityState,
    WasmPluginFile, WasmRuntime,
};
use crate::{Engine, LixError, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbeOperation {
    Read,
    Write,
}

impl ProbeOperation {
    fn from_env() -> Self {
        let value = match std::env::var("LIX_PLUGIN_BENCH_OPERATION") {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => "write".to_string(),
            Err(error) => panic!("LIX_PLUGIN_BENCH_OPERATION must be valid Unicode: {error}"),
        };
        match value.as_str() {
            "read" => Self::Read,
            "write" => Self::Write,
            value => panic!("LIX_PLUGIN_BENCH_OPERATION must be read or write, got {value:?}"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "release-only engine-overhead probe with a stub WASM runtime"]
async fn plugin_reconciliation_benchmark_probe() {
    let file_count = env_usize("LIX_PLUGIN_BENCH_FILES", 1_000);
    let plugin_count = env_usize("LIX_PLUGIN_BENCH_PLUGINS", 0);
    let file_offset = env_usize("LIX_PLUGIN_BENCH_OFFSET", 0);
    let batch_size = env_usize("LIX_PLUGIN_BENCH_BATCH", 1);
    let rounds = env_usize("LIX_PLUGIN_BENCH_ROUNDS", 20);
    let warmup_rounds = env_usize("LIX_PLUGIN_BENCH_WARMUPS", 4);
    let matching = env_bool("LIX_PLUGIN_BENCH_MATCHING", false);
    let operation = ProbeOperation::from_env();

    assert!(file_count > 0, "benchmark needs at least one file");
    assert!(batch_size > 0, "benchmark batch must not be empty");
    assert!(
        file_offset
            .checked_add(batch_size)
            .is_some_and(|end| end <= file_count),
        "offset plus batch cannot exceed file count"
    );
    assert!(rounds > 0, "benchmark needs at least one timed round");
    assert!(
        !matching || plugin_count > 0,
        "matching mode needs at least one plugin"
    );

    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new_with_wasm_runtime(storage, Arc::new(ProbeWasmRuntime))
        .await
        .expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    let values = (0..file_count)
        .map(|index| format!("('seed-{index:05}', '/seed-{index:05}.md', X'01')"))
        .collect::<Vec<_>>()
        .join(", ");
    session
        .execute(
            &format!("INSERT INTO lix_file (id, path, data) VALUES {values}"),
            &[],
        )
        .await
        .expect("benchmark files should insert");

    for index in 0..plugin_count {
        let key = format!("plugin_bench_{index:03}");
        let archive = plugin_archive(index, matching && index == 0);
        session
            .execute(
                "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
                &[
                    Value::Text(format!("/.lix/plugins/{key}.lixplugin")),
                    Value::Blob(archive),
                ],
            )
            .await
            .expect("benchmark plugin should install");
    }

    let ids = (file_offset..file_offset + batch_size)
        .map(|index| format!("'seed-{index:05}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let read_sql = format!("SELECT id, data FROM lix_file WHERE id IN ({ids}) ORDER BY id");
    let write_sql = [
        format!("UPDATE lix_file SET data = X'02' WHERE id IN ({ids})"),
        format!("UPDATE lix_file SET data = X'03' WHERE id IN ({ids})"),
    ];

    if matching {
        session
            .execute(&write_sql[0], &[])
            .await
            .expect("matching benchmark files should materialize plugin state");
    }

    for round in 0..warmup_rounds {
        execute_probe_round(
            &session, operation, &read_sql, &write_sql, round, batch_size,
        )
        .await;
    }

    reset_plugin_bench_stats();
    let mut samples = Vec::with_capacity(rounds);
    for round in 0..rounds {
        let started = Instant::now();
        execute_probe_round(
            &session,
            operation,
            &read_sql,
            &write_sql,
            warmup_rounds + round,
            batch_size,
        )
        .await;
        samples.push(started.elapsed());
    }
    samples.sort_unstable();
    let stats = plugin_bench_stats();

    println!(
        "plugin_reconciliation_probe wasm_runtime=stub operation={} files={} plugins={} matching={} offset={} batch={} \
         rounds={} p50_us={} p95_us={} stats={stats:?}",
        operation.as_str(),
        file_count,
        plugin_count,
        matching,
        file_offset,
        batch_size,
        rounds,
        percentile(&samples, 50, 100).as_micros(),
        percentile(&samples, 95, 100).as_micros(),
    );

    session.close().await.expect("session should close");
}

fn percentile(
    samples: &[std::time::Duration],
    numerator: usize,
    denominator: usize,
) -> std::time::Duration {
    assert!(!samples.is_empty(), "percentile needs at least one sample");
    assert!(
        denominator > 0 && (1..=denominator).contains(&numerator),
        "percentile must be in (0, 1]"
    );
    let rank = samples
        .len()
        .checked_mul(numerator)
        .expect("sample count should fit percentile arithmetic")
        .div_ceil(denominator);
    samples[rank - 1]
}

#[test]
fn percentile_uses_nearest_rank() {
    let samples = [
        std::time::Duration::from_micros(10),
        std::time::Duration::from_micros(20),
    ];
    assert_eq!(percentile(&samples, 50, 100), samples[0]);
    assert_eq!(percentile(&samples, 95, 100), samples[1]);
}

async fn execute_probe_round(
    session: &crate::session::SessionContext<Memory>,
    operation: ProbeOperation,
    read_sql: &str,
    write_sql: &[String; 2],
    round: usize,
    batch_size: usize,
) {
    let result = match operation {
        ProbeOperation::Read => session.execute(read_sql, &[]).await,
        ProbeOperation::Write => {
            session
                .execute(&write_sql[round % write_sql.len()], &[])
                .await
        }
    }
    .expect("benchmark operation should succeed");
    match operation {
        ProbeOperation::Read => {
            assert_eq!(result.len(), batch_size, "benchmark row count should match");
        }
        ProbeOperation::Write => assert_eq!(
            result.rows_affected(),
            batch_size as u64,
            "benchmark affected-row count should match"
        ),
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|error| panic!("{name} must be an unsigned integer: {error}")),
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => panic!("{name} must be valid Unicode: {error}"),
    }
}

fn env_bool(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(value) => match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" => true,
            "0" | "false" | "no" => false,
            _ => panic!("{name} must be one of 1/true/yes or 0/false/no, got {value:?}"),
        },
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => panic!("{name} must be valid Unicode: {error}"),
    }
}

#[derive(Debug, Default)]
struct ProbeWasmRuntime;

#[derive(Debug)]
struct ProbeWasmComponent {
    schema_key: String,
}

#[async_trait]
impl WasmRuntime for ProbeWasmRuntime {
    async fn init_component(
        &self,
        bytes: Vec<u8>,
        _limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
        let index_bytes: [u8; 4] = bytes
            .get(8..12)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| LixError::unknown("benchmark wasm is missing its schema index"))?;
        let index = u32::from_le_bytes(index_bytes);
        Ok(Arc::new(ProbeWasmComponent {
            schema_key: format!("plugin_note_{index:03}"),
        }))
    }
}

#[async_trait]
impl WasmComponentInstance for ProbeWasmComponent {
    async fn detect_changes(
        &self,
        _state: Vec<WasmPluginEntityState>,
        _file: WasmPluginFile,
    ) -> Result<Vec<WasmPluginDetectedChange>, LixError> {
        Ok(vec![WasmPluginDetectedChange {
            entity_pk: vec!["note".to_string()],
            schema_key: self.schema_key.clone(),
            snapshot_content: Some(r#"{"id":"note","value":"detected"}"#.to_string()),
            metadata: None,
        }])
    }

    async fn render(&self, _state: Vec<WasmPluginEntityState>) -> Result<Vec<u8>, LixError> {
        Ok(b"plugin-rendered".to_vec())
    }
}

fn plugin_archive(index: usize, matching: bool) -> Vec<u8> {
    let plugin_key = format!("plugin_bench_{index:03}");
    let schema_key = format!("plugin_note_{index:03}");
    let schema_path = format!("schema/{schema_key}.json");
    let path_glob = if matching {
        "*.md".to_string()
    } else {
        format!("*.plugin-{index:03}")
    };
    let manifest = format!(
        r#"{{
            "key": "{plugin_key}",
            "runtime": "wasm-component-v1",
            "api_version": "0.1.0",
            "match": {{ "path_glob": "{path_glob}" }},
            "entry": "plugin.wasm",
            "schemas": ["{schema_path}"]
        }}"#
    );
    let schema = format!(
        r#"{{
            "x-lix-key": "{schema_key}",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {{
                "id": {{ "type": "string" }},
                "value": {{ "type": "string" }}
            }},
            "required": ["id", "value"],
            "additionalProperties": false
        }}"#
    );
    let mut wasm = b"\0asm\x01\0\0\0".to_vec();
    let schema_index = u32::try_from(index).expect("benchmark plugin index should fit in u32");
    wasm.extend_from_slice(&schema_index.to_le_bytes());

    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        ("manifest.json", manifest.as_bytes()),
        (schema_path.as_str(), schema.as_bytes()),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer
            .start_file(path, options)
            .expect("benchmark archive entry should start");
        writer
            .write_all(bytes)
            .expect("benchmark archive entry should write");
    }
    writer
        .finish()
        .expect("benchmark archive should finish")
        .into_inner()
}
