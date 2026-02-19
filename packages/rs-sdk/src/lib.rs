mod backend;

use lix_engine::{boot, BootArgs, BootKeyValue};
use serde_json::Value as JsonValue;

pub struct OpenLixConfig {
    pub backend: Option<Box<dyn LixBackend + Send + Sync>>,
    pub key_values: Vec<BootKeyValueConfig>,
}

impl Default for OpenLixConfig {
    fn default() -> Self {
        Self {
            backend: None,
            key_values: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BootKeyValueConfig {
    pub key: String,
    pub value: JsonValue,
    pub version_id: Option<String>,
}

pub struct Lix {
    engine: Engine,
}

pub async fn open_lix(config: OpenLixConfig) -> Result<Lix, LixError> {
    let backend = match config.backend {
        Some(backend) => backend,
        None => Box::new(backend::sqlite::SqliteBackend::in_memory()?),
    };
    let key_values = config
        .key_values
        .into_iter()
        .map(|item| BootKeyValue {
            key: item.key,
            value: item.value,
            version_id: item.version_id,
        })
        .collect();
    let engine = boot(BootArgs {
        backend,
        wasm_runtime: None,
        key_values,
        active_account: None,
        access_to_internal: false,
    });
    engine.init().await?;
    Ok(Lix { engine })
}

impl Lix {
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        self.engine.execute(sql, params).await
    }
}

pub use backend::sqlite::SqliteBackend;
pub use lix_engine::{Engine, LixBackend, LixError, QueryResult, Value};
