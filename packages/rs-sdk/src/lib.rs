mod backend;

use lix_engine::boot;

pub struct OpenLixConfig {
    pub backend: Option<Box<dyn LixBackend + Send + Sync>>,
}

impl Default for OpenLixConfig {
    fn default() -> Self {
        Self { backend: None }
    }
}

pub struct Lix {
    engine: Engine,
}

pub async fn open_lix(config: OpenLixConfig) -> Result<Lix, LixError> {
    let backend = match config.backend {
        Some(backend) => backend,
        None => Box::new(backend::sqlite::SqliteBackend::in_memory()?),
    };
    Ok(Lix {
        engine: boot(backend),
    })
}

impl Lix {
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        self.engine.execute(sql, params).await
    }
}

pub use backend::sqlite::SqliteBackend;
pub use lix_engine::{Engine, LixBackend, LixError, QueryResult, Value};
