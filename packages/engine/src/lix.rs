use crate::{LixBackend, LixError, QueryResult, Value};

pub struct OpenLixConfig {
    pub backend: Box<dyn LixBackend + Send + Sync>,
}

pub struct Lix {
    backend: Box<dyn LixBackend + Send + Sync>,
}

pub async fn open_lix(config: OpenLixConfig) -> Result<Lix, LixError> {
    Ok(Lix {
        backend: config.backend,
    })
}

impl Lix {
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        self.backend.execute(sql, params).await
    }
}
