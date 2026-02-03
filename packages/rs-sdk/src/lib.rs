use lix_engine::boot;

pub struct OpenLixConfig {
    pub backend: Box<dyn LixBackend + Send + Sync>,
}

pub struct Lix {
    engine: Engine,
    backend: Box<dyn LixBackend + Send + Sync>,
}

pub async fn open_lix(config: OpenLixConfig) -> Result<Lix, LixError> {
    Ok(Lix {
        engine: boot(),
        backend: config.backend,
    })
}

impl Lix {
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let plan = self.engine.preprocess(sql, params)?;
        let result = self.backend.execute(&plan.sql, &plan.params).await?;
        self.engine.postprocess(&plan, &result)?;
        Ok(result)
    }
}

pub use lix_engine::{Engine, LixBackend, LixError, Plan, QueryResult, Value};
