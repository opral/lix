use crate::{LixBackend, LixError, QueryResult, Value};

pub struct Engine {
    backend: Box<dyn LixBackend + Send + Sync>,
}

pub fn boot(backend: Box<dyn LixBackend + Send + Sync>) -> Engine {
    Engine { backend }
}

impl Engine {
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        self.backend.execute(sql, params).await
    }
}
