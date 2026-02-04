use async_trait::async_trait;

use crate::{LixError, QueryResult, Value};

#[async_trait(?Send)]
pub trait LixBackend: Send + Sync {
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError>;
}
