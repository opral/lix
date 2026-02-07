use async_trait::async_trait;

use crate::{LixError, QueryResult, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    Sqlite,
    Postgres,
}

#[async_trait(?Send)]
pub trait LixBackend: Send + Sync {
    fn dialect(&self) -> SqlDialect;

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError>;
}
