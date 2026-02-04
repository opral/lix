use crate::{LixBackend, LixError, QueryResult, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub struct Engine {
    backend: Box<dyn LixBackend + Send + Sync>,
}

pub fn boot(backend: Box<dyn LixBackend + Send + Sync>) -> Engine {
    Engine { backend }
}

impl Engine {
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).map_err(|err| LixError {
            message: err.to_string(),
        })?;
        let normalized_sql = statements
            .iter()
            .map(|statement| statement.to_string())
            .collect::<Vec<_>>()
            .join("; ");
        eprintln!(
            "[lix_engine] SQL normalize: input=`{}` output=`{}`",
            sql, normalized_sql
        );
        self.backend.execute(&normalized_sql, params).await
    }
}
