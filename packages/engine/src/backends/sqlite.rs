use async_trait::async_trait;

use crate::{LixBackend, LixError, QueryResult, Value};

#[derive(Debug, Clone)]
pub struct SqliteConfig {
	pub filename: String,
}

pub struct SqliteBackend {
	_config: SqliteConfig,
}

impl SqliteBackend {
	pub fn new(config: SqliteConfig) -> Self {
		Self { _config: config }
	}
}

#[async_trait]
impl LixBackend for SqliteBackend {
	async fn execute(&self, _sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
		Err(LixError {
			message: "SqliteBackend not implemented yet".to_string(),
		})
	}
}
