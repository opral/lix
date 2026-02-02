use async_trait::async_trait;

use crate::{LixBackend, LixError, QueryResult, Value};

#[derive(Debug, Clone)]
pub struct PostgresConfig {
	pub connection_string: String,
}

pub struct PostgresBackend {
	_config: PostgresConfig,
}

impl PostgresBackend {
	pub fn new(config: PostgresConfig) -> Self {
		Self { _config: config }
	}
}

#[async_trait]
impl LixBackend for PostgresBackend {
	async fn execute(&self, _sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
		Err(LixError {
			message: "PostgresBackend not implemented yet".to_string(),
		})
	}
}
