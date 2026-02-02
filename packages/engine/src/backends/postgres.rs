use async_trait::async_trait;
use sqlx::{Row, PgPool};
use tokio::sync::OnceCell;

use crate::{LixBackend, LixError, QueryResult, Value};

#[derive(Debug, Clone)]
pub struct PostgresConfig {
	pub connection_string: String,
}

pub struct PostgresBackend {
	config: PostgresConfig,
	pool: OnceCell<PgPool>,
}

impl PostgresBackend {
	pub fn new(config: PostgresConfig) -> Self {
		Self {
			config,
			pool: OnceCell::const_new(),
		}
	}

	async fn pool(&self) -> Result<&PgPool, LixError> {
		self.pool
			.get_or_try_init(|| async {
				PgPool::connect(&self.config.connection_string)
					.await
					.map_err(|err| LixError {
						message: err.to_string(),
					})
			})
			.await
	}
}

#[async_trait]
impl LixBackend for PostgresBackend {
	async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
		let pool = self.pool().await?;
		let mut query = sqlx::query(sql);

		for param in params {
			query = bind_param(query, param);
		}

		let rows = query
			.fetch_all(pool)
			.await
			.map_err(|err| LixError {
				message: err.to_string(),
			})?;

		let mut result_rows = Vec::with_capacity(rows.len());
		for row in rows {
			let mut out = Vec::with_capacity(row.columns().len());
			for i in 0..row.columns().len() {
				out.push(map_postgres_value(&row, i)?);
			}
			result_rows.push(out);
		}

		Ok(QueryResult { rows: result_rows })
	}
}

fn bind_param<'q>(
	query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
	param: &'q Value,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
	match param {
		Value::Null => query.bind(Option::<i64>::None),
		Value::Integer(v) => query.bind(*v),
		Value::Real(v) => query.bind(*v),
		Value::Text(v) => query.bind(v.as_str()),
		Value::Blob(v) => query.bind(v.as_slice()),
	}
}

fn map_postgres_value(row: &sqlx::postgres::PgRow, index: usize) -> Result<Value, LixError> {
	if let Ok(value) = row.try_get::<i64, _>(index) {
		return Ok(Value::Integer(value));
	}
	if let Ok(value) = row.try_get::<f64, _>(index) {
		return Ok(Value::Real(value));
	}
	if let Ok(value) = row.try_get::<bool, _>(index) {
		return Ok(Value::Integer(if value { 1 } else { 0 }));
	}
	if let Ok(value) = row.try_get::<String, _>(index) {
		return Ok(Value::Text(value));
	}
	if let Ok(value) = row.try_get::<Vec<u8>, _>(index) {
		return Ok(Value::Blob(value));
	}

	Ok(Value::Null)
}
