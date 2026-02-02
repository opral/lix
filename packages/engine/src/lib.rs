mod backend;
mod error;
mod lix;
mod types;

pub mod backends;

pub use backend::LixBackend;
pub use backends::{PostgresBackend, PostgresConfig, SqliteBackend, SqliteConfig};
pub use error::LixError;
pub use lix::{open_lix, Lix, OpenLixConfig};
pub use types::{QueryResult, Value};

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    struct MockBackend;

    #[async_trait]
    impl LixBackend for MockBackend {
        async fn execute(&self, _sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            Ok(QueryResult { rows: vec![] })
        }
    }

    #[tokio::test]
    async fn open_and_execute() {
        let backend: Box<dyn LixBackend + Send + Sync> = Box::new(MockBackend);
        let lix = open_lix(OpenLixConfig { backend })
            .await
            .expect("open_lix should succeed");

        let result = lix.execute("SELECT 1 + 1", &[]).await.unwrap();
        assert_eq!(result.rows.len(), 0);
    }
}
