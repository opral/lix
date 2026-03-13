use async_trait::async_trait;

use crate::{ImageChunkReader, ImageChunkWriter, LixError, QueryResult, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    Sqlite,
    Postgres,
}

#[async_trait(?Send)]
pub trait LixBackend: Send + Sync {
    fn dialect(&self) -> SqlDialect;

    /// Executes one engine SQL unit of work.
    ///
    /// `sql` may be a single statement or a semicolon-separated batch/script.
    /// Backends must treat one call as one execution roundtrip.
    /// `params` bind across the full SQL payload.
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError>;

    async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError>;

    /// Exports the current Lix database snapshot as a SQLite database file payload.
    ///
    /// Implementations should write a valid SQLite3 database image (for example `.lix`)
    /// to `writer` in one or more chunks.
    async fn export_image(&self, _writer: &mut dyn ImageChunkWriter) -> Result<(), LixError> {
        Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "export_image is not supported by this backend".to_string(),
        })
    }

    /// Restores backend state from a SQLite database file payload stream.
    async fn restore_from_image(&self, _reader: &mut dyn ImageChunkReader) -> Result<(), LixError> {
        Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "restore_from_image is not supported by this backend".to_string(),
        })
    }
}

#[async_trait(?Send)]
pub trait LixTransaction {
    fn dialect(&self) -> SqlDialect;

    /// Executes one SQL unit of work inside the current transaction.
    ///
    /// `sql` may be a single statement or a semicolon-separated batch/script.
    /// Backends must treat one call as one execution roundtrip.
    /// `params` bind across the full SQL payload.
    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError>;

    async fn commit(self: Box<Self>) -> Result<(), LixError>;

    async fn rollback(self: Box<Self>) -> Result<(), LixError>;
}
