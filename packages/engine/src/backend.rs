use async_trait::async_trait;

use crate::{LixError, QueryResult, SnapshotChunkReader, SnapshotChunkWriter, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    Sqlite,
    Postgres,
}

#[async_trait(?Send)]
pub trait LixBackend: Send + Sync {
    fn dialect(&self) -> SqlDialect;

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError>;

    async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError>;

    /// Exports the current Lix database snapshot as a SQLite database file payload.
    ///
    /// Implementations should write a valid SQLite3 database image (for example `.lix`)
    /// to `writer` in one or more chunks.
    async fn export_snapshot(&self, _writer: &mut dyn SnapshotChunkWriter) -> Result<(), LixError> {
        Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: "export_snapshot is not supported by this backend".to_string(),
        })
    }

    /// Restores backend state from a SQLite database file payload stream.
    async fn restore_from_snapshot(
        &self,
        _reader: &mut dyn SnapshotChunkReader,
    ) -> Result<(), LixError> {
        Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: "restore_from_snapshot is not supported by this backend".to_string(),
        })
    }
}

#[async_trait(?Send)]
pub trait LixTransaction {
    fn dialect(&self) -> SqlDialect;

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError>;

    async fn commit(self: Box<Self>) -> Result<(), LixError>;

    async fn rollback(self: Box<Self>) -> Result<(), LixError>;
}
