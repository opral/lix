use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::binary_cas::BlobDataReader;
use crate::engine2::schema_registry::SchemaRegistry;
use crate::live_state::{CommittedLiveStateContext, LiveStateContext};
use crate::sql2::SqlExecutionContext;
use crate::{LixBackend, LixError};

mod execute;

pub use execute::{ExecuteResult, Row, RowRef, RowSet};

#[derive(Clone)]
pub struct Session {
    active_version_id: String,
    backend: Arc<dyn LixBackend + Send + Sync>,
    committed_live_state: Arc<CommittedLiveStateContext>,
    schema_registry: Arc<SchemaRegistry>,
}

impl Session {
    pub(crate) async fn open(
        active_version_id: String,
        backend: Arc<dyn LixBackend + Send + Sync>,
        committed_live_state: Arc<CommittedLiveStateContext>,
        schema_registry: Arc<SchemaRegistry>,
    ) -> Result<Self, LixError> {
        Ok(Self::new(
            active_version_id,
            backend,
            committed_live_state,
            schema_registry,
        ))
    }

    pub(crate) fn new(
        active_version_id: String,
        backend: Arc<dyn LixBackend + Send + Sync>,
        committed_live_state: Arc<CommittedLiveStateContext>,
        schema_registry: Arc<SchemaRegistry>,
    ) -> Self {
        Self {
            active_version_id,
            backend,
            committed_live_state,
            schema_registry,
        }
    }

    pub fn active_version_id(&self) -> &str {
        &self.active_version_id
    }
}

struct SessionSqlExecutionContext<'a> {
    active_version_id: &'a str,
    backend: Arc<dyn LixBackend + Send + Sync>,
    committed_live_state: Arc<CommittedLiveStateContext>,
    visible_schemas: Vec<JsonValue>,
}

impl SqlExecutionContext for SessionSqlExecutionContext<'_> {
    fn active_version_id(&self) -> &str {
        self.active_version_id
    }

    fn live_state(&self) -> Arc<dyn LiveStateContext> {
        let live_state: Arc<dyn LiveStateContext> = self.committed_live_state.clone();
        live_state
    }

    fn blob_reader(&self) -> Arc<dyn BlobDataReader> {
        Arc::new(BackendBlobReader(Arc::clone(&self.backend)))
    }

    fn list_visible_schemas(&self, version_id: &str) -> Result<Vec<JsonValue>, LixError> {
        let _ = version_id;
        Ok(self.visible_schemas.clone())
    }
}

struct BackendBlobReader(Arc<dyn LixBackend + Send + Sync>);

#[async_trait]
impl BlobDataReader for BackendBlobReader {
    async fn load_blob_data_by_hash(&self, blob_hash: &str) -> Result<Option<Vec<u8>>, LixError> {
        crate::binary_cas::load_blob_data_by_hash(self.0.as_ref(), blob_hash).await
    }
}
