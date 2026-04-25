use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::binary_cas::{BinaryCasContext, BlobDataReader};
use crate::engine2::live_state::CommittedLiveStateContext;
use crate::engine2::live_state::LiveStateContext;
use crate::engine2::schema_registry::SchemaRegistry;
use crate::functions::{
    DynFunctionProvider, LixFunctionProvider, SharedFunctionProvider, SystemFunctionProvider,
};
use crate::sql2::SqlExecutionContext;
use crate::{LixBackend, LixError};

mod execute;

pub use execute::{ExecuteResult, Row, RowRef, RowSet};

#[derive(Clone)]
pub struct Session {
    active_version_id: String,
    backend: Arc<dyn LixBackend + Send + Sync>,
    committed_live_state: Arc<CommittedLiveStateContext>,
    binary_cas: Arc<BinaryCasContext>,
    schema_registry: Arc<SchemaRegistry>,
    functions: DynFunctionProvider,
}

impl Session {
    pub(crate) async fn open(
        active_version_id: String,
        backend: Arc<dyn LixBackend + Send + Sync>,
        committed_live_state: Arc<CommittedLiveStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        schema_registry: Arc<SchemaRegistry>,
    ) -> Result<Self, LixError> {
        Ok(Self::new(
            active_version_id,
            backend,
            committed_live_state,
            binary_cas,
            schema_registry,
        ))
    }

    pub(crate) fn new(
        active_version_id: String,
        backend: Arc<dyn LixBackend + Send + Sync>,
        committed_live_state: Arc<CommittedLiveStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        schema_registry: Arc<SchemaRegistry>,
    ) -> Self {
        Self {
            active_version_id,
            backend,
            committed_live_state,
            binary_cas,
            schema_registry,
            // The session owns the function source so reads, writes, UDFs, and
            // provider-side staging can share one execution lineage.
            // TODO(engine2): replace the system provider with runtime-bound or
            // deterministic bindings when engine2 owns that boot layer.
            functions: SharedFunctionProvider::new(
                Box::new(SystemFunctionProvider) as Box<dyn LixFunctionProvider + Send>
            ),
        }
    }

    pub fn active_version_id(&self) -> &str {
        &self.active_version_id
    }
}

struct SessionSqlExecutionContext<'a> {
    active_version_id: &'a str,
    committed_live_state: Arc<CommittedLiveStateContext>,
    binary_cas: Arc<BinaryCasContext>,
    visible_schemas: Vec<JsonValue>,
    functions: DynFunctionProvider,
}

impl SqlExecutionContext for SessionSqlExecutionContext<'_> {
    fn active_version_id(&self) -> &str {
        self.active_version_id
    }

    fn live_state(&self) -> Arc<dyn LiveStateContext> {
        let live_state: Arc<dyn LiveStateContext> = self.committed_live_state.clone();
        live_state
    }

    fn functions(&self) -> DynFunctionProvider {
        self.functions.clone()
    }

    fn blob_reader(&self) -> Arc<dyn BlobDataReader> {
        Arc::clone(&self.binary_cas) as Arc<dyn BlobDataReader>
    }

    fn list_visible_schemas(&self, version_id: &str) -> Result<Vec<JsonValue>, LixError> {
        let _ = version_id;
        Ok(self.visible_schemas.clone())
    }
}

#[async_trait]
impl BlobDataReader for BinaryCasContext {
    async fn load_blob_data_by_hash(&self, blob_hash: &str) -> Result<Option<Vec<u8>>, LixError> {
        self.load_blob_data_by_hash(blob_hash).await
    }
}
