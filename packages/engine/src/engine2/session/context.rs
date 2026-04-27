use std::sync::Arc;

use serde_json::Value as JsonValue;

use crate::binary_cas::{BinaryCasContext, BlobDataReader};
use crate::engine2::changelog::ChangelogContext;
use crate::engine2::functions::FunctionProviderHandle;
use crate::engine2::live_state::{LiveStateContext, LiveStateReader};
use crate::engine2::schema_registry::SchemaRegistry;
use crate::sql2::SqlExecutionContext;
use crate::{LixBackend, LixError};

/// Session-context state for engine2 SQL execution.
///
/// A session context pins the active version selector and shared execution
/// services. Each call to `execute(...)` projects this state into a read-only
/// SQL context or a transaction-owned write context.
#[derive(Clone)]
pub struct SessionContext {
    pub(super) active_version_id: String,
    pub(super) backend: Arc<dyn LixBackend + Send + Sync>,
    pub(super) live_state: Arc<LiveStateContext>,
    pub(super) binary_cas: Arc<BinaryCasContext>,
    pub(super) changelog: Arc<ChangelogContext>,
    pub(super) schema_registry: Arc<SchemaRegistry>,
}

impl SessionContext {
    pub(crate) async fn open(
        active_version_id: String,
        backend: Arc<dyn LixBackend + Send + Sync>,
        live_state: Arc<LiveStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        changelog: Arc<ChangelogContext>,
        schema_registry: Arc<SchemaRegistry>,
    ) -> Result<Self, LixError> {
        Ok(Self::new(
            active_version_id,
            backend,
            live_state,
            binary_cas,
            changelog,
            schema_registry,
        ))
    }

    pub(crate) fn new(
        active_version_id: String,
        backend: Arc<dyn LixBackend + Send + Sync>,
        live_state: Arc<LiveStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        changelog: Arc<ChangelogContext>,
        schema_registry: Arc<SchemaRegistry>,
    ) -> Self {
        Self {
            active_version_id,
            backend,
            live_state,
            binary_cas,
            changelog,
            schema_registry,
        }
    }

    pub fn active_version_id(&self) -> &str {
        &self.active_version_id
    }
}

/// Read-only SQL execution context derived from a session.
///
/// Write statements re-plan against `Transaction`; this context intentionally
/// has no write stager.
pub(super) struct SessionSqlExecutionContext<'a> {
    pub(super) active_version_id: &'a str,
    pub(super) backend: Arc<dyn LixBackend + Send + Sync>,
    pub(super) live_state: Arc<LiveStateContext>,
    pub(super) binary_cas: Arc<BinaryCasContext>,
    pub(super) visible_schemas: Vec<JsonValue>,
    pub(super) functions: FunctionProviderHandle,
}

impl SqlExecutionContext for SessionSqlExecutionContext<'_> {
    fn active_version_id(&self) -> &str {
        self.active_version_id
    }

    fn live_state(&self) -> Arc<dyn LiveStateReader> {
        Arc::new(self.live_state.reader(Arc::clone(&self.backend))) as Arc<dyn LiveStateReader>
    }

    fn functions(&self) -> FunctionProviderHandle {
        self.functions.clone()
    }

    fn blob_reader(&self) -> Arc<dyn BlobDataReader> {
        Arc::new(self.binary_cas.reader(Arc::clone(&self.backend))) as Arc<dyn BlobDataReader>
    }

    fn list_visible_schemas(&self, version_id: &str) -> Result<Vec<JsonValue>, LixError> {
        let _ = version_id;
        Ok(self.visible_schemas.clone())
    }
}
