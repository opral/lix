use std::sync::Arc;

use serde_json::Value as JsonValue;

use crate::binary_cas::{BinaryCasContext, BlobDataReader};
use crate::engine2::changelog::{ChangelogContext, ChangelogReader};
use crate::engine2::commit_graph::{CommitGraphContext, CommitGraphReader};
use crate::engine2::entity_identity::EntityIdentity;
use crate::engine2::functions::FunctionProviderHandle;
use crate::engine2::live_state::{LiveStateContext, LiveStateReader, LiveStateRowRequest};
use crate::engine2::schema_registry::SchemaRegistry;
use crate::engine2::tracked_state::TrackedStateContext;
use crate::engine2::version_ref::{VersionRefContext, VersionRefReader};
use crate::sql2::SqlExecutionContext;
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackend, LixError, NullableKeyFilter};

pub(super) const WORKSPACE_VERSION_KEY: &str = "lix_workspace_version_id";

#[derive(Clone)]
pub(super) enum SessionMode {
    Pinned { version_id: String },
    Workspace,
}

/// Session-context state for engine2 SQL execution.
///
/// A session context pins the active version selector and shared execution
/// services. Each call to `execute(...)` projects this state into a read-only
/// SQL context or a transaction-owned write context.
#[derive(Clone)]
pub struct SessionContext {
    pub(super) mode: SessionMode,
    pub(super) backend: Arc<dyn LixBackend + Send + Sync>,
    pub(super) live_state: Arc<LiveStateContext>,
    pub(super) tracked_state: Arc<TrackedStateContext>,
    pub(super) binary_cas: Arc<BinaryCasContext>,
    pub(super) changelog: Arc<ChangelogContext>,
    pub(super) version_ref: Arc<VersionRefContext>,
    pub(super) schema_registry: Arc<SchemaRegistry>,
}

impl SessionContext {
    pub(crate) async fn open_workspace(
        backend: Arc<dyn LixBackend + Send + Sync>,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        changelog: Arc<ChangelogContext>,
        version_ref: Arc<VersionRefContext>,
        schema_registry: Arc<SchemaRegistry>,
    ) -> Result<Self, LixError> {
        let session = Self::new(
            SessionMode::Workspace,
            backend,
            live_state,
            tracked_state,
            binary_cas,
            changelog,
            version_ref,
            schema_registry,
        );
        session.active_version_id().await?;
        Ok(session)
    }

    pub(crate) async fn open(
        active_version_id: String,
        backend: Arc<dyn LixBackend + Send + Sync>,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        changelog: Arc<ChangelogContext>,
        version_ref: Arc<VersionRefContext>,
        schema_registry: Arc<SchemaRegistry>,
    ) -> Result<Self, LixError> {
        Ok(Self::new(
            SessionMode::Pinned {
                version_id: active_version_id,
            },
            backend,
            live_state,
            tracked_state,
            binary_cas,
            changelog,
            version_ref,
            schema_registry,
        ))
    }

    pub(super) fn new(
        mode: SessionMode,
        backend: Arc<dyn LixBackend + Send + Sync>,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        changelog: Arc<ChangelogContext>,
        version_ref: Arc<VersionRefContext>,
        schema_registry: Arc<SchemaRegistry>,
    ) -> Self {
        Self {
            mode,
            backend,
            live_state,
            tracked_state,
            binary_cas,
            changelog,
            version_ref,
            schema_registry,
        }
    }

    /// Resolves the version this session should operate on right now.
    ///
    /// Pinned sessions are pure in-memory views over one version. Workspace
    /// sessions read the shared workspace selector from untracked global
    /// `lix_key_value` state so multiple open app sessions can observe the same
    /// active workspace version.
    pub async fn active_version_id(&self) -> Result<String, LixError> {
        match &self.mode {
            SessionMode::Pinned { version_id } => Ok(version_id.clone()),
            SessionMode::Workspace => self.load_workspace_version_id().await,
        }
    }

    async fn load_workspace_version_id(&self) -> Result<String, LixError> {
        let row = self
            .live_state
            .reader(Arc::clone(&self.backend))
            .load_row(&LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                entity_id: EntityIdentity::single(WORKSPACE_VERSION_KEY),
                file_id: NullableKeyFilter::Null,
            })
            .await?
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "workspace version selector is missing lix_key_value:lix_workspace_version_id",
                )
            })?;
        let snapshot_content = row.snapshot_content.as_deref().ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "workspace version selector is missing snapshot_content",
            )
        })?;
        let snapshot = serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("workspace version selector snapshot is invalid JSON: {error}"),
            )
        })?;
        let version_id = snapshot
            .get("value")
            .and_then(JsonValue::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "workspace version selector value must be a non-empty string",
                )
            })?
            .to_string();

        let head = self
            .version_ref
            .reader(Arc::clone(&self.backend))
            .load_head_commit_id(&version_id)
            .await?;
        if head.is_none() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("workspace version selector points to missing version ref '{version_id}'"),
            ));
        }

        Ok(version_id)
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
    pub(super) changelog: Arc<ChangelogContext>,
    pub(super) version_ref: Arc<VersionRefContext>,
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

    fn changelog(&self) -> Arc<dyn ChangelogReader> {
        Arc::new(self.changelog.reader(Arc::clone(&self.backend)))
    }

    fn commit_graph(&self) -> Box<dyn CommitGraphReader> {
        Box::new(CommitGraphContext::new(ChangelogContext::new()).reader(Arc::clone(&self.backend)))
    }

    fn version_ref(&self) -> Arc<dyn VersionRefReader> {
        Arc::new(self.version_ref.reader(Arc::clone(&self.backend)))
    }

    fn functions(&self) -> FunctionProviderHandle {
        self.functions.clone()
    }

    fn blob_reader(&self) -> Arc<dyn BlobDataReader> {
        Arc::new(self.binary_cas.reader(Arc::clone(&self.backend))) as Arc<dyn BlobDataReader>
    }

    fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
        Ok(self.visible_schemas.clone())
    }
}
