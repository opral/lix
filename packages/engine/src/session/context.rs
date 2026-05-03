use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde_json::Value as JsonValue;

use crate::backend::ReadScope;
use crate::binary_cas::{BinaryCasContext, BlobDataReader};
use crate::changelog::ChangelogContext;
use crate::commit_graph::{CommitGraphContext, CommitGraphReader};
use crate::entity_identity::EntityIdentity;
use crate::functions::{FunctionContext, FunctionProviderHandle};
use crate::json_store::JsonStoreContext;
use crate::live_state::{LiveStateContext, LiveStateReader, LiveStateRowRequest};
use crate::schema_registry::SchemaRegistry;
use crate::sql2::{ChangelogQuerySource, SqlChangelogQuerySource, SqlExecutionContext};
use crate::tracked_state::TrackedStateContext;
use crate::transaction::{open_transaction, Transaction};
use crate::version_ref::{VersionRefContext, VersionRefReader};
use crate::GLOBAL_VERSION_ID;
use crate::{LixBackend, LixError, NullableKeyFilter};

pub(crate) const WORKSPACE_VERSION_KEY: &str = "lix_workspace_version_id";

#[derive(Clone)]
pub(crate) enum SessionMode {
    Pinned { version_id: String },
    Workspace,
}

/// Session-context state for engine2 execution.
///
/// A session context pins the active version selector and shared execution
/// services. Each call to `execute(...)` projects this state into a read-only
/// SQL context or a transaction-owned write context.
///
/// Write transaction invariant: any engine2 operation that may write must enter
/// through `SessionContext::with_write_transaction`. Reads that influence writes
/// are only available from that transaction capability, not from session-level
/// helpers.
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
    closed: Arc<AtomicBool>,
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
        Self::new_with_closed(
            mode,
            backend,
            live_state,
            tracked_state,
            binary_cas,
            changelog,
            version_ref,
            schema_registry,
            Arc::new(AtomicBool::new(false)),
        )
    }

    pub(super) fn new_with_closed(
        mode: SessionMode,
        backend: Arc<dyn LixBackend + Send + Sync>,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        changelog: Arc<ChangelogContext>,
        version_ref: Arc<VersionRefContext>,
        schema_registry: Arc<SchemaRegistry>,
        closed: Arc<AtomicBool>,
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
            closed,
        }
    }

    /// Releases this logical session handle. This is a lifecycle boundary only:
    /// successful writes are committed before their operation returns.
    pub async fn close(&self) -> Result<(), LixError> {
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub(crate) fn closed_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.closed)
    }

    pub(crate) fn ensure_open(&self) -> Result<(), LixError> {
        if self.is_closed() {
            return Err(closed_error());
        }
        Ok(())
    }

    /// Resolves the version this session should operate on right now.
    ///
    /// This is a read-path helper. Write flows must resolve the active version
    /// through the transaction capability so the read is scoped to the
    /// same backend transaction as the writes it influences.
    ///
    /// Pinned sessions are pure in-memory views over one version. Workspace
    /// sessions read the shared workspace selector from untracked global
    /// `lix_key_value` state so multiple open app sessions can observe the same
    /// active workspace version.
    pub async fn active_version_id(&self) -> Result<String, LixError> {
        self.ensure_open()?;
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

    pub(crate) async fn with_write_transaction<T, F>(&self, f: F) -> Result<T, LixError>
    where
        F: for<'tx> FnOnce(
            &'tx mut Transaction<'_>,
        ) -> Pin<Box<dyn Future<Output = Result<T, LixError>> + 'tx>>,
    {
        self.ensure_open()?;
        let live_state: Arc<dyn LiveStateReader> =
            Arc::new(self.live_state.reader(Arc::clone(&self.backend)));
        let runtime_functions = FunctionContext::prepare(live_state.as_ref()).await?;
        let mut transaction = open_transaction(
            &self.mode,
            &self.backend,
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.changelog),
            Arc::clone(&self.version_ref),
            Arc::clone(&self.schema_registry),
            runtime_functions.provider(),
        )
        .await?;

        match f(&mut transaction).await {
            Ok(value) => {
                transaction.commit(&runtime_functions).await?;
                Ok(value)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }
}

fn closed_error() -> LixError {
    LixError::new(LixError::CODE_CLOSED, "Lix handle is closed")
        .with_hint("Open a new Lix handle before calling this method.")
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

    fn changelog_query_source(&self) -> SqlChangelogQuerySource {
        let read_scope = ReadScope::new(Arc::clone(&self.backend));
        ChangelogQuerySource {
            changelog_reader: Arc::new(self.changelog.reader(read_scope.store())),
            json_reader: JsonStoreContext::new().reader(read_scope.store()),
        }
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
