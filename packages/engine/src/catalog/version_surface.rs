use std::fmt::Debug;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;

use async_trait::async_trait;
use tokio::sync::oneshot;

use crate::backend::TransactionBeginMode;
use crate::live_state::load_version_head_commit_map_with_executor;
use crate::live_state::tracked::{
    load_exact_row_with_backend as load_exact_tracked_row_with_backend,
    scan_rows_with_backend as scan_tracked_rows_with_backend, ExactTrackedRowRequest, TrackedRow,
    TrackedScanRequest,
};
use crate::version::{
    version_descriptor_schema_key, version_descriptor_schema_version, GLOBAL_VERSION_ID,
};
use crate::{LixBackend, LixError, NullableKeyFilter};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionSurfaceColumn {
    Id,
    Name,
    Hidden,
    CommitId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionSurfaceRow {
    pub id: String,
    pub name: String,
    pub hidden: bool,
    pub commit_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VersionSurfaceScanRequest {
    pub projection: Vec<VersionSurfaceColumn>,
    pub limit: Option<usize>,
}

#[async_trait(?Send)]
pub trait VersionSurfaceSnapshot: Debug + Send + Sync {
    async fn scan_versions(
        &self,
        request: &VersionSurfaceScanRequest,
    ) -> Result<Vec<VersionSurfaceRow>, LixError>;
}

pub async fn open_version_surface_snapshot(
    backend: &dyn LixBackend,
) -> Result<Arc<dyn VersionSurfaceSnapshot>, LixError> {
    Ok(Arc::new(SnapshotBackedVersionSurface::load(backend).await?))
}

pub(crate) async fn load_version_surface_row_with_backend(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<Option<VersionSurfaceRow>, LixError> {
    load_version_surface_row(backend, version_id).await
}

pub async fn open_version_surface_snapshot_with_shared_backend(
    backend: Arc<dyn LixBackend + Send + Sync>,
) -> Result<Arc<dyn VersionSurfaceSnapshot>, LixError> {
    let (command_tx, command_rx) = mpsc::channel::<TransactionBackedVersionSurfaceCommand>();
    let (ready_tx, ready_rx) = oneshot::channel::<Result<(), LixError>>();
    thread::Builder::new()
        .name("version-surface-query-snapshot".to_string())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("version surface runtime should build");
            let init = runtime
                .block_on(async { backend.begin_transaction(TransactionBeginMode::Read).await });

            let mut transaction = match init {
                Ok(transaction) => {
                    let _ = ready_tx.send(Ok(()));
                    transaction
                }
                Err(error) => {
                    let _ = ready_tx.send(Err(error));
                    return;
                }
            };

            while let Ok(command) = command_rx.recv() {
                match command {
                    TransactionBackedVersionSurfaceCommand::Scan { request, reply } => {
                        let result = runtime.block_on(async {
                            let backend =
                                crate::backend::transaction_backend_view(transaction.as_mut());
                            load_version_surface_rows(&backend, &request).await
                        });
                        let _ = reply.send(result);
                    }
                }
            }

            let _ = runtime.block_on(transaction.rollback());
        })
        .map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("failed to spawn version surface snapshot worker: {error}"),
            )
        })?;

    ready_rx.await.map_err(|_| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "version surface snapshot worker dropped initialization reply",
        )
    })??;

    Ok(Arc::new(TransactionBackedVersionSurface {
        commands: command_tx,
    }))
}

#[derive(Debug, Clone)]
struct SnapshotBackedVersionSurface {
    rows: Vec<VersionSurfaceRow>,
}

impl SnapshotBackedVersionSurface {
    async fn load(backend: &dyn LixBackend) -> Result<Self, LixError> {
        let rows = load_version_surface_rows(
            backend,
            &VersionSurfaceScanRequest {
                projection: Vec::new(),
                limit: None,
            },
        )
        .await?;
        Ok(Self { rows })
    }
}

#[derive(Debug)]
enum TransactionBackedVersionSurfaceCommand {
    Scan {
        request: VersionSurfaceScanRequest,
        reply: oneshot::Sender<Result<Vec<VersionSurfaceRow>, LixError>>,
    },
}

struct TransactionBackedVersionSurface {
    commands: mpsc::Sender<TransactionBackedVersionSurfaceCommand>,
}

impl std::fmt::Debug for TransactionBackedVersionSurface {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionBackedVersionSurface").finish()
    }
}

#[async_trait(?Send)]
impl VersionSurfaceSnapshot for SnapshotBackedVersionSurface {
    async fn scan_versions(
        &self,
        request: &VersionSurfaceScanRequest,
    ) -> Result<Vec<VersionSurfaceRow>, LixError> {
        let mut rows = self.rows.clone();
        if let Some(limit) = request.limit {
            rows.truncate(limit);
        }
        Ok(rows)
    }
}

#[async_trait(?Send)]
impl VersionSurfaceSnapshot for TransactionBackedVersionSurface {
    async fn scan_versions(
        &self,
        request: &VersionSurfaceScanRequest,
    ) -> Result<Vec<VersionSurfaceRow>, LixError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.commands
            .send(TransactionBackedVersionSurfaceCommand::Scan {
                request: request.clone(),
                reply: reply_tx,
            })
            .map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("failed to enqueue version surface scan: {error}"),
                )
            })?;
        reply_rx.await.map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "version surface snapshot worker dropped scan reply",
            )
        })?
    }
}

async fn load_version_surface_rows(
    backend: &dyn LixBackend,
    request: &VersionSurfaceScanRequest,
) -> Result<Vec<VersionSurfaceRow>, LixError> {
    let descriptor_rows = scan_tracked_rows_with_backend(
        backend,
        &TrackedScanRequest {
            schema_key: version_descriptor_schema_key().to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            constraints: Vec::new(),
            required_columns: vec!["id".to_string(), "name".to_string(), "hidden".to_string()],
        },
    )
    .await?
    .into_iter()
    .filter(|row| {
        row.file_id.is_none()
            && row.plugin_key.is_none()
            && row.schema_version == version_descriptor_schema_version()
    })
    .collect::<Vec<_>>();
    let mut executor = backend;
    let head_commit_ids = load_version_head_commit_map_with_executor(&mut executor)
        .await?
        .unwrap_or_default();
    let mut rows = descriptor_rows
        .iter()
        .map(|row| version_surface_row_from_tracked_row(row, &head_commit_ids))
        .collect::<Result<Vec<_>, _>>()?;
    rows.sort_by(|left, right| left.id.cmp(&right.id));
    if let Some(limit) = request.limit {
        rows.truncate(limit);
    }
    Ok(rows)
}

async fn load_version_surface_row(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<Option<VersionSurfaceRow>, LixError> {
    let Some(row) = load_exact_tracked_row_with_backend(
        backend,
        &ExactTrackedRowRequest {
            schema_key: version_descriptor_schema_key().to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            entity_id: version_id.to_string(),
            file_id: NullableKeyFilter::Null,
        },
    )
    .await?
    .filter(|row| {
        row.file_id.is_none()
            && row.plugin_key.is_none()
            && row.schema_version == version_descriptor_schema_version()
    }) else {
        return Ok(None);
    };

    let mut executor = backend;
    let head_commit_ids = load_version_head_commit_map_with_executor(&mut executor)
        .await?
        .unwrap_or_default();
    Ok(Some(version_surface_row_from_tracked_row(
        &row,
        &head_commit_ids,
    )?))
}

fn version_surface_row_from_tracked_row(
    row: &TrackedRow,
    head_commit_ids: &std::collections::BTreeMap<String, String>,
) -> Result<VersionSurfaceRow, LixError> {
    let id = row
        .values
        .get("id")
        .and_then(|value| match value {
            crate::Value::Text(value) => Some(value.clone()),
            _ => None,
        })
        .unwrap_or_else(|| row.entity_id.clone());
    let name = row
        .values
        .get("name")
        .and_then(|value| match value {
            crate::Value::Text(value) => Some(value.clone()),
            _ => None,
        })
        .unwrap_or_default();
    let hidden = row
        .values
        .get("hidden")
        .and_then(|value| match value {
            crate::Value::Boolean(value) => Some(*value),
            crate::Value::Integer(value) => Some(*value != 0),
            _ => None,
        })
        .unwrap_or(false);
    Ok(VersionSurfaceRow {
        id,
        name,
        hidden,
        commit_id: head_commit_ids
            .get(&row.entity_id)
            .cloned()
            .unwrap_or_default(),
    })
}
