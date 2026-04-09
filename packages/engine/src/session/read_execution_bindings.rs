use async_trait::async_trait;

use crate::contracts::artifacts::PreparedPublicReadArtifact;
use crate::contracts::traits::PendingView;
use crate::execution::read::{
    execute_prepared_public_read_artifact_with_backend, PendingPublicReadExecutionBackend,
    ReadExecutionBindings, ReadTimeProjectionRow,
};
use crate::projections::ProjectionRegistry;
use crate::session::collaborators::SessionCollaborators;
use crate::{LixBackend, LixError, QueryResult};

pub(crate) struct ProjectionRegistryReadExecutionBindings<'a> {
    projection_registry: &'a ProjectionRegistry,
}

impl<'a> ProjectionRegistryReadExecutionBindings<'a> {
    pub(crate) fn new(projection_registry: &'a ProjectionRegistry) -> Self {
        Self {
            projection_registry,
        }
    }
}

pub(crate) async fn derive_read_time_projection_rows_with_registry(
    projection_registry: &ProjectionRegistry,
    backend: &dyn LixBackend,
) -> Result<Vec<ReadTimeProjectionRow>, LixError> {
    Ok(
        crate::live_state::derive_read_time_surface_rows(backend, projection_registry)
            .await?
            .into_iter()
            .map(|row| ReadTimeProjectionRow {
                surface_name: row.surface_name,
                identity: row.identity,
                values: row.values,
            })
            .collect(),
    )
}

#[async_trait(?Send)]
impl ReadExecutionBindings for ProjectionRegistryReadExecutionBindings<'_> {
    async fn derive_read_time_projection_rows(
        &self,
        backend: &dyn LixBackend,
    ) -> Result<Vec<ReadTimeProjectionRow>, LixError> {
        derive_read_time_projection_rows_with_registry(self.projection_registry, backend).await
    }
}

#[async_trait(?Send)]
impl ReadExecutionBindings for SessionCollaborators {
    async fn derive_read_time_projection_rows(
        &self,
        backend: &dyn LixBackend,
    ) -> Result<Vec<ReadTimeProjectionRow>, LixError> {
        derive_read_time_projection_rows_with_registry(self.projection_registry(), backend).await
    }
}

#[async_trait(?Send)]
impl PendingPublicReadExecutionBackend for dyn LixBackend + '_ {
    async fn execute_prepared_public_read_with_pending_view(
        &self,
        bindings: &dyn ReadExecutionBindings,
        pending_view: Option<&dyn PendingView>,
        public_read: &PreparedPublicReadArtifact,
    ) -> Result<QueryResult, LixError> {
        match public_read.contract.execution_mode() {
            crate::contracts::artifacts::PublicReadExecutionMode::PendingView => {
                crate::session::pending_reads::execute_prepared_public_read_with_pending_view(
                    self,
                    pending_view,
                    public_read,
                )
                .await
            }
            crate::contracts::artifacts::PublicReadExecutionMode::Committed(_) => {
                execute_prepared_public_read_artifact_with_backend(self, bindings, public_read)
                    .await
            }
        }
    }
}
