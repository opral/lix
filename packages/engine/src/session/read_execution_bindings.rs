use async_trait::async_trait;

use crate::execution::read::{ReadExecutionBindings, ReadTimeProjectionRow};
use crate::projections::ProjectionRegistry;
use crate::session::collaborators::SessionCollaborators;
use crate::{LixBackend, LixError};

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
        crate::live_state::projection::dispatch::derive_read_time_projection_rows_with_backend(
            backend,
            projection_registry,
        )
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
