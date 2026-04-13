//! Public-read execution adapters.
//!
//! These adapters bridge catalog-backed read-time projection derivation and
//! committed-versus-pending public-read execution onto the generic read
//! execution traits.

use async_trait::async_trait;

use crate::catalog::CatalogProjectionRegistry;
use crate::contracts::{PendingPublicReadHost, ReadExecutionHost, ReadTimeProjectionRow};
use crate::contracts::{PreparedPublicRead, ReadTimeProjectionPlan};
use crate::execution::read::execute_prepared_public_read_artifact_with_backend;
use crate::session::host::SessionExecutionContext;
use crate::transaction::PendingOverlay;
use crate::{LixBackend, LixError, QueryResult};

pub(crate) struct ProjectionReadExecutionHost<'a> {
    projection_registry: &'a CatalogProjectionRegistry,
}

impl<'a> ProjectionReadExecutionHost<'a> {
    pub(crate) fn new(projection_registry: &'a CatalogProjectionRegistry) -> Self {
        Self {
            projection_registry,
        }
    }
}

pub(crate) async fn derive_read_time_projection_rows_with_registry(
    projection_registry: &CatalogProjectionRegistry,
    backend: &dyn LixBackend,
    artifact: &ReadTimeProjectionPlan,
) -> Result<Vec<ReadTimeProjectionRow>, LixError> {
    Ok(
        crate::live_state::derive_read_time_surface_rows(backend, projection_registry, artifact)
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
impl ReadExecutionHost for ProjectionReadExecutionHost<'_> {
    async fn derive_read_time_projection_rows(
        &self,
        backend: &dyn LixBackend,
        artifact: &ReadTimeProjectionPlan,
    ) -> Result<Vec<ReadTimeProjectionRow>, LixError> {
        derive_read_time_projection_rows_with_registry(self.projection_registry, backend, artifact)
            .await
    }
}

#[async_trait(?Send)]
impl ReadExecutionHost for SessionExecutionContext<'_> {
    async fn derive_read_time_projection_rows(
        &self,
        backend: &dyn LixBackend,
        artifact: &ReadTimeProjectionPlan,
    ) -> Result<Vec<ReadTimeProjectionRow>, LixError> {
        derive_read_time_projection_rows_with_registry(
            self.session_host().catalog_projection_registry(),
            backend,
            artifact,
        )
        .await
    }
}

#[async_trait(?Send)]
impl PendingPublicReadHost for dyn LixBackend + '_ {
    async fn execute_prepared_public_read_with_pending_overlay(
        &self,
        host: &dyn ReadExecutionHost,
        pending_overlay: Option<&dyn PendingOverlay>,
        public_read: &PreparedPublicRead,
    ) -> Result<QueryResult, LixError> {
        match public_read.contract.source() {
            crate::contracts::PublicReadSource::PendingOverlay => {
                crate::session::pending_reads::execute_prepared_public_read_with_pending_overlay(
                    self,
                    pending_overlay,
                    public_read,
                )
                .await
            }
            crate::contracts::PublicReadSource::Committed(_) => {
                execute_prepared_public_read_artifact_with_backend(self, host, public_read).await
            }
        }
    }
}
