//! Public-read execution adapters.
//!
//! These adapters bridge catalog-backed read-time projection derivation and
//! committed-versus-pending public-read execution onto the generic read
//! execution traits.

use async_trait::async_trait;

use crate::catalog::{
    CatalogProjectionRegistry, CatalogReadTimeProjectionRequest, SurfaceReadFreshness,
};
use crate::execution::{ReadExecutionHost, ReadTimeProjectionIdentity, ReadTimeProjectionRow};
use crate::session::host::SessionExecutionContext;
use crate::{LixBackend, LixBackendTransaction, LixError};

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
    request: &CatalogReadTimeProjectionRequest,
) -> Result<Vec<ReadTimeProjectionRow>, LixError> {
    Ok(
        crate::live_state::derive_read_time_surface_rows(backend, projection_registry, request)
            .await?
            .into_iter()
            .map(|row| ReadTimeProjectionRow {
                surface_name: row.surface_name,
                identity: row.identity.map(|identity| ReadTimeProjectionIdentity {
                    schema_key: identity.schema_key,
                    version_id: identity.version_id,
                    entity_id: identity.entity_id,
                    file_id: identity.file_id,
                }),
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
        request: &CatalogReadTimeProjectionRequest,
    ) -> Result<Vec<ReadTimeProjectionRow>, LixError> {
        derive_read_time_projection_rows_with_registry(self.projection_registry, backend, request)
            .await
    }

    async fn ensure_projection_freshness_with_backend(
        &self,
        backend: &dyn LixBackend,
        freshness_contract: SurfaceReadFreshness,
        resolved_relations: &[String],
    ) -> Result<(), LixError> {
        crate::live_state::ensure_projection_read_freshness_with_backend(
            backend,
            freshness_contract,
            resolved_relations,
        )
        .await
    }

    async fn ensure_projection_freshness_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        freshness_contract: SurfaceReadFreshness,
        resolved_relations: &[String],
    ) -> Result<(), LixError> {
        crate::live_state::ensure_projection_read_freshness_in_transaction(
            transaction,
            freshness_contract,
            resolved_relations,
        )
        .await
    }
}

#[async_trait(?Send)]
impl ReadExecutionHost for SessionExecutionContext<'_> {
    async fn derive_read_time_projection_rows(
        &self,
        backend: &dyn LixBackend,
        request: &CatalogReadTimeProjectionRequest,
    ) -> Result<Vec<ReadTimeProjectionRow>, LixError> {
        derive_read_time_projection_rows_with_registry(
            self.session_host().catalog_projection_registry(),
            backend,
            request,
        )
        .await
    }

    async fn ensure_projection_freshness_with_backend(
        &self,
        backend: &dyn LixBackend,
        freshness_contract: SurfaceReadFreshness,
        resolved_relations: &[String],
    ) -> Result<(), LixError> {
        crate::live_state::ensure_projection_read_freshness_with_backend(
            backend,
            freshness_contract,
            resolved_relations,
        )
        .await
    }

    async fn ensure_projection_freshness_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        freshness_contract: SurfaceReadFreshness,
        resolved_relations: &[String],
    ) -> Result<(), LixError> {
        crate::live_state::ensure_projection_read_freshness_in_transaction(
            transaction,
            freshness_contract,
            resolved_relations,
        )
        .await
    }
}
