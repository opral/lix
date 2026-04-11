use crate::catalog::{
    CatalogDerivedRow, CatalogProjectionLifecycle, CatalogProjectionRegistry,
    RegisteredCatalogProjection,
};
use crate::contracts::ReadTimeProjectionRead;
use crate::live_state::projection::hydration::hydrate_projection_input_with_backend;
use crate::{LixBackend, LixError};

pub(crate) async fn derive_read_time_projection_rows_with_backend(
    backend: &dyn LixBackend,
    registry: &CatalogProjectionRegistry,
    artifact: &ReadTimeProjectionRead,
) -> Result<Vec<CatalogDerivedRow>, LixError> {
    let mut rows = Vec::new();
    for registration in registry.registrations() {
        if registration.lifecycle() != CatalogProjectionLifecycle::ReadTime {
            continue;
        }
        if !registration
            .projection()
            .surfaces()
            .iter()
            .any(|surface| surface.public_name == artifact.surface_name)
        {
            continue;
        }
        rows.extend(
            derive_registered_projection_rows_with_backend(backend, &registration, artifact)
                .await?,
        );
    }
    Ok(rows)
}

async fn derive_registered_projection_rows_with_backend(
    backend: &dyn LixBackend,
    registration: &RegisteredCatalogProjection,
    artifact: &ReadTimeProjectionRead,
) -> Result<Vec<CatalogDerivedRow>, LixError> {
    let projection = registration.projection();
    let input = hydrate_projection_input_with_backend(
        backend,
        projection,
        artifact.requested_version_id.as_deref(),
    )
    .await?;
    projection.derive(&input)
}
