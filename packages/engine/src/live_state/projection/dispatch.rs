use crate::live_state::projection::hydration::hydrate_projection_input_with_backend;
use crate::projections::{
    DerivedRow, ProjectionLifecycle, ProjectionRegistry, RegisteredProjection,
};
use crate::{LixBackend, LixError};

pub(crate) async fn derive_read_time_projection_rows_with_backend(
    backend: &dyn LixBackend,
    registry: &ProjectionRegistry,
) -> Result<Vec<DerivedRow>, LixError> {
    let mut rows = Vec::new();
    for registration in registry.registrations() {
        if registration.lifecycle() != ProjectionLifecycle::ReadTime {
            continue;
        }
        rows.extend(derive_registered_projection_rows_with_backend(backend, &registration).await?);
    }
    Ok(rows)
}

async fn derive_registered_projection_rows_with_backend(
    backend: &dyn LixBackend,
    registration: &RegisteredProjection,
) -> Result<Vec<DerivedRow>, LixError> {
    let projection = registration.projection();
    let input = hydrate_projection_input_with_backend(backend, projection).await?;
    projection.derive(&input)
}
