use crate::contracts::artifacts::{DerivedRow, ProjectionLifecycle};
use crate::live_state::projection::hydration::hydrate_projection_input_with_backend;
use crate::projections::{builtin_projection_registrations, BuiltinProjectionRegistration};
use crate::{LixBackend, LixError};

/// Derive rows for all built-in `ReadTime` projections through the trait seam.
///
/// This path is intentionally internal and bounded. It proves `live_state` can
/// enumerate built-in registrations, hydrate their declared input, and call
/// `derive()` without coupling to projection-specific storage access.
pub(crate) async fn derive_read_time_projection_rows_with_backend(
    backend: &dyn LixBackend,
) -> Result<Vec<DerivedRow>, LixError> {
    let mut rows = Vec::new();
    for registration in builtin_projection_registrations() {
        if registration.lifecycle() != ProjectionLifecycle::ReadTime {
            continue;
        }
        rows.extend(derive_registered_projection_rows_with_backend(backend, &registration).await?);
    }
    Ok(rows)
}

async fn derive_registered_projection_rows_with_backend(
    backend: &dyn LixBackend,
    registration: &BuiltinProjectionRegistration,
) -> Result<Vec<DerivedRow>, LixError> {
    let projection = registration.projection();
    let input = hydrate_projection_input_with_backend(backend, projection).await?;
    projection.derive(&input)
}
