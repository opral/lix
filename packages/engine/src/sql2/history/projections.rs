use crate::{LixBackend, LixError};

pub(crate) async fn refresh_working_projection_for_read_query(
    backend: &dyn LixBackend,
    active_version_id: &str,
) -> Result<(), LixError> {
    crate::sql::refresh_working_projection_for_read_query(backend, active_version_id).await
}
