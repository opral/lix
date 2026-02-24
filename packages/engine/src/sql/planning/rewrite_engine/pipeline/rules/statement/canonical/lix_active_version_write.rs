use sqlparser::ast::{Insert, Update};

use crate::engine::sql::planning::rewrite_engine::steps::lix_active_version_view_write;
use crate::{LixBackend, LixError, Value};

pub(crate) async fn rewrite_update_with_backend(
    backend: &dyn LixBackend,
    update: Update,
    params: &[Value],
) -> Result<Option<Vec<Insert>>, LixError> {
    lix_active_version_view_write::rewrite_update_with_backend(backend, update, params).await
}
