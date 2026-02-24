use sqlparser::ast::{Delete, Insert, Update};

use crate::engine::sql2::planning::rewrite_engine::steps::lix_state_view_write;
use crate::{LixBackend, LixError, Value};

pub(crate) async fn rewrite_insert_with_backend(
    backend: &dyn LixBackend,
    insert: Insert,
) -> Result<Option<Insert>, LixError> {
    lix_state_view_write::rewrite_insert_with_backend(backend, insert).await
}

pub(crate) async fn rewrite_update_with_backend(
    backend: &dyn LixBackend,
    update: Update,
    params: &[Value],
) -> Result<Option<Update>, LixError> {
    lix_state_view_write::rewrite_update_with_backend(backend, update, params).await
}

pub(crate) async fn rewrite_delete_with_backend(
    backend: &dyn LixBackend,
    delete: Delete,
) -> Result<Option<Delete>, LixError> {
    lix_state_view_write::rewrite_delete_with_backend(backend, delete).await
}
