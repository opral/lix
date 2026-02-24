use sqlparser::ast::{Delete, Insert, Update};

use crate::engine::sql::planning::rewrite_engine::steps::lix_version_view_write;
use crate::{LixBackend, LixError, Value};

pub(crate) fn rewrite_insert(
    insert: Insert,
    params: &[Value],
) -> Result<Option<Vec<Insert>>, LixError> {
    lix_version_view_write::rewrite_insert(insert, params)
}

pub(crate) async fn rewrite_insert_with_backend(
    backend: &dyn LixBackend,
    insert: Insert,
    params: &[Value],
) -> Result<Option<Vec<Insert>>, LixError> {
    lix_version_view_write::rewrite_insert_with_backend(backend, insert, params).await
}

pub(crate) async fn rewrite_update_with_backend(
    backend: &dyn LixBackend,
    update: Update,
    params: &[Value],
) -> Result<Option<Vec<Insert>>, LixError> {
    lix_version_view_write::rewrite_update_with_backend(backend, update, params).await
}

pub(crate) async fn rewrite_delete_with_backend(
    backend: &dyn LixBackend,
    delete: Delete,
    params: &[Value],
) -> Result<Option<Vec<Insert>>, LixError> {
    lix_version_view_write::rewrite_delete_with_backend(backend, delete, params).await
}
