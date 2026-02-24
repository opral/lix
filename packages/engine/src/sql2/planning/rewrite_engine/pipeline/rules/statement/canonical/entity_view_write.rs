use sqlparser::ast::{Delete, Insert, Update};

use crate::cel::CelEvaluator;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::engine::sql2::planning::rewrite_engine::entity_views::write as entity_view_write;
use crate::{LixBackend, LixError, Value};

pub(crate) fn rewrite_insert(insert: Insert, params: &[Value]) -> Result<Option<Insert>, LixError> {
    entity_view_write::rewrite_insert(insert, params)
}

pub(crate) async fn rewrite_insert_with_backend<P>(
    backend: &dyn LixBackend,
    insert: Insert,
    params: &[Value],
    functions: &mut P,
) -> Result<Option<Insert>, LixError>
where
    P: LixFunctionProvider + Clone + Send + 'static,
{
    entity_view_write::rewrite_insert_with_backend(
        backend,
        insert,
        params,
        &CelEvaluator::new(),
        SharedFunctionProvider::new(functions.clone()),
    )
    .await
}

pub(crate) fn rewrite_update(update: Update, params: &[Value]) -> Result<Option<Update>, LixError> {
    entity_view_write::rewrite_update(update, params)
}

pub(crate) async fn rewrite_update_with_backend(
    backend: &dyn LixBackend,
    update: Update,
    params: &[Value],
) -> Result<Option<Update>, LixError> {
    entity_view_write::rewrite_update_with_backend(backend, update, params).await
}

pub(crate) fn rewrite_delete(delete: Delete) -> Result<Option<Delete>, LixError> {
    entity_view_write::rewrite_delete(delete)
}

pub(crate) async fn rewrite_delete_with_backend(
    backend: &dyn LixBackend,
    delete: Delete,
) -> Result<Option<Delete>, LixError> {
    entity_view_write::rewrite_delete_with_backend(backend, delete).await
}
