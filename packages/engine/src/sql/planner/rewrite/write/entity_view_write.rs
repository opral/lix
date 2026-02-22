use sqlparser::ast::{Delete, Insert, Update};

use crate::cel::CelEvaluator;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::sql::entity_views::write as entity_view_write;
use crate::{LixBackend, LixError, Value};

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

pub(crate) async fn rewrite_update_with_backend(
    backend: &dyn LixBackend,
    update: Update,
    params: &[Value],
) -> Result<Option<Update>, LixError> {
    entity_view_write::rewrite_update_with_backend(backend, update, params).await
}

pub(crate) async fn rewrite_delete_with_backend(
    backend: &dyn LixBackend,
    delete: Delete,
) -> Result<Option<Delete>, LixError> {
    entity_view_write::rewrite_delete_with_backend(backend, delete).await
}
