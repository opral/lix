use sqlparser::ast::{Delete, Insert, Statement};

use crate::sql::steps::lix_active_account_view_write;
use crate::{LixBackend, LixError, Value};

pub(crate) fn rewrite_insert(
    insert: Insert,
    params: &[Value],
) -> Result<Option<Vec<Insert>>, LixError> {
    lix_active_account_view_write::rewrite_insert(insert, params)
}

pub(crate) async fn rewrite_delete_with_backend(
    backend: &dyn LixBackend,
    delete: Delete,
    params: &[Value],
) -> Result<Option<Statement>, LixError> {
    lix_active_account_view_write::rewrite_delete_with_backend(backend, delete, params).await
}
