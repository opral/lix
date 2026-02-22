use sqlparser::ast::{Delete, Insert, Statement, Update};

use crate::filesystem::mutation_rewrite::ResolvedDirectoryIdMap;
use crate::sql::steps::filesystem_step;
use crate::LixBackend;
use crate::{LixError, Value};

pub(crate) async fn insert_side_effects_with_backend(
    backend: &dyn LixBackend,
    insert: &Insert,
    params: &[Value],
) -> Result<crate::filesystem::mutation_rewrite::FilesystemInsertSideEffects, LixError> {
    filesystem_step::insert_side_effect_statements_with_backend(backend, insert, params).await
}

pub(crate) async fn rewrite_insert_with_backend(
    backend: &dyn LixBackend,
    insert: Insert,
    params: &[Value],
    resolved_directory_ids: Option<&ResolvedDirectoryIdMap>,
    active_version_id: Option<&str>,
) -> Result<Option<Insert>, LixError> {
    filesystem_step::rewrite_insert_with_backend(
        backend,
        insert,
        params,
        resolved_directory_ids,
        active_version_id,
    )
    .await
}

pub(crate) async fn rewrite_update_with_backend(
    backend: &dyn LixBackend,
    update: Update,
    params: &[Value],
) -> Result<Option<Statement>, LixError> {
    filesystem_step::rewrite_update_with_backend(backend, update, params).await
}

pub(crate) async fn rewrite_delete_with_backend(
    backend: &dyn LixBackend,
    delete: Delete,
    params: &[Value],
) -> Result<Option<Delete>, LixError> {
    filesystem_step::rewrite_delete_with_backend(backend, delete, params).await
}
