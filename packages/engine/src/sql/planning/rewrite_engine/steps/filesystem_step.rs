use sqlparser::ast::{Delete, Insert, Update};

use crate::filesystem::mutation_rewrite;
pub(crate) use crate::filesystem::mutation_rewrite::FilesystemUpdateRewrite;
use crate::filesystem::mutation_rewrite::{FilesystemInsertSideEffects, ResolvedDirectoryIdMap};
use crate::{LixBackend, LixError, Value as EngineValue};

pub fn rewrite_insert(insert: Insert) -> Result<Option<Insert>, LixError> {
    mutation_rewrite::rewrite_insert(insert)
}

pub async fn rewrite_insert_with_backend(
    backend: &dyn LixBackend,
    insert: Insert,
    params: &[EngineValue],
    resolved_directory_ids: Option<&ResolvedDirectoryIdMap>,
    active_version_id_hint: Option<&str>,
) -> Result<Option<Insert>, LixError> {
    mutation_rewrite::rewrite_insert_with_backend(
        backend,
        insert,
        params,
        resolved_directory_ids,
        active_version_id_hint,
    )
    .await
}

pub async fn insert_side_effect_statements_with_backend(
    backend: &dyn LixBackend,
    insert: &Insert,
    params: &[EngineValue],
    active_version_id_hint: Option<&str>,
) -> Result<FilesystemInsertSideEffects, LixError> {
    mutation_rewrite::insert_side_effect_statements_with_backend(
        backend,
        insert,
        params,
        active_version_id_hint,
    )
    .await
}

pub fn rewrite_update(update: Update) -> Result<Option<FilesystemUpdateRewrite>, LixError> {
    mutation_rewrite::rewrite_update(update)
}

pub async fn rewrite_update_with_backend(
    backend: &dyn LixBackend,
    update: Update,
    params: &[EngineValue],
    active_version_id_hint: Option<&str>,
) -> Result<Option<FilesystemUpdateRewrite>, LixError> {
    mutation_rewrite::rewrite_update_with_backend(backend, update, params, active_version_id_hint)
        .await
}

pub fn rewrite_delete(delete: Delete) -> Result<Option<Delete>, LixError> {
    mutation_rewrite::rewrite_delete(delete)
}

pub async fn rewrite_delete_with_backend(
    backend: &dyn LixBackend,
    delete: Delete,
    params: &[EngineValue],
    active_version_id_hint: Option<&str>,
) -> Result<Option<Delete>, LixError> {
    mutation_rewrite::rewrite_delete_with_backend(backend, delete, params, active_version_id_hint)
        .await
}
