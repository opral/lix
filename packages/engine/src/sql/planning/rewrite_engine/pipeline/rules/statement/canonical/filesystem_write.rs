use sqlparser::ast::{Delete, Insert, Update};

use crate::engine::sql::planning::rewrite_engine::steps::filesystem_step;
use crate::filesystem::mutation_rewrite::{FilesystemUpdateRewrite, ResolvedDirectoryIdMap};
use crate::LixBackend;
use crate::{LixError, Value};

pub(crate) fn rewrite_insert(insert: Insert) -> Result<Option<Insert>, LixError> {
    filesystem_step::rewrite_insert(insert)
}

pub(crate) async fn insert_side_effects_with_backend(
    backend: &dyn LixBackend,
    insert: &Insert,
    params: &[Value],
    active_version_id_hint: Option<&str>,
) -> Result<crate::filesystem::mutation_rewrite::FilesystemInsertSideEffects, LixError> {
    filesystem_step::insert_side_effect_statements_with_backend(
        backend,
        insert,
        params,
        active_version_id_hint,
    )
    .await
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

pub(crate) fn rewrite_update(update: Update) -> Result<Option<FilesystemUpdateRewrite>, LixError> {
    filesystem_step::rewrite_update(update)
}

pub(crate) async fn rewrite_update_with_backend(
    backend: &dyn LixBackend,
    update: Update,
    params: &[Value],
    active_version_id_hint: Option<&str>,
) -> Result<Option<FilesystemUpdateRewrite>, LixError> {
    filesystem_step::rewrite_update_with_backend(backend, update, params, active_version_id_hint)
        .await
}

pub(crate) fn rewrite_delete(delete: Delete) -> Result<Option<Delete>, LixError> {
    filesystem_step::rewrite_delete(delete)
}

pub(crate) async fn rewrite_delete_with_backend(
    backend: &dyn LixBackend,
    delete: Delete,
    params: &[Value],
    active_version_id_hint: Option<&str>,
) -> Result<Option<Delete>, LixError> {
    filesystem_step::rewrite_delete_with_backend(backend, delete, params, active_version_id_hint)
        .await
}
