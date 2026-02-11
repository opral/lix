use sqlparser::ast::{Delete, Insert, Query, Statement, Update};

use crate::filesystem::mutation_rewrite::FilesystemInsertSideEffects;
use crate::filesystem::{mutation_rewrite, select_rewrite};
use crate::{LixBackend, LixError, Value as EngineValue};

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    select_rewrite::rewrite_query(query)
}

pub fn rewrite_insert(insert: Insert) -> Result<Option<Insert>, LixError> {
    mutation_rewrite::rewrite_insert(insert)
}

pub async fn rewrite_insert_with_backend(
    backend: &dyn LixBackend,
    insert: Insert,
    params: &[EngineValue],
) -> Result<Option<Insert>, LixError> {
    mutation_rewrite::rewrite_insert_with_backend(backend, insert, params).await
}

pub async fn insert_side_effect_statements_with_backend(
    backend: &dyn LixBackend,
    insert: &Insert,
    params: &[EngineValue],
) -> Result<FilesystemInsertSideEffects, LixError> {
    mutation_rewrite::insert_side_effect_statements_with_backend(backend, insert, params).await
}

pub fn rewrite_update(update: Update) -> Result<Option<Statement>, LixError> {
    mutation_rewrite::rewrite_update(update)
}

pub async fn rewrite_update_with_backend(
    backend: &dyn LixBackend,
    update: Update,
    params: &[EngineValue],
) -> Result<Option<Statement>, LixError> {
    mutation_rewrite::rewrite_update_with_backend(backend, update, params).await
}

pub fn rewrite_delete(delete: Delete) -> Result<Option<Delete>, LixError> {
    mutation_rewrite::rewrite_delete(delete)
}

pub async fn rewrite_delete_with_backend(
    backend: &dyn LixBackend,
    delete: Delete,
    params: &[EngineValue],
) -> Result<Option<Delete>, LixError> {
    mutation_rewrite::rewrite_delete_with_backend(backend, delete, params).await
}
