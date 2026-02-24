use sqlparser::ast::{Delete, Insert, Update};

use crate::engine::sql::planning::rewrite_engine::steps::lix_state_by_version_view_write;
use crate::LixError;

pub(crate) fn rewrite_insert(insert: Insert) -> Result<Option<Insert>, LixError> {
    lix_state_by_version_view_write::rewrite_insert(insert)
}

pub(crate) fn rewrite_update(update: Update) -> Result<Option<Update>, LixError> {
    lix_state_by_version_view_write::rewrite_update(update)
}

pub(crate) fn rewrite_delete(delete: Delete) -> Result<Option<Delete>, LixError> {
    lix_state_by_version_view_write::rewrite_delete(delete)
}
