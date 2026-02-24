use sqlparser::ast::Query;

use crate::engine::sql::planning::rewrite_engine::steps::{
    lix_state_by_version_view_read, lix_state_view_read,
};
use crate::LixError;

pub(crate) fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    let mut current = query;
    let mut changed = false;

    if let Some(rewritten) = lix_state_by_version_view_read::rewrite_query(current.clone())? {
        current = rewritten;
        changed = true;
    }

    if let Some(rewritten) = lix_state_view_read::rewrite_query(current.clone())? {
        current = rewritten;
        changed = true;
    }

    if changed {
        Ok(Some(current))
    } else {
        Ok(None)
    }
}
