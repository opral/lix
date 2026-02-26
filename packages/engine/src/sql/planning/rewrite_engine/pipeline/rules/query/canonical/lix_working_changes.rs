use sqlparser::ast::Query;

use crate::engine::sql::planning::rewrite_engine::steps::lix_working_changes_view_read;
use crate::LixError;

pub(crate) fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    lix_working_changes_view_read::rewrite_query(query)
}
