use sqlparser::ast::Query;

use crate::sql::steps::lix_state_by_version_view_read;
use crate::LixError;

pub(crate) fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    lix_state_by_version_view_read::rewrite_query(query)
}
