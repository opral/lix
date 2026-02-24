use sqlparser::ast::Query;

use crate::engine::sql2::planning::rewrite_engine::steps::filesystem_step;
use crate::{LixError, Value};

pub(crate) fn rewrite_query(query: Query, params: &[Value]) -> Result<Option<Query>, LixError> {
    if params.is_empty() {
        filesystem_step::rewrite_query(query)
    } else {
        filesystem_step::rewrite_query_with_params(query, params)
    }
}
