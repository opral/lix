use sqlparser::ast::Insert;

use crate::engine::sql::planning::rewrite_engine::steps::stored_schema;
use crate::LixError;
use crate::Value;

pub(crate) fn rewrite_insert(
    insert: Insert,
    params: &[Value],
) -> Result<Option<stored_schema::StoredSchemaRewrite>, LixError> {
    stored_schema::rewrite_insert(insert, params)
}
