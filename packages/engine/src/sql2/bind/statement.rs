use datafusion::sql::parser::Statement as DataFusionStatement;
use serde_json::Value as JsonValue;

use crate::LixError;

use super::read::BoundRead;
use super::write::BoundWrite;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BoundStatement {
    Read(BoundRead),
    Write(BoundWrite),
}

pub(crate) fn bind_statement(
    _statement: &DataFusionStatement,
    _visible_schemas: &[JsonValue],
    _active_version_id: &str,
) -> Result<BoundStatement, LixError> {
    Err(super::error::unsupported(
        "sql2 bound statement pipeline is not wired yet",
    ))
}
