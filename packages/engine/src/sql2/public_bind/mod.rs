mod assignment;
mod capability;
mod dml;
mod table;

use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::parser::Statement as DataFusionStatement;
use serde_json::Value as JsonValue;

use crate::LixError;

pub(crate) use dml::DmlOperation;

pub(crate) fn validate_public_dml_statement(
    statement: &DataFusionStatement,
    visible_schemas: &[JsonValue],
) -> Result<(), LixError> {
    dml::validate_datafusion_statement(statement, visible_schemas)
}

pub(crate) fn validate_public_dml_plan(
    plan: &LogicalPlan,
    visible_schemas: &[JsonValue],
) -> Result<(), LixError> {
    dml::validate_plan(plan, visible_schemas)
}
