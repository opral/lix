use datafusion::logical_expr::{LogicalPlan, WriteOp};
use datafusion::sql::parser::Statement as DataFusionStatement;
use datafusion::sql::sqlparser::ast::{
    Assignment, AssignmentTarget, Delete, FromTable, ObjectName, Statement, TableFactor,
    TableObject, TableWithJoins, Update,
};
use serde_json::Value as JsonValue;

use crate::LixError;

use super::assignment::validate_update_assignments;
use super::capability::validate_table_operation;
use super::table::{PublicSurface, PublicTableContracts};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DmlOperation {
    Insert,
    Update,
    Delete,
}

impl DmlOperation {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

pub(crate) fn validate_datafusion_statement(
    statement: &DataFusionStatement,
    visible_schemas: &[JsonValue],
) -> Result<(), LixError> {
    let contracts = PublicTableContracts::new(visible_schemas)?;
    validate_datafusion_statement_with_contracts(statement, &contracts)
}

fn validate_datafusion_statement_with_contracts(
    statement: &DataFusionStatement,
    contracts: &PublicTableContracts,
) -> Result<(), LixError> {
    match statement {
        DataFusionStatement::Statement(statement) => validate_statement(statement, contracts),
        DataFusionStatement::Explain(explain) => {
            validate_datafusion_statement_with_contracts(explain.statement.as_ref(), contracts)
        }
        _ => Ok(()),
    }
}

pub(crate) fn validate_plan(
    plan: &LogicalPlan,
    visible_schemas: &[JsonValue],
) -> Result<(), LixError> {
    let contracts = PublicTableContracts::new(visible_schemas)?;
    validate_plan_with_contracts(plan, &contracts)
}

fn validate_plan_with_contracts(
    plan: &LogicalPlan,
    contracts: &PublicTableContracts,
) -> Result<(), LixError> {
    if let LogicalPlan::Dml(dml) = plan {
        let surface = PublicSurface::named(dml.table_name.table());
        validate_table_operation(&surface, operation_from_write_op(&dml.op), contracts)?;
    }
    for input in plan.inputs() {
        validate_plan_with_contracts(input, contracts)?;
    }
    Ok(())
}

fn operation_from_write_op(op: &WriteOp) -> DmlOperation {
    match op {
        WriteOp::Insert(_) | WriteOp::Ctas => DmlOperation::Insert,
        WriteOp::Update => DmlOperation::Update,
        WriteOp::Delete | WriteOp::Truncate => DmlOperation::Delete,
    }
}

fn validate_statement(
    statement: &Statement,
    contracts: &PublicTableContracts,
) -> Result<(), LixError> {
    match statement {
        Statement::Insert(insert) => {
            let Some(table_name) = insert_target_name(&insert.table) else {
                return Ok(());
            };
            let surface = PublicSurface::named(table_name);
            validate_table_operation(&surface, DmlOperation::Insert, contracts)
        }
        Statement::Update(update) => validate_update(update, contracts),
        Statement::Delete(delete) => validate_delete(delete, contracts),
        Statement::Explain { statement, .. } => validate_statement(statement, contracts),
        _ => Ok(()),
    }
}

fn validate_update(update: &Update, contracts: &PublicTableContracts) -> Result<(), LixError> {
    let Some(table_name) = table_with_joins_target_name(&update.table) else {
        return Ok(());
    };
    let surface = PublicSurface::named(table_name);
    validate_table_operation(&surface, DmlOperation::Update, contracts)?;
    validate_update_assignments(
        &surface,
        assignment_column_names(&update.assignments)?,
        contracts,
    )
}

fn validate_delete(delete: &Delete, contracts: &PublicTableContracts) -> Result<(), LixError> {
    for table in delete_from_tables(delete) {
        let Some(table_name) = table_with_joins_target_name(table) else {
            continue;
        };
        let surface = PublicSurface::named(table_name);
        validate_table_operation(&surface, DmlOperation::Delete, contracts)?;
    }
    Ok(())
}

fn delete_from_tables(delete: &Delete) -> &[TableWithJoins] {
    match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    }
}

fn assignment_column_names(assignments: &[Assignment]) -> Result<Vec<String>, LixError> {
    let mut columns = Vec::new();
    for assignment in assignments {
        match &assignment.target {
            AssignmentTarget::ColumnName(name) => {
                if let Some(column) = object_name_leaf(name) {
                    columns.push(column);
                }
            }
            AssignmentTarget::Tuple(names) => {
                for name in names {
                    if let Some(column) = object_name_leaf(name) {
                        columns.push(column);
                    }
                }
            }
        }
    }
    Ok(columns)
}

fn insert_target_name(table: &TableObject) -> Option<String> {
    match table {
        TableObject::TableName(name) => object_name_leaf(name),
        _ => None,
    }
}

fn table_with_joins_target_name(table: &TableWithJoins) -> Option<String> {
    match &table.relation {
        TableFactor::Table { name, .. } => object_name_leaf(name),
        _ => None,
    }
}

fn object_name_leaf(name: &ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.to_ascii_lowercase())
}
