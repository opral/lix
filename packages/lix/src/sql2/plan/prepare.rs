//! Admission for SQL dependency preparation; execution stays with session.
use crate::LixError;
use datafusion::sql::parser::Statement as DataFusionStatement;
use datafusion::sql::sqlparser::ast::{
    AssignmentTarget, Expr, Statement, TableFactor, Visit, Visitor,
};
use std::ops::ControlFlow;

fn unsupported() -> LixError {
    LixError::new(
        "LIX_SQL_PREPARATION_UNSUPPORTED",
        "prepare supports function-free SELECT and deterministic UPDATE of lix_key_value.value or lix_file.content; execute other SQL normally",
    )
}
pub(crate) fn validate_sql_preparation(sql: &str) -> Result<bool, LixError> {
    let DataFusionStatement::Statement(statement) = crate::sql2::parse_statement(sql)? else {
        return Err(unsupported());
    };
    struct Deterministic;
    impl Visitor for Deterministic {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            match expr {
                Expr::Identifier(_)
                | Expr::CompoundIdentifier(_)
                | Expr::Value(_)
                | Expr::BinaryOp { .. }
                | Expr::UnaryOp { .. }
                | Expr::Nested(_)
                | Expr::Cast { .. }
                | Expr::IsNull(_)
                | Expr::IsNotNull(_)
                | Expr::InList { .. }
                | Expr::Between { .. } => ControlFlow::Continue(()),
                _ => ControlFlow::Break(()),
            }
        }
        fn pre_visit_table_factor(&mut self, table: &TableFactor) -> ControlFlow<()> {
            if matches!(table, TableFactor::Table { args: None, .. }) {
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break(())
            }
        }
    }
    if statement.visit(&mut Deterministic).is_break() {
        return Err(unsupported());
    }
    match statement.as_ref() {
        Statement::Query(query) => {
            let datafusion::sql::sqlparser::ast::SetExpr::Select(select) = query.body.as_ref()
            else {
                return Err(unsupported());
            };
            if query.with.is_some() || select.into.is_some() || !query.locks.is_empty() {
                return Err(unsupported());
            }
            Ok(false)
        }
        Statement::Update(update) => {
            if update.from.is_some()
                || update.returning.is_some()
                || update.limit.is_some()
                || update.or.is_some()
                || !update.table.joins.is_empty()
                || update.selection.is_none()
            {
                return Err(unsupported());
            }
            let TableFactor::Table {
                name, args: None, ..
            } = &update.table.relation
            else {
                return Err(unsupported());
            };
            let field = match name.to_string().as_str() {
                "lix_key_value" => "value",
                "lix_file" => "content",
                _ => return Err(unsupported()),
            };
            if update.assignments.len() != 1
                || !matches!(&update.assignments[0].target, AssignmentTarget::ColumnName(name) if name.to_string()==field)
            {
                return Err(unsupported());
            }
            Ok(true)
        }
        _ => Err(unsupported()),
    }
}
