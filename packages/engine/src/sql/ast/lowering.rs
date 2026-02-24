#[path = "lower_json_fn.rs"]
mod lower_json_fn;
#[path = "lower_logical_fn.rs"]
mod lower_logical_fn;

use std::ops::ControlFlow;

use sqlparser::ast::{Expr, Statement};
use sqlparser::ast::{VisitMut, VisitorMut};

use crate::backend::SqlDialect;
use crate::LixError;

use self::lower_json_fn::{lower_lix_empty_blob, lower_lix_json_text};
use self::lower_logical_fn::{parse_lix_empty_blob, parse_lix_json_text};

pub(crate) fn lower_statement(
    statement: Statement,
    dialect: SqlDialect,
) -> Result<Statement, LixError> {
    let mut statement = statement;
    let mut lowerer = LogicalFunctionLowerer { dialect };
    if let ControlFlow::Break(error) = statement.visit(&mut lowerer) {
        return Err(error);
    }
    Ok(statement)
}

struct LogicalFunctionLowerer {
    dialect: SqlDialect,
}

impl VisitorMut for LogicalFunctionLowerer {
    type Break = LixError;

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        let Expr::Function(function) = expr else {
            return ControlFlow::Continue(());
        };

        let parsed = match parse_lix_json_text(function) {
            Ok(parsed) => parsed,
            Err(error) => return ControlFlow::Break(error),
        };

        let Some(call) = parsed else {
            let parsed_empty_blob = match parse_lix_empty_blob(function) {
                Ok(parsed) => parsed,
                Err(error) => return ControlFlow::Break(error),
            };
            if parsed_empty_blob.is_some() {
                *expr = lower_lix_empty_blob(self.dialect);
            }
            return ControlFlow::Continue(());
        };

        let lowered = lower_lix_json_text(&call, self.dialect);
        *expr = lowered;
        ControlFlow::Continue(())
    }
}
