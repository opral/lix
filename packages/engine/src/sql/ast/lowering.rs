use std::ops::ControlFlow;

use sqlparser::ast::{Expr, Statement};
use sqlparser::ast::{VisitMut, VisitorMut};

use crate::LixError;
use crate::SqlDialect;

use super::lower_json_fn::{
    lower_lix_empty_blob, lower_lix_json, lower_lix_json_extract, lower_lix_json_extract_boolean,
    lower_lix_json_extract_json, lower_lix_text_decode, lower_lix_text_encode,
};
use super::lower_logical_fn::{
    parse_lix_empty_blob, parse_lix_json, parse_lix_json_extract, parse_lix_json_extract_boolean,
    parse_lix_json_extract_json, parse_lix_text_decode, parse_lix_text_encode,
};

pub(crate) fn lower_statement(
    statement: Statement,
    dialect: SqlDialect,
) -> Result<Statement, LixError> {
    let mut statement = statement;
    apply_logical_function_lowering(&mut statement, dialect)?;
    Ok(statement)
}

fn apply_logical_function_lowering<T>(node: &mut T, dialect: SqlDialect) -> Result<(), LixError>
where
    T: VisitMut,
{
    let mut lowerer = LogicalFunctionLowerer { dialect };
    if let ControlFlow::Break(error) = node.visit(&mut lowerer) {
        return Err(error);
    }
    Ok(())
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

        let parsed = match parse_lix_json_extract(function) {
            Ok(parsed) => parsed,
            Err(error) => return ControlFlow::Break(error),
        };

        let Some(call) = parsed else {
            let parsed_boolean = match parse_lix_json_extract_boolean(function) {
                Ok(parsed) => parsed,
                Err(error) => return ControlFlow::Break(error),
            };
            if let Some(call) = parsed_boolean {
                *expr = lower_lix_json_extract_boolean(&call, self.dialect);
                return ControlFlow::Continue(());
            }
            let parsed_json_value = match parse_lix_json_extract_json(function) {
                Ok(parsed) => parsed,
                Err(error) => return ControlFlow::Break(error),
            };
            if let Some(call) = parsed_json_value {
                *expr = lower_lix_json_extract_json(&call, self.dialect);
                return ControlFlow::Continue(());
            }
            let parsed_json = match parse_lix_json(function) {
                Ok(parsed) => parsed,
                Err(error) => return ControlFlow::Break(error),
            };
            if let Some(call) = parsed_json {
                *expr = lower_lix_json(&call, self.dialect);
                return ControlFlow::Continue(());
            }
            let parsed_empty_blob = match parse_lix_empty_blob(function) {
                Ok(parsed) => parsed,
                Err(error) => return ControlFlow::Break(error),
            };
            if parsed_empty_blob.is_some() {
                *expr = lower_lix_empty_blob(self.dialect);
                return ControlFlow::Continue(());
            }
            let parsed_text_encode = match parse_lix_text_encode(function) {
                Ok(parsed) => parsed,
                Err(error) => return ControlFlow::Break(error),
            };
            if let Some(call) = parsed_text_encode {
                *expr = lower_lix_text_encode(&call, self.dialect);
                return ControlFlow::Continue(());
            }
            let parsed_text_decode = match parse_lix_text_decode(function) {
                Ok(parsed) => parsed,
                Err(error) => return ControlFlow::Break(error),
            };
            if let Some(call) = parsed_text_decode {
                *expr = lower_lix_text_decode(&call, self.dialect);
                return ControlFlow::Continue(());
            }
            return ControlFlow::Continue(());
        };

        let lowered = lower_lix_json_extract(&call, self.dialect);
        *expr = lowered;
        ControlFlow::Continue(())
    }
}
