use datafusion::sql::parser::Statement as DataFusionStatement;
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, SelectItem, SetExpr,
    Statement as SqlStatement, TableFactor,
};

use crate::LixError;

/// A deliberately narrow mutating table-function statement.
///
/// It is recognized before ordinary read planning. Treating checkpointing as
/// a DataFusion UDF would let an optimizer evaluate it more than once and
/// would give a read provider write authority. This plan is instead executed
/// exactly once by Lix's transaction coordinator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CheckpointFunctionPlan {
    Full,
    Empty,
    SelectionQuery(String),
}

pub(crate) fn checkpoint_function_plan(
    statement: &DataFusionStatement,
) -> Result<Option<CheckpointFunctionPlan>, LixError> {
    let DataFusionStatement::Statement(statement) = statement else {
        return Ok(None);
    };
    let SqlStatement::Query(query) = statement.as_ref() else {
        return Ok(None);
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    let [from] = select.from.as_slice() else {
        return Ok(None);
    };
    let TableFactor::Table {
        name,
        args: Some(arguments),
        ..
    } = &from.relation
    else {
        return Ok(None);
    };
    if !crate::sql2::parse::object_name_is_public_function(name, "lix_create_checkpoint") {
        return Ok(None);
    }

    // Once the target is recognized, reject every compositional surface. A
    // checkpoint is one mutation yielding one receipt row, not a relation an
    // optimizer may join, filter, aggregate, or invoke repeatedly.
    let projection_is_commit_id = matches!(
        select.projection.as_slice(),
        [SelectItem::UnnamedExpr(Expr::Identifier(identifier))]
            if identifier.quote_style.as_ref().map_or_else(
                || identifier.value.eq_ignore_ascii_case("commit_id"),
                |_| identifier.value == "commit_id",
            )
    );
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || !query.pipe_operators.is_empty()
        || !from.joins.is_empty()
        || !projection_is_commit_id
        || select.distinct.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || select.selection.is_some()
        || !matches!(select.group_by, datafusion::sql::sqlparser::ast::GroupByExpr::Expressions(ref expressions, ref modifiers) if expressions.is_empty() && modifiers.is_empty())
        || select.having.is_some()
        || select.qualify.is_some()
        || arguments.settings.is_some()
    {
        return Err(invalid_checkpoint_function_call());
    }

    match arguments.args.as_slice() {
        [] => Ok(Some(CheckpointFunctionPlan::Full)),
        [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Array(array)))] if array.named => {
            if array.elem.is_empty() {
                return Ok(Some(CheckpointFunctionPlan::Empty));
            }
            let selection_query = array
                .elem
                .iter()
                .map(|expression| format!("SELECT {expression} AS row_ref"))
                .collect::<Vec<_>>()
                .join(" UNION ALL ");
            Ok(Some(CheckpointFunctionPlan::SelectionQuery(selection_query)))
        }
        [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(function)))]
            if crate::sql2::parse::object_name_is_public_function(&function.name, "array")
                && matches!(function.parameters, FunctionArguments::None)
                && function.filter.is_none()
                && function.null_treatment.is_none()
                && function.over.is_none()
                && function.within_group.is_empty() =>
        {
            let FunctionArguments::Subquery(selection) = &function.args else {
                return Err(invalid_checkpoint_function_call());
            };
            Ok(Some(CheckpointFunctionPlan::SelectionQuery(
                selection.to_string(),
            )))
        }
        _ => Err(invalid_checkpoint_function_call()),
    }
}

fn invalid_checkpoint_function_call() -> LixError {
    LixError::new(
        LixError::CODE_UNSUPPORTED_SQL,
        "lix_create_checkpoint must be called exactly as SELECT commit_id FROM lix_create_checkpoint() or with one ARRAY of row references",
    )
}

#[cfg(test)]
mod tests {
    use super::{CheckpointFunctionPlan, checkpoint_function_plan};

    fn plan(sql: &str) -> Result<Option<CheckpointFunctionPlan>, crate::LixError> {
        checkpoint_function_plan(&crate::sql2::parse_statement(sql)?)
    }

    #[test]
    fn recognizes_only_single_invocation_checkpoint_statements() {
        assert_eq!(
            plan("SELECT commit_id FROM lix_create_checkpoint()").unwrap(),
            Some(CheckpointFunctionPlan::Full)
        );
        assert_eq!(
            plan("SELECT commit_id FROM lix_create_checkpoint(ARRAY[])").unwrap(),
            Some(CheckpointFunctionPlan::Empty)
        );
        assert!(matches!(
            plan("SELECT commit_id FROM lix_create_checkpoint(ARRAY[lix_row_ref('lix_file', $1)])").unwrap(),
            Some(CheckpointFunctionPlan::SelectionQuery(_))
        ));
        assert!(matches!(
            plan("SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_file')))").unwrap(),
            Some(CheckpointFunctionPlan::SelectionQuery(_))
        ));
        assert!(plan("SELECT * FROM lix_create_checkpoint()").is_err());
        assert!(plan("SELECT commit_id FROM lix_create_checkpoint() WHERE true").is_err());
        assert!(plan("SELECT * FROM lix_file").unwrap().is_none());
    }
}
