use datafusion::sql::parser::Statement as DataFusionStatement;
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, SelectFlavor, SelectItem, SetExpr,
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
    Recovery {
        command: RecoveryCommand,
        commits_query: String,
        selection: Box<Self>,
    },
}

/// Public recovery command selected by the top-level positional SQL function.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryCommand {
    Restore,
    Revert,
    RevertRange,
    Apply,
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
        alias,
        args: Some(arguments),
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
    } = &from.relation
    else {
        return Ok(None);
    };
    let function_name = [
        "lix_create_checkpoint",
        "lix_restore",
        "lix_revert",
        "lix_revert_range",
        "lix_apply",
    ]
    .into_iter()
    .find(|candidate| crate::sql2::parse::object_name_is_public_function(name, candidate));
    let Some(function_name) = function_name else {
        return Ok(None);
    };

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
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
        || !from.joins.is_empty()
        || alias.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
        || !projection_is_commit_id
        || select.flavor != SelectFlavor::Standard
        || select.optimizer_hint.is_some()
        || select.distinct.is_some()
        || select.select_modifiers.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || select.selection.is_some()
        || !select.connect_by.is_empty()
        || !matches!(select.group_by, datafusion::sql::sqlparser::ast::GroupByExpr::Expressions(ref expressions, ref modifiers) if expressions.is_empty() && modifiers.is_empty())
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.window_before_qualify
        || select.value_table_mode.is_some()
        || arguments.settings.is_some()
    {
        return Err(invalid_function_call(function_name));
    }

    if function_name == "lix_create_checkpoint" {
        return checkpoint_arguments_plan(arguments.args.as_slice());
    }
    recovery_function_plan(function_name, arguments.args.as_slice())
}

fn checkpoint_arguments_plan(
    arguments: &[FunctionArg],
) -> Result<Option<CheckpointFunctionPlan>, LixError> {
    match arguments {
        [] => Ok(Some(CheckpointFunctionPlan::Full)),
        [argument] => Ok(Some(selection_plan(argument)?)),
        _ => Err(invalid_function_call("lix_create_checkpoint")),
    }
}

fn recovery_function_plan(
    function_name: &str,
    arguments: &[FunctionArg],
) -> Result<Option<CheckpointFunctionPlan>, LixError> {
    let command = match function_name {
        "lix_restore" => RecoveryCommand::Restore,
        "lix_revert" => RecoveryCommand::Revert,
        "lix_revert_range" => RecoveryCommand::RevertRange,
        "lix_apply" => RecoveryCommand::Apply,
        _ => return Ok(None),
    };
    let endpoint_count = match command {
        RecoveryCommand::Restore | RecoveryCommand::Revert => 1,
        RecoveryCommand::RevertRange | RecoveryCommand::Apply => 2,
    };
    if arguments.len() != endpoint_count && arguments.len() != endpoint_count + 1 {
        return Err(invalid_function_call(function_name));
    }
    let endpoint_expressions = arguments[..endpoint_count]
        .iter()
        .map(endpoint_expression)
        .collect::<Result<Vec<_>, _>>()?;
    let commits_query = match endpoint_expressions.as_slice() {
        [source] => format!("SELECT {source} AS commit_id"),
        [before, after] => {
            format!("SELECT {before} AS before_commit_id, {after} AS after_commit_id")
        }
        _ => unreachable!("recovery endpoint arity is one or two"),
    };
    let selection = arguments
        .get(endpoint_count)
        .map(selection_plan)
        .transpose()?
        .unwrap_or(CheckpointFunctionPlan::Full);
    Ok(Some(CheckpointFunctionPlan::Recovery {
        command,
        commits_query,
        selection: Box::new(selection),
    }))
}

fn endpoint_expression(argument: &FunctionArg) -> Result<String, LixError> {
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) = argument else {
        return Err(invalid_function_call("lix recovery function"));
    };
    Ok(expression.to_string())
}

fn selection_plan(argument: &FunctionArg) -> Result<CheckpointFunctionPlan, LixError> {
    match argument {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Array(array))) if array.named => {
            if array.elem.is_empty() {
                return Ok(CheckpointFunctionPlan::Empty);
            }
            let selection_query = array
                .elem
                .iter()
                .map(|expression| format!("SELECT {expression} AS row_ref"))
                .collect::<Vec<_>>()
                .join(" UNION ALL ");
            Ok(CheckpointFunctionPlan::SelectionQuery(selection_query))
        }
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(function)))
            if crate::sql2::parse::object_name_is_public_function(&function.name, "array")
                && matches!(function.parameters, FunctionArguments::None)
                && function.filter.is_none()
                && function.null_treatment.is_none()
                && function.over.is_none()
                && function.within_group.is_empty() =>
        {
            let FunctionArguments::Subquery(selection) = &function.args else {
                return Err(invalid_function_call("lix recovery function"));
            };
            Ok(CheckpointFunctionPlan::SelectionQuery(
                selection.to_string(),
            ))
        }
        _ => Err(invalid_function_call("lix recovery function")),
    }
}

fn invalid_function_call(function_name: &str) -> LixError {
    LixError::new(
        LixError::CODE_UNSUPPORTED_SQL,
        format!(
            "{function_name} must be called as a top-level SELECT commit_id function with positional commit and optional ARRAY row references"
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::{CheckpointFunctionPlan, RecoveryCommand, checkpoint_function_plan};

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
            plan("SELECT commit_id FROM lix_create_checkpoint(ARRAY[lix_row_ref('lix_file', $1)])")
                .unwrap(),
            Some(CheckpointFunctionPlan::SelectionQuery(_))
        ));
        assert!(matches!(
            plan("SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_file')))").unwrap(),
            Some(CheckpointFunctionPlan::SelectionQuery(_))
        ));
        assert!(plan("SELECT * FROM lix_create_checkpoint()").is_err());
        assert!(plan("SELECT commit_id FROM lix_create_checkpoint() WHERE true").is_err());
        assert!(matches!(
            plan("SELECT commit_id FROM lix_restore($1)").unwrap(),
            Some(CheckpointFunctionPlan::Recovery {
                command: RecoveryCommand::Restore,
                commits_query,
                selection,
            }) if commits_query == "SELECT $1 AS commit_id"
                && *selection == CheckpointFunctionPlan::Full
        ));
        assert!(matches!(
            plan("SELECT commit_id FROM lix_revert_range($1, $2, ARRAY[lix_row_ref('lix_file', $3)])").unwrap(),
            Some(CheckpointFunctionPlan::Recovery {
                command: RecoveryCommand::RevertRange,
                commits_query,
                selection,
            }) if commits_query == "SELECT $1 AS before_commit_id, $2 AS after_commit_id"
                && matches!(*selection, CheckpointFunctionPlan::SelectionQuery(_))
        ));
        assert!(matches!(
            plan("SELECT commit_id FROM lix_apply($1, $2, ARRAY(SELECT row_ref FROM lix_diff('lix_file', $2, $1)))").unwrap(),
            Some(CheckpointFunctionPlan::Recovery {
                command: RecoveryCommand::Apply,
                commits_query,
                selection,
            }) if commits_query == "SELECT $1 AS before_commit_id, $2 AS after_commit_id"
                && matches!(*selection, CheckpointFunctionPlan::SelectionQuery(_))
        ));
        for sql in [
            "SELECT commit_id FROM lix_restore($1, $2)",
            "SELECT commit_id FROM lix_revert_range($1)",
            "SELECT commit_id FROM lix_apply($1, $2, $3, $4)",
            "SELECT commit_id FROM lix_revert($1, NULL)",
        ] {
            assert!(plan(sql).is_err(), "invalid recovery syntax: {sql}");
        }
        for sql in [
            "SELECT commit_id FROM lix_restore($1) AS restore",
            "SELECT commit_id FROM lix_restore($1) WITH ORDINALITY",
            "SELECT commit_id FROM lix_restore($1) SETTINGS setting = true",
            "SELECT commit_id FROM lix_restore($1) PREWHERE true",
            "SELECT commit_id FROM lix_restore($1) WINDOW window_name AS (ORDER BY commit_id)",
            "SELECT commit_id FROM lix_restore($1) SORT BY commit_id",
        ] {
            assert!(plan(sql).is_err(), "invalid recovery modifiers: {sql}");
        }
        assert!(plan("SELECT * FROM lix_file").unwrap().is_none());
    }
}
