use super::*;
use crate::schema::builtin_schema_definition;
use crate::sql::logical_plan::public_ir::{
    BroadPublicReadAlias, BroadPublicReadDistinct, BroadPublicReadGroupBy,
    BroadPublicReadGroupByKind, BroadPublicReadJoin, BroadPublicReadJoinConstraint,
    BroadPublicReadJoinKind, BroadPublicReadLimitClause, BroadPublicReadLimitClauseKind,
    BroadPublicReadOffset, BroadPublicReadOrderBy, BroadPublicReadOrderByExpr,
    BroadPublicReadOrderByKind, BroadPublicReadProjectionItem, BroadPublicReadProjectionItemKind,
    BroadPublicReadQuery, BroadPublicReadRelation, BroadPublicReadSelect, BroadPublicReadSetExpr,
    BroadPublicReadSetOperationKind, BroadPublicReadSetQuantifier, BroadPublicReadStatement,
    BroadPublicReadTableFactor, BroadPublicReadTableWithJoins, BroadPublicReadWith,
    BroadSqlCaseWhen, BroadSqlExpr, BroadSqlExprKind, BroadSqlFunction, BroadSqlFunctionArg,
    BroadSqlFunctionArgExpr, BroadSqlFunctionArguments,
};
use crate::sql::physical_plan::source_sql::build_working_changes_public_read_source_sql;
use serde_json::Value as JsonValue;
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    Cte, Distinct, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, GroupByExpr, JoinConstraint, JoinOperator, LimitClause, Offset, OrderBy,
    OrderByExpr, OrderByKind, OrderByOptions, Query, Select, SelectFlavor, SelectItem,
    SelectItemQualifiedWildcardKind, SetExpr, SetOperator, SetQuantifier, Statement, Table,
    TableAlias, TableFactor, TableWithJoins, TypedString, With,
};
use std::cell::RefCell;

std::thread_local! {
    static BROAD_RENDER_SUBSTITUTION_STACK: RefCell<Vec<RenderRelationSubstitutionCollector>> =
        const { RefCell::new(Vec::new()) };
}

fn with_broad_render_substitution_collector<T>(
    work: impl FnOnce() -> Result<T, LixError>,
) -> Result<(T, Vec<TerminalRelationRenderNode>), LixError> {
    BROAD_RENDER_SUBSTITUTION_STACK.with(|stack| {
        stack
            .borrow_mut()
            .push(RenderRelationSubstitutionCollector::default());
    });

    let result = work();

    let substitutions = BROAD_RENDER_SUBSTITUTION_STACK.with(|stack| {
        stack
            .borrow_mut()
            .pop()
            .expect("broad render substitution collector stack should not underflow")
            .into_substitutions()
    });

    result.map(|value| (value, substitutions))
}

fn with_current_broad_render_substitution_collector<T>(
    work: impl FnOnce(&mut RenderRelationSubstitutionCollector) -> Result<T, LixError>,
) -> Result<T, LixError> {
    BROAD_RENDER_SUBSTITUTION_STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        let Some(collector) = stack.last_mut() else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "broad public-read lowering lost its terminal relation substitution collector",
            ));
        };
        work(collector)
    })
}

fn effective_state_version_scope(
    version_scope: VersionScope,
) -> crate::contracts::EffectiveStateVersionScope {
    match version_scope {
        VersionScope::ActiveVersion => crate::contracts::EffectiveStateVersionScope::ActiveVersion,
        VersionScope::ExplicitVersion => {
            crate::contracts::EffectiveStateVersionScope::ExplicitVersion
        }
        VersionScope::History => crate::contracts::EffectiveStateVersionScope::History,
    }
}

pub(crate) fn lower_broad_public_read_for_execution(
    statement: &BroadPublicReadStatement,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    params_len: usize,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Option<LoweredReadProgram>, LixError> {
    ensure_broad_public_read_statement_is_fully_typed(statement)?;

    if broad_public_read_statement_contains_public_relations(statement)
        || !broad_public_read_statement_contains_lowered_public_relations(statement)
    {
        return Ok(None);
    }

    let (lowered_statement, relation_render_nodes) =
        with_broad_render_substitution_collector(|| {
            lower_broad_public_read_statement(
                statement,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )
        })?;

    let compiled_statement = if relation_render_nodes.is_empty() {
        compile_final_read_statement(dialect, params_len, lowered_statement)?
    } else {
        compile_terminal_read_statement_from_template(
            dialect,
            params_len,
            lowered_statement,
            relation_render_nodes,
        )?
    };

    Ok(Some(LoweredReadProgram {
        statements: vec![compiled_statement],
        pushdown_decision: PushdownDecision::default(),
        result_columns: LoweredResultColumns::Static(Vec::new()),
    }))
}

pub(crate) fn broad_public_relation_supports_terminal_render(
    binding: &SurfaceBinding,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<bool, LixError> {
    build_supported_public_read_surface_sql(
        &binding.descriptor.public_name,
        registry,
        false,
        dialect,
        active_version_id,
        known_live_layouts,
    )
    .map(|sql| sql.is_some())
}

fn broad_public_read_statement_contains_public_relations(
    statement: &BroadPublicReadStatement,
) -> bool {
    broad_public_read_statement_contains_relation_kind(statement, |relation| {
        matches!(relation, BroadPublicReadRelation::Public(_))
    })
}

fn broad_public_read_statement_contains_lowered_public_relations(
    statement: &BroadPublicReadStatement,
) -> bool {
    broad_public_read_statement_contains_relation_kind(statement, |relation| {
        matches!(relation, BroadPublicReadRelation::LoweredPublic(_))
    })
}

fn ensure_broad_public_read_statement_is_fully_typed(
    statement: &BroadPublicReadStatement,
) -> Result<(), LixError> {
    match statement {
        BroadPublicReadStatement::Query(query) => {
            ensure_broad_public_read_query_is_fully_typed(query, "query")
        }
        BroadPublicReadStatement::Explain { statement, .. } => {
            ensure_broad_public_read_statement_is_fully_typed(statement)
        }
    }
}

fn ensure_broad_public_read_query_is_fully_typed(
    query: &BroadPublicReadQuery,
    path: &str,
) -> Result<(), LixError> {
    if let Some(with) = &query.with {
        for (index, cte) in with.cte_tables.iter().enumerate() {
            ensure_broad_public_read_query_is_fully_typed(
                &cte.query,
                &format!("{path}.with.cte[{index}]"),
            )?;
        }
    }
    ensure_broad_public_read_set_expr_is_fully_typed(&query.body, &format!("{path}.body"))?;
    if let Some(order_by) = &query.order_by {
        ensure_broad_public_read_order_by_is_fully_typed(order_by, &format!("{path}.order_by"))?;
    }
    if let Some(limit_clause) = &query.limit_clause {
        ensure_broad_public_read_limit_clause_is_fully_typed(
            limit_clause,
            &format!("{path}.limit_clause"),
        )?;
    }
    Ok(())
}

fn ensure_broad_public_read_set_expr_is_fully_typed(
    expr: &BroadPublicReadSetExpr,
    path: &str,
) -> Result<(), LixError> {
    match expr {
        BroadPublicReadSetExpr::Select(select) => {
            ensure_broad_public_read_select_is_fully_typed(select, path)
        }
        BroadPublicReadSetExpr::Query(query) => {
            ensure_broad_public_read_query_is_fully_typed(query, path)
        }
        BroadPublicReadSetExpr::SetOperation { left, right, .. } => {
            ensure_broad_public_read_set_expr_is_fully_typed(left, &format!("{path}.left"))?;
            ensure_broad_public_read_set_expr_is_fully_typed(right, &format!("{path}.right"))
        }
        BroadPublicReadSetExpr::Table { .. } => Ok(()),
    }
}

fn ensure_broad_public_read_select_is_fully_typed(
    select: &BroadPublicReadSelect,
    path: &str,
) -> Result<(), LixError> {
    if let Some(distinct) = &select.distinct {
        ensure_broad_public_read_distinct_is_fully_typed(distinct, &format!("{path}.distinct"))?;
    }
    for (index, projection) in select.projection.iter().enumerate() {
        ensure_broad_public_read_projection_item_is_fully_typed(
            projection,
            &format!("{path}.projection[{index}]"),
        )?;
    }
    for (index, table) in select.from.iter().enumerate() {
        ensure_broad_public_read_table_with_joins_is_fully_typed(
            table,
            &format!("{path}.from[{index}]"),
        )?;
    }
    if let Some(selection) = &select.selection {
        ensure_broad_sql_expr_is_fully_typed(selection, &format!("{path}.selection"))?;
    }
    ensure_broad_public_read_group_by_is_fully_typed(
        &select.group_by,
        &format!("{path}.group_by"),
    )?;
    if let Some(having) = &select.having {
        ensure_broad_sql_expr_is_fully_typed(having, &format!("{path}.having"))?;
    }
    Ok(())
}

fn ensure_broad_public_read_distinct_is_fully_typed(
    distinct: &BroadPublicReadDistinct,
    path: &str,
) -> Result<(), LixError> {
    if let BroadPublicReadDistinct::On(expressions) = distinct {
        for (index, expr) in expressions.iter().enumerate() {
            ensure_broad_sql_expr_is_fully_typed(expr, &format!("{path}.on[{index}]"))?;
        }
    }
    Ok(())
}

fn ensure_broad_public_read_table_with_joins_is_fully_typed(
    table: &BroadPublicReadTableWithJoins,
    path: &str,
) -> Result<(), LixError> {
    ensure_broad_public_read_table_factor_is_fully_typed(
        &table.relation,
        &format!("{path}.relation"),
    )?;
    for (index, join) in table.joins.iter().enumerate() {
        ensure_broad_public_read_join_is_fully_typed(join, &format!("{path}.joins[{index}]"))?;
    }
    Ok(())
}

fn ensure_broad_public_read_join_is_fully_typed(
    join: &BroadPublicReadJoin,
    path: &str,
) -> Result<(), LixError> {
    ensure_broad_public_read_table_factor_is_fully_typed(
        &join.relation,
        &format!("{path}.relation"),
    )?;
    ensure_broad_public_read_join_kind_is_fully_typed(&join.kind, &format!("{path}.kind"))
}

fn ensure_broad_public_read_join_kind_is_fully_typed(
    kind: &BroadPublicReadJoinKind,
    path: &str,
) -> Result<(), LixError> {
    match kind {
        BroadPublicReadJoinKind::Join(constraint)
        | BroadPublicReadJoinKind::Inner(constraint)
        | BroadPublicReadJoinKind::Left(constraint)
        | BroadPublicReadJoinKind::LeftOuter(constraint)
        | BroadPublicReadJoinKind::Right(constraint)
        | BroadPublicReadJoinKind::RightOuter(constraint)
        | BroadPublicReadJoinKind::FullOuter(constraint)
        | BroadPublicReadJoinKind::CrossJoin(constraint)
        | BroadPublicReadJoinKind::Semi(constraint)
        | BroadPublicReadJoinKind::LeftSemi(constraint)
        | BroadPublicReadJoinKind::RightSemi(constraint)
        | BroadPublicReadJoinKind::Anti(constraint)
        | BroadPublicReadJoinKind::LeftAnti(constraint)
        | BroadPublicReadJoinKind::RightAnti(constraint)
        | BroadPublicReadJoinKind::StraightJoin(constraint) => {
            ensure_broad_public_read_join_constraint_is_fully_typed(constraint, path)
        }
        BroadPublicReadJoinKind::CrossApply | BroadPublicReadJoinKind::OuterApply => Ok(()),
        BroadPublicReadJoinKind::AsOf {
            match_condition,
            constraint,
        } => {
            ensure_broad_sql_expr_is_fully_typed(match_condition, &format!("{path}.match"))?;
            ensure_broad_public_read_join_constraint_is_fully_typed(
                constraint,
                &format!("{path}.constraint"),
            )
        }
    }
}

fn ensure_broad_public_read_join_constraint_is_fully_typed(
    constraint: &BroadPublicReadJoinConstraint,
    path: &str,
) -> Result<(), LixError> {
    match constraint {
        BroadPublicReadJoinConstraint::On(expr) => {
            ensure_broad_sql_expr_is_fully_typed(expr, &format!("{path}.on"))
        }
        BroadPublicReadJoinConstraint::None
        | BroadPublicReadJoinConstraint::Natural
        | BroadPublicReadJoinConstraint::Using(_) => Ok(()),
    }
}

fn ensure_broad_public_read_table_factor_is_fully_typed(
    factor: &BroadPublicReadTableFactor,
    path: &str,
) -> Result<(), LixError> {
    match factor {
        BroadPublicReadTableFactor::Table { .. } => Ok(()),
        BroadPublicReadTableFactor::Derived { subquery, .. } => {
            ensure_broad_public_read_query_is_fully_typed(subquery, &format!("{path}.subquery"))
        }
        BroadPublicReadTableFactor::NestedJoin {
            table_with_joins, ..
        } => ensure_broad_public_read_table_with_joins_is_fully_typed(
            table_with_joins,
            &format!("{path}.nested_join"),
        ),
    }
}

fn ensure_broad_public_read_projection_item_is_fully_typed(
    item: &BroadPublicReadProjectionItem,
    path: &str,
) -> Result<(), LixError> {
    match &item.kind {
        BroadPublicReadProjectionItemKind::Expr { expr, .. } => {
            ensure_broad_sql_expr_is_fully_typed(expr, path)
        }
        BroadPublicReadProjectionItemKind::Wildcard
        | BroadPublicReadProjectionItemKind::QualifiedWildcard { .. } => Ok(()),
    }
}

fn ensure_broad_public_read_group_by_is_fully_typed(
    group_by: &BroadPublicReadGroupBy,
    path: &str,
) -> Result<(), LixError> {
    match &group_by.kind {
        BroadPublicReadGroupByKind::All => Ok(()),
        BroadPublicReadGroupByKind::Expressions(expressions) => {
            for (index, expr) in expressions.iter().enumerate() {
                ensure_broad_sql_expr_is_fully_typed(
                    expr,
                    &format!("{path}.expressions[{index}]"),
                )?;
            }
            Ok(())
        }
    }
}

fn ensure_broad_public_read_order_by_is_fully_typed(
    order_by: &BroadPublicReadOrderBy,
    path: &str,
) -> Result<(), LixError> {
    match &order_by.kind {
        BroadPublicReadOrderByKind::All => Ok(()),
        BroadPublicReadOrderByKind::Expressions(expressions) => {
            for (index, expr) in expressions.iter().enumerate() {
                ensure_broad_sql_expr_is_fully_typed(
                    &expr.expr,
                    &format!("{path}.expressions[{index}].expr"),
                )?;
            }
            Ok(())
        }
    }
}

fn ensure_broad_public_read_limit_clause_is_fully_typed(
    limit_clause: &BroadPublicReadLimitClause,
    path: &str,
) -> Result<(), LixError> {
    match &limit_clause.kind {
        BroadPublicReadLimitClauseKind::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if let Some(limit) = limit {
                ensure_broad_sql_expr_is_fully_typed(limit, &format!("{path}.limit"))?;
            }
            if let Some(offset) = offset {
                ensure_broad_sql_expr_is_fully_typed(&offset.value, &format!("{path}.offset"))?;
            }
            for (index, expr) in limit_by.iter().enumerate() {
                ensure_broad_sql_expr_is_fully_typed(expr, &format!("{path}.limit_by[{index}]"))?;
            }
            Ok(())
        }
        BroadPublicReadLimitClauseKind::OffsetCommaLimit { offset, limit } => {
            ensure_broad_sql_expr_is_fully_typed(offset, &format!("{path}.offset"))?;
            ensure_broad_sql_expr_is_fully_typed(limit, &format!("{path}.limit"))
        }
    }
}

fn ensure_broad_sql_expr_is_fully_typed(expr: &BroadSqlExpr, path: &str) -> Result<(), LixError> {
    match &expr.kind {
        BroadSqlExprKind::Identifier(_)
        | BroadSqlExprKind::CompoundIdentifier(_)
        | BroadSqlExprKind::Value(_)
        | BroadSqlExprKind::TypedString { .. } => Ok(()),
        BroadSqlExprKind::BinaryOp { left, right, .. }
        | BroadSqlExprKind::AnyOp { left, right, .. }
        | BroadSqlExprKind::AllOp { left, right, .. }
        | BroadSqlExprKind::IsDistinctFrom { left, right }
        | BroadSqlExprKind::IsNotDistinctFrom { left, right } => {
            ensure_broad_sql_expr_is_fully_typed(left, &format!("{path}.left"))?;
            ensure_broad_sql_expr_is_fully_typed(right, &format!("{path}.right"))
        }
        BroadSqlExprKind::UnaryOp { expr, .. }
        | BroadSqlExprKind::Nested(expr)
        | BroadSqlExprKind::IsNull(expr)
        | BroadSqlExprKind::IsNotNull(expr)
        | BroadSqlExprKind::IsTrue(expr)
        | BroadSqlExprKind::IsNotTrue(expr)
        | BroadSqlExprKind::IsFalse(expr)
        | BroadSqlExprKind::IsNotFalse(expr)
        | BroadSqlExprKind::IsUnknown(expr)
        | BroadSqlExprKind::IsNotUnknown(expr) => {
            ensure_broad_sql_expr_is_fully_typed(expr, &format!("{path}.expr"))
        }
        BroadSqlExprKind::Cast { expr, .. } => {
            ensure_broad_sql_expr_is_fully_typed(expr, &format!("{path}.expr"))
        }
        BroadSqlExprKind::InList { expr, list, .. } => {
            ensure_broad_sql_expr_is_fully_typed(expr, &format!("{path}.expr"))?;
            for (index, item) in list.iter().enumerate() {
                ensure_broad_sql_expr_is_fully_typed(item, &format!("{path}.list[{index}]"))?;
            }
            Ok(())
        }
        BroadSqlExprKind::InSubquery { expr, subquery, .. } => {
            ensure_broad_sql_expr_is_fully_typed(expr, &format!("{path}.expr"))?;
            ensure_broad_public_read_query_is_fully_typed(subquery, &format!("{path}.subquery"))
        }
        BroadSqlExprKind::InUnnest {
            expr, array_expr, ..
        } => {
            ensure_broad_sql_expr_is_fully_typed(expr, &format!("{path}.expr"))?;
            ensure_broad_sql_expr_is_fully_typed(array_expr, &format!("{path}.array_expr"))
        }
        BroadSqlExprKind::Between {
            expr, low, high, ..
        } => {
            ensure_broad_sql_expr_is_fully_typed(expr, &format!("{path}.expr"))?;
            ensure_broad_sql_expr_is_fully_typed(low, &format!("{path}.low"))?;
            ensure_broad_sql_expr_is_fully_typed(high, &format!("{path}.high"))
        }
        BroadSqlExprKind::Like { expr, pattern, .. }
        | BroadSqlExprKind::ILike { expr, pattern, .. } => {
            ensure_broad_sql_expr_is_fully_typed(expr, &format!("{path}.expr"))?;
            ensure_broad_sql_expr_is_fully_typed(pattern, &format!("{path}.pattern"))
        }
        BroadSqlExprKind::Function(function) => {
            ensure_broad_sql_function_is_fully_typed(function, path)
        }
        BroadSqlExprKind::Case {
            operand,
            conditions,
            else_result,
        } => {
            if let Some(operand) = operand {
                ensure_broad_sql_expr_is_fully_typed(operand, &format!("{path}.operand"))?;
            }
            for (index, when) in conditions.iter().enumerate() {
                ensure_broad_sql_case_when_is_fully_typed(when, &format!("{path}.when[{index}]"))?;
            }
            if let Some(else_result) = else_result {
                ensure_broad_sql_expr_is_fully_typed(else_result, &format!("{path}.else_result"))?;
            }
            Ok(())
        }
        BroadSqlExprKind::Exists { subquery, .. } | BroadSqlExprKind::ScalarSubquery(subquery) => {
            ensure_broad_public_read_query_is_fully_typed(subquery, &format!("{path}.subquery"))
        }
        BroadSqlExprKind::Tuple(items) => {
            for (index, item) in items.iter().enumerate() {
                ensure_broad_sql_expr_is_fully_typed(item, &format!("{path}.items[{index}]"))?;
            }
            Ok(())
        }
        BroadSqlExprKind::Unsupported { .. } => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "broad public-read physical lowering requires fully typed scalar IR; unsupported expression remains at {path}"
            ),
        )),
    }
}

fn ensure_broad_sql_case_when_is_fully_typed(
    when: &BroadSqlCaseWhen,
    path: &str,
) -> Result<(), LixError> {
    ensure_broad_sql_expr_is_fully_typed(&when.condition, &format!("{path}.condition"))?;
    ensure_broad_sql_expr_is_fully_typed(&when.result, &format!("{path}.result"))
}

fn ensure_broad_sql_function_is_fully_typed(
    function: &BroadSqlFunction,
    path: &str,
) -> Result<(), LixError> {
    ensure_broad_sql_function_arguments_are_fully_typed(
        &function.parameters,
        &format!("{path}.parameters"),
    )?;
    ensure_broad_sql_function_arguments_are_fully_typed(&function.args, &format!("{path}.args"))?;
    if let Some(filter) = &function.filter {
        ensure_broad_sql_expr_is_fully_typed(filter, &format!("{path}.filter"))?;
    }
    for (index, expr) in function.within_group.iter().enumerate() {
        ensure_broad_sql_expr_is_fully_typed(&expr.expr, &format!("{path}.within_group[{index}]"))?;
    }
    Ok(())
}

fn ensure_broad_sql_function_arguments_are_fully_typed(
    arguments: &BroadSqlFunctionArguments,
    path: &str,
) -> Result<(), LixError> {
    match arguments {
        BroadSqlFunctionArguments::None => Ok(()),
        BroadSqlFunctionArguments::Subquery(query) => {
            ensure_broad_public_read_query_is_fully_typed(query, &format!("{path}.subquery"))
        }
        BroadSqlFunctionArguments::List(list) => {
            for (index, arg) in list.args.iter().enumerate() {
                ensure_broad_sql_function_arg_is_fully_typed(
                    arg,
                    &format!("{path}.args[{index}]"),
                )?;
            }
            Ok(())
        }
    }
}

fn ensure_broad_sql_function_arg_is_fully_typed(
    arg: &BroadSqlFunctionArg,
    path: &str,
) -> Result<(), LixError> {
    match arg {
        BroadSqlFunctionArg::Named { arg, .. } => {
            ensure_broad_sql_function_arg_expr_is_fully_typed(arg, &format!("{path}.value"))
        }
        BroadSqlFunctionArg::ExprNamed { name, arg, .. } => {
            ensure_broad_sql_expr_is_fully_typed(name, &format!("{path}.name"))?;
            ensure_broad_sql_function_arg_expr_is_fully_typed(arg, &format!("{path}.value"))
        }
        BroadSqlFunctionArg::Unnamed(arg) => {
            ensure_broad_sql_function_arg_expr_is_fully_typed(arg, &format!("{path}.value"))
        }
    }
}

fn ensure_broad_sql_function_arg_expr_is_fully_typed(
    arg: &BroadSqlFunctionArgExpr,
    path: &str,
) -> Result<(), LixError> {
    match arg {
        BroadSqlFunctionArgExpr::Expr(expr) => ensure_broad_sql_expr_is_fully_typed(expr, path),
        BroadSqlFunctionArgExpr::QualifiedWildcard(_) | BroadSqlFunctionArgExpr::Wildcard => Ok(()),
    }
}

fn broad_public_read_statement_contains_relation_kind(
    statement: &BroadPublicReadStatement,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    match statement {
        BroadPublicReadStatement::Query(query) => {
            broad_public_read_query_contains_relation_kind(query, predicate)
        }
        BroadPublicReadStatement::Explain { statement, .. } => {
            broad_public_read_statement_contains_relation_kind(statement, predicate)
        }
    }
}

fn broad_public_read_query_contains_relation_kind(
    query: &BroadPublicReadQuery,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    query.with.as_ref().is_some_and(|with| {
        with.cte_tables
            .iter()
            .any(|cte| broad_public_read_query_contains_relation_kind(&cte.query, predicate))
    }) || broad_public_read_set_expr_contains_relation_kind(&query.body, predicate)
        || query.order_by.as_ref().is_some_and(|order_by| {
            broad_public_read_order_by_contains_relation_kind(order_by, predicate)
        })
        || query.limit_clause.as_ref().is_some_and(|limit_clause| {
            broad_public_read_limit_clause_contains_relation_kind(limit_clause, predicate)
        })
}

fn broad_public_read_set_expr_contains_relation_kind(
    expr: &BroadPublicReadSetExpr,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    match expr {
        BroadPublicReadSetExpr::Select(select) => {
            broad_public_read_select_contains_relation_kind(select, predicate)
        }
        BroadPublicReadSetExpr::Query(query) => {
            broad_public_read_query_contains_relation_kind(query, predicate)
        }
        BroadPublicReadSetExpr::SetOperation { left, right, .. } => {
            broad_public_read_set_expr_contains_relation_kind(left, predicate)
                || broad_public_read_set_expr_contains_relation_kind(right, predicate)
        }
        BroadPublicReadSetExpr::Table { relation, .. } => predicate(relation),
    }
}

fn broad_public_read_select_contains_relation_kind(
    select: &BroadPublicReadSelect,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    broad_public_read_distinct_contains_relation_kind(select.distinct.as_ref(), predicate)
        || select.projection.iter().any(|projection| {
            broad_public_read_projection_item_contains_relation_kind(projection, predicate)
        })
        || select.from.iter().any(|table| {
            broad_public_read_table_with_joins_contains_relation_kind(table, predicate)
        })
        || select
            .selection
            .as_ref()
            .is_some_and(|selection| broad_sql_expr_contains_relation_kind(selection, predicate))
        || broad_public_read_group_by_contains_relation_kind(&select.group_by, predicate)
        || select
            .having
            .as_ref()
            .is_some_and(|having| broad_sql_expr_contains_relation_kind(having, predicate))
}

fn broad_public_read_distinct_contains_relation_kind(
    distinct: Option<&BroadPublicReadDistinct>,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    match distinct {
        Some(BroadPublicReadDistinct::On(expressions)) => expressions
            .iter()
            .any(|expr| broad_sql_expr_contains_relation_kind(expr, predicate)),
        Some(BroadPublicReadDistinct::Distinct) | None => false,
    }
}

fn broad_public_read_table_with_joins_contains_relation_kind(
    table: &BroadPublicReadTableWithJoins,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    broad_public_read_table_factor_contains_relation_kind(&table.relation, predicate)
        || table
            .joins
            .iter()
            .any(|join| broad_public_read_join_contains_relation_kind(join, predicate))
}

fn broad_public_read_join_contains_relation_kind(
    join: &BroadPublicReadJoin,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    broad_public_read_table_factor_contains_relation_kind(&join.relation, predicate)
        || broad_public_read_join_kind_contains_relation_kind(&join.kind, predicate)
}

fn broad_public_read_join_kind_contains_relation_kind(
    kind: &BroadPublicReadJoinKind,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    match kind {
        BroadPublicReadJoinKind::Join(constraint)
        | BroadPublicReadJoinKind::Inner(constraint)
        | BroadPublicReadJoinKind::Left(constraint)
        | BroadPublicReadJoinKind::LeftOuter(constraint)
        | BroadPublicReadJoinKind::Right(constraint)
        | BroadPublicReadJoinKind::RightOuter(constraint)
        | BroadPublicReadJoinKind::FullOuter(constraint)
        | BroadPublicReadJoinKind::CrossJoin(constraint)
        | BroadPublicReadJoinKind::Semi(constraint)
        | BroadPublicReadJoinKind::LeftSemi(constraint)
        | BroadPublicReadJoinKind::RightSemi(constraint)
        | BroadPublicReadJoinKind::Anti(constraint)
        | BroadPublicReadJoinKind::LeftAnti(constraint)
        | BroadPublicReadJoinKind::RightAnti(constraint)
        | BroadPublicReadJoinKind::StraightJoin(constraint) => {
            broad_public_read_join_constraint_contains_relation_kind(constraint, predicate)
        }
        BroadPublicReadJoinKind::CrossApply | BroadPublicReadJoinKind::OuterApply => false,
        BroadPublicReadJoinKind::AsOf {
            match_condition,
            constraint,
        } => {
            broad_sql_expr_contains_relation_kind(match_condition, predicate)
                || broad_public_read_join_constraint_contains_relation_kind(constraint, predicate)
        }
    }
}

fn broad_public_read_join_constraint_contains_relation_kind(
    constraint: &BroadPublicReadJoinConstraint,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    match constraint {
        BroadPublicReadJoinConstraint::On(expr) => {
            broad_sql_expr_contains_relation_kind(expr, predicate)
        }
        BroadPublicReadJoinConstraint::None
        | BroadPublicReadJoinConstraint::Natural
        | BroadPublicReadJoinConstraint::Using(_) => false,
    }
}

fn broad_public_read_table_factor_contains_relation_kind(
    factor: &BroadPublicReadTableFactor,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    match factor {
        BroadPublicReadTableFactor::Table { relation, .. } => predicate(relation),
        BroadPublicReadTableFactor::Derived { subquery, .. } => {
            broad_public_read_query_contains_relation_kind(subquery, predicate)
        }
        BroadPublicReadTableFactor::NestedJoin {
            table_with_joins, ..
        } => broad_public_read_table_with_joins_contains_relation_kind(table_with_joins, predicate),
    }
}

fn broad_public_read_projection_item_contains_relation_kind(
    item: &BroadPublicReadProjectionItem,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    match &item.kind {
        BroadPublicReadProjectionItemKind::Expr { expr, .. } => {
            broad_sql_expr_contains_relation_kind(expr, predicate)
        }
        BroadPublicReadProjectionItemKind::Wildcard
        | BroadPublicReadProjectionItemKind::QualifiedWildcard { .. } => false,
    }
}

fn broad_public_read_group_by_contains_relation_kind(
    group_by: &BroadPublicReadGroupBy,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    match &group_by.kind {
        BroadPublicReadGroupByKind::All => false,
        BroadPublicReadGroupByKind::Expressions(expressions) => expressions
            .iter()
            .any(|expr| broad_sql_expr_contains_relation_kind(expr, predicate)),
    }
}

fn broad_public_read_order_by_contains_relation_kind(
    order_by: &BroadPublicReadOrderBy,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    match &order_by.kind {
        BroadPublicReadOrderByKind::All => false,
        BroadPublicReadOrderByKind::Expressions(expressions) => expressions
            .iter()
            .any(|expr| broad_sql_expr_contains_relation_kind(&expr.expr, predicate)),
    }
}

fn broad_public_read_limit_clause_contains_relation_kind(
    limit_clause: &BroadPublicReadLimitClause,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    match &limit_clause.kind {
        BroadPublicReadLimitClauseKind::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            limit
                .as_ref()
                .is_some_and(|expr| broad_sql_expr_contains_relation_kind(expr, predicate))
                || offset.as_ref().is_some_and(|offset| {
                    broad_sql_expr_contains_relation_kind(&offset.value, predicate)
                })
                || limit_by
                    .iter()
                    .any(|expr| broad_sql_expr_contains_relation_kind(expr, predicate))
        }
        BroadPublicReadLimitClauseKind::OffsetCommaLimit { offset, limit } => {
            broad_sql_expr_contains_relation_kind(offset, predicate)
                || broad_sql_expr_contains_relation_kind(limit, predicate)
        }
    }
}

fn broad_sql_expr_contains_relation_kind(
    expr: &BroadSqlExpr,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    match &expr.kind {
        BroadSqlExprKind::Identifier(_)
        | BroadSqlExprKind::CompoundIdentifier(_)
        | BroadSqlExprKind::Value(_)
        | BroadSqlExprKind::TypedString { .. }
        | BroadSqlExprKind::Unsupported { .. } => false,
        BroadSqlExprKind::BinaryOp { left, right, .. }
        | BroadSqlExprKind::AnyOp { left, right, .. }
        | BroadSqlExprKind::AllOp { left, right, .. }
        | BroadSqlExprKind::IsDistinctFrom { left, right }
        | BroadSqlExprKind::IsNotDistinctFrom { left, right } => {
            broad_sql_expr_contains_relation_kind(left, predicate)
                || broad_sql_expr_contains_relation_kind(right, predicate)
        }
        BroadSqlExprKind::UnaryOp { expr, .. }
        | BroadSqlExprKind::Nested(expr)
        | BroadSqlExprKind::IsNull(expr)
        | BroadSqlExprKind::IsNotNull(expr)
        | BroadSqlExprKind::IsTrue(expr)
        | BroadSqlExprKind::IsNotTrue(expr)
        | BroadSqlExprKind::IsFalse(expr)
        | BroadSqlExprKind::IsNotFalse(expr)
        | BroadSqlExprKind::IsUnknown(expr)
        | BroadSqlExprKind::IsNotUnknown(expr) => {
            broad_sql_expr_contains_relation_kind(expr, predicate)
        }
        BroadSqlExprKind::Cast { expr, .. } => {
            broad_sql_expr_contains_relation_kind(expr, predicate)
        }
        BroadSqlExprKind::InList { expr, list, .. } => {
            broad_sql_expr_contains_relation_kind(expr, predicate)
                || list
                    .iter()
                    .any(|item| broad_sql_expr_contains_relation_kind(item, predicate))
        }
        BroadSqlExprKind::InSubquery { expr, subquery, .. } => {
            broad_sql_expr_contains_relation_kind(expr, predicate)
                || broad_public_read_query_contains_relation_kind(subquery, predicate)
        }
        BroadSqlExprKind::InUnnest {
            expr, array_expr, ..
        } => {
            broad_sql_expr_contains_relation_kind(expr, predicate)
                || broad_sql_expr_contains_relation_kind(array_expr, predicate)
        }
        BroadSqlExprKind::Between {
            expr, low, high, ..
        } => {
            broad_sql_expr_contains_relation_kind(expr, predicate)
                || broad_sql_expr_contains_relation_kind(low, predicate)
                || broad_sql_expr_contains_relation_kind(high, predicate)
        }
        BroadSqlExprKind::Like { expr, pattern, .. }
        | BroadSqlExprKind::ILike { expr, pattern, .. } => {
            broad_sql_expr_contains_relation_kind(expr, predicate)
                || broad_sql_expr_contains_relation_kind(pattern, predicate)
        }
        BroadSqlExprKind::Function(function) => {
            broad_sql_function_contains_relation_kind(function, predicate)
        }
        BroadSqlExprKind::Case {
            operand,
            conditions,
            else_result,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| broad_sql_expr_contains_relation_kind(expr, predicate))
                || conditions.iter().any(|when| {
                    broad_sql_expr_contains_relation_kind(&when.condition, predicate)
                        || broad_sql_expr_contains_relation_kind(&when.result, predicate)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|expr| broad_sql_expr_contains_relation_kind(expr, predicate))
        }
        BroadSqlExprKind::Exists { subquery, .. } | BroadSqlExprKind::ScalarSubquery(subquery) => {
            broad_public_read_query_contains_relation_kind(subquery, predicate)
        }
        BroadSqlExprKind::Tuple(items) => items
            .iter()
            .any(|item| broad_sql_expr_contains_relation_kind(item, predicate)),
    }
}

fn broad_sql_function_contains_relation_kind(
    function: &BroadSqlFunction,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    broad_sql_function_arguments_contains_relation_kind(&function.parameters, predicate)
        || broad_sql_function_arguments_contains_relation_kind(&function.args, predicate)
        || function
            .filter
            .as_ref()
            .is_some_and(|filter| broad_sql_expr_contains_relation_kind(filter, predicate))
        || function
            .within_group
            .iter()
            .any(|expr| broad_sql_expr_contains_relation_kind(&expr.expr, predicate))
}

fn broad_sql_function_arguments_contains_relation_kind(
    arguments: &BroadSqlFunctionArguments,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    match arguments {
        BroadSqlFunctionArguments::None => false,
        BroadSqlFunctionArguments::Subquery(query) => {
            broad_public_read_query_contains_relation_kind(query, predicate)
        }
        BroadSqlFunctionArguments::List(list) => list.args.iter().any(|arg| match arg {
            BroadSqlFunctionArg::Named { arg, .. } => {
                broad_sql_function_arg_expr_contains_relation_kind(arg, predicate)
            }
            BroadSqlFunctionArg::ExprNamed { name, arg, .. } => {
                broad_sql_expr_contains_relation_kind(name, predicate)
                    || broad_sql_function_arg_expr_contains_relation_kind(arg, predicate)
            }
            BroadSqlFunctionArg::Unnamed(arg) => {
                broad_sql_function_arg_expr_contains_relation_kind(arg, predicate)
            }
        }),
    }
}

fn broad_sql_function_arg_expr_contains_relation_kind(
    arg: &BroadSqlFunctionArgExpr,
    predicate: impl Copy + Fn(&BroadPublicReadRelation) -> bool,
) -> bool {
    match arg {
        BroadSqlFunctionArgExpr::Expr(expr) => {
            broad_sql_expr_contains_relation_kind(expr, predicate)
        }
        BroadSqlFunctionArgExpr::QualifiedWildcard(_) | BroadSqlFunctionArgExpr::Wildcard => false,
    }
}

fn lower_broad_public_read_statement(
    statement: &BroadPublicReadStatement,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Statement, LixError> {
    match statement {
        BroadPublicReadStatement::Query(query) => {
            Ok(Statement::Query(Box::new(lower_broad_public_read_query(
                query,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?)))
        }
        BroadPublicReadStatement::Explain {
            original,
            statement: inner,
        } => {
            let mut lowered = original.clone();
            if let Statement::Explain { statement, .. } = &mut lowered {
                **statement = lower_broad_public_read_statement(
                    inner,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )?;
            }
            Ok(lowered)
        }
    }
}

fn lower_broad_public_read_query(
    query: &BroadPublicReadQuery,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Query, LixError> {
    Ok(Query {
        with: query
            .with
            .as_ref()
            .map(|with| {
                lower_broad_public_read_with(
                    with,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )
            })
            .transpose()?,
        body: Box::new(lower_broad_public_read_set_expr(
            &query.body,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?),
        order_by: query
            .order_by
            .as_ref()
            .map(|order_by| {
                lower_broad_public_read_order_by(
                    order_by,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )
            })
            .transpose()?,
        limit_clause: query
            .limit_clause
            .as_ref()
            .map(|limit_clause| {
                lower_broad_public_read_limit_clause(
                    limit_clause,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )
            })
            .transpose()?,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    })
}

fn lower_broad_public_read_with(
    with: &BroadPublicReadWith,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<With, LixError> {
    Ok(With {
        with_token: AttachedToken::empty(),
        recursive: with.recursive,
        cte_tables: with
            .cte_tables
            .iter()
            .map(|cte| {
                Ok(Cte {
                    alias: lower_broad_public_read_alias(&cte.alias),
                    query: Box::new(lower_broad_public_read_query(
                        &cte.query,
                        registry,
                        dialect,
                        active_version_id,
                        known_live_layouts,
                    )?),
                    from: cte.from.as_ref().map(Ident::new),
                    materialized: cte.materialized.clone(),
                    closing_paren_token: AttachedToken::empty(),
                })
            })
            .collect::<Result<_, LixError>>()?,
    })
}

fn lower_broad_public_read_set_expr(
    expr: &BroadPublicReadSetExpr,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<SetExpr, LixError> {
    match expr {
        BroadPublicReadSetExpr::Select(select) => {
            Ok(SetExpr::Select(Box::new(lower_broad_public_read_select(
                select,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?)))
        }
        BroadPublicReadSetExpr::Query(query) => {
            Ok(SetExpr::Query(Box::new(lower_broad_public_read_query(
                query,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?)))
        }
        BroadPublicReadSetExpr::SetOperation {
            operator,
            quantifier,
            left,
            right,
            ..
        } => Ok(SetExpr::SetOperation {
            op: lower_broad_public_read_set_operation_kind(*operator),
            set_quantifier: lower_broad_public_read_set_quantifier(*quantifier),
            left: Box::new(lower_broad_public_read_set_expr(
                left,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            right: Box::new(lower_broad_public_read_set_expr(
                right,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        }),
        BroadPublicReadSetExpr::Table { relation, .. } => lower_broad_public_read_table_set_expr(
            relation,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        ),
    }
}

fn lower_broad_public_read_select(
    select: &BroadPublicReadSelect,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Select, LixError> {
    Ok(Select {
        select_token: AttachedToken::empty(),
        distinct: select
            .distinct
            .as_ref()
            .map(|distinct| {
                lower_broad_public_read_distinct(
                    distinct,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )
            })
            .transpose()?,
        top: None,
        top_before_distinct: false,
        projection: select
            .projection
            .iter()
            .map(|item| {
                lower_broad_public_read_projection_item(
                    item,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )
            })
            .collect::<Result<_, _>>()?,
        exclude: None,
        into: None,
        from: select
            .from
            .iter()
            .map(|table| {
                lower_broad_public_read_table_with_joins(
                    table,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )
            })
            .collect::<Result<_, _>>()?,
        lateral_views: Vec::new(),
        prewhere: None,
        selection: select
            .selection
            .as_ref()
            .map(|expr| {
                lower_broad_sql_expr(
                    expr,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )
            })
            .transpose()?,
        group_by: lower_broad_public_read_group_by(
            &select.group_by,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?,
        cluster_by: Vec::new(),
        distribute_by: Vec::new(),
        sort_by: Vec::new(),
        having: select
            .having
            .as_ref()
            .map(|expr| {
                lower_broad_sql_expr(
                    expr,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )
            })
            .transpose()?,
        named_window: Vec::new(),
        qualify: None,
        window_before_qualify: false,
        value_table_mode: None,
        connect_by: None,
        flavor: SelectFlavor::Standard,
    })
}

fn lower_broad_public_read_distinct(
    distinct: &BroadPublicReadDistinct,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Distinct, LixError> {
    match distinct {
        BroadPublicReadDistinct::Distinct => Ok(Distinct::Distinct),
        BroadPublicReadDistinct::On(expressions) => Ok(Distinct::On(
            expressions
                .iter()
                .map(|expr| {
                    lower_broad_sql_expr(
                        expr,
                        registry,
                        dialect,
                        active_version_id,
                        known_live_layouts,
                    )
                })
                .collect::<Result<_, _>>()?,
        )),
    }
}

fn lower_broad_public_read_projection_item(
    item: &BroadPublicReadProjectionItem,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<SelectItem, LixError> {
    match &item.kind {
        BroadPublicReadProjectionItemKind::Wildcard => Ok(SelectItem::Wildcard(Default::default())),
        BroadPublicReadProjectionItemKind::QualifiedWildcard { qualifier } => {
            Ok(SelectItem::QualifiedWildcard(
                SelectItemQualifiedWildcardKind::ObjectName(qualifier.clone()),
                Default::default(),
            ))
        }
        BroadPublicReadProjectionItemKind::Expr { alias, expr } => {
            let lowered = lower_broad_sql_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?;
            Ok(match alias {
                Some(alias) => SelectItem::ExprWithAlias {
                    expr: lowered,
                    alias: Ident::new(alias),
                },
                None => SelectItem::UnnamedExpr(lowered),
            })
        }
    }
}

fn lower_broad_public_read_table_with_joins(
    table: &BroadPublicReadTableWithJoins,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<TableWithJoins, LixError> {
    Ok(TableWithJoins {
        relation: lower_broad_public_read_table_factor(
            &table.relation,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?,
        joins: table
            .joins
            .iter()
            .map(|join| {
                lower_broad_public_read_join(
                    join,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )
            })
            .collect::<Result<_, _>>()?,
    })
}

fn lower_broad_public_read_join(
    join: &BroadPublicReadJoin,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<sqlparser::ast::Join, LixError> {
    Ok(sqlparser::ast::Join {
        relation: lower_broad_public_read_table_factor(
            &join.relation,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?,
        global: join.global,
        join_operator: lower_broad_public_read_join_kind(
            &join.kind,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?,
    })
}

fn lower_broad_public_read_join_kind(
    kind: &BroadPublicReadJoinKind,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<JoinOperator, LixError> {
    Ok(match kind {
        BroadPublicReadJoinKind::Join(constraint) => {
            JoinOperator::Join(lower_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?)
        }
        BroadPublicReadJoinKind::Inner(constraint) => {
            JoinOperator::Inner(lower_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?)
        }
        BroadPublicReadJoinKind::Left(constraint) => {
            JoinOperator::Left(lower_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?)
        }
        BroadPublicReadJoinKind::LeftOuter(constraint) => {
            JoinOperator::LeftOuter(lower_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?)
        }
        BroadPublicReadJoinKind::Right(constraint) => {
            JoinOperator::Right(lower_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?)
        }
        BroadPublicReadJoinKind::RightOuter(constraint) => {
            JoinOperator::RightOuter(lower_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?)
        }
        BroadPublicReadJoinKind::FullOuter(constraint) => {
            JoinOperator::FullOuter(lower_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?)
        }
        BroadPublicReadJoinKind::CrossJoin(constraint) => {
            JoinOperator::CrossJoin(lower_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?)
        }
        BroadPublicReadJoinKind::Semi(constraint) => {
            JoinOperator::Semi(lower_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?)
        }
        BroadPublicReadJoinKind::LeftSemi(constraint) => {
            JoinOperator::LeftSemi(lower_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?)
        }
        BroadPublicReadJoinKind::RightSemi(constraint) => {
            JoinOperator::RightSemi(lower_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?)
        }
        BroadPublicReadJoinKind::Anti(constraint) => {
            JoinOperator::Anti(lower_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?)
        }
        BroadPublicReadJoinKind::LeftAnti(constraint) => {
            JoinOperator::LeftAnti(lower_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?)
        }
        BroadPublicReadJoinKind::RightAnti(constraint) => {
            JoinOperator::RightAnti(lower_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?)
        }
        BroadPublicReadJoinKind::StraightJoin(constraint) => {
            JoinOperator::StraightJoin(lower_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?)
        }
        BroadPublicReadJoinKind::CrossApply => JoinOperator::CrossApply,
        BroadPublicReadJoinKind::OuterApply => JoinOperator::OuterApply,
        BroadPublicReadJoinKind::AsOf {
            match_condition,
            constraint,
        } => JoinOperator::AsOf {
            match_condition: lower_broad_sql_expr(
                match_condition,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
            constraint: lower_broad_public_read_join_constraint(
                constraint,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
        },
    })
}

fn lower_broad_public_read_join_constraint(
    constraint: &BroadPublicReadJoinConstraint,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<JoinConstraint, LixError> {
    Ok(match constraint {
        BroadPublicReadJoinConstraint::None => JoinConstraint::None,
        BroadPublicReadJoinConstraint::Natural => JoinConstraint::Natural,
        BroadPublicReadJoinConstraint::Using(columns) => JoinConstraint::Using(
            columns
                .iter()
                .map(|column| ObjectName(vec![ObjectNamePart::Identifier(Ident::new(column))]))
                .collect(),
        ),
        BroadPublicReadJoinConstraint::On(expr) => JoinConstraint::On(lower_broad_sql_expr(
            expr,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?),
    })
}

fn lower_broad_public_read_group_by(
    group_by: &BroadPublicReadGroupBy,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<GroupByExpr, LixError> {
    match &group_by.kind {
        BroadPublicReadGroupByKind::All => Ok(GroupByExpr::All(Vec::new())),
        BroadPublicReadGroupByKind::Expressions(expressions) => Ok(GroupByExpr::Expressions(
            expressions
                .iter()
                .map(|expr| {
                    lower_broad_sql_expr(
                        expr,
                        registry,
                        dialect,
                        active_version_id,
                        known_live_layouts,
                    )
                })
                .collect::<Result<_, _>>()?,
            Vec::new(),
        )),
    }
}

fn lower_broad_public_read_order_by(
    order_by: &BroadPublicReadOrderBy,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<OrderBy, LixError> {
    Ok(OrderBy {
        kind: match &order_by.kind {
            BroadPublicReadOrderByKind::All => OrderByKind::All(Default::default()),
            BroadPublicReadOrderByKind::Expressions(expressions) => OrderByKind::Expressions(
                expressions
                    .iter()
                    .map(|expr| {
                        lower_broad_public_read_order_by_expr(
                            expr,
                            registry,
                            dialect,
                            active_version_id,
                            known_live_layouts,
                        )
                    })
                    .collect::<Result<_, _>>()?,
            ),
        },
        interpolate: None,
    })
}

fn lower_broad_public_read_order_by_expr(
    expr: &BroadPublicReadOrderByExpr,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<OrderByExpr, LixError> {
    Ok(OrderByExpr {
        expr: lower_broad_sql_expr(
            &expr.expr,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?,
        options: OrderByOptions {
            asc: expr.asc,
            nulls_first: expr.nulls_first,
        },
        with_fill: None,
    })
}

fn lower_broad_public_read_limit_clause(
    limit_clause: &BroadPublicReadLimitClause,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<LimitClause, LixError> {
    Ok(match &limit_clause.kind {
        BroadPublicReadLimitClauseKind::LimitOffset {
            limit,
            offset,
            limit_by,
        } => LimitClause::LimitOffset {
            limit: limit
                .as_ref()
                .map(|expr| {
                    lower_broad_sql_expr(
                        expr,
                        registry,
                        dialect,
                        active_version_id,
                        known_live_layouts,
                    )
                })
                .transpose()?,
            offset: offset
                .as_ref()
                .map(|offset| {
                    lower_broad_public_read_offset(
                        offset,
                        registry,
                        dialect,
                        active_version_id,
                        known_live_layouts,
                    )
                })
                .transpose()?,
            limit_by: limit_by
                .iter()
                .map(|expr| {
                    lower_broad_sql_expr(
                        expr,
                        registry,
                        dialect,
                        active_version_id,
                        known_live_layouts,
                    )
                })
                .collect::<Result<_, _>>()?,
        },
        BroadPublicReadLimitClauseKind::OffsetCommaLimit { offset, limit } => {
            LimitClause::OffsetCommaLimit {
                offset: lower_broad_sql_expr(
                    offset,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )?,
                limit: lower_broad_sql_expr(
                    limit,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )?,
            }
        }
    })
}

fn lower_broad_public_read_offset(
    offset: &BroadPublicReadOffset,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Offset, LixError> {
    Ok(Offset {
        value: lower_broad_sql_expr(
            &offset.value,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?,
        rows: offset.rows,
    })
}

fn lower_broad_sql_expr(
    expr: &BroadSqlExpr,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Expr, LixError> {
    Ok(match &expr.kind {
        BroadSqlExprKind::Identifier(ident) => Expr::Identifier(ident.clone()),
        BroadSqlExprKind::CompoundIdentifier(parts) => Expr::CompoundIdentifier(parts.clone()),
        BroadSqlExprKind::Value(value) => Expr::Value(value.clone()),
        BroadSqlExprKind::TypedString {
            data_type,
            value,
            uses_odbc_syntax,
        } => Expr::TypedString(TypedString {
            data_type: data_type.clone(),
            value: value.clone(),
            uses_odbc_syntax: *uses_odbc_syntax,
        }),
        BroadSqlExprKind::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(lower_broad_sql_expr(
                left,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            op: op.clone(),
            right: Box::new(lower_broad_sql_expr(
                right,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        },
        BroadSqlExprKind::AnyOp {
            left,
            compare_op,
            right,
            is_some,
        } => Expr::AnyOp {
            left: Box::new(lower_broad_sql_expr(
                left,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            compare_op: compare_op.clone(),
            right: Box::new(lower_broad_sql_expr(
                right,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            is_some: *is_some,
        },
        BroadSqlExprKind::AllOp {
            left,
            compare_op,
            right,
        } => Expr::AllOp {
            left: Box::new(lower_broad_sql_expr(
                left,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            compare_op: compare_op.clone(),
            right: Box::new(lower_broad_sql_expr(
                right,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        },
        BroadSqlExprKind::UnaryOp { op, expr } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(lower_broad_sql_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        },
        BroadSqlExprKind::Nested(expr) => Expr::Nested(Box::new(lower_broad_sql_expr(
            expr,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?)),
        BroadSqlExprKind::IsNull(expr) => Expr::IsNull(Box::new(lower_broad_sql_expr(
            expr,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?)),
        BroadSqlExprKind::IsNotNull(expr) => Expr::IsNotNull(Box::new(lower_broad_sql_expr(
            expr,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?)),
        BroadSqlExprKind::IsTrue(expr) => Expr::IsTrue(Box::new(lower_broad_sql_expr(
            expr,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?)),
        BroadSqlExprKind::IsNotTrue(expr) => Expr::IsNotTrue(Box::new(lower_broad_sql_expr(
            expr,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?)),
        BroadSqlExprKind::IsFalse(expr) => Expr::IsFalse(Box::new(lower_broad_sql_expr(
            expr,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?)),
        BroadSqlExprKind::IsNotFalse(expr) => Expr::IsNotFalse(Box::new(lower_broad_sql_expr(
            expr,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?)),
        BroadSqlExprKind::IsUnknown(expr) => Expr::IsUnknown(Box::new(lower_broad_sql_expr(
            expr,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?)),
        BroadSqlExprKind::IsNotUnknown(expr) => Expr::IsNotUnknown(Box::new(lower_broad_sql_expr(
            expr,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?)),
        BroadSqlExprKind::IsDistinctFrom { left, right } => Expr::IsDistinctFrom(
            Box::new(lower_broad_sql_expr(
                left,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            Box::new(lower_broad_sql_expr(
                right,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        ),
        BroadSqlExprKind::IsNotDistinctFrom { left, right } => Expr::IsNotDistinctFrom(
            Box::new(lower_broad_sql_expr(
                left,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            Box::new(lower_broad_sql_expr(
                right,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        ),
        BroadSqlExprKind::Cast {
            kind,
            expr,
            data_type,
            format,
        } => Expr::Cast {
            kind: kind.clone(),
            expr: Box::new(lower_broad_sql_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            data_type: data_type.clone(),
            format: format.clone(),
        },
        BroadSqlExprKind::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(lower_broad_sql_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            list: list
                .iter()
                .map(|item| {
                    lower_broad_sql_expr(
                        item,
                        registry,
                        dialect,
                        active_version_id,
                        known_live_layouts,
                    )
                })
                .collect::<Result<_, _>>()?,
            negated: *negated,
        },
        BroadSqlExprKind::InSubquery {
            expr,
            subquery,
            negated,
        } => Expr::InSubquery {
            expr: Box::new(lower_broad_sql_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            subquery: Box::new(lower_broad_public_read_query(
                subquery,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            negated: *negated,
        },
        BroadSqlExprKind::InUnnest {
            expr,
            array_expr,
            negated,
        } => Expr::InUnnest {
            expr: Box::new(lower_broad_sql_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            array_expr: Box::new(lower_broad_sql_expr(
                array_expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            negated: *negated,
        },
        BroadSqlExprKind::Between {
            expr,
            negated,
            low,
            high,
        } => Expr::Between {
            expr: Box::new(lower_broad_sql_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            negated: *negated,
            low: Box::new(lower_broad_sql_expr(
                low,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            high: Box::new(lower_broad_sql_expr(
                high,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        },
        BroadSqlExprKind::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => Expr::Like {
            negated: *negated,
            any: *any,
            expr: Box::new(lower_broad_sql_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            pattern: Box::new(lower_broad_sql_expr(
                pattern,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            escape_char: escape_char.clone(),
        },
        BroadSqlExprKind::ILike {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => Expr::ILike {
            negated: *negated,
            any: *any,
            expr: Box::new(lower_broad_sql_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            pattern: Box::new(lower_broad_sql_expr(
                pattern,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            escape_char: escape_char.clone(),
        },
        BroadSqlExprKind::Function(function) => Expr::Function(lower_broad_sql_function(
            function,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?),
        BroadSqlExprKind::Case {
            operand,
            conditions,
            else_result,
        } => Expr::Case {
            case_token: AttachedToken::empty(),
            end_token: AttachedToken::empty(),
            operand: operand
                .as_ref()
                .map(|expr| {
                    lower_broad_sql_expr(
                        expr,
                        registry,
                        dialect,
                        active_version_id,
                        known_live_layouts,
                    )
                })
                .transpose()?
                .map(Box::new),
            conditions: conditions
                .iter()
                .map(|when| {
                    Ok(sqlparser::ast::CaseWhen {
                        condition: lower_broad_sql_expr(
                            &when.condition,
                            registry,
                            dialect,
                            active_version_id,
                            known_live_layouts,
                        )?,
                        result: lower_broad_sql_expr(
                            &when.result,
                            registry,
                            dialect,
                            active_version_id,
                            known_live_layouts,
                        )?,
                    })
                })
                .collect::<Result<_, LixError>>()?,
            else_result: else_result
                .as_ref()
                .map(|expr| {
                    lower_broad_sql_expr(
                        expr,
                        registry,
                        dialect,
                        active_version_id,
                        known_live_layouts,
                    )
                })
                .transpose()?
                .map(Box::new),
        },
        BroadSqlExprKind::Exists { negated, subquery } => Expr::Exists {
            negated: *negated,
            subquery: Box::new(lower_broad_public_read_query(
                subquery,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
        },
        BroadSqlExprKind::ScalarSubquery(subquery) => {
            Expr::Subquery(Box::new(lower_broad_public_read_query(
                subquery,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?))
        }
        BroadSqlExprKind::Tuple(items) => Expr::Tuple(
            items
                .iter()
                .map(|item| {
                    lower_broad_sql_expr(
                        item,
                        registry,
                        dialect,
                        active_version_id,
                        known_live_layouts,
                    )
                })
                .collect::<Result<_, _>>()?,
        ),
        BroadSqlExprKind::Unsupported { diagnostics_sql } => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "broad public-read physical lowering does not support unsupported expression '{}'",
                    diagnostics_sql
                ),
            ));
        }
    })
}

fn lower_broad_sql_function(
    function: &BroadSqlFunction,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Function, LixError> {
    Ok(Function {
        name: function.name.clone(),
        uses_odbc_syntax: function.uses_odbc_syntax,
        parameters: lower_broad_sql_function_arguments(
            &function.parameters,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?,
        args: lower_broad_sql_function_arguments(
            &function.args,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?,
        filter: function
            .filter
            .as_ref()
            .map(|expr| {
                lower_broad_sql_expr(
                    expr,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )
            })
            .transpose()?
            .map(Box::new),
        null_treatment: function.null_treatment,
        over: None,
        within_group: function
            .within_group
            .iter()
            .map(|expr| {
                lower_broad_public_read_order_by_expr(
                    expr,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                )
            })
            .collect::<Result<_, _>>()?,
    })
}

fn lower_broad_sql_function_arguments(
    arguments: &BroadSqlFunctionArguments,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<FunctionArguments, LixError> {
    Ok(match arguments {
        BroadSqlFunctionArguments::None => FunctionArguments::None,
        BroadSqlFunctionArguments::Subquery(query) => {
            FunctionArguments::Subquery(Box::new(lower_broad_public_read_query(
                query,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?))
        }
        BroadSqlFunctionArguments::List(list) => FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: list.duplicate_treatment,
            args: list
                .args
                .iter()
                .map(|arg| {
                    lower_broad_sql_function_arg(
                        arg,
                        registry,
                        dialect,
                        active_version_id,
                        known_live_layouts,
                    )
                })
                .collect::<Result<_, _>>()?,
            clauses: Vec::new(),
        }),
    })
}

fn lower_broad_sql_function_arg(
    arg: &BroadSqlFunctionArg,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<FunctionArg, LixError> {
    Ok(match arg {
        BroadSqlFunctionArg::Named {
            name,
            arg,
            operator,
        } => FunctionArg::Named {
            name: name.clone(),
            arg: lower_broad_sql_function_arg_expr(
                arg,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
            operator: operator.clone(),
        },
        BroadSqlFunctionArg::ExprNamed {
            name,
            arg,
            operator,
        } => FunctionArg::ExprNamed {
            name: lower_broad_sql_expr(
                name,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
            arg: lower_broad_sql_function_arg_expr(
                arg,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?,
            operator: operator.clone(),
        },
        BroadSqlFunctionArg::Unnamed(arg) => {
            FunctionArg::Unnamed(lower_broad_sql_function_arg_expr(
                arg,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?)
        }
    })
}

fn lower_broad_sql_function_arg_expr(
    arg: &BroadSqlFunctionArgExpr,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<FunctionArgExpr, LixError> {
    Ok(match arg {
        BroadSqlFunctionArgExpr::Expr(expr) => FunctionArgExpr::Expr(lower_broad_sql_expr(
            expr,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        )?),
        BroadSqlFunctionArgExpr::QualifiedWildcard(object_name) => {
            FunctionArgExpr::QualifiedWildcard(object_name.clone())
        }
        BroadSqlFunctionArgExpr::Wildcard => FunctionArgExpr::Wildcard,
    })
}

fn lower_broad_public_read_table_factor(
    factor: &BroadPublicReadTableFactor,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<TableFactor, LixError> {
    match factor {
        BroadPublicReadTableFactor::Table {
            alias, relation, ..
        } => lower_broad_public_read_relation_factor(
            relation,
            alias.as_ref(),
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        ),
        BroadPublicReadTableFactor::Derived {
            lateral,
            alias,
            subquery,
            ..
        } => Ok(TableFactor::Derived {
            lateral: *lateral,
            subquery: Box::new(lower_broad_public_read_query(
                subquery,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            alias: alias.as_ref().map(lower_broad_public_read_alias),
        }),
        BroadPublicReadTableFactor::NestedJoin {
            alias,
            table_with_joins,
            ..
        } => Ok(TableFactor::NestedJoin {
            table_with_joins: Box::new(lower_broad_public_read_table_with_joins(
                table_with_joins,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
            )?),
            alias: alias.as_ref().map(lower_broad_public_read_alias),
        }),
    }
}

fn lower_broad_public_read_relation_factor(
    relation: &BroadPublicReadRelation,
    alias: Option<&BroadPublicReadAlias>,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<TableFactor, LixError> {
    match relation {
        BroadPublicReadRelation::LoweredPublic(binding) => {
            let Some(source_sql) = build_supported_public_read_surface_sql(
                &binding.descriptor.public_name,
                registry,
                false,
                dialect,
                active_version_id,
                known_live_layouts,
            )?
            else {
                return Ok(table_factor_for_relation_name(
                    &binding.descriptor.public_name,
                    alias,
                ));
            };
            with_current_broad_render_substitution_collector(|collector| {
                Ok(collector.replacement_table_factor(
                    &binding.descriptor.public_name,
                    alias.map(lower_broad_public_read_alias),
                    source_sql,
                ))
            })
        }
        BroadPublicReadRelation::Public(binding) => Ok(table_factor_for_relation_name(
            &binding.descriptor.public_name,
            alias,
        )),
        BroadPublicReadRelation::Internal(relation_name)
        | BroadPublicReadRelation::External(relation_name)
        | BroadPublicReadRelation::Cte(relation_name) => {
            Ok(table_factor_for_relation_name(relation_name, alias))
        }
    }
}

fn lower_broad_public_read_table_set_expr(
    relation: &BroadPublicReadRelation,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<SetExpr, LixError> {
    match relation {
        BroadPublicReadRelation::LoweredPublic(binding) => {
            let Some(source_sql) = build_supported_public_read_surface_sql(
                &binding.descriptor.public_name,
                registry,
                true,
                dialect,
                active_version_id,
                known_live_layouts,
            )?
            else {
                return Ok(table_set_expr_for_relation_name(
                    &binding.descriptor.public_name,
                ));
            };
            with_current_broad_render_substitution_collector(|collector| {
                Ok(SetExpr::Query(Box::new(Query {
                    with: None,
                    body: Box::new(SetExpr::Select(Box::new(Select {
                        select_token: AttachedToken::empty(),
                        distinct: None,
                        top: None,
                        top_before_distinct: false,
                        projection: vec![SelectItem::Wildcard(Default::default())],
                        exclude: None,
                        into: None,
                        from: vec![TableWithJoins {
                            relation: collector.replacement_table_factor(
                                &binding.descriptor.public_name,
                                None,
                                source_sql,
                            ),
                            joins: Vec::new(),
                        }],
                        lateral_views: Vec::new(),
                        prewhere: None,
                        selection: None,
                        group_by: GroupByExpr::Expressions(Vec::new(), Vec::new()),
                        cluster_by: Vec::new(),
                        distribute_by: Vec::new(),
                        sort_by: Vec::new(),
                        having: None,
                        named_window: Vec::new(),
                        qualify: None,
                        window_before_qualify: false,
                        value_table_mode: None,
                        connect_by: None,
                        flavor: SelectFlavor::Standard,
                    }))),
                    order_by: None,
                    limit_clause: None,
                    fetch: None,
                    locks: Vec::new(),
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                    pipe_operators: Vec::new(),
                })))
            })
        }
        BroadPublicReadRelation::Public(binding) => Ok(table_set_expr_for_relation_name(
            &binding.descriptor.public_name,
        )),
        BroadPublicReadRelation::Internal(relation_name)
        | BroadPublicReadRelation::External(relation_name)
        | BroadPublicReadRelation::Cte(relation_name) => {
            Ok(table_set_expr_for_relation_name(relation_name))
        }
    }
}

fn lower_broad_public_read_alias(alias: &BroadPublicReadAlias) -> TableAlias {
    TableAlias {
        explicit: alias.explicit,
        name: Ident::new(&alias.name),
        columns: alias
            .columns
            .iter()
            .cloned()
            .map(sqlparser::ast::TableAliasColumnDef::from_name)
            .collect(),
    }
}

fn table_factor_for_relation_name(
    relation_name: &str,
    alias: Option<&BroadPublicReadAlias>,
) -> TableFactor {
    TableFactor::Table {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(relation_name))]),
        alias: alias.map(lower_broad_public_read_alias),
        args: None,
        with_hints: vec![],
        version: None,
        with_ordinality: false,
        partitions: vec![],
        json_path: None,
        sample: None,
        index_hints: vec![],
    }
}

fn table_set_expr_for_relation_name(relation_name: &str) -> SetExpr {
    SetExpr::Table(Box::new(Table {
        table_name: Some(relation_name.to_string()),
        schema_name: None,
    }))
}

fn lower_broad_public_read_set_operation_kind(
    kind: BroadPublicReadSetOperationKind,
) -> SetOperator {
    match kind {
        BroadPublicReadSetOperationKind::Union => SetOperator::Union,
        BroadPublicReadSetOperationKind::Except => SetOperator::Except,
        BroadPublicReadSetOperationKind::Intersect => SetOperator::Intersect,
        BroadPublicReadSetOperationKind::Minus => SetOperator::Minus,
    }
}

fn lower_broad_public_read_set_quantifier(
    quantifier: Option<BroadPublicReadSetQuantifier>,
) -> SetQuantifier {
    match quantifier {
        Some(BroadPublicReadSetQuantifier::All) => SetQuantifier::All,
        Some(BroadPublicReadSetQuantifier::Distinct) => SetQuantifier::Distinct,
        Some(BroadPublicReadSetQuantifier::ByName) => SetQuantifier::ByName,
        Some(BroadPublicReadSetQuantifier::AllByName) => SetQuantifier::AllByName,
        Some(BroadPublicReadSetQuantifier::DistinctByName) => SetQuantifier::DistinctByName,
        None => SetQuantifier::None,
    }
}

fn build_supported_public_read_surface_sql(
    surface_name: &str,
    registry: &SurfaceRegistry,
    _top_level: bool,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Option<String>, LixError> {
    let Some(surface_binding) = registry.bind_relation_name(surface_name) else {
        return Ok(None);
    };

    match surface_binding.descriptor.surface_family {
        SurfaceFamily::State => build_public_state_surface_sql(
            &surface_binding,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        ),
        SurfaceFamily::Entity => build_entity_surface_sql_for_broad_lowering(
            dialect,
            &surface_binding,
            active_version_id,
            known_live_layouts,
        ),
        SurfaceFamily::Filesystem => build_nested_filesystem_surface_sql(
            dialect,
            active_version_id,
            &surface_binding.descriptor.public_name,
        ),
        SurfaceFamily::Admin => build_public_admin_surface_sql(dialect, &surface_binding),
        SurfaceFamily::Change => {
            build_public_change_surface_sql(dialect, &surface_binding, active_version_id)
        }
    }
}

fn build_public_state_surface_sql(
    surface_binding: &SurfaceBinding,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Option<String>, LixError> {
    let Some(state_scan) = CanonicalStateScan::from_surface_binding(surface_binding.clone()) else {
        return Ok(None);
    };
    let schema_set: BTreeSet<String> = registry
        .registered_state_surface_schema_keys()
        .into_iter()
        .collect();
    let request = EffectiveStateRequest {
        schema_set,
        version_scope: effective_state_version_scope(state_scan.version_scope),
        include_global_overlay: true,
        include_untracked_overlay: true,
        include_tombstones: state_scan.include_tombstones,
        predicate_classes: Vec::new(),
        required_columns: surface_binding
            .descriptor
            .visible_columns
            .iter()
            .chain(surface_binding.descriptor.hidden_columns.iter())
            .cloned()
            .collect(),
    };
    if state_scan.version_scope == VersionScope::ActiveVersion && active_version_id.is_none() {
        return Ok(None);
    }
    build_state_source_sql(
        dialect,
        active_version_id,
        surface_binding,
        &request,
        &[],
        known_live_layouts,
    )
}

fn build_public_admin_surface_sql(
    dialect: SqlDialect,
    surface_binding: &SurfaceBinding,
) -> Result<Option<String>, LixError> {
    let Some(admin_scan) = CanonicalAdminScan::from_surface_binding(surface_binding.clone()) else {
        return Ok(None);
    };
    build_admin_source_sql(admin_scan.kind, dialect).map(Some)
}

fn build_public_change_surface_sql(
    dialect: SqlDialect,
    surface_binding: &SurfaceBinding,
    active_version_id: Option<&str>,
) -> Result<Option<String>, LixError> {
    if CanonicalWorkingChangesScan::from_surface_binding(surface_binding.clone()).is_some() {
        let Some(active_version_id) = active_version_id else {
            return Ok(None);
        };
        return Ok(Some(build_working_changes_public_read_source_sql(
            dialect,
            active_version_id,
        )));
    }
    if CanonicalChangeScan::from_surface_binding(surface_binding.clone()).is_some() {
        return Ok(Some(build_change_source_sql()));
    }
    Ok(None)
}

fn build_entity_surface_sql_for_broad_lowering(
    dialect: SqlDialect,
    surface_binding: &SurfaceBinding,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Option<String>, LixError> {
    let Some(schema_key) = surface_binding.implicit_overrides.fixed_schema_key.clone() else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "public-surface lowering requires fixed schema binding for '{}'",
                surface_binding.descriptor.public_name
            ),
        });
    };
    if builtin_schema_definition(&schema_key).is_none()
        && !known_live_layouts.contains_key(&schema_key)
    {
        return Ok(None);
    }
    let Some(state_scan) = CanonicalStateScan::from_surface_binding(surface_binding.clone()) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "public-surface lowering could not build canonical state scan for '{}'",
                surface_binding.descriptor.public_name
            ),
        });
    };
    let request = EffectiveStateRequest {
        schema_set: BTreeSet::from([schema_key]),
        version_scope: effective_state_version_scope(state_scan.version_scope),
        include_global_overlay: true,
        include_untracked_overlay: true,
        include_tombstones: state_scan.include_tombstones,
        predicate_classes: Vec::new(),
        required_columns: surface_binding
            .descriptor
            .visible_columns
            .iter()
            .chain(surface_binding.descriptor.hidden_columns.iter())
            .cloned()
            .collect(),
    };
    if state_scan.version_scope == VersionScope::ActiveVersion && active_version_id.is_none() {
        return Ok(None);
    }
    Ok(Some(
        build_entity_source_sql(
            dialect,
            active_version_id,
            surface_binding,
            &request,
            &[],
            known_live_layouts,
        )?
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "public-surface lowering could not lower entity surface '{}'",
                surface_binding.descriptor.public_name
            ),
        })?,
    ))
}
