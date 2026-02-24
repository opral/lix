use super::types::{MutationRow, UpdateValidationPlan};
use crate::engine::sql::planning::rewrite_engine::{expr_references_column_name, ColumnReferenceOptions};
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
    parse_active_version_snapshot,
};
use crate::LixError;
use sqlparser::ast::{
    BinaryOperator, Expr, FromTable, ObjectName, ObjectNamePart, Query, Select, SetExpr, Statement,
    TableFactor, TableObject, TableWithJoins, Update, UpdateTableFromKind,
};

#[cfg(test)]
pub(crate) fn should_refresh_file_cache_for_sql(sql: &str) -> bool {
    let Ok(statements) = crate::engine::sql::planning::rewrite_engine::parse_sql_statements(sql) else {
        return false;
    };
    should_refresh_file_cache_for_statements(&statements)
}

pub(crate) fn should_refresh_file_cache_for_statements(statements: &[Statement]) -> bool {
    statements
        .iter()
        .any(statement_targets_file_cache_refresh_table)
}

#[cfg(test)]
pub(crate) fn is_query_only_sql(sql: &str) -> bool {
    let Ok(statements) = crate::engine::sql::planning::rewrite_engine::parse_sql_statements(sql) else {
        return false;
    };
    is_query_only_statements(&statements)
}

pub(crate) fn is_query_only_statements(statements: &[Statement]) -> bool {
    !statements.is_empty()
        && statements
            .iter()
            .all(|statement| matches!(statement, Statement::Query(_)))
}

fn statement_targets_file_cache_refresh_table(statement: &Statement) -> bool {
    statement_targets_table_name(statement, "lix_state")
        || statement_targets_table_name(statement, "lix_state_by_version")
}

pub(crate) fn should_invalidate_installed_plugins_cache_for_sql(sql: &str) -> bool {
    let Ok(statements) = crate::engine::sql::planning::rewrite_engine::parse_sql_statements(sql) else {
        return false;
    };
    should_invalidate_installed_plugins_cache_for_statements(&statements)
}

pub(crate) fn should_invalidate_installed_plugins_cache_for_statements(
    statements: &[Statement],
) -> bool {
    statements
        .iter()
        .any(|statement| statement_targets_table_name(statement, "lix_internal_plugin"))
}

pub(crate) fn statement_targets_table_name(statement: &Statement, table_name: &str) -> bool {
    match statement {
        Statement::Insert(insert) => table_object_targets_table_name(&insert.table, table_name),
        Statement::Update(update) => table_with_joins_targets_table_name(&update.table, table_name),
        Statement::Delete(delete) => {
            let tables = match &delete.from {
                FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
            };
            tables
                .iter()
                .any(|table| table_with_joins_targets_table_name(table, table_name))
        }
        _ => false,
    }
}

pub(crate) fn table_object_targets_table_name(table: &TableObject, table_name: &str) -> bool {
    let TableObject::TableName(name) = table else {
        return false;
    };
    object_name_targets_table_name(name, table_name)
}

pub(crate) fn table_with_joins_targets_table_name(
    table: &TableWithJoins,
    table_name: &str,
) -> bool {
    let TableFactor::Table { name, .. } = &table.relation else {
        return false;
    };
    object_name_targets_table_name(name, table_name)
}

pub(crate) fn object_name_targets_table_name(name: &ObjectName, table_name: &str) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.eq_ignore_ascii_case(table_name))
        .unwrap_or(false)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileReadMaterializationScope {
    ActiveVersionOnly,
    AllVersions,
}

#[cfg(test)]
pub(crate) fn file_read_materialization_scope_for_sql(
    sql: &str,
) -> Option<FileReadMaterializationScope> {
    let Ok(statements) = crate::engine::sql::planning::rewrite_engine::parse_sql_statements(sql) else {
        return None;
    };
    file_read_materialization_scope_for_statements(&statements)
}

pub(crate) fn file_read_materialization_scope_for_statements(
    statements: &[Statement],
) -> Option<FileReadMaterializationScope> {
    let mut scope = None;
    for statement in statements {
        let Some(statement_scope) = file_read_materialization_scope_for_statement(statement) else {
            continue;
        };
        match statement_scope {
            FileReadMaterializationScope::AllVersions => {
                return Some(FileReadMaterializationScope::AllVersions);
            }
            FileReadMaterializationScope::ActiveVersionOnly => {
                scope.get_or_insert(FileReadMaterializationScope::ActiveVersionOnly);
            }
        }
    }
    scope
}

#[cfg(test)]
pub(crate) fn file_history_read_materialization_required_for_sql(sql: &str) -> bool {
    let Ok(statements) = crate::engine::sql::planning::rewrite_engine::parse_sql_statements(sql) else {
        return false;
    };
    file_history_read_materialization_required_for_statements(&statements)
}

pub(crate) fn file_history_read_materialization_required_for_statements(
    statements: &[Statement],
) -> bool {
    statements
        .iter()
        .any(file_history_read_materialization_required_for_statement)
}

fn file_history_read_materialization_required_for_statement(statement: &Statement) -> bool {
    statement_reads_table_name(statement, "lix_file_history")
}

fn file_read_materialization_scope_for_statement(
    statement: &Statement,
) -> Option<FileReadMaterializationScope> {
    let mentions_by_version = statement_reads_table_name(statement, "lix_file_by_version");
    if mentions_by_version {
        return Some(FileReadMaterializationScope::AllVersions);
    }
    if statement_reads_table_name(statement, "lix_file") {
        return Some(FileReadMaterializationScope::ActiveVersionOnly);
    }
    None
}

fn statement_reads_table_name(statement: &Statement, table_name: &str) -> bool {
    match statement {
        Statement::Query(query) => query_mentions_table_name(query, table_name),
        Statement::Insert(insert) => insert
            .source
            .as_deref()
            .is_some_and(|query| query_mentions_table_name(query, table_name)),
        Statement::Update(update) => {
            let target_matches = table_with_joins_mentions_table_name(&update.table, table_name);
            (target_matches && update_references_data_column(update))
                || update.from.as_ref().is_some_and(|from| match from {
                    UpdateTableFromKind::BeforeSet(from) | UpdateTableFromKind::AfterSet(from) => {
                        from.iter()
                            .any(|table| table_with_joins_mentions_table_name(table, table_name))
                    }
                })
                || update
                    .selection
                    .as_ref()
                    .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
                || update
                    .assignments
                    .iter()
                    .any(|assignment| expr_mentions_table_name(&assignment.value, table_name))
        }
        Statement::Delete(delete) => {
            delete.using.as_ref().is_some_and(|tables| {
                tables
                    .iter()
                    .any(|table| table_with_joins_mentions_table_name(table, table_name))
            }) || delete
                .selection
                .as_ref()
                .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
        }
        _ => false,
    }
}

fn update_references_data_column(update: &Update) -> bool {
    update
        .selection
        .as_ref()
        .is_some_and(expr_references_data_column)
        || update
            .assignments
            .iter()
            .any(|assignment| expr_references_data_column(&assignment.value))
}

fn expr_references_data_column(expr: &Expr) -> bool {
    expr_references_column_name(
        expr,
        "data",
        ColumnReferenceOptions {
            include_from_derived_subqueries: true,
        },
    )
}

fn query_mentions_table_name(query: &Query, table_name: &str) -> bool {
    if query_set_expr_mentions_table_name(query.body.as_ref(), table_name) {
        return true;
    }

    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            if query_mentions_table_name(&cte.query, table_name) {
                return true;
            }
        }
    }

    if query
        .order_by
        .as_ref()
        .is_some_and(|order_by| order_by_mentions_table_name(order_by, table_name))
    {
        return true;
    }

    if query
        .limit_clause
        .as_ref()
        .is_some_and(|limit_clause| limit_clause_mentions_table_name(limit_clause, table_name))
    {
        return true;
    }

    if query
        .fetch
        .as_ref()
        .and_then(|fetch| fetch.quantity.as_ref())
        .is_some_and(|quantity| expr_mentions_table_name(quantity, table_name))
    {
        return true;
    }

    false
}

fn query_set_expr_mentions_table_name(expr: &SetExpr, table_name: &str) -> bool {
    match expr {
        SetExpr::Select(select) => select_mentions_table_name(select, table_name),
        SetExpr::Query(query) => query_mentions_table_name(query, table_name),
        SetExpr::SetOperation { left, right, .. } => {
            query_set_expr_mentions_table_name(left.as_ref(), table_name)
                || query_set_expr_mentions_table_name(right.as_ref(), table_name)
        }
        SetExpr::Values(values) => values
            .rows
            .iter()
            .flatten()
            .any(|expr| expr_mentions_table_name(expr, table_name)),
        SetExpr::Insert(statement)
        | SetExpr::Update(statement)
        | SetExpr::Delete(statement)
        | SetExpr::Merge(statement) => statement_reads_table_name(statement, table_name),
        SetExpr::Table(table) => table
            .table_name
            .as_ref()
            .is_some_and(|name| name.eq_ignore_ascii_case(table_name)),
    }
}

fn select_mentions_table_name(select: &Select, table_name: &str) -> bool {
    if select
        .from
        .iter()
        .any(|table| table_with_joins_mentions_table_name(table, table_name))
    {
        return true;
    }

    if select
        .projection
        .iter()
        .any(|item| select_item_mentions_table_name(item, table_name))
    {
        return true;
    }

    if select
        .prewhere
        .as_ref()
        .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
    {
        return true;
    }

    if select
        .selection
        .as_ref()
        .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
    {
        return true;
    }

    if group_by_expr_mentions_table_name(&select.group_by, table_name) {
        return true;
    }

    if select
        .cluster_by
        .iter()
        .any(|expr| expr_mentions_table_name(expr, table_name))
    {
        return true;
    }

    if select
        .distribute_by
        .iter()
        .any(|expr| expr_mentions_table_name(expr, table_name))
    {
        return true;
    }

    if select
        .sort_by
        .iter()
        .any(|order_by_expr| order_by_expr_mentions_table_name(order_by_expr, table_name))
    {
        return true;
    }

    if select
        .having
        .as_ref()
        .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
    {
        return true;
    }

    if select
        .qualify
        .as_ref()
        .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
    {
        return true;
    }

    if select.connect_by.as_ref().is_some_and(|connect_by| {
        expr_mentions_table_name(&connect_by.condition, table_name)
            || connect_by
                .relationships
                .iter()
                .any(|expr| expr_mentions_table_name(expr, table_name))
    }) {
        return true;
    }

    false
}

fn table_with_joins_mentions_table_name(table: &TableWithJoins, table_name: &str) -> bool {
    if table_factor_mentions_table_name(&table.relation, table_name) {
        return true;
    }

    table.joins.iter().any(|join| {
        table_factor_mentions_table_name(&join.relation, table_name)
            || join_operator_mentions_table_name(&join.join_operator, table_name)
    })
}

fn table_factor_mentions_table_name(table: &TableFactor, table_name: &str) -> bool {
    match table {
        TableFactor::Table { name, .. } => object_name_matches_table_name(name, table_name),
        TableFactor::Derived { subquery, .. } => query_mentions_table_name(subquery, table_name),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => table_with_joins_mentions_table_name(table_with_joins, table_name),
        _ => false,
    }
}

fn select_item_mentions_table_name(item: &sqlparser::ast::SelectItem, table_name: &str) -> bool {
    match item {
        sqlparser::ast::SelectItem::UnnamedExpr(expr)
        | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
            expr_mentions_table_name(expr, table_name)
        }
        sqlparser::ast::SelectItem::QualifiedWildcard(
            sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(expr),
            _,
        ) => expr_mentions_table_name(expr, table_name),
        _ => false,
    }
}

fn group_by_expr_mentions_table_name(
    group_by: &sqlparser::ast::GroupByExpr,
    table_name: &str,
) -> bool {
    match group_by {
        sqlparser::ast::GroupByExpr::All(_) => false,
        sqlparser::ast::GroupByExpr::Expressions(expressions, _) => expressions
            .iter()
            .any(|expr| expr_mentions_table_name(expr, table_name)),
    }
}

fn order_by_mentions_table_name(order_by: &sqlparser::ast::OrderBy, table_name: &str) -> bool {
    match &order_by.kind {
        sqlparser::ast::OrderByKind::All(_) => false,
        sqlparser::ast::OrderByKind::Expressions(expressions) => expressions
            .iter()
            .any(|expr| order_by_expr_mentions_table_name(expr, table_name)),
    }
}

fn order_by_expr_mentions_table_name(
    order_by_expr: &sqlparser::ast::OrderByExpr,
    table_name: &str,
) -> bool {
    if expr_mentions_table_name(&order_by_expr.expr, table_name) {
        return true;
    }

    order_by_expr.with_fill.as_ref().is_some_and(|with_fill| {
        with_fill
            .from
            .as_ref()
            .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
            || with_fill
                .to
                .as_ref()
                .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
            || with_fill
                .step
                .as_ref()
                .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
    })
}

fn limit_clause_mentions_table_name(
    limit_clause: &sqlparser::ast::LimitClause,
    table_name: &str,
) -> bool {
    match limit_clause {
        sqlparser::ast::LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            limit
                .as_ref()
                .is_some_and(|expr| expr_mentions_table_name(expr, table_name))
                || offset
                    .as_ref()
                    .is_some_and(|offset| expr_mentions_table_name(&offset.value, table_name))
                || limit_by
                    .iter()
                    .any(|expr| expr_mentions_table_name(expr, table_name))
        }
        sqlparser::ast::LimitClause::OffsetCommaLimit { offset, limit } => {
            expr_mentions_table_name(offset, table_name)
                || expr_mentions_table_name(limit, table_name)
        }
    }
}

fn join_operator_mentions_table_name(
    join_operator: &sqlparser::ast::JoinOperator,
    table_name: &str,
) -> bool {
    let (match_condition, constraint) = match join_operator {
        sqlparser::ast::JoinOperator::AsOf {
            match_condition,
            constraint,
        } => (Some(match_condition), Some(constraint)),
        sqlparser::ast::JoinOperator::Join(constraint)
        | sqlparser::ast::JoinOperator::Inner(constraint)
        | sqlparser::ast::JoinOperator::Left(constraint)
        | sqlparser::ast::JoinOperator::LeftOuter(constraint)
        | sqlparser::ast::JoinOperator::Right(constraint)
        | sqlparser::ast::JoinOperator::RightOuter(constraint)
        | sqlparser::ast::JoinOperator::FullOuter(constraint)
        | sqlparser::ast::JoinOperator::CrossJoin(constraint)
        | sqlparser::ast::JoinOperator::Semi(constraint)
        | sqlparser::ast::JoinOperator::LeftSemi(constraint)
        | sqlparser::ast::JoinOperator::RightSemi(constraint)
        | sqlparser::ast::JoinOperator::Anti(constraint)
        | sqlparser::ast::JoinOperator::LeftAnti(constraint)
        | sqlparser::ast::JoinOperator::RightAnti(constraint)
        | sqlparser::ast::JoinOperator::StraightJoin(constraint) => (None, Some(constraint)),
        sqlparser::ast::JoinOperator::CrossApply | sqlparser::ast::JoinOperator::OuterApply => {
            (None, None)
        }
    };

    match_condition.is_some_and(|expr| expr_mentions_table_name(expr, table_name))
        || constraint
            .is_some_and(|constraint| join_constraint_mentions_table_name(constraint, table_name))
}

fn join_constraint_mentions_table_name(
    constraint: &sqlparser::ast::JoinConstraint,
    table_name: &str,
) -> bool {
    match constraint {
        sqlparser::ast::JoinConstraint::On(expr) => expr_mentions_table_name(expr, table_name),
        _ => false,
    }
}

fn expr_mentions_table_name(expr: &Expr, table_name: &str) -> bool {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            expr_mentions_table_name(left, table_name)
                || expr_mentions_table_name(right, table_name)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => expr_mentions_table_name(expr, table_name),
        Expr::InList { expr, list, .. } => {
            expr_mentions_table_name(expr, table_name)
                || list
                    .iter()
                    .any(|item| expr_mentions_table_name(item, table_name))
        }
        Expr::InSubquery { expr, subquery, .. } => {
            expr_mentions_table_name(expr, table_name)
                || query_mentions_table_name(subquery, table_name)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_mentions_table_name(expr, table_name)
                || expr_mentions_table_name(low, table_name)
                || expr_mentions_table_name(high, table_name)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            expr_mentions_table_name(expr, table_name)
                || expr_mentions_table_name(pattern, table_name)
        }
        Expr::InUnnest {
            expr, array_expr, ..
        } => {
            expr_mentions_table_name(expr, table_name)
                || expr_mentions_table_name(array_expr, table_name)
        }
        Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
            expr_mentions_table_name(left, table_name)
                || expr_mentions_table_name(right, table_name)
        }
        Expr::Exists { subquery, .. } | Expr::Subquery(subquery) => {
            query_mentions_table_name(subquery, table_name)
        }
        Expr::Function(function) => match &function.args {
            sqlparser::ast::FunctionArguments::List(list) => {
                list.args.iter().any(|arg| match arg {
                    sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(expr),
                    ) => expr_mentions_table_name(expr, table_name),
                    sqlparser::ast::FunctionArg::Named { arg, .. }
                    | sqlparser::ast::FunctionArg::ExprNamed { arg, .. } => match arg {
                        sqlparser::ast::FunctionArgExpr::Expr(expr) => {
                            expr_mentions_table_name(expr, table_name)
                        }
                        _ => false,
                    },
                    _ => false,
                })
            }
            _ => false,
        },
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand
                .as_ref()
                .is_some_and(|operand| expr_mentions_table_name(operand, table_name))
                || conditions.iter().any(|condition| {
                    expr_mentions_table_name(&condition.condition, table_name)
                        || expr_mentions_table_name(&condition.result, table_name)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|value| expr_mentions_table_name(value, table_name))
        }
        Expr::Tuple(items) => items
            .iter()
            .any(|item| expr_mentions_table_name(item, table_name)),
        _ => false,
    }
}

fn object_name_matches_table_name(name: &ObjectName, table_name: &str) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.eq_ignore_ascii_case(table_name))
        .unwrap_or(false)
}

pub(crate) fn active_version_from_mutations(
    mutations: &[MutationRow],
) -> Result<Option<String>, LixError> {
    for mutation in mutations.iter().rev() {
        if !mutation.untracked {
            continue;
        }
        if mutation.schema_key != active_version_schema_key()
            || mutation.file_id != active_version_file_id()
            || mutation.version_id != active_version_storage_version_id()
        {
            continue;
        }

        let snapshot = mutation.snapshot_content.as_ref().ok_or_else(|| LixError {
            message: "active version mutation is missing snapshot_content".to_string(),
        })?;
        let snapshot_content = serde_json::to_string(snapshot).map_err(|error| LixError {
            message: format!("active version mutation snapshot_content invalid JSON: {error}"),
        })?;
        return parse_active_version_snapshot(&snapshot_content).map(Some);
    }

    Ok(None)
}

pub(crate) fn active_version_from_update_validations(
    plans: &[UpdateValidationPlan],
) -> Result<Option<String>, LixError> {
    for plan in plans.iter().rev() {
        if !plan
            .table
            .eq_ignore_ascii_case("lix_internal_state_untracked")
        {
            continue;
        }
        if !where_clause_targets_active_version(plan.where_clause.as_ref()) {
            continue;
        }
        let Some(snapshot) = plan.snapshot_content.as_ref() else {
            continue;
        };

        let snapshot_content = serde_json::to_string(snapshot).map_err(|error| LixError {
            message: format!("active version update snapshot_content invalid JSON: {error}"),
        })?;
        return parse_active_version_snapshot(&snapshot_content).map(Some);
    }

    Ok(None)
}

pub(crate) fn where_clause_targets_active_version(where_clause: Option<&Expr>) -> bool {
    let Some(where_clause) = where_clause else {
        return false;
    };
    let Some(schema_keys) = schema_keys_from_expr(where_clause) else {
        return false;
    };
    schema_keys
        .iter()
        .any(|value| value.eq_ignore_ascii_case(active_version_schema_key()))
}

fn schema_keys_from_expr(expr: &Expr) -> Option<Vec<String>> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if expr_is_schema_key_column(left) {
                return schema_key_literal_value(right).map(|value| vec![value]);
            }
            if expr_is_schema_key_column(right) {
                return schema_key_literal_value(left).map(|value| vec![value]);
            }
            None
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => match (schema_keys_from_expr(left), schema_keys_from_expr(right)) {
            (Some(left), Some(right)) => {
                let intersection = intersect_strings(&left, &right);
                if intersection.is_empty() {
                    None
                } else {
                    Some(intersection)
                }
            }
            (Some(keys), None) | (None, Some(keys)) => Some(keys),
            (None, None) => None,
        },
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => match (schema_keys_from_expr(left), schema_keys_from_expr(right)) {
            (Some(left), Some(right)) => Some(union_strings(&left, &right)),
            _ => None,
        },
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            if !expr_is_schema_key_column(expr) {
                return None;
            }
            let mut values = Vec::with_capacity(list.len());
            for item in list {
                let value = schema_key_literal_value(item)?;
                values.push(value);
            }
            if values.is_empty() {
                None
            } else {
                Some(dedup_strings(values))
            }
        }
        Expr::Nested(inner) => schema_keys_from_expr(inner),
        _ => None,
    }
}

fn expr_is_schema_key_column(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case("schema_key"),
        Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|ident| ident.value.eq_ignore_ascii_case("schema_key"))
            .unwrap_or(false),
        _ => false,
    }
}

fn schema_key_literal_value(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(value) => value.value.clone().into_string(),
        Expr::Identifier(ident) if ident.quote_style == Some('"') => Some(ident.value.clone()),
        _ => None,
    }
}

fn dedup_strings(values: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    for value in values {
        if !out.contains(&value) {
            out.push(value);
        }
    }
    out
}

fn union_strings(left: &[String], right: &[String]) -> Vec<String> {
    let mut out = left.to_vec();
    for value in right {
        if !out.contains(value) {
            out.push(value.clone());
        }
    }
    out
}

fn intersect_strings(left: &[String], right: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for value in left {
        if right.contains(value) && !out.contains(value) {
            out.push(value.clone());
        }
    }
    out
}
