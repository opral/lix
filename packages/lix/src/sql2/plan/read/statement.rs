use crate::{Value, sql2};
use datafusion::sql::parser::Statement as DataFusionStatement;
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident,
    LimitClause, OrderByKind, Query, Select, SelectFlavor, SelectItem, SetExpr,
    Statement as SqlStatement, TableAlias, TableFactor, UnaryOperator, Value as SqlValue, Visit,
    Visitor,
};
use std::collections::BTreeSet;
use std::ops::ControlFlow;
/// Returns true only when SQL directly delivers one file's bytes to the
/// caller. Materializing `data` inside an aggregate, join, filter, or derived
/// expression is not acknowledgement: the caller did not receive those bytes
/// and must not gain the ability to delete rows that only existed there.
///
/// This intentionally recognizes a narrow, predictable MVP surface. False
/// negatives merely preserve an omitted row; false positives can lose one.
pub(crate) fn is_acknowledgeable_file_content_read(
    statement: &DataFusionStatement,
    params: &[Value],
) -> bool {
    let Some(point_read) = simple_point_read(statement) else {
        return false;
    };

    if !point_read.select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::UnnamedExpr(expression)
                | SelectItem::ExprWithAlias {
                    expr: expression,
                    ..
                } if direct_column_name(expression).as_deref() == Some("content")
        )
    }) {
        return false;
    }

    let selection = point_read
        .select
        .selection
        .as_ref()
        .expect("simple point read requires a predicate");
    let mut equality_columns = BTreeSet::new();
    if !collect_literal_equalities(selection, &mut equality_columns, params) {
        return false;
    }
    match point_read.table_name.as_str() {
        "lix_file" => {
            equality_columns.len() == 1
                && (equality_columns.contains("id") || equality_columns.contains("path"))
        }
        _ => false,
    }
}

struct SimplePointRead<'a> {
    select: &'a Select,
    table_name: String,
    exact_table_shape: bool,
}

struct SimpleSingleTableSelect<'a> {
    query: &'a Query,
    select: &'a Select,
    table_identifier: &'a Ident,
    table_name: String,
    unqualified_unquoted_table: bool,
    alias: Option<&'a TableAlias>,
}

fn simple_single_table_select(
    statement: &DataFusionStatement,
) -> Option<SimpleSingleTableSelect<'_>> {
    let DataFusionStatement::Statement(statement) = statement else {
        return None;
    };
    let SqlStatement::Query(query) = statement.as_ref() else {
        return None;
    };
    if query.with.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return None;
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if select.flavor != SelectFlavor::Standard
        || !select.optimizer_hints.is_empty()
        || select.distinct.is_some()
        || select.select_modifiers.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !group_by_is_empty(&select.group_by)
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return None;
    }

    let [from] = select.from.as_slice() else {
        return None;
    };
    if !from.joins.is_empty() {
        return None;
    }
    let TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
        ..
    } = &from.relation
    else {
        return None;
    };
    if args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
    {
        return None;
    }
    let table_identifier = name.0.last().and_then(|part| part.as_ident())?;
    let table_name = table_identifier.value.to_ascii_lowercase();

    Some(SimpleSingleTableSelect {
        query,
        select,
        table_identifier,
        table_name,
        unqualified_unquoted_table: name.0.len() == 1 && table_identifier.quote_style.is_none(),
        alias: alias.as_ref(),
    })
}

fn simple_point_read(statement: &DataFusionStatement) -> Option<SimplePointRead<'_>> {
    let simple = simple_single_table_select(statement)?;
    if simple.query.order_by.is_some()
        || !point_read_limit_is_safe(simple.query.limit_clause.as_ref())
        || simple.query.fetch.is_some()
    {
        return None;
    }

    simple.select.selection.as_ref()?;
    Some(SimplePointRead {
        select: simple.select,
        table_name: simple.table_name,
        exact_table_shape: simple.unqualified_unquoted_table
            && simple.alias.is_none()
            && simple.query.limit_clause.is_none(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LateMaterializedLixFileContentRead {
    pub(crate) statement: Box<DataFusionStatement>,
    pub(crate) data_column_index: usize,
    pub(crate) projection: LateLixFileProjection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LateLixFileProjection {
    Content,
    OctetLength,
    Substring { start: i64, length: u64 },
}

impl LateLixFileProjection {
    pub(crate) fn acknowledges_content(&self) -> bool {
        matches!(self, Self::Content | Self::Substring { .. })
    }
}

/// Defers an unchanged `lix_file.content` projection until DataFusion has applied
/// metadata predicates, ordering, and limits. This keeps SQL semantics in
/// DataFusion while preventing large file bytes from entering Arrow at all.
pub(crate) fn late_materialized_lix_file_content_read(
    statement: &DataFusionStatement,
    params: &[Value],
) -> Option<LateMaterializedLixFileContentRead> {
    let simple = simple_single_table_select(statement)?;
    if simple.table_name != "lix_file"
        || !simple.unqualified_unquoted_table
        || simple.alias.is_some_and(|alias| !alias.columns.is_empty())
    {
        return None;
    }
    let qualifier = simple
        .alias
        .map_or(simple.table_identifier, |alias| &alias.name)
        .clone();

    let mut statement = statement.clone();
    let DataFusionStatement::Statement(sql_statement) = &mut statement else {
        return None;
    };
    let SqlStatement::Query(query) = sql_statement.as_mut() else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_mut() else {
        return None;
    };

    let mut data_column_index = None;
    let mut data_output_name = None;
    let mut data_projection = None;
    let mut replacement = None;
    let mut removed_parameters = Vec::new();
    for (index, item) in select.projection.iter().enumerate() {
        let expression = projection_source_expression(item)?;
        if let Some((projection, output_name, path_expression, projection_parameters)) =
            replaceable_lix_file_content_projection(item, expression, &qualifier, params)
        {
            if data_column_index.is_some() {
                return None;
            }
            data_column_index = Some(index);
            data_output_name = Some(output_name.value.to_ascii_lowercase());
            data_projection = Some(projection);
            replacement = Some((path_expression, output_name));
            removed_parameters.extend(projection_parameters);
        } else if direct_projection_identifier(expression).is_none()
            || expression_mentions_column(expression, "content")
        {
            return None;
        }
    }
    let data_column_index = data_column_index?;
    let data_output_name = data_output_name?;
    let data_projection = data_projection?;
    let (path_expression, output_name) = replacement?;
    select.projection[data_column_index] = SelectItem::ExprWithAlias {
        expr: path_expression,
        alias: output_name,
    };

    if select
        .selection
        .as_ref()
        .is_some_and(|selection| expression_mentions_column(selection, "content"))
    {
        return None;
    }
    if let Some(order_by) = &query.order_by {
        if order_by.interpolate.is_some() {
            return None;
        }
        let OrderByKind::Expressions(expressions) = &order_by.kind else {
            return None;
        };
        if expressions.iter().any(|order| {
            order.with_fill.is_some()
                || expression_mentions_column(&order.expr, "content")
                || direct_column_name(&order.expr)
                    .is_none_or(|column| column == data_output_name)
        }) {
            return None;
        }
    }

    // The SQL executor binds positional parameters by the highest placeholder
    // number in the rewritten statement. SUBSTRING's slice arguments are
    // removed from its SELECT expression, but still occupy their original
    // positions in the caller's parameter list. Since these arguments were
    // validated as integers, non-null checks preserve those positions without
    // changing the selected rows.
    for parameter in removed_parameters {
        let predicate = Expr::IsNotNull(Box::new(parameter));
        select.selection = Some(match select.selection.take() {
            Some(selection) => Expr::BinaryOp {
                left: Box::new(selection),
                op: BinaryOperator::And,
                right: Box::new(predicate),
            },
            None => predicate,
        });
    }

    Some(LateMaterializedLixFileContentRead {
        statement: Box::new(statement),
        data_column_index,
        projection: data_projection,
    })
}

fn projection_source_expression(item: &SelectItem) -> Option<&Expr> {
    match item {
        SelectItem::UnnamedExpr(expression)
        | SelectItem::ExprWithAlias {
            expr: expression, ..
        } => Some(expression),
        SelectItem::QualifiedWildcard(..)
        | SelectItem::Wildcard(..)
        | SelectItem::ExprWithAliases { .. } => None,
    }
}

fn replaceable_lix_file_content_projection(
    item: &SelectItem,
    expression: &Expr,
    qualifier: &Ident,
    params: &[Value],
) -> Option<(LateLixFileProjection, Ident, Expr, Vec<Expr>)> {
    let projection = if direct_file_content_path_expression(expression, qualifier).is_some() {
        LateLixFileProjection::Content
    } else if is_octet_length_of_content(expression, qualifier) {
        LateLixFileProjection::OctetLength
    } else if let Some((start, length)) = substring_of_content(expression, qualifier, params) {
        LateLixFileProjection::Substring { start, length }
    } else {
        return None;
    };
    let output_name = match item {
        SelectItem::ExprWithAlias { alias, .. } => alias.clone(),
        SelectItem::UnnamedExpr(_) => match &projection {
            LateLixFileProjection::Content => direct_projection_identifier(expression)?.clone(),
            LateLixFileProjection::OctetLength | LateLixFileProjection::Substring { .. } => {
                Ident::with_quote('"', expression.to_string())
            }
        },
        SelectItem::QualifiedWildcard(..)
        | SelectItem::Wildcard(..)
        | SelectItem::ExprWithAliases { .. } => return None,
    };
    let content_expression = match expression {
        Expr::Function(function) if matches!(projection, LateLixFileProjection::OctetLength) => {
            let FunctionArguments::List(arguments) = &function.args else {
                return None;
            };
            let [FunctionArg::Unnamed(FunctionArgExpr::Expr(content))] =
                arguments.args.as_slice()
            else {
                return None;
            };
            content
        }
        Expr::Substring { expr, .. } => expr.as_ref(),
        _ => expression,
    };
    let path_expression = direct_file_content_path_expression(content_expression, qualifier)?;
    let preserved_parameters = match expression {
        Expr::Substring {
            substring_from: Some(start),
            substring_for: Some(length),
            ..
        } if matches!(projection, LateLixFileProjection::Substring { .. }) => {
            [start.as_ref(), length.as_ref()]
                .into_iter()
                .filter(|expression| {
                    matches!(
                        expression,
                        Expr::Value(value)
                            if matches!(&value.value, SqlValue::Placeholder(_))
                    )
                })
                .cloned()
                .collect()
        }
        _ => Vec::new(),
    };
    Some((projection, output_name, path_expression, preserved_parameters))
}

fn is_octet_length_of_content(expression: &Expr, qualifier: &Ident) -> bool {
    let Expr::Function(function) = expression else {
        return false;
    };
    let [name] = function.name.0.as_slice() else {
        return false;
    };
    let Some(name) = name.as_ident() else {
        return false;
    };
    if !identifier_matches(name, "octet_length")
        || function.uses_odbc_syntax
        || function.parameters != FunctionArguments::None
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return false;
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return false;
    };
    if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
        return false;
    }
    let [FunctionArg::Unnamed(FunctionArgExpr::Expr(content))] = arguments.args.as_slice() else {
        return false;
    };
    direct_file_content_path_expression(content, qualifier).is_some()
}

fn substring_of_content(
    expression: &Expr,
    qualifier: &Ident,
    params: &[Value],
) -> Option<(i64, u64)> {
    let Expr::Substring {
        expr,
        substring_from: Some(start),
        substring_for: Some(length),
        ..
    } = expression
    else {
        return None;
    };
    direct_file_content_path_expression(expr, qualifier)?;
    let start = integer_expression(start, params)?;
    let length = integer_expression(length, params)?;
    Some((start, u64::try_from(length).ok()?))
}

fn integer_expression(expression: &Expr, params: &[Value]) -> Option<i64> {
    match expression {
        Expr::Value(value) => match &value.value {
            SqlValue::Number(number, _) => number.parse::<i64>().ok(),
            SqlValue::Placeholder(placeholder) => {
                let index = placeholder.strip_prefix('$')?.parse::<usize>().ok()?.checked_sub(1)?;
                match params.get(index)? {
                    Value::Integer(value) => Some(*value),
                    _ => None,
                }
            }
            _ => None,
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match expr.as_ref() {
            Expr::Value(value) => match &value.value {
                SqlValue::Number(number, _) => number.parse::<i64>().ok()?.checked_neg(),
                _ => None,
            },
            _ => None,
        },
        _ => None,
    }
}

fn direct_file_content_path_expression(expression: &Expr, qualifier: &Ident) -> Option<Expr> {
    match expression {
        Expr::Identifier(identifier) if identifier_matches(identifier, "content") => {
            let mut path = identifier.clone();
            path.value = "path".to_string();
            Some(Expr::Identifier(path))
        }
        Expr::CompoundIdentifier(identifiers) => {
            let [expression_qualifier, identifier] = identifiers.as_slice() else {
                return None;
            };
            if !identifiers_match(expression_qualifier, qualifier)
                || !identifier_matches(identifier, "content")
            {
                return None;
            }
            let mut identifiers = identifiers.clone();
            identifiers.last_mut()?.value = "path".to_string();
            Some(Expr::CompoundIdentifier(identifiers))
        }
        _ => None,
    }
}

fn direct_projection_identifier(expression: &Expr) -> Option<&Ident> {
    match expression {
        Expr::Identifier(identifier) => Some(identifier),
        Expr::CompoundIdentifier(identifiers) => identifiers.last(),
        _ => None,
    }
}

fn identifier_matches(identifier: &Ident, expected: &str) -> bool {
    if identifier.quote_style.is_some() {
        identifier.value == expected
    } else {
        identifier.value.eq_ignore_ascii_case(expected)
    }
}

fn identifiers_match(left: &Ident, right: &Ident) -> bool {
    if left.quote_style.is_some() || right.quote_style.is_some() {
        left.quote_style == right.quote_style && left.value == right.value
    } else {
        left.value.eq_ignore_ascii_case(&right.value)
    }
}

fn expression_mentions_column(expression: &Expr, column: &str) -> bool {
    let mut visitor = ColumnReferenceVisitor {
        column,
        found: false,
    };
    let _ = expression.visit(&mut visitor);
    visitor.found
}

struct ColumnReferenceVisitor<'a> {
    column: &'a str,
    found: bool,
}

impl Visitor for ColumnReferenceVisitor<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<Self::Break> {
        if direct_column_name(expression).as_deref() == Some(self.column) {
            self.found = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ExactFilesystemRead {
    RootFileListing,
    RootDirectoryListing,
    Point(sql2::ExactLixFileReadSelector, sql2::ExactLixFileReadColumn),
    PathContentBatch(BTreeSet<String>),
    IdManifestBatch(BTreeSet<String>),
}

fn exact_schema_read_route(
    statement: &DataFusionStatement,
    params: &[Value],
) -> Option<super::UnboundSchemaRead> {
    let simple = simple_single_table_select(statement)?;
    if !simple.unqualified_unquoted_table
        || simple.alias.is_some()
        || simple.query.fetch.is_some()
        || !point_read_limit_is_safe(simple.query.limit_clause.as_ref())
    {
        return None;
    }
    let mut projected_columns = Vec::with_capacity(simple.select.projection.len());
    let mut output_columns = Vec::with_capacity(simple.select.projection.len());
    for item in &simple.select.projection {
        let (expression, alias) = match item {
            SelectItem::UnnamedExpr(expression) => (expression, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias)),
            _ => return None,
        };
        let column = exact_point_column(expression)?;
        projected_columns.push(column.clone());
        output_columns.push(alias.map_or(column, |alias| alias.value.clone()));
    }
    if projected_columns.is_empty() {
        return None;
    }
    let order_by_columns = match &simple.query.order_by {
        None => Vec::new(),
        Some(order_by) if order_by.interpolate.is_none() => {
            let OrderByKind::Expressions(expressions) = &order_by.kind else {
                return None;
            };
            expressions
                .iter()
                .map(|order| {
                    (order.with_fill.is_none()
                        && order.options.asc != Some(false)
                        && order.options.nulls_first.is_none())
                    .then(|| exact_point_column(&order.expr))
                    .flatten()
                })
                .collect::<Option<Vec<_>>>()?
        }
        Some(_) => return None,
    };
    super::UnboundSchemaRead::new(
        simple.table_name,
        projected_columns,
        output_columns,
        simple.select.selection.as_ref()?.clone(),
        params.to_vec(),
        order_by_columns,
        simple.query.limit_clause.is_some(),
    )
}

pub(crate) fn exact_filesystem_read_route(
    statement: &DataFusionStatement,
    params: &[Value],
) -> Option<ExactFilesystemRead> {
    if exact_lix_file_root_listing(statement, params) {
        return Some(ExactFilesystemRead::RootFileListing);
    }
    if exact_lix_directory_root_listing(statement, params) {
        return Some(ExactFilesystemRead::RootDirectoryListing);
    }
    if let Some((selector, column)) = exact_lix_file_point_read(statement, params) {
        return Some(ExactFilesystemRead::Point(selector, column));
    }

    let point_read = simple_point_read(statement)?;
    if point_read.table_name != "lix_file" || !point_read.exact_table_shape {
        return None;
    }
    exact_path_content_batch(point_read.select, params)
        .map(ExactFilesystemRead::PathContentBatch)
        .or_else(|| {
            exact_id_manifest_batch(point_read.select, params)
                .map(ExactFilesystemRead::IdManifestBatch)
    })
}

/// The operation-level read-interest seed may accept a literal equality in
/// addition to the parameterized shapes accepted by the native execution
/// fast path. This deliberately remains a separate route: literal SQL keeps
/// DataFusion's normal planning/execution semantics and is never redirected
/// to the narrow native executor merely because it can seed discovery.
pub(crate) fn exact_filesystem_read_interest_route(
    statement: &DataFusionStatement,
    params: &[Value],
) -> Option<ExactFilesystemRead> {
    exact_filesystem_read_route(statement, params).or_else(|| {
        exact_lix_file_point_read_with_literals(statement, params)
            .map(|(selector, column)| ExactFilesystemRead::Point(selector, column))
    }).or_else(|| {
        // The SQL executor still evaluates every projected expression locally.
        // This route only seeds immutable dependencies before planning or branch
        // resolution can miss native inputs on a cold partial replica.
        let point = simple_point_read(statement)?;
        if point.table_name != "lix_file" {
            return None;
        }
        let (column, value) = exact_point_identity_inner(
            point.select.selection.as_ref()?,
            params,
            true,
        )?;
        let selector = match column.as_str() {
            "id" => sql2::ExactLixFileReadSelector::Id(value),
            "path" => sql2::ExactLixFileReadSelector::Path(value),
            _ => return None,
        };
        let content = point.select.projection.iter().any(|item| match item {
            SelectItem::UnnamedExpr(expression) => expression_mentions_column(expression, "content"),
            SelectItem::ExprWithAlias { expr, .. } | SelectItem::ExprWithAliases { expr, .. } => {
                expression_mentions_column(expr, "content")
            }
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => true,
        });
        Some(ExactFilesystemRead::Point(
            selector,
            if content {
                sql2::ExactLixFileReadColumn::Content
            } else {
                sql2::ExactLixFileReadColumn::ChangeId
            },
        ))
    })
}

pub(crate) fn exact_lix_file_root_listing(
    statement: &DataFusionStatement,
    params: &[Value],
) -> bool {
    exact_root_listing(
        statement,
        params,
        "lix_file",
        &["id", "path", "name", "lixcol_metadata", "lixcol_updated_at"],
        "directory_id",
    )
}

pub(crate) fn exact_lix_directory_root_listing(
    statement: &DataFusionStatement,
    params: &[Value],
) -> bool {
    exact_root_listing(
        statement,
        params,
        "lix_directory",
        &["id", "path", "name", "lixcol_updated_at"],
        "parent_id",
    )
}

fn exact_root_listing(
    statement: &DataFusionStatement,
    params: &[Value],
    table_name: &str,
    projection: &[&str],
    parent_column: &str,
) -> bool {
    if !params.is_empty() {
        return false;
    }
    let Some(simple) = simple_single_table_select(statement) else {
        return false;
    };
    if simple.table_name != table_name
        || !simple.unqualified_unquoted_table
        || simple.alias.is_some()
        || simple.query.limit_clause.is_some()
        || simple.query.fetch.is_some()
    {
        return false;
    }
    if simple.select.projection.len() != projection.len()
        || !simple
            .select
            .projection
            .iter()
            .zip(projection)
            .all(|(item, expected)| {
                let SelectItem::UnnamedExpr(expression) = item else {
                    return false;
                };
                exact_point_column(expression).as_deref() == Some(*expected)
            })
    {
        return false;
    }
    let Some(Expr::IsNull(parent)) = simple.select.selection.as_ref() else {
        return false;
    };
    if exact_point_column(parent).as_deref() != Some(parent_column) {
        return false;
    }
    let Some(order_by) = &simple.query.order_by else {
        return false;
    };
    if order_by.interpolate.is_some() {
        return false;
    }
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return false;
    };
    let [order] = expressions.as_slice() else {
        return false;
    };
    order.with_fill.is_none()
        && order.options.asc != Some(false)
        && order.options.nulls_first.is_none()
        && exact_point_column(&order.expr).as_deref() == Some("name")
}

pub(crate) fn exact_lix_file_point_read(
    statement: &DataFusionStatement,
    params: &[Value],
) -> Option<(sql2::ExactLixFileReadSelector, sql2::ExactLixFileReadColumn)> {
    exact_lix_file_point_read_inner(statement, params, false)
}

fn exact_lix_file_point_read_with_literals(
    statement: &DataFusionStatement,
    params: &[Value],
) -> Option<(sql2::ExactLixFileReadSelector, sql2::ExactLixFileReadColumn)> {
    exact_lix_file_point_read_inner(statement, params, true)
}

fn exact_lix_file_point_read_inner(
    statement: &DataFusionStatement,
    params: &[Value],
    allow_literals: bool,
) -> Option<(sql2::ExactLixFileReadSelector, sql2::ExactLixFileReadColumn)> {
    let point_read = simple_point_read(statement)?;
    if point_read.table_name != "lix_file" || !point_read.exact_table_shape {
        return None;
    }
    let [SelectItem::UnnamedExpr(projection)] = point_read.select.projection.as_slice() else {
        return None;
    };
    let Expr::Identifier(projection) = projection else {
        return None;
    };
    if projection.quote_style.is_some() {
        return None;
    }
    let column = match projection.value.to_ascii_lowercase().as_str() {
        "content" => sql2::ExactLixFileReadColumn::Content,
        "lixcol_change_id" => sql2::ExactLixFileReadColumn::ChangeId,
        _ => return None,
    };
    let selection = point_read.select.selection.as_ref()?;
    let (identity_column, identity_value) =
        exact_point_identity_inner(selection, params, allow_literals)?;
    let selector = match identity_column.as_str() {
        "id" => sql2::ExactLixFileReadSelector::Id(identity_value),
        "path" => sql2::ExactLixFileReadSelector::Path(identity_value),
        _ => return None,
    };
    Some((selector, column))
}

fn exact_path_content_batch(select: &Select, params: &[Value]) -> Option<BTreeSet<String>> {
    exact_parameter_batch(select, params, &["path", "content"], "path", false)
}

fn exact_id_manifest_batch(select: &Select, params: &[Value]) -> Option<BTreeSet<String>> {
    exact_parameter_batch(
        select,
        params,
        &["id", "path", "content", "lixcol_metadata"],
        "id",
        true,
    )
}

/// Native batches require a complete projection and a numbered parameter list.
/// Manifest requests preserve their existing duplicate-ID fallback; downloads
/// deduplicate paths as SQL IN does.
fn exact_parameter_batch(
    select: &Select,
    params: &[Value],
    projection: &[&str],
    identity: &str,
    require_unique: bool,
) -> Option<BTreeSet<String>> {
    if select.projection.len() != projection.len() || !select.projection.iter().zip(projection).all(|(item, expected)| {
        matches!(item, SelectItem::UnnamedExpr(expr) if exact_point_column(expr).as_deref() == Some(*expected))
    }) { return None; }
    let Expr::InList {
        expr,
        list,
        negated: false,
    } = select.selection.as_ref()?
    else {
        return None;
    };
    if exact_point_column(expr).as_deref() != Some(identity)
        || list.is_empty()
        || list.len() != params.len()
    {
        return None;
    }
    let mut values = BTreeSet::new();
    for (index, (expression, param)) in list.iter().zip(params).enumerate() {
        let Expr::Value(value) = expression else {
            return None;
        };
        let SqlValue::Placeholder(placeholder) = &value.value else {
            return None;
        };
        if placeholder != &format!("${}", index + 1) {
            return None;
        }
        let Value::Text(value) = param else {
            return None;
        };
        values.insert(value.clone());
    }
    (!require_unique || values.len() == list.len()).then_some(values)
}

fn exact_point_identity_inner(
    expression: &Expr,
    params: &[Value],
    allow_literals: bool,
) -> Option<(String, String)> {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = expression
    else {
        return None;
    };
    match (exact_point_column(left), exact_point_column(right)) {
        (Some(column), None) => Some((
            column,
            exact_point_text_value(right, params, allow_literals)?,
        )),
        (None, Some(column)) => Some((
            column,
            exact_point_text_value(left, params, allow_literals)?,
        )),
        _ => None,
    }
}

fn exact_point_column(expression: &Expr) -> Option<String> {
    let Expr::Identifier(identifier) = expression else {
        return None;
    };
    if identifier.quote_style.is_some() {
        return None;
    }
    Some(identifier.value.to_ascii_lowercase())
}

fn exact_point_text_value(
    expression: &Expr,
    params: &[Value],
    allow_literals: bool,
) -> Option<String> {
    let Expr::Value(value) = expression else {
        return None;
    };
    match &value.value {
        SqlValue::Placeholder(placeholder) if params.len() == 1 && placeholder == "$1" => {
            let Value::Text(value) = &params[0] else {
                return None;
            };
            Some(value.clone())
        }
        value if allow_literals => value.clone().into_string(),
        _ => None,
    }
}

/// A unique id/path predicate can return at most one row. `LIMIT 1` therefore
/// leaves that delivered row unchanged, while offsets and dynamic limits can
/// hide a materialized row and must remain non-acknowledging.
fn point_read_limit_is_safe(limit_clause: Option<&LimitClause>) -> bool {
    let Some(limit_clause) = limit_clause else {
        return true;
    };
    let LimitClause::LimitOffset {
        limit,
        offset,
        limit_by,
    } = limit_clause
    else {
        return false;
    };
    if offset.is_some() || !limit_by.is_empty() {
        return false;
    }
    let Some(Expr::Value(value)) = limit else {
        // `LIMIT ALL` does not remove the unique point row.
        return limit.is_none();
    };
    matches!(&value.value, SqlValue::Number(number, _) if number.parse::<u64>().is_ok_and(|number| number > 0))
}

fn group_by_is_empty(group_by: &GroupByExpr) -> bool {
    matches!(group_by, GroupByExpr::Expressions(expressions, modifiers)
        if expressions.is_empty() && modifiers.is_empty())
}

fn direct_column_name(expression: &Expr) -> Option<String> {
    let identifier = match expression {
        Expr::Identifier(identifier) => identifier,
        Expr::CompoundIdentifier(identifiers) => identifiers.last()?,
        Expr::Nested(expression) => return direct_column_name(expression),
        _ => return None,
    };
    Some(identifier.value.to_ascii_lowercase())
}

fn collect_literal_equalities(
    expression: &Expr,
    columns: &mut BTreeSet<String>,
    params: &[Value],
) -> bool {
    match expression {
        Expr::Nested(expression) => collect_literal_equalities(expression, columns, params),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_literal_equalities(left, columns, params)
                && collect_literal_equalities(right, columns, params)
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            let column = match (direct_column_name(left), direct_column_name(right)) {
                (Some(column), None) if point_identity_value_is_text(right, params) => column,
                (None, Some(column)) if point_identity_value_is_text(left, params) => column,
                _ => return false,
            };
            columns.insert(column)
        }
        _ => false,
    }
}

fn point_identity_value_is_text(expression: &Expr, params: &[Value]) -> bool {
    let Expr::Value(value) = expression else {
        return false;
    };
    match &value.value {
        SqlValue::Placeholder(placeholder) => {
            let index = placeholder
                .strip_prefix('$')
                .and_then(|index| index.parse::<usize>().ok())
                .and_then(|index| index.checked_sub(1));
            index
                .and_then(|index| params.get(index))
                .is_some_and(|value| matches!(value, Value::Text(_)))
        }
        value => value.clone().into_string().is_some(),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StatementReadPlan {
    pub(crate) native: Option<NativeReadPlan>,
    pub(crate) late_content: Option<LateMaterializedLixFileContentRead>,
    pub(crate) acknowledge_file_views: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum NativeReadPlan {
    Filesystem(ExactFilesystemRead),
    Schema(Box<super::UnboundSchemaRead>),
}

pub(crate) fn plan_read_statement(
    statement: &DataFusionStatement,
    params: &[Value],
) -> StatementReadPlan {
    let filesystem = exact_filesystem_read_route(statement, params);
    // A syntactic schema candidate can later fail catalog binding. It must
    // never suppress file hydration or acknowledgement in the SQL fallback.
    let late_content = filesystem
        .is_none()
        .then(|| late_materialized_lix_file_content_read(statement, params))
        .flatten();
    let native = filesystem.map(NativeReadPlan::Filesystem).or_else(|| {
        late_content
            .is_none()
            .then(|| {
                exact_schema_read_route(statement, params)
                    .map(|schema| NativeReadPlan::Schema(Box::new(schema)))
            })
            .flatten()
    });
    let acknowledge_file_views = is_acknowledgeable_file_content_read(statement, params)
        || matches!(
            native,
            Some(NativeReadPlan::Filesystem(
                ExactFilesystemRead::PathContentBatch(_)
            ))
        )
        || late_content
            .as_ref()
            .is_some_and(|plan| plan.projection.acknowledges_content());
    StatementReadPlan {
        native,
        late_content,
        acknowledge_file_views,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filesystem_fallback_retains_late_content_and_acknowledgement() {
        let statement =
            sql2::parse_statement("SELECT content FROM lix_file WHERE path IN ($1) LIMIT 1")
                .unwrap();
        let params = [Value::Text("/example.txt".into())];
        assert!(exact_filesystem_read_route(&statement, &params).is_none());
        assert!(
            exact_schema_read_route(&statement, &params).is_some(),
            "shape alone is not schema binding"
        );
        let plan = plan_read_statement(&statement, &params);
        assert!(plan.native.is_none());
        assert!(plan.late_content.is_some());
        assert!(plan.acknowledge_file_views);
    }

    #[test]
    fn file_size_and_bounded_substring_projections_late_materialize() {
        let size_statement = sql2::parse_statement(
            "SELECT f.path AS path, OCTET_LENGTH(f.content) AS size_bytes \
             FROM lix_file AS f WHERE f.path = $1",
        )
        .unwrap();
        let params = [Value::Text("/large.bin".into())];
        let size_plan = late_materialized_lix_file_content_read(&size_statement, &params)
            .expect("OCTET_LENGTH(content) should use blob-ref size metadata");
        assert_eq!(size_plan.data_column_index, 1);
        assert_eq!(size_plan.projection, LateLixFileProjection::OctetLength);
        assert!(size_plan.statement.to_string().contains("f.path AS size_bytes"));
        assert!(!plan_read_statement(&size_statement, &params).acknowledge_file_views);

        let substring_statement = sql2::parse_statement(
            "SELECT SUBSTRING(f.content FROM $2 FOR $3) AS slice \
             FROM lix_file AS f WHERE f.path = $1 ORDER BY f.path",
        )
        .unwrap();
        let params = [
            Value::Text("/large.bin".into()),
            Value::Integer(-1),
            Value::Integer(3),
        ];
        let substring_plan = late_materialized_lix_file_content_read(&substring_statement, &params)
            .expect("bounded SUBSTRING(content) should use CAS ranges");
        assert_eq!(substring_plan.data_column_index, 0);
        assert_eq!(
            substring_plan.projection,
            LateLixFileProjection::Substring {
                start: -1,
                length: 3,
            }
        );
        assert!(substring_plan.statement.to_string().contains("f.path AS slice"));
        let rewritten = substring_plan.statement.to_string();
        assert!(rewritten.contains("$2 IS NOT NULL"));
        assert!(rewritten.contains("$3 IS NOT NULL"));
        assert!(plan_read_statement(&substring_statement, &params).acknowledge_file_views);

        let slice_parameters_only = sql2::parse_statement(
            "SELECT SUBSTRING(content FROM $1 FOR $2) AS slice \
             FROM lix_file WHERE path = '/large.bin'",
        )
        .unwrap();
        let slice_params = [Value::Integer(1), Value::Integer(3)];
        let slice_plan = late_materialized_lix_file_content_read(
            &slice_parameters_only,
            &slice_params,
        )
        .expect("slice-only parameters should remain bindable after rewrite");
        let rewritten = slice_plan.statement.to_string();
        assert!(rewritten.contains("$1 IS NOT NULL"));
        assert!(rewritten.contains("$2 IS NOT NULL"));
    }

    #[test]
    fn unsupported_substring_shapes_keep_the_ordinary_sql_path() {
        for sql in [
            "SELECT SUBSTRING(content FROM 1) FROM lix_file",
            "SELECT SUBSTRING(content FROM 1 FOR -2) FROM lix_file",
            "SELECT SUBSTRING(content FROM 1 FOR 2) AS part FROM lix_file ORDER BY part",
            "SELECT SUBSTRING(content FROM 1 FOR 2) FROM lix_file WHERE content IS NOT NULL",
            "SELECT public.OCTET_LENGTH(content) FROM lix_file",
            "SELECT OCTET_LENGTH(content) FILTER (WHERE path IS NOT NULL) FROM lix_file",
            "SELECT OCTET_LENGTH(content) OVER () FROM lix_file",
            "SELECT OCTET_LENGTH(content) WITHIN GROUP (ORDER BY path) FROM lix_file",
            "SELECT OCTET_LENGTH(content) RESPECT NULLS FROM lix_file",
        ] {
            let statement = sql2::parse_statement(sql).unwrap();
            assert_eq!(
                late_materialized_lix_file_content_read(&statement, &[]),
                None,
                "unsupported range shape should not be rewritten: {sql}"
            );
        }
    }
}
