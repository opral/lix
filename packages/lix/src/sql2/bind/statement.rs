use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;

use datafusion::sql::parser::Statement as DataFusionStatement;
use datafusion::sql::sqlparser::ast::{
    AssignmentTarget, BinaryOperator, CastKind, ConflictTarget, DataType as SqlDataType, Delete,
    Expr, FromTable, Function, FunctionArg, FunctionArgExpr, FunctionArguments, Insert, ObjectName,
    ObjectNamePart, OnConflictAction, OnInsert, Query, SelectItem, SetExpr,
    Statement as SqlStatement, TableFactor, TableObject, TableWithJoins,
    UnaryOperator, Update, Value, Visit, Visitor, WildcardAdditionalOptions,
};
#[cfg(test)]
use serde_json::Value as JsonValue;

use crate::LixError;
use crate::sql2::catalog::{PublicCatalog, PublicSurfaceContract, PublicSurfaceKind};
use crate::sql2::plan::branch_scope::BranchScope;
use crate::sql2::plan::predicate::BoundPredicate;

use super::expr::{
    BoundBinaryOperator, BoundCastType, BoundExpr, BoundLiteral, BoundParamRef,
    bind_public_cast_type,
};
use super::read::BoundRead;
use super::table::{
    BoundTable, bind_public_column_ref, bind_public_table, require_writable_column,
};
use super::write::{
    BoundAssignment, BoundConflictAction, BoundInsertConflict, BoundInsertValues, BoundParamMap,
    BoundReturning, BoundReturningItem, BoundWrite, BoundWriteInput, BoundWriteOp,
    BoundWriteTarget, DirectoryWriteSurface, FileWriteSurface, RowWriteSurface,
};
use crate::sql2::write_normalization::LIX_FILE_CONTENT_CAST_HINT;

#[cfg(test)]
pub(crate) fn bind_statement(
    statement: &DataFusionStatement,
    visible_schemas: &[JsonValue],
    active_branch_id: &str,
) -> Result<BoundWrite, LixError> {
    let catalog = PublicCatalog::from_visible_schemas(visible_schemas)?;
    bind_statement_with_catalog(statement, &catalog, active_branch_id)
}

pub(crate) fn bind_statement_with_catalog(
    statement: &DataFusionStatement,
    catalog: &PublicCatalog,
    active_branch_id: &str,
) -> Result<BoundWrite, LixError> {
    match statement {
        DataFusionStatement::Statement(statement) => {
            bind_sql_statement(statement, catalog, active_branch_id)
        }
        DataFusionStatement::Explain(_) => Err(super::error::unsupported(
            "EXPLAIN statements are not supported by SQL write binding",
        )),
        _ => Err(super::error::unsupported(format!(
            "SQL statement is not supported by Lix SQL: {statement}"
        ))),
    }
}

fn bind_sql_statement(
    statement: &SqlStatement,
    catalog: &PublicCatalog,
    active_branch_id: &str,
) -> Result<BoundWrite, LixError> {
    match statement {
        SqlStatement::Insert(insert) => bind_insert_bound(insert, catalog, active_branch_id),
        SqlStatement::Update(update) => bind_update_bound(update, catalog, active_branch_id),
        SqlStatement::Delete(delete) => bind_delete_bound(delete, catalog, active_branch_id),
        SqlStatement::Explain { .. } => Err(super::error::unsupported(
            "EXPLAIN statements are not supported by SQL write binding",
        )),
        _ => Err(super::error::unsupported(
            "sql2 bound statement pipeline is not wired yet",
        )),
    }
}

pub(super) fn bind_insert_bound(
    insert: &Insert,
    catalog: &PublicCatalog,
    active_branch_id: &str,
) -> Result<BoundWrite, LixError> {
    let mut params = ParamBinder::default();
    reject_unsupported_insert_clauses(insert)?;
    let TableObject::TableName(name) = &insert.table else {
        return Err(super::error::unsupported("unsupported INSERT target"));
    };
    let table = bind_public_table(catalog, name)?;
    require_write_capability(&table.surface, BoundWriteOp::Insert)?;
    // sqlparser represents standard `INSERT INTO table DEFAULT VALUES` as an
    // INSERT without a source and without target columns. Support it only for
    // registered-schema base tables and the metadata-only full-checkpoint
    // command. Keep it distinct from `INSERT INTO table VALUES (...)`, whose
    // implicit public column list is deliberately unsupported.
    let default_values = matches!(
        table.surface.kind,
        PublicSurfaceKind::SchemaBase { .. }
    )
        && insert.columns.is_empty()
        && insert.source.is_none();
    if insert.columns.is_empty() && !default_values {
        return Err(super::error::unsupported(
            "INSERT requires an explicit public column list",
        ));
    }
    let mut target_columns = BTreeSet::new();
    let mut columns = Vec::new();
    for column in &insert.columns {
        let column_name = normalize_identifier(column);
        reject_duplicate_target_column(&mut target_columns, &column_name)?;
        columns.push(require_writable_column(
            &table,
            &column_name,
            BoundWriteOp::Insert,
        )?);
    }
    let input = if default_values {
        // Preserve the cardinality of `DEFAULT VALUES`: it inserts one row,
        // with no selected relation rows. Registered schemas materialize
        // defaults; the checkpoint command aliases the current branch state.
        BoundWriteInput::Values(BoundInsertValues {
            columns: Vec::new(),
            rows: vec![Vec::new()],
        })
    } else {
        bind_insert_input(
            &table.surface.kind,
            &columns,
            insert.source.as_deref(),
            &mut params,
        )?
    };
    let returning = bind_insert_returning(&table, insert.returning.as_ref(), &mut params)?;
    let conflict = bind_insert_conflict(insert.on.as_ref(), &table, &mut params)?;
    if conflict.is_some() {
        if !matches!(
            table.surface.kind,
            PublicSurfaceKind::SchemaBase { .. }
                | PublicSurfaceKind::Branch
                | PublicSurfaceKind::File
                | PublicSurfaceKind::Directory
        ) {
            return Err(super::error::unsupported(
                "INSERT ON CONFLICT is not supported for this SQL surface yet",
            ));
        }
        require_write_capability(&table.surface, BoundWriteOp::Update)?;
    }
    let branch_scope = bind_write_branch_scope(
        &table.surface.kind,
        &input,
        &BoundPredicate::True,
        active_branch_id,
    )?;
    let target = bound_insert_target(&table.surface.kind, &input)?;
    Ok(BoundWrite {
        target,
        op: BoundWriteOp::Insert,
        input,
        predicate: BoundPredicate::True,
        assignments: Vec::new(),
        conflict,
        returning,
        params: params.into_map(),
        branch_scope,
    })
}

pub(super) fn bind_update_bound(
    update: &Update,
    catalog: &PublicCatalog,
    active_branch_id: &str,
) -> Result<BoundWrite, LixError> {
    let mut params = ParamBinder::default();
    reject_unsupported_update_clauses(update)?;
    let table = bind_table_with_joins(catalog, &update.table)?;
    require_write_capability(&table.surface, BoundWriteOp::Update)?;
    let mut target_columns = BTreeSet::new();
    let mut assignments = Vec::new();
    for assignment in &update.assignments {
        let column = bind_assignment_target(&table, &assignment.target)?;
        reject_duplicate_target_column(&mut target_columns, &column.name)?;
        assignments.push(BoundAssignment {
            column,
            value: bind_expr(&table, &assignment.value, &mut params)?,
        });
    }
    let predicate = bind_optional_predicate(&table, update.selection.as_ref(), &mut params)?;
    let returning = bind_returning(&table, update.returning.as_ref(), &mut params, "UPDATE")?;
    let branch_scope = bind_write_branch_scope(
        &table.surface.kind,
        &BoundWriteInput::None,
        &predicate,
        active_branch_id,
    )?;
    Ok(BoundWrite {
        target: bound_write_target(&table.surface.kind),
        op: BoundWriteOp::Update,
        input: BoundWriteInput::None,
        predicate,
        assignments,
        conflict: None,
        returning,
        params: params.into_map(),
        branch_scope,
    })
}

pub(super) fn bind_delete_bound(
    delete: &Delete,
    catalog: &PublicCatalog,
    active_branch_id: &str,
) -> Result<BoundWrite, LixError> {
    let mut params = ParamBinder::default();
    reject_unsupported_delete_clauses(delete)?;
    let table = bind_delete_target(catalog, &delete.from)?;
    require_write_capability(&table.surface, BoundWriteOp::Delete)?;
    let predicate = bind_optional_predicate(&table, delete.selection.as_ref(), &mut params)?;
    let returning = bind_returning(&table, delete.returning.as_ref(), &mut params, "DELETE")?;
    let branch_scope = bind_write_branch_scope(
        &table.surface.kind,
        &BoundWriteInput::None,
        &predicate,
        active_branch_id,
    )?;
    Ok(BoundWrite {
        target: bound_write_target(&table.surface.kind),
        op: BoundWriteOp::Delete,
        input: BoundWriteInput::None,
        predicate,
        assignments: Vec::new(),
        conflict: None,
        returning,
        params: params.into_map(),
        branch_scope,
    })
}

/// Bind a normal DML `RETURNING` list against the target surface itself. It
/// is not a nested SELECT: execution chooses the appropriate before/after
/// row image for the write operation.
fn bind_returning(
    table: &BoundTable,
    returning: Option<&Vec<SelectItem>>,
    params: &mut ParamBinder,
    action: &str,
) -> Result<Option<BoundReturning>, LixError> {
    let Some(returning) = returning else {
        return Ok(None);
    };

    let mut items = Vec::new();
    for item in returning {
        match item {
            SelectItem::Wildcard(options) => {
                reject_returning_wildcard_options(options, action)?;
                for column in table
                    .surface
                    .columns
                    .iter()
                    .filter(|column| column.is_public())
                {
                    items.push(BoundReturningItem {
                        expr: BoundExpr::Column(bind_public_column_ref(table, &column.name)?),
                        output_name: column.name.clone(),
                    });
                }
            }
            SelectItem::QualifiedWildcard(_, _) => {
                return Err(super::error::unsupported(format!(
                    "qualified wildcards in {action} RETURNING are not supported"
                )));
            }
            SelectItem::UnnamedExpr(sql_expr) => {
                let expr = bind_expr(table, sql_expr, params)?;
                let output_name = match &expr {
                    BoundExpr::Column(column) => column.name.clone(),
                    _ => sql_expr.to_string(),
                };
                items.push(BoundReturningItem { expr, output_name });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                items.push(BoundReturningItem {
                    expr: bind_expr(table, expr, params)?,
                    output_name: normalize_identifier(alias),
                });
            }
        }
    }

    if items.is_empty() {
        return Err(super::error::unsupported(format!(
            "{action} RETURNING requires at least one result expression"
        )));
    }

    Ok(Some(BoundReturning { items }))
}

fn bind_insert_returning(
    table: &BoundTable,
    returning: Option<&Vec<SelectItem>>,
    params: &mut ParamBinder,
) -> Result<Option<BoundReturning>, LixError> {
    let Some(returning) = returning else {
        return Ok(None);
    };

    if !matches!(
        table.surface.kind,
        PublicSurfaceKind::Revert
            | PublicSurfaceKind::Apply
            | PublicSurfaceKind::Restore
    ) {
        return bind_returning(table, Some(returning), params, "INSERT");
    }

    let mut items = Vec::with_capacity(returning.len());
    for item in returning {
        let (sql_expr, output_name) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, "commit_id".to_string()),
            SelectItem::ExprWithAlias { expr, alias } => (expr, normalize_identifier(alias)),
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                return Err(super::error::unsupported(
                    "command sink INSERT RETURNING only supports commit_id",
                ));
            }
        };
        let expr = bind_expr(table, sql_expr, params)?;
        if !matches!(&expr, BoundExpr::Column(column) if column.name == "commit_id") {
            return Err(super::error::unsupported(
                "command sink INSERT RETURNING only supports commit_id",
            ));
        }
        items.push(BoundReturningItem { expr, output_name });
    }
    if items.is_empty() {
        return Err(super::error::unsupported(
            "diff command INSERT RETURNING requires commit_id",
        ));
    }
    Ok(Some(BoundReturning { items }))
}

fn reject_returning_wildcard_options(
    options: &WildcardAdditionalOptions,
    action: &str,
) -> Result<(), LixError> {
    if options.opt_ilike.is_some()
        || options.opt_exclude.is_some()
        || options.opt_except.is_some()
        || options.opt_replace.is_some()
        || options.opt_rename.is_some()
    {
        return Err(super::error::unsupported(format!(
            "{action} RETURNING wildcard modifiers are not supported"
        )));
    }
    Ok(())
}

fn reject_unsupported_insert_clauses(insert: &Insert) -> Result<(), LixError> {
    if insert.optimizer_hint.is_some() {
        return Err(super::error::unsupported(
            "INSERT optimizer hints are not supported",
        ));
    }
    if insert.or.is_some() {
        return Err(super::error::unsupported(
            "INSERT conflict clauses are not supported",
        ));
    }
    if insert.ignore {
        return Err(super::error::unsupported("INSERT IGNORE is not supported"));
    }
    if insert.table_alias.is_some() {
        return Err(super::error::unsupported(
            "INSERT target aliases are not supported",
        ));
    }
    if insert.overwrite {
        return Err(super::error::unsupported(
            "INSERT OVERWRITE is not supported",
        ));
    }
    if !insert.assignments.is_empty() {
        return Err(super::error::unsupported("INSERT ... SET is not supported"));
    }
    if insert.partitioned.is_some() || !insert.after_columns.is_empty() {
        return Err(super::error::unsupported(
            "partitioned INSERT is not supported",
        ));
    }
    if insert.replace_into {
        return Err(super::error::unsupported("REPLACE INTO is not supported"));
    }
    if insert.priority.is_some() {
        return Err(super::error::unsupported(
            "INSERT priority clauses are not supported",
        ));
    }
    if insert.insert_alias.is_some() {
        return Err(super::error::unsupported(
            "INSERT row aliases are not supported",
        ));
    }
    if insert.settings.is_some() || insert.format_clause.is_some() {
        return Err(super::error::unsupported(
            "INSERT settings and format clauses are not supported",
        ));
    }
    Ok(())
}

fn bind_insert_conflict(
    on: Option<&OnInsert>,
    table: &BoundTable,
    params: &mut ParamBinder,
) -> Result<Option<BoundInsertConflict>, LixError> {
    let Some(on) = on else {
        return Ok(None);
    };
    let OnInsert::OnConflict(conflict) = on else {
        return Err(super::error::unsupported(
            "INSERT ON DUPLICATE KEY UPDATE is not supported",
        ));
    };
    let Some(ConflictTarget::Columns(columns)) = &conflict.conflict_target else {
        return Err(super::error::unsupported(
            "INSERT ON CONFLICT requires an explicit column target",
        ));
    };
    let mut seen_target_columns = BTreeSet::new();
    let target_columns = columns
        .iter()
        .map(|column| {
            let column_name = normalize_identifier(column);
            reject_duplicate_target_column(&mut seen_target_columns, &column_name)?;
            bind_public_column_ref(table, &column_name)
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let action = match &conflict.action {
        OnConflictAction::DoNothing => BoundConflictAction::DoNothing,
        OnConflictAction::DoUpdate(update) => {
            if update.selection.is_some() {
                return Err(super::error::unsupported(
                    "INSERT ON CONFLICT DO UPDATE WHERE is not supported",
                ));
            }
            let mut seen_assignments = BTreeSet::new();
            let assignments = update
                .assignments
                .iter()
                .map(|assignment| {
                    let column = bind_assignment_target(table, &assignment.target)?;
                    reject_duplicate_target_column(&mut seen_assignments, &column.name)?;
                    Ok(BoundAssignment {
                        column,
                        value: bind_conflict_expr(table, &assignment.value, params)?,
                    })
                })
                .collect::<Result<Vec<_>, LixError>>()?;
            BoundConflictAction::DoUpdate { assignments }
        }
    };

    Ok(Some(BoundInsertConflict {
        target_columns,
        action,
    }))
}

fn reject_unsupported_update_clauses(update: &Update) -> Result<(), LixError> {
    if update.optimizer_hint.is_some() {
        return Err(super::error::unsupported(
            "UPDATE optimizer hints are not supported",
        ));
    }
    if update.from.is_some() {
        return Err(super::error::unsupported("UPDATE FROM is not supported"));
    }
    if update.or.is_some() {
        return Err(super::error::unsupported(
            "UPDATE conflict clauses are not supported",
        ));
    }
    if update.limit.is_some() {
        return Err(super::error::unsupported("UPDATE LIMIT is not supported"));
    }
    Ok(())
}

fn reject_unsupported_delete_clauses(delete: &Delete) -> Result<(), LixError> {
    if delete.optimizer_hint.is_some() {
        return Err(super::error::unsupported(
            "DELETE optimizer hints are not supported",
        ));
    }
    if !delete.tables.is_empty() {
        return Err(super::error::unsupported(
            "multi-table DELETE is not supported",
        ));
    }
    if delete.using.is_some() {
        return Err(super::error::unsupported("DELETE USING is not supported"));
    }
    if !delete.order_by.is_empty() {
        return Err(super::error::unsupported(
            "DELETE ORDER BY is not supported",
        ));
    }
    if delete.limit.is_some() {
        return Err(super::error::unsupported("DELETE LIMIT is not supported"));
    }
    Ok(())
}

fn bind_table_with_joins(
    catalog: &PublicCatalog,
    table: &TableWithJoins,
) -> Result<BoundTable, LixError> {
    if !table.joins.is_empty() {
        return Err(super::error::unsupported(
            "joined DML targets are not supported",
        ));
    }
    let TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
        ..
    } = &table.relation
    else {
        return Err(super::error::unsupported("unsupported DML target"));
    };
    if alias.is_some() {
        return Err(super::error::unsupported(
            "DML target aliases are not supported",
        ));
    }
    if args.is_some()
        || !with_hints.is_empty()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
    {
        return Err(super::error::unsupported(
            "DML target table modifiers are not supported",
        ));
    }
    bind_public_table(catalog, name)
}

fn bind_delete_target(catalog: &PublicCatalog, from: &FromTable) -> Result<BoundTable, LixError> {
    let tables = match from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    if tables.len() != 1 {
        return Err(super::error::unsupported(
            "DELETE requires exactly one target table",
        ));
    }
    bind_table_with_joins(catalog, &tables[0])
}

fn bind_assignment_target(
    table: &BoundTable,
    target: &AssignmentTarget,
) -> Result<super::expr::BoundColumnRef, LixError> {
    match target {
        AssignmentTarget::ColumnName(name) => {
            let column_name = bind_exact_column_name(name)?;
            require_writable_column(table, &column_name, BoundWriteOp::Update)
        }
        AssignmentTarget::Tuple(_) => Err(super::error::unsupported(
            "tuple UPDATE assignments are not supported",
        )),
    }
}

fn bind_insert_input(
    surface_kind: &PublicSurfaceKind,
    columns: &[super::expr::BoundColumnRef],
    source: Option<&Query>,
    params: &mut ParamBinder,
) -> Result<BoundWriteInput, LixError> {
    let Some(source) = source else {
        return Err(super::error::unsupported("INSERT source is required"));
    };
    if matches!(source.body.as_ref(), SetExpr::Values(_)) {
        reject_unsupported_insert_values_query_clauses(source)?;
    }
    if matches!(
        surface_kind,
        PublicSurfaceKind::Revert | PublicSurfaceKind::Apply
    ) && matches!(source.body.as_ref(), SetExpr::Values(_))
    {
        return Err(super::error::unsupported(
            "diff command sinks require INSERT ... SELECT; INSERT ... VALUES is not supported",
        ));
    }
    let SetExpr::Values(values) = source.body.as_ref() else {
        if matches!(surface_kind, PublicSurfaceKind::SchemaBase { .. }) {
            return Err(super::error::unsupported(
                "INSERT ... SELECT is not supported for schema SQL surfaces yet",
            ));
        }
        if columns
            .iter()
            .any(|column| column.table == "lix_file" && column.name == "content")
        {
            return Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                "lix_file.content expects binary content",
            )
            .with_hint(LIX_FILE_CONTENT_CAST_HINT));
        }
        let statement =
            DataFusionStatement::Statement(Box::new(SqlStatement::Query(Box::new(source.clone()))));
        super::read::bind_read_statement(&source.to_string(), &statement)?;
        bind_query_params(source, params)?;
        return Ok(BoundWriteInput::Query {
            query: Box::new(BoundRead {
                query: Box::new(source.clone()),
            }),
            columns: columns.to_vec(),
        });
    };
    let mut rows = Vec::with_capacity(values.rows.len());
    for row in &values.rows {
        if row.len() != columns.len() {
            return Err(super::error::unsupported(format!(
                "INSERT has {} target columns but row has {} values",
                columns.len(),
                row.len()
            )));
        }
        rows.push(
            row.iter()
                .map(|value| bind_insert_value_expr(value, params))
                .collect::<Result<Vec<_>, LixError>>()?,
        );
    }
    Ok(BoundWriteInput::Values(BoundInsertValues {
        columns: columns.to_vec(),
        rows,
    }))
}

fn bind_query_params(query: &Query, params: &mut ParamBinder) -> Result<(), LixError> {
    let mut visitor = QueryParamVisitor { params };
    match query.visit(&mut visitor) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(error) => Err(*error),
    }
}

struct QueryParamVisitor<'a> {
    params: &'a mut ParamBinder,
}

impl Visitor for QueryParamVisitor<'_> {
    type Break = Box<LixError>;

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        let Expr::Value(value) = expr else {
            return ControlFlow::Continue(());
        };
        let Value::Placeholder(name) = &value.value else {
            return ControlFlow::Continue(());
        };
        match self.params.bind(name) {
            Ok(_) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(Box::new(error)),
        }
    }
}

fn bind_insert_value_expr(expr: &Expr, params: &mut ParamBinder) -> Result<BoundExpr, LixError> {
    match expr {
        Expr::Value(value) => bind_value(&value.value, params),
        Expr::Nested(expr) => bind_insert_value_expr(expr, params),
        Expr::Cast {
            kind,
            expr,
            data_type,
            array,
            format,
        } => bind_cast_expr(
            kind,
            expr,
            data_type,
            *array,
            format.is_some(),
            params,
            bind_insert_value_expr,
        ),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => bind_negative_number_expr(expr),
        Expr::Function(function) => bind_insert_value_function(function, params),
        Expr::BinaryOp { left, op, right }
            if matches!(op, BinaryOperator::Arrow | BinaryOperator::LongArrow) =>
        {
            Ok(BoundExpr::Function {
                name: if *op == BinaryOperator::Arrow {
                    "__lix_json_get"
                } else {
                    "__lix_json_get_text"
                }
                .to_string(),
                args: vec![
                    bind_insert_value_expr(left, params)?,
                    bind_insert_value_expr(right, params)?,
                ],
            })
        }
        _ => Err(super::error::unsupported(format!(
            "unsupported INSERT VALUES expression '{expr}'"
        ))),
    }
}

fn reject_unsupported_insert_values_query_clauses(source: &Query) -> Result<(), LixError> {
    if source.with.is_some()
        || source.order_by.is_some()
        || source.limit_clause.is_some()
        || source.fetch.is_some()
        || !source.locks.is_empty()
        || source.for_clause.is_some()
        || source.settings.is_some()
        || source.format_clause.is_some()
        || !source.pipe_operators.is_empty()
    {
        return Err(super::error::unsupported(
            "INSERT VALUES query clauses are not supported",
        ));
    }
    Ok(())
}

fn bind_optional_predicate(
    table: &BoundTable,
    expr: Option<&Expr>,
    params: &mut ParamBinder,
) -> Result<BoundPredicate, LixError> {
    expr.map_or_else(
        || Ok(BoundPredicate::True),
        |expr| bind_predicate(table, expr, params),
    )
}

fn bind_predicate(
    table: &BoundTable,
    expr: &Expr,
    params: &mut ParamBinder,
) -> Result<BoundPredicate, LixError> {
    match expr {
        Expr::Nested(expr) => bind_predicate(table, expr, params),
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
            let mut predicates = Vec::new();
            flatten_and_predicate(table, left, params, &mut predicates)?;
            flatten_and_predicate(table, right, params, &mut predicates)?;
            Ok(BoundPredicate::And(predicates))
        }
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Or => {
            let mut predicates = Vec::new();
            flatten_or_predicate(table, left, params, &mut predicates)?;
            flatten_or_predicate(table, right, params, &mut predicates)?;
            Ok(BoundPredicate::Or(predicates))
        }
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Eq => Ok(BoundPredicate::Eq(
            bind_expr(table, left, params)?,
            bind_expr(table, right, params)?,
        )),
        Expr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => bind_like_predicate(
            table,
            *negated,
            *any,
            expr,
            pattern,
            escape_char.as_ref(),
            false,
            params,
        ),
        Expr::ILike {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => bind_like_predicate(
            table,
            *negated,
            *any,
            expr,
            pattern,
            escape_char.as_ref(),
            true,
            params,
        ),
        Expr::IsNull(expr) => Ok(BoundPredicate::IsNull(bind_expr(table, expr, params)?)),
        Expr::IsNotNull(expr) => Ok(BoundPredicate::IsNotNull(bind_expr(table, expr, params)?)),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            if *negated {
                return Err(super::error::unsupported(
                    "NOT IN predicates are not supported",
                ));
            }
            Ok(BoundPredicate::In {
                expr: bind_expr(table, expr, params)?,
                values: list
                    .iter()
                    .map(|value| bind_expr(table, value, params))
                    .collect::<Result<Vec<_>, _>>()?,
            })
        }
        Expr::Value(value) if value.value == Value::Boolean(true) => Ok(BoundPredicate::True),
        Expr::Value(value) if value.value == Value::Boolean(false) => Ok(BoundPredicate::False),
        _ => Err(super::error::unsupported(format!(
            "unsupported SQL predicate '{expr}'"
        ))),
    }
}

#[expect(clippy::too_many_arguments)]
fn bind_like_predicate(
    table: &BoundTable,
    negated: bool,
    any: bool,
    expr: &Expr,
    pattern: &Expr,
    escape_char: Option<&Value>,
    case_insensitive: bool,
    params: &mut ParamBinder,
) -> Result<BoundPredicate, LixError> {
    if any {
        return Err(super::error::unsupported(
            "ANY in LIKE predicates is not supported",
        ));
    }
    let escape_char = match escape_char {
        // Keep the bound-write surface aligned with DataFusion's SQL planner:
        // its LIKE escape parser accepts one single-byte quoted character.
        Some(Value::SingleQuotedString(value)) if value.len() == 1 => {
            Some(value.chars().next().expect("single-character LIKE escape"))
        }
        Some(value) => {
            return Err(super::error::unsupported(format!(
                "invalid LIKE escape character; expected a single-quoted single character, got {value}"
            )));
        }
        None => None,
    };
    Ok(BoundPredicate::Like {
        expr: bind_expr(table, expr, params)?,
        pattern: bind_expr(table, pattern, params)?,
        negated,
        case_insensitive,
        escape_char,
    })
}

fn flatten_and_predicate(
    table: &BoundTable,
    expr: &Expr,
    params: &mut ParamBinder,
    predicates: &mut Vec<BoundPredicate>,
) -> Result<(), LixError> {
    match bind_predicate(table, expr, params)? {
        BoundPredicate::And(items) => predicates.extend(items),
        predicate => predicates.push(predicate),
    }
    Ok(())
}

fn flatten_or_predicate(
    table: &BoundTable,
    expr: &Expr,
    params: &mut ParamBinder,
    predicates: &mut Vec<BoundPredicate>,
) -> Result<(), LixError> {
    match bind_predicate(table, expr, params)? {
        BoundPredicate::Or(items) => predicates.extend(items),
        predicate => predicates.push(predicate),
    }
    Ok(())
}

fn bind_expr(
    table: &BoundTable,
    expr: &Expr,
    params: &mut ParamBinder,
) -> Result<BoundExpr, LixError> {
    match expr {
        Expr::Identifier(ident) => {
            let column_name = normalize_identifier(ident);
            Ok(BoundExpr::Column(bind_public_column_ref(
                table,
                &column_name,
            )?))
        }
        Expr::CompoundIdentifier(idents) if idents.len() == 2 => {
            let table_name = normalize_identifier(&idents[0]);
            if table_name != table.name {
                return Err(super::error::unsupported(format!(
                    "unknown SQL table qualifier '{table_name}'"
                )));
            }
            let column_name = normalize_identifier(&idents[1]);
            Ok(BoundExpr::Column(bind_public_column_ref(
                table,
                &column_name,
            )?))
        }
        Expr::Value(value) => bind_value(&value.value, params),
        Expr::Nested(expr) => bind_expr(table, expr, params),
        Expr::Cast {
            kind,
            expr,
            data_type,
            array,
            format,
        } => bind_cast_expr(
            kind,
            expr,
            data_type,
            *array,
            format.is_some(),
            params,
            |expr, params| bind_expr(table, expr, params),
        ),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => bind_negative_number_expr(expr),
        Expr::Function(function) => bind_function_expr(table, function, params),
        Expr::BinaryOp { left, op, right }
            if matches!(op, BinaryOperator::Arrow | BinaryOperator::LongArrow) =>
        {
            Ok(BoundExpr::Function {
                name: if *op == BinaryOperator::Arrow {
                    "__lix_json_get"
                } else {
                    "__lix_json_get_text"
                }
                .to_string(),
                args: vec![
                    bind_expr(table, left, params)?,
                    bind_expr(table, right, params)?,
                ],
            })
        }
        Expr::BinaryOp { left, op, right } => Ok(BoundExpr::Binary {
            left: Box::new(bind_expr(table, left, params)?),
            op: bind_arithmetic_operator(op)?,
            right: Box::new(bind_expr(table, right, params)?),
        }),
        _ => Err(super::error::unsupported(format!(
            "unsupported SQL expression '{expr}'"
        ))),
    }
}

fn bind_conflict_expr(
    table: &BoundTable,
    expr: &Expr,
    params: &mut ParamBinder,
) -> Result<BoundExpr, LixError> {
    match expr {
        Expr::CompoundIdentifier(idents) if idents.len() == 2 => {
            let qualifier = normalize_identifier(&idents[0]);
            if qualifier == "excluded" {
                let column_name = normalize_identifier(&idents[1]);
                return Ok(BoundExpr::ExcludedColumn(bind_public_column_ref(
                    table,
                    &column_name,
                )?));
            }
            bind_expr(table, expr, params)
        }
        Expr::Nested(expr) => bind_conflict_expr(table, expr, params),
        Expr::Cast {
            kind,
            expr,
            data_type,
            array,
            format,
        } => bind_cast_expr(
            kind,
            expr,
            data_type,
            *array,
            format.is_some(),
            params,
            |expr, params| bind_conflict_expr(table, expr, params),
        ),
        Expr::Function(function) => bind_function(function, params, |expr, params| {
            bind_conflict_expr(table, expr, params)
        }),
        Expr::BinaryOp { left, op, right }
            if matches!(op, BinaryOperator::Arrow | BinaryOperator::LongArrow) =>
        {
            Ok(BoundExpr::Function {
                name: if *op == BinaryOperator::Arrow {
                    "__lix_json_get"
                } else {
                    "__lix_json_get_text"
                }
                .to_string(),
                args: vec![
                    bind_conflict_expr(table, left, params)?,
                    bind_conflict_expr(table, right, params)?,
                ],
            })
        }
        Expr::BinaryOp { left, op, right } => Ok(BoundExpr::Binary {
            left: Box::new(bind_conflict_expr(table, left, params)?),
            op: bind_arithmetic_operator(op)?,
            right: Box::new(bind_conflict_expr(table, right, params)?),
        }),
        _ => bind_expr(table, expr, params),
    }
}

fn bind_arithmetic_operator(op: &BinaryOperator) -> Result<BoundBinaryOperator, LixError> {
    match op {
        BinaryOperator::Plus => Ok(BoundBinaryOperator::Add),
        BinaryOperator::Minus => Ok(BoundBinaryOperator::Subtract),
        BinaryOperator::Multiply => Ok(BoundBinaryOperator::Multiply),
        BinaryOperator::Divide => Ok(BoundBinaryOperator::Divide),
        BinaryOperator::Modulo => Ok(BoundBinaryOperator::Modulo),
        _ => Err(super::error::unsupported(format!(
            "unsupported SQL binary operator '{op}'"
        ))),
    }
}

fn bind_cast_expr(
    kind: &CastKind,
    expr: &Expr,
    data_type: &SqlDataType,
    array: bool,
    has_format: bool,
    params: &mut ParamBinder,
    bind_inner: impl FnOnce(&Expr, &mut ParamBinder) -> Result<BoundExpr, LixError>,
) -> Result<BoundExpr, LixError> {
    let data_type = bind_public_cast_type(kind, expr, data_type, array, has_format)?;
    let expr = bind_inner(expr, params)?;
    if data_type == BoundCastType::Jsonb {
        return match expr {
            BoundExpr::Literal(BoundLiteral::Text(raw)) => serde_json::from_str(&raw)
                .map(BoundLiteral::Json)
                .map(BoundExpr::Literal)
                .map_err(|error| {
                    LixError::new(
                        LixError::CODE_TYPE_MISMATCH,
                        format!("invalid JSONB literal: {error}"),
                    )
                }),
            BoundExpr::Literal(BoundLiteral::Json(_) | BoundLiteral::Null) => Ok(expr),
            _ => Ok(BoundExpr::Cast {
                expr: Box::new(expr),
                data_type,
            }),
        };
    }
    Ok(BoundExpr::Cast {
        expr: Box::new(expr),
        data_type,
    })
}

fn bind_value(value: &Value, params: &mut ParamBinder) -> Result<BoundExpr, LixError> {
    match value {
        Value::Null => Ok(BoundExpr::Literal(BoundLiteral::Null)),
        Value::Boolean(value) => Ok(BoundExpr::Literal(BoundLiteral::Bool(*value))),
        Value::SingleQuotedString(value) | Value::DoubleQuotedString(value) => {
            Ok(BoundExpr::Literal(BoundLiteral::Text(value.clone())))
        }
        Value::Number(value, _) => bind_number_literal(value),
        Value::Placeholder(name) => Ok(BoundExpr::Param(params.bind(name)?)),
        _ => Err(super::error::unsupported(format!(
            "unsupported SQL literal '{value}'"
        ))),
    }
}

fn bind_number_literal(value: &str) -> Result<BoundExpr, LixError> {
    if let Ok(value) = value.parse::<i64>() {
        return Ok(BoundExpr::Literal(BoundLiteral::Integer(value)));
    }
    let number = value.parse::<serde_json::Number>().or_else(|_| {
        value
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .ok_or(())
    });
    number
        .map(|number| BoundLiteral::Number {
            raw: value.to_string(),
            value: number,
        })
        .map(BoundExpr::Literal)
        .map_err(|()| super::error::unsupported(format!("unsupported numeric literal '{value}'")))
}

fn bind_negative_number_expr(expr: &Expr) -> Result<BoundExpr, LixError> {
    let Expr::Value(value) = expr else {
        return Err(super::error::unsupported(format!(
            "unsupported negative SQL expression '-{expr}'"
        )));
    };
    let Value::Number(value, _) = &value.value else {
        return Err(super::error::unsupported(format!(
            "unsupported negative SQL literal '-{}'",
            value.value
        )));
    };
    bind_number_literal(&format!("-{value}"))
}

fn bind_insert_value_function(
    function: &Function,
    params: &mut ParamBinder,
) -> Result<BoundExpr, LixError> {
    bind_function(function, params, |expr, params| {
        bind_insert_value_expr(expr, params)
    })
}

fn bind_function_expr(
    table: &BoundTable,
    function: &Function,
    params: &mut ParamBinder,
) -> Result<BoundExpr, LixError> {
    bind_function(function, params, |expr, params| {
        bind_expr(table, expr, params)
    })
}

fn bind_function(
    function: &Function,
    params: &mut ParamBinder,
    mut bind_arg_expr: impl FnMut(&Expr, &mut ParamBinder) -> Result<BoundExpr, LixError>,
) -> Result<BoundExpr, LixError> {
    reject_unsupported_function_modifiers(function)?;
    let name = bind_lix_function_name(function)?;
    let raw_args = function_args(&function.args)?;
    let args = raw_args
        .iter()
        .map(|arg| bind_arg_expr(arg, params))
        .collect::<Result<Vec<_>, _>>()?;
    validate_bound_function_arity(&name, args.len())?;
    Ok(BoundExpr::Function { name, args })
}

fn reject_unsupported_function_modifiers(function: &Function) -> Result<(), LixError> {
    if function.uses_odbc_syntax
        || !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return Err(super::error::unsupported(
            "SQL function modifiers are not supported by bound writes",
        ));
    }
    if let FunctionArguments::List(list) = &function.args {
        if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
            return Err(super::error::unsupported(
                "SQL function argument modifiers are not supported by bound writes",
            ));
        }
    }
    Ok(())
}

fn validate_bound_function_arity(name: &str, actual: usize) -> Result<(), LixError> {
    match name {
        "__lix_current_timestamp"
        | "uuidv7"
        | "lix_active_branch_id"
        | "lix_active_branch_commit_id" => expect_exact_function_arity(name, actual, 0),
        "__lix_json_get"
        | "__lix_json_get_text"
        | "__lix_json_path_get"
        | "__lix_json_path_get_text"
        | "__lix_json_contains"
        | "__lix_json_exists" => expect_exact_function_arity(name, actual, 2),
        "__lix_jsonb" => expect_exact_function_arity(name, actual, 1),
        _ => Err(super::error::unsupported(format!(
            "unsupported SQL function '{name}'"
        ))),
    }
}

fn expect_exact_function_arity(name: &str, actual: usize, expected: usize) -> Result<(), LixError> {
    if actual != expected {
        return Err(super::error::unsupported(format!(
            "{name} requires exactly {expected} argument"
        )));
    }
    Ok(())
}

fn bind_lix_function_name(function: &Function) -> Result<String, LixError> {
    if function.name.0.len() != 1 {
        return Err(super::error::unsupported(
            "qualified SQL function names are not supported by bound writes",
        ));
    }
    let Some(ObjectNamePart::Identifier(ident)) = function.name.0.first() else {
        return Err(super::error::unsupported(
            "unsupported SQL function name in bound write",
        ));
    };
    let name = if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    };
    match name.as_str() {
        "__lix_current_timestamp"
        | "uuidv7"
        | "lix_active_branch_id"
        | "lix_active_branch_commit_id"
        | "__lix_json_get"
        | "__lix_json_get_text"
        | "__lix_json_path_get"
        | "__lix_json_path_get_text"
        | "__lix_json_contains"
        | "__lix_json_exists"
        | "__lix_jsonb" => Ok(name),
        _ => Err(super::error::unsupported(format!(
            "unsupported SQL function '{name}'"
        ))),
    }
}

fn function_args(args: &FunctionArguments) -> Result<Vec<&Expr>, LixError> {
    let FunctionArguments::List(list) = args else {
        return Err(super::error::unsupported(
            "only ordinary SQL function argument lists are supported",
        ));
    };
    list.args
        .iter()
        .map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr),
            _ => Err(super::error::unsupported(
                "named, wildcard, and qualified function arguments are not supported",
            )),
        })
        .collect()
}

fn bind_exact_column_name(name: &ObjectName) -> Result<String, LixError> {
    if name.0.len() != 1 {
        return Err(super::error::unsupported(
            "qualified SQL column names are not supported",
        ));
    }
    name.0
        .first()
        .and_then(|part| part.as_ident())
        .map(normalize_identifier)
        .ok_or_else(|| super::error::unsupported("unsupported SQL column name"))
}

fn normalize_identifier(ident: &datafusion::sql::sqlparser::ast::Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

fn reject_duplicate_target_column(
    target_columns: &mut BTreeSet<String>,
    column_name: &str,
) -> Result<(), LixError> {
    if target_columns.insert(column_name.to_string()) {
        Ok(())
    } else {
        Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("duplicate write target column '{column_name}'"),
        ))
    }
}

fn require_write_capability(
    surface: &PublicSurfaceContract,
    op: BoundWriteOp,
) -> Result<(), LixError> {
    let allowed = match op {
        BoundWriteOp::Insert => surface.capabilities.insert,
        BoundWriteOp::Update => surface.capabilities.update,
        BoundWriteOp::Delete => surface.capabilities.delete,
    };
    if allowed {
        Ok(())
    } else {
        let mut error = LixError::new(
            LixError::CODE_READ_ONLY,
            format!("DML cannot write read-only SQL table '{}'", surface.name),
        );
        if let PublicSurfaceKind::SchemaBase { schema_key } = &surface.kind
            && let Some(hint) = crate::sql2::read_only::read_only_schema_surface_hint(schema_key)
        {
            error = error.with_hint(hint);
        }
        Err(error)
    }
}

fn bound_write_target(kind: &PublicSurfaceKind) -> BoundWriteTarget {
    match kind {
        PublicSurfaceKind::SchemaBase { schema_key } => {
            BoundWriteTarget::Row(RowWriteSurface::Base {
                schema_key: schema_key.clone(),
            })
        }
        PublicSurfaceKind::File => BoundWriteTarget::File(FileWriteSurface::Base),
        PublicSurfaceKind::Directory => BoundWriteTarget::Directory(DirectoryWriteSurface::Base),
        PublicSurfaceKind::Branch => BoundWriteTarget::Branch,
        PublicSurfaceKind::Revert => {
            BoundWriteTarget::DiffCommand(crate::sql2::DiffCommand::Revert)
        }
        PublicSurfaceKind::Apply => BoundWriteTarget::DiffCommand(crate::sql2::DiffCommand::Apply),
        PublicSurfaceKind::Change
        | PublicSurfaceKind::HistoryFunction
        | PublicSurfaceKind::DiffFunction
        | PublicSurfaceKind::CheckpointFunction
        | PublicSurfaceKind::StateAtFunction
        | PublicSurfaceKind::Restore
        | PublicSurfaceKind::CommitAncestryFunction => {
            unreachable!("write capability checked before target binding")
        }
    }
}

fn bound_insert_target(
    kind: &PublicSurfaceKind,
    input: &BoundWriteInput,
) -> Result<BoundWriteTarget, LixError> {
    if !matches!(kind, PublicSurfaceKind::Restore) {
        return Ok(bound_write_target(kind));
    }
    let BoundWriteInput::Values(values) = input else {
        return Err(super::error::unsupported(
            "lix_restore requires exactly one INSERT ... VALUES row",
        ));
    };
    let Some(commit_id_index) = values.column_index("commit_id") else {
        return Err(super::error::unsupported(
            "lix_restore requires the commit_id column",
        ));
    };
    let [row] = values.rows.as_slice() else {
        return Err(super::error::unsupported(
            "lix_restore requires exactly one INSERT ... VALUES row",
        ));
    };
    let Some(commit_id) = row.get(commit_id_index) else {
        return Err(super::error::unsupported(
            "lix_restore requires the commit_id column",
        ));
    };
    Ok(BoundWriteTarget::Restore {
        commit_id: commit_id.clone(),
    })
}

fn bind_write_branch_scope(
    kind: &PublicSurfaceKind,
    input: &BoundWriteInput,
    predicate: &BoundPredicate,
    active_branch_id: &str,
) -> Result<BranchScope, LixError> {
    let _ = input;
    Ok(bind_base_write_branch_scope(
        kind,
        predicate,
        active_branch_id,
    ))
}

fn bind_base_write_branch_scope(
    kind: &PublicSurfaceKind,
    predicate: &BoundPredicate,
    active_branch_id: &str,
) -> BranchScope {
    if predicate == &BoundPredicate::False {
        return BranchScope::Empty;
    }
    if matches!(kind, PublicSurfaceKind::Branch) {
        return BranchScope::Global;
    }
    active_branch_scope(active_branch_id)
}

fn active_branch_scope(active_branch_id: &str) -> BranchScope {
    BranchScope::Active {
        branch_id: active_branch_id.to_string(),
    }
}

#[derive(Default)]
struct ParamBinder {
    params: BTreeMap<usize, BoundParamRef>,
}

impl ParamBinder {
    fn bind(&mut self, name: &str) -> Result<BoundParamRef, LixError> {
        let index = name
            .strip_prefix('$')
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|index| *index > 0)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_PARSE_ERROR,
                    format!("unsupported SQL parameter placeholder '{name}'"),
                )
                .with_hint("Use PostgreSQL-style numbered placeholders like $1, $2, ...")
            })?;
        let param = BoundParamRef { index };
        self.params.entry(index).or_insert(param);
        Ok(param)
    }

    fn into_map(self) -> BoundParamMap {
        BoundParamMap {
            params: self.params,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::parser::Statement as DataFusionStatement;

    #[test]
    fn bind_statement_binds_restore_command_sink() {
        let statement = parse_statement(
            "INSERT INTO lix_restore (commit_id) VALUES ($1) RETURNING commit_id",
        );
        let bound = bind_statement(&statement, &[], "branch1").expect("restore should bind");

        assert!(matches!(
            bound.target,
            BoundWriteTarget::Restore {
                commit_id: BoundExpr::Param(BoundParamRef { index: 1 }),
            }
        ));
        assert_eq!(
            bound.branch_scope,
            BranchScope::Active {
                branch_id: "branch1".to_string()
            }
        );
    }

    #[test]
    fn bind_statement_rejects_removed_restore_scalar_syntax() {
        let sql = "SELECT lix_restore($1)";
        let error = bind_statement(&parse_statement(sql), &[], "branch1")
            .expect_err("the removed scalar-shaped restore command must fail");
        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
    }

    #[test]
    fn bind_statement_uses_exact_table_binding_for_write_targets() {
        let statement = parse_statement("INSERT INTO foo.lix_file (id) VALUES ('file1')");
        let error = bind_statement(&statement, &[], "branch1")
            .expect_err("qualified write target should be rejected by the binder");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error.message.contains("qualified SQL table names"));
    }

    #[test]
    fn bind_statement_rejects_hidden_insert_columns() {
        let statement = parse_statement(
            "INSERT INTO lix_file (id, path, directory_id, name, content, lixcol_schema_key) VALUES ('file1', '/a', null, 'a', null, 'schema')",
        );
        let error = bind_statement(&statement, &[], "branch1")
            .expect_err("hidden columns should not bind through statement binder");

        assert_eq!(error.code, LixError::CODE_COLUMN_NOT_FOUND);
        assert!(error.message.contains("not part of public SQL surface"));
    }

    #[test]
    fn bind_statement_rejects_implicit_insert_columns() {
        let statement = parse_statement("INSERT INTO lix_file VALUES ('file1')");
        let error = bind_statement(&statement, &[], "branch1")
            .expect_err("implicit insert column list should fail closed");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(
            error
                .message
                .contains("INSERT requires an explicit public column list")
        );
    }

    #[test]
    fn bind_statement_rejects_default_values_for_non_row_tables() {
        let statement = parse_statement("INSERT INTO lix_file DEFAULT VALUES");
        let error = bind_statement(&statement, &[], "branch1")
            .expect_err("DEFAULT VALUES should remain unsupported outside registered schemas");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(
            error
                .message
                .contains("INSERT requires an explicit public column list")
        );
    }

    #[test]
    fn bind_statement_binds_standard_insert_default_values_as_one_empty_row() {
        let statement = parse_statement("INSERT INTO test_state_schema DEFAULT VALUES");
        let bound = bind_statement(
            &statement,
            &[serde_json::json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "test_state_schema",
                "columns": [
                    { "name": "id", "type": "uuid", "nullable": false, "default_expression": "uuidv7()" },
                    { "name": "label", "type": "text", "nullable": false, "default_value": "untitled" },
                ],
                "primary_key": ["id"],
            })],
            "branch1",
        )
        .expect("standard DEFAULT VALUES should bind");

        assert!(matches!(
            bound.target,
            BoundWriteTarget::Row(RowWriteSurface::Base { .. })
        ));
        assert!(matches!(
            bound.branch_scope,
            BranchScope::Active { ref branch_id } if branch_id == "branch1"
        ));
        let BoundWriteInput::Values(values) = bound.input else {
            panic!("DEFAULT VALUES should bind as a VALUES input");
        };
        assert!(values.columns.is_empty());
        assert_eq!(values.rows, vec![Vec::new()]);
    }

    #[test]
    fn bind_statement_rejects_row_insert_select() {
        let statement = parse_statement(
            "INSERT INTO test_state_schema (lixcol_row_pk, value) SELECT '[\"a\"]'::jsonb, 'A'",
        );
        let error = bind_statement(
            &statement,
            &[serde_json::json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "test_state_schema",
                "columns": [
                    { "name": "value", "type": "text", "nullable": false },
                ],
                "primary_key": ["value"],
            })],
            "branch1",
        )
        .expect_err("row INSERT SELECT should fail closed at binding");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(
            error
                .message
                .contains("INSERT ... SELECT is not supported for schema SQL surfaces yet")
        );
    }

    #[test]
    fn bind_statement_rejects_duplicate_insert_columns() {
        let statement = parse_statement("INSERT INTO lix_file (id, id) VALUES ('file1', 'file2')");
        let error = bind_statement(&statement, &[], "branch1")
            .expect_err("duplicate insert columns should be rejected");

        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(error.message.contains("duplicate write target column 'id'"));
    }

    #[test]
    fn bind_statement_rejects_duplicate_update_columns() {
        let statement = parse_statement("UPDATE lix_file SET name = 'a', name = 'b'");
        let error = bind_statement(&statement, &[], "branch1")
            .expect_err("duplicate update columns should be rejected");

        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(
            error
                .message
                .contains("duplicate write target column 'name'")
        );
    }

    #[test]
    fn bind_statement_rejects_removed_history_suffix_writes() {
        let statement = parse_statement("DELETE FROM lix_file_history");
        let error = bind_statement(&statement, &[], "branch1")
            .expect_err("history suffix surfaces should not exist");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
    }

    #[test]
    fn bind_statement_preserves_update_assignment_and_predicate() {
        let statement =
            parse_statement("UPDATE test_state_schema SET name = 'next' WHERE id = 'row-1'");
        let bound = bind_statement(
            &statement,
            &[serde_json::json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "test_state_schema",
                "columns": [
                    { "name": "id", "type": "text", "nullable": false },
                    { "name": "name", "type": "text", "nullable": true },
                ],
                "primary_key": ["id"],
            })],
            "branch1",
        )
        .expect("write body should bind");

        let write = bound;
        assert!(matches!(
            write.target,
            BoundWriteTarget::Row(RowWriteSurface::Base { .. })
        ));
        assert_eq!(write.op, BoundWriteOp::Update);
        assert_eq!(write.assignments.len(), 1);
        assert_eq!(write.assignments[0].column.name, "name");
        assert!(matches!(
            write.assignments[0].value,
            BoundExpr::Literal(BoundLiteral::Text(ref value)) if value == "next"
        ));
        assert!(matches!(
            write.predicate,
            BoundPredicate::Eq(
                BoundExpr::Column(ref column),
                BoundExpr::Literal(BoundLiteral::Text(ref value)),
            ) if column.name == "id" && value == "row-1"
        ));
        assert!(matches!(
            write.branch_scope,
            BranchScope::Active { ref branch_id } if branch_id == "branch1"
        ));
    }

    #[test]
    fn bind_statement_rejects_hidden_predicate_columns() {
        let statement = parse_statement("DELETE FROM lix_file WHERE lixcol_schema_key = 'schema'");
        let error = bind_statement(&statement, &[], "branch1")
            .expect_err("hidden predicate columns should not bind");

        assert_eq!(error.code, LixError::CODE_COLUMN_NOT_FOUND);
        assert!(error.message.contains("not part of public SQL surface"));
    }

    #[test]
    fn bind_statement_binds_insert_values_and_params_once() {
        let statement = parse_statement("INSERT INTO lix_file (id, name) VALUES ($1, $2)");
        let bound = bind_statement(&statement, &[], "branch1").expect("insert should bind");

        let write = bound;
        assert_eq!(write.op, BoundWriteOp::Insert);
        assert_eq!(
            write.params.params.keys().copied().collect::<Vec<_>>(),
            vec![1, 2]
        );
        let BoundWriteInput::Values(values) = write.input else {
            panic!("expected values input");
        };
        assert_eq!(
            values
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "name"]
        );
        assert_eq!(values.rows.len(), 1);
        assert_eq!(values.rows[0].len(), 2);
        assert!(
            values.rows[0]
                .iter()
                .any(|value| matches!(value, BoundExpr::Param(param) if param.index == 1))
        );
        assert!(
            values.rows[0]
                .iter()
                .any(|value| matches!(value, BoundExpr::Param(param) if param.index == 2))
        );
    }

    #[test]
    fn bind_statement_rejects_insert_values_column_refs() {
        let statement = parse_statement("INSERT INTO lix_file (id) VALUES (name)");
        let error = bind_statement(&statement, &[], "branch1")
            .expect_err("VALUES rows should not bind target table column refs");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(
            error
                .message
                .contains("unsupported INSERT VALUES expression")
        );
    }

    #[test]
    fn bind_statement_preserves_private_jsonb_casts() {
        let statement = parse_statement(
            "INSERT INTO app_json (id, payload, metadata) VALUES ('e1', '{\"id\":\"e1\"}'::jsonb, '{\"source\":\"test\"}'::jsonb)",
        );
        let bound = bind_statement(
            &statement,
            &[serde_json::json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "app_json",
                "columns": [
                    { "name": "id", "type": "text", "nullable": false },
                    { "name": "payload", "type": "jsonb", "nullable": false },
                    { "name": "metadata", "type": "jsonb", "nullable": true },
                ],
                "primary_key": ["id"],
            })],
            "branch1",
        )
        .expect("insert should bind");

        let write = bound;
        let BoundWriteInput::Values(values) = write.input else {
            panic!("expected values input");
        };
        assert_eq!(values.rows[0].len(), 3);
        assert!(
            values.rows[0]
                .iter()
                .filter(|value| matches!(value, BoundExpr::Function { name, .. } if name == "__lix_jsonb"))
                .count()
                >= 2
        );
    }

    #[test]
    fn bind_statement_binds_public_values_functions() {
        let statement = parse_statement(
            "INSERT INTO lix_file (id, path, content) VALUES (uuidv7(), CURRENT_TIMESTAMP, CAST('hello' AS BYTEA))",
        );
        let bound = bind_statement(&statement, &[], "branch1").expect("insert should bind");

        let write = bound;
        let BoundWriteInput::Values(values) = write.input else {
            panic!("expected values input");
        };
        let function_names = values.rows[0]
            .iter()
            .filter_map(|value| match value {
                BoundExpr::Function { name, .. } => Some(name.as_str()),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            function_names,
            BTreeSet::from(["__lix_current_timestamp", "uuidv7"])
        );
    }

    #[test]
    fn bind_statement_rejects_unsupported_function_details() {
        let sql = "INSERT INTO lix_file (id) VALUES (uuidv7() FILTER (WHERE false))";
        let error = bind_statement(&parse_statement(sql), &[], "branch1")
            .expect_err("unsupported function details should fail closed");
        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
    }

    #[test]
    fn bind_statement_binds_false_base_predicates_as_empty() {
        for sql in [
            "DELETE FROM lix_file WHERE false",
            "UPDATE lix_file SET name = 'renamed' WHERE false",
            "DELETE FROM lix_branch WHERE false",
        ] {
            let bound = bind_statement(&parse_statement(sql), &[], "branch1")
                .expect("no-match write should bind");
            let write = bound;
            assert_eq!(write.branch_scope, BranchScope::Empty, "{sql}");
        }
    }

    #[test]
    fn bind_statement_accepts_is_null_and_is_not_null_predicates() {
        for sql in [
            "DELETE FROM lix_file WHERE content IS NULL",
            "DELETE FROM lix_file WHERE content IS NOT NULL",
        ] {
            bind_statement(&parse_statement(sql), &[], "branch1")
                .unwrap_or_else(|error| panic!("{sql} should bind, got {error:?}"));
        }
    }

    #[test]
    fn bind_statement_rejects_dynamic_row_primary_key_updates() {
        let statement = parse_statement("UPDATE project_message SET id = 'm2' WHERE id = 'm1'");
        let error = bind_statement(
            &statement,
            &[serde_json::json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "project_message",
                "columns": [
                    { "name": "id", "type": "text", "nullable": false },
                    { "name": "body", "type": "text", "nullable": false },
                ],
                "primary_key": ["id"],
            })],
            "branch1",
        )
        .expect_err("row primary key columns should be insert-only");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error.message.contains("is not writable"));
    }

    #[test]
    fn bind_statement_binds_branch_writes_as_global() {
        let statement =
            parse_statement("INSERT INTO lix_branch (id, name) VALUES ('draft', 'Draft')");
        let bound = bind_statement(&statement, &[], "branch1").expect("insert should bind");

        let write = bound;
        assert_eq!(write.branch_scope, BranchScope::Global);
    }

    #[test]
    fn bind_statement_binds_negative_numeric_literals() {
        let statement = parse_statement("UPDATE lix_file SET name = -1 WHERE id = 'file1'");
        let bound = bind_statement(&statement, &[], "branch1").expect("update should bind");

        let write = bound;
        assert!(matches!(
            write.assignments[0].value,
            BoundExpr::Literal(BoundLiteral::Integer(-1))
        ));
    }

    #[test]
    fn bind_number_literal_keeps_sql_numbers_distinct_from_json() {
        for raw in ["1.0", "01.0", "9223372036854775808"] {
            let BoundExpr::Literal(BoundLiteral::Number { .. }) =
                bind_number_literal(raw).expect("SQL numeric literal should bind")
            else {
                panic!("{raw} should bind as a SQL number");
            };
        }
    }

    #[test]
    fn bind_statement_binds_delete_like_predicate_and_parameter() {
        let statement = parse_statement("DELETE FROM lix_file WHERE path NOT LIKE $1");
        let bound = bind_statement(&statement, &[], "branch1").expect("DELETE LIKE should bind");

        assert!(matches!(
            bound.predicate,
            BoundPredicate::Like {
                expr: BoundExpr::Column(ref column),
                pattern: BoundExpr::Param(param),
                negated: true,
                case_insensitive: false,
                escape_char: None,
            } if column.name == "path" && param.index == 1
        ));
        assert_eq!(
            bound.params.params.keys().copied().collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn bind_statement_expands_delete_returning_star_and_preserves_aliases() {
        let statement = parse_statement(
            "DELETE FROM lix_file WHERE id = 'readme' RETURNING *, path AS deleted_path",
        );
        let bound = bind_statement(&statement, &[], "branch1").expect("DELETE should bind");
        let returning = bound.returning.expect("RETURNING should be bound");

        assert_eq!(
            returning
                .items
                .iter()
                .map(|item| item.output_name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "id",
                "path",
                "directory_id",
                "name",
                "content",
                "lixcol_global",
                "lixcol_change_id",
                "lixcol_created_at",
                "lixcol_updated_at",
                "lixcol_untracked",
                "lixcol_metadata",
                "deleted_path",
            ]
        );
        assert!(matches!(
            returning.items.last().expect("aliased item").expr,
            BoundExpr::Column(ref column) if column.name == "path"
        ));
    }

    #[test]
    fn bind_statement_binds_returning_parameters_into_write_parameter_map() {
        let statement =
            parse_statement("DELETE FROM lix_file WHERE id = $1 RETURNING $2 AS marker");
        let bound = bind_statement(&statement, &[], "branch1").expect("DELETE should bind");

        assert_eq!(
            bound.params.params.keys().copied().collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(matches!(
            bound
                .returning
                .expect("RETURNING should be bound")
                .items
                .as_slice(),
            [BoundReturningItem {
                expr: BoundExpr::Param(param),
                output_name,
            }] if param.index == 2 && output_name == "marker"
        ));
    }

    #[test]
    fn bind_statement_allows_only_commit_id_for_diff_command_insert_returning() {
        let bound = bind_statement(
            &parse_statement(
                "INSERT INTO lix_revert (relation, row_pk) \
                 SELECT 'lix_key_value', CAST('[\"test\"]' AS JSONB) \
                 RETURNING commit_id AS created_commit_id",
            ),
            &[],
            "branch1",
        )
        .expect("diff command RETURNING commit_id should bind");
        assert!(matches!(
            bound
                .returning
                .expect("RETURNING should be bound")
                .items
                .as_slice(),
            [BoundReturningItem {
                expr: BoundExpr::Column(column),
                output_name,
            }] if column.name == "commit_id" && output_name == "created_commit_id"
        ));

        for sql in [
            "INSERT INTO lix_revert (relation, row_pk) SELECT 'lix_key_value', CAST('[\"test\"]' AS JSONB) RETURNING row_pk",
            "INSERT INTO lix_revert (relation, row_pk) SELECT 'lix_key_value', CAST('[\"test\"]' AS JSONB) RETURNING *",
        ] {
            let error = bind_statement(&parse_statement(sql), &[], "branch1")
                .expect_err("unsupported INSERT RETURNING shape should fail");
            assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL, "{sql}");
        }
    }

    #[test]
    fn bind_statement_binds_returning_for_registered_row_writes() {
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "project_task",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "title", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        let inserted = bind_statement(
            &parse_statement(
                "INSERT INTO project_task (id, title) VALUES ($1, $2) \
                 RETURNING id, title AS inserted_title",
            ),
            std::slice::from_ref(&schema),
            "branch1",
        )
        .expect("registered row INSERT RETURNING should bind");
        assert_eq!(
            inserted
                .returning
                .expect("INSERT RETURNING should be bound")
                .items
                .iter()
                .map(|item| item.output_name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "inserted_title"]
        );
        assert_eq!(
            inserted.params.params.keys().copied().collect::<Vec<_>>(),
            vec![1, 2]
        );

        let updated = bind_statement(
            &parse_statement(
                "UPDATE project_task SET title = $1 WHERE id = $2 \
                 RETURNING id, title AS updated_title, $3 AS marker",
            ),
            std::slice::from_ref(&schema),
            "branch1",
        )
        .expect("registered row UPDATE RETURNING should bind");
        assert_eq!(
            updated
                .returning
                .expect("UPDATE RETURNING should be bound")
                .items
                .iter()
                .map(|item| item.output_name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "updated_title", "marker"]
        );
        assert_eq!(
            updated.params.params.keys().copied().collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn bind_statement_binds_returning_on_every_writable_surface() {
        for sql in [
            "INSERT INTO lix_file (path) VALUES ('readme.md') RETURNING id",
            "INSERT INTO lix_directory (path) VALUES ('/docs/') RETURNING id",
            "UPDATE lix_file SET name = 'readme.md' RETURNING id",
            "INSERT INTO lix_branch (id, name) VALUES ('draft', 'Draft') RETURNING id",
        ] {
            let bound =
                bind_statement(&parse_statement(sql), &[], "branch1").unwrap_or_else(|error| {
                    panic!("writable surface RETURNING should bind for {sql}: {error:?}")
                });
            assert_eq!(
                bound
                    .returning
                    .expect("RETURNING should be bound")
                    .items
                    .iter()
                    .map(|item| item.output_name.as_str())
                    .collect::<Vec<_>>(),
                vec!["id"],
                "{sql}"
            );
        }
    }

    #[test]
    fn bind_statement_rejects_anonymous_placeholders() {
        let mut params = ParamBinder::default();
        let error = params
            .bind("?")
            .expect_err("anonymous placeholders are unsupported");

        assert_eq!(error.code, LixError::CODE_PARSE_ERROR);
        assert!(
            error
                .message
                .contains("unsupported SQL parameter placeholder")
        );
    }

    #[test]
    fn bind_statement_rejects_provider_read_only_update_columns() {
        let statement = parse_statement("UPDATE lix_file SET id = 'next'");
        let error = bind_statement(&statement, &[], "branch1")
            .expect_err("file identity columns are insert-only");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error.message.contains("is not writable"));
    }

    #[test]
    fn bind_statement_rejects_explain_wrappers() {
        let statement =
            parse_statement("EXPLAIN UPDATE lix_file SET name = 'x' WHERE id = 'file1'");
        let error = bind_statement(&statement, &[], "branch1")
            .expect_err("EXPLAIN should not bind as a write");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(
            error
                .message
                .contains("EXPLAIN statements are not supported")
        );
    }

    #[test]
    fn bind_statement_rejects_unsupported_write_clauses() {
        let statement =
            parse_statement("UPDATE lix_file AS f SET name = 'next' WHERE f.id = 'file1'");
        let error = bind_statement(&statement, &[], "branch1")
            .expect_err("target aliases should not be ignored");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(
            error
                .message
                .contains("DML target aliases are not supported")
        );
    }

    fn parse_statement(sql: &str) -> DataFusionStatement {
        crate::sql2::parse_statement(sql).expect("parse SQL")
    }
}
