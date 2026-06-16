use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;

use datafusion::sql::parser::Statement as DataFusionStatement;
use datafusion::sql::sqlparser::ast::{
    AssignmentTarget, BinaryOperator, ConflictTarget, Delete, Expr, FromTable, Function,
    FunctionArg, FunctionArgExpr, FunctionArguments, Insert, ObjectName, ObjectNamePart,
    OnConflictAction, OnInsert, Query, SetExpr, Statement as SqlStatement, TableFactor,
    TableObject, TableWithJoins, UnaryOperator, Update, Value, Visit, Visitor,
};
use serde_json::Value as JsonValue;

use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
use crate::sql2::catalog::{PublicCatalog, PublicSurfaceContract, PublicSurfaceKind};
use crate::sql2::plan::branch_scope::BranchScope;
use crate::sql2::plan::predicate::BoundPredicate;

use super::expr::{BoundExpr, BoundLiteral, BoundParamRef};
use super::read::BoundRead;
use super::table::{
    BoundTable, bind_public_column_ref, bind_public_table, require_writable_column,
};
use super::write::{
    BoundAssignment, BoundConflictAction, BoundInsertConflict, BoundInsertValues, BoundParamMap,
    BoundWrite,
    BoundWriteInput, BoundWriteOp, BoundWriteTarget, DirectoryWriteSurface, EntityWriteSurface,
    FileWriteSurface,
};

pub(crate) fn bind_statement(
    statement: &DataFusionStatement,
    visible_schemas: &[JsonValue],
    active_branch_id: &str,
) -> Result<BoundWrite, LixError> {
    let catalog = PublicCatalog::from_visible_schemas(visible_schemas)?;
    match statement {
        DataFusionStatement::Statement(statement) => {
            bind_sql_statement(statement, &catalog, active_branch_id)
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
    if insert.columns.is_empty() {
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
    let input = bind_insert_input(
        &table.surface.kind,
        &columns,
        insert.source.as_deref(),
        &mut params,
    )?;
    let conflict = bind_insert_conflict(insert.on.as_ref(), &table, &mut params)?;
    if conflict.is_some() {
        if !matches!(
            table.surface.kind,
            PublicSurfaceKind::EntityBase { .. }
                | PublicSurfaceKind::EntityByBranch { .. }
                | PublicSurfaceKind::LixState
                | PublicSurfaceKind::LixStateByBranch
                | PublicSurfaceKind::Branch
                | PublicSurfaceKind::File
                | PublicSurfaceKind::FileByBranch
                | PublicSurfaceKind::Directory
                | PublicSurfaceKind::DirectoryByBranch
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
    Ok(BoundWrite {
        target: bound_write_target(&table.surface.kind),
        op: BoundWriteOp::Insert,
        input,
        predicate: BoundPredicate::True,
        assignments: Vec::new(),
        conflict,
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
        params: params.into_map(),
        branch_scope,
    })
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
    if insert.returning.is_some() {
        return Err(super::error::unsupported(
            "INSERT RETURNING is not supported",
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
    if update.returning.is_some() {
        return Err(super::error::unsupported(
            "UPDATE RETURNING is not supported",
        ));
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
    if delete.returning.is_some() {
        return Err(super::error::unsupported(
            "DELETE RETURNING is not supported",
        ));
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
    reject_unsupported_insert_query_clauses(source)?;
    let SetExpr::Values(values) = source.body.as_ref() else {
        if matches!(
            surface_kind,
            PublicSurfaceKind::EntityBase { .. } | PublicSurfaceKind::EntityByBranch { .. }
        ) {
            return Err(super::error::unsupported(
                "INSERT ... SELECT is not supported for entity SQL surfaces yet",
            ));
        }
        if columns
            .iter()
            .any(|column| column.table == "lix_file" && column.name == "data")
        {
            return Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                "lix_file.data expects binary data",
            )
            .with_hint("Use X'...' or a binary parameter for file contents."));
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
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => bind_negative_number_expr(expr),
        Expr::Function(function) => bind_insert_value_function(function, params),
        _ => Err(super::error::unsupported(format!(
            "unsupported INSERT VALUES expression '{expr}'"
        ))),
    }
}

fn reject_unsupported_insert_query_clauses(source: &Query) -> Result<(), LixError> {
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
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => bind_negative_number_expr(expr),
        Expr::Function(function) => bind_function_expr(table, function, params),
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
        Expr::Function(function) => bind_function(function, params, |expr, params| {
            bind_conflict_expr(table, expr, params)
        }),
        _ => bind_expr(table, expr, params),
    }
}

fn bind_value(value: &Value, params: &mut ParamBinder) -> Result<BoundExpr, LixError> {
    match value {
        Value::Null => Ok(BoundExpr::Literal(BoundLiteral::Null)),
        Value::Boolean(value) => Ok(BoundExpr::Literal(BoundLiteral::Bool(*value))),
        Value::SingleQuotedString(value) | Value::DoubleQuotedString(value) => {
            Ok(BoundExpr::Literal(BoundLiteral::Text(value.clone())))
        }
        Value::HexStringLiteral(value) => decode_hex_literal(value),
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
    let value = value
        .parse::<f64>()
        .map_err(|_| super::error::unsupported(format!("unsupported numeric literal '{value}'")))?;
    let Some(number) = serde_json::Number::from_f64(value) else {
        return Err(super::error::unsupported(
            "unsupported non-finite numeric literal",
        ));
    };
    Ok(BoundExpr::Literal(BoundLiteral::Json(JsonValue::Number(
        number,
    ))))
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

fn decode_hex_literal(value: &str) -> Result<BoundExpr, LixError> {
    if !value.len().is_multiple_of(2) {
        return Err(super::error::unsupported(format!(
            "hex literal has odd length '{value}'"
        )));
    }
    let bytes = value
        .as_bytes()
        .chunks_exact(2)
        .map(|chunk| {
            let high = hex_digit(chunk[0])?;
            let low = hex_digit(chunk[1])?;
            Ok((high << 4) | low)
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    Ok(BoundExpr::Literal(BoundLiteral::Blob(bytes)))
}

fn bind_insert_value_function(
    function: &Function,
    params: &mut ParamBinder,
) -> Result<BoundExpr, LixError> {
    if let Some(value) = bind_insert_lix_json_literal(function)? {
        return Ok(value);
    }
    bind_function(function, params, |expr, params| {
        bind_insert_value_expr(expr, params)
    })
}

fn bind_insert_lix_json_literal(function: &Function) -> Result<Option<BoundExpr>, LixError> {
    reject_unsupported_function_modifiers(function)?;
    let name = bind_lix_function_name(function)?;
    if name != "lix_json" {
        return Ok(None);
    }
    let raw_args = function_args(&function.args)?;
    validate_bound_function_arity(&name, raw_args.len())?;
    let Expr::Value(value) = raw_args[0] else {
        return Ok(None);
    };
    let (Value::SingleQuotedString(raw) | Value::DoubleQuotedString(raw)) = &value.value else {
        return Ok(None);
    };
    let value = serde_json::from_str(raw).map_err(|error| {
        LixError::new(
            LixError::CODE_TYPE_MISMATCH,
            format!("lix_json argument is not valid JSON: {error}"),
        )
    })?;
    Ok(Some(BoundExpr::Literal(BoundLiteral::Json(value))))
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
    validate_text_encoding_literal(&name, &raw_args)?;
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
        "lix_json" => expect_exact_function_arity(name, actual, 1),
        "lix_empty_blob" | "lix_timestamp" | "lix_uuid_v7" | "lix_active_branch_commit_id" => {
            expect_exact_function_arity(name, actual, 0)
        }
        "lix_json_get" | "lix_json_get_text" => expect_min_function_arity(name, actual, 2),
        "lix_text_encode" | "lix_text_decode" => {
            if (1..=2).contains(&actual) {
                Ok(())
            } else {
                Err(super::error::unsupported(format!(
                    "{name} requires 1 or 2 arguments"
                )))
            }
        }
        _ => Err(super::error::unsupported(format!(
            "unsupported SQL function '{name}'"
        ))),
    }
}

fn validate_text_encoding_literal(name: &str, args: &[&Expr]) -> Result<(), LixError> {
    if !matches!(name, "lix_text_encode" | "lix_text_decode") || args.len() < 2 {
        return Ok(());
    }
    let Expr::Value(value) = args[1] else {
        return Ok(());
    };
    let Some(encoding) = string_literal_value(&value.value) else {
        return Ok(());
    };
    let normalized = encoding.trim().to_ascii_uppercase().replace('-', "");
    if normalized == "UTF8" {
        Ok(())
    } else {
        Err(super::error::unsupported(format!(
            "{name} only supports UTF8 encoding, got '{encoding}'"
        )))
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

fn expect_min_function_arity(name: &str, actual: usize, minimum: usize) -> Result<(), LixError> {
    if actual < minimum {
        return Err(super::error::unsupported(format!(
            "{name} requires at least {minimum} arguments"
        )));
    }
    Ok(())
}

fn string_literal_value(value: &Value) -> Option<&str> {
    match value {
        Value::SingleQuotedString(value)
        | Value::DoubleQuotedString(value)
        | Value::TripleSingleQuotedString(value)
        | Value::TripleDoubleQuotedString(value)
        | Value::EscapedStringLiteral(value)
        | Value::UnicodeStringLiteral(value)
        | Value::NationalStringLiteral(value)
        | Value::SingleQuotedRawStringLiteral(value)
        | Value::DoubleQuotedRawStringLiteral(value)
        | Value::TripleSingleQuotedRawStringLiteral(value)
        | Value::TripleDoubleQuotedRawStringLiteral(value) => Some(value.as_str()),
        Value::DollarQuotedString(value) => Some(value.value.as_str()),
        _ => None,
    }
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
        "lix_json"
        | "lix_json_get"
        | "lix_json_get_text"
        | "lix_empty_blob"
        | "lix_timestamp"
        | "lix_uuid_v7"
        | "lix_active_branch_commit_id"
        | "lix_text_encode"
        | "lix_text_decode" => Ok(name),
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

fn hex_digit(byte: u8) -> Result<u8, LixError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(super::error::unsupported(format!(
            "invalid hex literal digit '{}'",
            byte as char
        ))),
    }
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
        if matches!(
            surface.kind,
            PublicSurfaceKind::EntityHistory { .. }
                | PublicSurfaceKind::FileHistory
                | PublicSurfaceKind::DirectoryHistory
                | PublicSurfaceKind::History
        ) {
            error = error.with_hint("History views are query-only.");
        }
        Err(error)
    }
}

fn bound_write_target(kind: &PublicSurfaceKind) -> BoundWriteTarget {
    match kind {
        PublicSurfaceKind::LixState => BoundWriteTarget::LixState,
        PublicSurfaceKind::LixStateByBranch => BoundWriteTarget::LixStateByBranch,
        PublicSurfaceKind::EntityBase { schema_key } => {
            BoundWriteTarget::Entity(EntityWriteSurface::Base {
                schema_key: schema_key.clone(),
            })
        }
        PublicSurfaceKind::EntityByBranch { schema_key } => {
            BoundWriteTarget::Entity(EntityWriteSurface::ByBranch {
                schema_key: schema_key.clone(),
            })
        }
        PublicSurfaceKind::File => BoundWriteTarget::File(FileWriteSurface::Base),
        PublicSurfaceKind::FileByBranch => BoundWriteTarget::File(FileWriteSurface::ByBranch),
        PublicSurfaceKind::Directory => BoundWriteTarget::Directory(DirectoryWriteSurface::Base),
        PublicSurfaceKind::DirectoryByBranch => {
            BoundWriteTarget::Directory(DirectoryWriteSurface::ByBranch)
        }
        PublicSurfaceKind::Branch => BoundWriteTarget::Branch,
        PublicSurfaceKind::EntityHistory { .. }
        | PublicSurfaceKind::FileHistory
        | PublicSurfaceKind::DirectoryHistory
        | PublicSurfaceKind::Change
        | PublicSurfaceKind::History => {
            unreachable!("write capability checked before target binding")
        }
    }
}

fn bind_write_branch_scope(
    kind: &PublicSurfaceKind,
    input: &BoundWriteInput,
    predicate: &BoundPredicate,
    active_branch_id: &str,
) -> Result<BranchScope, LixError> {
    let Some(branch_column) = by_branch_column_name(kind) else {
        return bind_base_write_branch_scope(kind, input, predicate, active_branch_id);
    };
    let branch_selector = match input {
        BoundWriteInput::Values(values) => {
            let mut selector = BranchSelector::Missing;
            if let Some(column_index) = values.column_index(branch_column) {
                for row in &values.rows {
                    let value = &row[column_index];
                    selector = selector.union(value_branch_selector(value)?);
                }
            }
            selector
        }
        BoundWriteInput::None => predicate_branch_selector(predicate, branch_column)?,
        BoundWriteInput::Query { .. } => Err(super::error::unsupported(
            "INSERT ... SELECT by-branch writes are not supported",
        ))?,
    };
    if matches!(kind, PublicSurfaceKind::LixStateByBranch) {
        let global_selector = match input {
            BoundWriteInput::Values(values) => {
                let mut selector = GlobalSelector::Missing;
                let global_column_index = values.column_index("global");
                for row in &values.rows {
                    selector =
                        selector.union(insert_row_global_selector(global_column_index, row)?);
                }
                selector
            }
            BoundWriteInput::None => predicate_global_selector(predicate)?,
            BoundWriteInput::Query { .. } => GlobalSelector::Missing,
        };
        return lix_state_by_branch_scope(input, branch_column, branch_selector, global_selector);
    }
    by_branch_scope(input, branch_column, branch_selector)
}

fn by_branch_scope(
    input: &BoundWriteInput,
    branch_column: &str,
    selector: BranchSelector,
) -> Result<BranchScope, LixError> {
    match (input, selector) {
        (_, selector) if selector.is_empty() => Ok(BranchScope::Empty),
        (BoundWriteInput::Values(_), BranchSelector::Missing) => Err(super::error::unsupported(
            format!("INSERT into by-branch SQL table requires explicit '{branch_column}'"),
        )),
        (BoundWriteInput::Values(_), BranchSelector::Static(branch_ids)) => {
            Ok(BranchScope::Explicit { branch_ids })
        }
        (
            BoundWriteInput::Values(_),
            BranchSelector::Dynamic {
                branch_ids,
                param_indexes,
            },
        ) => Ok(BranchScope::ExplicitDynamic {
            branch_ids,
            param_indexes,
        }),
        (BoundWriteInput::None, BranchSelector::Missing) => Err(super::error::unsupported(
            format!("by-branch SQL writes require an explicit '{branch_column}' predicate"),
        )),
        (BoundWriteInput::None, BranchSelector::Static(branch_ids)) => {
            Ok(BranchScope::ExplicitRequired { branch_ids })
        }
        (
            BoundWriteInput::None,
            BranchSelector::Dynamic {
                branch_ids,
                param_indexes,
            },
        ) => Ok(BranchScope::ExplicitRequiredDynamic {
            branch_ids,
            param_indexes,
        }),
        (BoundWriteInput::Query { .. }, _) => Err(super::error::unsupported(
            "INSERT ... SELECT by-branch writes are not supported",
        )),
    }
}

fn lix_state_by_branch_scope(
    input: &BoundWriteInput,
    branch_column: &str,
    branch_selector: BranchSelector,
    global_selector: GlobalSelector,
) -> Result<BranchScope, LixError> {
    if matches!(global_selector, GlobalSelector::Empty) || branch_selector.is_empty() {
        return Ok(BranchScope::Empty);
    }

    match global_selector {
        GlobalSelector::Static(true) => match branch_selector {
            BranchSelector::Missing => Ok(BranchScope::Global),
            BranchSelector::Static(branch_ids)
                if branch_ids == BTreeSet::from([GLOBAL_BRANCH_ID.to_string()]) =>
            {
                Ok(BranchScope::Global)
            }
            BranchSelector::Static(_) => Err(super::error::unsupported(
                "lix_state_by_branch writes cannot combine global = true with non-global branch_id",
            )),
            BranchSelector::Dynamic { .. } => {
                by_branch_scope(input, branch_column, branch_selector)
            }
        },
        GlobalSelector::Static(false) => match &branch_selector {
            BranchSelector::Static(branch_ids) if branch_ids.contains(GLOBAL_BRANCH_ID) => {
                Err(super::error::unsupported(
                    "lix_state_by_branch writes cannot combine global = false with global branch_id",
                ))
            }
            _ => by_branch_scope(input, branch_column, branch_selector),
        },
        GlobalSelector::Dynamic => by_branch_scope(input, branch_column, branch_selector),
        GlobalSelector::Missing => match &branch_selector {
            BranchSelector::Static(branch_ids)
                if branch_ids == &BTreeSet::from([GLOBAL_BRANCH_ID.to_string()]) =>
            {
                Ok(BranchScope::Global)
            }
            BranchSelector::Static(branch_ids) if branch_ids.contains(GLOBAL_BRANCH_ID) => {
                Err(super::error::unsupported(
                    "lix_state_by_branch writes cannot mix global and non-global branch scopes",
                ))
            }
            _ => by_branch_scope(input, branch_column, branch_selector),
        },
        GlobalSelector::Mixed => Err(super::error::unsupported(
            "lix_state_by_branch writes cannot mix global and branch-specific rows",
        )),
        GlobalSelector::Empty => Ok(BranchScope::Empty),
    }
}

fn bind_base_write_branch_scope(
    kind: &PublicSurfaceKind,
    input: &BoundWriteInput,
    predicate: &BoundPredicate,
    active_branch_id: &str,
) -> Result<BranchScope, LixError> {
    if predicate == &BoundPredicate::False {
        return Ok(BranchScope::Empty);
    }
    if matches!(kind, PublicSurfaceKind::Branch) {
        return Ok(BranchScope::Global);
    }
    if !matches!(kind, PublicSurfaceKind::LixState) {
        return Ok(active_branch_scope(active_branch_id));
    }
    match input {
        BoundWriteInput::Values(values) => {
            let mut selector = GlobalSelector::Missing;
            let global_column_index = values.column_index("global");
            for row in &values.rows {
                selector = selector.union(insert_row_global_selector(global_column_index, row)?);
            }
            match selector {
                GlobalSelector::Missing | GlobalSelector::Static(false) => {
                    Ok(active_branch_scope(active_branch_id))
                }
                GlobalSelector::Static(true) => Ok(BranchScope::Global),
                GlobalSelector::Empty => Ok(BranchScope::Empty),
                GlobalSelector::Dynamic => Err(super::error::unsupported(
                    "parameterized lix_state global scope selectors are not supported yet",
                )),
                GlobalSelector::Mixed => Err(super::error::unsupported(
                    "lix_state INSERT cannot mix global and active-branch rows",
                )),
            }
        }
        BoundWriteInput::None => match predicate_global_selector(predicate)? {
            GlobalSelector::Static(true) => Ok(BranchScope::Global),
            GlobalSelector::Static(false) | GlobalSelector::Missing => {
                Ok(active_branch_scope(active_branch_id))
            }
            GlobalSelector::Empty => Ok(BranchScope::Empty),
            GlobalSelector::Dynamic => Err(super::error::unsupported(
                "parameterized lix_state global scope selectors are not supported yet",
            )),
            GlobalSelector::Mixed => Err(super::error::unsupported(
                "lix_state global predicates select mixed branch scopes",
            )),
        },
        BoundWriteInput::Query { .. } => Ok(active_branch_scope(active_branch_id)),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GlobalSelector {
    Missing,
    Static(bool),
    Dynamic,
    Mixed,
    Empty,
}

impl GlobalSelector {
    fn union(self, other: Self) -> Self {
        match (self, other) {
            (Self::Mixed, _) | (_, Self::Mixed) => Self::Mixed,
            (Self::Dynamic, _) | (_, Self::Dynamic) => Self::Dynamic,
            (Self::Empty | Self::Missing, selector) | (selector, Self::Empty | Self::Missing) => {
                selector
            }
            (Self::Static(left), Self::Static(right)) if left == right => Self::Static(left),
            (Self::Static(_), Self::Static(_)) => Self::Mixed,
        }
    }

    fn intersect(self, other: Self) -> Self {
        match (self, other) {
            (Self::Empty, _) | (_, Self::Empty) => Self::Empty,
            (Self::Dynamic, Self::Missing) | (Self::Missing, Self::Dynamic) => Self::Dynamic,
            (Self::Dynamic, selector) | (selector, Self::Dynamic) => selector,
            (Self::Mixed, Self::Missing) | (Self::Missing, Self::Mixed) => Self::Mixed,
            (Self::Mixed | Self::Missing, selector) | (selector, Self::Mixed | Self::Missing) => {
                selector
            }
            (Self::Static(left), Self::Static(right)) if left == right => Self::Static(left),
            (Self::Static(_), Self::Static(_)) => Self::Empty,
        }
    }
}

fn insert_row_global_selector(
    column_index: Option<usize>,
    row: &[BoundExpr],
) -> Result<GlobalSelector, LixError> {
    let Some(column_index) = column_index else {
        return Ok(GlobalSelector::Missing);
    };
    global_selector_value(&row[column_index])
}

fn predicate_global_selector(predicate: &BoundPredicate) -> Result<GlobalSelector, LixError> {
    match predicate {
        BoundPredicate::True | BoundPredicate::IsNull(_) | BoundPredicate::IsNotNull(_) => {
            Ok(GlobalSelector::Missing)
        }
        BoundPredicate::False => Ok(GlobalSelector::Empty),
        BoundPredicate::And(predicates) => {
            let mut result = GlobalSelector::Missing;
            for predicate in predicates {
                result = result.intersect(predicate_global_selector(predicate)?);
            }
            Ok(result)
        }
        BoundPredicate::Or(predicates) => {
            let mut result = GlobalSelector::Empty;
            let mut has_missing_branch = false;
            for predicate in predicates {
                let selector = predicate_global_selector(predicate)?;
                if selector == GlobalSelector::Missing {
                    has_missing_branch = true;
                    continue;
                }
                result = result.union(selector);
            }
            if has_missing_branch {
                if result == GlobalSelector::Empty {
                    Ok(GlobalSelector::Missing)
                } else {
                    Ok(GlobalSelector::Mixed)
                }
            } else {
                Ok(result)
            }
        }
        BoundPredicate::Eq(left, right) => global_value_from_binary_exprs(left, right)
            .or_else(|| global_value_from_binary_exprs(right, left))
            .transpose()
            .map(|selector| selector.unwrap_or(GlobalSelector::Missing)),
        BoundPredicate::In { expr, values } => {
            let BoundExpr::Column(column) = expr else {
                return Ok(GlobalSelector::Missing);
            };
            if column.name != "global" {
                return Ok(GlobalSelector::Missing);
            }
            let mut result = GlobalSelector::Missing;
            for value in values {
                result = result.union(global_selector_value(value)?);
            }
            Ok(result)
        }
    }
}

fn global_value_from_binary_exprs(
    column_expr: &BoundExpr,
    value_expr: &BoundExpr,
) -> Option<Result<GlobalSelector, LixError>> {
    let BoundExpr::Column(column) = column_expr else {
        return None;
    };
    if column.name != "global" {
        return None;
    }
    Some(global_selector_value(value_expr))
}

fn global_selector_value(expr: &BoundExpr) -> Result<GlobalSelector, LixError> {
    match expr {
        BoundExpr::Literal(BoundLiteral::Bool(value)) => Ok(GlobalSelector::Static(*value)),
        BoundExpr::Param(_) => Ok(GlobalSelector::Dynamic),
        _ => Err(super::error::unsupported(
            "lix_state global predicates require boolean literals",
        )),
    }
}

fn by_branch_column_name(kind: &PublicSurfaceKind) -> Option<&'static str> {
    match kind {
        PublicSurfaceKind::LixStateByBranch => Some("branch_id"),
        PublicSurfaceKind::EntityByBranch { .. }
        | PublicSurfaceKind::FileByBranch
        | PublicSurfaceKind::DirectoryByBranch => Some("lixcol_branch_id"),
        _ => None,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum BranchSelector {
    Missing,
    Static(BTreeSet<String>),
    Dynamic {
        branch_ids: BTreeSet<String>,
        param_indexes: BTreeSet<usize>,
    },
}

impl BranchSelector {
    fn is_empty(&self) -> bool {
        matches!(self, Self::Static(branch_ids) if branch_ids.is_empty())
    }

    fn intersect(self, other: Self) -> Self {
        match (self, other) {
            (Self::Missing, selector) | (selector, Self::Missing) => selector,
            (Self::Static(branch_ids), Self::Dynamic { param_indexes, .. })
            | (Self::Dynamic { param_indexes, .. }, Self::Static(branch_ids))
                if branch_ids.is_empty() || param_indexes.is_empty() =>
            {
                Self::Static(BTreeSet::new())
            }
            (Self::Static(left), Self::Static(right)) => {
                Self::Static(left.intersection(&right).cloned().collect())
            }
            (
                Self::Dynamic {
                    mut branch_ids,
                    mut param_indexes,
                },
                Self::Dynamic {
                    branch_ids: right_branches,
                    param_indexes: right_params,
                },
            ) => {
                branch_ids.extend(right_branches);
                param_indexes.extend(right_params);
                Self::Dynamic {
                    branch_ids,
                    param_indexes,
                }
            }
            (
                Self::Static(mut branch_ids),
                Self::Dynamic {
                    branch_ids: right_branches,
                    param_indexes,
                },
            )
            | (
                Self::Dynamic {
                    branch_ids: right_branches,
                    param_indexes,
                },
                Self::Static(mut branch_ids),
            ) => {
                branch_ids.extend(right_branches);
                Self::Dynamic {
                    branch_ids,
                    param_indexes,
                }
            }
        }
    }

    fn union(self, other: Self) -> Self {
        match (self, other) {
            (Self::Missing, selector) | (selector, Self::Missing) => selector,
            (Self::Static(mut left), Self::Static(right)) => {
                left.extend(right);
                Self::Static(left)
            }
            (
                Self::Dynamic {
                    mut branch_ids,
                    mut param_indexes,
                },
                Self::Dynamic {
                    branch_ids: right_branches,
                    param_indexes: right_params,
                },
            ) => {
                branch_ids.extend(right_branches);
                param_indexes.extend(right_params);
                Self::Dynamic {
                    branch_ids,
                    param_indexes,
                }
            }
            (
                Self::Static(mut branch_ids),
                Self::Dynamic {
                    branch_ids: right_branches,
                    param_indexes,
                },
            )
            | (
                Self::Dynamic {
                    branch_ids: right_branches,
                    param_indexes,
                },
                Self::Static(mut branch_ids),
            ) => {
                branch_ids.extend(right_branches);
                Self::Dynamic {
                    branch_ids,
                    param_indexes,
                }
            }
        }
    }
}

fn predicate_branch_selector(
    predicate: &BoundPredicate,
    branch_column: &str,
) -> Result<BranchSelector, LixError> {
    match predicate {
        BoundPredicate::True | BoundPredicate::IsNull(_) | BoundPredicate::IsNotNull(_) => {
            Ok(BranchSelector::Missing)
        }
        BoundPredicate::False => Ok(BranchSelector::Static(BTreeSet::new())),
        BoundPredicate::And(predicates) => {
            let mut result = BranchSelector::Missing;
            for predicate in predicates {
                result = result.intersect(predicate_branch_selector(predicate, branch_column)?);
            }
            Ok(result)
        }
        BoundPredicate::Or(predicates) => {
            let mut result = BranchSelector::Static(BTreeSet::new());
            for predicate in predicates {
                let selector = predicate_branch_selector(predicate, branch_column)?;
                if selector == BranchSelector::Missing {
                    return Ok(BranchSelector::Missing);
                }
                result = result.union(selector);
            }
            Ok(result)
        }
        BoundPredicate::Eq(left, right) => {
            branch_selector_from_binary_exprs(left, right, branch_column)
                .or_else(|| branch_selector_from_binary_exprs(right, left, branch_column))
                .transpose()
                .map(|selector| selector.unwrap_or(BranchSelector::Missing))
        }
        BoundPredicate::In { expr, values } => {
            let BoundExpr::Column(column) = expr else {
                return Ok(BranchSelector::Missing);
            };
            if column.name != branch_column {
                return Ok(BranchSelector::Missing);
            }
            let mut selector = BranchSelector::Missing;
            for value in values {
                selector = selector.union(value_branch_selector(value)?);
            }
            Ok(selector)
        }
    }
}

fn branch_selector_from_binary_exprs(
    column_expr: &BoundExpr,
    value_expr: &BoundExpr,
    branch_column: &str,
) -> Option<Result<BranchSelector, LixError>> {
    let BoundExpr::Column(column) = column_expr else {
        return None;
    };
    if column.name != branch_column {
        return None;
    }
    Some(value_branch_selector(value_expr))
}

fn value_branch_selector(expr: &BoundExpr) -> Result<BranchSelector, LixError> {
    match expr {
        BoundExpr::Literal(BoundLiteral::Text(branch_id)) => {
            Ok(BranchSelector::Static(BTreeSet::from([branch_id.clone()])))
        }
        BoundExpr::Param(param) => Ok(BranchSelector::Dynamic {
            branch_ids: BTreeSet::new(),
            param_indexes: BTreeSet::from([param.index]),
        }),
        _ => Err(super::error::unsupported(
            "by-branch SQL write predicates require string branch ids",
        )),
    }
}

fn active_branch_scope(active_branch_id: &str) -> BranchScope {
    BranchScope::Active {
        branch_id: active_branch_id.to_string(),
    }
}

#[derive(Default)]
struct ParamBinder {
    next_implicit_index: usize,
    mode: Option<ParamMode>,
    params: BTreeMap<usize, BoundParamRef>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ParamMode {
    Implicit,
    Numbered,
}

impl ParamBinder {
    fn bind(&mut self, name: &str) -> Result<BoundParamRef, LixError> {
        let index = if name == "?" {
            self.require_mode(ParamMode::Implicit, name)?;
            self.next_implicit_index += 1;
            self.next_implicit_index
        } else {
            self.require_mode(ParamMode::Numbered, name)?;
            name.strip_prefix('$')
                .and_then(|raw| raw.parse::<usize>().ok())
                .filter(|index| *index > 0)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_PARSE_ERROR,
                        format!("unsupported SQL parameter placeholder '{name}'"),
                    )
                    .with_hint(
                        "Use placeholders like ?, ? or numbered placeholders like $1, $2, ...",
                    )
                })?
        };
        let param = BoundParamRef { index };
        self.params.entry(index).or_insert(param);
        Ok(param)
    }

    fn require_mode(&mut self, mode: ParamMode, name: &str) -> Result<(), LixError> {
        match self.mode {
            Some(existing) if existing != mode => Err(LixError::new(
                LixError::CODE_PARSE_ERROR,
                format!("cannot mix SQL parameter placeholder styles near '{name}'"),
            )
            .with_hint("Use either positional ? placeholders or numbered $1 placeholders.")),
            Some(_) => Ok(()),
            None => {
                self.mode = Some(mode);
                Ok(())
            }
        }
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
            "INSERT INTO lix_file (id, path, directory_id, name, data, lixcol_schema_key) VALUES ('file1', '/a', null, 'a', null, 'schema')",
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
    fn bind_statement_rejects_entity_insert_select() {
        let statement = parse_statement(
            "INSERT INTO test_state_schema (lixcol_entity_pk, value) SELECT lix_json('[\"a\"]'), 'A'",
        );
        let error = bind_statement(
            &statement,
            &[serde_json::json!({
                "x-lix-key": "test_state_schema",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
            "branch1",
        )
        .expect_err("entity INSERT SELECT should fail closed at binding");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(
            error
                .message
                .contains("INSERT ... SELECT is not supported for entity SQL surfaces yet")
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
    fn bind_statement_rejects_duplicate_lix_state_by_branch_insert_columns() {
        let statement = parse_statement(
            "INSERT INTO lix_state_by_branch (\
             entity_pk, schema_key, snapshot_content, branch_id, branch_id\
             ) VALUES (\
             '[\"entity1\"]', 'lix_key_value', '{\"key\":\"k\",\"value\":\"v\"}', 'branch1', 'branch2'\
             )",
        );
        let error = bind_statement(&statement, &[], "branch1")
            .expect_err("duplicate lix_state_by_branch insert columns should be rejected");

        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(
            error
                .message
                .contains("duplicate write target column 'branch_id'")
        );
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
    fn bind_statement_rejects_read_only_history_writes() {
        let statement = parse_statement("DELETE FROM lix_file_history");
        let error = bind_statement(&statement, &[], "branch1")
            .expect_err("history surfaces should be read-only");

        assert_eq!(error.code, LixError::CODE_READ_ONLY);
    }

    #[test]
    fn bind_statement_preserves_update_assignment_and_predicate() {
        let statement = parse_statement(
            "UPDATE test_state_schema_by_branch SET name = 'next' WHERE lixcol_branch_id = 'branch2'",
        );
        let bound = bind_statement(
            &statement,
            &[serde_json::json!({
                "x-lix-key": "test_state_schema",
                "properties": {
                    "id": { "type": "string" },
                    "name": { "type": "string" }
                }
            })],
            "branch1",
        )
        .expect("write body should bind");

        let write = bound;
        assert!(matches!(
            write.target,
            BoundWriteTarget::Entity(EntityWriteSurface::ByBranch { .. })
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
            ) if column.name == "lixcol_branch_id" && value == "branch2"
        ));
        assert!(matches!(
            write.branch_scope,
            BranchScope::ExplicitRequired { ref branch_ids }
                if branch_ids == &BTreeSet::from(["branch2".to_string()])
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
    fn bind_statement_binds_hex_literals_as_blobs() {
        let statement =
            parse_statement("INSERT INTO lix_file (id, data) VALUES ('file1', X'4142')");
        let bound = bind_statement(&statement, &[], "branch1").expect("insert should bind");

        let write = bound;
        let BoundWriteInput::Values(values) = write.input else {
            panic!("expected values input");
        };
        assert!(values.rows[0]
            .iter()
            .any(|value| matches!(value, BoundExpr::Literal(BoundLiteral::Blob(bytes)) if bytes == &vec![0x41, 0x42])));
    }

    #[test]
    fn bind_statement_predecodes_lix_json_literal_values() {
        let statement = parse_statement(
            "INSERT INTO lix_state (entity_pk, schema_key, snapshot_content) VALUES (lix_json('[\"e1\"]'), 'app.test', lix_json('{\"id\":\"e1\"}'))",
        );
        let bound = bind_statement(&statement, &[], "branch1").expect("insert should bind");

        let write = bound;
        let BoundWriteInput::Values(values) = write.input else {
            panic!("expected values input");
        };
        assert_eq!(values.rows[0].len(), 3);
        assert!(
            values.rows[0]
                .iter()
                .filter(|value| matches!(value, BoundExpr::Literal(BoundLiteral::Json(_))))
                .count()
                >= 2
        );
    }

    #[test]
    fn bind_statement_binds_public_values_functions() {
        let statement = parse_statement(
            "INSERT INTO lix_file (id, path, data) VALUES (lix_uuid_v7(), lix_timestamp(), lix_text_encode('hello'))",
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
            BTreeSet::from(["lix_text_encode", "lix_timestamp", "lix_uuid_v7"])
        );
    }

    #[test]
    fn bind_statement_rejects_unsupported_function_details() {
        for sql in [
            "INSERT INTO lix_file (id, data) VALUES ('f1', lix_text_encode('Ada', 'base64'))",
            "INSERT INTO lix_file (id, data) VALUES ('f1', lix_empty_blob() FILTER (WHERE false))",
        ] {
            let error = bind_statement(&parse_statement(sql), &[], "branch1")
                .expect_err("unsupported function details should fail closed");
            assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL, "{sql}");
        }
    }

    #[test]
    fn bind_statement_binds_by_branch_insert_scope_from_branch_column() {
        let statement = parse_statement(
            "INSERT INTO lix_file_by_branch (id, name, lixcol_branch_id) VALUES ('file1', 'a', 'branch2')",
        );
        let bound = bind_statement(&statement, &[], "branch1").expect("insert should bind");

        let write = bound;
        assert!(matches!(
            write.branch_scope,
            BranchScope::Explicit { ref branch_ids }
                if branch_ids == &BTreeSet::from(["branch2".to_string()])
        ));
    }

    #[test]
    fn bind_statement_preserves_parameterized_by_branch_scope_selectors() {
        let update = bind_statement(
            &parse_statement(
                "UPDATE lix_file_by_branch SET name = 'renamed.txt' WHERE id = 'file1' AND lixcol_branch_id = $1",
            ),
            &[],
            "branch1",
        )
        .expect("parameterized update branch scope should bind");
        assert_eq!(
            update.branch_scope,
            BranchScope::ExplicitRequiredDynamic {
                branch_ids: BTreeSet::new(),
                param_indexes: BTreeSet::from([1])
            }
        );

        let insert = bind_statement(
            &parse_statement(
                "INSERT INTO lix_file_by_branch (id, name, lixcol_branch_id) VALUES ('file1', 'a', $1)",
            ),
            &[],
            "branch1",
        )
        .expect("parameterized insert branch scope should bind");
        assert_eq!(
            insert.branch_scope,
            BranchScope::ExplicitDynamic {
                branch_ids: BTreeSet::new(),
                param_indexes: BTreeSet::from([1])
            }
        );
    }

    #[test]
    fn bind_statement_binds_contradictory_by_branch_selectors_as_empty() {
        let statement = parse_statement(
            "DELETE FROM lix_file_by_branch WHERE lixcol_branch_id IN ('v1') AND lixcol_branch_id IN ('v2')",
        );
        let bound = bind_statement(&statement, &[], "branch1").expect("delete should bind");

        let write = bound;
        assert_eq!(write.branch_scope, BranchScope::Empty);
    }

    #[test]
    fn bind_statement_binds_false_by_branch_predicate_as_empty() {
        let statement = parse_statement("DELETE FROM lix_file_by_branch WHERE false");
        let bound = bind_statement(&statement, &[], "branch1").expect("no-match delete binds");

        let write = bound;
        assert_eq!(write.branch_scope, BranchScope::Empty);
    }

    #[test]
    fn bind_statement_binds_false_base_predicates_as_empty() {
        for sql in [
            "DELETE FROM lix_file WHERE false",
            "UPDATE lix_state SET metadata = '{}' WHERE false",
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
            "DELETE FROM lix_file WHERE data IS NULL",
            "DELETE FROM lix_file WHERE data IS NOT NULL",
        ] {
            bind_statement(&parse_statement(sql), &[], "branch1")
                .unwrap_or_else(|error| panic!("{sql} should bind, got {error:?}"));
        }
    }

    #[test]
    fn bind_statement_binds_global_lix_state_insert_scope() {
        let statement = parse_statement(
            "INSERT INTO lix_state (entity_pk, schema_key, snapshot_content, global) VALUES ('[\"e1\"]', 'app.test', '{}', true)",
        );
        let bound = bind_statement(&statement, &[], "branch1").expect("insert should bind");

        let write = bound;
        assert_eq!(write.branch_scope, BranchScope::Global);
    }

    #[test]
    fn bind_statement_rejects_parameterized_lix_state_global_scope() {
        let statement = parse_statement(
            "INSERT INTO lix_state (entity_pk, schema_key, snapshot_content, global) VALUES ('[\"e1\"]', 'app.test', '{}', $1)",
        );
        let error = bind_statement(&statement, &[], "branch1")
            .expect_err("parameterized global scope should fail closed until scope resolution");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(
            error
                .message
                .contains("parameterized lix_state global scope selectors")
        );
    }

    #[test]
    fn bind_statement_rejects_lix_state_by_branch_global_true_with_branch_id() {
        let statement = parse_statement(
            "INSERT INTO lix_state_by_branch (entity_pk, schema_key, snapshot_content, branch_id, global) VALUES ('[\"e1\"]', 'app.test', '{}', 'v1', true)",
        );
        let error = bind_statement(&statement, &[], "branch1")
            .expect_err("global true and branch_id select contradictory scopes");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(
            error
                .message
                .contains("cannot combine global = true with non-global branch_id")
        );
    }

    #[test]
    fn bind_statement_binds_lix_state_by_branch_global_true_with_global_branch() {
        let statement = parse_statement(
            "INSERT INTO lix_state_by_branch (entity_pk, schema_key, snapshot_content, branch_id, global) VALUES ('[\"e1\"]', 'app.test', '{}', 'global', true)",
        );
        let bound = bind_statement(&statement, &[], "branch1").expect("global row should bind");

        let write = bound;
        assert_eq!(write.branch_scope, BranchScope::Global);
    }

    #[test]
    fn bind_statement_rejects_lix_state_by_branch_global_false_with_global_branch() {
        let statement = parse_statement(
            "INSERT INTO lix_state_by_branch (entity_pk, schema_key, snapshot_content, branch_id, global) VALUES ('[\"e1\"]', 'app.test', '{}', 'global', false)",
        );
        let error = bind_statement(&statement, &[], "branch1")
            .expect_err("global false cannot target the global branch");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(
            error
                .message
                .contains("cannot combine global = false with global branch_id")
        );
    }

    #[test]
    fn bind_statement_binds_parameterized_lix_state_by_branch_branch_id() {
        let statement = parse_statement(
            "INSERT INTO lix_state_by_branch (entity_pk, schema_key, snapshot_content, branch_id) VALUES ('[\"e1\"]', 'app.test', '{}', $1)",
        );
        let bound = bind_statement(&statement, &[], "branch1")
            .expect("parameterized lix_state_by_branch branch_id should bind");

        assert_eq!(
            bound.branch_scope,
            BranchScope::ExplicitDynamic {
                branch_ids: BTreeSet::new(),
                param_indexes: BTreeSet::from([1])
            }
        );
    }

    #[test]
    fn bind_statement_binds_parameterized_lix_state_by_branch_global_false_branch_id() {
        let statement = parse_statement(
            "INSERT INTO lix_state_by_branch (entity_pk, schema_key, snapshot_content, branch_id, global) VALUES ('[\"e1\"]', 'app.test', '{}', $1, false)",
        );
        let bound = bind_statement(&statement, &[], "branch1")
            .expect("parameterized lix_state_by_branch non-global row should bind");

        assert_eq!(
            bound.branch_scope,
            BranchScope::ExplicitDynamic {
                branch_ids: BTreeSet::new(),
                param_indexes: BTreeSet::from([1])
            }
        );
    }

    #[test]
    fn bind_statement_binds_contradictory_lix_state_global_predicates_as_empty() {
        let statement = parse_statement(
            "UPDATE lix_state SET metadata = '{}' WHERE global = true AND global = false",
        );
        let bound = bind_statement(&statement, &[], "branch1").expect("no-match scope should bind");

        let write = bound;
        assert_eq!(write.branch_scope, BranchScope::Empty);
    }

    #[test]
    fn bind_statement_rejects_mixed_or_lix_state_global_scope() {
        let statement = parse_statement(
            "UPDATE lix_state SET metadata = '{}' WHERE global = true OR schema_key = 'app.test'",
        );
        let error = bind_statement(&statement, &[], "branch1")
            .expect_err("mixed global OR scope should fail closed");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(
            error
                .message
                .contains("lix_state global predicates select mixed branch scopes"),
            "{}",
            error.message
        );
    }

    #[test]
    fn global_selector_mixed_intersect_missing_stays_mixed() {
        assert_eq!(
            GlobalSelector::Mixed.intersect(GlobalSelector::Missing),
            GlobalSelector::Mixed
        );
        assert_eq!(
            GlobalSelector::Missing.intersect(GlobalSelector::Mixed),
            GlobalSelector::Mixed
        );
    }

    #[test]
    fn bind_statement_rejects_dynamic_entity_primary_key_updates() {
        let statement = parse_statement("UPDATE project_message SET id = 'm2' WHERE id = 'm1'");
        let error = bind_statement(
            &statement,
            &[serde_json::json!({
                "x-lix-key": "project_message",
                "x-lix-primary-key": ["/id"],
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "body": { "type": "string" }
                },
                "required": ["id", "body"],
                "additionalProperties": false
            })],
            "branch1",
        )
        .expect_err("entity primary key columns should be insert-only");

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
        let statement = parse_statement(
            "UPDATE lix_state SET snapshot_content = -1 WHERE entity_pk = '[\"e1\"]'",
        );
        let bound = bind_statement(&statement, &[], "branch1").expect("update should bind");

        let write = bound;
        assert!(matches!(
            write.assignments[0].value,
            BoundExpr::Literal(BoundLiteral::Integer(-1))
        ));
    }

    #[test]
    fn bind_statement_rejects_by_branch_writes_without_branch_selector() {
        let statement = parse_statement(
            "UPDATE lix_file_by_branch SET name = 'renamed.txt' WHERE id = 'file1'",
        );
        let error = bind_statement(&statement, &[], "branch1")
            .expect_err("by-branch writes should require explicit branch predicate");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error.message.contains("require an explicit"));
    }

    #[test]
    fn bind_statement_rejects_mixed_placeholder_styles() {
        let mut params = ParamBinder::default();
        params.bind("?").expect("implicit placeholder should bind");
        let error = params
            .bind("$1")
            .expect_err("mixed placeholder styles should be rejected");

        assert_eq!(error.code, LixError::CODE_PARSE_ERROR);
        assert!(
            error
                .message
                .contains("cannot mix SQL parameter placeholder styles")
        );
    }

    #[test]
    fn bind_statement_rejects_read_only_by_branch_columns_as_write_targets() {
        let statement =
            parse_statement("UPDATE lix_file_by_branch SET lixcol_branch_id = 'branch2'");
        let error = bind_statement(&statement, &[], "branch1")
            .expect_err("by-branch branch columns are filter-only");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error.message.contains("is not writable"));
    }

    #[test]
    fn bind_statement_rejects_provider_read_only_update_columns() {
        let statement = parse_statement("UPDATE lix_state SET entity_pk = '[\"next\"]'");
        let error = bind_statement(&statement, &[], "branch1")
            .expect_err("lix_state identity columns are insert-only");

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
