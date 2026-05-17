use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;

use datafusion::sql::parser::Statement as DataFusionStatement;
use datafusion::sql::sqlparser::ast::{
    AssignmentTarget, BinaryOperator, Delete, Expr, FromTable, Function, FunctionArg,
    FunctionArgExpr, FunctionArguments, Insert, ObjectName, ObjectNamePart, Query, SetExpr,
    Statement as SqlStatement, TableFactor, TableObject, TableWithJoins, UnaryOperator, Update,
    Value, Visit, Visitor,
};
use serde_json::Value as JsonValue;

use crate::sql2::catalog::{PublicCatalog, PublicSurfaceContract, PublicSurfaceKind};
use crate::sql2::plan::predicate::BoundPredicate;
use crate::sql2::plan::version_scope::VersionScope;
use crate::LixError;
use crate::GLOBAL_VERSION_ID;

use super::expr::{BoundExpr, BoundLiteral, BoundParamRef};
use super::read::BoundRead;
use super::table::{
    bind_public_column_ref, bind_public_table, require_writable_column, BoundTable,
};
use super::write::{
    BoundAssignment, BoundInsertRow, BoundParamMap, BoundWrite, BoundWriteInput, BoundWriteOp,
    BoundWriteTarget, DirectoryWriteSurface, EntityWriteSurface, FileWriteSurface,
};

pub(crate) fn bind_statement(
    statement: &DataFusionStatement,
    visible_schemas: &[JsonValue],
    active_version_id: &str,
) -> Result<BoundWrite, LixError> {
    let catalog = PublicCatalog::from_visible_schemas(visible_schemas)?;
    match statement {
        DataFusionStatement::Statement(statement) => {
            bind_sql_statement(statement, &catalog, active_version_id)
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
    active_version_id: &str,
) -> Result<BoundWrite, LixError> {
    match statement {
        SqlStatement::Insert(insert) => bind_insert_bound(insert, catalog, active_version_id),
        SqlStatement::Update(update) => bind_update_bound(update, catalog, active_version_id),
        SqlStatement::Delete(delete) => bind_delete_bound(delete, catalog, active_version_id),
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
    active_version_id: &str,
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
    let version_scope = bind_write_version_scope(
        &table.surface.kind,
        &input,
        &BoundPredicate::True,
        active_version_id,
    )?;
    Ok(BoundWrite {
        target: bound_write_target(&table.surface.kind),
        op: BoundWriteOp::Insert,
        input,
        predicate: BoundPredicate::True,
        assignments: Vec::new(),
        params: params.into_map(),
        version_scope,
    })
}

pub(super) fn bind_update_bound(
    update: &Update,
    catalog: &PublicCatalog,
    active_version_id: &str,
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
    let version_scope = bind_write_version_scope(
        &table.surface.kind,
        &BoundWriteInput::None,
        &predicate,
        active_version_id,
    )?;
    Ok(BoundWrite {
        target: bound_write_target(&table.surface.kind),
        op: BoundWriteOp::Update,
        input: BoundWriteInput::None,
        predicate,
        assignments,
        params: params.into_map(),
        version_scope,
    })
}

pub(super) fn bind_delete_bound(
    delete: &Delete,
    catalog: &PublicCatalog,
    active_version_id: &str,
) -> Result<BoundWrite, LixError> {
    let mut params = ParamBinder::default();
    reject_unsupported_delete_clauses(delete)?;
    let table = bind_delete_target(catalog, &delete.from)?;
    require_write_capability(&table.surface, BoundWriteOp::Delete)?;
    let predicate = bind_optional_predicate(&table, delete.selection.as_ref(), &mut params)?;
    let version_scope = bind_write_version_scope(
        &table.surface.kind,
        &BoundWriteInput::None,
        &predicate,
        active_version_id,
    )?;
    Ok(BoundWrite {
        target: bound_write_target(&table.surface.kind),
        op: BoundWriteOp::Delete,
        input: BoundWriteInput::None,
        predicate,
        assignments: Vec::new(),
        params: params.into_map(),
        version_scope,
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
    if insert.on.is_some() {
        return Err(super::error::unsupported(
            "INSERT ON clauses are not supported",
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
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
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
        || version.is_some()
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
            PublicSurfaceKind::EntityBase { .. } | PublicSurfaceKind::EntityByVersion { .. }
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
        let mut bound_values = BTreeMap::new();
        for (column, value) in columns.iter().zip(row) {
            bound_values.insert(column.clone(), bind_insert_value_expr(value, params)?);
        }
        rows.push(BoundInsertRow {
            values: bound_values,
        });
    }
    Ok(BoundWriteInput::Values(rows))
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
    match expr {
        Some(expr) => bind_predicate(table, expr, params),
        None => Ok(BoundPredicate::True),
    }
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
    if value.len() % 2 != 0 {
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
        "lix_empty_blob" => expect_exact_function_arity(name, actual, 0),
        "lix_timestamp" => expect_exact_function_arity(name, actual, 0),
        "lix_uuid_v7" => expect_exact_function_arity(name, actual, 0),
        "lix_active_version_commit_id" => expect_exact_function_arity(name, actual, 0),
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
        | "lix_active_version_commit_id"
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
        PublicSurfaceKind::LixStateByVersion => BoundWriteTarget::LixStateByVersion,
        PublicSurfaceKind::EntityBase { schema_key } => {
            BoundWriteTarget::Entity(EntityWriteSurface::Base {
                schema_key: schema_key.clone(),
            })
        }
        PublicSurfaceKind::EntityByVersion { schema_key } => {
            BoundWriteTarget::Entity(EntityWriteSurface::ByVersion {
                schema_key: schema_key.clone(),
            })
        }
        PublicSurfaceKind::File => BoundWriteTarget::File(FileWriteSurface::Base),
        PublicSurfaceKind::FileByVersion => BoundWriteTarget::File(FileWriteSurface::ByVersion),
        PublicSurfaceKind::Directory => BoundWriteTarget::Directory(DirectoryWriteSurface::Base),
        PublicSurfaceKind::DirectoryByVersion => {
            BoundWriteTarget::Directory(DirectoryWriteSurface::ByVersion)
        }
        PublicSurfaceKind::Version => BoundWriteTarget::Version,
        PublicSurfaceKind::EntityHistory { .. }
        | PublicSurfaceKind::FileHistory
        | PublicSurfaceKind::DirectoryHistory
        | PublicSurfaceKind::Change
        | PublicSurfaceKind::History => {
            unreachable!("write capability checked before target binding")
        }
    }
}

fn bind_write_version_scope(
    kind: &PublicSurfaceKind,
    input: &BoundWriteInput,
    predicate: &BoundPredicate,
    active_version_id: &str,
) -> Result<VersionScope, LixError> {
    let Some(version_column) = by_version_column_name(kind) else {
        return bind_base_write_version_scope(kind, input, predicate, active_version_id);
    };
    let version_selector = match input {
        BoundWriteInput::Values(rows) => {
            let mut selector = VersionSelector::Missing;
            for row in rows {
                if let Some((_, value)) = row
                    .values
                    .iter()
                    .find(|(column, _)| column.name == version_column)
                {
                    selector = selector.union(value_version_selector(value)?);
                }
            }
            selector
        }
        BoundWriteInput::None => predicate_version_selector(predicate, version_column)?,
        BoundWriteInput::Query { .. } => Err(super::error::unsupported(
            "INSERT ... SELECT by-version writes are not supported",
        ))?,
    };
    if matches!(kind, PublicSurfaceKind::LixStateByVersion) {
        let global_selector = match input {
            BoundWriteInput::Values(rows) => {
                let mut selector = GlobalSelector::Missing;
                for row in rows {
                    selector = selector.union(insert_row_global_selector(row)?);
                }
                selector
            }
            BoundWriteInput::None => predicate_global_selector(predicate)?,
            BoundWriteInput::Query { .. } => GlobalSelector::Missing,
        };
        return lix_state_by_version_scope(
            input,
            version_column,
            version_selector,
            global_selector,
        );
    }
    by_version_scope(input, version_column, version_selector)
}

fn by_version_scope(
    input: &BoundWriteInput,
    version_column: &str,
    selector: VersionSelector,
) -> Result<VersionScope, LixError> {
    match (input, selector) {
        (_, selector) if selector.is_empty() => Ok(VersionScope::Empty),
        (BoundWriteInput::Values(_), VersionSelector::Missing) => Err(super::error::unsupported(
            format!("INSERT into by-version SQL table requires explicit '{version_column}'"),
        )),
        (BoundWriteInput::Values(_), VersionSelector::Static(version_ids)) => {
            Ok(VersionScope::Explicit { version_ids })
        }
        (
            BoundWriteInput::Values(_),
            VersionSelector::Dynamic {
                version_ids,
                param_indexes,
            },
        ) => Ok(VersionScope::ExplicitDynamic {
            version_ids,
            param_indexes,
        }),
        (BoundWriteInput::None, VersionSelector::Missing) => Err(super::error::unsupported(
            format!("by-version SQL writes require an explicit '{version_column}' predicate"),
        )),
        (BoundWriteInput::None, VersionSelector::Static(version_ids)) => {
            Ok(VersionScope::ExplicitRequired { version_ids })
        }
        (
            BoundWriteInput::None,
            VersionSelector::Dynamic {
                version_ids,
                param_indexes,
            },
        ) => Ok(VersionScope::ExplicitRequiredDynamic {
            version_ids,
            param_indexes,
        }),
        (BoundWriteInput::Query { .. }, _) => Err(super::error::unsupported(
            "INSERT ... SELECT by-version writes are not supported",
        )),
    }
}

fn lix_state_by_version_scope(
    input: &BoundWriteInput,
    version_column: &str,
    version_selector: VersionSelector,
    global_selector: GlobalSelector,
) -> Result<VersionScope, LixError> {
    if matches!(global_selector, GlobalSelector::Empty) || version_selector.is_empty() {
        return Ok(VersionScope::Empty);
    }

    match global_selector {
        GlobalSelector::Static(true) => match version_selector {
            VersionSelector::Missing => Ok(VersionScope::Global),
            VersionSelector::Static(version_ids)
                if version_ids == BTreeSet::from([GLOBAL_VERSION_ID.to_string()]) =>
            {
                Ok(VersionScope::Global)
            }
            VersionSelector::Static(_) => Err(super::error::unsupported(
                "lix_state_by_version writes cannot combine global = true with non-global version_id",
            )),
            VersionSelector::Dynamic { .. } => Err(super::error::unsupported(
                "parameterized lix_state global scope selectors are not supported yet",
            )),
        },
        GlobalSelector::Static(false) => match &version_selector {
            VersionSelector::Static(version_ids) if version_ids.contains(GLOBAL_VERSION_ID) => {
                Err(super::error::unsupported(
                    "lix_state_by_version writes cannot combine global = false with global version_id",
                ))
            }
            _ => by_version_scope(input, version_column, version_selector),
        },
        GlobalSelector::Missing => match &version_selector {
            VersionSelector::Static(version_ids)
                if version_ids == &BTreeSet::from([GLOBAL_VERSION_ID.to_string()]) =>
            {
                Ok(VersionScope::Global)
            }
            VersionSelector::Static(version_ids) if version_ids.contains(GLOBAL_VERSION_ID) => {
                Err(super::error::unsupported(
                    "lix_state_by_version writes cannot mix global and non-global version scopes",
                ))
            }
            _ => by_version_scope(input, version_column, version_selector),
        },
        GlobalSelector::Mixed => Err(super::error::unsupported(
            "lix_state_by_version writes cannot mix global and version-specific rows",
        )),
        GlobalSelector::Empty => Ok(VersionScope::Empty),
    }
}

fn bind_base_write_version_scope(
    kind: &PublicSurfaceKind,
    input: &BoundWriteInput,
    predicate: &BoundPredicate,
    active_version_id: &str,
) -> Result<VersionScope, LixError> {
    if predicate == &BoundPredicate::False {
        return Ok(VersionScope::Empty);
    }
    if matches!(kind, PublicSurfaceKind::Version) {
        return Ok(VersionScope::Global);
    }
    if !matches!(kind, PublicSurfaceKind::LixState) {
        return Ok(active_version_scope(active_version_id));
    }
    match input {
        BoundWriteInput::Values(rows) => {
            let mut selector = GlobalSelector::Missing;
            for row in rows {
                selector = selector.union(insert_row_global_selector(row)?);
            }
            match selector {
                GlobalSelector::Missing | GlobalSelector::Static(false) => {
                    Ok(active_version_scope(active_version_id))
                }
                GlobalSelector::Static(true) => Ok(VersionScope::Global),
                GlobalSelector::Empty => Ok(VersionScope::Empty),
                GlobalSelector::Mixed => Err(super::error::unsupported(
                    "lix_state INSERT cannot mix global and active-version rows",
                )),
            }
        }
        BoundWriteInput::None => match predicate_global_selector(predicate)? {
            GlobalSelector::Static(true) => Ok(VersionScope::Global),
            GlobalSelector::Static(false) | GlobalSelector::Missing => {
                Ok(active_version_scope(active_version_id))
            }
            GlobalSelector::Empty => Ok(VersionScope::Empty),
            GlobalSelector::Mixed => Err(super::error::unsupported(
                "lix_state global predicates select mixed version scopes",
            )),
        },
        BoundWriteInput::Query { .. } => Ok(active_version_scope(active_version_id)),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GlobalSelector {
    Missing,
    Static(bool),
    Mixed,
    Empty,
}

impl GlobalSelector {
    fn union(self, other: Self) -> Self {
        match (self, other) {
            (Self::Mixed, _) | (_, Self::Mixed) => Self::Mixed,
            (Self::Empty, selector) | (selector, Self::Empty) => selector,
            (Self::Missing, selector) | (selector, Self::Missing) => selector,
            (Self::Static(left), Self::Static(right)) if left == right => Self::Static(left),
            (Self::Static(_), Self::Static(_)) => Self::Mixed,
        }
    }

    fn intersect(self, other: Self) -> Self {
        match (self, other) {
            (Self::Empty, _) | (_, Self::Empty) => Self::Empty,
            (Self::Mixed, Self::Missing) | (Self::Missing, Self::Mixed) => Self::Mixed,
            (Self::Mixed, selector) | (selector, Self::Mixed) => selector,
            (Self::Missing, selector) | (selector, Self::Missing) => selector,
            (Self::Static(left), Self::Static(right)) if left == right => Self::Static(left),
            (Self::Static(_), Self::Static(_)) => Self::Empty,
        }
    }
}

fn insert_row_global_selector(row: &BoundInsertRow) -> Result<GlobalSelector, LixError> {
    let Some((_, value)) = row
        .values
        .iter()
        .find(|(column, _)| column.name == "global")
    else {
        return Ok(GlobalSelector::Missing);
    };
    global_selector_value(value)
}

fn predicate_global_selector(predicate: &BoundPredicate) -> Result<GlobalSelector, LixError> {
    match predicate {
        BoundPredicate::True => Ok(GlobalSelector::Missing),
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
        BoundExpr::Param(_) => Err(super::error::unsupported(
            "parameterized lix_state global scope selectors are not supported yet",
        )),
        _ => Err(super::error::unsupported(
            "lix_state global predicates require boolean literals",
        )),
    }
}

fn by_version_column_name(kind: &PublicSurfaceKind) -> Option<&'static str> {
    match kind {
        PublicSurfaceKind::LixStateByVersion => Some("version_id"),
        PublicSurfaceKind::EntityByVersion { .. }
        | PublicSurfaceKind::FileByVersion
        | PublicSurfaceKind::DirectoryByVersion => Some("lixcol_version_id"),
        _ => None,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum VersionSelector {
    Missing,
    Static(BTreeSet<String>),
    Dynamic {
        version_ids: BTreeSet<String>,
        param_indexes: BTreeSet<usize>,
    },
}

impl VersionSelector {
    fn is_empty(&self) -> bool {
        matches!(self, Self::Static(version_ids) if version_ids.is_empty())
    }

    fn intersect(self, other: Self) -> Self {
        match (self, other) {
            (Self::Missing, selector) | (selector, Self::Missing) => selector,
            (Self::Static(version_ids), Self::Dynamic { param_indexes, .. })
            | (Self::Dynamic { param_indexes, .. }, Self::Static(version_ids))
                if version_ids.is_empty() || param_indexes.is_empty() =>
            {
                Self::Static(BTreeSet::new())
            }
            (Self::Static(left), Self::Static(right)) => {
                Self::Static(left.intersection(&right).cloned().collect())
            }
            (
                Self::Dynamic {
                    mut version_ids,
                    mut param_indexes,
                },
                Self::Dynamic {
                    version_ids: right_versions,
                    param_indexes: right_params,
                },
            ) => {
                version_ids.extend(right_versions);
                param_indexes.extend(right_params);
                Self::Dynamic {
                    version_ids,
                    param_indexes,
                }
            }
            (
                Self::Static(mut version_ids),
                Self::Dynamic {
                    version_ids: right_versions,
                    param_indexes,
                },
            )
            | (
                Self::Dynamic {
                    version_ids: right_versions,
                    param_indexes,
                },
                Self::Static(mut version_ids),
            ) => {
                version_ids.extend(right_versions);
                Self::Dynamic {
                    version_ids,
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
                    mut version_ids,
                    mut param_indexes,
                },
                Self::Dynamic {
                    version_ids: right_versions,
                    param_indexes: right_params,
                },
            ) => {
                version_ids.extend(right_versions);
                param_indexes.extend(right_params);
                Self::Dynamic {
                    version_ids,
                    param_indexes,
                }
            }
            (
                Self::Static(mut version_ids),
                Self::Dynamic {
                    version_ids: right_versions,
                    param_indexes,
                },
            )
            | (
                Self::Dynamic {
                    version_ids: right_versions,
                    param_indexes,
                },
                Self::Static(mut version_ids),
            ) => {
                version_ids.extend(right_versions);
                Self::Dynamic {
                    version_ids,
                    param_indexes,
                }
            }
        }
    }
}

fn predicate_version_selector(
    predicate: &BoundPredicate,
    version_column: &str,
) -> Result<VersionSelector, LixError> {
    match predicate {
        BoundPredicate::True => Ok(VersionSelector::Missing),
        BoundPredicate::False => Ok(VersionSelector::Static(BTreeSet::new())),
        BoundPredicate::And(predicates) => {
            let mut result = VersionSelector::Missing;
            for predicate in predicates {
                result = result.intersect(predicate_version_selector(predicate, version_column)?);
            }
            Ok(result)
        }
        BoundPredicate::Or(predicates) => {
            let mut result = VersionSelector::Static(BTreeSet::new());
            for predicate in predicates {
                let selector = predicate_version_selector(predicate, version_column)?;
                if selector == VersionSelector::Missing {
                    return Ok(VersionSelector::Missing);
                }
                result = result.union(selector);
            }
            Ok(result)
        }
        BoundPredicate::Eq(left, right) => {
            version_selector_from_binary_exprs(left, right, version_column)
                .or_else(|| version_selector_from_binary_exprs(right, left, version_column))
                .transpose()
                .map(|selector| selector.unwrap_or(VersionSelector::Missing))
        }
        BoundPredicate::In { expr, values } => {
            let BoundExpr::Column(column) = expr else {
                return Ok(VersionSelector::Missing);
            };
            if column.name != version_column {
                return Ok(VersionSelector::Missing);
            }
            let mut selector = VersionSelector::Missing;
            for value in values {
                selector = selector.union(value_version_selector(value)?);
            }
            Ok(selector)
        }
    }
}

fn version_selector_from_binary_exprs(
    column_expr: &BoundExpr,
    value_expr: &BoundExpr,
    version_column: &str,
) -> Option<Result<VersionSelector, LixError>> {
    let BoundExpr::Column(column) = column_expr else {
        return None;
    };
    if column.name != version_column {
        return None;
    }
    Some(value_version_selector(value_expr))
}

fn value_version_selector(expr: &BoundExpr) -> Result<VersionSelector, LixError> {
    match expr {
        BoundExpr::Literal(BoundLiteral::Text(version_id)) => Ok(VersionSelector::Static(
            BTreeSet::from([version_id.clone()]),
        )),
        BoundExpr::Param(param) => Ok(VersionSelector::Dynamic {
            version_ids: BTreeSet::new(),
            param_indexes: BTreeSet::from([param.index]),
        }),
        _ => Err(super::error::unsupported(
            "by-version SQL write predicates require string version ids",
        )),
    }
}

fn active_version_scope(active_version_id: &str) -> VersionScope {
    VersionScope::Active {
        version_id: active_version_id.to_string(),
    }
}

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

impl Default for ParamBinder {
    fn default() -> Self {
        Self {
            next_implicit_index: 0,
            mode: None,
            params: BTreeMap::new(),
        }
    }
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
        let error = bind_statement(&statement, &[], "version1")
            .expect_err("qualified write target should be rejected by the binder");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error.message.contains("qualified SQL table names"));
    }

    #[test]
    fn bind_statement_rejects_hidden_insert_columns() {
        let statement = parse_statement(
            "INSERT INTO lix_file (id, path, directory_id, name, data, lixcol_schema_key) VALUES ('file1', '/a', null, 'a', null, 'schema')",
        );
        let error = bind_statement(&statement, &[], "version1")
            .expect_err("hidden columns should not bind through statement binder");

        assert_eq!(error.code, LixError::CODE_COLUMN_NOT_FOUND);
        assert!(error.message.contains("not part of public SQL surface"));
    }

    #[test]
    fn bind_statement_rejects_implicit_insert_columns() {
        let statement = parse_statement("INSERT INTO lix_file VALUES ('file1')");
        let error = bind_statement(&statement, &[], "version1")
            .expect_err("implicit insert column list should fail closed");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error
            .message
            .contains("INSERT requires an explicit public column list"));
    }

    #[test]
    fn bind_statement_rejects_entity_insert_select() {
        let statement = parse_statement(
            "INSERT INTO test_state_schema (lixcol_entity_id, value) SELECT lix_json('[\"a\"]'), 'A'",
        );
        let error = bind_statement(
            &statement,
            &[serde_json::json!({
                "x-lix-key": "test_state_schema",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
            "version1",
        )
        .expect_err("entity INSERT SELECT should fail closed at binding");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error
            .message
            .contains("INSERT ... SELECT is not supported for entity SQL surfaces yet"));
    }

    #[test]
    fn bind_statement_rejects_duplicate_insert_columns() {
        let statement = parse_statement("INSERT INTO lix_file (id, id) VALUES ('file1', 'file2')");
        let error = bind_statement(&statement, &[], "version1")
            .expect_err("duplicate insert columns should be rejected");

        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(error.message.contains("duplicate write target column 'id'"));
    }

    #[test]
    fn bind_statement_rejects_duplicate_update_columns() {
        let statement = parse_statement("UPDATE lix_file SET name = 'a', name = 'b'");
        let error = bind_statement(&statement, &[], "version1")
            .expect_err("duplicate update columns should be rejected");

        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(error
            .message
            .contains("duplicate write target column 'name'"));
    }

    #[test]
    fn bind_statement_rejects_read_only_history_writes() {
        let statement = parse_statement("DELETE FROM lix_file_history");
        let error = bind_statement(&statement, &[], "version1")
            .expect_err("history surfaces should be read-only");

        assert_eq!(error.code, LixError::CODE_READ_ONLY);
    }

    #[test]
    fn bind_statement_preserves_update_assignment_and_predicate() {
        let statement = parse_statement(
            "UPDATE test_state_schema_by_version SET name = 'next' WHERE lixcol_version_id = 'version2'",
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
            "version1",
        )
        .expect("write body should bind");

        let write = bound;
        assert!(matches!(
            write.target,
            BoundWriteTarget::Entity(EntityWriteSurface::ByVersion { .. })
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
            ) if column.name == "lixcol_version_id" && value == "version2"
        ));
        assert!(matches!(
            write.version_scope,
            VersionScope::ExplicitRequired { ref version_ids }
                if version_ids == &BTreeSet::from(["version2".to_string()])
        ));
    }

    #[test]
    fn bind_statement_rejects_hidden_predicate_columns() {
        let statement = parse_statement("DELETE FROM lix_file WHERE lixcol_schema_key = 'schema'");
        let error = bind_statement(&statement, &[], "version1")
            .expect_err("hidden predicate columns should not bind");

        assert_eq!(error.code, LixError::CODE_COLUMN_NOT_FOUND);
        assert!(error.message.contains("not part of public SQL surface"));
    }

    #[test]
    fn bind_statement_binds_insert_values_and_params_once() {
        let statement = parse_statement("INSERT INTO lix_file (id, name) VALUES ($1, $2)");
        let bound = bind_statement(&statement, &[], "version1").expect("insert should bind");

        let write = bound;
        assert_eq!(write.op, BoundWriteOp::Insert);
        assert_eq!(
            write.params.params.keys().copied().collect::<Vec<_>>(),
            vec![1, 2]
        );
        let BoundWriteInput::Values(rows) = write.input else {
            panic!("expected values input");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values.len(), 2);
        assert!(rows[0]
            .values
            .values()
            .any(|value| matches!(value, BoundExpr::Param(param) if param.index == 1)));
        assert!(rows[0]
            .values
            .values()
            .any(|value| matches!(value, BoundExpr::Param(param) if param.index == 2)));
    }

    #[test]
    fn bind_statement_rejects_insert_values_column_refs() {
        let statement = parse_statement("INSERT INTO lix_file (id) VALUES (name)");
        let error = bind_statement(&statement, &[], "version1")
            .expect_err("VALUES rows should not bind target table column refs");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error
            .message
            .contains("unsupported INSERT VALUES expression"));
    }

    #[test]
    fn bind_statement_binds_hex_literals_as_blobs() {
        let statement =
            parse_statement("INSERT INTO lix_file (id, data) VALUES ('file1', X'4142')");
        let bound = bind_statement(&statement, &[], "version1").expect("insert should bind");

        let write = bound;
        let BoundWriteInput::Values(rows) = write.input else {
            panic!("expected values input");
        };
        assert!(rows[0]
            .values
            .values()
            .any(|value| matches!(value, BoundExpr::Literal(BoundLiteral::Blob(bytes)) if bytes == &vec![0x41, 0x42])));
    }

    #[test]
    fn bind_statement_binds_lix_json_values_functions() {
        let statement = parse_statement(
            "INSERT INTO lix_state (entity_id, schema_key, snapshot_content) VALUES (lix_json('[\"e1\"]'), 'app.test', lix_json('{\"id\":\"e1\"}'))",
        );
        let bound = bind_statement(&statement, &[], "version1").expect("insert should bind");

        let write = bound;
        let BoundWriteInput::Values(rows) = write.input else {
            panic!("expected values input");
        };
        assert_eq!(rows[0].values.len(), 3);
        assert!(rows[0].values.values().any(|value| {
            matches!(
                value,
                BoundExpr::Function { name, args }
                    if name == "lix_json" && args.len() == 1
            )
        }));
    }

    #[test]
    fn bind_statement_binds_public_values_functions() {
        let statement = parse_statement(
            "INSERT INTO lix_file (id, path, data) VALUES (lix_uuid_v7(), lix_timestamp(), lix_text_encode('hello'))",
        );
        let bound = bind_statement(&statement, &[], "version1").expect("insert should bind");

        let write = bound;
        let BoundWriteInput::Values(rows) = write.input else {
            panic!("expected values input");
        };
        let function_names = rows[0]
            .values
            .values()
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
            let error = bind_statement(&parse_statement(sql), &[], "version1")
                .expect_err("unsupported function details should fail closed");
            assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL, "{sql}");
        }
    }

    #[test]
    fn bind_statement_binds_by_version_insert_scope_from_version_column() {
        let statement = parse_statement(
            "INSERT INTO lix_file_by_version (id, name, lixcol_version_id) VALUES ('file1', 'a', 'version2')",
        );
        let bound = bind_statement(&statement, &[], "version1").expect("insert should bind");

        let write = bound;
        assert!(matches!(
            write.version_scope,
            VersionScope::Explicit { ref version_ids }
                if version_ids == &BTreeSet::from(["version2".to_string()])
        ));
    }

    #[test]
    fn bind_statement_preserves_parameterized_by_version_scope_selectors() {
        let update = bind_statement(
            &parse_statement(
                "UPDATE lix_file_by_version SET hidden = true WHERE id = 'file1' AND lixcol_version_id = $1",
            ),
            &[],
            "version1",
        )
        .expect("parameterized update version scope should bind");
        assert_eq!(
            update.version_scope,
            VersionScope::ExplicitRequiredDynamic {
                version_ids: BTreeSet::new(),
                param_indexes: BTreeSet::from([1])
            }
        );

        let insert = bind_statement(
            &parse_statement(
                "INSERT INTO lix_file_by_version (id, name, lixcol_version_id) VALUES ('file1', 'a', $1)",
            ),
            &[],
            "version1",
        )
        .expect("parameterized insert version scope should bind");
        assert_eq!(
            insert.version_scope,
            VersionScope::ExplicitDynamic {
                version_ids: BTreeSet::new(),
                param_indexes: BTreeSet::from([1])
            }
        );
    }

    #[test]
    fn bind_statement_binds_contradictory_by_version_selectors_as_empty() {
        let statement = parse_statement(
            "DELETE FROM lix_file_by_version WHERE lixcol_version_id IN ('v1') AND lixcol_version_id IN ('v2')",
        );
        let bound = bind_statement(&statement, &[], "version1").expect("delete should bind");

        let write = bound;
        assert_eq!(write.version_scope, VersionScope::Empty);
    }

    #[test]
    fn bind_statement_binds_false_by_version_predicate_as_empty() {
        let statement = parse_statement("DELETE FROM lix_file_by_version WHERE false");
        let bound = bind_statement(&statement, &[], "version1").expect("no-match delete binds");

        let write = bound;
        assert_eq!(write.version_scope, VersionScope::Empty);
    }

    #[test]
    fn bind_statement_binds_false_base_predicates_as_empty() {
        for sql in [
            "DELETE FROM lix_file WHERE false",
            "UPDATE lix_state SET metadata = '{}' WHERE false",
            "DELETE FROM lix_version WHERE false",
        ] {
            let bound = bind_statement(&parse_statement(sql), &[], "version1")
                .expect("no-match write should bind");
            let write = bound;
            assert_eq!(write.version_scope, VersionScope::Empty, "{sql}");
        }
    }

    #[test]
    fn bind_statement_binds_global_lix_state_insert_scope() {
        let statement = parse_statement(
            "INSERT INTO lix_state (entity_id, schema_key, snapshot_content, global) VALUES ('[\"e1\"]', 'app.test', '{}', true)",
        );
        let bound = bind_statement(&statement, &[], "version1").expect("insert should bind");

        let write = bound;
        assert_eq!(write.version_scope, VersionScope::Global);
    }

    #[test]
    fn bind_statement_rejects_parameterized_lix_state_global_scope() {
        let statement = parse_statement(
            "INSERT INTO lix_state (entity_id, schema_key, snapshot_content, global) VALUES ('[\"e1\"]', 'app.test', '{}', $1)",
        );
        let error = bind_statement(&statement, &[], "version1")
            .expect_err("parameterized global scope should fail closed until scope resolution");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error
            .message
            .contains("parameterized lix_state global scope selectors"));
    }

    #[test]
    fn bind_statement_rejects_lix_state_by_version_global_true_with_version_id() {
        let statement = parse_statement(
            "INSERT INTO lix_state_by_version (entity_id, schema_key, snapshot_content, version_id, global) VALUES ('[\"e1\"]', 'app.test', '{}', 'v1', true)",
        );
        let error = bind_statement(&statement, &[], "version1")
            .expect_err("global true and version_id select contradictory scopes");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error
            .message
            .contains("cannot combine global = true with non-global version_id"));
    }

    #[test]
    fn bind_statement_binds_lix_state_by_version_global_true_with_global_version() {
        let statement = parse_statement(
            "INSERT INTO lix_state_by_version (entity_id, schema_key, snapshot_content, version_id, global) VALUES ('[\"e1\"]', 'app.test', '{}', 'global', true)",
        );
        let bound = bind_statement(&statement, &[], "version1").expect("global row should bind");

        let write = bound;
        assert_eq!(write.version_scope, VersionScope::Global);
    }

    #[test]
    fn bind_statement_rejects_lix_state_by_version_global_false_with_global_version() {
        let statement = parse_statement(
            "INSERT INTO lix_state_by_version (entity_id, schema_key, snapshot_content, version_id, global) VALUES ('[\"e1\"]', 'app.test', '{}', 'global', false)",
        );
        let error = bind_statement(&statement, &[], "version1")
            .expect_err("global false cannot target the global version");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error
            .message
            .contains("cannot combine global = false with global version_id"));
    }

    #[test]
    fn bind_statement_binds_contradictory_lix_state_global_predicates_as_empty() {
        let statement = parse_statement(
            "UPDATE lix_state SET metadata = '{}' WHERE global = true AND global = false",
        );
        let bound =
            bind_statement(&statement, &[], "version1").expect("no-match scope should bind");

        let write = bound;
        assert_eq!(write.version_scope, VersionScope::Empty);
    }

    #[test]
    fn bind_statement_rejects_mixed_or_lix_state_global_scope() {
        let statement = parse_statement(
            "UPDATE lix_state SET metadata = '{}' WHERE global = true OR schema_key = 'app.test'",
        );
        let error = bind_statement(&statement, &[], "version1")
            .expect_err("mixed global OR scope should fail closed");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(
            error
                .message
                .contains("lix_state global predicates select mixed version scopes"),
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
            "version1",
        )
        .expect_err("entity primary key columns should be insert-only");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error.message.contains("is not writable"));
    }

    #[test]
    fn bind_statement_binds_version_writes_as_global() {
        let statement =
            parse_statement("INSERT INTO lix_version (id, name) VALUES ('draft', 'Draft')");
        let bound = bind_statement(&statement, &[], "version1").expect("insert should bind");

        let write = bound;
        assert_eq!(write.version_scope, VersionScope::Global);
    }

    #[test]
    fn bind_statement_binds_negative_numeric_literals() {
        let statement = parse_statement(
            "UPDATE lix_state SET snapshot_content = -1 WHERE entity_id = '[\"e1\"]'",
        );
        let bound = bind_statement(&statement, &[], "version1").expect("update should bind");

        let write = bound;
        assert!(matches!(
            write.assignments[0].value,
            BoundExpr::Literal(BoundLiteral::Integer(-1))
        ));
    }

    #[test]
    fn bind_statement_rejects_by_version_writes_without_version_selector() {
        let statement =
            parse_statement("UPDATE lix_file_by_version SET hidden = true WHERE id = 'file1'");
        let error = bind_statement(&statement, &[], "version1")
            .expect_err("by-version writes should require explicit version predicate");

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
        assert!(error
            .message
            .contains("cannot mix SQL parameter placeholder styles"));
    }

    #[test]
    fn bind_statement_rejects_read_only_by_version_columns_as_write_targets() {
        let statement =
            parse_statement("UPDATE lix_file_by_version SET lixcol_version_id = 'version2'");
        let error = bind_statement(&statement, &[], "version1")
            .expect_err("by-version version columns are filter-only");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error.message.contains("is not writable"));
    }

    #[test]
    fn bind_statement_rejects_provider_read_only_update_columns() {
        let statement = parse_statement("UPDATE lix_state SET entity_id = '[\"next\"]'");
        let error = bind_statement(&statement, &[], "version1")
            .expect_err("lix_state identity columns are insert-only");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error.message.contains("is not writable"));
    }

    #[test]
    fn bind_statement_rejects_explain_wrappers() {
        let statement =
            parse_statement("EXPLAIN UPDATE lix_file SET name = 'x' WHERE id = 'file1'");
        let error = bind_statement(&statement, &[], "version1")
            .expect_err("EXPLAIN should not bind as a write");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error
            .message
            .contains("EXPLAIN statements are not supported"));
    }

    #[test]
    fn bind_statement_rejects_unsupported_write_clauses() {
        let statement =
            parse_statement("UPDATE lix_file AS f SET name = 'next' WHERE f.id = 'file1'");
        let error = bind_statement(&statement, &[], "version1")
            .expect_err("target aliases should not be ignored");

        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(error
            .message
            .contains("DML target aliases are not supported"));
    }

    fn parse_statement(sql: &str) -> DataFusionStatement {
        crate::sql2::parse_statement(sql).expect("parse SQL")
    }
}
