use sqlparser::ast::{
    AssignmentTarget, BinaryOperator, Expr, FromTable, Insert, ObjectName, ObjectNamePart, Query,
    SetExpr, Statement, TableObject, Value as SqlAstValue,
};
use std::collections::BTreeSet;

use crate::{LixError, SqlDialect, Value};

use super::super::ast::walk::object_name_matches;
use super::super::semantics::state_resolution::canonical::{
    table_object_targets_table_name, table_with_joins_targets_table_name,
};

pub(crate) fn extract_explicit_transaction_script_from_statements(
    statements: &[Statement],
    params: &[Value],
) -> Result<Option<Vec<Statement>>, LixError> {
    let _ = params;
    if statements.len() < 2 {
        return Ok(None);
    }

    let first_is_begin = matches!(statements.first(), Some(Statement::StartTransaction { .. }));
    let last_is_commit = matches!(statements.last(), Some(Statement::Commit { .. }));
    if !first_is_begin || !last_is_commit {
        return Ok(None);
    }

    let middle = &statements[1..statements.len() - 1];
    if middle.iter().any(|statement| {
        matches!(
            statement,
            Statement::StartTransaction { .. }
                | Statement::Commit { .. }
                | Statement::Rollback { .. }
        )
    }) {
        return Err(LixError {
            message:
                "nested transaction statements are not supported inside BEGIN ... COMMIT scripts"
                    .to_string(),
        });
    }

    Ok(Some(middle.to_vec()))
}

#[derive(Debug, Clone)]
struct LixFileWriteRow {
    id: String,
    path_sql: String,
    data_sql: String,
}

pub(crate) fn coalesce_lix_file_transaction_statements(
    statements: &[Statement],
    dialect: Option<SqlDialect>,
) -> Option<Vec<String>> {
    if statements.is_empty() {
        return Some(Vec::new());
    }
    if !matches!(dialect, Some(SqlDialect::Sqlite)) {
        return None;
    }

    let mut delete_ids = Vec::new();
    let mut insert_rows = Vec::new();
    let mut update_rows = Vec::new();
    let mut seen_ids = BTreeSet::new();
    let mut saw_update = false;

    for statement in statements {
        if let Some(ids) = parse_lix_file_delete_ids(statement) {
            if saw_update {
                return None;
            }
            for id in ids {
                if !seen_ids.insert(id.clone()) {
                    return None;
                }
                delete_ids.push(id);
            }
            continue;
        }
        if let Some(rows) = parse_lix_file_insert_rows(statement) {
            if saw_update {
                return None;
            }
            for row in rows {
                if !seen_ids.insert(row.id.clone()) {
                    return None;
                }
                insert_rows.push(row);
            }
            continue;
        }
        if let Some(row) = parse_lix_file_update_row(statement) {
            if !seen_ids.insert(row.id.clone()) {
                return None;
            }
            saw_update = true;
            update_rows.push(row);
            continue;
        }
        return None;
    }

    let mut rewritten = Vec::new();

    if !delete_ids.is_empty() {
        let id_list = delete_ids
            .iter()
            .map(|id| format!("'{}'", escape_sql_string(id)))
            .collect::<Vec<_>>()
            .join(", ");
        rewritten.push(format!("DELETE FROM lix_file WHERE id IN ({id_list})"));
    }

    if !insert_rows.is_empty() {
        let values = insert_rows
            .iter()
            .map(|row| {
                format!(
                    "('{}', {}, {})",
                    escape_sql_string(&row.id),
                    row.path_sql,
                    row.data_sql
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        rewritten.push(format!(
            "INSERT INTO lix_file (id, path, data) VALUES {values}"
        ));
    }

    if !update_rows.is_empty() {
        let path_cases = update_rows
            .iter()
            .map(|row| {
                format!(
                    "WHEN '{}' THEN {}",
                    escape_sql_string(&row.id),
                    row.path_sql
                )
            })
            .collect::<Vec<_>>()
            .join(" ");
        let data_cases = update_rows
            .iter()
            .map(|row| {
                format!(
                    "WHEN '{}' THEN {}",
                    escape_sql_string(&row.id),
                    row.data_sql
                )
            })
            .collect::<Vec<_>>()
            .join(" ");
        let id_list = update_rows
            .iter()
            .map(|row| format!("'{}'", escape_sql_string(&row.id)))
            .collect::<Vec<_>>()
            .join(", ");
        rewritten.push(format!(
            "UPDATE lix_file \
             SET path = CASE id {path_cases} ELSE path END, \
                 data = CASE id {data_cases} ELSE data END \
             WHERE id IN ({id_list})"
        ));
    }

    Some(rewritten)
}

pub(crate) fn coalesce_vtable_inserts_in_statement_list(
    statements: Vec<Statement>,
) -> Result<Vec<Statement>, LixError> {
    let mut result = Vec::with_capacity(statements.len());
    let mut pending_insert: Option<Insert> = None;

    for statement in statements {
        match statement {
            Statement::Insert(insert) => {
                if let Some(existing) = pending_insert.as_mut() {
                    if can_merge_vtable_insert(existing, &insert) {
                        append_insert_rows(existing, &insert)?;
                    } else {
                        flush_pending_insert(&mut result, &mut pending_insert);
                        pending_insert = Some(insert);
                    }
                } else {
                    pending_insert = Some(insert);
                }
            }
            other => {
                flush_pending_insert(&mut result, &mut pending_insert);
                result.push(other);
            }
        }
    }

    flush_pending_insert(&mut result, &mut pending_insert);
    Ok(result)
}

fn flush_pending_insert(result: &mut Vec<Statement>, pending_insert: &mut Option<Insert>) {
    if let Some(insert) = pending_insert.take() {
        result.push(Statement::Insert(insert));
    }
}

fn can_merge_vtable_insert(left: &Insert, right: &Insert) -> bool {
    if !insert_targets_vtable(left) || !insert_targets_vtable(right) {
        return false;
    }
    if insert_targets_stored_schema(left) || insert_targets_stored_schema(right) {
        return false;
    }
    if left.columns != right.columns {
        return false;
    }

    if left.or.is_some()
        || right.or.is_some()
        || left.ignore
        || right.ignore
        || left.overwrite
        || right.overwrite
        || !left.assignments.is_empty()
        || !right.assignments.is_empty()
        || left.partitioned.is_some()
        || right.partitioned.is_some()
        || !left.after_columns.is_empty()
        || !right.after_columns.is_empty()
        || left.on.is_some()
        || right.on.is_some()
        || left.returning.is_some()
        || right.returning.is_some()
        || left.replace_into
        || right.replace_into
        || left.priority.is_some()
        || right.priority.is_some()
        || left.insert_alias.is_some()
        || right.insert_alias.is_some()
        || left.settings.is_some()
        || right.settings.is_some()
        || left.format_clause.is_some()
        || right.format_clause.is_some()
    {
        return false;
    }

    if left.table.to_string() != right.table.to_string() {
        return false;
    }
    if left.table_alias != right.table_alias {
        return false;
    }
    if left.into != right.into || left.has_table_keyword != right.has_table_keyword {
        return false;
    }

    plain_values_rows(left).is_some() && plain_values_rows(right).is_some()
}

fn append_insert_rows(target: &mut Insert, incoming: &Insert) -> Result<(), LixError> {
    let incoming_rows = plain_values_rows(incoming)
        .ok_or_else(|| LixError {
            message: "transaction insert coalescing expected VALUES rows".to_string(),
        })?
        .to_vec();

    let target_rows = plain_values_rows_mut(target).ok_or_else(|| LixError {
        message: "transaction insert coalescing expected mutable VALUES rows".to_string(),
    })?;
    target_rows.extend(incoming_rows);
    Ok(())
}

fn insert_targets_vtable(insert: &Insert) -> bool {
    match &insert.table {
        TableObject::TableName(name) => object_name_matches(name, "lix_internal_state_vtable"),
        _ => false,
    }
}

fn insert_targets_stored_schema(insert: &Insert) -> bool {
    let schema_key_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("schema_key"));
    let Some(schema_key_index) = schema_key_index else {
        return false;
    };

    let Some(rows) = plain_values_rows(insert) else {
        return false;
    };

    rows.iter().any(|row| {
        row.get(schema_key_index)
            .is_some_and(expr_is_stored_schema_literal)
    })
}

fn expr_is_stored_schema_literal(expr: &Expr) -> bool {
    let Expr::Value(value) = expr else {
        return false;
    };
    let literal = match &value.value {
        SqlAstValue::SingleQuotedString(text) | SqlAstValue::DoubleQuotedString(text) => text,
        _ => return false,
    };
    literal.eq_ignore_ascii_case("lix_stored_schema")
}

fn plain_values_rows(insert: &Insert) -> Option<&Vec<Vec<Expr>>> {
    let source = insert.source.as_ref()?;
    if !query_is_plain_values(source) {
        return None;
    }
    let SetExpr::Values(values) = source.body.as_ref() else {
        return None;
    };
    Some(&values.rows)
}

fn plain_values_rows_mut(insert: &mut Insert) -> Option<&mut Vec<Vec<Expr>>> {
    let source = insert.source.as_mut()?;
    if !query_is_plain_values(source) {
        return None;
    }
    let SetExpr::Values(values) = source.body.as_mut() else {
        return None;
    };
    Some(&mut values.rows)
}

fn query_is_plain_values(query: &Query) -> bool {
    query.with.is_none()
        && query.order_by.is_none()
        && query.limit_clause.is_none()
        && query.fetch.is_none()
        && query.locks.is_empty()
        && query.for_clause.is_none()
        && query.settings.is_none()
        && query.format_clause.is_none()
        && query.pipe_operators.is_empty()
}

fn parse_lix_file_insert_rows(statement: &Statement) -> Option<Vec<LixFileWriteRow>> {
    let Statement::Insert(insert) = statement else {
        return None;
    };
    if !table_object_targets_table_name(&insert.table, "lix_file") {
        return None;
    }
    if insert.columns.is_empty() {
        return None;
    }
    if !insert.assignments.is_empty() || insert.on.is_some() || insert.returning.is_some() {
        return None;
    }
    let source = insert.source.as_deref()?;
    let SetExpr::Values(values) = source.body.as_ref() else {
        return None;
    };

    if insert.columns.len() != 3 {
        return None;
    }

    let mut id_index = None;
    let mut path_index = None;
    let mut data_index = None;
    for (index, column) in insert.columns.iter().enumerate() {
        if column.value.eq_ignore_ascii_case("id") {
            if id_index.replace(index).is_some() {
                return None;
            }
        } else if column.value.eq_ignore_ascii_case("path") {
            if path_index.replace(index).is_some() {
                return None;
            }
        } else if column.value.eq_ignore_ascii_case("data") {
            if data_index.replace(index).is_some() {
                return None;
            }
        } else {
            return None;
        }
    }
    let id_index = id_index?;
    let path_index = path_index?;
    let data_index = data_index?;

    let mut rows = Vec::with_capacity(values.rows.len());
    for row in &values.rows {
        let id = expr_as_string_literal(row.get(id_index)?)?;
        let path_sql = row.get(path_index)?.to_string();
        let data_sql = row.get(data_index)?.to_string();
        rows.push(LixFileWriteRow {
            id,
            path_sql,
            data_sql,
        });
    }
    Some(rows)
}

fn parse_lix_file_update_row(statement: &Statement) -> Option<LixFileWriteRow> {
    let Statement::Update(update) = statement else {
        return None;
    };
    if !table_with_joins_targets_table_name(&update.table, "lix_file") {
        return None;
    }
    if update.from.is_some() || update.returning.is_some() || update.limit.is_some() {
        return None;
    }

    let mut path_sql = None;
    let mut data_sql = None;
    for assignment in &update.assignments {
        let AssignmentTarget::ColumnName(target) = &assignment.target else {
            return None;
        };
        let column = object_name_last_ident_value(target)?;
        if column.eq_ignore_ascii_case("path") {
            path_sql = Some(assignment.value.to_string());
        } else if column.eq_ignore_ascii_case("data") {
            data_sql = Some(assignment.value.to_string());
        } else {
            return None;
        }
    }

    let id = parse_id_eq_selection(update.selection.as_ref()?)?;
    Some(LixFileWriteRow {
        id,
        path_sql: path_sql?,
        data_sql: data_sql?,
    })
}

fn parse_lix_file_delete_ids(statement: &Statement) -> Option<Vec<String>> {
    let Statement::Delete(delete) = statement else {
        return None;
    };
    if !delete.tables.is_empty()
        || delete.using.is_some()
        || delete.returning.is_some()
        || !delete.order_by.is_empty()
        || delete.limit.is_some()
    {
        return None;
    }

    let from = match &delete.from {
        FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from) => from,
    };
    if from.len() != 1 || !table_with_joins_targets_table_name(&from[0], "lix_file") {
        return None;
    }

    parse_id_selection(delete.selection.as_ref()?)
}

fn parse_id_selection(selection: &Expr) -> Option<Vec<String>> {
    match selection {
        Expr::InList {
            expr,
            list,
            negated: false,
        } if expr_is_column_name(expr, "id") => list.iter().map(expr_as_string_literal).collect(),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if expr_is_column_name(left, "id") {
                return Some(vec![expr_as_string_literal(right)?]);
            }
            if expr_is_column_name(right, "id") {
                return Some(vec![expr_as_string_literal(left)?]);
            }
            None
        }
        _ => None,
    }
}

fn parse_id_eq_selection(selection: &Expr) -> Option<String> {
    let ids = parse_id_selection(selection)?;
    if ids.len() == 1 {
        return ids.into_iter().next();
    }
    None
}

fn expr_as_string_literal(expr: &Expr) -> Option<String> {
    let Expr::Value(value) = expr else {
        return None;
    };
    match &value.value {
        SqlAstValue::SingleQuotedString(text)
        | SqlAstValue::DoubleQuotedString(text)
        | SqlAstValue::NationalStringLiteral(text)
        | SqlAstValue::EscapedStringLiteral(text)
        | SqlAstValue::UnicodeStringLiteral(text) => Some(text.clone()),
        _ => None,
    }
}

fn expr_is_column_name(expr: &Expr, name: &str) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case(name),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .is_some_and(|ident| ident.value.eq_ignore_ascii_case(name)),
        Expr::Nested(inner) => expr_is_column_name(inner, name),
        _ => false,
    }
}

fn object_name_last_ident_value(name: &ObjectName) -> Option<&str> {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.as_str())
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}
