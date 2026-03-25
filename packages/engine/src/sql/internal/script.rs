use sqlparser::ast::{Expr, Insert, SetExpr, Statement, TableObject, Value as SqlAstValue};

use crate::sql::ast::walk::object_name_matches;
use crate::{LixError, Value};

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
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description:
                "nested transaction statements are not supported inside BEGIN ... COMMIT scripts"
                    .to_string(),
        });
    }

    Ok(Some(middle.to_vec()))
}

pub(crate) fn coalesce_vtable_inserts_in_transactions(
    statements: Vec<Statement>,
) -> Result<Vec<Statement>, LixError> {
    let mut result = Vec::with_capacity(statements.len());
    let mut in_transaction = false;
    let mut pending_insert: Option<Insert> = None;

    for statement in statements {
        match statement {
            Statement::StartTransaction { .. } => {
                flush_pending_insert(&mut result, &mut pending_insert);
                in_transaction = true;
                result.push(statement);
            }
            Statement::Commit { .. } | Statement::Rollback { .. } => {
                flush_pending_insert(&mut result, &mut pending_insert);
                in_transaction = false;
                result.push(statement);
            }
            Statement::Insert(insert) if in_transaction => {
                if let Some(existing) = pending_insert.as_mut() {
                    if can_merge_coalescable_insert(existing, &insert) {
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

fn can_merge_coalescable_insert(left: &Insert, right: &Insert) -> bool {
    if !insert_targets_coalescable_table(left) || !insert_targets_coalescable_table(right) {
        return false;
    }
    if insert_targets_registered_schema(left) || insert_targets_registered_schema(right) {
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
    if insert_has_transaction_sensitive_public_filesystem_columns(left)
        || insert_has_transaction_sensitive_public_filesystem_columns(right)
    {
        return false;
    }

    plain_values_rows(left).is_some() && plain_values_rows(right).is_some()
}

fn append_insert_rows(target: &mut Insert, incoming: &Insert) -> Result<(), LixError> {
    let incoming_rows = plain_values_rows(incoming)
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction insert coalescing expected VALUES rows".to_string(),
        })?
        .to_vec();

    let target_rows = plain_values_rows_mut(target).ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "transaction insert coalescing expected mutable VALUES rows".to_string(),
    })?;
    target_rows.extend(incoming_rows);
    Ok(())
}

fn insert_targets_coalescable_table(insert: &Insert) -> bool {
    match &insert.table {
        TableObject::TableName(name) => {
            object_name_matches(name, "lix_internal_state_vtable")
                || object_name_matches(name, "lix_file")
        }
        _ => false,
    }
}

fn insert_has_transaction_sensitive_public_filesystem_columns(insert: &Insert) -> bool {
    if !matches!(&insert.table, TableObject::TableName(name) if object_name_matches(name, "lix_file"))
    {
        return false;
    }

    insert.columns.iter().any(|column| {
        matches!(
            column.value.to_ascii_lowercase().as_str(),
            "untracked" | "version_id" | "global"
        )
    })
}

fn insert_targets_registered_schema(insert: &Insert) -> bool {
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
            .is_some_and(expr_is_registered_schema_literal)
    })
}

fn plain_values_rows(insert: &Insert) -> Option<&[Vec<Expr>]> {
    let source = insert.source.as_ref()?;
    let SetExpr::Values(values) = source.body.as_ref() else {
        return None;
    };
    if values.explicit_row || values.value_keyword {
        return None;
    }
    Some(values.rows.as_slice())
}

fn plain_values_rows_mut(insert: &mut Insert) -> Option<&mut Vec<Vec<Expr>>> {
    let source = insert.source.as_mut()?;
    let SetExpr::Values(values) = source.body.as_mut() else {
        return None;
    };
    if values.explicit_row || values.value_keyword {
        return None;
    }
    Some(&mut values.rows)
}

fn expr_is_registered_schema_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Value(value) => match &value.value {
            SqlAstValue::SingleQuotedString(text) | SqlAstValue::DoubleQuotedString(text) => {
                text.eq_ignore_ascii_case("lix_registered_schema")
            }
            _ => false,
        },
        _ => false,
    }
}
