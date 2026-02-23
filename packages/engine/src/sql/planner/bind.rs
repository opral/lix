use sqlparser::ast::{Expr, Insert, Query, SetExpr, Statement};

use crate::sql::extract_explicit_transaction_script_from_statements;
use crate::sql::object_name_matches;
use crate::LixError;

use super::types::StatementBlock;

pub(crate) fn prepare_statement_block_with_transaction_flag(
    statements: Vec<Statement>,
) -> Result<StatementBlock, LixError> {
    let statements = if let Some(inner) =
        extract_explicit_transaction_script_from_statements(&statements)?
    {
        coalesce_vtable_inserts_in_explicit_script(inner)?
    } else {
        statements
    };

    Ok(StatementBlock { statements })
}

fn coalesce_vtable_inserts_in_explicit_script(
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
    if left.columns != right.columns {
        return false;
    }

    // Conservative merge policy: only plain VALUES inserts with no dialect-specific modifiers.
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
        sqlparser::ast::TableObject::TableName(name) => {
            object_name_matches(name, "lix_internal_state_vtable")
        }
        _ => false,
    }
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

#[cfg(test)]
mod tests {
    use super::prepare_statement_block_with_transaction_flag;
    use crate::sql::parse_sql_statements;

    #[test]
    fn unwraps_begin_commit_scripts_before_execution_planning() {
        let statements =
            parse_sql_statements("BEGIN; SELECT ?; SELECT ?; COMMIT;").expect("parse SQL");
        let block = prepare_statement_block_with_transaction_flag(statements)
            .expect("prepare statement block");

        assert_eq!(block.statements.len(), 2);
    }

    #[test]
    fn keeps_non_script_statement_blocks_unchanged() {
        let statements = parse_sql_statements("SELECT ?; SELECT ?").expect("parse SQL");
        let block = prepare_statement_block_with_transaction_flag(statements)
            .expect("prepare statement block");

        assert_eq!(block.statements.len(), 2);
    }

    #[test]
    fn rejects_nested_transaction_statements_inside_scripts() {
        let statements =
            parse_sql_statements("BEGIN; SELECT 1; ROLLBACK; COMMIT;").expect("parse SQL");

        let error = prepare_statement_block_with_transaction_flag(statements)
            .expect_err("nested transaction script should fail");
        assert!(
            error
                .message
                .contains("nested transaction statements are not supported"),
            "unexpected message: {}",
            error.message
        );
    }
}
