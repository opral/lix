use crate::contracts::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::sql::binder::insert_values_rows_mut;
use crate::LixError;
use sqlparser::ast::{
    Expr, ObjectName, ObjectNamePart, Statement, TableObject, Value as AstValue, ValueWithSpan,
};

pub(crate) fn ensure_generated_filesystem_insert_ids<P: LixFunctionProvider>(
    statements: &mut [Statement],
    functions: &SharedFunctionProvider<P>,
) -> Result<(), LixError> {
    for statement in statements.iter_mut() {
        if !statement_requires_generated_filesystem_insert_id(statement) {
            continue;
        }
        let Statement::Insert(insert) = statement else {
            continue;
        };

        let current_column_count = insert.columns.len();
        insert.columns.push("id".into());
        let Some(rows) = insert_values_rows_mut(insert) else {
            continue;
        };
        for row in rows.iter_mut() {
            if row.len() != current_column_count {
                return Err(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: "filesystem insert row length does not match column count"
                        .to_string(),
                });
            }
            row.push(string_literal_expr(functions.call_uuid_v7()));
        }
    }

    Ok(())
}

fn statement_requires_generated_filesystem_insert_id(statement: &Statement) -> bool {
    let Statement::Insert(insert) = statement else {
        return false;
    };
    if file_write_target_from_insert(&insert.table).is_none() {
        return false;
    }
    let data_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("data"));
    let path_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("path"));
    let id_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("id"));
    data_index.is_some() && path_index.is_some() && id_index.is_none()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileWriteTarget {
    ActiveVersion,
    ExplicitVersion,
}

fn file_write_target_from_insert(table: &TableObject) -> Option<FileWriteTarget> {
    let TableObject::TableName(name) = table else {
        return None;
    };
    let table_name = object_name_terminal(name)?;
    if table_name.eq_ignore_ascii_case("lix_file") {
        Some(FileWriteTarget::ActiveVersion)
    } else if table_name.eq_ignore_ascii_case("lix_file_by_version") {
        Some(FileWriteTarget::ExplicitVersion)
    } else {
        None
    }
}

fn object_name_terminal(name: &ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.clone())
}

fn string_literal_expr(value: String) -> Expr {
    Expr::Value(ValueWithSpan::from(AstValue::SingleQuotedString(value)))
}
