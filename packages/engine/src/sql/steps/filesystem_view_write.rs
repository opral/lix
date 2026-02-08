use sqlparser::ast::{
    Delete, Expr, FromTable, Ident, Insert, ObjectName, ObjectNamePart, SetExpr, Statement,
    TableFactor, TableObject, TableWithJoins, Update, Value as AstValue, ValueWithSpan, Values,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::sql::escape_sql_string;
use crate::sql::lowering::lower_statement;
use crate::sql::route::rewrite_read_query_with_backend;
use crate::sql::row_resolution::{resolve_expr_cell_with_state, resolve_values_rows, ResolvedCell};
use crate::sql::PlaceholderState;
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
    parse_active_version_snapshot,
};
use crate::{LixBackend, LixError, Value as EngineValue};

const FILE_VIEW: &str = "lix_file";
const FILE_BY_VERSION_VIEW: &str = "lix_file_by_version";
const FILE_HISTORY_VIEW: &str = "lix_file_history";
const DIRECTORY_VIEW: &str = "lix_directory";
const DIRECTORY_BY_VERSION_VIEW: &str = "lix_directory_by_version";
const DIRECTORY_HISTORY_VIEW: &str = "lix_directory_history";

const FILE_DESCRIPTOR_VIEW: &str = "lix_file_descriptor";
const FILE_DESCRIPTOR_BY_VERSION_VIEW: &str = "lix_file_descriptor_by_version";
const DIRECTORY_DESCRIPTOR_VIEW: &str = "lix_directory_descriptor";
const DIRECTORY_DESCRIPTOR_BY_VERSION_VIEW: &str = "lix_directory_descriptor_by_version";

pub fn rewrite_insert(mut insert: Insert) -> Result<Option<Insert>, LixError> {
    let Some(target) = target_from_table_object(&insert.table) else {
        return Ok(None);
    };
    if target.read_only {
        return Err(LixError {
            message: format!("{} does not support INSERT", target.view_name),
        });
    }

    if target.is_file {
        strip_file_data_from_insert(&mut insert)?;
    }

    insert.table = TableObject::TableName(table_name(target.rewrite_view_name));
    Ok(Some(insert))
}

pub async fn rewrite_insert_with_backend(
    backend: &dyn LixBackend,
    mut insert: Insert,
    params: &[EngineValue],
) -> Result<Option<Insert>, LixError> {
    let Some(target) = target_from_table_object(&insert.table) else {
        return Ok(None);
    };
    if target.read_only {
        return Err(LixError {
            message: format!("{} does not support INSERT", target.view_name),
        });
    }

    if !target.is_file {
        validate_directory_insert_uniqueness(backend, &insert, params, target).await?;
    }

    if target.is_file {
        strip_file_data_from_insert(&mut insert)?;
    }

    insert.table = TableObject::TableName(table_name(target.rewrite_view_name));
    Ok(Some(insert))
}

pub async fn insert_side_effect_statements_with_backend(
    backend: &dyn LixBackend,
    insert: &Insert,
    params: &[EngineValue],
) -> Result<Vec<Statement>, LixError> {
    let Some(target) = target_from_table_object(&insert.table) else {
        return Ok(Vec::new());
    };
    if target.read_only {
        return Ok(Vec::new());
    }

    let source = match &insert.source {
        Some(source) => source,
        None => return Ok(Vec::new()),
    };
    let SetExpr::Values(values) = source.body.as_ref() else {
        return Ok(Vec::new());
    };
    if values.rows.is_empty() {
        return Ok(Vec::new());
    }

    let path_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("path"));
    let Some(path_index) = path_index else {
        return Ok(Vec::new());
    };

    let by_version_index = insert.columns.iter().position(|column| {
        column.value.eq_ignore_ascii_case("lixcol_version_id")
            || column.value.eq_ignore_ascii_case("version_id")
    });
    let active_version_id = if target.uses_active_version_scope() {
        Some(load_active_version_id(backend).await?)
    } else {
        None
    };

    let resolved_rows = resolve_values_rows(&values.rows, params)?;
    let mut directory_requests: Vec<(String, String)> = Vec::new();

    for (row, resolved_row) in values.rows.iter().zip(resolved_rows.iter()) {
        if row.len() != insert.columns.len() {
            return Err(LixError {
                message: "filesystem insert row length does not match column count".to_string(),
            });
        }

        let Some(path) =
            resolve_text_expr(row.get(path_index), resolved_row.get(path_index), "path")?
        else {
            continue;
        };

        let version_id = if let Some(active_version_id) = &active_version_id {
            active_version_id.clone()
        } else if target.requires_explicit_version_scope() {
            let Some(version_index) = by_version_index else {
                continue;
            };
            let Some(version_id) = resolve_text_expr(
                row.get(version_index),
                resolved_row.get(version_index),
                "version_id",
            )?
            else {
                continue;
            };
            version_id
        } else {
            continue;
        };

        if target.is_file {
            for ancestor in file_ancestor_directory_paths(&path) {
                directory_requests.push((version_id.clone(), ancestor));
            }
        } else {
            for ancestor in directory_ancestor_paths(&path) {
                directory_requests.push((version_id.clone(), ancestor));
            }
        }
    }

    if directory_requests.is_empty() {
        return Ok(Vec::new());
    }

    directory_requests.sort_by(|left, right| {
        let version_order = left.0.cmp(&right.0);
        if version_order != std::cmp::Ordering::Equal {
            return version_order;
        }
        let left_depth = path_depth(&left.1);
        let right_depth = path_depth(&right.1);
        left_depth
            .cmp(&right_depth)
            .then_with(|| left.1.cmp(&right.1))
    });
    directory_requests.dedup();

    let mut known_ids: std::collections::BTreeMap<(String, String), String> =
        std::collections::BTreeMap::new();
    let mut statements: Vec<Statement> = Vec::new();

    for (version_id, path) in directory_requests {
        let key = (version_id.clone(), path.clone());
        if known_ids.contains_key(&key) {
            continue;
        }

        if let Some(existing_id) = find_directory_id_by_path(backend, &version_id, &path).await? {
            known_ids.insert(key, existing_id);
            continue;
        }

        let parent_id = match parent_directory_path(&path) {
            Some(parent_path) => {
                let parent_key = (version_id.clone(), parent_path.clone());
                if let Some(parent_id) = known_ids.get(&parent_key) {
                    Some(parent_id.clone())
                } else if let Some(existing_parent_id) =
                    find_directory_id_by_path(backend, &version_id, &parent_path).await?
                {
                    known_ids.insert(parent_key, existing_parent_id.clone());
                    Some(existing_parent_id)
                } else {
                    None
                }
            }
            None => None,
        };

        let id = auto_directory_id(&version_id, &path);
        let name = directory_name_from_path(&path).unwrap_or_default();
        let statement_sql = if target.uses_active_version_scope() {
            format!(
                "INSERT INTO {table} (id, path, parent_id, name, hidden, lixcol_untracked) \
                 VALUES ('{id}', '{path}', {parent_id}, '{name}', 0, 1)",
                table = DIRECTORY_DESCRIPTOR_VIEW,
                id = escape_sql_string(&id),
                path = escape_sql_string(&path),
                parent_id = parent_id
                    .map(|value| format!("'{}'", escape_sql_string(&value)))
                    .unwrap_or_else(|| "NULL".to_string()),
                name = escape_sql_string(&name),
            )
        } else {
            format!(
                "INSERT INTO {table} (id, path, parent_id, name, hidden, lixcol_version_id, lixcol_untracked) \
                 VALUES ('{id}', '{path}', {parent_id}, '{name}', 0, '{version_id}', 1)",
                table = DIRECTORY_DESCRIPTOR_BY_VERSION_VIEW,
                id = escape_sql_string(&id),
                path = escape_sql_string(&path),
                parent_id = parent_id
                    .map(|value| format!("'{}'", escape_sql_string(&value)))
                    .unwrap_or_else(|| "NULL".to_string()),
                name = escape_sql_string(&name),
                version_id = escape_sql_string(&version_id),
            )
        };
        statements.push(parse_single_statement(&statement_sql)?);
        known_ids.insert(key, id);
    }

    Ok(statements)
}

pub fn rewrite_update(mut update: Update) -> Result<Option<Statement>, LixError> {
    let Some(target) = target_from_update_table(&update.table) else {
        return Ok(None);
    };
    if target.read_only {
        return Err(LixError {
            message: format!("{} does not support UPDATE", target.view_name),
        });
    }

    if target.is_file {
        update.assignments.retain(|assignment| {
            assignment_target_name(assignment)
                .map(|name| !name.eq_ignore_ascii_case("data"))
                .unwrap_or(true)
        });
        if update.assignments.is_empty() {
            return Ok(Some(noop_statement()?));
        }
    }

    replace_update_target_table(&mut update.table, target.rewrite_view_name)?;
    Ok(Some(Statement::Update(update)))
}

pub async fn rewrite_update_with_backend(
    backend: &dyn LixBackend,
    mut update: Update,
    params: &[EngineValue],
) -> Result<Option<Statement>, LixError> {
    let Some(target) = target_from_update_table(&update.table) else {
        return Ok(None);
    };
    if target.read_only {
        return Err(LixError {
            message: format!("{} does not support UPDATE", target.view_name),
        });
    }

    if !target.is_file {
        validate_directory_update_uniqueness(backend, &update, params, target).await?;
    }

    if target.is_file {
        update.assignments.retain(|assignment| {
            assignment_target_name(assignment)
                .map(|name| !name.eq_ignore_ascii_case("data"))
                .unwrap_or(true)
        });
        if update.assignments.is_empty() {
            return Ok(Some(noop_statement()?));
        }
    }

    replace_update_target_table(&mut update.table, target.rewrite_view_name)?;
    Ok(Some(Statement::Update(update)))
}

pub fn rewrite_delete(mut delete: Delete) -> Result<Option<Delete>, LixError> {
    let Some(target) = target_from_delete(&delete) else {
        return Ok(None);
    };
    if target.read_only {
        return Err(LixError {
            message: format!("{} does not support DELETE", target.view_name),
        });
    }

    replace_delete_target_table(&mut delete, target.rewrite_view_name)?;
    Ok(Some(delete))
}

pub async fn rewrite_delete_with_backend(
    backend: &dyn LixBackend,
    mut delete: Delete,
) -> Result<Option<Delete>, LixError> {
    let Some(target) = target_from_delete(&delete) else {
        return Ok(None);
    };
    if target.read_only {
        return Err(LixError {
            message: format!("{} does not support DELETE", target.view_name),
        });
    }

    if target.is_directory() {
        let directory_paths = directory_paths_matching_delete(backend, &delete, target).await?;
        if !directory_paths.is_empty() {
            let mut predicate_clauses: Vec<String> = Vec::new();
            if let Some(selection) = &delete.selection {
                predicate_clauses.push(format!("({selection})"));
            }
            for path in directory_paths {
                predicate_clauses.push(format!("path LIKE '{}%'", escape_sql_string(&path)));
            }
            if !predicate_clauses.is_empty() {
                let combined = predicate_clauses.join(" OR ");
                delete.selection = Some(parse_expression(&combined)?);
            }
        }
    }

    replace_delete_target_table(&mut delete, target.rewrite_view_name)?;
    Ok(Some(delete))
}

#[derive(Clone, Copy)]
struct FilesystemTarget {
    view_name: &'static str,
    rewrite_view_name: &'static str,
    read_only: bool,
    is_file: bool,
}

impl FilesystemTarget {
    fn is_directory(self) -> bool {
        !self.is_file
    }

    fn uses_active_version_scope(self) -> bool {
        self.view_name.eq_ignore_ascii_case(FILE_VIEW)
            || self.view_name.eq_ignore_ascii_case(DIRECTORY_VIEW)
    }

    fn requires_explicit_version_scope(self) -> bool {
        self.view_name.eq_ignore_ascii_case(FILE_BY_VERSION_VIEW)
            || self
                .view_name
                .eq_ignore_ascii_case(DIRECTORY_BY_VERSION_VIEW)
    }
}

fn target_from_table_object(table: &TableObject) -> Option<FilesystemTarget> {
    let name = match table {
        TableObject::TableName(name) => name,
        _ => return None,
    };
    let view_name = object_name_terminal(name)?;
    target_from_view_name(&view_name)
}

fn target_from_update_table(table: &TableWithJoins) -> Option<FilesystemTarget> {
    if !table.joins.is_empty() {
        return None;
    }
    let TableFactor::Table { name, .. } = &table.relation else {
        return None;
    };
    let view_name = object_name_terminal(name)?;
    target_from_view_name(&view_name)
}

fn target_from_delete(delete: &Delete) -> Option<FilesystemTarget> {
    let tables = match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    if tables.len() != 1 {
        return None;
    }
    target_from_update_table(&tables[0])
}

fn target_from_view_name(view_name: &str) -> Option<FilesystemTarget> {
    match view_name.to_ascii_lowercase().as_str() {
        FILE_VIEW => Some(FilesystemTarget {
            view_name: FILE_VIEW,
            rewrite_view_name: FILE_DESCRIPTOR_VIEW,
            read_only: false,
            is_file: true,
        }),
        FILE_BY_VERSION_VIEW => Some(FilesystemTarget {
            view_name: FILE_BY_VERSION_VIEW,
            rewrite_view_name: FILE_DESCRIPTOR_BY_VERSION_VIEW,
            read_only: false,
            is_file: true,
        }),
        FILE_HISTORY_VIEW => Some(FilesystemTarget {
            view_name: FILE_HISTORY_VIEW,
            rewrite_view_name: FILE_DESCRIPTOR_VIEW,
            read_only: true,
            is_file: true,
        }),
        DIRECTORY_VIEW => Some(FilesystemTarget {
            view_name: DIRECTORY_VIEW,
            rewrite_view_name: DIRECTORY_DESCRIPTOR_VIEW,
            read_only: false,
            is_file: false,
        }),
        DIRECTORY_BY_VERSION_VIEW => Some(FilesystemTarget {
            view_name: DIRECTORY_BY_VERSION_VIEW,
            rewrite_view_name: DIRECTORY_DESCRIPTOR_BY_VERSION_VIEW,
            read_only: false,
            is_file: false,
        }),
        DIRECTORY_HISTORY_VIEW => Some(FilesystemTarget {
            view_name: DIRECTORY_HISTORY_VIEW,
            rewrite_view_name: DIRECTORY_DESCRIPTOR_VIEW,
            read_only: true,
            is_file: false,
        }),
        _ => None,
    }
}

async fn validate_directory_insert_uniqueness(
    backend: &dyn LixBackend,
    insert: &Insert,
    params: &[EngineValue],
    target: FilesystemTarget,
) -> Result<(), LixError> {
    if !target.is_directory() || target.read_only {
        return Ok(());
    }

    let path_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("path"));
    let Some(path_index) = path_index else {
        return Ok(());
    };

    let id_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("id"));
    let by_version_index = insert.columns.iter().position(|column| {
        column.value.eq_ignore_ascii_case("lixcol_version_id")
            || column.value.eq_ignore_ascii_case("version_id")
    });

    let active_version_id = if target.uses_active_version_scope() {
        Some(load_active_version_id(backend).await?)
    } else {
        None
    };

    let source = match &insert.source {
        Some(source) => source,
        None => return Ok(()),
    };
    let SetExpr::Values(values) = source.body.as_ref() else {
        return Ok(());
    };

    let resolved_rows = resolve_values_rows(&values.rows, params)?;
    let mut pending_paths: Vec<(String, String, Option<String>)> = Vec::new();

    for (row, resolved_row) in values.rows.iter().zip(resolved_rows.iter()) {
        if row.len() != insert.columns.len() {
            return Err(LixError {
                message: "directory insert row length does not match column count".to_string(),
            });
        }

        let Some(path) = resolve_text_expr(
            row.get(path_index),
            resolved_row.get(path_index),
            "directory path",
        )?
        else {
            continue;
        };

        let version_id = if let Some(active_version_id) = &active_version_id {
            active_version_id.clone()
        } else if target.requires_explicit_version_scope() {
            let Some(version_index) = by_version_index else {
                continue;
            };
            let Some(version_id) = resolve_text_expr(
                row.get(version_index),
                resolved_row.get(version_index),
                "directory version_id",
            )?
            else {
                continue;
            };
            version_id
        } else {
            continue;
        };

        let id = match id_index {
            Some(index) => {
                resolve_text_expr(row.get(index), resolved_row.get(index), "directory id")?
            }
            None => None,
        };

        for (existing_version_id, existing_path, existing_id) in &pending_paths {
            if existing_version_id == &version_id && existing_path == &path {
                let same_id = match (existing_id.as_deref(), id.as_deref()) {
                    (Some(existing_id), Some(id)) => existing_id == id,
                    _ => false,
                };
                if !same_id {
                    return Err(directory_path_unique_error(&path, &version_id));
                }
            }
        }

        if let Some(existing_id) = find_directory_id_by_path(backend, &version_id, &path).await? {
            let same_id = id
                .as_deref()
                .map(|candidate_id| candidate_id == existing_id.as_str())
                .unwrap_or(false);
            if !same_id {
                return Err(directory_path_unique_error(&path, &version_id));
            }
        }

        pending_paths.push((version_id, path, id));
    }

    Ok(())
}

async fn validate_directory_update_uniqueness(
    backend: &dyn LixBackend,
    update: &Update,
    params: &[EngineValue],
    target: FilesystemTarget,
) -> Result<(), LixError> {
    if !target.is_directory() || target.read_only {
        return Ok(());
    }

    let mut placeholder_state = PlaceholderState::new();
    let mut next_path: Option<String> = None;
    for assignment in &update.assignments {
        let Some(column) = assignment_target_name(assignment) else {
            continue;
        };
        if !column.eq_ignore_ascii_case("path") {
            let _ =
                resolve_expr_cell_with_state(&assignment.value, params, &mut placeholder_state)?;
            continue;
        }
        let resolved =
            resolve_expr_cell_with_state(&assignment.value, params, &mut placeholder_state)?;
        next_path = resolve_text_expr(
            Some(&assignment.value),
            Some(&resolved),
            "directory update path",
        )?;
    }

    let Some(path) = next_path else {
        return Ok(());
    };

    let version_id = if target.uses_active_version_scope() {
        load_active_version_id(backend).await?
    } else if target.requires_explicit_version_scope() {
        let Some(version_id) = extract_predicate_string(
            update.selection.as_ref(),
            &["lixcol_version_id", "version_id"],
        ) else {
            return Ok(());
        };
        version_id
    } else {
        return Ok(());
    };

    let current_directory_id =
        extract_predicate_string(update.selection.as_ref(), &["id", "lixcol_entity_id"]);
    if let Some(existing_id) = find_directory_id_by_path(backend, &version_id, &path).await? {
        let same_id = current_directory_id
            .as_deref()
            .map(|candidate_id| candidate_id == existing_id.as_str())
            .unwrap_or(false);
        if !same_id {
            return Err(directory_path_unique_error(&path, &version_id));
        }
    }

    Ok(())
}

fn directory_path_unique_error(path: &str, version_id: &str) -> LixError {
    LixError {
        message: format!(
            "Unique constraint violation: directory path '{}' already exists in version '{}'",
            path, version_id
        ),
    }
}

async fn find_directory_id_by_path(
    backend: &dyn LixBackend,
    version_id: &str,
    path: &str,
) -> Result<Option<String>, LixError> {
    let lookup_sql = "SELECT id \
         FROM lix_directory_descriptor_by_version \
         WHERE lixcol_version_id = $1 AND path = $2 \
         LIMIT 1";
    let rewritten_lookup_sql = rewrite_single_read_query_for_backend(backend, lookup_sql).await?;
    let result = backend
        .execute(
            &rewritten_lookup_sql,
            &[
                EngineValue::Text(version_id.to_string()),
                EngineValue::Text(path.to_string()),
            ],
        )
        .await?;
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    let Some(value) = row.first() else {
        return Ok(None);
    };
    let EngineValue::Text(id) = value else {
        return Err(LixError {
            message: format!("directory uniqueness check expected text id, got {value:?}"),
        });
    };
    Ok(Some(id.clone()))
}

async fn rewrite_single_read_query_for_backend(
    backend: &dyn LixBackend,
    sql: &str,
) -> Result<String, LixError> {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql).map_err(|error| LixError {
        message: error.to_string(),
    })?;
    if statements.len() != 1 {
        return Err(LixError {
            message: "expected a single SELECT statement".to_string(),
        });
    }
    let statement = statements.remove(0);
    let Statement::Query(query) = statement else {
        return Err(LixError {
            message: "expected SELECT statement".to_string(),
        });
    };
    let rewritten = rewrite_read_query_with_backend(backend, *query).await?;
    let lowered = lower_statement(Statement::Query(Box::new(rewritten)), backend.dialect())?;
    Ok(lowered.to_string())
}

fn resolve_text_expr(
    expr: Option<&Expr>,
    cell: Option<&ResolvedCell>,
    context: &str,
) -> Result<Option<String>, LixError> {
    if let Some(cell) = cell {
        if let Some(value) = &cell.value {
            return match value {
                EngineValue::Null => Ok(None),
                EngineValue::Text(value) => Ok(Some(value.clone())),
                EngineValue::Integer(value) => Ok(Some(value.to_string())),
                EngineValue::Real(value) => Ok(Some(value.to_string())),
                EngineValue::Blob(_) => Err(LixError {
                    message: format!("{context} does not support blob values"),
                }),
            };
        }
    }

    let Some(expr) = expr else {
        return Ok(None);
    };
    let Expr::Value(ValueWithSpan { value, .. }) = expr else {
        return Ok(None);
    };
    match value {
        AstValue::Null => Ok(None),
        AstValue::SingleQuotedString(value)
        | AstValue::DoubleQuotedString(value)
        | AstValue::TripleSingleQuotedString(value)
        | AstValue::TripleDoubleQuotedString(value)
        | AstValue::EscapedStringLiteral(value)
        | AstValue::UnicodeStringLiteral(value)
        | AstValue::NationalStringLiteral(value)
        | AstValue::HexStringLiteral(value)
        | AstValue::SingleQuotedRawStringLiteral(value)
        | AstValue::DoubleQuotedRawStringLiteral(value)
        | AstValue::TripleSingleQuotedRawStringLiteral(value)
        | AstValue::TripleDoubleQuotedRawStringLiteral(value)
        | AstValue::SingleQuotedByteStringLiteral(value)
        | AstValue::DoubleQuotedByteStringLiteral(value)
        | AstValue::TripleSingleQuotedByteStringLiteral(value)
        | AstValue::TripleDoubleQuotedByteStringLiteral(value) => Ok(Some(value.clone())),
        AstValue::DollarQuotedString(value) => Ok(Some(value.value.clone())),
        AstValue::Number(value, _) => Ok(Some(value.clone())),
        AstValue::Boolean(value) => Ok(Some(if *value {
            "1".to_string()
        } else {
            "0".to_string()
        })),
        AstValue::Placeholder(_) => Ok(None),
    }
}

fn extract_predicate_string(selection: Option<&Expr>, columns: &[&str]) -> Option<String> {
    let selection = selection?;
    match selection {
        Expr::BinaryOp { left, op, right } => {
            if op.to_string().eq_ignore_ascii_case("=") {
                if let Some(column) = expr_column_name(left) {
                    if columns
                        .iter()
                        .any(|candidate| column.eq_ignore_ascii_case(candidate))
                    {
                        if let Some(value) = expr_string_literal(right) {
                            return Some(value);
                        }
                    }
                }
                if let Some(column) = expr_column_name(right) {
                    if columns
                        .iter()
                        .any(|candidate| column.eq_ignore_ascii_case(candidate))
                    {
                        if let Some(value) = expr_string_literal(left) {
                            return Some(value);
                        }
                    }
                }
            }
            extract_predicate_string(Some(left), columns)
                .or_else(|| extract_predicate_string(Some(right), columns))
        }
        Expr::Nested(inner) => extract_predicate_string(Some(inner), columns),
        _ => None,
    }
}

fn expr_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|ident| ident.value.clone()),
        _ => None,
    }
}

fn expr_string_literal(expr: &Expr) -> Option<String> {
    let Expr::Value(ValueWithSpan { value, .. }) = expr else {
        return None;
    };
    match value {
        AstValue::SingleQuotedString(value)
        | AstValue::DoubleQuotedString(value)
        | AstValue::TripleSingleQuotedString(value)
        | AstValue::TripleDoubleQuotedString(value)
        | AstValue::EscapedStringLiteral(value)
        | AstValue::UnicodeStringLiteral(value)
        | AstValue::NationalStringLiteral(value)
        | AstValue::HexStringLiteral(value)
        | AstValue::SingleQuotedRawStringLiteral(value)
        | AstValue::DoubleQuotedRawStringLiteral(value)
        | AstValue::TripleSingleQuotedRawStringLiteral(value)
        | AstValue::TripleDoubleQuotedRawStringLiteral(value)
        | AstValue::SingleQuotedByteStringLiteral(value)
        | AstValue::DoubleQuotedByteStringLiteral(value)
        | AstValue::TripleSingleQuotedByteStringLiteral(value)
        | AstValue::TripleDoubleQuotedByteStringLiteral(value) => Some(value.clone()),
        AstValue::DollarQuotedString(value) => Some(value.value.clone()),
        AstValue::Number(value, _) => Some(value.clone()),
        AstValue::Boolean(value) => Some(if *value {
            "1".to_string()
        } else {
            "0".to_string()
        }),
        AstValue::Null | AstValue::Placeholder(_) => None,
    }
}

async fn load_active_version_id(backend: &dyn LixBackend) -> Result<String, LixError> {
    let result = backend
        .execute(
            "SELECT snapshot_content \
             FROM lix_internal_state_untracked \
             WHERE schema_key = $1 \
               AND file_id = $2 \
               AND version_id = $3 \
               AND snapshot_content IS NOT NULL \
             ORDER BY updated_at DESC \
             LIMIT 1",
            &[
                EngineValue::Text(active_version_schema_key().to_string()),
                EngineValue::Text(active_version_file_id().to_string()),
                EngineValue::Text(active_version_storage_version_id().to_string()),
            ],
        )
        .await?;
    let Some(row) = result.rows.first() else {
        return Err(LixError {
            message: "filesystem rewrite requires an active version".to_string(),
        });
    };
    let Some(snapshot_content) = row.first() else {
        return Err(LixError {
            message: "filesystem active version query row is missing snapshot_content".to_string(),
        });
    };
    let EngineValue::Text(snapshot_content) = snapshot_content else {
        return Err(LixError {
            message: format!(
                "filesystem active version snapshot_content must be text, got {snapshot_content:?}"
            ),
        });
    };
    parse_active_version_snapshot(snapshot_content)
}

fn path_depth(path: &str) -> usize {
    path.trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .count()
}

fn file_ancestor_directory_paths(path: &str) -> Vec<String> {
    let segments = path
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.len() <= 1 {
        return Vec::new();
    }

    let mut ancestors = Vec::with_capacity(segments.len() - 1);
    let mut prefix_segments: Vec<&str> = Vec::with_capacity(segments.len() - 1);
    for segment in segments.iter().take(segments.len() - 1) {
        prefix_segments.push(segment);
        ancestors.push(format!("/{}/", prefix_segments.join("/")));
    }
    ancestors
}

fn directory_ancestor_paths(path: &str) -> Vec<String> {
    let segments = path
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.len() <= 1 {
        return Vec::new();
    }

    let mut ancestors = Vec::with_capacity(segments.len() - 1);
    let mut prefix_segments: Vec<&str> = Vec::with_capacity(segments.len() - 1);
    for segment in segments.iter().take(segments.len() - 1) {
        prefix_segments.push(segment);
        ancestors.push(format!("/{}/", prefix_segments.join("/")));
    }
    ancestors
}

fn parent_directory_path(path: &str) -> Option<String> {
    let segments = path
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.len() <= 1 {
        return None;
    }
    Some(format!("/{}/", segments[..segments.len() - 1].join("/")))
}

fn directory_name_from_path(path: &str) -> Option<String> {
    path.trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .next_back()
        .map(|segment| segment.to_string())
}

fn auto_directory_id(version_id: &str, path: &str) -> String {
    format!("lix-auto-dir:{}:{}", version_id, path)
}

async fn directory_paths_matching_delete(
    backend: &dyn LixBackend,
    delete: &Delete,
    target: FilesystemTarget,
) -> Result<Vec<String>, LixError> {
    let where_clause = delete
        .selection
        .as_ref()
        .map(|selection| format!(" WHERE {selection}"))
        .unwrap_or_default();
    let sql = format!(
        "SELECT path FROM {view_name}{where_clause}",
        view_name = target.view_name,
        where_clause = where_clause,
    );
    let rewritten_sql = rewrite_single_read_query_for_backend(backend, &sql).await?;
    let result = backend.execute(&rewritten_sql, &[]).await?;

    let mut paths = Vec::new();
    for row in result.rows {
        let Some(value) = row.first() else {
            continue;
        };
        match value {
            EngineValue::Text(path) => paths.push(path.clone()),
            other => {
                return Err(LixError {
                    message: format!("directory delete path lookup expected text, got {other:?}"),
                });
            }
        }
    }
    paths.sort();
    paths.dedup();
    Ok(paths)
}

fn parse_expression(sql: &str) -> Result<Expr, LixError> {
    let wrapped_sql = format!("SELECT 1 WHERE {sql}");
    let mut statements =
        Parser::parse_sql(&GenericDialect {}, &wrapped_sql).map_err(|error| LixError {
            message: error.to_string(),
        })?;
    if statements.len() != 1 {
        return Err(LixError {
            message: "failed to parse expression".to_string(),
        });
    }
    let Statement::Query(query) = statements.remove(0) else {
        return Err(LixError {
            message: "failed to parse expression query".to_string(),
        });
    };
    let SetExpr::Select(select) = *query.body else {
        return Err(LixError {
            message: "failed to parse expression SELECT".to_string(),
        });
    };
    select.selection.ok_or_else(|| LixError {
        message: "failed to parse expression selection".to_string(),
    })
}

fn parse_single_statement(sql: &str) -> Result<Statement, LixError> {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql).map_err(|error| LixError {
        message: error.to_string(),
    })?;
    if statements.len() != 1 {
        return Err(LixError {
            message: "expected a single statement".to_string(),
        });
    }
    Ok(statements.remove(0))
}

fn strip_file_data_from_insert(insert: &mut Insert) -> Result<(), LixError> {
    let data_column_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("data"));
    let Some(data_column_index) = data_column_index else {
        return Ok(());
    };

    insert.columns.remove(data_column_index);
    let source = insert.source.as_mut().ok_or_else(|| LixError {
        message: "file insert with data requires VALUES rows".to_string(),
    })?;
    let SetExpr::Values(Values { rows, .. }) = source.body.as_mut() else {
        return Err(LixError {
            message: "file insert with data requires VALUES rows".to_string(),
        });
    };

    for row in rows.iter_mut() {
        if data_column_index >= row.len() {
            return Err(LixError {
                message: "file insert row length does not match column count".to_string(),
            });
        }
        row.remove(data_column_index);
    }

    if insert.columns.is_empty() {
        return Err(LixError {
            message: "file insert requires at least one non-data column".to_string(),
        });
    }

    Ok(())
}

fn replace_update_target_table(
    table: &mut TableWithJoins,
    rewrite_view_name: &str,
) -> Result<(), LixError> {
    if !table.joins.is_empty() {
        return Err(LixError {
            message: "filesystem update does not support JOIN targets".to_string(),
        });
    }
    let TableFactor::Table { name, .. } = &mut table.relation else {
        return Err(LixError {
            message: "filesystem update requires table target".to_string(),
        });
    };
    *name = table_name(rewrite_view_name);
    Ok(())
}

fn replace_delete_target_table(
    delete: &mut Delete,
    rewrite_view_name: &str,
) -> Result<(), LixError> {
    let tables = match &mut delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    let Some(table) = tables.first_mut() else {
        return Err(LixError {
            message: "filesystem delete requires table target".to_string(),
        });
    };
    replace_update_target_table(table, rewrite_view_name)
}

fn assignment_target_name(assignment: &sqlparser::ast::Assignment) -> Option<String> {
    let sqlparser::ast::AssignmentTarget::ColumnName(name) = &assignment.target else {
        return None;
    };
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.clone())
}

fn object_name_terminal(name: &ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.clone())
}

fn table_name(name: &str) -> ObjectName {
    ObjectName(vec![ObjectNamePart::Identifier(Ident::new(name))])
}

fn noop_statement() -> Result<Statement, LixError> {
    let mut statements =
        Parser::parse_sql(&GenericDialect {}, "SELECT 0 WHERE 1 = 0").map_err(|error| {
            LixError {
                message: error.to_string(),
            }
        })?;
    if statements.len() != 1 {
        return Err(LixError {
            message: "failed to build filesystem no-op statement".to_string(),
        });
    }
    Ok(statements.remove(0))
}

#[cfg(test)]
mod tests {
    use super::{rewrite_delete, rewrite_insert, rewrite_update};
    use crate::sql::pipeline::parse_sql_statements;
    use sqlparser::ast::Statement;

    #[test]
    fn rewrites_file_insert_and_drops_data_column() {
        let sql =
            "INSERT INTO lix_file (id, path, data, metadata) VALUES ('f1', '/a.txt', X'00', '{}')";
        let statements = parse_sql_statements(sql).expect("parse");
        let insert = match statements.into_iter().next().expect("statement") {
            Statement::Insert(insert) => insert,
            _ => panic!("expected insert"),
        };

        let rewritten = rewrite_insert(insert)
            .expect("rewrite")
            .expect("should rewrite");
        assert_eq!(rewritten.table.to_string(), "lix_file_descriptor");
        assert_eq!(
            rewritten
                .columns
                .iter()
                .map(|column| column.value.clone())
                .collect::<Vec<_>>(),
            vec!["id", "path", "metadata"]
        );
    }

    #[test]
    fn rewrites_data_only_update_to_noop_statement() {
        let sql = "UPDATE lix_file SET data = X'01' WHERE id = 'f1'";
        let statements = parse_sql_statements(sql).expect("parse");
        let update = match statements.into_iter().next().expect("statement") {
            Statement::Update(update) => update,
            _ => panic!("expected update"),
        };

        let rewritten = rewrite_update(update)
            .expect("rewrite")
            .expect("should rewrite");
        assert_eq!(rewritten.to_string(), "SELECT 0 WHERE 1 = 0");
    }

    #[test]
    fn rewrites_directory_delete_target() {
        let sql = "DELETE FROM lix_directory WHERE path = '/docs/'";
        let statements = parse_sql_statements(sql).expect("parse");
        let delete = match statements.into_iter().next().expect("statement") {
            Statement::Delete(delete) => delete,
            _ => panic!("expected delete"),
        };

        let rewritten = rewrite_delete(delete)
            .expect("rewrite")
            .expect("should rewrite");
        assert!(rewritten
            .to_string()
            .contains("DELETE FROM lix_directory_descriptor"));
    }
}
