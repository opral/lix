use crate::cel::CelEvaluator;
use crate::sql::{
    bind_sql_with_state, escape_sql_string, parse_sql_statements, preprocess_sql,
    resolve_expr_cell_with_state, resolve_values_rows, PlaceholderState,
};
use crate::{LixBackend, LixError, QueryResult, Value};
use sqlparser::ast::{
    Assignment, FromTable, ObjectName, ObjectNamePart, SetExpr, Statement, TableFactor,
    TableObject, Update,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone)]
pub(crate) struct PendingFileWrite {
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) before_path: Option<String>,
    pub(crate) path: String,
    pub(crate) data_is_authoritative: bool,
    pub(crate) before_data: Option<Vec<u8>>,
    pub(crate) after_data: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileWriteTarget {
    ActiveVersion,
    ExplicitVersion,
}

pub(crate) async fn collect_pending_file_writes(
    backend: &dyn LixBackend,
    sql: &str,
    params: &[Value],
    active_version_id: &str,
) -> Result<Vec<PendingFileWrite>, LixError> {
    let statements = parse_sql_statements(sql)?;
    let mut writes = Vec::new();
    let mut overlay = BTreeMap::<(String, String), OverlayWriteState>::new();

    for statement in statements {
        let start_len = writes.len();
        match statement {
            Statement::Insert(insert) => {
                collect_insert_writes(&insert, params, active_version_id, &mut writes)?;
            }
            Statement::Update(update) => {
                collect_update_writes(
                    backend,
                    &update,
                    params,
                    active_version_id,
                    &overlay,
                    &mut writes,
                )
                .await?;
            }
            _ => {}
        }
        apply_statement_writes_to_overlay(&writes[start_len..], &mut overlay);
    }

    Ok(writes)
}

pub(crate) async fn collect_pending_file_delete_targets(
    backend: &dyn LixBackend,
    sql: &str,
    params: &[Value],
    active_version_id: &str,
) -> Result<BTreeSet<(String, String)>, LixError> {
    let statements = parse_sql_statements(sql)?;
    let mut targets = BTreeSet::new();
    let mut overlay = BTreeMap::<(String, String), OverlayWriteState>::new();
    let mut writes = Vec::new();

    for statement in statements {
        let start_len = writes.len();
        match statement {
            Statement::Insert(insert) => {
                collect_insert_writes(&insert, params, active_version_id, &mut writes)?;
                apply_statement_writes_to_overlay(&writes[start_len..], &mut overlay);
            }
            Statement::Update(update) => {
                collect_update_writes(
                    backend,
                    &update,
                    params,
                    active_version_id,
                    &overlay,
                    &mut writes,
                )
                .await?;
                apply_statement_writes_to_overlay(&writes[start_len..], &mut overlay);
            }
            Statement::Delete(delete) => {
                let statement_targets = collect_delete_targets(
                    backend,
                    &delete,
                    params,
                    active_version_id,
                    &overlay,
                    &mut targets,
                )
                .await?;
                for target in statement_targets {
                    overlay.remove(&target);
                }
            }
            _ => {}
        }
    }

    Ok(targets)
}

fn collect_insert_writes(
    insert: &sqlparser::ast::Insert,
    params: &[Value],
    active_version_id: &str,
    writes: &mut Vec<PendingFileWrite>,
) -> Result<(), LixError> {
    let Some(target) = file_write_target_from_insert(&insert.table) else {
        return Ok(());
    };

    let Some(source) = insert.source.as_ref() else {
        return Ok(());
    };
    let SetExpr::Values(values) = source.body.as_ref() else {
        return Ok(());
    };

    let data_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("data"));
    let id_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("id"));
    let path_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("path"));
    let version_index = insert.columns.iter().position(|column| {
        column.value.eq_ignore_ascii_case("lixcol_version_id")
            || column.value.eq_ignore_ascii_case("version_id")
    });

    let (Some(data_index), Some(id_index), Some(path_index)) = (data_index, id_index, path_index)
    else {
        return Ok(());
    };

    let resolved_rows = resolve_values_rows(&values.rows, params)?;
    for (row, resolved_row) in values.rows.iter().zip(resolved_rows.iter()) {
        if row.len() != insert.columns.len() {
            continue;
        }

        let Some(file_id) = resolved_cell_text(resolved_row.get(id_index)) else {
            continue;
        };
        let Some(path) = resolved_cell_text(resolved_row.get(path_index)) else {
            continue;
        };
        let Some(after_data) = resolved_cell_blob_or_text_bytes(resolved_row.get(data_index))
        else {
            continue;
        };

        let version_id = match target {
            FileWriteTarget::ActiveVersion => active_version_id.to_string(),
            FileWriteTarget::ExplicitVersion => {
                let Some(version_index) = version_index else {
                    continue;
                };
                let Some(version_id) = resolved_cell_text(resolved_row.get(version_index)) else {
                    continue;
                };
                version_id
            }
        };

        writes.push(PendingFileWrite {
            file_id,
            version_id,
            before_path: None,
            path,
            data_is_authoritative: true,
            before_data: None,
            after_data,
        });
    }

    Ok(())
}

async fn collect_update_writes(
    backend: &dyn LixBackend,
    update: &Update,
    params: &[Value],
    active_version_id: &str,
    overlay: &BTreeMap<(String, String), OverlayWriteState>,
    writes: &mut Vec<PendingFileWrite>,
) -> Result<(), LixError> {
    let Some(target) = file_write_target_from_update(update) else {
        return Ok(());
    };

    let mut placeholder_state = PlaceholderState::new();
    let mut assigned_after_data: Option<Vec<u8>> = None;
    let mut saw_data_assignment = false;
    let mut next_path: Option<String> = None;
    let mut next_file_id: Option<String> = None;

    for assignment in &update.assignments {
        let Some(column) = assignment_target_name(assignment) else {
            continue;
        };
        let resolved =
            resolve_expr_cell_with_state(&assignment.value, params, &mut placeholder_state)?;
        if column.eq_ignore_ascii_case("data") {
            saw_data_assignment = true;
            assigned_after_data = resolved_cell_blob_or_text_bytes(Some(&resolved));
            if assigned_after_data.is_none() {
                return Err(LixError {
                    message: format!(
                        "unsupported file data update expression '{}': only literal/blob or bound placeholder values are supported",
                        assignment.value
                    ),
                });
            }
        } else if column.eq_ignore_ascii_case("path") {
            next_path = resolved_cell_text(Some(&resolved));
        } else if column.eq_ignore_ascii_case("id") {
            next_file_id = resolved_cell_text(Some(&resolved));
        }
    }

    if !saw_data_assignment && next_path.is_none() {
        return Ok(());
    }

    let mut query_sql = match target {
        FileWriteTarget::ActiveVersion => "SELECT id, path, data FROM lix_file".to_string(),
        FileWriteTarget::ExplicitVersion => {
            "SELECT id, path, data, lixcol_version_id FROM lix_file_by_version".to_string()
        }
    };
    if let Some(selection) = update.selection.as_ref() {
        query_sql.push_str(" WHERE ");
        query_sql.push_str(&selection.to_string());
    }

    let bound = bind_sql_with_state(&query_sql, params, backend.dialect(), placeholder_state)?;
    let rows = execute_prefetch_query(backend, &bound.sql, &bound.params)
        .await
        .map_err(|error| LixError {
            message: format!(
                "pending_file_writes prefetch failed for '{}': {}",
                bound.sql, error.message
            ),
        })?
        .rows;

    let mut pending = Vec::with_capacity(rows.len());
    let mut cache_lookup_keys = BTreeSet::<(String, String)>::new();

    for row in rows {
        let Some(before_file_id) = row.get(0).and_then(value_as_text) else {
            continue;
        };
        let Some(before_path) = row.get(1).and_then(value_as_text) else {
            continue;
        };
        let before_path_for_write = before_path.clone();
        let file_id = next_file_id.clone().unwrap_or(before_file_id);
        let path = next_path.clone().unwrap_or(before_path);
        let version_id = match target {
            FileWriteTarget::ActiveVersion => active_version_id.to_string(),
            FileWriteTarget::ExplicitVersion => row
                .get(3)
                .and_then(value_as_text)
                .unwrap_or_else(|| active_version_id.to_string()),
        };
        let before_data = row.get(2).and_then(value_as_blob_or_text_bytes);
        if before_data.is_none() || before_data.as_ref().is_some_and(|bytes| bytes.is_empty()) {
            cache_lookup_keys.insert((file_id.clone(), version_id.clone()));
        }

        pending.push(PendingFileWrite {
            file_id,
            version_id,
            before_path: Some(before_path_for_write),
            path,
            data_is_authoritative: saw_data_assignment,
            before_data,
            after_data: assigned_after_data.clone().unwrap_or_else(|| Vec::new()),
        });
    }

    if !cache_lookup_keys.is_empty() {
        let cache_data = load_before_data_from_cache_batch(
            backend,
            &cache_lookup_keys.into_iter().collect::<Vec<_>>(),
        )
        .await?;
        for write in &mut pending {
            let key = (write.file_id.clone(), write.version_id.clone());
            if write.before_data.is_none() {
                write.before_data = cache_data.get(&key).cloned();
            } else if write
                .before_data
                .as_ref()
                .is_some_and(|bytes| bytes.is_empty())
                && !cache_data.contains_key(&key)
            {
                // lix_file views coalesce cache misses to empty blobs; convert that shape back
                // to None so detect stage can reconstruct true before_data from state.
                write.before_data = None;
            }
        }
    }

    for write in &mut pending {
        if let Some(overlay_state) = overlay.get(&(write.file_id.clone(), write.version_id.clone()))
        {
            write.before_data = Some(overlay_state.data.clone());
            write.before_path = Some(overlay_state.path.clone());
            if next_path.is_none() {
                write.path = overlay_state.path.clone();
            }
        }
        if !saw_data_assignment {
            write.after_data = write.before_data.clone().unwrap_or_default();
        }
    }

    writes.extend(pending);

    Ok(())
}

async fn collect_delete_targets(
    backend: &dyn LixBackend,
    delete: &sqlparser::ast::Delete,
    params: &[Value],
    active_version_id: &str,
    overlay: &BTreeMap<(String, String), OverlayWriteState>,
    targets: &mut BTreeSet<(String, String)>,
) -> Result<BTreeSet<(String, String)>, LixError> {
    let Some(target) = file_write_target_from_delete(delete) else {
        return Ok(BTreeSet::new());
    };
    let mut statement_targets = BTreeSet::new();

    let mut query_sql = match target {
        FileWriteTarget::ActiveVersion => "SELECT id FROM lix_file".to_string(),
        FileWriteTarget::ExplicitVersion => {
            "SELECT id, lixcol_version_id FROM lix_file_by_version".to_string()
        }
    };
    if let Some(selection) = delete.selection.as_ref() {
        query_sql.push_str(" WHERE ");
        query_sql.push_str(&selection.to_string());
    }

    let bound = bind_sql_with_state(
        &query_sql,
        params,
        backend.dialect(),
        PlaceholderState::new(),
    )?;
    let rows = execute_prefetch_query(backend, &bound.sql, &bound.params)
        .await
        .map_err(|error| LixError {
            message: format!(
                "pending_file_writes delete prefetch failed for '{}': {}",
                bound.sql, error.message
            ),
        })?
        .rows;

    for row in rows {
        let Some(file_id) = row.first().and_then(value_as_text) else {
            continue;
        };
        let version_id = match target {
            FileWriteTarget::ActiveVersion => active_version_id.to_string(),
            FileWriteTarget::ExplicitVersion => row
                .get(1)
                .and_then(value_as_text)
                .unwrap_or_else(|| active_version_id.to_string()),
        };
        let key = (file_id, version_id);
        statement_targets.insert(key.clone());
        targets.insert(key);
    }

    let overlay_rows = execute_delete_overlay_prefetch_query(
        backend,
        delete,
        params,
        active_version_id,
        target,
        overlay,
    )
    .await?;
    for row in overlay_rows {
        let Some(file_id) = row.first().and_then(value_as_text) else {
            continue;
        };
        let version_id = row
            .get(1)
            .and_then(value_as_text)
            .unwrap_or_else(|| active_version_id.to_string());
        let key = (file_id, version_id);
        statement_targets.insert(key.clone());
        targets.insert(key);
    }

    Ok(statement_targets)
}

#[derive(Debug, Clone)]
struct OverlayWriteState {
    path: String,
    data: Vec<u8>,
}

fn apply_statement_writes_to_overlay(
    statement_writes: &[PendingFileWrite],
    overlay: &mut BTreeMap<(String, String), OverlayWriteState>,
) {
    for write in statement_writes {
        overlay.insert(
            (write.file_id.clone(), write.version_id.clone()),
            OverlayWriteState {
                path: write.path.clone(),
                data: write.after_data.clone(),
            },
        );
    }
}

async fn load_before_data_from_cache_batch(
    backend: &dyn LixBackend,
    keys: &[(String, String)],
) -> Result<BTreeMap<(String, String), Vec<u8>>, LixError> {
    if keys.is_empty() {
        return Ok(BTreeMap::new());
    }

    const PAIRS_PER_CHUNK: usize = 200;
    let mut out = BTreeMap::new();

    for chunk in keys.chunks(PAIRS_PER_CHUNK) {
        let mut params = Vec::with_capacity(chunk.len() * 2);
        let mut predicates = Vec::with_capacity(chunk.len());
        for (index, (file_id, version_id)) in chunk.iter().enumerate() {
            let file_param = index * 2 + 1;
            let version_param = file_param + 1;
            predicates.push(format!(
                "(file_id = ${file_param} AND version_id = ${version_param})"
            ));
            params.push(Value::Text(file_id.clone()));
            params.push(Value::Text(version_id.clone()));
        }

        let sql = format!(
            "SELECT file_id, version_id, data \
             FROM lix_internal_file_data_cache \
             WHERE {}",
            predicates.join(" OR ")
        );
        let rows = backend.execute(&sql, &params).await?.rows;
        for row in rows {
            let Some(file_id) = row.first().and_then(value_as_text) else {
                continue;
            };
            let Some(version_id) = row.get(1).and_then(value_as_text) else {
                continue;
            };
            let Some(data) = row.get(2).and_then(value_as_blob_or_text_bytes) else {
                continue;
            };
            out.insert((file_id, version_id), data);
        }
    }

    Ok(out)
}

fn file_write_target_from_insert(table: &TableObject) -> Option<FileWriteTarget> {
    let TableObject::TableName(name) = table else {
        return None;
    };

    let table_name = object_name_terminal(name)?;
    file_write_target_from_name(&table_name)
}

fn file_write_target_from_update(update: &Update) -> Option<FileWriteTarget> {
    if !update.table.joins.is_empty() {
        return None;
    }
    let TableFactor::Table { name, .. } = &update.table.relation else {
        return None;
    };

    let table_name = object_name_terminal(name)?;
    file_write_target_from_name(&table_name)
}

fn file_write_target_from_delete(delete: &sqlparser::ast::Delete) -> Option<FileWriteTarget> {
    let tables = match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    if tables.len() != 1 {
        return None;
    }

    let table = &tables[0];
    if !table.joins.is_empty() {
        return None;
    }
    let TableFactor::Table { name, .. } = &table.relation else {
        return None;
    };

    let table_name = object_name_terminal(name)?;
    file_write_target_from_name(&table_name)
}

fn file_write_target_from_name(table_name: &str) -> Option<FileWriteTarget> {
    if table_name.eq_ignore_ascii_case("lix_file") {
        Some(FileWriteTarget::ActiveVersion)
    } else if table_name.eq_ignore_ascii_case("lix_file_by_version") {
        Some(FileWriteTarget::ExplicitVersion)
    } else {
        None
    }
}

fn assignment_target_name(assignment: &Assignment) -> Option<String> {
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

fn resolved_cell_text(cell: Option<&crate::sql::ResolvedCell>) -> Option<String> {
    match cell.and_then(|entry| entry.value.as_ref()) {
        Some(Value::Text(value)) => Some(value.clone()),
        _ => None,
    }
}

fn resolved_cell_blob_or_text_bytes(cell: Option<&crate::sql::ResolvedCell>) -> Option<Vec<u8>> {
    cell.and_then(|entry| entry.value.as_ref())
        .and_then(value_as_blob_or_text_bytes)
}

fn value_as_text(value: &Value) -> Option<String> {
    match value {
        Value::Text(text) => Some(text.clone()),
        _ => None,
    }
}

fn value_as_blob_or_text_bytes(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::Blob(bytes) => Some(bytes.clone()),
        Value::Text(text) => Some(text.as_bytes().to_vec()),
        _ => None,
    }
}

async fn execute_prefetch_query(
    backend: &dyn LixBackend,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, LixError> {
    let output = preprocess_sql(backend, &CelEvaluator::new(), sql, params).await?;
    backend.execute(&output.sql, &output.params).await
}

async fn execute_delete_overlay_prefetch_query(
    backend: &dyn LixBackend,
    delete: &sqlparser::ast::Delete,
    params: &[Value],
    active_version_id: &str,
    target: FileWriteTarget,
    overlay: &BTreeMap<(String, String), OverlayWriteState>,
) -> Result<Vec<Vec<Value>>, LixError> {
    let overlay_rows = overlay_rows_for_target(overlay, active_version_id, target);
    if overlay_rows.is_empty() {
        return Ok(Vec::new());
    }

    let alias = match target {
        FileWriteTarget::ActiveVersion => "lix_file",
        FileWriteTarget::ExplicitVersion => "lix_file_by_version",
    };
    let mut query_sql = format!(
        "WITH {alias}(id, path, lixcol_version_id, version_id) AS (VALUES {}) \
         SELECT id, lixcol_version_id FROM {alias}",
        overlay_rows
            .iter()
            .map(|(file_id, path, version_id)| {
                format!(
                    "('{}', '{}', '{}', '{}')",
                    escape_sql_string(file_id),
                    escape_sql_string(path),
                    escape_sql_string(version_id),
                    escape_sql_string(version_id)
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    );
    if let Some(selection) = delete.selection.as_ref() {
        query_sql.push_str(" WHERE ");
        query_sql.push_str(&selection.to_string());
    }

    let bound = bind_sql_with_state(
        &query_sql,
        params,
        backend.dialect(),
        PlaceholderState::new(),
    )?;
    let rows = backend.execute(&bound.sql, &bound.params).await?.rows;
    Ok(rows)
}

fn overlay_rows_for_target(
    overlay: &BTreeMap<(String, String), OverlayWriteState>,
    active_version_id: &str,
    target: FileWriteTarget,
) -> Vec<(String, String, String)> {
    overlay
        .iter()
        .filter_map(|((file_id, version_id), state)| {
            if matches!(target, FileWriteTarget::ActiveVersion) && version_id != active_version_id {
                return None;
            }
            Some((file_id.clone(), state.path.clone(), version_id.clone()))
        })
        .collect()
}
