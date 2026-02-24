use crate::cel::CelEvaluator;
#[cfg(test)]
use crate::engine::sql2::ast::utils::parse_sql_statements;
use crate::engine::sql2::legacy_bridge::{
    bind_sql_with_sql_bridge_state as bind_sql_with_state,
    escape_sql_string_with_sql_bridge as escape_sql_string,
    preprocess_sql_with_sql_bridge as preprocess_sql,
    resolve_expr_cell_with_sql_bridge as resolve_expr_cell_with_state,
    resolve_values_rows_with_sql_bridge as resolve_values_rows,
    SqlBridgePlaceholderState as PlaceholderState, SqlBridgeResolvedCell as ResolvedCell,
};
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
    parse_active_version_snapshot,
};
use crate::{LixBackend, LixError, QueryResult, Value};
use sqlparser::ast::{
    Assignment, Expr, FromTable, ObjectName, ObjectNamePart, SetExpr, Statement, TableFactor,
    TableObject, Update,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

#[derive(Debug, Clone)]
pub(crate) struct PendingFileWrite {
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) before_path: Option<String>,
    pub(crate) after_path: Option<String>,
    pub(crate) data_is_authoritative: bool,
    pub(crate) before_data: Option<Vec<u8>>,
    pub(crate) after_data: Vec<u8>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PendingFileWriteCollection {
    pub(crate) writes: Vec<PendingFileWrite>,
    pub(crate) writes_by_statement: Vec<Vec<PendingFileWrite>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileWriteTarget {
    ActiveVersion,
    ExplicitVersion,
}

#[derive(Debug, Clone)]
struct ExactFileUpdateTarget {
    file_id: String,
    explicit_version_id: Option<String>,
}

#[cfg(test)]
pub(crate) async fn collect_pending_file_writes(
    backend: &dyn LixBackend,
    sql: &str,
    params: &[Value],
    active_version_id: &str,
) -> Result<PendingFileWriteCollection, LixError> {
    let statements = parse_sql_statements(sql)?;
    collect_pending_file_writes_from_statements(backend, &statements, params, active_version_id)
        .await
}

pub(crate) async fn collect_pending_file_writes_from_statements(
    backend: &dyn LixBackend,
    statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
) -> Result<PendingFileWriteCollection, LixError> {
    let mut writes = Vec::new();
    let mut writes_by_statement = Vec::with_capacity(statements.len());
    let mut overlay = BTreeMap::<(String, String), OverlayWriteState>::new();
    let mut effective_active_version_id = active_version_id.to_string();

    for statement in statements {
        let start_len = writes.len();
        let mut delete_statement_writes: Option<Vec<PendingFileWrite>> = None;
        match &statement {
            Statement::Insert(insert) => {
                collect_insert_writes(insert, params, &effective_active_version_id, &mut writes)?;
            }
            Statement::Update(update) => {
                collect_update_writes(
                    backend,
                    update,
                    params,
                    &effective_active_version_id,
                    &overlay,
                    &mut writes,
                )
                .await?;
            }
            Statement::Delete(delete) => {
                let statement_writes = collect_delete_writes(
                    backend,
                    delete,
                    params,
                    &effective_active_version_id,
                    &overlay,
                )
                .await?;
                for write in &statement_writes {
                    overlay.remove(&(write.file_id.clone(), write.version_id.clone()));
                }
                delete_statement_writes = Some(statement_writes);
            }
            _ => {}
        }
        if let Some(statement_writes) = delete_statement_writes {
            writes_by_statement.push(statement_writes);
        } else {
            writes_by_statement.push(writes[start_len..].to_vec());
            apply_statement_writes_to_overlay(&writes[start_len..], &mut overlay);
        }
        if let Some(next_active_version_id) = next_active_version_id_from_statement(
            backend,
            &statement,
            params,
            &effective_active_version_id,
        )
        .await?
        {
            effective_active_version_id = next_active_version_id;
        }
    }

    Ok(PendingFileWriteCollection {
        writes,
        writes_by_statement,
    })
}

pub(crate) async fn collect_pending_file_delete_targets_from_statements(
    backend: &dyn LixBackend,
    statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
) -> Result<BTreeSet<(String, String)>, LixError> {
    let mut targets = BTreeSet::new();
    let mut overlay = BTreeMap::<(String, String), OverlayWriteState>::new();
    let mut writes = Vec::new();
    let mut effective_active_version_id = active_version_id.to_string();

    for statement in statements {
        let start_len = writes.len();
        match &statement {
            Statement::Insert(insert) => {
                collect_insert_writes(insert, params, &effective_active_version_id, &mut writes)?;
                apply_statement_writes_to_overlay(&writes[start_len..], &mut overlay);
            }
            Statement::Update(update) => {
                collect_update_writes(
                    backend,
                    update,
                    params,
                    &effective_active_version_id,
                    &overlay,
                    &mut writes,
                )
                .await?;
                apply_statement_writes_to_overlay(&writes[start_len..], &mut overlay);
            }
            Statement::Delete(delete) => {
                let statement_targets = collect_delete_targets(
                    backend,
                    delete,
                    params,
                    &effective_active_version_id,
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
        if let Some(next_active_version_id) = next_active_version_id_from_statement(
            backend,
            &statement,
            params,
            &effective_active_version_id,
        )
        .await?
        {
            effective_active_version_id = next_active_version_id;
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
            after_path: Some(path),
            data_is_authoritative: true,
            before_data: None,
            after_data,
        });
    }

    Ok(())
}

async fn collect_delete_writes(
    backend: &dyn LixBackend,
    delete: &sqlparser::ast::Delete,
    params: &[Value],
    active_version_id: &str,
    overlay: &BTreeMap<(String, String), OverlayWriteState>,
) -> Result<Vec<PendingFileWrite>, LixError> {
    let Some(target) = file_write_target_from_delete(delete) else {
        return Ok(Vec::new());
    };

    let mut query_sql = match target {
        FileWriteTarget::ActiveVersion => format!(
            "SELECT id, path, data, lixcol_version_id, \
                    'pending.collect_delete_writes' AS __lix_trace \
             FROM lix_file_by_version \
             WHERE lixcol_version_id = '{}'",
            escape_sql_string(active_version_id)
        ),
        FileWriteTarget::ExplicitVersion => "SELECT id, path, data, lixcol_version_id, \
                    'pending.collect_delete_writes' AS __lix_trace \
             FROM lix_file_by_version"
            .to_string(),
    };
    if let Some(selection) = delete.selection.as_ref() {
        query_sql.push_str(if matches!(target, FileWriteTarget::ActiveVersion) {
            " AND "
        } else {
            " WHERE "
        });
        query_sql.push_str(&selection.to_string());
    }

    let bound = bind_sql_with_state(
        &query_sql,
        params,
        backend.dialect(),
        PlaceholderState::new(),
    )?;
    let rows = execute_prefetch_query(
        backend,
        "pending.collect_delete_writes",
        &bound.sql,
        &bound.params,
    )
    .await
    .map_err(|error| LixError {
        message: format!(
            "pending_file_writes delete prefetch failed for '{}': {}",
            bound.sql, error.message
        ),
    })?
    .rows;

    let mut pending = Vec::with_capacity(rows.len());
    let mut seen = BTreeSet::<(String, String)>::new();
    for row in rows {
        let Some(file_id) = row.first().and_then(value_as_text) else {
            continue;
        };
        let Some(before_path_from_row) = row.get(1).and_then(value_as_text) else {
            continue;
        };
        let version_id = match target {
            FileWriteTarget::ActiveVersion => row
                .get(3)
                .and_then(value_as_text)
                .unwrap_or_else(|| active_version_id.to_string()),
            FileWriteTarget::ExplicitVersion => row
                .get(3)
                .and_then(value_as_text)
                .unwrap_or_else(|| active_version_id.to_string()),
        };
        let key = (file_id.clone(), version_id.clone());
        if !seen.insert(key.clone()) {
            continue;
        }
        let before_data_from_row = row.get(2).and_then(value_as_blob_or_text_bytes);
        let (before_path, before_data) = if let Some(overlay_state) = overlay.get(&key) {
            (overlay_state.path.clone(), Some(overlay_state.data.clone()))
        } else {
            (before_path_from_row, before_data_from_row)
        };

        pending.push(PendingFileWrite {
            file_id,
            version_id,
            before_path: Some(before_path),
            after_path: None,
            data_is_authoritative: true,
            before_data,
            after_data: Vec::new(),
        });
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
        let key = (file_id.clone(), version_id.clone());
        if !seen.insert(key.clone()) {
            continue;
        }
        let Some(overlay_state) = overlay.get(&key) else {
            continue;
        };
        pending.push(PendingFileWrite {
            file_id,
            version_id,
            before_path: Some(overlay_state.path.clone()),
            after_path: None,
            data_is_authoritative: true,
            before_data: Some(overlay_state.data.clone()),
            after_data: Vec::new(),
        });
    }

    Ok(pending)
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
    let mut assigned_after_data_by_id: Option<BTreeMap<String, Vec<u8>>> = None;
    let mut saw_data_assignment = false;
    let mut next_path: Option<String> = None;
    let mut next_path_by_id: Option<BTreeMap<String, String>> = None;
    let mut next_file_id: Option<String> = None;

    for assignment in &update.assignments {
        let Some(column) = assignment_target_name(assignment) else {
            continue;
        };
        if column.eq_ignore_ascii_case("data") {
            saw_data_assignment = true;
            if let Some(case_values) = resolve_case_assignment_text_or_blob_by_id(
                &assignment.value,
                "data",
                params,
                &mut placeholder_state,
            )? {
                assigned_after_data_by_id = Some(case_values);
                assigned_after_data = None;
                continue;
            }
            let resolved =
                resolve_expr_cell_with_state(&assignment.value, params, &mut placeholder_state)?;
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
            if let Some(case_values) = resolve_case_assignment_text_by_id(
                &assignment.value,
                "path",
                params,
                &mut placeholder_state,
            )? {
                next_path_by_id = Some(case_values);
                next_path = None;
                continue;
            }
            let resolved =
                resolve_expr_cell_with_state(&assignment.value, params, &mut placeholder_state)?;
            next_path = resolved_cell_text(Some(&resolved));
        } else if column.eq_ignore_ascii_case("id") {
            let resolved =
                resolve_expr_cell_with_state(&assignment.value, params, &mut placeholder_state)?;
            next_file_id = resolved_cell_text(Some(&resolved));
        }
    }

    if !saw_data_assignment && next_path.is_none() && next_path_by_id.is_none() {
        return Ok(());
    }

    if assigned_after_data_by_id.is_none() && next_path_by_id.is_none() && next_file_id.is_none() {
        let mut fast_path_state = placeholder_state;
        if let Some(exact_target) = extract_exact_file_update_target(
            update.selection.as_ref(),
            params,
            &mut fast_path_state,
        )? {
            let version_id = match target {
                FileWriteTarget::ActiveVersion => active_version_id.to_string(),
                FileWriteTarget::ExplicitVersion => {
                    exact_target.explicit_version_id.clone().unwrap_or_default()
                }
            };
            if !version_id.is_empty() {
                let key = (exact_target.file_id.clone(), version_id.clone());
                if let Some(overlay_state) = overlay.get(&key) {
                    let before_path = overlay_state.path.clone();
                    let before_data = Some(overlay_state.data.clone());
                    let path = next_path.clone().unwrap_or_else(|| before_path.clone());
                    let mut write = PendingFileWrite {
                        file_id: exact_target.file_id,
                        version_id,
                        before_path: Some(before_path),
                        after_path: Some(path),
                        data_is_authoritative: saw_data_assignment,
                        before_data,
                        after_data: assigned_after_data.clone().unwrap_or_default(),
                    };
                    if !write.data_is_authoritative {
                        write.after_data = write.before_data.clone().unwrap_or_default();
                    }
                    writes.push(write);
                    return Ok(());
                }

                let cache_keys = [key.clone()];
                let before_paths = load_before_path_from_cache_batch(backend, &cache_keys).await?;
                if let Some(before_path) = before_paths.get(&key) {
                    let before_data = load_before_data_from_cache_batch(backend, &cache_keys)
                        .await?
                        .get(&key)
                        .cloned();
                    let path = next_path.clone().unwrap_or_else(|| before_path.clone());
                    let mut write = PendingFileWrite {
                        file_id: exact_target.file_id,
                        version_id,
                        before_path: Some(before_path.clone()),
                        after_path: Some(path),
                        data_is_authoritative: saw_data_assignment,
                        before_data,
                        after_data: assigned_after_data.clone().unwrap_or_default(),
                    };
                    if !write.data_is_authoritative && write.before_data.is_some() {
                        write.after_data = write.before_data.clone().unwrap_or_default();
                    }
                    // If this was a non-data update and data cache is missing, we cannot trust
                    // an empty blob fallback. Continue with the full prefetch path so before_data
                    // is resolved from state and after_data is preserved.
                    if write.data_is_authoritative || write.before_data.is_some() {
                        writes.push(write);
                        return Ok(());
                    }
                }
            }
        }
    }

    let mut query_sql = match target {
        FileWriteTarget::ActiveVersion => format!(
            "SELECT id, path, data, lixcol_version_id, \
                    'pending.collect_update_writes' AS __lix_trace \
             FROM lix_file_by_version \
             WHERE lixcol_version_id = '{}'",
            escape_sql_string(active_version_id)
        ),
        FileWriteTarget::ExplicitVersion => "SELECT id, path, data, lixcol_version_id, \
                    'pending.collect_update_writes' AS __lix_trace \
             FROM lix_file_by_version"
            .to_string(),
    };
    if let Some(selection) = update.selection.as_ref() {
        query_sql.push_str(if matches!(target, FileWriteTarget::ActiveVersion) {
            " AND "
        } else {
            " WHERE "
        });
        query_sql.push_str(&selection.to_string());
    }

    let bound = bind_sql_with_state(&query_sql, params, backend.dialect(), placeholder_state)?;
    let rows = execute_prefetch_query(
        backend,
        "pending.collect_update_writes",
        &bound.sql,
        &bound.params,
    )
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
        let file_id = next_file_id
            .clone()
            .unwrap_or_else(|| before_file_id.clone());
        let path = if let Some(path_by_id) = &next_path_by_id {
            path_by_id
                .get(&before_file_id)
                .cloned()
                .or_else(|| next_path.clone())
                .unwrap_or(before_path)
        } else {
            next_path.clone().unwrap_or(before_path)
        };
        let version_id = match target {
            FileWriteTarget::ActiveVersion => row
                .get(3)
                .and_then(value_as_text)
                .unwrap_or_else(|| active_version_id.to_string()),
            FileWriteTarget::ExplicitVersion => row
                .get(3)
                .and_then(value_as_text)
                .unwrap_or_else(|| active_version_id.to_string()),
        };
        let before_data = row.get(2).and_then(value_as_blob_or_text_bytes);
        if before_data.is_none() || before_data.as_ref().is_some_and(|bytes| bytes.is_empty()) {
            cache_lookup_keys.insert((file_id.clone(), version_id.clone()));
        }

        let (data_is_authoritative, after_data) = if saw_data_assignment {
            if let Some(data_by_id) = &assigned_after_data_by_id {
                if let Some(after_data) = data_by_id.get(&before_file_id) {
                    (true, after_data.clone())
                } else {
                    (false, Vec::new())
                }
            } else {
                (true, assigned_after_data.clone().unwrap_or_default())
            }
        } else {
            (false, Vec::new())
        };

        pending.push(PendingFileWrite {
            file_id,
            version_id,
            before_path: Some(before_path_for_write),
            after_path: Some(path),
            data_is_authoritative,
            before_data,
            after_data,
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
            if next_path.is_none() && next_path_by_id.is_none() {
                write.after_path = Some(overlay_state.path.clone());
            }
        }
        if !write.data_is_authoritative {
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

    let mut exact_placeholder_state = PlaceholderState::new();
    if let Some(exact_targets) = extract_exact_file_delete_targets(
        delete.selection.as_ref(),
        params,
        &mut exact_placeholder_state,
        target,
        active_version_id,
    )? {
        for key in exact_targets {
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

        return Ok(statement_targets);
    }

    let selection_uses_id_projection_only = delete
        .selection
        .as_ref()
        .is_none_or(delete_selection_supports_id_projection);
    let mut query_sql = match (target, selection_uses_id_projection_only) {
        (FileWriteTarget::ActiveVersion, true) => format!(
            "SELECT id, lixcol_version_id, \
                    'pending.collect_delete_targets.id_projection' AS __lix_trace \
             FROM (\
                 SELECT \
                     lix_json_text(snapshot_content, 'id') AS id, \
                     version_id AS lixcol_version_id \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_file_descriptor' \
                   AND snapshot_content IS NOT NULL\
             ) AS file_descriptor_ids \
             WHERE lixcol_version_id = '{}'",
            escape_sql_string(active_version_id)
        ),
        (FileWriteTarget::ExplicitVersion, true) => "SELECT id, lixcol_version_id, \
                    'pending.collect_delete_targets.id_projection' AS __lix_trace \
             FROM (\
                 SELECT \
                     lix_json_text(snapshot_content, 'id') AS id, \
                     version_id AS lixcol_version_id \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_file_descriptor' \
                   AND snapshot_content IS NOT NULL\
             ) AS file_descriptor_ids"
            .to_string(),
        (FileWriteTarget::ActiveVersion, false) => format!(
            "SELECT id, lixcol_version_id, \
                    'pending.collect_delete_targets' AS __lix_trace \
             FROM lix_file_by_version \
             WHERE lixcol_version_id = '{}'",
            escape_sql_string(active_version_id)
        ),
        (FileWriteTarget::ExplicitVersion, false) => "SELECT id, lixcol_version_id, \
                    'pending.collect_delete_targets' AS __lix_trace \
             FROM lix_file_by_version"
            .to_string(),
    };
    if let Some(selection) = delete.selection.as_ref() {
        query_sql.push_str(if matches!(target, FileWriteTarget::ActiveVersion) {
            " AND "
        } else {
            " WHERE "
        });
        query_sql.push_str(&selection.to_string());
    }

    let bound = bind_sql_with_state(
        &query_sql,
        params,
        backend.dialect(),
        PlaceholderState::new(),
    )?;
    let rows = execute_prefetch_query(
        backend,
        "pending.collect_delete_targets",
        &bound.sql,
        &bound.params,
    )
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
            FileWriteTarget::ActiveVersion => row
                .get(1)
                .and_then(value_as_text)
                .unwrap_or_else(|| active_version_id.to_string()),
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

const ACTIVE_VERSION_VIEW: &str = "lix_active_version";
const INTERNAL_STATE_VTABLE: &str = "lix_internal_state_vtable";
const INTERNAL_STATE_UNTRACKED: &str = "lix_internal_state_untracked";

fn apply_statement_writes_to_overlay(
    statement_writes: &[PendingFileWrite],
    overlay: &mut BTreeMap<(String, String), OverlayWriteState>,
) {
    for write in statement_writes {
        let Some(path) = write.after_path.as_ref() else {
            continue;
        };
        overlay.insert(
            (write.file_id.clone(), write.version_id.clone()),
            OverlayWriteState {
                path: path.clone(),
                data: write.after_data.clone(),
            },
        );
    }
}

async fn next_active_version_id_from_statement(
    backend: &dyn LixBackend,
    statement: &Statement,
    params: &[Value],
    current_active_version_id: &str,
) -> Result<Option<String>, LixError> {
    match statement {
        Statement::Update(update) => {
            let Some(table_name) = update_target_table_name(update) else {
                return Ok(None);
            };
            if table_name.eq_ignore_ascii_case(ACTIVE_VERSION_VIEW) {
                return active_version_id_from_lix_active_version_update(backend, update, params)
                    .await;
            }
            if table_name.eq_ignore_ascii_case(INTERNAL_STATE_VTABLE)
                || table_name.eq_ignore_ascii_case(INTERNAL_STATE_UNTRACKED)
            {
                return active_version_id_from_internal_state_update(backend, update, params).await;
            }
            Ok(None)
        }
        Statement::Insert(insert) => {
            active_version_id_from_internal_state_insert(insert, params, current_active_version_id)
        }
        _ => Ok(None),
    }
}

async fn active_version_id_from_lix_active_version_update(
    backend: &dyn LixBackend,
    update: &Update,
    params: &[Value],
) -> Result<Option<String>, LixError> {
    let mut placeholder_state = PlaceholderState::new();
    let mut next_active_version_id: Option<String> = None;
    for assignment in &update.assignments {
        let Some(column) = assignment_target_name(assignment) else {
            continue;
        };
        let resolved =
            resolve_expr_cell_with_state(&assignment.value, params, &mut placeholder_state)?;
        if column.eq_ignore_ascii_case("version_id") {
            next_active_version_id = resolved_cell_text(Some(&resolved));
        }
    }
    let Some(next_active_version_id) = next_active_version_id else {
        return Ok(None);
    };

    let mut query_sql = format!("SELECT 1 FROM {ACTIVE_VERSION_VIEW}");
    if let Some(selection) = update.selection.as_ref() {
        query_sql.push_str(" WHERE ");
        query_sql.push_str(&selection.to_string());
    }
    let bound = bind_sql_with_state(&query_sql, params, backend.dialect(), placeholder_state)?;
    let rows = execute_prefetch_query(
        backend,
        "pending.active_version_update",
        &bound.sql,
        &bound.params,
    )
    .await
    .map_err(|error| LixError {
        message: format!(
            "active version update prefetch failed for '{}': {}",
            bound.sql, error.message
        ),
    })?
    .rows;
    if rows.is_empty() {
        return Ok(None);
    }

    Ok(Some(next_active_version_id))
}

async fn active_version_id_from_internal_state_update(
    backend: &dyn LixBackend,
    update: &Update,
    params: &[Value],
) -> Result<Option<String>, LixError> {
    let Some(table_name) = update_target_table_name(update) else {
        return Ok(None);
    };
    let mut placeholder_state = PlaceholderState::new();
    let mut next_active_version_id: Option<String> = None;
    for assignment in &update.assignments {
        let Some(column) = assignment_target_name(assignment) else {
            continue;
        };
        let resolved =
            resolve_expr_cell_with_state(&assignment.value, params, &mut placeholder_state)?;
        if column.eq_ignore_ascii_case("snapshot_content") {
            next_active_version_id = resolved
                .value
                .as_ref()
                .and_then(active_version_id_from_snapshot_value);
        }
    }
    let Some(next_active_version_id) = next_active_version_id else {
        return Ok(None);
    };

    let active_version_predicate = if table_name.eq_ignore_ascii_case(INTERNAL_STATE_VTABLE) {
        format!(
            "schema_key = '{}' AND file_id = '{}' AND version_id = '{}' AND untracked = 1",
            escape_sql_string(active_version_schema_key()),
            escape_sql_string(active_version_file_id()),
            escape_sql_string(active_version_storage_version_id()),
        )
    } else if table_name.eq_ignore_ascii_case(INTERNAL_STATE_UNTRACKED) {
        format!(
            "schema_key = '{}' AND file_id = '{}' AND version_id = '{}'",
            escape_sql_string(active_version_schema_key()),
            escape_sql_string(active_version_file_id()),
            escape_sql_string(active_version_storage_version_id()),
        )
    } else {
        return Ok(None);
    };

    let mut query_sql = format!("SELECT 1 FROM {table_name} WHERE {active_version_predicate}");
    if let Some(selection) = update.selection.as_ref() {
        query_sql.push_str(" AND (");
        query_sql.push_str(&selection.to_string());
        query_sql.push(')');
    }
    let bound = bind_sql_with_state(&query_sql, params, backend.dialect(), placeholder_state)?;
    let rows = execute_prefetch_query(
        backend,
        "pending.active_version_internal_update",
        &bound.sql,
        &bound.params,
    )
    .await
    .map_err(|error| LixError {
        message: format!(
            "active version internal update prefetch failed for '{}': {}",
            bound.sql, error.message
        ),
    })?
    .rows;
    if rows.is_empty() {
        return Ok(None);
    }

    Ok(Some(next_active_version_id))
}

fn active_version_id_from_internal_state_insert(
    insert: &sqlparser::ast::Insert,
    params: &[Value],
    current_active_version_id: &str,
) -> Result<Option<String>, LixError> {
    let Some(table_name) = insert_target_table_name(insert) else {
        return Ok(None);
    };
    let is_vtable = table_name.eq_ignore_ascii_case(INTERNAL_STATE_VTABLE);
    let is_untracked = table_name.eq_ignore_ascii_case(INTERNAL_STATE_UNTRACKED);
    if !is_vtable && !is_untracked {
        return Ok(None);
    }

    let Some(source) = insert.source.as_ref() else {
        return Ok(None);
    };
    let SetExpr::Values(values) = source.body.as_ref() else {
        return Ok(None);
    };

    let schema_key_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("schema_key"));
    let file_id_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("file_id"));
    let version_id_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("version_id"));
    let snapshot_content_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("snapshot_content"));
    let untracked_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("untracked"));
    let (
        Some(schema_key_index),
        Some(file_id_index),
        Some(version_id_index),
        Some(snapshot_content_index),
    ) = (
        schema_key_index,
        file_id_index,
        version_id_index,
        snapshot_content_index,
    )
    else {
        return Ok(None);
    };

    let resolved_rows = resolve_values_rows(&values.rows, params)?;
    let mut next_active_version_id = None;
    for resolved_row in &resolved_rows {
        let schema_key = resolved_cell_text(resolved_row.get(schema_key_index))
            .unwrap_or_else(|| "".to_string());
        let file_id =
            resolved_cell_text(resolved_row.get(file_id_index)).unwrap_or_else(|| "".to_string());
        let version_id = resolved_cell_text(resolved_row.get(version_id_index))
            .unwrap_or_else(|| current_active_version_id.to_string());
        if schema_key != active_version_schema_key()
            || file_id != active_version_file_id()
            || version_id != active_version_storage_version_id()
        {
            continue;
        }

        if is_vtable {
            let Some(untracked_index) = untracked_index else {
                continue;
            };
            let is_untracked_row = resolved_row
                .get(untracked_index)
                .and_then(|cell| cell.value.as_ref())
                .is_some_and(value_is_truthy);
            if !is_untracked_row {
                continue;
            }
        }

        let parsed = resolved_row
            .get(snapshot_content_index)
            .and_then(|cell| cell.value.as_ref())
            .and_then(active_version_id_from_snapshot_value);
        if parsed.is_some() {
            next_active_version_id = parsed;
        }
    }

    Ok(next_active_version_id)
}

fn extract_exact_file_update_target(
    selection: Option<&Expr>,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Result<Option<ExactFileUpdateTarget>, LixError> {
    let Some(selection) = selection else {
        return Ok(None);
    };
    let mut file_id: Option<String> = None;
    let mut version_id: Option<String> = None;
    if !collect_exact_file_update_predicates(
        selection,
        params,
        placeholder_state,
        &mut file_id,
        &mut version_id,
    )? {
        return Ok(None);
    }
    let Some(file_id) = file_id else {
        return Ok(None);
    };
    Ok(Some(ExactFileUpdateTarget {
        file_id,
        explicit_version_id: version_id,
    }))
}

fn extract_exact_file_delete_targets(
    selection: Option<&Expr>,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
    target: FileWriteTarget,
    active_version_id: &str,
) -> Result<Option<BTreeSet<(String, String)>>, LixError> {
    let Some(selection) = selection else {
        return Ok(None);
    };
    let mut file_ids: Option<BTreeSet<String>> = None;
    let mut version_ids: Option<BTreeSet<String>> = None;
    if !collect_exact_file_delete_predicates(
        selection,
        params,
        placeholder_state,
        &mut file_ids,
        &mut version_ids,
    )? {
        return Ok(None);
    }
    let Some(file_ids) = file_ids else {
        return Ok(None);
    };

    let effective_versions = match target {
        FileWriteTarget::ActiveVersion => {
            if let Some(ref constrained_versions) = version_ids {
                if !constrained_versions.contains(active_version_id) {
                    return Ok(Some(BTreeSet::new()));
                }
            }
            let mut versions = BTreeSet::new();
            versions.insert(active_version_id.to_string());
            versions
        }
        FileWriteTarget::ExplicitVersion => {
            let Some(versions) = version_ids else {
                return Ok(None);
            };
            versions
        }
    };

    let mut targets = BTreeSet::new();
    for file_id in &file_ids {
        for version_id in &effective_versions {
            targets.insert((file_id.clone(), version_id.clone()));
        }
    }
    Ok(Some(targets))
}

fn collect_exact_file_delete_predicates(
    expr: &Expr,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
    file_ids: &mut Option<BTreeSet<String>>,
    version_ids: &mut Option<BTreeSet<String>>,
) -> Result<bool, LixError> {
    match expr {
        Expr::Nested(inner) => collect_exact_file_delete_predicates(
            inner,
            params,
            placeholder_state,
            file_ids,
            version_ids,
        ),
        Expr::BinaryOp { left, op, right } => {
            if op.to_string().eq_ignore_ascii_case("AND") {
                let left_ok = collect_exact_file_delete_predicates(
                    left,
                    params,
                    placeholder_state,
                    file_ids,
                    version_ids,
                )?;
                let right_ok = collect_exact_file_delete_predicates(
                    right,
                    params,
                    placeholder_state,
                    file_ids,
                    version_ids,
                )?;
                return Ok(left_ok && right_ok);
            }

            if op.to_string().eq_ignore_ascii_case("=") {
                if let Some(column) = expr_column_name(left) {
                    if let Some(value) =
                        expr_text_literal_or_placeholder(right, params, placeholder_state)?
                    {
                        return Ok(apply_exact_file_delete_predicate(
                            &column,
                            std::iter::once(value).collect(),
                            file_ids,
                            version_ids,
                        ));
                    }
                    return Ok(false);
                }
                if let Some(column) = expr_column_name(right) {
                    if let Some(value) =
                        expr_text_literal_or_placeholder(left, params, placeholder_state)?
                    {
                        return Ok(apply_exact_file_delete_predicate(
                            &column,
                            std::iter::once(value).collect(),
                            file_ids,
                            version_ids,
                        ));
                    }
                    return Ok(false);
                }
            }

            Ok(false)
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            let Some(column) = expr_column_name(expr) else {
                return Ok(false);
            };
            let mut values = BTreeSet::new();
            for candidate in list {
                let Some(value) =
                    expr_text_literal_or_placeholder(candidate, params, placeholder_state)?
                else {
                    return Ok(false);
                };
                values.insert(value);
            }
            if values.is_empty() {
                return Ok(false);
            }
            Ok(apply_exact_file_delete_predicate(
                &column,
                values,
                file_ids,
                version_ids,
            ))
        }
        _ => Ok(false),
    }
}

fn apply_exact_file_delete_predicate(
    column: &str,
    values: BTreeSet<String>,
    file_ids: &mut Option<BTreeSet<String>>,
    version_ids: &mut Option<BTreeSet<String>>,
) -> bool {
    if column.eq_ignore_ascii_case("id")
        || column.eq_ignore_ascii_case("lixcol_entity_id")
        || column.eq_ignore_ascii_case("lixcol_file_id")
    {
        merge_exact_delete_constraint_values(file_ids, values);
        return true;
    }
    if column.eq_ignore_ascii_case("lixcol_version_id") || column.eq_ignore_ascii_case("version_id")
    {
        merge_exact_delete_constraint_values(version_ids, values);
        return true;
    }
    false
}

fn merge_exact_delete_constraint_values(
    slot: &mut Option<BTreeSet<String>>,
    values: BTreeSet<String>,
) {
    if let Some(existing) = slot.as_mut() {
        existing.retain(|value| values.contains(value));
        return;
    }
    *slot = Some(values);
}

fn collect_exact_file_update_predicates(
    expr: &Expr,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
    file_id: &mut Option<String>,
    version_id: &mut Option<String>,
) -> Result<bool, LixError> {
    match expr {
        Expr::Nested(inner) => collect_exact_file_update_predicates(
            inner,
            params,
            placeholder_state,
            file_id,
            version_id,
        ),
        Expr::BinaryOp { left, op, right } => {
            if op.to_string().eq_ignore_ascii_case("AND") {
                let left_ok = collect_exact_file_update_predicates(
                    left,
                    params,
                    placeholder_state,
                    file_id,
                    version_id,
                )?;
                let right_ok = collect_exact_file_update_predicates(
                    right,
                    params,
                    placeholder_state,
                    file_id,
                    version_id,
                )?;
                return Ok(left_ok && right_ok);
            }
            if op.to_string().eq_ignore_ascii_case("=") {
                if let Some(column) = expr_column_name(left) {
                    if let Some(value) =
                        expr_text_literal_or_placeholder(right, params, placeholder_state)?
                    {
                        if apply_exact_file_update_predicate(&column, &value, file_id, version_id) {
                            return Ok(true);
                        }
                    } else {
                        return Ok(false);
                    }
                }
                if let Some(column) = expr_column_name(right) {
                    if let Some(value) =
                        expr_text_literal_or_placeholder(left, params, placeholder_state)?
                    {
                        if apply_exact_file_update_predicate(&column, &value, file_id, version_id) {
                            return Ok(true);
                        }
                    } else {
                        return Ok(false);
                    }
                }
            }
            Ok(false)
        }
        _ => Ok(false),
    }
}

fn apply_exact_file_update_predicate(
    column: &str,
    value: &str,
    file_id: &mut Option<String>,
    version_id: &mut Option<String>,
) -> bool {
    if column.eq_ignore_ascii_case("id")
        || column.eq_ignore_ascii_case("lixcol_entity_id")
        || column.eq_ignore_ascii_case("lixcol_file_id")
    {
        if file_id.as_ref().is_some_and(|existing| existing != value) {
            return false;
        }
        *file_id = Some(value.to_string());
        return true;
    }
    if column.eq_ignore_ascii_case("lixcol_version_id") || column.eq_ignore_ascii_case("version_id")
    {
        if version_id
            .as_ref()
            .is_some_and(|existing| existing != value)
        {
            return false;
        }
        *version_id = Some(value.to_string());
        return true;
    }
    false
}

fn active_version_id_from_snapshot_value(value: &Value) -> Option<String> {
    match value {
        Value::Text(text) => parse_active_version_snapshot(text).ok(),
        _ => None,
    }
}

fn value_is_truthy(value: &Value) -> bool {
    match value {
        Value::Integer(value) => *value != 0,
        Value::Text(value) => {
            let normalized = value.trim().to_ascii_lowercase();
            normalized == "1" || normalized == "true"
        }
        _ => false,
    }
}

fn update_target_table_name(update: &Update) -> Option<String> {
    if !update.table.joins.is_empty() {
        return None;
    }
    let TableFactor::Table { name, .. } = &update.table.relation else {
        return None;
    };
    object_name_terminal(name)
}

fn insert_target_table_name(insert: &sqlparser::ast::Insert) -> Option<String> {
    let TableObject::TableName(name) = &insert.table else {
        return None;
    };
    object_name_terminal(name)
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

async fn load_before_path_from_cache_batch(
    backend: &dyn LixBackend,
    keys: &[(String, String)],
) -> Result<BTreeMap<(String, String), String>, LixError> {
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
            "SELECT file_id, version_id, path \
             FROM lix_internal_file_path_cache \
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
            let Some(path) = row.get(2).and_then(value_as_text) else {
                continue;
            };
            out.insert((file_id, version_id), path);
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

fn delete_selection_supports_id_projection(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(ident) => delete_projection_column_allowed(&ident.value),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .is_some_and(|ident| delete_projection_column_allowed(&ident.value)),
        Expr::BinaryOp { left, right, .. } => {
            delete_selection_supports_id_projection(left)
                && delete_selection_supports_id_projection(right)
        }
        Expr::UnaryOp { expr, .. } => delete_selection_supports_id_projection(expr),
        Expr::Nested(inner) => delete_selection_supports_id_projection(inner),
        Expr::InList { expr, list, .. } => {
            delete_selection_supports_id_projection(expr)
                && list.iter().all(delete_selection_supports_id_projection)
        }
        Expr::InSubquery { .. } | Expr::Subquery(_) | Expr::Exists { .. } => false,
        Expr::Between {
            expr, low, high, ..
        } => {
            delete_selection_supports_id_projection(expr)
                && delete_selection_supports_id_projection(low)
                && delete_selection_supports_id_projection(high)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            delete_selection_supports_id_projection(expr)
                && delete_selection_supports_id_projection(pattern)
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            delete_selection_supports_id_projection(inner)
        }
        Expr::Cast { expr, .. } => delete_selection_supports_id_projection(expr),
        Expr::Function(function) => match &function.args {
            sqlparser::ast::FunctionArguments::List(list) => {
                list.args.iter().all(|arg| match arg {
                    sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(expr),
                    ) => delete_selection_supports_id_projection(expr),
                    sqlparser::ast::FunctionArg::Named { arg, .. }
                    | sqlparser::ast::FunctionArg::ExprNamed { arg, .. } => match arg {
                        sqlparser::ast::FunctionArgExpr::Expr(expr) => {
                            delete_selection_supports_id_projection(expr)
                        }
                        _ => true,
                    },
                    _ => true,
                })
            }
            _ => false,
        },
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand
                .as_ref()
                .is_none_or(|expr| delete_selection_supports_id_projection(expr))
                && conditions.iter().all(|when| {
                    delete_selection_supports_id_projection(&when.condition)
                        && delete_selection_supports_id_projection(&when.result)
                })
                && else_result
                    .as_ref()
                    .is_none_or(|expr| delete_selection_supports_id_projection(expr))
        }
        Expr::Tuple(items) => items.iter().all(delete_selection_supports_id_projection),
        Expr::Value(_) => true,
        _ => false,
    }
}

fn delete_projection_column_allowed(column: &str) -> bool {
    column.eq_ignore_ascii_case("id")
        || column.eq_ignore_ascii_case("lixcol_version_id")
        || column.eq_ignore_ascii_case("version_id")
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

fn resolve_case_assignment_text_or_blob_by_id(
    expr: &sqlparser::ast::Expr,
    else_column_name: &str,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Result<Option<BTreeMap<String, Vec<u8>>>, LixError> {
    let Some(case) = expr_as_id_case(expr, else_column_name) else {
        return Ok(None);
    };
    let mut values = BTreeMap::new();
    for when in case.conditions {
        let key_cell = resolve_expr_cell_with_state(&when.condition, params, placeholder_state)?;
        let Some(key) = resolved_cell_text(Some(&key_cell)) else {
            return Ok(None);
        };
        let value_cell = resolve_expr_cell_with_state(&when.result, params, placeholder_state)?;
        let Some(value) = resolved_cell_blob_or_text_bytes(Some(&value_cell)) else {
            return Ok(None);
        };
        values.insert(key, value);
    }
    Ok(Some(values))
}

fn resolve_case_assignment_text_by_id(
    expr: &sqlparser::ast::Expr,
    else_column_name: &str,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Result<Option<BTreeMap<String, String>>, LixError> {
    let Some(case) = expr_as_id_case(expr, else_column_name) else {
        return Ok(None);
    };
    let mut values = BTreeMap::new();
    for when in case.conditions {
        let key_cell = resolve_expr_cell_with_state(&when.condition, params, placeholder_state)?;
        let Some(key) = resolved_cell_text(Some(&key_cell)) else {
            return Ok(None);
        };
        let value_cell = resolve_expr_cell_with_state(&when.result, params, placeholder_state)?;
        let Some(value) = resolved_cell_text(Some(&value_cell)) else {
            return Ok(None);
        };
        values.insert(key, value);
    }
    Ok(Some(values))
}

fn expr_as_id_case<'a>(
    expr: &'a sqlparser::ast::Expr,
    else_column_name: &str,
) -> Option<ExprCase<'a>> {
    let sqlparser::ast::Expr::Case {
        operand,
        conditions,
        else_result,
        ..
    } = expr
    else {
        return None;
    };
    if !operand
        .as_deref()
        .is_some_and(|operand| expr_is_column_name(operand, "id"))
    {
        return None;
    }
    if let Some(else_expr) = else_result.as_deref() {
        if !expr_is_column_name(else_expr, else_column_name) {
            return None;
        }
    }

    Some(ExprCase { conditions })
}

fn expr_is_column_name(expr: &sqlparser::ast::Expr, column_name: &str) -> bool {
    match expr {
        sqlparser::ast::Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case(column_name),
        sqlparser::ast::Expr::CompoundIdentifier(parts) => parts
            .last()
            .is_some_and(|ident| ident.value.eq_ignore_ascii_case(column_name)),
        sqlparser::ast::Expr::Nested(inner) => expr_is_column_name(inner, column_name),
        _ => false,
    }
}

struct ExprCase<'a> {
    conditions: &'a [sqlparser::ast::CaseWhen],
}

fn object_name_terminal(name: &ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.clone())
}

fn expr_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|ident| ident.value.clone()),
        Expr::Nested(inner) => expr_column_name(inner),
        _ => None,
    }
}

fn expr_text_literal_or_placeholder(
    expr: &Expr,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Result<Option<String>, LixError> {
    if let Expr::Value(value) = expr {
        return match &value.value {
            sqlparser::ast::Value::SingleQuotedString(value)
            | sqlparser::ast::Value::DoubleQuotedString(value)
            | sqlparser::ast::Value::TripleSingleQuotedString(value)
            | sqlparser::ast::Value::TripleDoubleQuotedString(value)
            | sqlparser::ast::Value::EscapedStringLiteral(value)
            | sqlparser::ast::Value::UnicodeStringLiteral(value)
            | sqlparser::ast::Value::NationalStringLiteral(value)
            | sqlparser::ast::Value::HexStringLiteral(value)
            | sqlparser::ast::Value::SingleQuotedRawStringLiteral(value)
            | sqlparser::ast::Value::DoubleQuotedRawStringLiteral(value)
            | sqlparser::ast::Value::TripleSingleQuotedRawStringLiteral(value)
            | sqlparser::ast::Value::TripleDoubleQuotedRawStringLiteral(value)
            | sqlparser::ast::Value::SingleQuotedByteStringLiteral(value)
            | sqlparser::ast::Value::DoubleQuotedByteStringLiteral(value)
            | sqlparser::ast::Value::TripleSingleQuotedByteStringLiteral(value)
            | sqlparser::ast::Value::TripleDoubleQuotedByteStringLiteral(value) => {
                Ok(Some(value.clone()))
            }
            sqlparser::ast::Value::DollarQuotedString(value) => Ok(Some(value.value.clone())),
            sqlparser::ast::Value::Number(value, _) => Ok(Some(value.clone())),
            sqlparser::ast::Value::Boolean(value) => {
                Ok(Some(if *value { "1" } else { "0" }.to_string()))
            }
            sqlparser::ast::Value::Null => Ok(None),
            sqlparser::ast::Value::Placeholder(_) => {
                let resolved = resolve_expr_cell_with_state(expr, params, placeholder_state)?;
                Ok(resolved_cell_text(Some(&resolved)))
            }
        };
    }
    let resolved = resolve_expr_cell_with_state(expr, params, placeholder_state)?;
    Ok(resolved_cell_text(Some(&resolved)))
}

fn resolved_cell_text(cell: Option<&ResolvedCell>) -> Option<String> {
    match cell.and_then(|entry| entry.value.as_ref()) {
        Some(Value::Text(value)) => Some(value.clone()),
        _ => None,
    }
}

fn resolved_cell_blob_or_text_bytes(cell: Option<&ResolvedCell>) -> Option<Vec<u8>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{LixBackend, LixTransaction, SqlDialect};
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    struct FastPathFallbackBackend {
        fallback_query_seen: Arc<AtomicBool>,
    }

    struct UnusedTransaction;
    struct CasePathOverlayBackend;

    #[async_trait(?Send)]
    impl LixBackend for FastPathFallbackBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM lix_internal_file_path_cache") {
                return Ok(QueryResult {
                    rows: vec![vec![
                        Value::Text("file-1".to_string()),
                        Value::Text("v1".to_string()),
                        Value::Text("/src/a.md".to_string()),
                    ]],
                });
            }
            if sql.contains("FROM lix_internal_file_data_cache") {
                return Ok(QueryResult { rows: Vec::new() });
            }
            if sql.contains("pending.collect_update_writes") {
                self.fallback_query_seen.store(true, Ordering::SeqCst);
                return Ok(QueryResult {
                    rows: vec![vec![
                        Value::Text("file-1".to_string()),
                        Value::Text("/src/a.md".to_string()),
                        Value::Blob(b"seed-data".to_vec()),
                        Value::Text("v1".to_string()),
                    ]],
                });
            }
            Ok(QueryResult { rows: Vec::new() })
        }

        async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
            Ok(Box::new(UnusedTransaction))
        }
    }

    #[async_trait(?Send)]
    impl LixTransaction for UnusedTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(
            &mut self,
            _sql: &str,
            _params: &[Value],
        ) -> Result<QueryResult, LixError> {
            Ok(QueryResult { rows: Vec::new() })
        }

        async fn commit(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }

        async fn rollback(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }
    }

    #[async_trait(?Send)]
    impl LixBackend for CasePathOverlayBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("pending.collect_update_writes") {
                return Ok(QueryResult {
                    rows: vec![vec![
                        Value::Text("file-1".to_string()),
                        Value::Text("/seed.md".to_string()),
                        Value::Blob(b"seed".to_vec()),
                        Value::Text("v1".to_string()),
                    ]],
                });
            }
            Ok(QueryResult { rows: Vec::new() })
        }

        async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
            Ok(Box::new(UnusedTransaction))
        }
    }

    fn parse_delete(sql: &str) -> sqlparser::ast::Delete {
        let statements = parse_sql_statements(sql).expect("parse SQL");
        let statement = statements.into_iter().next().expect("statement");
        let Statement::Delete(delete) = statement else {
            panic!("expected delete statement");
        };
        delete
    }

    #[test]
    fn exact_delete_targets_active_version_id_in_list() {
        let delete = parse_delete("DELETE FROM lix_file WHERE id IN ('a', 'b')");
        let mut state = PlaceholderState::new();
        let targets = extract_exact_file_delete_targets(
            delete.selection.as_ref(),
            &[],
            &mut state,
            FileWriteTarget::ActiveVersion,
            "v1",
        )
        .expect("extract targets")
        .expect("exact target");
        assert_eq!(
            targets,
            BTreeSet::from([
                ("a".to_string(), "v1".to_string()),
                ("b".to_string(), "v1".to_string()),
            ])
        );
    }

    #[test]
    fn exact_delete_targets_explicit_version_with_id_and_version_in_lists() {
        let delete = parse_delete(
            "DELETE FROM lix_file_by_version \
             WHERE id IN ('a', 'b') AND lixcol_version_id IN ('v1', 'v2')",
        );
        let mut state = PlaceholderState::new();
        let targets = extract_exact_file_delete_targets(
            delete.selection.as_ref(),
            &[],
            &mut state,
            FileWriteTarget::ExplicitVersion,
            "ignored",
        )
        .expect("extract targets")
        .expect("exact target");
        assert_eq!(
            targets,
            BTreeSet::from([
                ("a".to_string(), "v1".to_string()),
                ("a".to_string(), "v2".to_string()),
                ("b".to_string(), "v1".to_string()),
                ("b".to_string(), "v2".to_string()),
            ])
        );
    }

    #[test]
    fn exact_delete_targets_reject_non_exact_predicates() {
        let delete = parse_delete("DELETE FROM lix_file WHERE id = 'a' AND path LIKE '/%a%'");
        let mut state = PlaceholderState::new();
        let targets = extract_exact_file_delete_targets(
            delete.selection.as_ref(),
            &[],
            &mut state,
            FileWriteTarget::ActiveVersion,
            "v1",
        )
        .expect("extract targets");
        assert!(targets.is_none());
    }

    #[tokio::test]
    async fn exact_update_fast_path_falls_back_when_data_cache_misses() {
        let fallback_query_seen = Arc::new(AtomicBool::new(false));
        let backend = FastPathFallbackBackend {
            fallback_query_seen: Arc::clone(&fallback_query_seen),
        };

        let writes = collect_pending_file_writes(
            &backend,
            "UPDATE lix_file SET path = '/src/b.md' WHERE id = 'file-1'",
            &[],
            "v1",
        )
        .await
        .expect("collect_pending_file_writes should succeed");

        assert!(
            fallback_query_seen.load(Ordering::SeqCst),
            "cache miss must fall back to full prefetch query instead of early return"
        );
        assert_eq!(writes.writes.len(), 1);
        let write = &writes.writes[0];
        assert_eq!(write.file_id, "file-1");
        assert_eq!(write.version_id, "v1");
        assert_eq!(write.before_path.as_deref(), Some("/src/a.md"));
        assert_eq!(write.after_path.as_deref(), Some("/src/b.md"));
        assert_eq!(write.before_data.as_deref(), Some(b"seed-data".as_slice()));
        assert_eq!(write.after_data, b"seed-data".to_vec());
    }

    #[tokio::test]
    async fn case_path_update_keeps_case_selected_path_with_overlay() {
        let writes = collect_pending_file_writes(
            &CasePathOverlayBackend,
            "INSERT INTO lix_file (id, path, data) VALUES ('file-1', '/seed.md', 'seed'); \
             UPDATE lix_file \
             SET path = CASE id WHEN 'file-1' THEN '/next.md' ELSE path END \
             WHERE id = 'file-1'",
            &[],
            "v1",
        )
        .await
        .expect("collect_pending_file_writes should succeed");

        assert_eq!(writes.writes_by_statement.len(), 2);
        assert_eq!(writes.writes_by_statement[0].len(), 1);
        assert_eq!(writes.writes_by_statement[1].len(), 1);

        let second = &writes.writes_by_statement[1][0];
        assert_eq!(second.file_id, "file-1");
        assert_eq!(second.version_id, "v1");
        assert_eq!(second.before_path.as_deref(), Some("/seed.md"));
        assert_eq!(second.after_path.as_deref(), Some("/next.md"));
        assert_eq!(second.before_data.as_deref(), Some(b"seed".as_slice()));
        assert_eq!(second.after_data, b"seed".to_vec());
    }
}

async fn execute_prefetch_query(
    backend: &dyn LixBackend,
    label: &str,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, LixError> {
    let trace = file_prefetch_trace_enabled();
    // Keep the rewrite future on the heap to avoid stack blow-ups in deep
    // query rewrite paths on tokio test threads with smaller default stacks.
    let output = Box::pin(preprocess_sql(backend, &CelEvaluator::new(), sql, params)).await?;
    let result = backend
        .execute(&output.sql, output.single_statement_params()?)
        .await?;
    if trace {
        eprintln!(
            "[trace][file-prefetch] module=pending_file_writes label={label} source_sql_chars={} rewritten_sql_chars={} rows={}",
            sql.len(),
            output.sql.len(),
            result.rows.len(),
        );
    }
    Ok(result)
}

fn file_prefetch_trace_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("LIX_TRACE_FILE_PREFETCH")
            .ok()
            .map(|value| {
                let normalized = value.trim().to_ascii_lowercase();
                normalized == "1" || normalized == "true" || normalized == "yes"
            })
            .unwrap_or(false)
    })
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
        "WITH {alias}(\
            id, directory_id, name, extension, path, data, metadata, hidden, \
            lixcol_entity_id, lixcol_schema_key, lixcol_file_id, lixcol_version_id, version_id, \
            lixcol_plugin_key, lixcol_schema_version, lixcol_inherited_from_version_id, \
            lixcol_change_id, lixcol_created_at, lixcol_updated_at, lixcol_commit_id, \
            lixcol_writer_key, lixcol_untracked, lixcol_metadata\
         ) AS (VALUES {}) \
         SELECT id, lixcol_version_id FROM {alias}",
        overlay_rows
            .iter()
            .map(|(file_id, path, version_id)| {
                format!(
                    "('{}', NULL, NULL, NULL, '{}', NULL, NULL, 0, \
                      '{}', 'lix_file_descriptor', '{}', '{}', '{}', \
                      'lix', '1', NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL)",
                    escape_sql_string(file_id),
                    escape_sql_string(path),
                    escape_sql_string(file_id),
                    escape_sql_string(file_id),
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
