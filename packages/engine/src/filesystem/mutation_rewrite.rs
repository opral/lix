use serde_json::json;
use sqlparser::ast::{
    Assignment, Delete, Expr, FromTable, Ident, Insert, ObjectName, ObjectNamePart, SetExpr,
    Statement, TableFactor, TableObject, TableWithJoins, Update, Value as AstValue, ValueWithSpan,
    Values,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, OnceLock};

use crate::engine::sql::ast::lowering::lower_statement;
use crate::engine::sql::ast::utils::{
    bind_sql_with_state, resolve_expr_cell_with_state, resolve_values_rows, PlaceholderState,
    ResolvedCell,
};
use crate::engine::sql::contracts::effects::DetectedFileDomainChange;
use crate::engine::sql::planning::rewrite_engine::{
    rewrite_read_query_with_backend_and_params_in_session, ReadRewriteSession,
};
use crate::engine::sql::storage::sql_text::escape_sql_string;
use crate::filesystem::path::{
    compose_directory_path, directory_ancestor_paths, directory_name_from_path,
    file_ancestor_directory_paths, normalize_directory_path, normalize_file_path,
    normalize_path_segment, parent_directory_path, parse_file_path, path_depth,
};
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
    parse_active_version_snapshot, version_descriptor_file_id, version_descriptor_schema_key,
    version_descriptor_storage_version_id,
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
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_VIEW: &str = "lix_directory_descriptor";
const DIRECTORY_DESCRIPTOR_BY_VERSION_VIEW: &str = "lix_directory_descriptor_by_version";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_VERSION: &str = "1";
const INTERNAL_DESCRIPTOR_FILE_ID: &str = "lix";
const INTERNAL_DESCRIPTOR_PLUGIN_KEY: &str = "lix";
static REWRITTEN_HELPER_SQL_CACHE: OnceLock<Mutex<BTreeMap<String, String>>> = OnceLock::new();

pub type ResolvedDirectoryIdMap = BTreeMap<(String, String), String>;

#[derive(Debug, Default)]
pub struct FilesystemInsertSideEffects {
    pub statements: Vec<Statement>,
    pub tracked_directory_changes: Vec<DetectedFileDomainChange>,
    pub resolved_directory_ids: ResolvedDirectoryIdMap,
    pub active_version_id: Option<String>,
}

#[derive(Debug, Default)]
pub struct FilesystemUpdateSideEffects {
    pub tracked_directory_changes: Vec<DetectedFileDomainChange>,
    pub untracked_directory_changes: Vec<DetectedFileDomainChange>,
}

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
    resolved_directory_ids: Option<&ResolvedDirectoryIdMap>,
    active_version_id_hint: Option<&str>,
) -> Result<Option<Insert>, LixError> {
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
        rewrite_file_insert_columns_with_backend(
            backend,
            &mut insert,
            params,
            target,
            resolved_directory_ids,
            active_version_id_hint,
        )
        .await?;
    } else {
        rewrite_directory_insert_columns_with_backend(
            backend,
            &mut insert,
            params,
            target,
            active_version_id_hint,
        )
        .await?;
    }

    insert.table = TableObject::TableName(table_name(target.rewrite_view_name));
    Ok(Some(insert))
}

pub async fn insert_side_effect_statements_with_backend(
    backend: &dyn LixBackend,
    insert: &Insert,
    params: &[EngineValue],
) -> Result<FilesystemInsertSideEffects, LixError> {
    let Some(target) = target_from_table_object(&insert.table) else {
        return Ok(FilesystemInsertSideEffects::default());
    };
    if target.read_only {
        return Ok(FilesystemInsertSideEffects::default());
    }
    let mut read_rewrite_session = ReadRewriteSession::default();

    let source = match &insert.source {
        Some(source) => source,
        None => return Ok(FilesystemInsertSideEffects::default()),
    };
    let SetExpr::Values(values) = source.body.as_ref() else {
        return Ok(FilesystemInsertSideEffects::default());
    };
    if values.rows.is_empty() {
        return Ok(FilesystemInsertSideEffects::default());
    }

    let path_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("path"));
    let Some(path_index) = path_index else {
        return Ok(FilesystemInsertSideEffects::default());
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
    let mut side_effects = FilesystemInsertSideEffects {
        active_version_id: active_version_id.clone(),
        ..FilesystemInsertSideEffects::default()
    };

    let resolved_rows = resolve_values_rows(&values.rows, params)?;
    let untracked_index = insert.columns.iter().position(|column| {
        column.value.eq_ignore_ascii_case("lixcol_untracked")
            || column.value.eq_ignore_ascii_case("untracked")
    });
    let mut directory_requests: BTreeMap<(String, String), bool> = BTreeMap::new();

    for (row, resolved_row) in values.rows.iter().zip(resolved_rows.iter()) {
        if row.len() != insert.columns.len() {
            return Err(LixError {
                message: "filesystem insert row length does not match column count".to_string(),
            });
        }

        let Some(raw_path) =
            resolve_text_expr(row.get(path_index), resolved_row.get(path_index), "path")?
        else {
            continue;
        };
        let normalized_path = if target.is_file {
            normalize_file_path(&raw_path)?
        } else {
            normalize_directory_path(&raw_path)?
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
        let untracked = untracked_index
            .map(|index| {
                resolve_untracked_expr(
                    row.get(index),
                    resolved_row.get(index),
                    "filesystem insert untracked",
                )
            })
            .transpose()?
            .flatten()
            .unwrap_or(false);

        if target.is_file {
            for ancestor in file_ancestor_directory_paths(&normalized_path) {
                let key = (version_id.clone(), ancestor);
                directory_requests
                    .entry(key)
                    .and_modify(|existing| *existing = *existing && untracked)
                    .or_insert(untracked);
            }
        } else {
            for ancestor in directory_ancestor_paths(&normalized_path) {
                let key = (version_id.clone(), ancestor);
                directory_requests
                    .entry(key)
                    .and_modify(|existing| *existing = *existing && untracked)
                    .or_insert(untracked);
            }
        }
    }

    if directory_requests.is_empty() {
        return Ok(side_effects);
    }

    let mut ordered_requests = directory_requests.into_iter().collect::<Vec<_>>();
    ordered_requests.sort_by(|left, right| {
        let version_order = left.0 .0.cmp(&right.0 .0);
        if version_order != std::cmp::Ordering::Equal {
            return version_order;
        }
        let left_depth = path_depth(&left.0 .1);
        let right_depth = path_depth(&right.0 .1);
        left_depth
            .cmp(&right_depth)
            .then_with(|| left.0 .1.cmp(&right.0 .1))
    });

    let mut known_ids: BTreeMap<(String, String), String> = BTreeMap::new();

    for ((version_id, path), untracked) in ordered_requests {
        let key = (version_id.clone(), path.clone());
        if known_ids.contains_key(&key) {
            continue;
        }

        if let Some(existing_id) =
            find_directory_id_by_path(backend, &version_id, &path, &mut read_rewrite_session)
                .await?
        {
            known_ids.insert(key, existing_id);
            continue;
        }

        let parent_id = match parent_directory_path(&path) {
            Some(parent_path) => {
                let parent_key = (version_id.clone(), parent_path.clone());
                if let Some(parent_id) = known_ids.get(&parent_key) {
                    Some(parent_id.clone())
                } else if let Some(existing_parent_id) = find_directory_id_by_path(
                    backend,
                    &version_id,
                    &parent_path,
                    &mut read_rewrite_session,
                )
                .await?
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
        if untracked {
            let statement_sql = if target.uses_active_version_scope() {
                format!(
                    "INSERT INTO {table} (id, parent_id, name, hidden, lixcol_untracked) \
                     VALUES ('{id}', {parent_id}, '{name}', 0, 1)",
                    table = DIRECTORY_DESCRIPTOR_VIEW,
                    id = escape_sql_string(&id),
                    parent_id = parent_id
                        .as_ref()
                        .map(|value| format!("'{}'", escape_sql_string(value)))
                        .unwrap_or_else(|| "NULL".to_string()),
                    name = escape_sql_string(&name),
                )
            } else {
                format!(
                    "INSERT INTO {table} (id, parent_id, name, hidden, lixcol_version_id, lixcol_untracked) \
                     VALUES ('{id}', {parent_id}, '{name}', 0, '{version_id}', 1)",
                    table = DIRECTORY_DESCRIPTOR_BY_VERSION_VIEW,
                    id = escape_sql_string(&id),
                    parent_id = parent_id
                        .as_ref()
                        .map(|value| format!("'{}'", escape_sql_string(value)))
                        .unwrap_or_else(|| "NULL".to_string()),
                    name = escape_sql_string(&name),
                    version_id = escape_sql_string(&version_id),
                )
            };
            side_effects
                .statements
                .push(parse_single_statement(&statement_sql)?);
        } else {
            let snapshot_content = json!({
                "id": id,
                "parent_id": parent_id,
                "name": name,
                "hidden": false,
            })
            .to_string();
            side_effects
                .tracked_directory_changes
                .push(DetectedFileDomainChange {
                    entity_id: id.clone(),
                    schema_key: DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
                    schema_version: DIRECTORY_DESCRIPTOR_SCHEMA_VERSION.to_string(),
                    file_id: INTERNAL_DESCRIPTOR_FILE_ID.to_string(),
                    version_id: version_id.clone(),
                    plugin_key: INTERNAL_DESCRIPTOR_PLUGIN_KEY.to_string(),
                    snapshot_content: Some(snapshot_content),
                    metadata: None,
                    writer_key: None,
                });
        }
        known_ids.insert(key, id);
    }

    side_effects.resolved_directory_ids = known_ids;

    Ok(side_effects)
}

pub async fn update_side_effects_with_backend(
    backend: &dyn LixBackend,
    update: &Update,
    params: &[EngineValue],
    placeholder_state: &mut PlaceholderState,
) -> Result<FilesystemUpdateSideEffects, LixError> {
    let statement_start_state = *placeholder_state;
    let statement_sql = update.to_string();
    let bound = bind_sql_with_state(
        &statement_sql,
        params,
        backend.dialect(),
        statement_start_state,
    )
    .map_err(|error| LixError {
        message: format!(
            "filesystem update placeholder binding failed for '{}': {}",
            statement_sql, error.message
        ),
    })?;
    *placeholder_state = bound.state;

    let Some(target) = target_from_update_table(&update.table) else {
        return Ok(FilesystemUpdateSideEffects::default());
    };
    if target.read_only || !target.is_file {
        return Ok(FilesystemUpdateSideEffects::default());
    }
    let mut read_rewrite_session = ReadRewriteSession::default();

    let mut statement_placeholder_state = statement_start_state;
    let mut next_path: Option<String> = None;
    for assignment in &update.assignments {
        let Some(column) = assignment_target_name(assignment) else {
            continue;
        };
        let resolved = resolve_expr_cell_with_state(
            &assignment.value,
            params,
            &mut statement_placeholder_state,
        )?;
        if column.eq_ignore_ascii_case("path") {
            next_path = resolve_text_expr(Some(&assignment.value), Some(&resolved), "file path")?;
        }
    }

    let Some(raw_path) = next_path else {
        return Ok(FilesystemUpdateSideEffects::default());
    };

    let selection_placeholder_state = statement_placeholder_state;
    let mut version_predicate_state = selection_placeholder_state;
    let version_id = resolve_update_version_id(
        backend,
        update,
        params,
        target,
        &mut version_predicate_state,
    )
    .await?;
    let matching_file_ids = file_ids_matching_update(
        backend,
        update,
        target,
        params,
        selection_placeholder_state,
        &mut read_rewrite_session,
    )
    .await?;
    if matching_file_ids.is_empty() {
        return Ok(FilesystemUpdateSideEffects::default());
    }
    let all_untracked = matching_file_ids.iter().all(|row| row.untracked);
    let parsed = parse_file_path(&raw_path)?;
    let mut ancestor_paths = file_ancestor_directory_paths(&parsed.normalized_path);
    if ancestor_paths.is_empty() {
        return Ok(FilesystemUpdateSideEffects::default());
    }

    ancestor_paths.sort_by(|left, right| {
        path_depth(left)
            .cmp(&path_depth(right))
            .then_with(|| left.cmp(right))
    });
    ancestor_paths.dedup();
    let directory_changes =
        tracked_missing_directory_changes(backend, &version_id, &ancestor_paths).await?;

    if all_untracked {
        Ok(FilesystemUpdateSideEffects {
            tracked_directory_changes: Vec::new(),
            untracked_directory_changes: directory_changes,
        })
    } else {
        Ok(FilesystemUpdateSideEffects {
            tracked_directory_changes: directory_changes,
            untracked_directory_changes: Vec::new(),
        })
    }
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
    reject_immutable_id_update(&update, target)?;

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
    reject_immutable_id_update(&update, target)?;

    if target.is_file {
        let original_assignments = update.assignments.clone();
        update.assignments.retain(|assignment| {
            assignment_target_name(assignment)
                .map(|name| !name.eq_ignore_ascii_case("data"))
                .unwrap_or(true)
        });
        if update.assignments.is_empty() {
            return Ok(Some(noop_statement()?));
        }
        rewrite_file_update_assignments_with_backend(
            backend,
            &mut update,
            &original_assignments,
            params,
            target,
        )
        .await?;
    } else {
        rewrite_directory_update_assignments_with_backend(backend, &mut update, params, target)
            .await?;
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
    params: &[EngineValue],
) -> Result<Option<Delete>, LixError> {
    let Some(target) = target_from_delete(&delete) else {
        return Ok(None);
    };
    if target.read_only {
        return Err(LixError {
            message: format!("{} does not support DELETE", target.view_name),
        });
    }
    if target.requires_explicit_version_scope() {
        let version_id = extract_predicate_string_with_params_and_dialect(
            delete.selection.as_ref(),
            &["lixcol_version_id", "version_id"],
            params,
            backend.dialect(),
        )?;
        if version_id.is_none() {
            return Err(LixError {
                message: format!(
                    "{} delete requires a version_id predicate",
                    target.view_name
                ),
            });
        }
    }
    let mut read_rewrite_session = ReadRewriteSession::default();

    if target.is_directory() {
        let selected = directory_rows_matching_delete(
            backend,
            &delete,
            target,
            params,
            &mut read_rewrite_session,
        )
        .await?;
        let expanded =
            expand_directory_descendants(backend, &selected, &mut read_rewrite_session).await?;
        if !expanded.is_empty() {
            let predicate = build_directory_delete_selection(&expanded, target)?;
            delete.selection = Some(parse_expression(&predicate)?);
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

async fn rewrite_file_insert_columns_with_backend(
    backend: &dyn LixBackend,
    insert: &mut Insert,
    params: &[EngineValue],
    target: FilesystemTarget,
    resolved_directory_ids: Option<&ResolvedDirectoryIdMap>,
    active_version_id_hint: Option<&str>,
) -> Result<(), LixError> {
    let mut read_rewrite_session = ReadRewriteSession::default();
    let id_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("id"));
    let path_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("path"))
        .ok_or_else(|| LixError {
            message: format!("{} insert requires path", target.view_name),
        })?;
    let by_version_index = insert.columns.iter().position(|column| {
        column.value.eq_ignore_ascii_case("lixcol_version_id")
            || column.value.eq_ignore_ascii_case("version_id")
    });
    let active_version_id = if target.uses_active_version_scope() {
        Some(
            active_version_id_hint
                .map(ToString::to_string)
                .unwrap_or(load_active_version_id(backend).await?),
        )
    } else {
        None
    };

    let source = insert.source.as_ref().ok_or_else(|| LixError {
        message: "filesystem insert requires VALUES rows".to_string(),
    })?;
    let SetExpr::Values(values) = source.body.as_ref() else {
        return Err(LixError {
            message: "filesystem insert requires VALUES rows".to_string(),
        });
    };
    let row_exprs = values.rows.clone();
    let resolved_rows = resolve_values_rows(&row_exprs, params)?;

    let directory_id_index = ensure_insert_column(insert, "directory_id")?;
    let name_index = ensure_insert_column(insert, "name")?;
    let extension_index = ensure_insert_column(insert, "extension")?;
    let rows = insert_values_rows_mut(insert)?;

    for (row_index, row) in rows.iter_mut().enumerate() {
        let resolved_row = resolved_rows.get(row_index).ok_or_else(|| LixError {
            message: "filesystem insert row resolution mismatch".to_string(),
        })?;
        let row_expr = row_exprs.get(row_index).ok_or_else(|| LixError {
            message: "filesystem insert row expression mismatch".to_string(),
        })?;

        let raw_path = resolve_text_expr(
            row_expr.get(path_index),
            resolved_row.get(path_index),
            "file path",
        )?
        .ok_or_else(|| LixError {
            message: "lix_file insert requires path".to_string(),
        })?;
        let parsed = parse_file_path(&raw_path)?;
        let version_id = resolve_insert_row_version_id(
            target,
            &active_version_id,
            by_version_index,
            row_expr,
            resolved_row,
        )?;
        let explicit_id = id_index
            .map(|index| resolve_text_expr(row_expr.get(index), resolved_row.get(index), "file id"))
            .transpose()?
            .flatten();

        let directory_id = if let Some(directory_path) = &parsed.directory_path {
            let lookup_key = (version_id.clone(), directory_path.clone());
            if let Some(existing_id) = resolved_directory_ids
                .and_then(|known| known.get(&lookup_key))
                .cloned()
            {
                Some(existing_id)
            } else if let Some(existing_id) = find_directory_id_by_path(
                backend,
                &version_id,
                directory_path,
                &mut read_rewrite_session,
            )
            .await?
            {
                Some(existing_id)
            } else {
                Some(auto_directory_id(&version_id, directory_path))
            }
        } else {
            None
        };

        let candidate_name = match parsed.extension.as_deref() {
            Some(extension) => format!("{}.{}", parsed.name, extension),
            None => parsed.name.clone(),
        };
        if find_directory_child_id(
            backend,
            &version_id,
            directory_id.as_deref(),
            &candidate_name,
            &mut read_rewrite_session,
        )
        .await?
        .is_some()
        {
            return Err(LixError {
                message: format!(
                    "File path collides with existing directory path: {}/",
                    parsed.normalized_path
                ),
            });
        }

        if let Some(existing_id) = find_file_id_by_components(
            backend,
            &version_id,
            directory_id.as_deref(),
            &parsed.name,
            parsed.extension.as_deref(),
            &mut read_rewrite_session,
        )
        .await?
        {
            let same_id = explicit_id
                .as_deref()
                .map(|value| value == existing_id.as_str())
                .unwrap_or(false);
            if !same_id {
                return Err(file_unique_error(&parsed.normalized_path, &version_id));
            }
        }

        row[directory_id_index] = optional_string_literal_expr(directory_id.as_deref());
        row[name_index] = string_literal_expr(&parsed.name);
        row[extension_index] = optional_string_literal_expr(parsed.extension.as_deref());
    }

    remove_insert_column(insert, "path")?;
    Ok(())
}

async fn rewrite_directory_insert_columns_with_backend(
    backend: &dyn LixBackend,
    insert: &mut Insert,
    params: &[EngineValue],
    target: FilesystemTarget,
    active_version_id_hint: Option<&str>,
) -> Result<(), LixError> {
    let mut read_rewrite_session = ReadRewriteSession::default();
    let path_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("path"));
    let parent_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("parent_id"));
    let name_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("name"));
    let id_index = insert
        .columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case("id"));
    let by_version_index = insert.columns.iter().position(|column| {
        column.value.eq_ignore_ascii_case("lixcol_version_id")
            || column.value.eq_ignore_ascii_case("version_id")
    });
    let active_version_id = if target.uses_active_version_scope() {
        Some(
            active_version_id_hint
                .map(ToString::to_string)
                .unwrap_or(load_active_version_id(backend).await?),
        )
    } else {
        None
    };

    let source = insert.source.as_ref().ok_or_else(|| LixError {
        message: "filesystem insert requires VALUES rows".to_string(),
    })?;
    let SetExpr::Values(values) = source.body.as_ref() else {
        return Err(LixError {
            message: "filesystem insert requires VALUES rows".to_string(),
        });
    };
    let row_exprs = values.rows.clone();
    let resolved_rows = resolve_values_rows(&row_exprs, params)?;

    let ensured_parent_index = ensure_insert_column(insert, "parent_id")?;
    let ensured_name_index = ensure_insert_column(insert, "name")?;
    let rows = insert_values_rows_mut(insert)?;

    for (row_index, row) in rows.iter_mut().enumerate() {
        let resolved_row = resolved_rows.get(row_index).ok_or_else(|| LixError {
            message: "filesystem insert row resolution mismatch".to_string(),
        })?;
        let row_expr = row_exprs.get(row_index).ok_or_else(|| LixError {
            message: "filesystem insert row expression mismatch".to_string(),
        })?;
        let version_id = resolve_insert_row_version_id(
            target,
            &active_version_id,
            by_version_index,
            row_expr,
            resolved_row,
        )?;

        let explicit_id = id_index
            .map(|index| {
                resolve_text_expr(row_expr.get(index), resolved_row.get(index), "directory id")
            })
            .transpose()?
            .flatten();
        let explicit_parent_id = parent_index
            .map(|index| {
                resolve_text_expr(
                    row_expr.get(index),
                    resolved_row.get(index),
                    "directory parent_id",
                )
            })
            .transpose()?
            .flatten();
        let explicit_name = name_index
            .map(|index| {
                resolve_text_expr(
                    row_expr.get(index),
                    resolved_row.get(index),
                    "directory name",
                )
            })
            .transpose()?
            .flatten();
        let explicit_path = path_index
            .map(|index| {
                resolve_text_expr(
                    row_expr.get(index),
                    resolved_row.get(index),
                    "directory path",
                )
            })
            .transpose()?
            .flatten();

        let (computed_parent_id, computed_name, computed_path) =
            if let Some(raw_path) = explicit_path {
                let normalized_path = normalize_directory_path(&raw_path)?;
                let derived_name =
                    directory_name_from_path(&normalized_path).ok_or_else(|| LixError {
                        message: "Directory name must be provided".to_string(),
                    })?;
                let parent_path = parent_directory_path(&normalized_path);
                let derived_parent_id = match parent_path {
                    Some(ref parent_path) => {
                        if let Some(existing_parent_id) = find_directory_id_by_path(
                            backend,
                            &version_id,
                            parent_path,
                            &mut read_rewrite_session,
                        )
                        .await?
                        {
                            Some(existing_parent_id)
                        } else {
                            Some(auto_directory_id(&version_id, parent_path))
                        }
                    }
                    None => None,
                };

                if explicit_parent_id.as_deref() != derived_parent_id.as_deref()
                    && explicit_parent_id.is_some()
                {
                    return Err(LixError {
                        message: format!(
                            "Provided parent_id does not match parent derived from path {}",
                            normalized_path
                        ),
                    });
                }
                if let Some(name) = explicit_name {
                    if normalize_path_segment(&name)? != derived_name {
                        return Err(LixError {
                            message: format!(
                                "Provided directory name '{}' does not match path '{}'",
                                name, normalized_path
                            ),
                        });
                    }
                }

                (derived_parent_id, derived_name, normalized_path)
            } else {
                let raw_name = explicit_name.unwrap_or_default();
                if raw_name.trim().is_empty() {
                    return Err(LixError {
                        message: "Directory name must be provided".to_string(),
                    });
                }
                let name = normalize_path_segment(&raw_name)?;
                let parent_path = match explicit_parent_id.as_deref() {
                    Some(parent_id) => read_directory_path_by_id(
                        backend,
                        &version_id,
                        parent_id,
                        &mut read_rewrite_session,
                    )
                    .await?
                    .ok_or_else(|| LixError {
                        message: format!("Parent directory does not exist for id {parent_id}"),
                    })?,
                    None => "/".to_string(),
                };
                let computed_path = compose_directory_path(parent_path.as_str(), &name)?;
                (explicit_parent_id, name, computed_path)
            };

        if let Some(existing_id) = find_directory_id_by_path(
            backend,
            &version_id,
            &computed_path,
            &mut read_rewrite_session,
        )
        .await?
        {
            let same_id = explicit_id
                .as_deref()
                .map(|value| value == existing_id.as_str())
                .unwrap_or(false);
            if !same_id {
                return Err(directory_unique_error(&computed_path, &version_id));
            }
        }

        assert_no_file_at_directory_path(
            backend,
            &version_id,
            &computed_path,
            &mut read_rewrite_session,
        )
        .await?;

        row[ensured_parent_index] = optional_string_literal_expr(computed_parent_id.as_deref());
        row[ensured_name_index] = string_literal_expr(&computed_name);
    }

    remove_insert_column(insert, "path")?;
    Ok(())
}

async fn rewrite_file_update_assignments_with_backend(
    backend: &dyn LixBackend,
    update: &mut Update,
    original_assignments: &[Assignment],
    params: &[EngineValue],
    target: FilesystemTarget,
) -> Result<(), LixError> {
    let mut read_rewrite_session = ReadRewriteSession::default();
    let mut placeholder_state = PlaceholderState::new();
    let mut next_path: Option<String> = None;
    for assignment in original_assignments {
        let Some(column) = assignment_target_name(assignment) else {
            continue;
        };
        let resolved =
            resolve_expr_cell_with_state(&assignment.value, params, &mut placeholder_state)?;
        if column.eq_ignore_ascii_case("path") {
            next_path = resolve_text_expr(Some(&assignment.value), Some(&resolved), "file path")?;
        }
    }
    let Some(raw_path) = next_path else {
        return Ok(());
    };

    let selection_placeholder_state = placeholder_state;
    let mut version_predicate_state = selection_placeholder_state;
    let version_id = resolve_update_version_id(
        backend,
        update,
        params,
        target,
        &mut version_predicate_state,
    )
    .await?;
    let parsed = parse_file_path(&raw_path)?;
    assert_no_directory_at_file_path(
        backend,
        &version_id,
        &parsed.normalized_path,
        &mut read_rewrite_session,
    )
    .await?;
    let matching_file_ids = file_ids_matching_update(
        backend,
        update,
        target,
        params,
        selection_placeholder_state,
        &mut read_rewrite_session,
    )
    .await?;
    let matching_file_ids = matching_file_ids
        .into_iter()
        .map(|row| row.id)
        .collect::<Vec<_>>();
    if matching_file_ids.len() > 1 {
        return Err(file_unique_error(&parsed.normalized_path, &version_id));
    }
    if !matching_file_ids.is_empty() {
        if let Some(existing_id) = find_file_id_by_path(
            backend,
            &version_id,
            &parsed.normalized_path,
            &mut read_rewrite_session,
        )
        .await?
        {
            let touches_existing = matching_file_ids.iter().any(|id| id == &existing_id);
            let touches_other = matching_file_ids.iter().any(|id| id != &existing_id);
            if !touches_existing || touches_other {
                return Err(file_unique_error(&parsed.normalized_path, &version_id));
            }
        }
    }

    let directory_id = if let Some(directory_path) = &parsed.directory_path {
        Some(
            find_directory_id_by_path(
                backend,
                &version_id,
                directory_path,
                &mut read_rewrite_session,
            )
            .await?
            .unwrap_or_else(|| auto_directory_id(&version_id, directory_path)),
        )
    } else {
        None
    };

    update.assignments.retain(|assignment| {
        assignment_target_name(assignment)
            .map(|name| !name.eq_ignore_ascii_case("path"))
            .unwrap_or(true)
    });
    set_or_replace_update_assignment(
        update,
        "directory_id",
        optional_string_literal_expr(directory_id.as_deref()),
    );
    set_or_replace_update_assignment(update, "name", string_literal_expr(&parsed.name));
    set_or_replace_update_assignment(
        update,
        "extension",
        optional_string_literal_expr(parsed.extension.as_deref()),
    );

    Ok(())
}

async fn tracked_missing_directory_changes(
    backend: &dyn LixBackend,
    version_id: &str,
    ancestor_paths: &[String],
) -> Result<Vec<DetectedFileDomainChange>, LixError> {
    if ancestor_paths.is_empty() {
        return Ok(Vec::new());
    }

    let mut known_ids: BTreeMap<String, String> = BTreeMap::new();
    let mut tracked_directory_changes = Vec::new();
    let mut read_rewrite_session = ReadRewriteSession::default();

    for path in ancestor_paths {
        if known_ids.contains_key(path) {
            continue;
        }
        if let Some(existing_id) =
            find_directory_id_by_path(backend, version_id, path, &mut read_rewrite_session).await?
        {
            known_ids.insert(path.clone(), existing_id);
            continue;
        }

        let parent_id = match parent_directory_path(path) {
            Some(parent_path) => {
                if let Some(parent_id) = known_ids.get(&parent_path) {
                    Some(parent_id.clone())
                } else if let Some(existing_parent_id) = find_directory_id_by_path(
                    backend,
                    version_id,
                    &parent_path,
                    &mut read_rewrite_session,
                )
                .await?
                {
                    known_ids.insert(parent_path, existing_parent_id.clone());
                    Some(existing_parent_id)
                } else {
                    None
                }
            }
            None => None,
        };
        let id = auto_directory_id(version_id, path);
        let name = directory_name_from_path(path).unwrap_or_default();
        let snapshot_content = json!({
            "id": id,
            "parent_id": parent_id,
            "name": name,
            "hidden": false,
        })
        .to_string();
        tracked_directory_changes.push(DetectedFileDomainChange {
            entity_id: id.clone(),
            schema_key: DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
            schema_version: DIRECTORY_DESCRIPTOR_SCHEMA_VERSION.to_string(),
            file_id: INTERNAL_DESCRIPTOR_FILE_ID.to_string(),
            version_id: version_id.to_string(),
            plugin_key: INTERNAL_DESCRIPTOR_PLUGIN_KEY.to_string(),
            snapshot_content: Some(snapshot_content),
            metadata: None,
            writer_key: None,
        });
        known_ids.insert(path.clone(), id);
    }

    Ok(tracked_directory_changes)
}

async fn rewrite_directory_update_assignments_with_backend(
    backend: &dyn LixBackend,
    update: &mut Update,
    params: &[EngineValue],
    target: FilesystemTarget,
) -> Result<(), LixError> {
    let mut placeholder_state = PlaceholderState::new();
    let mut read_rewrite_session = ReadRewriteSession::default();
    let mut next_path: Option<String> = None;
    let mut next_parent_id: Option<Option<String>> = None;
    let mut next_name: Option<String> = None;

    for assignment in &update.assignments {
        let Some(column) = assignment_target_name(assignment) else {
            continue;
        };
        let resolved =
            resolve_expr_cell_with_state(&assignment.value, params, &mut placeholder_state)?;
        if column.eq_ignore_ascii_case("path") {
            next_path =
                resolve_text_expr(Some(&assignment.value), Some(&resolved), "directory path")?;
        } else if column.eq_ignore_ascii_case("parent_id") {
            next_parent_id = Some(resolve_text_expr(
                Some(&assignment.value),
                Some(&resolved),
                "directory parent_id",
            )?);
        } else if column.eq_ignore_ascii_case("name") {
            next_name =
                resolve_text_expr(Some(&assignment.value), Some(&resolved), "directory name")?;
        }
    }

    if next_path.is_none() && next_parent_id.is_none() && next_name.is_none() {
        return Ok(());
    }

    let version_id =
        resolve_update_version_id(backend, update, params, target, &mut placeholder_state).await?;
    let current_directory_id =
        extract_predicate_string(update.selection.as_ref(), &["id", "lixcol_entity_id"]);
    let Some(current_directory_id) = current_directory_id else {
        return Err(LixError {
            message: "lix_directory update requires an id predicate".to_string(),
        });
    };

    let existing = read_directory_descriptor_by_id(
        backend,
        &version_id,
        &current_directory_id,
        &mut read_rewrite_session,
    )
    .await?
    .ok_or_else(|| LixError {
        message: format!("Directory does not exist for id {}", current_directory_id),
    })?;

    let (resolved_parent_id, resolved_name, resolved_path) = if let Some(raw_path) = next_path {
        let normalized_path = normalize_directory_path(&raw_path)?;
        let name = directory_name_from_path(&normalized_path).ok_or_else(|| LixError {
            message: "Directory name must be provided".to_string(),
        })?;
        let parent_id = match parent_directory_path(&normalized_path) {
            Some(parent_path) => find_directory_id_by_path(
                backend,
                &version_id,
                &parent_path,
                &mut read_rewrite_session,
            )
            .await?
            .ok_or_else(|| LixError {
                message: format!("Parent directory does not exist for path {}", parent_path),
            })?,
            None => String::new(),
        };
        let parent_id_opt = if parent_id.is_empty() {
            None
        } else {
            Some(parent_id)
        };
        (parent_id_opt, name, normalized_path)
    } else {
        let parent_id = next_parent_id.unwrap_or(existing.parent_id.clone());
        let name_raw = next_name.unwrap_or(existing.name.clone());
        let name = normalize_path_segment(&name_raw)?;
        if name.is_empty() {
            return Err(LixError {
                message: "Directory name must be provided".to_string(),
            });
        }
        let parent_path = if let Some(parent_id) = parent_id.as_deref() {
            read_directory_path_by_id(backend, &version_id, parent_id, &mut read_rewrite_session)
                .await?
                .ok_or_else(|| LixError {
                    message: format!("Parent directory does not exist for id {}", parent_id),
                })?
        } else {
            "/".to_string()
        };
        let path = compose_directory_path(&parent_path, &name)?;
        (parent_id, name, path)
    };

    if resolved_parent_id.as_deref() == Some(current_directory_id.as_str()) {
        return Err(LixError {
            message: "Directory cannot be its own parent".to_string(),
        });
    }
    if let Some(parent_id) = resolved_parent_id.as_deref() {
        assert_no_directory_cycle(
            backend,
            &version_id,
            current_directory_id.as_str(),
            parent_id,
            &mut read_rewrite_session,
        )
        .await?;
    }
    if let Some(existing_id) = find_directory_id_by_path(
        backend,
        &version_id,
        &resolved_path,
        &mut read_rewrite_session,
    )
    .await?
    {
        if existing_id != current_directory_id {
            return Err(directory_unique_error(&resolved_path, &version_id));
        }
    }
    assert_no_file_at_directory_path(
        backend,
        &version_id,
        &resolved_path,
        &mut read_rewrite_session,
    )
    .await?;

    update.assignments.retain(|assignment| {
        assignment_target_name(assignment)
            .map(|name| !name.eq_ignore_ascii_case("path"))
            .unwrap_or(true)
    });
    set_or_replace_update_assignment(
        update,
        "parent_id",
        optional_string_literal_expr(resolved_parent_id.as_deref()),
    );
    set_or_replace_update_assignment(update, "name", string_literal_expr(&resolved_name));

    Ok(())
}

fn resolve_insert_row_version_id(
    target: FilesystemTarget,
    active_version_id: &Option<String>,
    by_version_index: Option<usize>,
    row_expr: &[Expr],
    resolved_row: &[ResolvedCell],
) -> Result<String, LixError> {
    if let Some(version_id) = active_version_id {
        return Ok(version_id.clone());
    }
    if target.requires_explicit_version_scope() {
        let version_index = by_version_index.ok_or_else(|| LixError {
            message: format!(
                "{} insert requires lixcol_version_id or version_id",
                target.view_name
            ),
        })?;
        return resolve_text_expr(
            row_expr.get(version_index),
            resolved_row.get(version_index),
            "version_id",
        )?
        .ok_or_else(|| LixError {
            message: format!(
                "{} insert requires lixcol_version_id or version_id",
                target.view_name
            ),
        });
    }
    Err(LixError {
        message: "filesystem insert could not resolve version scope".to_string(),
    })
}

async fn resolve_update_version_id(
    backend: &dyn LixBackend,
    update: &Update,
    params: &[EngineValue],
    target: FilesystemTarget,
    placeholder_state: &mut PlaceholderState,
) -> Result<String, LixError> {
    if target.uses_active_version_scope() {
        return load_active_version_id(backend).await;
    }
    if target.requires_explicit_version_scope() {
        return extract_predicate_string_with_params_and_state(
            update.selection.as_ref(),
            &["lixcol_version_id", "version_id"],
            params,
            placeholder_state,
            backend.dialect(),
        )
        .map_err(|error| LixError {
            message: format!(
                "{} update version predicate failed: {}",
                target.view_name, error.message
            ),
        })?
        .ok_or_else(|| LixError {
            message: format!(
                "{} update requires a version_id predicate",
                target.view_name
            ),
        });
    }
    Err(LixError {
        message: "filesystem update could not resolve version scope".to_string(),
    })
}

fn set_or_replace_update_assignment(update: &mut Update, column: &str, value: Expr) {
    if let Some(existing) = update.assignments.iter_mut().find(|assignment| {
        assignment_target_name(assignment)
            .map(|name| name.eq_ignore_ascii_case(column))
            .unwrap_or(false)
    }) {
        existing.value = value;
        return;
    }
    update.assignments.push(sqlparser::ast::Assignment {
        target: sqlparser::ast::AssignmentTarget::ColumnName(table_name(column)),
        value,
    });
}

fn reject_immutable_id_update(update: &Update, target: FilesystemTarget) -> Result<(), LixError> {
    let mutates_id = update.assignments.iter().any(|assignment| {
        assignment_target_name(assignment)
            .map(|name| {
                name.eq_ignore_ascii_case("id") || name.eq_ignore_ascii_case("lixcol_entity_id")
            })
            .unwrap_or(false)
    });
    if !mutates_id {
        return Ok(());
    }

    Err(LixError {
        message: format!(
            "{} id is immutable; create a new row and delete the old row instead",
            target.view_name
        ),
    })
}

fn ensure_insert_column(insert: &mut Insert, column: &str) -> Result<usize, LixError> {
    if let Some(index) = insert
        .columns
        .iter()
        .position(|candidate| candidate.value.eq_ignore_ascii_case(column))
    {
        return Ok(index);
    }
    insert.columns.push(Ident::new(column));
    for row in insert_values_rows_mut(insert)? {
        row.push(Expr::Value(AstValue::Null.into()));
    }
    Ok(insert.columns.len() - 1)
}

fn remove_insert_column(insert: &mut Insert, column: &str) -> Result<(), LixError> {
    let Some(index) = insert
        .columns
        .iter()
        .position(|candidate| candidate.value.eq_ignore_ascii_case(column))
    else {
        return Ok(());
    };
    insert.columns.remove(index);
    for row in insert_values_rows_mut(insert)? {
        if index < row.len() {
            row.remove(index);
        }
    }
    Ok(())
}

fn insert_values_rows_mut(insert: &mut Insert) -> Result<&mut Vec<Vec<Expr>>, LixError> {
    let source = insert.source.as_mut().ok_or_else(|| LixError {
        message: "filesystem insert requires VALUES rows".to_string(),
    })?;
    let SetExpr::Values(values) = source.body.as_mut() else {
        return Err(LixError {
            message: "filesystem insert requires VALUES rows".to_string(),
        });
    };
    Ok(&mut values.rows)
}

fn directory_unique_error(path: &str, version_id: &str) -> LixError {
    LixError {
        message: format!(
            "Unique constraint violation: directory path '{}' already exists in version '{}'",
            path, version_id
        ),
    }
}

fn file_unique_error(path: &str, version_id: &str) -> LixError {
    LixError {
        message: format!(
            "Unique constraint violation: file path '{}' already exists in version '{}'",
            path, version_id
        ),
    }
}

async fn find_directory_id_by_path(
    backend: &dyn LixBackend,
    version_id: &str,
    path: &str,
    read_rewrite_session: &mut ReadRewriteSession,
) -> Result<Option<String>, LixError> {
    let normalized_path = normalize_directory_path(path)?;
    let trimmed = normalized_path.trim_matches('/');
    if trimmed.is_empty() {
        return Ok(None);
    }

    let mut current_parent: Option<String> = None;
    for segment in trimmed.split('/') {
        let Some(next_id) = find_directory_child_id(
            backend,
            version_id,
            current_parent.as_deref(),
            segment,
            read_rewrite_session,
        )
        .await?
        else {
            return Ok(None);
        };
        current_parent = Some(next_id);
    }

    Ok(current_parent)
}

async fn find_directory_child_id(
    backend: &dyn LixBackend,
    version_id: &str,
    parent_id: Option<&str>,
    name: &str,
    read_rewrite_session: &mut ReadRewriteSession,
) -> Result<Option<String>, LixError> {
    let name_expr = json_text_expr_sql(backend.dialect(), "name");
    let parent_expr = json_text_expr_sql(backend.dialect(), "parent_id");
    let version_chain = load_version_chain_ids(backend, version_id, read_rewrite_session).await?;
    if version_chain.is_empty() {
        return Ok(None);
    }
    let mut params = version_chain
        .iter()
        .map(|value| EngineValue::Text(value.clone()))
        .collect::<Vec<_>>();
    let version_predicate = placeholder_range(1, version_chain.len());
    let name_index = params.len() + 1;
    params.push(EngineValue::Text(name.to_string()));
    let parent_predicate = if let Some(parent_id) = parent_id {
        let parent_index = params.len() + 1;
        params.push(EngineValue::Text(parent_id.to_string()));
        format!("{parent_expr} = ${parent_index}")
    } else {
        format!("{parent_expr} IS NULL")
    };
    let lookup_sql = format!(
        "SELECT entity_id, version_id, untracked \
         FROM ( \
           SELECT entity_id, version_id, 1 AS untracked \
           FROM lix_internal_state_untracked \
           WHERE schema_key = '{schema_key}' \
             AND version_id IN ({version_predicate}) \
             AND snapshot_content IS NOT NULL \
             AND {name_expr} = ${name_index} \
             AND {parent_predicate} \
           UNION ALL \
           SELECT entity_id, version_id, 0 AS untracked \
           FROM lix_internal_state_materialized_v1_lix_directory_descriptor \
           WHERE schema_key = '{schema_key}' \
             AND version_id IN ({version_predicate}) \
             AND is_tombstone = 0 \
             AND snapshot_content IS NOT NULL \
             AND {name_expr} = ${name_index} \
             AND {parent_predicate} \
         ) candidates",
        schema_key = DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
        version_predicate = version_predicate,
        name_index = name_index,
        name_expr = name_expr,
        parent_predicate = parent_predicate,
    );
    let result = backend.execute(&lookup_sql, &params).await?;
    let candidate = select_effective_entity_id(&result.rows, &version_chain)?;
    ensure_effective_entity_visible_in_chain(
        backend,
        DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
        "lix_internal_state_materialized_v1_lix_directory_descriptor",
        &version_chain,
        candidate,
    )
    .await
}

async fn find_file_id_by_path(
    backend: &dyn LixBackend,
    version_id: &str,
    path: &str,
    read_rewrite_session: &mut ReadRewriteSession,
) -> Result<Option<String>, LixError> {
    let parsed = parse_file_path(path)?;
    let directory_id = if let Some(directory_path) = parsed.directory_path.as_deref() {
        let resolved =
            find_directory_id_by_path(backend, version_id, directory_path, read_rewrite_session)
                .await?;
        let Some(directory_id) = resolved else {
            return Ok(None);
        };
        Some(directory_id)
    } else {
        None
    };

    find_file_id_by_components(
        backend,
        version_id,
        directory_id.as_deref(),
        &parsed.name,
        parsed.extension.as_deref(),
        read_rewrite_session,
    )
    .await
}

async fn find_file_id_by_components(
    backend: &dyn LixBackend,
    version_id: &str,
    directory_id: Option<&str>,
    name: &str,
    extension: Option<&str>,
    read_rewrite_session: &mut ReadRewriteSession,
) -> Result<Option<String>, LixError> {
    let name_expr = json_text_expr_sql(backend.dialect(), "name");
    let directory_expr = json_text_expr_sql(backend.dialect(), "directory_id");
    let extension_expr = json_text_expr_sql(backend.dialect(), "extension");
    let version_chain = load_version_chain_ids(backend, version_id, read_rewrite_session).await?;
    if version_chain.is_empty() {
        return Ok(None);
    }
    let mut params = version_chain
        .iter()
        .map(|value| EngineValue::Text(value.clone()))
        .collect::<Vec<_>>();
    let version_predicate = placeholder_range(1, version_chain.len());
    let name_index = params.len() + 1;
    params.push(EngineValue::Text(name.to_string()));
    let directory_predicate = if let Some(directory_id) = directory_id {
        let directory_index = params.len() + 1;
        params.push(EngineValue::Text(directory_id.to_string()));
        format!("{directory_expr} = ${directory_index}")
    } else {
        format!("{directory_expr} IS NULL")
    };
    let extension_predicate = if let Some(extension) = extension {
        let index = params.len() + 1;
        params.push(EngineValue::Text(extension.to_string()));
        format!("{extension_expr} = ${index}")
    } else {
        format!("{extension_expr} IS NULL")
    };
    let lookup_sql = format!(
        "SELECT entity_id, version_id, untracked \
         FROM ( \
           SELECT entity_id, version_id, 1 AS untracked \
           FROM lix_internal_state_untracked \
           WHERE schema_key = '{schema_key}' \
             AND version_id IN ({version_predicate}) \
             AND snapshot_content IS NOT NULL \
             AND {name_expr} = ${name_index} \
             AND {directory_predicate} \
             AND {extension_predicate} \
           UNION ALL \
           SELECT entity_id, version_id, 0 AS untracked \
           FROM lix_internal_state_materialized_v1_lix_file_descriptor \
           WHERE schema_key = '{schema_key}' \
             AND version_id IN ({version_predicate}) \
             AND is_tombstone = 0 \
             AND snapshot_content IS NOT NULL \
             AND {name_expr} = ${name_index} \
             AND {directory_predicate} \
             AND {extension_predicate} \
         ) candidates",
        schema_key = FILE_DESCRIPTOR_SCHEMA_KEY,
        version_predicate = version_predicate,
        name_index = name_index,
        name_expr = name_expr,
        directory_predicate = directory_predicate,
        extension_predicate = extension_predicate,
    );
    let result = backend.execute(&lookup_sql, &params).await?;
    let candidate = select_effective_entity_id(&result.rows, &version_chain)?;
    ensure_effective_entity_visible_in_chain(
        backend,
        FILE_DESCRIPTOR_SCHEMA_KEY,
        "lix_internal_state_materialized_v1_lix_file_descriptor",
        &version_chain,
        candidate,
    )
    .await
}

async fn ensure_effective_entity_visible_in_chain(
    backend: &dyn LixBackend,
    schema_key: &str,
    materialized_table: &str,
    version_chain: &[String],
    candidate: Option<String>,
) -> Result<Option<String>, LixError> {
    let Some(candidate_id) = candidate else {
        return Ok(None);
    };
    if !effective_entity_is_tombstoned_in_chain(
        backend,
        schema_key,
        materialized_table,
        version_chain,
        &candidate_id,
    )
    .await?
    {
        return Ok(Some(candidate_id));
    }
    Ok(None)
}

async fn effective_entity_is_tombstoned_in_chain(
    backend: &dyn LixBackend,
    schema_key: &str,
    materialized_table: &str,
    version_chain: &[String],
    entity_id: &str,
) -> Result<bool, LixError> {
    if version_chain.is_empty() {
        return Ok(false);
    }
    let mut params = version_chain
        .iter()
        .map(|value| EngineValue::Text(value.clone()))
        .collect::<Vec<_>>();
    let version_predicate = placeholder_range(1, version_chain.len());
    let entity_index = params.len() + 1;
    params.push(EngineValue::Text(entity_id.to_string()));
    let sql = format!(
        "SELECT version_id, untracked, tombstone \
         FROM ( \
           SELECT version_id, 1 AS untracked, \
                  CASE WHEN snapshot_content IS NULL THEN 1 ELSE 0 END AS tombstone \
           FROM lix_internal_state_untracked \
           WHERE schema_key = '{schema_key}' \
             AND version_id IN ({version_predicate}) \
             AND entity_id = ${entity_index} \
           UNION ALL \
           SELECT version_id, 0 AS untracked, \
                  CASE WHEN is_tombstone = 1 OR snapshot_content IS NULL THEN 1 ELSE 0 END AS tombstone \
           FROM {materialized_table} \
           WHERE schema_key = '{schema_key}' \
             AND version_id IN ({version_predicate}) \
             AND entity_id = ${entity_index} \
         ) candidates",
        schema_key = escape_sql_string(schema_key),
        version_predicate = version_predicate,
        entity_index = entity_index,
        materialized_table = materialized_table,
    );
    let result = backend.execute(&sql, &params).await?;
    let Some(tombstoned) = select_effective_entity_tombstone_state(&result.rows, version_chain)?
    else {
        return Ok(false);
    };
    Ok(tombstoned)
}

fn placeholder_range(start: usize, len: usize) -> String {
    (start..start + len)
        .map(|index| format!("${index}"))
        .collect::<Vec<_>>()
        .join(", ")
}

async fn load_version_chain_ids(
    backend: &dyn LixBackend,
    version_id: &str,
    read_rewrite_session: &mut ReadRewriteSession,
) -> Result<Vec<String>, LixError> {
    if let Some(cached) = read_rewrite_session.cached_version_chain(version_id) {
        return Ok(cached.to_vec());
    }

    let inherits_expr = json_text_expr_sql(backend.dialect(), "inherits_from_version_id");
    let sql = format!(
        "WITH RECURSIVE version_chain(version_id, depth) AS ( \
           SELECT $1 AS version_id, 0 AS depth \
           UNION ALL \
           SELECT \
             COALESCE( \
               (SELECT {inherits_expr} \
                FROM lix_internal_state_untracked \
                WHERE schema_key = '{schema_key}' \
                  AND file_id = '{file_id}' \
                  AND version_id = '{storage_version_id}' \
                  AND entity_id = vc.version_id \
                  AND snapshot_content IS NOT NULL \
                LIMIT 1), \
               (SELECT {inherits_expr} \
                FROM lix_internal_state_materialized_v1_lix_version_descriptor \
                WHERE schema_key = '{schema_key}' \
                  AND version_id = '{storage_version_id}' \
                  AND entity_id = vc.version_id \
                  AND is_tombstone = 0 \
                  AND snapshot_content IS NOT NULL \
                LIMIT 1) \
             ) AS version_id, \
             vc.depth + 1 AS depth \
           FROM version_chain vc \
           WHERE vc.version_id IS NOT NULL \
             AND vc.depth < 64 \
         ) \
         SELECT version_id \
         FROM version_chain \
         WHERE version_id IS NOT NULL",
        inherits_expr = inherits_expr,
        schema_key = escape_sql_string(version_descriptor_schema_key()),
        file_id = escape_sql_string(version_descriptor_file_id()),
        storage_version_id = escape_sql_string(version_descriptor_storage_version_id()),
    );
    let result = backend
        .execute(&sql, &[EngineValue::Text(version_id.to_string())])
        .await?;

    let mut chain = Vec::new();
    for row in &result.rows {
        let Some(value) = row.first() else {
            continue;
        };
        let EngineValue::Text(version_id) = value else {
            continue;
        };
        if chain.iter().any(|existing| existing == version_id) {
            continue;
        }
        chain.push(version_id.clone());
    }
    if chain.is_empty() {
        chain.push(version_id.to_string());
    }

    read_rewrite_session.cache_version_chain(version_id, &chain);
    Ok(chain)
}

fn select_effective_entity_id(
    rows: &[Vec<EngineValue>],
    version_chain: &[String],
) -> Result<Option<String>, LixError> {
    let depth_by_version = version_chain
        .iter()
        .enumerate()
        .map(|(depth, version_id)| (version_id.clone(), depth))
        .collect::<BTreeMap<_, _>>();

    let mut best: Option<(usize, usize, String)> = None;
    for row in rows {
        let Some(EngineValue::Text(entity_id)) = row.first() else {
            continue;
        };
        let Some(EngineValue::Text(version_id)) = row.get(1) else {
            continue;
        };
        let untracked = row
            .get(2)
            .map(parse_untracked_value)
            .transpose()?
            .unwrap_or(false);
        let depth = *depth_by_version.get(version_id).unwrap_or(&usize::MAX);
        let priority = if untracked { 0 } else { 1 };
        let candidate = (depth, priority, entity_id.clone());
        if best
            .as_ref()
            .map(|current| candidate < *current)
            .unwrap_or(true)
        {
            best = Some(candidate);
        }
    }

    Ok(best.map(|(_, _, entity_id)| entity_id))
}

fn select_effective_entity_tombstone_state(
    rows: &[Vec<EngineValue>],
    version_chain: &[String],
) -> Result<Option<bool>, LixError> {
    let depth_by_version = version_chain
        .iter()
        .enumerate()
        .map(|(depth, version_id)| (version_id.clone(), depth))
        .collect::<BTreeMap<_, _>>();

    let mut best_rank: Option<(usize, usize)> = None;
    let mut best_tombstone: Option<bool> = None;
    for row in rows {
        let Some(EngineValue::Text(version_id)) = row.first() else {
            continue;
        };
        let untracked = row
            .get(1)
            .map(parse_untracked_value)
            .transpose()?
            .unwrap_or(false);
        let tombstone = row
            .get(2)
            .map(parse_untracked_value)
            .transpose()?
            .unwrap_or(false);
        let depth = *depth_by_version.get(version_id).unwrap_or(&usize::MAX);
        let priority = if untracked { 0 } else { 1 };
        let candidate_rank = (depth, priority);
        if best_rank
            .as_ref()
            .map(|current| candidate_rank < *current)
            .unwrap_or(true)
        {
            best_rank = Some(candidate_rank);
            best_tombstone = Some(tombstone);
        }
    }

    Ok(best_tombstone)
}

fn json_text_expr_sql(dialect: crate::backend::SqlDialect, field: &str) -> String {
    match dialect {
        crate::backend::SqlDialect::Sqlite => {
            format!("json_extract(snapshot_content, '$.\"{field}\"')")
        }
        crate::backend::SqlDialect::Postgres => {
            format!("jsonb_extract_path_text(CAST(snapshot_content AS JSONB), '{field}')")
        }
    }
}

async fn rewrite_single_read_query_for_backend(
    backend: &dyn LixBackend,
    sql: &str,
    params: &[EngineValue],
    read_rewrite_session: &mut ReadRewriteSession,
) -> Result<String, LixError> {
    let cache_key = {
        let dialect = match backend.dialect() {
            crate::backend::SqlDialect::Sqlite => "sqlite",
            crate::backend::SqlDialect::Postgres => "postgres",
        };
        format!("{dialect}\n{sql}")
    };
    if let Some(cached) = helper_sql_cache()
        .lock()
        .expect("helper sql cache mutex poisoned")
        .get(&cache_key)
        .cloned()
    {
        return Ok(cached);
    }

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
    let rewritten = rewrite_read_query_with_backend_and_params_in_session(
        backend,
        *query,
        params,
        read_rewrite_session,
    )
    .await?;
    let lowered = lower_statement(Statement::Query(Box::new(rewritten)), backend.dialect())?;
    let lowered_sql = lowered.to_string();
    let mut cache = helper_sql_cache()
        .lock()
        .expect("helper sql cache mutex poisoned");
    if cache.len() >= 256 {
        cache.clear();
    }
    cache.insert(cache_key, lowered_sql.clone());
    Ok(lowered_sql)
}

fn helper_sql_cache() -> &'static Mutex<BTreeMap<String, String>> {
    REWRITTEN_HELPER_SQL_CACHE.get_or_init(|| Mutex::new(BTreeMap::new()))
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

fn resolve_untracked_expr(
    expr: Option<&Expr>,
    cell: Option<&ResolvedCell>,
    context: &str,
) -> Result<Option<bool>, LixError> {
    if let Some(cell) = cell {
        if let Some(value) = &cell.value {
            return match value {
                EngineValue::Null => Ok(None),
                EngineValue::Integer(value) => Ok(Some(*value != 0)),
                EngineValue::Real(value) => Ok(Some(*value != 0.0)),
                EngineValue::Text(value) => {
                    let normalized = value.trim().to_ascii_lowercase();
                    match normalized.as_str() {
                        "1" | "true" => Ok(Some(true)),
                        "0" | "false" | "" => Ok(Some(false)),
                        _ => Err(LixError {
                            message: format!(
                                "{context} expects a boolean-like value, got '{}'",
                                value
                            ),
                        }),
                    }
                }
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
        AstValue::Boolean(value) => Ok(Some(*value)),
        AstValue::Number(value, _) => match value.trim() {
            "1" => Ok(Some(true)),
            "0" => Ok(Some(false)),
            _ => Err(LixError {
                message: format!("{context} expects 0/1, got '{}'", value),
            }),
        },
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
        | AstValue::TripleDoubleQuotedByteStringLiteral(value) => {
            let normalized = value.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "1" | "true" => Ok(Some(true)),
                "0" | "false" | "" => Ok(Some(false)),
                _ => Err(LixError {
                    message: format!("{context} expects boolean-like text, got '{}'", value),
                }),
            }
        }
        AstValue::DollarQuotedString(value) => {
            let normalized = value.value.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "1" | "true" => Ok(Some(true)),
                "0" | "false" | "" => Ok(Some(false)),
                _ => Err(LixError {
                    message: format!("{context} expects boolean-like text, got '{}'", value.value),
                }),
            }
        }
        AstValue::Placeholder(_) => Ok(None),
    }
}

fn extract_predicate_string(selection: Option<&Expr>, columns: &[&str]) -> Option<String> {
    extract_predicate_string_with_params(selection, columns, &[])
        .ok()
        .flatten()
}

fn extract_predicate_string_with_params(
    selection: Option<&Expr>,
    columns: &[&str],
    params: &[EngineValue],
) -> Result<Option<String>, LixError> {
    extract_predicate_string_with_params_and_dialect(
        selection,
        columns,
        params,
        crate::SqlDialect::Sqlite,
    )
}

fn extract_predicate_string_with_params_and_dialect(
    selection: Option<&Expr>,
    columns: &[&str],
    params: &[EngineValue],
    dialect: crate::SqlDialect,
) -> Result<Option<String>, LixError> {
    let mut placeholder_state = PlaceholderState::new();
    extract_predicate_string_with_params_and_state(
        selection,
        columns,
        params,
        &mut placeholder_state,
        dialect,
    )
}

fn extract_predicate_string_with_params_and_state(
    selection: Option<&Expr>,
    columns: &[&str],
    params: &[EngineValue],
    placeholder_state: &mut PlaceholderState,
    dialect: crate::SqlDialect,
) -> Result<Option<String>, LixError> {
    let selection = match selection {
        Some(selection) => selection,
        None => return Ok(None),
    };
    match selection {
        Expr::BinaryOp { left, op, right } => {
            if op.to_string().eq_ignore_ascii_case("=") {
                if let Some(column) = expr_column_name(left) {
                    if columns
                        .iter()
                        .any(|candidate| column.eq_ignore_ascii_case(candidate))
                    {
                        if let Some(value) =
                            expr_string_literal_or_placeholder(right, params, placeholder_state)?
                        {
                            return Ok(Some(value));
                        }
                    }
                }
                if let Some(column) = expr_column_name(right) {
                    if columns
                        .iter()
                        .any(|candidate| column.eq_ignore_ascii_case(candidate))
                    {
                        if let Some(value) =
                            expr_string_literal_or_placeholder(left, params, placeholder_state)?
                        {
                            return Ok(Some(value));
                        }
                    }
                }
            }
            let left_match = extract_predicate_string_with_params_and_state(
                Some(left),
                columns,
                params,
                placeholder_state,
                dialect,
            )?;
            if left_match.is_some() {
                return Ok(left_match);
            }
            extract_predicate_string_with_params_and_state(
                Some(right),
                columns,
                params,
                placeholder_state,
                dialect,
            )
        }
        Expr::Nested(inner) => extract_predicate_string_with_params_and_state(
            Some(inner),
            columns,
            params,
            placeholder_state,
            dialect,
        ),
        _ => {
            consume_placeholders_in_expr(selection, params, placeholder_state, dialect)?;
            Ok(None)
        }
    }
}

fn consume_placeholders_in_expr(
    expr: &Expr,
    params: &[EngineValue],
    placeholder_state: &mut PlaceholderState,
    dialect: crate::SqlDialect,
) -> Result<(), LixError> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: AstValue::Placeholder(_),
            ..
        }) => {
            let _ = resolve_expr_cell_with_state(expr, params, placeholder_state)?;
            Ok(())
        }
        Expr::BinaryOp { left, right, .. } => {
            consume_placeholders_in_expr(left, params, placeholder_state, dialect)?;
            consume_placeholders_in_expr(right, params, placeholder_state, dialect)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => {
            consume_placeholders_in_expr(expr, params, placeholder_state, dialect)
        }
        Expr::InList { expr, list, .. } => {
            consume_placeholders_in_expr(expr, params, placeholder_state, dialect)?;
            for item in list {
                consume_placeholders_in_expr(item, params, placeholder_state, dialect)?;
            }
            Ok(())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            consume_placeholders_in_expr(expr, params, placeholder_state, dialect)?;
            consume_placeholders_in_expr(low, params, placeholder_state, dialect)?;
            consume_placeholders_in_expr(high, params, placeholder_state, dialect)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            consume_placeholders_in_expr(expr, params, placeholder_state, dialect)?;
            consume_placeholders_in_expr(pattern, params, placeholder_state, dialect)
        }
        Expr::Function(function) => match &function.args {
            sqlparser::ast::FunctionArguments::List(list) => {
                for arg in &list.args {
                    match arg {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(expr),
                        ) => {
                            consume_placeholders_in_expr(expr, params, placeholder_state, dialect)?
                        }
                        sqlparser::ast::FunctionArg::Named { arg, .. }
                        | sqlparser::ast::FunctionArg::ExprNamed { arg, .. } => {
                            if let sqlparser::ast::FunctionArgExpr::Expr(expr) = arg {
                                consume_placeholders_in_expr(
                                    expr,
                                    params,
                                    placeholder_state,
                                    dialect,
                                )?;
                            }
                        }
                        _ => {}
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        },
        Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
            consume_placeholders_in_expr(left, params, placeholder_state, dialect)?;
            consume_placeholders_in_expr(right, params, placeholder_state, dialect)
        }
        Expr::InSubquery { expr, subquery, .. } => {
            consume_placeholders_in_expr(expr, params, placeholder_state, dialect)?;
            consume_placeholders_in_query(subquery, params, placeholder_state, dialect)
        }
        Expr::Exists { subquery, .. } | Expr::Subquery(subquery) => {
            consume_placeholders_in_query(subquery, params, placeholder_state, dialect)
        }
        _ => Ok(()),
    }
}

fn consume_placeholders_in_query(
    query: &sqlparser::ast::Query,
    params: &[EngineValue],
    placeholder_state: &mut PlaceholderState,
    dialect: crate::SqlDialect,
) -> Result<(), LixError> {
    let probe_sql = format!("SELECT 1 WHERE EXISTS ({})", query);
    let bound = bind_sql_with_state(&probe_sql, params, dialect, *placeholder_state)?;
    *placeholder_state = bound.state;
    Ok(())
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

fn expr_string_literal_or_placeholder(
    expr: &Expr,
    params: &[EngineValue],
    placeholder_state: &mut PlaceholderState,
) -> Result<Option<String>, LixError> {
    if let Some(value) = expr_string_literal(expr) {
        return Ok(Some(value));
    }
    let resolved = resolve_expr_cell_with_state(expr, params, placeholder_state)?;
    let Some(value) = resolved.value else {
        return Ok(None);
    };
    match value {
        EngineValue::Text(value) => Ok(Some(value)),
        EngineValue::Integer(value) => Ok(Some(value.to_string())),
        EngineValue::Real(value) => Ok(Some(value.to_string())),
        EngineValue::Null => Ok(None),
        EngineValue::Blob(_) => Ok(None),
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

#[derive(Debug, Clone)]
struct DirectoryDescriptorSnapshot {
    parent_id: Option<String>,
    name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ScopedDirectoryId {
    id: String,
    version_id: String,
}

fn string_literal_expr(value: &str) -> Expr {
    Expr::Value(AstValue::SingleQuotedString(value.to_string()).into())
}

fn optional_string_literal_expr(value: Option<&str>) -> Expr {
    match value {
        Some(value) => string_literal_expr(value),
        None => Expr::Value(AstValue::Null.into()),
    }
}

fn auto_directory_id(version_id: &str, path: &str) -> String {
    format!("lix-auto-dir:{}:{}", version_id, path)
}

async fn assert_no_directory_at_file_path(
    backend: &dyn LixBackend,
    version_id: &str,
    file_path: &str,
    read_rewrite_session: &mut ReadRewriteSession,
) -> Result<(), LixError> {
    let directory_path = format!("{file_path}/");
    if find_directory_id_by_path(backend, version_id, &directory_path, read_rewrite_session)
        .await?
        .is_none()
    {
        return Ok(());
    }
    Err(LixError {
        message: format!("File path collides with existing directory path: {directory_path}"),
    })
}

async fn assert_no_file_at_directory_path(
    backend: &dyn LixBackend,
    version_id: &str,
    directory_path: &str,
    read_rewrite_session: &mut ReadRewriteSession,
) -> Result<(), LixError> {
    let file_path = directory_path.trim_end_matches('/').to_string();
    if find_file_id_by_path(backend, version_id, &file_path, read_rewrite_session)
        .await?
        .is_none()
    {
        return Ok(());
    }
    Err(LixError {
        message: format!("Directory path collides with existing file path: {file_path}"),
    })
}

async fn read_directory_path_by_id(
    backend: &dyn LixBackend,
    version_id: &str,
    directory_id: &str,
    read_rewrite_session: &mut ReadRewriteSession,
) -> Result<Option<String>, LixError> {
    let sql = "SELECT path \
         FROM lix_directory_by_version \
         WHERE lixcol_version_id = $1 AND id = $2 \
         LIMIT 1";
    let query_params = vec![
        EngineValue::Text(version_id.to_string()),
        EngineValue::Text(directory_id.to_string()),
    ];
    let rewritten_sql =
        rewrite_single_read_query_for_backend(backend, sql, &query_params, read_rewrite_session)
            .await?;
    let result = backend.execute(&rewritten_sql, &query_params).await?;
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    let Some(value) = row.first() else {
        return Ok(None);
    };
    match value {
        EngineValue::Text(path) => Ok(Some(path.clone())),
        other => Err(LixError {
            message: format!("directory path lookup expected text path, got {other:?}"),
        }),
    }
}

async fn read_directory_descriptor_by_id(
    backend: &dyn LixBackend,
    version_id: &str,
    directory_id: &str,
    read_rewrite_session: &mut ReadRewriteSession,
) -> Result<Option<DirectoryDescriptorSnapshot>, LixError> {
    let sql = "SELECT \
         lix_json_extract(snapshot_content, 'parent_id') AS parent_id, \
         lix_json_extract(snapshot_content, 'name') AS name \
         FROM lix_state_by_version \
         WHERE schema_key = 'lix_directory_descriptor' \
           AND snapshot_content IS NOT NULL \
           AND version_id = $1 \
           AND lix_json_extract(snapshot_content, 'id') = $2 \
         LIMIT 1";
    let query_params = vec![
        EngineValue::Text(version_id.to_string()),
        EngineValue::Text(directory_id.to_string()),
    ];
    let rewritten_sql =
        rewrite_single_read_query_for_backend(backend, sql, &query_params, read_rewrite_session)
            .await?;
    let result = backend.execute(&rewritten_sql, &query_params).await?;
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    if row.len() < 2 {
        return Err(LixError {
            message: "directory descriptor lookup expected two columns".to_string(),
        });
    }
    let parent_id = match &row[0] {
        EngineValue::Null => None,
        EngineValue::Text(value) => Some(value.clone()),
        other => {
            return Err(LixError {
                message: format!(
                    "directory descriptor parent_id expected text/null, got {other:?}"
                ),
            })
        }
    };
    let name = match &row[1] {
        EngineValue::Text(value) => value.clone(),
        other => {
            return Err(LixError {
                message: format!("directory descriptor name expected text, got {other:?}"),
            })
        }
    };
    Ok(Some(DirectoryDescriptorSnapshot { parent_id, name }))
}

async fn assert_no_directory_cycle(
    backend: &dyn LixBackend,
    version_id: &str,
    directory_id: &str,
    parent_id: &str,
    read_rewrite_session: &mut ReadRewriteSession,
) -> Result<(), LixError> {
    let mut safety = 0usize;
    let mut current_parent: Option<String> = Some(parent_id.to_string());
    while let Some(parent_id) = current_parent {
        if parent_id == directory_id {
            return Err(LixError {
                message: "Directory parent would create a cycle".to_string(),
            });
        }
        if safety > 1024 {
            return Err(LixError {
                message: "Directory hierarchy appears to be cyclic".to_string(),
            });
        }
        safety += 1;

        let Some(snapshot) =
            read_directory_descriptor_by_id(backend, version_id, &parent_id, read_rewrite_session)
                .await?
        else {
            return Err(LixError {
                message: format!("Parent directory does not exist for id {}", parent_id),
            });
        };
        current_parent = snapshot.parent_id;
    }
    Ok(())
}

async fn directory_rows_matching_delete(
    backend: &dyn LixBackend,
    delete: &Delete,
    target: FilesystemTarget,
    params: &[EngineValue],
    read_rewrite_session: &mut ReadRewriteSession,
) -> Result<Vec<ScopedDirectoryId>, LixError> {
    let where_clause = delete
        .selection
        .as_ref()
        .map(|selection| format!(" WHERE {selection}"))
        .unwrap_or_default();
    let active_version_id = if target.uses_active_version_scope() {
        Some(load_active_version_id(backend).await?)
    } else {
        None
    };
    let sql = if target.uses_active_version_scope() {
        format!(
            "SELECT id FROM {view_name}{where_clause}",
            view_name = target.view_name,
            where_clause = where_clause
        )
    } else {
        format!(
            "SELECT id, lixcol_version_id FROM {view_name}{where_clause}",
            view_name = target.view_name,
            where_clause = where_clause
        )
    };
    let rewritten_sql =
        rewrite_single_read_query_for_backend(backend, &sql, params, read_rewrite_session).await?;
    let result = backend
        .execute(&rewritten_sql, params)
        .await
        .map_err(|error| LixError {
            message: format!(
                "directory delete scope prefetch failed for '{}': {}",
                rewritten_sql, error.message
            ),
        })?;

    let mut rows: Vec<ScopedDirectoryId> = Vec::new();
    for row in result.rows {
        if target.uses_active_version_scope() {
            let version_id = active_version_id.clone().ok_or_else(|| LixError {
                message: "active version id is missing for directory delete".to_string(),
            })?;
            let id = match row.first() {
                Some(EngineValue::Text(value)) => value.clone(),
                Some(other) => {
                    return Err(LixError {
                        message: format!("directory delete id lookup expected text, got {other:?}"),
                    })
                }
                None => continue,
            };
            rows.push(ScopedDirectoryId { id, version_id });
        } else {
            if row.len() < 2 {
                continue;
            }
            let id = match &row[0] {
                EngineValue::Text(value) => value.clone(),
                other => {
                    return Err(LixError {
                        message: format!("directory delete id lookup expected text, got {other:?}"),
                    })
                }
            };
            let version_id = match &row[1] {
                EngineValue::Text(value) => value.clone(),
                other => {
                    return Err(LixError {
                        message: format!(
                            "directory delete version lookup expected text, got {other:?}"
                        ),
                    })
                }
            };
            rows.push(ScopedDirectoryId { id, version_id });
        }
    }
    rows.sort();
    rows.dedup();
    Ok(rows)
}

#[derive(Debug, Clone)]
struct ScopedFileUpdateRow {
    id: String,
    untracked: bool,
}

#[derive(Debug, Default, Clone)]
struct ExactFileUpdateSelection {
    file_id: Option<String>,
    explicit_version_id: Option<String>,
    invalid: bool,
}

async fn file_ids_matching_update(
    backend: &dyn LixBackend,
    update: &Update,
    target: FilesystemTarget,
    params: &[EngineValue],
    placeholder_state: PlaceholderState,
    read_rewrite_session: &mut ReadRewriteSession,
) -> Result<Vec<ScopedFileUpdateRow>, LixError> {
    let trace = file_prefetch_trace_enabled();
    if let Some(rows) = try_file_ids_matching_update_fast(
        backend,
        update,
        target,
        params,
        placeholder_state,
        read_rewrite_session,
    )
    .await?
    {
        if trace {
            eprintln!(
                "[trace][file-prefetch] module=mutation_rewrite label=file_ids_matching_update mode=fast-id rows={}",
                rows.len(),
            );
        }
        return Ok(rows);
    }

    let where_clause = update
        .selection
        .as_ref()
        .map(|selection| format!(" WHERE {selection}"))
        .unwrap_or_default();
    let sql = format!(
        "SELECT id, lixcol_untracked, \
                'mutation.file_ids_matching_update' AS __lix_trace \
         FROM {view_name}{where_clause}",
        view_name = target.view_name,
        where_clause = where_clause
    );
    let rewritten_sql =
        rewrite_single_read_query_for_backend(backend, &sql, params, read_rewrite_session).await?;
    let bound = bind_sql_with_state(&rewritten_sql, params, backend.dialect(), placeholder_state)?;
    let result = backend
        .execute(&bound.sql, &bound.params)
        .await
        .map_err(|error| LixError {
            message: format!(
                "file update scope prefetch failed for '{}': {}",
                bound.sql, error.message
            ),
        })?;
    if trace {
        eprintln!(
            "[trace][file-prefetch] module=mutation_rewrite label=file_ids_matching_update source_sql_chars={} rewritten_sql_chars={} rows={}",
            sql.len(),
            bound.sql.len(),
            result.rows.len(),
        );
    }

    let mut out: Vec<ScopedFileUpdateRow> = Vec::new();
    for row in result.rows {
        let Some(id_value) = row.first() else {
            continue;
        };
        let EngineValue::Text(id) = id_value else {
            return Err(LixError {
                message: format!("file update id lookup expected text, got {id_value:?}"),
            });
        };
        let untracked = row
            .get(1)
            .map(parse_untracked_value)
            .transpose()?
            .unwrap_or(false);
        out.push(ScopedFileUpdateRow {
            id: id.clone(),
            untracked,
        });
    }

    out.sort_by(|left, right| left.id.cmp(&right.id));
    out.dedup_by(|left, right| left.id == right.id);
    Ok(out)
}

async fn try_file_ids_matching_update_fast(
    backend: &dyn LixBackend,
    update: &Update,
    target: FilesystemTarget,
    params: &[EngineValue],
    placeholder_state: PlaceholderState,
    read_rewrite_session: &mut ReadRewriteSession,
) -> Result<Option<Vec<ScopedFileUpdateRow>>, LixError> {
    let mut selection_state = placeholder_state;
    let Some(selection) = extract_exact_file_update_selection_with_state(
        update.selection.as_ref(),
        params,
        &mut selection_state,
        backend.dialect(),
    )?
    else {
        return Ok(None);
    };
    if selection.invalid {
        return Ok(None);
    }
    let Some(file_id) = selection.file_id else {
        return Ok(None);
    };
    let version_id = if target.uses_active_version_scope() {
        load_active_version_id(backend).await?
    } else if target.requires_explicit_version_scope() {
        let Some(version_id) = selection.explicit_version_id else {
            return Ok(None);
        };
        version_id
    } else {
        return Ok(None);
    };

    if let Some(rows) =
        try_exact_file_descriptor_lookup_current_version(backend, &version_id, &file_id).await?
    {
        return Ok(Some(rows));
    }

    let sql = "SELECT entity_id, untracked, \
                    'mutation.file_ids_matching_update.fast_id' AS __lix_trace \
             FROM lix_state_by_version \
             WHERE schema_key = 'lix_file_descriptor' \
               AND snapshot_content IS NOT NULL \
               AND version_id = $1 \
               AND entity_id = $2";
    let query_params = vec![EngineValue::Text(version_id), EngineValue::Text(file_id)];
    let rewritten_sql =
        rewrite_single_read_query_for_backend(backend, sql, &query_params, read_rewrite_session)
            .await?;
    let result = backend
        .execute(&rewritten_sql, &query_params)
        .await
        .map_err(|error| LixError {
            message: format!(
                "file update fast-path prefetch failed for '{}': {}",
                rewritten_sql, error.message
            ),
        })?;

    let mut out = Vec::<ScopedFileUpdateRow>::with_capacity(result.rows.len());
    for row in result.rows {
        let Some(id_value) = row.first() else {
            continue;
        };
        let EngineValue::Text(id) = id_value else {
            return Err(LixError {
                message: format!("file update id lookup expected text, got {id_value:?}"),
            });
        };
        let untracked = row
            .get(1)
            .map(parse_untracked_value)
            .transpose()?
            .unwrap_or(false);
        out.push(ScopedFileUpdateRow {
            id: id.clone(),
            untracked,
        });
    }
    Ok(Some(out))
}

async fn try_exact_file_descriptor_lookup_current_version(
    backend: &dyn LixBackend,
    version_id: &str,
    file_id: &str,
) -> Result<Option<Vec<ScopedFileUpdateRow>>, LixError> {
    let untracked_sql = "SELECT entity_id, 1 AS untracked, snapshot_content, \
                              'mutation.file_ids_matching_update.fast_id.untracked' AS __lix_trace \
                       FROM lix_internal_state_untracked \
                       WHERE schema_key = 'lix_file_descriptor' \
                         AND version_id = $1 \
                         AND entity_id = $2 \
                       LIMIT 1";
    let untracked_result = backend
        .execute(
            untracked_sql,
            &[
                EngineValue::Text(version_id.to_string()),
                EngineValue::Text(file_id.to_string()),
            ],
        )
        .await
        .map_err(|error| LixError {
            message: format!(
                "file update exact current-version prefetch failed for '{}': {}",
                untracked_sql, error.message
            ),
        })?;
    if let Some(parsed) = parse_exact_file_descriptor_lookup_rows(untracked_result.rows)? {
        return Ok(Some(parsed));
    }

    let materialized_sql = "SELECT entity_id, 0 AS untracked, snapshot_content, \
                                 'mutation.file_ids_matching_update.fast_id.materialized' AS __lix_trace \
                          FROM lix_internal_state_materialized_v1_lix_file_descriptor \
                          WHERE schema_key = 'lix_file_descriptor' \
                            AND version_id = $1 \
                            AND entity_id = $2 \
                            AND is_tombstone = 0 \
                            AND snapshot_content IS NOT NULL \
                          LIMIT 1";
    let materialized_result = backend
        .execute(
            materialized_sql,
            &[
                EngineValue::Text(version_id.to_string()),
                EngineValue::Text(file_id.to_string()),
            ],
        )
        .await
        .map_err(|error| LixError {
            message: format!(
                "file update exact current-version prefetch failed for '{}': {}",
                materialized_sql, error.message
            ),
        })?;

    parse_exact_file_descriptor_lookup_rows(materialized_result.rows)
}

fn parse_exact_file_descriptor_lookup_rows(
    rows: Vec<Vec<EngineValue>>,
) -> Result<Option<Vec<ScopedFileUpdateRow>>, LixError> {
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let Some(id_value) = row.first() else {
        return Ok(Some(Vec::new()));
    };
    let EngineValue::Text(id) = id_value else {
        return Err(LixError {
            message: format!("file update id lookup expected text, got {id_value:?}"),
        });
    };
    let untracked = row
        .get(1)
        .map(parse_untracked_value)
        .transpose()?
        .unwrap_or(false);
    let is_tombstone = row
        .get(2)
        .is_none_or(|value| matches!(value, EngineValue::Null));
    if is_tombstone {
        return Ok(Some(Vec::new()));
    }
    Ok(Some(vec![ScopedFileUpdateRow {
        id: id.clone(),
        untracked,
    }]))
}

fn extract_exact_file_update_selection_with_state(
    selection: Option<&Expr>,
    params: &[EngineValue],
    placeholder_state: &mut PlaceholderState,
    dialect: crate::SqlDialect,
) -> Result<Option<ExactFileUpdateSelection>, LixError> {
    let Some(selection) = selection else {
        return Ok(None);
    };
    let mut out = ExactFileUpdateSelection::default();
    if !collect_exact_file_update_predicates(
        selection,
        params,
        placeholder_state,
        dialect,
        &mut out,
    )? {
        return Ok(None);
    }
    if out.invalid || out.file_id.is_none() {
        return Ok(None);
    }
    Ok(Some(out))
}

fn collect_exact_file_update_predicates(
    selection: &Expr,
    params: &[EngineValue],
    placeholder_state: &mut PlaceholderState,
    dialect: crate::SqlDialect,
    out: &mut ExactFileUpdateSelection,
) -> Result<bool, LixError> {
    match selection {
        Expr::Nested(inner) => {
            collect_exact_file_update_predicates(inner, params, placeholder_state, dialect, out)
        }
        Expr::BinaryOp { left, op, right } => {
            if op.to_string().eq_ignore_ascii_case("AND") {
                let left_ok = collect_exact_file_update_predicates(
                    left,
                    params,
                    placeholder_state,
                    dialect,
                    out,
                )?;
                let right_ok = collect_exact_file_update_predicates(
                    right,
                    params,
                    placeholder_state,
                    dialect,
                    out,
                )?;
                return Ok(left_ok && right_ok);
            }
            if op.to_string().eq_ignore_ascii_case("=") {
                if let Some(column) = expr_column_name(left) {
                    if let Some(value) =
                        expr_string_literal_or_placeholder(right, params, placeholder_state)?
                    {
                        if apply_exact_file_update_predicate(&column, &value, out) {
                            return Ok(true);
                        }
                    } else {
                        return Ok(false);
                    }
                }
                if let Some(column) = expr_column_name(right) {
                    if let Some(value) =
                        expr_string_literal_or_placeholder(left, params, placeholder_state)?
                    {
                        if apply_exact_file_update_predicate(&column, &value, out) {
                            return Ok(true);
                        }
                    } else {
                        return Ok(false);
                    }
                }
            }
            consume_placeholders_in_expr(selection, params, placeholder_state, dialect)?;
            Ok(false)
        }
        _ => {
            consume_placeholders_in_expr(selection, params, placeholder_state, dialect)?;
            Ok(false)
        }
    }
}

fn apply_exact_file_update_predicate(
    column: &str,
    value: &str,
    out: &mut ExactFileUpdateSelection,
) -> bool {
    if column.eq_ignore_ascii_case("id")
        || column.eq_ignore_ascii_case("lixcol_entity_id")
        || column.eq_ignore_ascii_case("lixcol_file_id")
    {
        if let Some(existing) = out.file_id.as_ref() {
            if existing != value {
                out.invalid = true;
            }
        } else {
            out.file_id = Some(value.to_string());
        }
        return true;
    }
    if column.eq_ignore_ascii_case("lixcol_version_id") || column.eq_ignore_ascii_case("version_id")
    {
        if let Some(existing) = out.explicit_version_id.as_ref() {
            if existing != value {
                out.invalid = true;
            }
        } else {
            out.explicit_version_id = Some(value.to_string());
        }
        return true;
    }
    false
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

fn parse_untracked_value(value: &EngineValue) -> Result<bool, LixError> {
    match value {
        EngineValue::Integer(v) => Ok(*v != 0),
        EngineValue::Text(v) => {
            let normalized = v.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "1" | "true" => Ok(true),
                "0" | "false" | "" => Ok(false),
                _ => Err(LixError {
                    message: format!(
                        "file update untracked lookup expected boolean-like text, got '{v}'"
                    ),
                }),
            }
        }
        EngineValue::Null => Ok(false),
        other => Err(LixError {
            message: format!(
                "file update untracked lookup expected boolean-like value, got {other:?}"
            ),
        }),
    }
}

async fn expand_directory_descendants(
    backend: &dyn LixBackend,
    selected: &[ScopedDirectoryId],
    read_rewrite_session: &mut ReadRewriteSession,
) -> Result<Vec<ScopedDirectoryId>, LixError> {
    let mut out: BTreeSet<ScopedDirectoryId> = BTreeSet::new();
    for scoped in selected {
        let ids = load_directory_descendants(
            backend,
            &scoped.version_id,
            &scoped.id,
            read_rewrite_session,
        )
        .await?;
        for id in ids {
            out.insert(ScopedDirectoryId {
                id,
                version_id: scoped.version_id.clone(),
            });
        }
    }
    Ok(out.into_iter().collect())
}

async fn load_directory_descendants(
    backend: &dyn LixBackend,
    version_id: &str,
    root_id: &str,
    read_rewrite_session: &mut ReadRewriteSession,
) -> Result<Vec<String>, LixError> {
    let sql = "WITH RECURSIVE directory_rows AS (\
         SELECT \
           lix_json_extract(snapshot_content, 'id') AS id, \
           lix_json_extract(snapshot_content, 'parent_id') AS parent_id, \
           version_id \
         FROM lix_state_by_version \
         WHERE schema_key = 'lix_directory_descriptor' \
           AND snapshot_content IS NOT NULL\
         ), \
         descendants(id) AS (\
           SELECT id FROM directory_rows \
           WHERE version_id = $1 AND id = $2 \
           UNION ALL \
           SELECT child.id \
           FROM directory_rows child \
           JOIN descendants parent ON child.parent_id = parent.id \
           WHERE child.version_id = $1\
         ) \
         SELECT id FROM descendants";
    let query_params = vec![
        EngineValue::Text(version_id.to_string()),
        EngineValue::Text(root_id.to_string()),
    ];
    let rewritten_sql =
        rewrite_single_read_query_for_backend(backend, sql, &query_params, read_rewrite_session)
            .await?;
    let result = backend.execute(&rewritten_sql, &query_params).await?;
    let mut ids = Vec::new();
    for row in result.rows {
        if let Some(EngineValue::Text(id)) = row.first() {
            ids.push(id.clone());
        }
    }
    ids.sort();
    ids.dedup();
    Ok(ids)
}

fn build_directory_delete_selection(
    rows: &[ScopedDirectoryId],
    target: FilesystemTarget,
) -> Result<String, LixError> {
    if rows.is_empty() {
        return Err(LixError {
            message: "directory delete selection expansion returned empty result".to_string(),
        });
    }
    if target.uses_active_version_scope() {
        let ids = rows
            .iter()
            .map(|row| format!("'{}'", escape_sql_string(&row.id)))
            .collect::<Vec<_>>()
            .join(", ");
        return Ok(format!("id IN ({ids})"));
    }

    let clauses = rows
        .iter()
        .map(|row| {
            format!(
                "(id = '{id}' AND lixcol_version_id = '{version_id}')",
                id = escape_sql_string(&row.id),
                version_id = escape_sql_string(&row.version_id),
            )
        })
        .collect::<Vec<_>>();
    Ok(clauses.join(" OR "))
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
    use super::{
        extract_predicate_string_with_params_and_state, json_text_expr_sql, load_version_chain_ids,
        parse_exact_file_descriptor_lookup_rows, parse_expression, rewrite_delete, rewrite_insert,
        rewrite_update, select_effective_entity_tombstone_state, ReadRewriteSession,
    };
    use crate::engine::sql::ast::utils::{
        parse_sql_statements, resolve_expr_cell_with_state, PlaceholderState,
    };
    use crate::{LixBackend, LixError, LixTransaction, QueryResult, SqlDialect, Value};
    use sqlparser::ast::Statement;
    use std::sync::Mutex;

    #[test]
    fn json_text_expr_sql_uses_backend_specific_expression() {
        assert_eq!(
            json_text_expr_sql(SqlDialect::Sqlite, "inherits_from_version_id"),
            "json_extract(snapshot_content, '$.\"inherits_from_version_id\"')"
        );
        assert_eq!(
            json_text_expr_sql(SqlDialect::Postgres, "inherits_from_version_id"),
            "jsonb_extract_path_text(CAST(snapshot_content AS JSONB), 'inherits_from_version_id')"
        );
    }

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
    fn rewrites_data_only_update_to_noop_main_statement() {
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
    fn rejects_file_id_update() {
        let sql = "UPDATE lix_file SET id = 'f2' WHERE id = 'f1'";
        let statements = parse_sql_statements(sql).expect("parse");
        let update = match statements.into_iter().next().expect("statement") {
            Statement::Update(update) => update,
            _ => panic!("expected update"),
        };

        let err = rewrite_update(update).expect_err("id update should fail");
        assert!(
            err.message.contains("lix_file id is immutable"),
            "unexpected error: {}",
            err.message
        );
    }

    #[test]
    fn rejects_directory_id_update() {
        let sql = "UPDATE lix_directory SET id = 'dir-2' WHERE id = 'dir-1'";
        let statements = parse_sql_statements(sql).expect("parse");
        let update = match statements.into_iter().next().expect("statement") {
            Statement::Update(update) => update,
            _ => panic!("expected update"),
        };

        let err = rewrite_update(update).expect_err("id update should fail");
        assert!(
            err.message.contains("lix_directory id is immutable"),
            "unexpected error: {}",
            err.message
        );
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

    #[test]
    fn version_predicate_extraction_respects_existing_placeholder_offset() {
        let selection =
            parse_expression("id = 'file-1' AND lixcol_version_id = ?").expect("parse selection");
        let prior_placeholder = parse_expression("?").expect("parse placeholder expression");
        let params = vec![
            Value::Text("metadata-placeholder".to_string()),
            Value::Text("version-target".to_string()),
        ];

        let mut offset_state = PlaceholderState::new();
        resolve_expr_cell_with_state(&prior_placeholder, &params, &mut offset_state)
            .expect("consume first placeholder");

        let extracted_with_offset = extract_predicate_string_with_params_and_state(
            Some(&selection),
            &["lixcol_version_id", "version_id"],
            &params,
            &mut offset_state,
            SqlDialect::Sqlite,
        )
        .expect("extract with offset");
        assert_eq!(extracted_with_offset.as_deref(), Some("version-target"));

        let mut fresh_state = PlaceholderState::new();
        let extracted_from_fresh = extract_predicate_string_with_params_and_state(
            Some(&selection),
            &["lixcol_version_id", "version_id"],
            &params,
            &mut fresh_state,
            SqlDialect::Sqlite,
        )
        .expect("extract with fresh state");
        assert_eq!(
            extracted_from_fresh.as_deref(),
            Some("metadata-placeholder")
        );
    }

    #[test]
    fn version_predicate_extraction_skips_unrelated_equality_placeholders() {
        let selection =
            parse_expression("id = ? AND lixcol_version_id = ?").expect("parse selection");
        let params = vec![
            Value::Text("file-placeholder".to_string()),
            Value::Text("version-target".to_string()),
        ];

        let mut state = PlaceholderState::new();
        let extracted = extract_predicate_string_with_params_and_state(
            Some(&selection),
            &["lixcol_version_id", "version_id"],
            &params,
            &mut state,
            SqlDialect::Sqlite,
        )
        .expect("extract version predicate");
        assert_eq!(extracted.as_deref(), Some("version-target"));
    }

    #[test]
    fn version_predicate_extraction_skips_unrelated_in_list_placeholders() {
        let selection =
            parse_expression("id IN (?, ?) AND lixcol_version_id = ?").expect("parse selection");
        let params = vec![
            Value::Text("file-a".to_string()),
            Value::Text("file-b".to_string()),
            Value::Text("version-target".to_string()),
        ];

        let mut state = PlaceholderState::new();
        let extracted = extract_predicate_string_with_params_and_state(
            Some(&selection),
            &["lixcol_version_id", "version_id"],
            &params,
            &mut state,
            SqlDialect::Sqlite,
        )
        .expect("extract version predicate");
        assert_eq!(extracted.as_deref(), Some("version-target"));
    }

    #[test]
    fn exact_file_descriptor_lookup_treats_untracked_tombstone_as_no_match() {
        let rows = vec![vec![
            Value::Text("file-a".to_string()),
            Value::Integer(1),
            Value::Null,
        ]];
        let parsed = parse_exact_file_descriptor_lookup_rows(rows).expect("parse rows");
        assert!(
            parsed.is_some(),
            "exact lookup should short-circuit fallback"
        );
        assert!(
            parsed.expect("result").is_empty(),
            "tombstoned row should not be treated as live match"
        );
    }

    #[test]
    fn exact_file_descriptor_lookup_keeps_live_untracked_row() {
        let rows = vec![vec![
            Value::Text("file-b".to_string()),
            Value::Integer(1),
            Value::Text("{\"path\":\"/bench/file-b.txt\"}".to_string()),
        ]];
        let parsed = parse_exact_file_descriptor_lookup_rows(rows).expect("parse rows");
        let parsed = parsed.expect("live row should exist");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].id, "file-b");
        assert!(parsed[0].untracked);
    }

    #[test]
    fn effective_entity_tombstone_state_prefers_newer_tombstone() {
        let rows = vec![
            vec![
                Value::Text("v1".to_string()),
                Value::Integer(0),
                Value::Integer(0),
            ],
            vec![
                Value::Text("v2".to_string()),
                Value::Integer(0),
                Value::Integer(1),
            ],
        ];
        let chain = vec!["v2".to_string(), "v1".to_string()];
        let tombstone =
            select_effective_entity_tombstone_state(&rows, &chain).expect("select state");
        assert_eq!(tombstone, Some(true));
    }

    #[test]
    fn effective_entity_tombstone_state_respects_untracked_overlay_priority() {
        let rows = vec![
            vec![
                Value::Text("v2".to_string()),
                Value::Integer(0),
                Value::Integer(1),
            ],
            vec![
                Value::Text("v2".to_string()),
                Value::Integer(1),
                Value::Integer(0),
            ],
        ];
        let chain = vec!["v2".to_string(), "v1".to_string()];
        let tombstone =
            select_effective_entity_tombstone_state(&rows, &chain).expect("select state");
        assert_eq!(tombstone, Some(false));
    }

    #[test]
    fn effective_entity_tombstone_state_tie_does_not_use_tombstone_as_tiebreaker() {
        let rows = vec![
            vec![
                Value::Text("v2".to_string()),
                Value::Integer(1),
                Value::Integer(1),
            ],
            vec![
                Value::Text("v2".to_string()),
                Value::Integer(1),
                Value::Integer(0),
            ],
        ];
        let chain = vec!["v2".to_string(), "v1".to_string()];
        let tombstone =
            select_effective_entity_tombstone_state(&rows, &chain).expect("select state");
        assert_eq!(tombstone, Some(true));
    }

    struct VersionChainBackendMock {
        execute_calls: Mutex<usize>,
    }

    impl VersionChainBackendMock {
        fn execute_calls(&self) -> usize {
            *self
                .execute_calls
                .lock()
                .expect("version chain execute_calls mutex poisoned")
        }
    }

    #[async_trait::async_trait(?Send)]
    impl LixBackend for VersionChainBackendMock {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, _sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
            let mut calls = self
                .execute_calls
                .lock()
                .expect("version chain execute_calls mutex poisoned");
            *calls += 1;

            let requested_version = match params.first() {
                Some(Value::Text(version_id)) => version_id.clone(),
                _ => "unknown".to_string(),
            };
            let parent_version = format!("{requested_version}-parent");

            Ok(QueryResult {
                rows: vec![
                    vec![Value::Text(requested_version)],
                    vec![Value::Text(parent_version)],
                ],
                columns: vec!["version_id".to_string()],
            })
        }

        async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
            Err(LixError {
                message: "transactions are not used in this unit test".to_string(),
            })
        }
    }

    #[tokio::test]
    async fn version_chain_lookup_uses_session_cache_for_repeated_version() {
        let backend = VersionChainBackendMock {
            execute_calls: Mutex::new(0),
        };
        let mut session = ReadRewriteSession::default();

        let first = load_version_chain_ids(&backend, "v-main", &mut session)
            .await
            .expect("first lookup should succeed");
        let second = load_version_chain_ids(&backend, "v-main", &mut session)
            .await
            .expect("cached lookup should succeed");

        assert_eq!(first, second);
        assert_eq!(
            backend.execute_calls(),
            1,
            "repeated lookup should reuse session cache"
        );
    }
}
