use super::super::*;
use super::*;
use crate::sql::{
    bind_statement_with_state, ensure_history_timeline_materialized_for_statement_with_state,
    PlaceholderState,
};
use crate::SqlDialect;

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const UPDATE_ROW_FILE_ID_INDEX: usize = 1;
const UPDATE_ROW_VERSION_ID_INDEX: usize = 2;
const UPDATE_ROW_SNAPSHOT_CONTENT_INDEX: usize = 5;
const DELETE_ROW_FILE_ID_INDEX: usize = 1;
const DELETE_ROW_VERSION_ID_INDEX: usize = 2;

#[derive(Debug, Clone)]
enum FileDescriptorExecutionState {
    Live { path: Option<String> },
    Tombstone,
}

impl Engine {
    pub(crate) async fn maybe_materialize_reads_with_backend_from_statements(
        &self,
        backend: &dyn LixBackend,
        statements: &[Statement],
        params: &[Value],
        placeholder_state: PlaceholderState,
        active_version_id: &str,
    ) -> Result<(), LixError> {
        let mut statement_placeholder_state = placeholder_state;
        for statement in statements {
            ensure_history_timeline_materialized_for_statement_with_state(
                backend,
                statement,
                params,
                statement_placeholder_state,
            )
            .await?;
            let bound = bind_statement_with_state(
                statement.clone(),
                params,
                backend.dialect(),
                statement_placeholder_state,
            )
            .map_err(|error| LixError {
                message: format!(
                    "history timeline maintenance placeholder binding failed for '{}': {}",
                    statement, error.message
                ),
            })?;
            statement_placeholder_state = bound.state;
        }

        if let Some(scope) = file_read_materialization_scope_for_statements(statements) {
            let versions = match scope {
                FileReadMaterializationScope::ActiveVersionOnly => {
                    let mut set = BTreeSet::new();
                    set.insert(active_version_id.to_string());
                    Some(set)
                }
                FileReadMaterializationScope::AllVersions => None,
            };
            crate::plugin::runtime::materialize_missing_file_data_with_plugins(
                backend,
                self.wasm_runtime.as_ref(),
                versions.as_ref(),
            )
            .await?;
        }
        if file_history_read_materialization_required_for_statements(statements) {
            crate::plugin::runtime::materialize_missing_file_history_data_with_plugins(
                backend,
                self.wasm_runtime.as_ref(),
            )
            .await?;
        }
        Ok(())
    }

    pub(crate) async fn maybe_refresh_working_change_projection_for_read_query(
        &self,
        backend: &dyn LixBackend,
        active_version_id: &str,
    ) -> Result<(), LixError> {
        crate::sql::refresh_working_projection_for_read_query(backend, active_version_id).await
    }

    pub(crate) async fn collect_execution_side_effects_with_backend_from_mutations(
        &self,
        backend: &dyn LixBackend,
        mutations: &[crate::sql::MutationRow],
        writer_key: Option<&str>,
    ) -> Result<CollectedExecutionSideEffects, LixError> {
        let mut touched_targets = BTreeSet::<(String, String)>::new();

        for mutation in mutations {
            if mutation.untracked
                || !mutation
                    .schema_key
                    .eq_ignore_ascii_case(FILE_DESCRIPTOR_SCHEMA_KEY)
            {
                continue;
            }

            touched_targets.insert((mutation.entity_id.clone(), mutation.version_id.clone()));
        }

        if touched_targets.is_empty() {
            return Ok(CollectedExecutionSideEffects {
                pending_file_writes: Vec::new(),
                pending_file_delete_targets: BTreeSet::new(),
                detected_file_domain_changes: Vec::new(),
                untracked_filesystem_update_domain_changes: Vec::new(),
            });
        }

        let execution_state = self
            .load_file_descriptor_execution_state_for_targets(backend, &touched_targets)
            .await?;

        let keys = touched_targets.iter().cloned().collect::<Vec<_>>();
        let before_paths =
            crate::filesystem::pending_file_writes::load_before_path_from_cache_batch(
                backend, &keys,
            )
            .await?;
        let before_data =
            crate::filesystem::pending_file_writes::load_before_data_from_cache_batch(
                backend, &keys,
            )
            .await?;

        let mut pending_file_writes = Vec::with_capacity(touched_targets.len());
        let mut pending_file_delete_targets = BTreeSet::<(String, String)>::new();
        let mut ancestor_paths_by_version = BTreeMap::<String, BTreeSet<String>>::new();
        for (file_id, version_id) in touched_targets {
            let key = (file_id.clone(), version_id.clone());
            let before_data_cell = before_data.get(&key).cloned();
            let (after_path, is_delete) = match execution_state.get(&key) {
                Some(FileDescriptorExecutionState::Live { path }) => (path.clone(), false),
                Some(FileDescriptorExecutionState::Tombstone) | None => {
                    pending_file_delete_targets.insert(key.clone());
                    (None, true)
                }
            };
            if let Some(path) = after_path.clone() {
                for ancestor in crate::filesystem::path::file_ancestor_directory_paths(&path) {
                    ancestor_paths_by_version
                        .entry(version_id.clone())
                        .or_default()
                        .insert(ancestor);
                }
            }
            pending_file_writes.push(crate::filesystem::pending_file_writes::PendingFileWrite {
                file_id,
                version_id,
                before_path: before_paths.get(&key).cloned(),
                after_path,
                data_is_authoritative: is_delete,
                before_data: before_data_cell.clone(),
                after_data: if is_delete {
                    Vec::new()
                } else {
                    before_data_cell.unwrap_or_default()
                },
            });
        }

        let mut tracked_changes = Vec::new();
        for (version_id, path_set) in ancestor_paths_by_version {
            let mut ordered_paths = path_set.into_iter().collect::<Vec<_>>();
            ordered_paths.sort_by(|left, right| {
                crate::filesystem::path::path_depth(left)
                    .cmp(&crate::filesystem::path::path_depth(right))
                    .then_with(|| left.cmp(right))
            });
            let mut missing_changes =
                crate::filesystem::mutation_rewrite::tracked_missing_directory_changes_for_paths(
                    backend,
                    &version_id,
                    &ordered_paths,
                )
                .await?;
            for change in &mut missing_changes {
                change.writer_key = writer_key.map(ToString::to_string);
            }
            tracked_changes.extend(missing_changes);
        }

        Ok(CollectedExecutionSideEffects {
            pending_file_writes,
            pending_file_delete_targets,
            detected_file_domain_changes: dedupe_detected_file_domain_changes(&tracked_changes),
            untracked_filesystem_update_domain_changes: Vec::new(),
        })
    }

    async fn load_file_descriptor_execution_state_for_targets(
        &self,
        backend: &dyn LixBackend,
        targets: &BTreeSet<(String, String)>,
    ) -> Result<BTreeMap<(String, String), FileDescriptorExecutionState>, LixError> {
        if targets.is_empty() {
            return Ok(BTreeMap::new());
        }

        const TARGETS_PER_CHUNK: usize = 200;
        let keys = targets.iter().cloned().collect::<Vec<_>>();
        let mut states = BTreeMap::<(String, String), FileDescriptorExecutionState>::new();

        for chunk in keys.chunks(TARGETS_PER_CHUNK) {
            let predicates = chunk
                .iter()
                .map(|(entity_id, version_id)| {
                    format!(
                        "(entity_id = '{}' AND version_id = '{}')",
                        crate::sql::escape_sql_string(entity_id),
                        crate::sql::escape_sql_string(version_id)
                    )
                })
                .collect::<Vec<_>>();
            let sql = format!(
                "SELECT entity_id, version_id, snapshot_content \
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = '{schema_key}' \
                   AND untracked = 0 \
                   AND ({predicates}) \
                 ORDER BY entity_id, version_id, updated_at DESC",
                schema_key = FILE_DESCRIPTOR_SCHEMA_KEY,
                predicates = predicates.join(" OR "),
            );
            let rows = backend.execute(&sql, &[]).await?.rows;
            for row in rows {
                let Some(entity_id) = row.first().and_then(value_as_text_column) else {
                    return Err(LixError {
                        message:
                            "filesystem side-effect state row missing text entity_id at index 0"
                                .to_string(),
                    });
                };
                let Some(version_id) = row.get(1).and_then(value_as_text_column) else {
                    return Err(LixError {
                        message:
                            "filesystem side-effect state row missing text version_id at index 1"
                                .to_string(),
                    });
                };
                let Some(snapshot_cell) = row.get(2) else {
                    return Err(LixError {
                        message:
                            "filesystem side-effect state row missing snapshot_content at index 2"
                                .to_string(),
                    });
                };
                let key = (entity_id.clone(), version_id.clone());
                if states.contains_key(&key) {
                    continue;
                }
                let state = match value_as_optional_text_column(snapshot_cell)? {
                    Some(snapshot_content) => FileDescriptorExecutionState::Live {
                        path: extract_path_from_file_descriptor_snapshot(&snapshot_content)?,
                    },
                    None => FileDescriptorExecutionState::Tombstone,
                };
                states.insert(key, state);
            }
        }

        Ok(states)
    }

    pub(crate) async fn collect_filesystem_update_detected_file_domain_changes_from_update_rows(
        &self,
        backend: &dyn LixBackend,
        schema_key: &str,
        rows: &[Vec<Value>],
        writer_key: Option<&str>,
    ) -> Result<(Vec<DetectedFileDomainChange>, Vec<DetectedFileDomainChange>), LixError> {
        if !schema_key.eq_ignore_ascii_case(FILE_DESCRIPTOR_SCHEMA_KEY) || rows.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        let mut ancestor_paths_by_version = BTreeMap::<String, BTreeSet<String>>::new();
        for row in rows {
            let Some(version_id) = row
                .get(UPDATE_ROW_VERSION_ID_INDEX)
                .and_then(value_as_text_column)
            else {
                return Err(LixError {
                    message: "filesystem update side-effect row missing text version_id at index 2"
                        .to_string(),
                });
            };
            let Some(snapshot_cell) = row.get(UPDATE_ROW_SNAPSHOT_CONTENT_INDEX) else {
                return Err(LixError {
                    message:
                        "filesystem update side-effect row missing snapshot_content at index 5"
                            .to_string(),
                });
            };
            let Some(snapshot_content) = value_as_optional_text_column(snapshot_cell)? else {
                continue;
            };
            let Some(path) = extract_path_from_file_descriptor_snapshot(&snapshot_content)? else {
                continue;
            };
            for ancestor in crate::filesystem::path::file_ancestor_directory_paths(&path) {
                ancestor_paths_by_version
                    .entry(version_id.clone())
                    .or_default()
                    .insert(ancestor);
            }
        }

        if ancestor_paths_by_version.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        let mut tracked_changes = Vec::new();
        for (version_id, path_set) in ancestor_paths_by_version {
            let mut ordered_paths = path_set.into_iter().collect::<Vec<_>>();
            ordered_paths.sort_by(|left, right| {
                crate::filesystem::path::path_depth(left)
                    .cmp(&crate::filesystem::path::path_depth(right))
                    .then_with(|| left.cmp(right))
            });
            let mut missing_changes =
                crate::filesystem::mutation_rewrite::tracked_missing_directory_changes_for_paths(
                    backend,
                    &version_id,
                    &ordered_paths,
                )
                .await?;
            for change in &mut missing_changes {
                change.writer_key = writer_key.map(ToString::to_string);
            }
            tracked_changes.extend(missing_changes);
        }

        Ok((
            dedupe_detected_file_domain_changes(&tracked_changes),
            Vec::new(),
        ))
    }

    pub(crate) async fn collect_filesystem_update_pending_file_writes_from_update_rows(
        &self,
        backend: &dyn LixBackend,
        schema_key: &str,
        rows: &[Vec<Value>],
    ) -> Result<Vec<crate::filesystem::pending_file_writes::PendingFileWrite>, LixError> {
        if !schema_key.eq_ignore_ascii_case(FILE_DESCRIPTOR_SCHEMA_KEY) || rows.is_empty() {
            return Ok(Vec::new());
        }

        let mut writes_by_target = BTreeMap::<(String, String), Option<String>>::new();
        for row in rows {
            let Some(file_id) = row
                .get(UPDATE_ROW_FILE_ID_INDEX)
                .and_then(value_as_text_column)
            else {
                return Err(LixError {
                    message: "filesystem update side-effect row missing text file_id at index 1"
                        .to_string(),
                });
            };
            let Some(version_id) = row
                .get(UPDATE_ROW_VERSION_ID_INDEX)
                .and_then(value_as_text_column)
            else {
                return Err(LixError {
                    message: "filesystem update side-effect row missing text version_id at index 2"
                        .to_string(),
                });
            };
            let Some(snapshot_cell) = row.get(UPDATE_ROW_SNAPSHOT_CONTENT_INDEX) else {
                return Err(LixError {
                    message:
                        "filesystem update side-effect row missing snapshot_content at index 5"
                            .to_string(),
                });
            };
            let after_path = value_as_optional_text_column(snapshot_cell)?
                .map(|snapshot_content| {
                    extract_path_from_file_descriptor_snapshot(&snapshot_content)
                })
                .transpose()?
                .flatten();
            writes_by_target.insert((file_id, version_id), after_path);
        }

        if writes_by_target.is_empty() {
            return Ok(Vec::new());
        }

        let keys = writes_by_target.keys().cloned().collect::<Vec<_>>();
        let before_paths =
            crate::filesystem::pending_file_writes::load_before_path_from_cache_batch(
                backend, &keys,
            )
            .await?;
        let before_data =
            crate::filesystem::pending_file_writes::load_before_data_from_cache_batch(
                backend, &keys,
            )
            .await?;

        let mut writes = Vec::with_capacity(writes_by_target.len());
        for ((file_id, version_id), after_path) in writes_by_target {
            let before_data_cell = before_data
                .get(&(file_id.clone(), version_id.clone()))
                .cloned();
            writes.push(crate::filesystem::pending_file_writes::PendingFileWrite {
                file_id: file_id.clone(),
                version_id: version_id.clone(),
                before_path: before_paths
                    .get(&(file_id.clone(), version_id.clone()))
                    .cloned(),
                after_path,
                data_is_authoritative: false,
                before_data: before_data_cell.clone(),
                after_data: before_data_cell.unwrap_or_default(),
            });
        }

        Ok(writes)
    }

    pub(crate) async fn collect_filesystem_update_data_pending_file_writes_from_rows(
        &self,
        backend: &dyn LixBackend,
        schema_key: &str,
        file_data_assignment: Option<&crate::sql::FileDataAssignmentPlan>,
        rows: &[Vec<Value>],
    ) -> Result<Vec<crate::filesystem::pending_file_writes::PendingFileWrite>, LixError> {
        if !schema_key.eq_ignore_ascii_case(FILE_DESCRIPTOR_SCHEMA_KEY) || rows.is_empty() {
            return Ok(Vec::new());
        }

        let Some(assignment) = file_data_assignment else {
            return Ok(Vec::new());
        };

        let mut writes_by_target = BTreeMap::<(String, String), Option<String>>::new();
        for row in rows {
            let Some(file_id) = row
                .get(UPDATE_ROW_FILE_ID_INDEX)
                .and_then(value_as_text_column)
            else {
                return Err(LixError {
                    message: "filesystem update side-effect row missing text file_id at index 1"
                        .to_string(),
                });
            };
            let Some(version_id) = row
                .get(UPDATE_ROW_VERSION_ID_INDEX)
                .and_then(value_as_text_column)
            else {
                return Err(LixError {
                    message: "filesystem update side-effect row missing text version_id at index 2"
                        .to_string(),
                });
            };
            let Some(snapshot_cell) = row.get(UPDATE_ROW_SNAPSHOT_CONTENT_INDEX) else {
                return Err(LixError {
                    message:
                        "filesystem update side-effect row missing snapshot_content at index 5"
                            .to_string(),
                });
            };
            let after_path = value_as_optional_text_column(snapshot_cell)?
                .map(|snapshot_content| {
                    extract_path_from_file_descriptor_snapshot(&snapshot_content)
                })
                .transpose()?
                .flatten();
            writes_by_target.insert((file_id, version_id), after_path);
        }

        if writes_by_target.is_empty() {
            return Ok(Vec::new());
        }

        let keys = writes_by_target.keys().cloned().collect::<Vec<_>>();
        let before_paths =
            crate::filesystem::pending_file_writes::load_before_path_from_cache_batch(
                backend, &keys,
            )
            .await?;
        let before_data =
            crate::filesystem::pending_file_writes::load_before_data_from_cache_batch(
                backend, &keys,
            )
            .await?;

        let mut writes = Vec::new();
        for ((file_id, version_id), after_path) in writes_by_target {
            let after_data = match assignment {
                crate::sql::FileDataAssignmentPlan::Uniform(bytes) => Some(bytes.clone()),
                crate::sql::FileDataAssignmentPlan::ByFileId(by_file_id) => {
                    by_file_id.get(&file_id).cloned()
                }
            };
            let Some(after_data) = after_data else {
                continue;
            };
            let key = (file_id.clone(), version_id.clone());
            writes.push(crate::filesystem::pending_file_writes::PendingFileWrite {
                file_id,
                version_id,
                before_path: before_paths.get(&key).cloned(),
                after_path,
                data_is_authoritative: true,
                before_data: before_data.get(&key).cloned(),
                after_data,
            });
        }

        Ok(writes)
    }

    pub(crate) async fn collect_filesystem_delete_side_effects_from_delete_rows(
        &self,
        backend: &dyn LixBackend,
        schema_key: &str,
        rows: &[Vec<Value>],
    ) -> Result<
        (
            Vec<crate::filesystem::pending_file_writes::PendingFileWrite>,
            BTreeSet<(String, String)>,
        ),
        LixError,
    > {
        if !schema_key.eq_ignore_ascii_case(FILE_DESCRIPTOR_SCHEMA_KEY) || rows.is_empty() {
            return Ok((Vec::new(), BTreeSet::new()));
        }

        let mut targets = BTreeSet::<(String, String)>::new();
        for row in rows {
            let Some(file_id) = row
                .get(DELETE_ROW_FILE_ID_INDEX)
                .and_then(value_as_text_column)
            else {
                return Err(LixError {
                    message: "filesystem delete side-effect row missing text file_id at index 1"
                        .to_string(),
                });
            };
            let Some(version_id) = row
                .get(DELETE_ROW_VERSION_ID_INDEX)
                .and_then(value_as_text_column)
            else {
                return Err(LixError {
                    message: "filesystem delete side-effect row missing text version_id at index 2"
                        .to_string(),
                });
            };
            targets.insert((file_id, version_id));
        }

        if targets.is_empty() {
            return Ok((Vec::new(), BTreeSet::new()));
        }

        let keys = targets.iter().cloned().collect::<Vec<_>>();
        let before_paths =
            crate::filesystem::pending_file_writes::load_before_path_from_cache_batch(
                backend, &keys,
            )
            .await?;
        let before_data =
            crate::filesystem::pending_file_writes::load_before_data_from_cache_batch(
                backend, &keys,
            )
            .await?;

        let mut writes = Vec::with_capacity(keys.len());
        for (file_id, version_id) in keys {
            let before_data_cell = before_data
                .get(&(file_id.clone(), version_id.clone()))
                .cloned();
            writes.push(crate::filesystem::pending_file_writes::PendingFileWrite {
                file_id: file_id.clone(),
                version_id: version_id.clone(),
                before_path: before_paths
                    .get(&(file_id.clone(), version_id.clone()))
                    .cloned(),
                after_path: None,
                data_is_authoritative: true,
                before_data: before_data_cell,
                after_data: Vec::new(),
            });
        }

        Ok((writes, targets))
    }

    pub(crate) async fn detect_file_changes_for_pending_writes_by_statement_with_backend(
        &self,
        backend: &dyn LixBackend,
        writes_by_statement: &[Vec<crate::filesystem::pending_file_writes::PendingFileWrite>],
        allow_plugin_cache: bool,
    ) -> Result<Vec<Vec<crate::plugin::runtime::DetectedFileChange>>, LixError> {
        let installed_plugins = self
            .load_installed_plugins_with_backend(backend, allow_plugin_cache)
            .await?;

        let mut loaded_instances = {
            self.plugin_component_cache
                .lock()
                .map_err(|_| LixError {
                    message: "plugin component cache lock poisoned".to_string(),
                })?
                .clone()
        };
        let mut detected_by_statement = Vec::with_capacity(writes_by_statement.len());
        for writes in writes_by_statement {
            if writes.is_empty() {
                detected_by_statement.push(Vec::new());
                continue;
            }

            let requests = writes
                .iter()
                .map(|write| crate::plugin::runtime::FileChangeDetectionRequest {
                    file_id: write.file_id.clone(),
                    version_id: write.version_id.clone(),
                    before_path: write.before_path.clone(),
                    after_path: write.after_path.clone(),
                    data_is_authoritative: write.data_is_authoritative,
                    before_data: write.before_data.clone(),
                    after_data: write.after_data.clone(),
                })
                .collect::<Vec<_>>();
            let detected = crate::plugin::runtime::detect_file_changes_with_plugins_with_cache(
                backend,
                self.wasm_runtime.as_ref(),
                &requests,
                &installed_plugins,
                &mut loaded_instances,
            )
            .await
            .map_err(|error| LixError {
                message: format!("file detect stage failed: {}", error.message),
            })?;
            detected_by_statement.push(dedupe_detected_file_changes(&detected));
        }
        {
            let mut guard = self.plugin_component_cache.lock().map_err(|_| LixError {
                message: "plugin component cache lock poisoned".to_string(),
            })?;
            *guard = loaded_instances;
        }

        Ok(detected_by_statement)
    }

    pub(crate) async fn persist_detected_file_domain_changes_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        changes: &[DetectedFileDomainChange],
    ) -> Result<(), LixError> {
        self.persist_detected_file_domain_changes_with_untracked_in_transaction(
            transaction,
            changes,
            false,
        )
        .await
    }

    pub(crate) async fn persist_untracked_file_domain_changes_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        changes: &[DetectedFileDomainChange],
    ) -> Result<(), LixError> {
        self.persist_detected_file_domain_changes_with_untracked_in_transaction(
            transaction,
            changes,
            true,
        )
        .await
    }

    pub(crate) async fn persist_detected_file_domain_changes_with_untracked_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        changes: &[DetectedFileDomainChange],
        untracked: bool,
    ) -> Result<(), LixError> {
        let deduped_changes = dedupe_detected_file_domain_changes(changes);
        if deduped_changes.is_empty() {
            return Ok(());
        }

        let (sql, params) = if untracked {
            let mut params = Vec::with_capacity(deduped_changes.len() * 10);
            let mut rows = Vec::with_capacity(deduped_changes.len());
            for (row_index, change) in deduped_changes.iter().enumerate() {
                let base = row_index * 10;
                rows.push(format!(
                    "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
                    base + 1,
                    base + 2,
                    base + 3,
                    base + 4,
                    base + 5,
                    base + 6,
                    base + 7,
                    base + 8,
                    base + 9,
                    base + 10
                ));
                params.push(Value::Text(change.entity_id.clone()));
                params.push(Value::Text(change.schema_key.clone()));
                params.push(Value::Text(change.file_id.clone()));
                params.push(Value::Text(change.version_id.clone()));
                params.push(Value::Text(change.plugin_key.clone()));
                params.push(match &change.snapshot_content {
                    Some(snapshot_content) => Value::Text(snapshot_content.clone()),
                    None => Value::Null,
                });
                params.push(Value::Text(change.schema_version.clone()));
                params.push(match &change.metadata {
                    Some(metadata) => Value::Text(metadata.clone()),
                    None => Value::Null,
                });
                params.push(match &change.writer_key {
                    Some(writer_key) => Value::Text(writer_key.clone()),
                    None => Value::Null,
                });
                params.push(Value::Integer(1));
            }
            (
                format!(
                    "INSERT INTO lix_internal_state_vtable (\
                     entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, metadata, writer_key, untracked\
                     ) VALUES {}",
                    rows.join(", ")
                ),
                params,
            )
        } else {
            let mut params = Vec::with_capacity(deduped_changes.len() * 9);
            let mut rows = Vec::with_capacity(deduped_changes.len());
            for (row_index, change) in deduped_changes.iter().enumerate() {
                let base = row_index * 9;
                rows.push(format!(
                    "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
                    base + 1,
                    base + 2,
                    base + 3,
                    base + 4,
                    base + 5,
                    base + 6,
                    base + 7,
                    base + 8,
                    base + 9
                ));
                params.push(Value::Text(change.entity_id.clone()));
                params.push(Value::Text(change.schema_key.clone()));
                params.push(Value::Text(change.file_id.clone()));
                params.push(Value::Text(change.version_id.clone()));
                params.push(Value::Text(change.plugin_key.clone()));
                params.push(match &change.snapshot_content {
                    Some(snapshot_content) => Value::Text(snapshot_content.clone()),
                    None => Value::Null,
                });
                params.push(Value::Text(change.schema_version.clone()));
                params.push(match &change.metadata {
                    Some(metadata) => Value::Text(metadata.clone()),
                    None => Value::Null,
                });
                params.push(match &change.writer_key {
                    Some(writer_key) => Value::Text(writer_key.clone()),
                    None => Value::Null,
                });
            }
            (
                format!(
                    "INSERT INTO lix_state_by_version (\
                     entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, metadata, writer_key\
                     ) VALUES {}",
                    rows.join(", ")
                ),
                params,
            )
        };
        let output = {
            let backend = TransactionBackendAdapter::new(transaction);
            preprocess_sql(&backend, &self.cel_evaluator, &sql, &params).await?
        };
        execute_prepared_with_transaction(transaction, &output.prepared_statements).await?;

        Ok(())
    }

    pub(crate) async fn persist_pending_file_data_updates_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        writes: &[crate::filesystem::pending_file_writes::PendingFileWrite],
    ) -> Result<(), LixError> {
        let mut latest_by_key: BTreeMap<(String, String), usize> = BTreeMap::new();
        for (index, write) in writes.iter().enumerate() {
            if !write.data_is_authoritative {
                continue;
            }
            latest_by_key.insert((write.file_id.clone(), write.version_id.clone()), index);
        }

        for index in latest_by_key.into_values() {
            let write = &writes[index];
            persist_binary_blob_with_fastcdc_in_transaction(
                transaction,
                &write.file_id,
                &write.version_id,
                &write.after_data,
            )
            .await?;
        }

        Ok(())
    }

    pub(crate) async fn persist_pending_file_path_updates_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        writes: &[crate::filesystem::pending_file_writes::PendingFileWrite],
    ) -> Result<(), LixError> {
        let mut latest_by_key: BTreeMap<(String, String), usize> = BTreeMap::new();
        for (index, write) in writes.iter().enumerate() {
            latest_by_key.insert((write.file_id.clone(), write.version_id.clone()), index);
        }

        for index in latest_by_key.into_values() {
            let write = &writes[index];
            let Some(path) = write.after_path.as_deref() else {
                continue;
            };
            let Some((name, extension)) = file_name_and_extension_from_path(path) else {
                continue;
            };
            transaction
                .execute(
                    "INSERT INTO lix_internal_file_path_cache \
                     (file_id, version_id, directory_id, name, extension, path) \
                     VALUES ($1, $2, NULL, $3, $4, $5) \
                     ON CONFLICT (file_id, version_id) DO UPDATE SET \
                     directory_id = EXCLUDED.directory_id, \
                     name = EXCLUDED.name, \
                     extension = EXCLUDED.extension, \
                     path = EXCLUDED.path",
                    &[
                        Value::Text(write.file_id.clone()),
                        Value::Text(write.version_id.clone()),
                        Value::Text(name),
                        match extension {
                            Some(value) => Value::Text(value),
                            None => Value::Null,
                        },
                        Value::Text(path.to_string()),
                    ],
                )
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn ensure_builtin_binary_blob_store_for_targets_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        targets: &BTreeSet<(String, String)>,
    ) -> Result<(), LixError> {
        for (file_id, version_id) in targets {
            let snapshot = load_builtin_binary_blob_ref_snapshot_for_target_in_transaction(
                transaction,
                file_id,
                version_id,
            )
            .await?;
            let Some(snapshot) = snapshot else {
                continue;
            };

            if binary_blob_exists_in_transaction(transaction, &snapshot.blob_hash).await? {
                continue;
            }

            let data = load_file_cache_blob_in_transaction(transaction, file_id, version_id)
                .await?
                .ok_or_else(|| LixError {
                    message: format!(
                        "builtin binary fallback: missing file_data_cache bytes for file '{}' version '{}' while backfilling blob hash '{}'",
                        file_id, version_id, snapshot.blob_hash
                    ),
                })?;
            let actual_hash = crate::plugin::runtime::binary_blob_hash_hex(&data);
            if actual_hash != snapshot.blob_hash {
                return Err(LixError {
                    message: format!(
                        "builtin binary fallback: cache bytes hash mismatch for file '{}' version '{}': expected '{}' from state, got '{}'",
                        file_id, version_id, snapshot.blob_hash, actual_hash
                    ),
                });
            }
            if data.len() as u64 != snapshot.size_bytes {
                return Err(LixError {
                    message: format!(
                        "builtin binary fallback: cache bytes size mismatch for file '{}' version '{}': expected {} bytes from state, got {}",
                        file_id,
                        version_id,
                        snapshot.size_bytes,
                        data.len()
                    ),
                });
            }

            persist_binary_blob_with_fastcdc_in_transaction(
                transaction,
                file_id,
                version_id,
                &data,
            )
            .await?;
        }

        Ok(())
    }

    pub(crate) async fn garbage_collect_unreachable_binary_cas_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
    ) -> Result<(), LixError> {
        garbage_collect_unreachable_binary_cas_in_transaction(transaction).await
    }

    pub(crate) async fn invalidate_file_data_cache_entries_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        targets: &BTreeSet<(String, String)>,
    ) -> Result<(), LixError> {
        if targets.is_empty() {
            return Ok(());
        }

        const PAIRS_PER_CHUNK: usize = 200;
        let keys = targets.iter().cloned().collect::<Vec<_>>();

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

            transaction
                .execute(
                    &format!(
                        "DELETE FROM lix_internal_file_data_cache \
                         WHERE {}",
                        predicates.join(" OR ")
                    ),
                    &params,
                )
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn invalidate_file_path_cache_entries_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        targets: &BTreeSet<(String, String)>,
    ) -> Result<(), LixError> {
        if targets.is_empty() {
            return Ok(());
        }

        const PAIRS_PER_CHUNK: usize = 200;
        let keys = targets.iter().cloned().collect::<Vec<_>>();

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

            transaction
                .execute(
                    &format!(
                        "DELETE FROM lix_internal_file_path_cache \
                         WHERE {}",
                        predicates.join(" OR ")
                    ),
                    &params,
                )
                .await?;
        }
        Ok(())
    }

    pub(crate) fn set_active_version_id(&self, version_id: String) {
        let mut guard = self.active_version_id.write().unwrap();
        if *guard == version_id {
            return;
        }
        *guard = version_id;
    }
}

fn value_as_text_column(value: &Value) -> Option<String> {
    match value {
        Value::Text(value) => Some(value.clone()),
        _ => None,
    }
}

fn value_as_optional_text_column(value: &Value) -> Result<Option<String>, LixError> {
    match value {
        Value::Null => Ok(None),
        Value::Text(value) => Ok(Some(value.clone())),
        other => Err(LixError {
            message: format!(
                "filesystem update side-effect snapshot_content expected text or null, got {other:?}"
            ),
        }),
    }
}

fn extract_path_from_file_descriptor_snapshot(
    snapshot_content: &str,
) -> Result<Option<String>, LixError> {
    let parsed: serde_json::Value =
        serde_json::from_str(snapshot_content).map_err(|error| LixError {
            message: format!(
                "filesystem update side-effect snapshot_content is invalid JSON: {error}"
            ),
        })?;
    let Some(path) = parsed.get("path").and_then(serde_json::Value::as_str) else {
        return Ok(None);
    };
    let normalized = crate::filesystem::path::normalize_file_path(path)?;
    Ok(Some(normalized))
}

fn parse_builtin_binary_blob_ref_snapshot(
    raw: &str,
) -> Result<crate::plugin::runtime::BuiltinBinaryBlobRefSnapshot, LixError> {
    serde_json::from_str(raw).map_err(|error| LixError {
        message: format!(
            "builtin binary fallback: invalid lix_binary_blob_ref snapshot_content: {error}"
        ),
    })
}

async fn load_builtin_binary_blob_ref_snapshot_for_target_in_transaction(
    transaction: &mut dyn LixTransaction,
    file_id: &str,
    version_id: &str,
) -> Result<Option<crate::plugin::runtime::BuiltinBinaryBlobRefSnapshot>, LixError> {
    let result = transaction
        .execute(
            "SELECT snapshot_content \
             FROM lix_internal_state_materialized_v1_lix_binary_blob_ref \
             WHERE file_id = $1 \
               AND version_id = $2 \
               AND plugin_key = $3 \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
             ORDER BY updated_at DESC \
             LIMIT 1",
            &[
                Value::Text(file_id.to_string()),
                Value::Text(version_id.to_string()),
                Value::Text(crate::plugin::runtime::BUILTIN_BINARY_FALLBACK_PLUGIN_KEY.to_string()),
            ],
        )
        .await?;
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    let raw_snapshot = text_value_required(row, 0, "snapshot_content")?;
    let parsed = parse_builtin_binary_blob_ref_snapshot(&raw_snapshot)?;
    if parsed.id != file_id {
        return Err(LixError {
            message: format!(
                "builtin binary fallback: snapshot id '{}' does not match file_id '{}'",
                parsed.id, file_id
            ),
        });
    }
    Ok(Some(parsed))
}

async fn binary_blob_exists_in_transaction(
    transaction: &mut dyn LixTransaction,
    blob_hash: &str,
) -> Result<bool, LixError> {
    let result = transaction
        .execute(
            "SELECT 1 \
             FROM (\
                 SELECT blob_hash FROM lix_internal_binary_blob_store \
                 UNION ALL \
                 SELECT blob_hash FROM lix_internal_binary_blob_manifest\
             ) AS blobs \
             WHERE blob_hash = $1 \
             LIMIT 1",
            &[Value::Text(blob_hash.to_string())],
        )
        .await?;
    Ok(!result.rows.is_empty())
}

async fn load_file_cache_blob_in_transaction(
    transaction: &mut dyn LixTransaction,
    file_id: &str,
    version_id: &str,
) -> Result<Option<Vec<u8>>, LixError> {
    let result = transaction
        .execute(
            "SELECT data \
             FROM lix_internal_file_data_cache \
             WHERE file_id = $1 \
               AND version_id = $2 \
             LIMIT 1",
            &[
                Value::Text(file_id.to_string()),
                Value::Text(version_id.to_string()),
            ],
        )
        .await?;
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    Ok(Some(blob_value_required(row, 0, "data")?))
}

const FASTCDC_MIN_CHUNK_BYTES: usize = 16 * 1024;
const FASTCDC_AVG_CHUNK_BYTES: usize = 64 * 1024;
const FASTCDC_MAX_CHUNK_BYTES: usize = 256 * 1024;
const BINARY_CHUNK_CODEC_RAW: &str = "raw";
const BINARY_CHUNK_CODEC_ZSTD: &str = "zstd";

struct EncodedBinaryChunkPayload {
    codec: &'static str,
    codec_dict_id: Option<String>,
    data: Vec<u8>,
}

#[async_trait::async_trait(?Send)]
trait BinaryCasExecutor {
    fn dialect(&self) -> SqlDialect;
    async fn execute_sql(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError>;
    async fn binary_blob_ref_relation_exists(&mut self) -> Result<bool, LixError>;
}

struct TransactionBinaryCasExecutor<'a> {
    transaction: &'a mut dyn LixTransaction,
}

#[async_trait::async_trait(?Send)]
impl<'a> BinaryCasExecutor for TransactionBinaryCasExecutor<'a> {
    fn dialect(&self) -> SqlDialect {
        self.transaction.dialect()
    }

    async fn execute_sql(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        self.transaction.execute(sql, params).await
    }

    async fn binary_blob_ref_relation_exists(&mut self) -> Result<bool, LixError> {
        binary_blob_ref_relation_exists_in_transaction(self.transaction).await
    }
}

async fn persist_binary_blob_with_fastcdc_in_transaction(
    transaction: &mut dyn LixTransaction,
    file_id: &str,
    version_id: &str,
    data: &[u8],
) -> Result<(), LixError> {
    let mut executor = TransactionBinaryCasExecutor { transaction };
    persist_binary_blob_with_fastcdc(&mut executor, file_id, version_id, data).await
}

async fn persist_binary_blob_with_fastcdc(
    executor: &mut dyn BinaryCasExecutor,
    file_id: &str,
    version_id: &str,
    data: &[u8],
) -> Result<(), LixError> {
    let blob_hash = crate::plugin::runtime::binary_blob_hash_hex(data);
    let size_bytes = i64::try_from(data.len()).map_err(|_| LixError {
        message: format!(
            "binary blob size exceeds supported range for file '{}' version '{}'",
            file_id, version_id
        ),
    })?;
    let chunk_ranges = fastcdc_chunk_ranges(data);
    let chunk_count = i64::try_from(chunk_ranges.len()).map_err(|_| LixError {
        message: format!(
            "binary chunk count exceeds supported range for file '{}' version '{}'",
            file_id, version_id
        ),
    })?;
    let now = crate::functions::timestamp::timestamp();

    executor
        .execute_sql(
            "INSERT INTO lix_internal_binary_blob_manifest (blob_hash, size_bytes, chunk_count, created_at) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (blob_hash) DO NOTHING",
            &[
                Value::Text(blob_hash.clone()),
                Value::Integer(size_bytes),
                Value::Integer(chunk_count),
                Value::Text(now.clone()),
            ],
        )
        .await?;

    for (chunk_index, (start, end)) in chunk_ranges.iter().copied().enumerate() {
        let chunk_data = data[start..end].to_vec();
        let encoded_chunk = encode_binary_chunk_payload(&chunk_data)?;
        let chunk_hash = crate::plugin::runtime::binary_blob_hash_hex(&chunk_data);
        let chunk_size = i64::try_from(chunk_data.len()).map_err(|_| LixError {
            message: format!(
                "binary chunk size exceeds supported range for file '{}' version '{}'",
                file_id, version_id
            ),
        })?;
        let stored_chunk_size = i64::try_from(encoded_chunk.data.len()).map_err(|_| LixError {
            message: format!(
                "binary stored chunk size exceeds supported range for file '{}' version '{}'",
                file_id, version_id
            ),
        })?;
        let chunk_index = i64::try_from(chunk_index).map_err(|_| LixError {
            message: format!(
                "binary chunk index exceeds supported range for file '{}' version '{}'",
                file_id, version_id
            ),
        })?;

        executor
            .execute_sql(
                "INSERT INTO lix_internal_binary_chunk_store (chunk_hash, data, size_bytes, codec, codec_dict_id, created_at) \
                 VALUES ($1, $2, $3, $4, $5, $6) \
                 ON CONFLICT (chunk_hash) DO NOTHING",
                &[
                    Value::Text(chunk_hash.clone()),
                    Value::Blob(encoded_chunk.data),
                    Value::Integer(stored_chunk_size),
                    Value::Text(encoded_chunk.codec.to_string()),
                    match encoded_chunk.codec_dict_id {
                        Some(codec_dict_id) => Value::Text(codec_dict_id),
                        None => Value::Null,
                    },
                    Value::Text(now.clone()),
                ],
            )
            .await?;
        executor
            .execute_sql(
                "INSERT INTO lix_internal_binary_blob_manifest_chunk (blob_hash, chunk_index, chunk_hash, chunk_size) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (blob_hash, chunk_index) DO NOTHING",
                &[
                    Value::Text(blob_hash.clone()),
                    Value::Integer(chunk_index),
                    Value::Text(chunk_hash),
                    Value::Integer(chunk_size),
                ],
            )
            .await?;
    }

    executor
        .execute_sql(
            "INSERT INTO lix_internal_binary_file_version_ref (file_id, version_id, blob_hash, size_bytes, updated_at) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (file_id, version_id) DO UPDATE SET \
             blob_hash = EXCLUDED.blob_hash, \
             size_bytes = EXCLUDED.size_bytes, \
             updated_at = EXCLUDED.updated_at",
            &[
                Value::Text(file_id.to_string()),
                Value::Text(version_id.to_string()),
                Value::Text(blob_hash),
                Value::Integer(size_bytes),
                Value::Text(now),
            ],
        )
        .await?;

    Ok(())
}

fn fastcdc_chunk_ranges(data: &[u8]) -> Vec<(usize, usize)> {
    if data.is_empty() {
        return Vec::new();
    }

    fastcdc::v2020::FastCDC::new(
        data,
        FASTCDC_MIN_CHUNK_BYTES as u32,
        FASTCDC_AVG_CHUNK_BYTES as u32,
        FASTCDC_MAX_CHUNK_BYTES as u32,
    )
    .map(|chunk| {
        let start = chunk.offset as usize;
        let end = start + (chunk.length as usize);
        (start, end)
    })
    .collect()
}

fn encode_binary_chunk_payload(chunk_data: &[u8]) -> Result<EncodedBinaryChunkPayload, LixError> {
    // Phase 2: per-chunk compression with "if smaller" admission.
    let compressed = compress_binary_chunk_payload(chunk_data)?;
    if compressed.len() < chunk_data.len() {
        return Ok(EncodedBinaryChunkPayload {
            codec: BINARY_CHUNK_CODEC_ZSTD,
            codec_dict_id: None,
            data: compressed,
        });
    }

    Ok(EncodedBinaryChunkPayload {
        codec: BINARY_CHUNK_CODEC_RAW,
        codec_dict_id: None,
        data: chunk_data.to_vec(),
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn compress_binary_chunk_payload(chunk_data: &[u8]) -> Result<Vec<u8>, LixError> {
    zstd::bulk::compress(chunk_data, 3).map_err(|error| LixError {
        message: format!("binary chunk compression failed: {error}"),
    })
}

#[cfg(target_arch = "wasm32")]
fn compress_binary_chunk_payload(chunk_data: &[u8]) -> Result<Vec<u8>, LixError> {
    Ok(ruzstd::encoding::compress_to_vec(
        chunk_data,
        ruzstd::encoding::CompressionLevel::Fastest,
    ))
}

fn text_value_required(row: &[Value], index: usize, column: &str) -> Result<String, LixError> {
    match row.get(index) {
        Some(Value::Text(value)) => Ok(value.clone()),
        _ => Err(LixError {
            message: format!(
                "builtin binary fallback: expected text column '{}' at index {}",
                column, index
            ),
        }),
    }
}

fn blob_value_required(row: &[Value], index: usize, column: &str) -> Result<Vec<u8>, LixError> {
    match row.get(index) {
        Some(Value::Blob(value)) => Ok(value.clone()),
        _ => Err(LixError {
            message: format!(
                "builtin binary fallback: expected blob column '{}' at index {}",
                column, index
            ),
        }),
    }
}

async fn garbage_collect_unreachable_binary_cas_in_transaction(
    transaction: &mut dyn LixTransaction,
) -> Result<(), LixError> {
    let mut executor = TransactionBinaryCasExecutor { transaction };
    garbage_collect_unreachable_binary_cas_with_executor(&mut executor).await
}

async fn garbage_collect_unreachable_binary_cas_with_executor(
    executor: &mut dyn BinaryCasExecutor,
) -> Result<(), LixError> {
    if !executor.binary_blob_ref_relation_exists().await? {
        return Ok(());
    }

    let state_blob_hash_expr = binary_blob_hash_extract_expr_sql(executor.dialect());

    executor
        .execute_sql(
            &format!(
                "WITH referenced AS (\
                     SELECT file_id, version_id, {state_blob_hash_expr} AS blob_hash \
                     FROM lix_state_by_version \
                     WHERE schema_key = 'lix_binary_blob_ref' \
                       AND snapshot_content IS NOT NULL \
                       AND {state_blob_hash_expr} IS NOT NULL\
                 ) \
                 DELETE FROM lix_internal_binary_file_version_ref \
             WHERE NOT EXISTS (\
                 SELECT 1 \
                 FROM referenced r \
                 WHERE r.file_id = lix_internal_binary_file_version_ref.file_id \
                   AND r.version_id = lix_internal_binary_file_version_ref.version_id \
                   AND r.blob_hash = lix_internal_binary_file_version_ref.blob_hash\
             )"
            ),
            &[],
        )
        .await?;

    executor
        .execute_sql(
            &format!(
                "WITH referenced AS (\
                     SELECT DISTINCT {state_blob_hash_expr} AS blob_hash \
                     FROM lix_state_by_version \
                     WHERE schema_key = 'lix_binary_blob_ref' \
                       AND snapshot_content IS NOT NULL \
                       AND {state_blob_hash_expr} IS NOT NULL\
                 ) \
                 DELETE FROM lix_internal_binary_blob_manifest_chunk \
             WHERE NOT EXISTS (\
                 SELECT 1 \
                 FROM referenced r \
                 WHERE r.blob_hash = lix_internal_binary_blob_manifest_chunk.blob_hash\
             )"
            ),
            &[],
        )
        .await?;

    executor
        .execute_sql(
            "DELETE FROM lix_internal_binary_chunk_store \
             WHERE NOT EXISTS (\
                 SELECT 1 \
                 FROM lix_internal_binary_blob_manifest_chunk mc \
                 WHERE mc.chunk_hash = lix_internal_binary_chunk_store.chunk_hash\
             )",
            &[],
        )
        .await?;

    executor
        .execute_sql(
            &format!(
                "WITH referenced AS (\
                     SELECT DISTINCT {state_blob_hash_expr} AS blob_hash \
                     FROM lix_state_by_version \
                     WHERE schema_key = 'lix_binary_blob_ref' \
                       AND snapshot_content IS NOT NULL \
                       AND {state_blob_hash_expr} IS NOT NULL\
                 ) \
                 DELETE FROM lix_internal_binary_blob_manifest \
             WHERE NOT EXISTS (\
                 SELECT 1 \
                 FROM referenced r \
                 WHERE r.blob_hash = lix_internal_binary_blob_manifest.blob_hash\
             ) \
             AND NOT EXISTS (\
                 SELECT 1 \
                 FROM lix_internal_binary_blob_manifest_chunk mc \
                 WHERE mc.blob_hash = lix_internal_binary_blob_manifest.blob_hash\
             )"
            ),
            &[],
        )
        .await?;

    executor
        .execute_sql(
            "DELETE FROM lix_internal_binary_blob_store \
             WHERE NOT EXISTS (\
                 SELECT 1 \
                 FROM lix_internal_binary_file_version_ref r \
                 WHERE r.blob_hash = lix_internal_binary_blob_store.blob_hash\
             )",
            &[],
        )
        .await?;

    Ok(())
}

async fn binary_blob_ref_relation_exists_in_transaction(
    transaction: &mut dyn LixTransaction,
) -> Result<bool, LixError> {
    match transaction.dialect() {
        SqlDialect::Sqlite => {
            let result = transaction
                .execute(
                    "SELECT 1 \
                     FROM sqlite_master \
                     WHERE name = $1 \
                       AND type IN ('table', 'view') \
                     LIMIT 1",
                    &[Value::Text("lix_state_by_version".to_string())],
                )
                .await?;
            Ok(!result.rows.is_empty())
        }
        SqlDialect::Postgres => {
            let result = transaction
                .execute(
                    "SELECT 1 \
                     FROM pg_catalog.pg_class c \
                     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                     WHERE n.nspname = current_schema() \
                       AND c.relname = $1 \
                     LIMIT 1",
                    &[Value::Text("lix_state_by_version".to_string())],
                )
                .await?;
            Ok(!result.rows.is_empty())
        }
    }
}

fn binary_blob_hash_extract_expr_sql(dialect: SqlDialect) -> &'static str {
    match dialect {
        SqlDialect::Sqlite => "json_extract(snapshot_content, '$.blob_hash')",
        SqlDialect::Postgres => "(snapshot_content::jsonb ->> 'blob_hash')",
    }
}
