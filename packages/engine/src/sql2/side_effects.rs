use super::super::*;
use super::execution::execute_prepared::execute_prepared_with_transaction;
use super::storage::queries::{
    filesystem as filesystem_queries, history as history_queries, state as state_queries,
};
use super::storage::tables;
use super::type_bridge::from_sql_prepared_statements;
use super::{
    history::plugin_inputs as history_plugin_inputs, history::projections as history_projections,
};
use crate::sql::preprocess_sql;
use crate::SqlDialect;

impl Engine {
    pub(crate) async fn maybe_materialize_reads_with_backend_from_statements(
        &self,
        backend: &dyn LixBackend,
        statements: &[Statement],
        active_version_id: &str,
    ) -> Result<(), LixError> {
        if let Some(scope) =
            history_plugin_inputs::file_read_materialization_scope_for_statements(statements)
        {
            let versions = match scope {
                history_plugin_inputs::FileReadMaterializationScope::ActiveVersionOnly => {
                    let mut set = BTreeSet::new();
                    set.insert(active_version_id.to_string());
                    Some(set)
                }
                history_plugin_inputs::FileReadMaterializationScope::AllVersions => None,
            };
            crate::plugin::runtime::materialize_missing_file_data_with_plugins(
                backend,
                self.wasm_runtime.as_ref(),
                versions.as_ref(),
            )
            .await?;
        }
        if history_plugin_inputs::file_history_read_materialization_required_for_statements(
            statements,
        ) {
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
        history_projections::refresh_working_projection_for_read_query(backend, active_version_id)
            .await
    }

    pub(crate) async fn collect_execution_side_effects_with_backend_from_statements(
        &self,
        backend: &dyn LixBackend,
        statements: &[Statement],
        params: &[Value],
        active_version_id: &str,
        writer_key: Option<&str>,
        allow_plugin_cache: bool,
        detect_plugin_file_changes: bool,
    ) -> Result<CollectedExecutionSideEffects, LixError> {
        let pending_file_write_collection =
            crate::filesystem::pending_file_writes::collect_pending_file_writes_from_statements(
                backend,
                statements,
                params,
                active_version_id,
            )
            .await
            .map_err(|error| LixError {
                message: format!("pending file writes collection failed: {}", error.message),
            })?;
        let crate::filesystem::pending_file_writes::PendingFileWriteCollection {
            writes: pending_file_writes,
            writes_by_statement: pending_file_writes_by_statement,
        } = pending_file_write_collection;
        let pending_file_delete_targets =
            crate::filesystem::pending_file_writes::collect_pending_file_delete_targets_from_statements(
                backend,
                statements,
                params,
                active_version_id,
            )
            .await
            .map_err(|error| LixError {
                message: format!("pending file delete collection failed: {}", error.message),
            })?;

        let detected_file_changes_by_statement = if detect_plugin_file_changes {
            self.detect_file_changes_for_pending_writes_by_statement_with_backend(
                backend,
                &pending_file_writes_by_statement,
                allow_plugin_cache,
            )
            .await?
        } else {
            vec![Vec::new(); pending_file_writes_by_statement.len()]
        };
        let mut detected_file_domain_changes_by_statement = detected_file_changes_by_statement
            .into_iter()
            .map(|changes| {
                detected_file_domain_changes_from_detected_file_changes(&changes, writer_key)
            })
            .collect::<Vec<_>>();

        let filesystem_update_domain_changes =
            collect_filesystem_update_detected_file_domain_changes_from_statements(
                backend, statements, params,
            )
            .await
            .map_err(|error| LixError {
                message: format!(
                    "filesystem update side-effect detection failed: {}",
                    error.message
                ),
            })?;
        let filesystem_update_tracked_changes_by_statement = filesystem_update_domain_changes
            .tracked_changes_by_statement
            .iter()
            .map(|changes| detected_file_domain_changes_with_writer_key(changes, writer_key))
            .collect::<Vec<_>>();
        let statement_count = detected_file_domain_changes_by_statement
            .len()
            .max(filesystem_update_tracked_changes_by_statement.len());
        detected_file_domain_changes_by_statement.resize_with(statement_count, Vec::new);
        for (index, tracked_changes) in filesystem_update_tracked_changes_by_statement
            .into_iter()
            .enumerate()
        {
            detected_file_domain_changes_by_statement[index].extend(tracked_changes);
            detected_file_domain_changes_by_statement[index] = dedupe_detected_file_domain_changes(
                &detected_file_domain_changes_by_statement[index],
            );
        }
        let mut detected_file_domain_changes = detected_file_domain_changes_by_statement
            .iter()
            .flat_map(|changes| changes.iter().cloned())
            .collect::<Vec<_>>();
        detected_file_domain_changes =
            dedupe_detected_file_domain_changes(&detected_file_domain_changes);
        let untracked_filesystem_update_domain_changes =
            dedupe_detected_file_domain_changes(&detected_file_domain_changes_with_writer_key(
                &filesystem_update_domain_changes.untracked_changes,
                writer_key,
            ));

        Ok(CollectedExecutionSideEffects {
            pending_file_writes,
            pending_file_delete_targets,
            detected_file_domain_changes_by_statement,
            detected_file_domain_changes,
            untracked_filesystem_update_domain_changes,
        })
    }

    pub(crate) async fn flush_deferred_transaction_side_effects_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        side_effects: &mut DeferredTransactionSideEffects,
        writer_key: Option<&str>,
    ) -> Result<(), LixError> {
        let collapsed_writes =
            collapse_pending_file_writes_for_transaction(&side_effects.pending_file_writes);
        side_effects.pending_file_writes = collapsed_writes;

        let mut detected_file_domain_changes =
            std::mem::take(&mut side_effects.detected_file_domain_changes);
        if !side_effects.pending_file_writes.is_empty() {
            let detected_by_statement = {
                let backend = TransactionBackendAdapter::new(transaction);
                self.detect_file_changes_for_pending_writes_by_statement_with_backend(
                    &backend,
                    &[side_effects.pending_file_writes.clone()],
                    false,
                )
                .await?
            };
            if let Some(detected) = detected_by_statement.into_iter().next() {
                detected_file_domain_changes.extend(
                    detected_file_domain_changes_from_detected_file_changes(&detected, writer_key),
                );
            }
        }
        detected_file_domain_changes =
            dedupe_detected_file_domain_changes(&detected_file_domain_changes);
        let should_run_binary_gc = should_run_binary_cas_gc(&[], &detected_file_domain_changes);
        let untracked_filesystem_update_domain_changes = dedupe_detected_file_domain_changes(
            &std::mem::take(&mut side_effects.untracked_filesystem_update_domain_changes),
        );
        side_effects
            .file_cache_invalidation_targets
            .extend(std::mem::take(
                &mut side_effects.pending_file_delete_targets,
            ));

        if !detected_file_domain_changes.is_empty() {
            self.persist_detected_file_domain_changes_in_transaction(
                transaction,
                &detected_file_domain_changes,
            )
            .await?;
        }
        if !untracked_filesystem_update_domain_changes.is_empty() {
            self.persist_untracked_file_domain_changes_in_transaction(
                transaction,
                &untracked_filesystem_update_domain_changes,
            )
            .await?;
        }
        self.persist_pending_file_data_updates_in_transaction(
            transaction,
            &side_effects.pending_file_writes,
        )
        .await?;
        self.persist_pending_file_path_updates_in_transaction(
            transaction,
            &side_effects.pending_file_writes,
        )
        .await?;
        self.ensure_builtin_binary_blob_store_for_targets_in_transaction(
            transaction,
            &side_effects.file_cache_invalidation_targets,
        )
        .await?;
        if should_run_binary_gc {
            self.garbage_collect_unreachable_binary_cas_in_transaction(transaction)
                .await?;
        }
        self.invalidate_file_data_cache_entries_in_transaction(
            transaction,
            &side_effects.file_cache_invalidation_targets,
        )
        .await?;
        self.invalidate_file_path_cache_entries_in_transaction(
            transaction,
            &side_effects.file_cache_invalidation_targets,
        )
        .await?;

        Ok(())
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

    pub(crate) async fn persist_detected_file_domain_changes(
        &self,
        changes: &[DetectedFileDomainChange],
    ) -> Result<(), LixError> {
        self.persist_detected_file_domain_changes_with_untracked(changes, false)
            .await
    }

    pub(crate) async fn persist_untracked_file_domain_changes(
        &self,
        changes: &[DetectedFileDomainChange],
    ) -> Result<(), LixError> {
        self.persist_detected_file_domain_changes_with_untracked(changes, true)
            .await
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

    pub(crate) async fn persist_detected_file_domain_changes_with_untracked(
        &self,
        changes: &[DetectedFileDomainChange],
        untracked: bool,
    ) -> Result<(), LixError> {
        let deduped_changes = dedupe_detected_file_domain_changes(changes);
        if deduped_changes.is_empty() {
            return Ok(());
        }

        let (sql, params) = build_detected_file_domain_changes_insert(&deduped_changes, untracked);
        let mut transaction = self.backend.begin_transaction().await?;
        let mut active_version_id = self.active_version_id.read().unwrap().clone();
        let previous_active_version_id = active_version_id.clone();
        let mut pending_state_commit_stream_changes = Vec::new();
        let result = self
            .execute_with_options_in_transaction(
                transaction.as_mut(),
                &sql,
                &params,
                &ExecuteOptions::default(),
                &mut active_version_id,
                None,
                true,
                &mut pending_state_commit_stream_changes,
            )
            .await;
        match result {
            Ok(_) => {
                transaction.commit().await?;
                if active_version_id != previous_active_version_id {
                    self.set_active_version_id(active_version_id);
                }
                self.emit_state_commit_stream_changes(pending_state_commit_stream_changes);
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        }

        Ok(())
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

        let (sql, params) = build_detected_file_domain_changes_insert(&deduped_changes, untracked);
        let output = {
            let backend = TransactionBackendAdapter::new(transaction);
            preprocess_sql(&backend, &self.cel_evaluator, &sql, &params).await?
        };
        let prepared_statements = from_sql_prepared_statements(output.prepared_statements);
        execute_prepared_with_transaction(transaction, &prepared_statements).await?;

        Ok(())
    }

    pub(crate) async fn persist_pending_file_data_updates(
        &self,
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
            persist_binary_blob_with_fastcdc_backend(
                self.backend.as_ref(),
                &write.file_id,
                &write.version_id,
                &write.after_data,
            )
            .await?;
        }

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

    pub(crate) async fn persist_pending_file_path_updates(
        &self,
        writes: &[crate::filesystem::pending_file_writes::PendingFileWrite],
    ) -> Result<(), LixError> {
        let upsert_sql = filesystem_queries::upsert_file_path_cache_sql();
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
            self.backend
                .execute(
                    &upsert_sql,
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

    pub(crate) async fn persist_pending_file_path_updates_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        writes: &[crate::filesystem::pending_file_writes::PendingFileWrite],
    ) -> Result<(), LixError> {
        let upsert_sql = filesystem_queries::upsert_file_path_cache_sql();
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
                    &upsert_sql,
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

    pub(crate) async fn ensure_builtin_binary_blob_store_for_targets(
        &self,
        targets: &BTreeSet<(String, String)>,
    ) -> Result<(), LixError> {
        for (file_id, version_id) in targets {
            let snapshot = load_builtin_binary_blob_ref_snapshot_for_target(
                self.backend.as_ref(),
                file_id,
                version_id,
            )
            .await?;
            let Some(snapshot) = snapshot else {
                continue;
            };

            if binary_blob_exists(self.backend.as_ref(), &snapshot.blob_hash).await? {
                continue;
            }

            let data = load_file_cache_blob(self.backend.as_ref(), file_id, version_id)
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

            persist_binary_blob_with_fastcdc_backend(
                self.backend.as_ref(),
                file_id,
                version_id,
                &data,
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

    pub(crate) async fn garbage_collect_unreachable_binary_cas(&self) -> Result<(), LixError> {
        garbage_collect_unreachable_binary_cas_with_backend(self.backend.as_ref()).await
    }

    pub(crate) async fn garbage_collect_unreachable_binary_cas_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
    ) -> Result<(), LixError> {
        garbage_collect_unreachable_binary_cas_in_transaction(transaction).await
    }

    pub(crate) async fn invalidate_file_data_cache_entries(
        &self,
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
            let sql =
                filesystem_queries::delete_file_data_cache_where_sql(&predicates.join(" OR "));

            self.backend.execute(&sql, &params).await?;
        }
        Ok(())
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
            let sql =
                filesystem_queries::delete_file_data_cache_where_sql(&predicates.join(" OR "));

            transaction.execute(&sql, &params).await?;
        }
        Ok(())
    }

    pub(crate) async fn invalidate_file_path_cache_entries(
        &self,
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
            let sql =
                filesystem_queries::delete_file_path_cache_where_sql(&predicates.join(" OR "));

            self.backend.execute(&sql, &params).await?;
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
            let sql =
                filesystem_queries::delete_file_path_cache_where_sql(&predicates.join(" OR "));

            transaction.execute(&sql, &params).await?;
        }
        Ok(())
    }

    pub(crate) async fn refresh_file_data_for_versions(
        &self,
        targets: BTreeSet<(String, String)>,
    ) -> Result<(), LixError> {
        let versions = targets
            .into_iter()
            .map(|(_, version_id)| version_id)
            .collect::<BTreeSet<_>>();
        if versions.is_empty() {
            return Ok(());
        }

        self.materialize(&MaterializationRequest {
            scope: MaterializationScope::Versions(versions),
            debug: MaterializationDebugMode::Off,
            debug_row_limit: 1,
        })
        .await?;
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

fn build_detected_file_domain_changes_insert(
    changes: &[DetectedFileDomainChange],
    untracked: bool,
) -> (String, Vec<Value>) {
    let values_per_row = if untracked { 10 } else { 9 };
    let mut params = Vec::with_capacity(changes.len() * values_per_row);
    let mut rows = Vec::with_capacity(changes.len());

    for (row_index, change) in changes.iter().enumerate() {
        rows.push(values_row_placeholders_sql(row_index, values_per_row));
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
        if untracked {
            params.push(Value::Integer(1));
        }
    }

    let sql = state_queries::insert_detected_file_domain_changes_sql(&rows.join(", "), untracked);
    (sql, params)
}

fn values_row_placeholders_sql(row_index: usize, values_per_row: usize) -> String {
    let base = row_index * values_per_row;
    let placeholders = (1..=values_per_row)
        .map(|offset| format!("${}", base + offset))
        .collect::<Vec<_>>()
        .join(", ");
    format!("({placeholders})")
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

async fn load_builtin_binary_blob_ref_snapshot_for_target(
    backend: &dyn LixBackend,
    file_id: &str,
    version_id: &str,
) -> Result<Option<crate::plugin::runtime::BuiltinBinaryBlobRefSnapshot>, LixError> {
    let sql = state_queries::select_builtin_binary_blob_ref_snapshot_sql();
    let result = backend
        .execute(
            &sql,
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

async fn load_builtin_binary_blob_ref_snapshot_for_target_in_transaction(
    transaction: &mut dyn LixTransaction,
    file_id: &str,
    version_id: &str,
) -> Result<Option<crate::plugin::runtime::BuiltinBinaryBlobRefSnapshot>, LixError> {
    let sql = state_queries::select_builtin_binary_blob_ref_snapshot_sql();
    let result = transaction
        .execute(
            &sql,
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

async fn binary_blob_exists(backend: &dyn LixBackend, blob_hash: &str) -> Result<bool, LixError> {
    let sql = filesystem_queries::binary_blob_exists_sql();
    let result = backend
        .execute(&sql, &[Value::Text(blob_hash.to_string())])
        .await?;
    Ok(!result.rows.is_empty())
}

async fn binary_blob_exists_in_transaction(
    transaction: &mut dyn LixTransaction,
    blob_hash: &str,
) -> Result<bool, LixError> {
    let sql = filesystem_queries::binary_blob_exists_sql();
    let result = transaction
        .execute(&sql, &[Value::Text(blob_hash.to_string())])
        .await?;
    Ok(!result.rows.is_empty())
}

async fn load_file_cache_blob(
    backend: &dyn LixBackend,
    file_id: &str,
    version_id: &str,
) -> Result<Option<Vec<u8>>, LixError> {
    let sql = filesystem_queries::select_file_data_cache_blob_sql();
    let result = backend
        .execute(
            &sql,
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

async fn load_file_cache_blob_in_transaction(
    transaction: &mut dyn LixTransaction,
    file_id: &str,
    version_id: &str,
) -> Result<Option<Vec<u8>>, LixError> {
    let sql = filesystem_queries::select_file_data_cache_blob_sql();
    let result = transaction
        .execute(
            &sql,
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

struct BackendBinaryCasExecutor<'a> {
    backend: &'a dyn LixBackend,
}

#[async_trait::async_trait(?Send)]
impl<'a> BinaryCasExecutor for BackendBinaryCasExecutor<'a> {
    fn dialect(&self) -> SqlDialect {
        self.backend.dialect()
    }

    async fn execute_sql(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        self.backend.execute(sql, params).await
    }

    async fn binary_blob_ref_relation_exists(&mut self) -> Result<bool, LixError> {
        binary_blob_ref_relation_exists_with_backend(self.backend).await
    }
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

async fn persist_binary_blob_with_fastcdc_backend(
    backend: &dyn LixBackend,
    file_id: &str,
    version_id: &str,
    data: &[u8],
) -> Result<(), LixError> {
    let mut executor = BackendBinaryCasExecutor { backend };
    persist_binary_blob_with_fastcdc(&mut executor, file_id, version_id, data).await
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
    let insert_manifest_sql = filesystem_queries::insert_binary_blob_manifest_sql();
    let insert_chunk_store_sql = filesystem_queries::insert_binary_chunk_store_sql();
    let insert_manifest_chunk_sql = filesystem_queries::insert_binary_blob_manifest_chunk_sql();
    let upsert_file_version_ref_sql = filesystem_queries::upsert_binary_file_version_ref_sql();

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
            &insert_manifest_sql,
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
                &insert_chunk_store_sql,
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
                &insert_manifest_chunk_sql,
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
            &upsert_file_version_ref_sql,
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

async fn garbage_collect_unreachable_binary_cas_with_backend(
    backend: &dyn LixBackend,
) -> Result<(), LixError> {
    let mut executor = BackendBinaryCasExecutor { backend };
    garbage_collect_unreachable_binary_cas_with_executor(&mut executor).await
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
    let delete_unreferenced_file_ref_sql =
        history_queries::delete_unreferenced_binary_file_version_ref_sql(state_blob_hash_expr);
    let delete_unreferenced_manifest_chunk_sql =
        history_queries::delete_unreferenced_binary_blob_manifest_chunk_sql(state_blob_hash_expr);
    let delete_unreferenced_chunk_store_sql =
        filesystem_queries::delete_unreferenced_binary_chunk_store_sql();
    let delete_unreferenced_manifest_sql =
        history_queries::delete_unreferenced_binary_blob_manifest_sql(state_blob_hash_expr);
    let delete_unreferenced_blob_store_sql =
        history_queries::delete_unreferenced_binary_blob_store_sql();

    executor
        .execute_sql(&delete_unreferenced_file_ref_sql, &[])
        .await?;

    executor
        .execute_sql(&delete_unreferenced_manifest_chunk_sql, &[])
        .await?;

    executor
        .execute_sql(&delete_unreferenced_chunk_store_sql, &[])
        .await?;

    executor
        .execute_sql(&delete_unreferenced_manifest_sql, &[])
        .await?;

    executor
        .execute_sql(&delete_unreferenced_blob_store_sql, &[])
        .await?;

    Ok(())
}

async fn binary_blob_ref_relation_exists_with_backend(
    backend: &dyn LixBackend,
) -> Result<bool, LixError> {
    match backend.dialect() {
        SqlDialect::Sqlite => {
            let result = backend
                .execute(
                    "SELECT 1 \
                     FROM sqlite_master \
                     WHERE name = $1 \
                       AND type IN ('table', 'view') \
                     LIMIT 1",
                    &[Value::Text(tables::state::STATE_BY_VERSION.to_string())],
                )
                .await?;
            Ok(!result.rows.is_empty())
        }
        SqlDialect::Postgres => {
            let result = backend
                .execute(
                    "SELECT 1 \
                     FROM pg_catalog.pg_class c \
                     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                     WHERE n.nspname = current_schema() \
                       AND c.relname = $1 \
                     LIMIT 1",
                    &[Value::Text(tables::state::STATE_BY_VERSION.to_string())],
                )
                .await?;
            Ok(!result.rows.is_empty())
        }
    }
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
                    &[Value::Text(tables::state::STATE_BY_VERSION.to_string())],
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
                    &[Value::Text(tables::state::STATE_BY_VERSION.to_string())],
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
