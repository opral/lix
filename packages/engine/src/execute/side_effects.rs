use super::super::*;
use super::*;
use serde::Deserialize;

impl Engine {
    pub(crate) async fn maybe_materialize_reads_with_backend_from_statements(
        &self,
        backend: &dyn LixBackend,
        statements: &[Statement],
        active_version_id: &str,
    ) -> Result<(), LixError> {
        let Some(runtime) = self.wasm_runtime.as_ref() else {
            return Ok(());
        };

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
                runtime.as_ref(),
                versions.as_ref(),
            )
            .await?;
        }
        if file_history_read_materialization_required_for_statements(statements) {
            crate::plugin::runtime::materialize_missing_file_history_data_with_plugins(
                backend,
                runtime.as_ref(),
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
        let Some(runtime) = self.wasm_runtime.as_ref() else {
            return Ok(vec![Vec::new(); writes_by_statement.len()]);
        };
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
                    before_data: write.before_data.clone(),
                    after_data: write.after_data.clone(),
                })
                .collect::<Vec<_>>();
            let detected = crate::plugin::runtime::detect_file_changes_with_plugins_with_cache(
                backend,
                runtime.as_ref(),
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
            let blob_hash = crate::plugin::runtime::binary_blob_hash_hex(&write.after_data);
            let size_bytes = i64::try_from(write.after_data.len()).map_err(|_| LixError {
                message: format!(
                    "binary blob size exceeds supported range for file '{}' version '{}'",
                    write.file_id, write.version_id
                ),
            })?;
            let now = crate::functions::timestamp::timestamp();
            self.backend
                .execute(
                    "INSERT INTO lix_internal_binary_blob_store (blob_hash, data, size_bytes, created_at) \
                     VALUES ($1, $2, $3, $4) \
                     ON CONFLICT (blob_hash) DO NOTHING",
                    &[
                        Value::Text(blob_hash.clone()),
                        Value::Blob(write.after_data.clone()),
                        Value::Integer(size_bytes),
                        Value::Text(now.clone()),
                    ],
                )
                .await?;
            self.backend
                .execute(
                    "INSERT INTO lix_internal_binary_file_version_ref (file_id, version_id, blob_hash, size_bytes, updated_at) \
                     VALUES ($1, $2, $3, $4, $5) \
                     ON CONFLICT (file_id, version_id) DO UPDATE SET \
                     blob_hash = EXCLUDED.blob_hash, \
                     size_bytes = EXCLUDED.size_bytes, \
                     updated_at = EXCLUDED.updated_at",
                    &[
                        Value::Text(write.file_id.clone()),
                        Value::Text(write.version_id.clone()),
                        Value::Text(blob_hash),
                        Value::Integer(size_bytes),
                        Value::Text(now),
                    ],
                )
                .await?;
            self.backend
                .execute(
                    "INSERT INTO lix_internal_file_data_cache (file_id, version_id, data) \
                     VALUES ($1, $2, $3) \
                     ON CONFLICT (file_id, version_id) DO UPDATE SET \
                     data = EXCLUDED.data",
                    &[
                        Value::Text(write.file_id.clone()),
                        Value::Text(write.version_id.clone()),
                        Value::Blob(write.after_data.clone()),
                    ],
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
            let blob_hash = crate::plugin::runtime::binary_blob_hash_hex(&write.after_data);
            let size_bytes = i64::try_from(write.after_data.len()).map_err(|_| LixError {
                message: format!(
                    "binary blob size exceeds supported range for file '{}' version '{}'",
                    write.file_id, write.version_id
                ),
            })?;
            let now = crate::functions::timestamp::timestamp();
            transaction
                .execute(
                    "INSERT INTO lix_internal_binary_blob_store (blob_hash, data, size_bytes, created_at) \
                     VALUES ($1, $2, $3, $4) \
                     ON CONFLICT (blob_hash) DO NOTHING",
                    &[
                        Value::Text(blob_hash.clone()),
                        Value::Blob(write.after_data.clone()),
                        Value::Integer(size_bytes),
                        Value::Text(now.clone()),
                    ],
                )
                .await?;
            transaction
                .execute(
                    "INSERT INTO lix_internal_binary_file_version_ref (file_id, version_id, blob_hash, size_bytes, updated_at) \
                     VALUES ($1, $2, $3, $4, $5) \
                     ON CONFLICT (file_id, version_id) DO UPDATE SET \
                     blob_hash = EXCLUDED.blob_hash, \
                     size_bytes = EXCLUDED.size_bytes, \
                     updated_at = EXCLUDED.updated_at",
                    &[
                        Value::Text(write.file_id.clone()),
                        Value::Text(write.version_id.clone()),
                        Value::Text(blob_hash),
                        Value::Integer(size_bytes),
                        Value::Text(now),
                    ],
                )
                .await?;
            transaction
                .execute(
                    "INSERT INTO lix_internal_file_data_cache (file_id, version_id, data) \
                     VALUES ($1, $2, $3) \
                     ON CONFLICT (file_id, version_id) DO UPDATE SET \
                     data = EXCLUDED.data",
                    &[
                        Value::Text(write.file_id.clone()),
                        Value::Text(write.version_id.clone()),
                        Value::Blob(write.after_data.clone()),
                    ],
                )
                .await?;
        }

        Ok(())
    }

    pub(crate) async fn persist_pending_file_path_updates(
        &self,
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
            self.backend
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

            let size_bytes = i64::try_from(data.len()).map_err(|_| LixError {
                message: format!(
                    "builtin binary fallback: blob size exceeds supported range for file '{}' version '{}'",
                    file_id, version_id
                ),
            })?;
            let now = crate::functions::timestamp::timestamp();
            self.backend
                .execute(
                    "INSERT INTO lix_internal_binary_blob_store (blob_hash, data, size_bytes, created_at) \
                     VALUES ($1, $2, $3, $4) \
                     ON CONFLICT (blob_hash) DO NOTHING",
                    &[
                        Value::Text(snapshot.blob_hash.clone()),
                        Value::Blob(data),
                        Value::Integer(size_bytes),
                        Value::Text(now.clone()),
                    ],
                )
                .await?;
            self.backend
                .execute(
                    "INSERT INTO lix_internal_binary_file_version_ref (file_id, version_id, blob_hash, size_bytes, updated_at) \
                     VALUES ($1, $2, $3, $4, $5) \
                     ON CONFLICT (file_id, version_id) DO UPDATE SET \
                     blob_hash = EXCLUDED.blob_hash, \
                     size_bytes = EXCLUDED.size_bytes, \
                     updated_at = EXCLUDED.updated_at",
                    &[
                        Value::Text(file_id.clone()),
                        Value::Text(version_id.clone()),
                        Value::Text(snapshot.blob_hash),
                        Value::Integer(size_bytes),
                        Value::Text(now),
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

            let size_bytes = i64::try_from(data.len()).map_err(|_| LixError {
                message: format!(
                    "builtin binary fallback: blob size exceeds supported range for file '{}' version '{}'",
                    file_id, version_id
                ),
            })?;
            let now = crate::functions::timestamp::timestamp();
            transaction
                .execute(
                    "INSERT INTO lix_internal_binary_blob_store (blob_hash, data, size_bytes, created_at) \
                     VALUES ($1, $2, $3, $4) \
                     ON CONFLICT (blob_hash) DO NOTHING",
                    &[
                        Value::Text(snapshot.blob_hash.clone()),
                        Value::Blob(data),
                        Value::Integer(size_bytes),
                        Value::Text(now.clone()),
                    ],
                )
                .await?;
            transaction
                .execute(
                    "INSERT INTO lix_internal_binary_file_version_ref (file_id, version_id, blob_hash, size_bytes, updated_at) \
                     VALUES ($1, $2, $3, $4, $5) \
                     ON CONFLICT (file_id, version_id) DO UPDATE SET \
                     blob_hash = EXCLUDED.blob_hash, \
                     size_bytes = EXCLUDED.size_bytes, \
                     updated_at = EXCLUDED.updated_at",
                    &[
                        Value::Text(file_id.clone()),
                        Value::Text(version_id.clone()),
                        Value::Text(snapshot.blob_hash),
                        Value::Integer(size_bytes),
                        Value::Text(now),
                    ],
                )
                .await?;
        }

        Ok(())
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

            self.backend
                .execute(
                    &format!(
                        "DELETE FROM lix_internal_file_data_cache \
                         WHERE {}",
                        predicates.join(" OR ")
                    ),
                    &params,
                )
                .await?;
            self.backend
                .execute(
                    &format!(
                        "DELETE FROM lix_internal_binary_file_version_ref \
                         WHERE {}",
                        predicates.join(" OR ")
                    ),
                    &params,
                )
                .await?;
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
            transaction
                .execute(
                    &format!(
                        "DELETE FROM lix_internal_binary_file_version_ref \
                         WHERE {}",
                        predicates.join(" OR ")
                    ),
                    &params,
                )
                .await?;
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

            self.backend
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

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BuiltinBinaryBlobRefSnapshot {
    id: String,
    blob_hash: String,
    size_bytes: u64,
}

fn parse_builtin_binary_blob_ref_snapshot(
    raw: &str,
) -> Result<BuiltinBinaryBlobRefSnapshot, LixError> {
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
) -> Result<Option<BuiltinBinaryBlobRefSnapshot>, LixError> {
    let result = backend
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

async fn load_builtin_binary_blob_ref_snapshot_for_target_in_transaction(
    transaction: &mut dyn LixTransaction,
    file_id: &str,
    version_id: &str,
) -> Result<Option<BuiltinBinaryBlobRefSnapshot>, LixError> {
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

async fn binary_blob_exists(backend: &dyn LixBackend, blob_hash: &str) -> Result<bool, LixError> {
    let result = backend
        .execute(
            "SELECT 1 \
             FROM lix_internal_binary_blob_store \
             WHERE blob_hash = $1 \
             LIMIT 1",
            &[Value::Text(blob_hash.to_string())],
        )
        .await?;
    Ok(!result.rows.is_empty())
}

async fn binary_blob_exists_in_transaction(
    transaction: &mut dyn LixTransaction,
    blob_hash: &str,
) -> Result<bool, LixError> {
    let result = transaction
        .execute(
            "SELECT 1 \
             FROM lix_internal_binary_blob_store \
             WHERE blob_hash = $1 \
             LIMIT 1",
            &[Value::Text(blob_hash.to_string())],
        )
        .await?;
    Ok(!result.rows.is_empty())
}

async fn load_file_cache_blob(
    backend: &dyn LixBackend,
    file_id: &str,
    version_id: &str,
) -> Result<Option<Vec<u8>>, LixError> {
    let result = backend
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
