use super::{
    collapse_pending_file_writes_for_transaction, dedupe_filesystem_payload_domain_changes,
    should_run_binary_cas_gc, CollectedExecutionSideEffects, DeferredTransactionSideEffects,
    Engine, TransactionBackendAdapter,
};
use crate::engine::query_history::plugin_inputs as history_plugin_inputs;
use crate::query_runtime::execute_prepared::execute_prepared_with_transaction;
use crate::query_runtime::preprocess::preprocess_sql_to_plan;
use crate::engine::query_storage::queries::{
    filesystem as filesystem_queries, history as history_queries, state as state_queries,
};
use crate::engine::query_storage::tables;
use crate::query_runtime::contracts::effects::FilesystemPayloadDomainChange;
use crate::{ExecuteOptions, LixBackend, LixError, LixTransaction, QueryResult, SqlDialect, Value};
use sqlparser::ast::Statement;
use std::collections::{BTreeMap, BTreeSet};

const INTERNAL_FILESYSTEM_PLUGIN_KEY: &str = "lix";
const BINARY_BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const BINARY_BLOB_REF_SCHEMA_VERSION: &str = "1";

async fn resolve_pending_write_file_id_with_backend(
    backend: &dyn LixBackend,
    write: &crate::filesystem::pending_file_writes::PendingFileWrite,
) -> Result<String, LixError> {
    let Some(path) =
        crate::filesystem::pending_file_writes::unresolved_auto_file_path_from_id(&write.file_id)
    else {
        return Ok(write.file_id.clone());
    };
    let resolved = crate::filesystem::live_projection::resolve_file_id_by_path_in_version(
        backend,
        &write.version_id,
        path,
    )
    .await?;
    let Some(file_id) = resolved else {
        return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
                "pending file write: unable to resolve auto-generated file id for path '{}' in version '{}'",
                path, write.version_id
            ),
        });
    };
    Ok(file_id)
}

async fn resolve_pending_write_file_id_in_transaction(
    transaction: &mut dyn LixTransaction,
    write: &crate::filesystem::pending_file_writes::PendingFileWrite,
) -> Result<String, LixError> {
    let Some(path) =
        crate::filesystem::pending_file_writes::unresolved_auto_file_path_from_id(&write.file_id)
    else {
        return Ok(write.file_id.clone());
    };
    let resolved = {
        let backend = TransactionBackendAdapter::new(transaction);
        crate::filesystem::live_projection::resolve_file_id_by_path_in_version(
            &backend,
            &write.version_id,
            path,
        )
        .await?
    };
    let Some(file_id) = resolved else {
        return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
                "pending file write: unable to resolve auto-generated file id for path '{}' in version '{}'",
                path, write.version_id
            ),
        });
    };
    Ok(file_id)
}

fn binary_blob_ref_change_for_bytes(
    file_id: &str,
    version_id: &str,
    data: &[u8],
    writer_key: Option<&str>,
) -> Result<FilesystemPayloadDomainChange, LixError> {
    let size_bytes = u64::try_from(data.len()).map_err(|_| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "binary blob size exceeds supported range for file '{}' version '{}'",
            file_id, version_id
        ),
    })?;
    let snapshot_content = serde_json::json!({
        "id": file_id,
        "blob_hash": crate::plugin::runtime::binary_blob_hash_hex(data),
        "size_bytes": size_bytes,
    })
    .to_string();
    Ok(FilesystemPayloadDomainChange {
        entity_id: file_id.to_string(),
        schema_key: BINARY_BLOB_REF_SCHEMA_KEY.to_string(),
        schema_version: BINARY_BLOB_REF_SCHEMA_VERSION.to_string(),
        file_id: file_id.to_string(),
        version_id: version_id.to_string(),
        plugin_key: INTERNAL_FILESYSTEM_PLUGIN_KEY.to_string(),
        snapshot_content: Some(snapshot_content),
        metadata: None,
        writer_key: writer_key.map(ToString::to_string),
    })
}

fn binary_blob_ref_tombstone_change_for_target(
    file_id: &str,
    version_id: &str,
    writer_key: Option<&str>,
) -> FilesystemPayloadDomainChange {
    FilesystemPayloadDomainChange {
        entity_id: file_id.to_string(),
        schema_key: BINARY_BLOB_REF_SCHEMA_KEY.to_string(),
        schema_version: BINARY_BLOB_REF_SCHEMA_VERSION.to_string(),
        file_id: file_id.to_string(),
        version_id: version_id.to_string(),
        plugin_key: INTERNAL_FILESYSTEM_PLUGIN_KEY.to_string(),
        snapshot_content: None,
        metadata: None,
        writer_key: writer_key.map(ToString::to_string),
    }
}

impl Engine {
    pub(crate) async fn maybe_materialize_reads_with_backend_from_statements(
        &self,
        backend: &dyn LixBackend,
        statements: &[Statement],
        active_version_id: &str,
    ) -> Result<(), LixError> {
        let _ = backend;
        let _ = active_version_id;
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

    pub(crate) async fn collect_execution_side_effects_with_backend_from_statements(
        &self,
        backend: &dyn LixBackend,
        statements: &[Statement],
        params: &[Value],
        active_version_id: &str,
        _writer_key: Option<&str>,
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
                code: error.code,
                description: format!(
                    "pending file writes collection failed: {}",
                    error.description
                ),
            })?;
        let crate::filesystem::pending_file_writes::PendingFileWriteCollection {
            writes: pending_file_writes,
            writes_by_statement: _pending_file_writes_by_statement,
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
                code: error.code,
                description: format!(
                    "pending file delete collection failed: {}",
                    error.description
                ),
            })?;

        Ok(CollectedExecutionSideEffects {
            pending_file_writes,
            pending_file_delete_targets,
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

        let filesystem_payload_domain_changes = dedupe_filesystem_payload_domain_changes(
            &self
                .collect_live_filesystem_payload_domain_changes_in_transaction(
                    transaction,
                    &side_effects.pending_file_writes,
                    &side_effects.pending_file_delete_targets,
                    writer_key,
                )
                .await?,
        );
        let should_run_binary_gc =
            should_run_binary_cas_gc(&[], &filesystem_payload_domain_changes);
        let _ = std::mem::take(&mut side_effects.pending_file_delete_targets);

        self.persist_pending_file_data_updates_in_transaction(
            transaction,
            &side_effects.pending_file_writes,
        )
        .await?;
        if !filesystem_payload_domain_changes.is_empty() {
            self.persist_filesystem_payload_domain_changes_in_transaction(
                transaction,
                &filesystem_payload_domain_changes,
            )
            .await?;
        }
        if should_run_binary_gc {
            self.garbage_collect_unreachable_binary_cas_in_transaction(transaction)
                .await?;
        }

        Ok(())
    }

    pub(crate) async fn persist_filesystem_payload_domain_changes(
        &self,
        changes: &[FilesystemPayloadDomainChange],
    ) -> Result<(), LixError> {
        self.persist_filesystem_payload_domain_changes_with_untracked(changes, false)
            .await
    }

    pub(crate) async fn persist_filesystem_payload_domain_changes_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        changes: &[FilesystemPayloadDomainChange],
    ) -> Result<(), LixError> {
        self.persist_filesystem_payload_domain_changes_with_untracked_in_transaction(
            transaction,
            changes,
            false,
        )
        .await
    }

    pub(crate) async fn persist_filesystem_payload_domain_changes_with_untracked(
        &self,
        changes: &[FilesystemPayloadDomainChange],
        untracked: bool,
    ) -> Result<(), LixError> {
        let deduped_changes = dedupe_filesystem_payload_domain_changes(changes);
        if deduped_changes.is_empty() {
            return Ok(());
        }

        let (sql, params) =
            build_filesystem_payload_domain_changes_insert(&deduped_changes, untracked);
        let mut transaction = self.backend.begin_transaction().await?;
        let mut active_version_id = self.require_active_version_id()?;
        let previous_active_version_id = active_version_id.clone();
        let mut pending_state_commit_stream_changes = Vec::new();
        let mut pending_sql2_append_session = None;
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
                &mut pending_sql2_append_session,
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

    pub(crate) async fn persist_filesystem_payload_domain_changes_with_untracked_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        changes: &[FilesystemPayloadDomainChange],
        untracked: bool,
    ) -> Result<(), LixError> {
        let deduped_changes = dedupe_filesystem_payload_domain_changes(changes);
        if deduped_changes.is_empty() {
            return Ok(());
        }

        let (sql, params) =
            build_filesystem_payload_domain_changes_insert(&deduped_changes, untracked);
        let output = {
            let backend = TransactionBackendAdapter::new(transaction);
            preprocess_sql_to_plan(&backend, &self.cel_evaluator, &sql, &params).await?
        };
        execute_prepared_with_transaction(transaction, &output.prepared_statements).await?;

        Ok(())
    }

    pub(crate) async fn collect_live_filesystem_payload_domain_changes(
        &self,
        writes: &[crate::filesystem::pending_file_writes::PendingFileWrite],
        delete_targets: &BTreeSet<(String, String)>,
        writer_key: Option<&str>,
    ) -> Result<Vec<FilesystemPayloadDomainChange>, LixError> {
        let mut latest_by_key = BTreeMap::new();

        for write in writes {
            if !write.data_is_authoritative {
                continue;
            }
            let resolved_file_id =
                resolve_pending_write_file_id_with_backend(self.backend.as_ref(), write).await?;
            let change = binary_blob_ref_change_for_bytes(
                &resolved_file_id,
                &write.version_id,
                &write.after_data,
                writer_key,
            )?;
            latest_by_key.insert((resolved_file_id, write.version_id.clone()), change);
        }

        for (file_id, version_id) in delete_targets {
            latest_by_key.insert(
                (file_id.clone(), version_id.clone()),
                binary_blob_ref_tombstone_change_for_target(file_id, version_id, writer_key),
            );
        }

        Ok(latest_by_key.into_values().collect())
    }

    pub(crate) async fn collect_live_filesystem_payload_domain_changes_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        writes: &[crate::filesystem::pending_file_writes::PendingFileWrite],
        delete_targets: &BTreeSet<(String, String)>,
        writer_key: Option<&str>,
    ) -> Result<Vec<FilesystemPayloadDomainChange>, LixError> {
        let mut latest_by_key = BTreeMap::new();

        for write in writes {
            if !write.data_is_authoritative {
                continue;
            }
            let resolved_file_id =
                resolve_pending_write_file_id_in_transaction(transaction, write).await?;
            let change = binary_blob_ref_change_for_bytes(
                &resolved_file_id,
                &write.version_id,
                &write.after_data,
                writer_key,
            )?;
            latest_by_key.insert((resolved_file_id, write.version_id.clone()), change);
        }

        for (file_id, version_id) in delete_targets {
            latest_by_key.insert(
                (file_id.clone(), version_id.clone()),
                binary_blob_ref_tombstone_change_for_target(file_id, version_id, writer_key),
            );
        }

        Ok(latest_by_key.into_values().collect())
    }

    pub(crate) async fn persist_pending_file_data_updates(
        &self,
        writes: &[crate::filesystem::pending_file_writes::PendingFileWrite],
    ) -> Result<(), LixError> {
        let mut latest_index_by_key: BTreeMap<(String, String), usize> = BTreeMap::new();
        for (index, write) in writes.iter().enumerate() {
            if !write.data_is_authoritative {
                continue;
            }
            let resolved_file_id =
                resolve_pending_write_file_id_with_backend(self.backend.as_ref(), write).await?;
            latest_index_by_key.insert((resolved_file_id, write.version_id.clone()), index);
        }

        for ((file_id, version_id), index) in &latest_index_by_key {
            let write = writes.get(*index).ok_or_else(|| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "file data write persistence failed: invalid write index {} for file '{}' version '{}'",
                    index, file_id, version_id
                ),
            })?;
            persist_binary_blob_with_fastcdc_backend(
                self.backend.as_ref(),
                file_id,
                version_id,
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
        let mut latest_index_by_key: BTreeMap<(String, String), usize> = BTreeMap::new();
        for (index, write) in writes.iter().enumerate() {
            if !write.data_is_authoritative {
                continue;
            }
            let resolved_file_id =
                resolve_pending_write_file_id_in_transaction(transaction, write).await?;
            latest_index_by_key.insert((resolved_file_id, write.version_id.clone()), index);
        }

        for ((file_id, version_id), index) in &latest_index_by_key {
            let write = writes.get(*index).ok_or_else(|| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "file data write persistence failed (tx): invalid write index {} for file '{}' version '{}'",
                    index, file_id, version_id
                ),
            })?;
            persist_binary_blob_with_fastcdc_in_transaction(
                transaction,
                file_id,
                version_id,
                &write.after_data,
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

    pub(crate) fn require_active_version_id(&self) -> Result<String, LixError> {
        let guard = self.active_version_id.read().map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "active version cache lock poisoned".to_string(),
        })?;
        guard
            .clone()
            .ok_or_else(crate::errors::not_initialized_error)
    }

    pub(crate) fn clear_active_version_id(&self) {
        let mut guard = self.active_version_id.write().unwrap();
        *guard = None;
    }

    pub(crate) fn set_active_version_id(&self, version_id: String) {
        let mut guard = self.active_version_id.write().unwrap();
        if guard.as_ref() == Some(&version_id) {
            return;
        }
        *guard = Some(version_id);
    }
}

fn build_filesystem_payload_domain_changes_insert(
    changes: &[FilesystemPayloadDomainChange],
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
            params.push(Value::Boolean(true));
        }
    }

    let sql =
        state_queries::insert_filesystem_payload_domain_changes_sql(&rows.join(", "), untracked);
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
    let upsert_blob_store_sql = filesystem_queries::upsert_binary_blob_store_sql();
    let insert_manifest_sql = filesystem_queries::insert_binary_blob_manifest_sql();
    let insert_chunk_store_sql = filesystem_queries::insert_binary_chunk_store_sql();
    let insert_manifest_chunk_sql = filesystem_queries::insert_binary_blob_manifest_chunk_sql();

    let blob_hash = crate::plugin::runtime::binary_blob_hash_hex(data);
    let size_bytes = i64::try_from(data.len()).map_err(|_| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "binary blob size exceeds supported range for file '{}' version '{}'",
            file_id, version_id
        ),
    })?;
    let chunk_ranges = fastcdc_chunk_ranges(data);
    let chunk_count = i64::try_from(chunk_ranges.len()).map_err(|_| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
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
    executor
        .execute_sql(
            &upsert_blob_store_sql,
            &[
                Value::Text(blob_hash.clone()),
                Value::Blob(data.to_vec()),
                Value::Integer(size_bytes),
                Value::Text(now.clone()),
            ],
        )
        .await?;

    for (chunk_index, (start, end)) in chunk_ranges.iter().copied().enumerate() {
        let chunk_data = data[start..end].to_vec();
        let encoded_chunk = encode_binary_chunk_payload(&chunk_data)?;
        let chunk_hash = crate::plugin::runtime::binary_blob_hash_hex(&chunk_data);
        let chunk_size = i64::try_from(chunk_data.len()).map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "binary chunk size exceeds supported range for file '{}' version '{}'",
                file_id, version_id
            ),
        })?;
        let stored_chunk_size = i64::try_from(encoded_chunk.data.len()).map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "binary stored chunk size exceeds supported range for file '{}' version '{}'",
                file_id, version_id
            ),
        })?;
        let chunk_index = i64::try_from(chunk_index).map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
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
    let _ = (file_id, version_id, blob_hash, size_bytes, now);

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
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("binary chunk compression failed: {error}"),
    })
}

#[cfg(target_arch = "wasm32")]
fn compress_binary_chunk_payload(chunk_data: &[u8]) -> Result<Vec<u8>, LixError> {
    Ok(ruzstd::encoding::compress_to_vec(
        chunk_data,
        ruzstd::encoding::CompressionLevel::Fastest,
    ))
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
