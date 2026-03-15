use crate::engine::{
    dedupe_filesystem_payload_domain_changes, CollectedExecutionSideEffects, Engine,
    TransactionBackendAdapter,
};
use crate::sql::analysis::history_reads as history_plugin_inputs;
use crate::sql::execution::contracts::effects::FilesystemPayloadDomainChange;
use crate::sql::execution::write_program_runner::execute_write_program_with_transaction;
use crate::sql::storage::queries::{
    filesystem as filesystem_queries, history as history_queries, state as state_queries,
};
use crate::sql::storage::tables;
use crate::state::internal::write_program::WriteProgram;
use crate::{LixBackend, LixError, LixTransaction, QueryResult, SqlDialect, Value};
use sqlparser::ast::Statement;
use std::collections::{BTreeMap, BTreeSet};

const INTERNAL_FILESYSTEM_PLUGIN_KEY: &str = "lix";
const BINARY_BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const BINARY_BLOB_REF_SCHEMA_VERSION: &str = "1";
const SQLITE_MAX_BIND_PARAMETERS_PER_STATEMENT: usize = 32_766;
const POSTGRES_MAX_BIND_PARAMETERS_PER_STATEMENT: usize = 65_535;

pub(crate) struct BinaryBlobWriteInput<'a> {
    pub(crate) file_id: &'a str,
    pub(crate) version_id: &'a str,
    pub(crate) data: &'a [u8],
}

#[derive(Debug, Clone)]
struct BinaryBlobManifestRow {
    blob_hash: String,
    size_bytes: i64,
    chunk_count: i64,
    created_at: String,
}

#[derive(Debug, Clone)]
struct BinaryBlobStoreRow {
    blob_hash: String,
    data: Vec<u8>,
    size_bytes: i64,
    created_at: String,
}

#[derive(Debug, Clone)]
struct BinaryChunkStoreRow {
    chunk_hash: String,
    data: Vec<u8>,
    size_bytes: i64,
    codec: String,
    codec_dict_id: Option<String>,
    created_at: String,
}

#[derive(Debug, Clone)]
struct BinaryBlobManifestChunkRow {
    blob_hash: String,
    chunk_index: i64,
    chunk_hash: String,
    chunk_size: i64,
}

#[derive(Debug, Default)]
struct TrackedFilesystemPayloadBatch {
    blob_manifest_rows: Vec<BinaryBlobManifestRow>,
    blob_store_rows: Vec<BinaryBlobStoreRow>,
    chunk_store_rows: Vec<BinaryChunkStoreRow>,
    manifest_chunk_rows: Vec<BinaryBlobManifestChunkRow>,
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
    untracked: bool,
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
        untracked,
        plugin_key: INTERNAL_FILESYSTEM_PLUGIN_KEY.to_string(),
        snapshot_content: Some(snapshot_content),
        metadata: None,
        writer_key: writer_key.map(ToString::to_string),
    })
}

fn binary_blob_ref_tombstone_change_for_target(
    file_id: &str,
    version_id: &str,
    untracked: bool,
    writer_key: Option<&str>,
) -> FilesystemPayloadDomainChange {
    FilesystemPayloadDomainChange {
        entity_id: file_id.to_string(),
        schema_key: BINARY_BLOB_REF_SCHEMA_KEY.to_string(),
        schema_version: BINARY_BLOB_REF_SCHEMA_VERSION.to_string(),
        file_id: file_id.to_string(),
        version_id: version_id.to_string(),
        untracked,
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
                self.wasm_runtime_ref(),
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

    pub(crate) async fn persist_filesystem_payload_domain_changes_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        changes: &[FilesystemPayloadDomainChange],
    ) -> Result<(), LixError> {
        self.persist_filesystem_payload_domain_changes_partitioned_in_transaction(
            transaction,
            changes,
        )
        .await
    }

    async fn persist_filesystem_payload_domain_changes_partitioned_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        changes: &[FilesystemPayloadDomainChange],
    ) -> Result<(), LixError> {
        let tracked = changes
            .iter()
            .filter(|change| !change.untracked)
            .cloned()
            .collect::<Vec<_>>();
        if !tracked.is_empty() {
            self.persist_filesystem_payload_domain_changes_with_untracked_in_transaction(
                transaction,
                &tracked,
                false,
            )
            .await?;
        }

        let untracked = changes
            .iter()
            .filter(|change| change.untracked)
            .cloned()
            .collect::<Vec<_>>();
        if !untracked.is_empty() {
            self.persist_filesystem_payload_domain_changes_with_untracked_in_transaction(
                transaction,
                &untracked,
                true,
            )
            .await?;
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
        transaction.execute(&sql, &params).await?;

        Ok(())
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
                write.untracked,
                writer_key,
            )?;
            latest_by_key.insert(
                (resolved_file_id, write.version_id.clone(), write.untracked),
                change,
            );
        }

        for (file_id, version_id) in delete_targets {
            latest_by_key.insert(
                (file_id.clone(), version_id.clone(), false),
                binary_blob_ref_tombstone_change_for_target(file_id, version_id, false, writer_key),
            );
        }

        Ok(latest_by_key.into_values().collect())
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

        let mut payloads = Vec::with_capacity(latest_index_by_key.len());
        for ((file_id, version_id), index) in &latest_index_by_key {
            let write = writes.get(*index).ok_or_else(|| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "file data write persistence failed (tx): invalid write index {} for file '{}' version '{}'",
                    index, file_id, version_id
                ),
            })?;
            payloads.push(BinaryBlobWriteInput {
                file_id,
                version_id,
                data: &write.after_data,
            });
        }
        let program = build_binary_blob_fastcdc_write_program(transaction.dialect(), &payloads)?;
        execute_write_program_with_transaction(transaction, program).await?;

        Ok(())
    }

    pub(crate) async fn garbage_collect_unreachable_binary_cas_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
    ) -> Result<(), LixError> {
        garbage_collect_unreachable_binary_cas_in_transaction(transaction).await
    }
}

pub(crate) fn build_filesystem_payload_domain_changes_insert(
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

fn build_bulk_insert_binary_blob_manifest_sql(rows: &[String]) -> String {
    format!(
        "INSERT INTO {} (blob_hash, size_bytes, chunk_count, created_at) \
         VALUES {} \
         ON CONFLICT (blob_hash) DO NOTHING",
        tables::filesystem::INTERNAL_BINARY_BLOB_MANIFEST,
        rows.join(", ")
    )
}

fn build_bulk_upsert_binary_blob_store_sql(rows: &[String]) -> String {
    format!(
        "INSERT INTO {} (blob_hash, data, size_bytes, created_at) \
         VALUES {} \
         ON CONFLICT (blob_hash) DO UPDATE SET \
         data = EXCLUDED.data, \
         size_bytes = EXCLUDED.size_bytes",
        tables::filesystem::INTERNAL_BINARY_BLOB_STORE,
        rows.join(", ")
    )
}

fn build_bulk_insert_binary_chunk_store_sql(rows: &[String]) -> String {
    format!(
        "INSERT INTO {} (chunk_hash, data, size_bytes, codec, codec_dict_id, created_at) \
         VALUES {} \
         ON CONFLICT (chunk_hash) DO NOTHING",
        tables::filesystem::INTERNAL_BINARY_CHUNK_STORE,
        rows.join(", ")
    )
}

fn build_bulk_insert_binary_blob_manifest_chunk_sql(rows: &[String]) -> String {
    format!(
        "INSERT INTO {} (blob_hash, chunk_index, chunk_hash, chunk_size) \
         VALUES {} \
         ON CONFLICT (blob_hash, chunk_index) DO NOTHING",
        tables::filesystem::INTERNAL_BINARY_BLOB_MANIFEST_CHUNK,
        rows.join(", ")
    )
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

pub(crate) fn build_binary_blob_fastcdc_write_program(
    dialect: SqlDialect,
    payloads: &[BinaryBlobWriteInput<'_>],
) -> Result<WriteProgram, LixError> {
    let batch = build_tracked_filesystem_payload_batch(payloads)?;
    let mut program = WriteProgram::new();

    push_blob_manifest_rows(&mut program, dialect, &batch.blob_manifest_rows);
    push_blob_store_rows(&mut program, dialect, &batch.blob_store_rows);
    push_chunk_store_rows(&mut program, dialect, &batch.chunk_store_rows);
    push_manifest_chunk_rows(&mut program, dialect, &batch.manifest_chunk_rows);

    Ok(program)
}

fn build_tracked_filesystem_payload_batch(
    payloads: &[BinaryBlobWriteInput<'_>],
) -> Result<TrackedFilesystemPayloadBatch, LixError> {
    let now = crate::functions::timestamp::timestamp();
    let mut manifest_rows = BTreeMap::<String, BinaryBlobManifestRow>::new();
    let mut blob_store_rows = BTreeMap::<String, BinaryBlobStoreRow>::new();
    let mut chunk_store_rows = BTreeMap::<String, BinaryChunkStoreRow>::new();
    let mut manifest_chunk_rows = BTreeMap::<(String, i64), BinaryBlobManifestChunkRow>::new();

    for payload in payloads {
        let blob_hash = crate::plugin::runtime::binary_blob_hash_hex(payload.data);
        let size_bytes = i64::try_from(payload.data.len()).map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "binary blob size exceeds supported range for file '{}' version '{}'",
                payload.file_id, payload.version_id
            ),
        })?;
        let chunk_ranges = fastcdc_chunk_ranges(payload.data);
        let chunk_count = i64::try_from(chunk_ranges.len()).map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "binary chunk count exceeds supported range for file '{}' version '{}'",
                payload.file_id, payload.version_id
            ),
        })?;

        manifest_rows
            .entry(blob_hash.clone())
            .or_insert_with(|| BinaryBlobManifestRow {
                blob_hash: blob_hash.clone(),
                size_bytes,
                chunk_count,
                created_at: now.clone(),
            });
        blob_store_rows
            .entry(blob_hash.clone())
            .or_insert_with(|| BinaryBlobStoreRow {
                blob_hash: blob_hash.clone(),
                data: payload.data.to_vec(),
                size_bytes,
                created_at: now.clone(),
            });

        for (chunk_index, (start, end)) in chunk_ranges.iter().copied().enumerate() {
            let chunk_data = payload.data[start..end].to_vec();
            let encoded_chunk = encode_binary_chunk_payload(&chunk_data)?;
            let chunk_hash = crate::plugin::runtime::binary_blob_hash_hex(&chunk_data);
            let chunk_size = i64::try_from(chunk_data.len()).map_err(|_| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "binary chunk size exceeds supported range for file '{}' version '{}'",
                    payload.file_id, payload.version_id
                ),
            })?;
            let stored_chunk_size =
                i64::try_from(encoded_chunk.data.len()).map_err(|_| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!(
                        "binary stored chunk size exceeds supported range for file '{}' version '{}'",
                        payload.file_id, payload.version_id
                    ),
                })?;
            let chunk_index = i64::try_from(chunk_index).map_err(|_| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "binary chunk index exceeds supported range for file '{}' version '{}'",
                    payload.file_id, payload.version_id
                ),
            })?;

            chunk_store_rows
                .entry(chunk_hash.clone())
                .or_insert_with(|| BinaryChunkStoreRow {
                    chunk_hash: chunk_hash.clone(),
                    data: encoded_chunk.data,
                    size_bytes: stored_chunk_size,
                    codec: encoded_chunk.codec.to_string(),
                    codec_dict_id: encoded_chunk.codec_dict_id,
                    created_at: now.clone(),
                });
            manifest_chunk_rows
                .entry((blob_hash.clone(), chunk_index))
                .or_insert_with(|| BinaryBlobManifestChunkRow {
                    blob_hash: blob_hash.clone(),
                    chunk_index,
                    chunk_hash,
                    chunk_size,
                });
        }
    }

    Ok(TrackedFilesystemPayloadBatch {
        blob_manifest_rows: manifest_rows.into_values().collect(),
        blob_store_rows: blob_store_rows.into_values().collect(),
        chunk_store_rows: chunk_store_rows.into_values().collect(),
        manifest_chunk_rows: manifest_chunk_rows.into_values().collect(),
    })
}

fn push_blob_manifest_rows(
    program: &mut WriteProgram,
    dialect: SqlDialect,
    rows: &[BinaryBlobManifestRow],
) {
    push_chunked_payload_statement(
        program,
        dialect,
        rows,
        4,
        |rows| build_bulk_insert_binary_blob_manifest_sql(rows),
        |row, params| {
            params.push(Value::Text(row.blob_hash.clone()));
            params.push(Value::Integer(row.size_bytes));
            params.push(Value::Integer(row.chunk_count));
            params.push(Value::Text(row.created_at.clone()));
        },
    );
}

fn push_blob_store_rows(
    program: &mut WriteProgram,
    dialect: SqlDialect,
    rows: &[BinaryBlobStoreRow],
) {
    push_chunked_payload_statement(
        program,
        dialect,
        rows,
        4,
        |rows| build_bulk_upsert_binary_blob_store_sql(rows),
        |row, params| {
            params.push(Value::Text(row.blob_hash.clone()));
            params.push(Value::Blob(row.data.clone()));
            params.push(Value::Integer(row.size_bytes));
            params.push(Value::Text(row.created_at.clone()));
        },
    );
}

fn push_chunk_store_rows(
    program: &mut WriteProgram,
    dialect: SqlDialect,
    rows: &[BinaryChunkStoreRow],
) {
    push_chunked_payload_statement(
        program,
        dialect,
        rows,
        6,
        |rows| build_bulk_insert_binary_chunk_store_sql(rows),
        |row, params| {
            params.push(Value::Text(row.chunk_hash.clone()));
            params.push(Value::Blob(row.data.clone()));
            params.push(Value::Integer(row.size_bytes));
            params.push(Value::Text(row.codec.clone()));
            params.push(match &row.codec_dict_id {
                Some(codec_dict_id) => Value::Text(codec_dict_id.clone()),
                None => Value::Null,
            });
            params.push(Value::Text(row.created_at.clone()));
        },
    );
}

fn push_manifest_chunk_rows(
    program: &mut WriteProgram,
    dialect: SqlDialect,
    rows: &[BinaryBlobManifestChunkRow],
) {
    push_chunked_payload_statement(
        program,
        dialect,
        rows,
        4,
        |rows| build_bulk_insert_binary_blob_manifest_chunk_sql(rows),
        |row, params| {
            params.push(Value::Text(row.blob_hash.clone()));
            params.push(Value::Integer(row.chunk_index));
            params.push(Value::Text(row.chunk_hash.clone()));
            params.push(Value::Integer(row.chunk_size));
        },
    );
}

fn push_chunked_payload_statement<Row>(
    program: &mut WriteProgram,
    dialect: SqlDialect,
    rows: &[Row],
    params_per_row: usize,
    build_sql: impl Fn(&[String]) -> String,
    mut bind_row: impl FnMut(&Row, &mut Vec<Value>),
) {
    if rows.is_empty() {
        return;
    }

    let max_rows_per_statement = max_rows_per_statement_for_dialect(dialect, params_per_row);
    for chunk in rows.chunks(max_rows_per_statement) {
        let placeholders = chunk
            .iter()
            .enumerate()
            .map(|(index, _)| values_row_placeholders_sql(index, params_per_row))
            .collect::<Vec<_>>();
        let mut params = Vec::with_capacity(chunk.len() * params_per_row);
        for row in chunk {
            bind_row(row, &mut params);
        }
        program.push_statement(build_sql(&placeholders), params);
    }
}

fn max_rows_per_statement_for_dialect(dialect: SqlDialect, params_per_row: usize) -> usize {
    let max_params = match dialect {
        SqlDialect::Sqlite => SQLITE_MAX_BIND_PARAMETERS_PER_STATEMENT,
        SqlDialect::Postgres => POSTGRES_MAX_BIND_PARAMETERS_PER_STATEMENT,
    };
    (max_params / params_per_row).max(1)
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
