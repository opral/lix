use crate::backend::program::WriteProgram;
use crate::backend::program_runner::execute_write_program_with_transaction;
use crate::contracts::artifacts::{FilesystemPayloadDomainChange, MutationRow, OptionalTextPatch};
use crate::engine::{dedupe_filesystem_payload_domain_changes, Engine, TransactionBackendAdapter};
use crate::filesystem::live_projection::FilesystemProjectionScope;
use crate::filesystem::queries::load_file_row_by_id_without_path;
use crate::sql::storage::queries::{
    filesystem as filesystem_queries, history as history_queries, state as state_queries,
};
use crate::sql::storage::tables;
use crate::{LixBackendTransaction, LixError, QueryResult, SqlDialect, Value};
use std::collections::{BTreeMap, BTreeSet};

const INTERNAL_FILESYSTEM_PLUGIN_KEY: &str = "lix";
pub(crate) const FILESYSTEM_DESCRIPTOR_FILE_ID: &str = "lix";
pub(crate) const FILESYSTEM_DESCRIPTOR_PLUGIN_KEY: &str = "lix";
pub(crate) const FILESYSTEM_FILE_SCHEMA_KEY: &str = "lix_file_descriptor";
pub(crate) const FILESYSTEM_FILE_SCHEMA_VERSION: &str = "1";
const BINARY_BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const BINARY_BLOB_REF_SCHEMA_VERSION: &str = "1";
const SQLITE_MAX_BIND_PARAMETERS_PER_STATEMENT: usize = 32_766;
const POSTGRES_MAX_BIND_PARAMETERS_PER_STATEMENT: usize = 65_535;

pub(crate) struct BinaryBlobWriteInput<'a> {
    pub(crate) file_id: &'a str,
    pub(crate) version_id: &'a str,
    pub(crate) data: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BinaryBlobWrite {
    pub(crate) file_id: Option<String>,
    pub(crate) auto_path: Option<String>,
    pub(crate) version_id: String,
    pub(crate) untracked: bool,
    pub(crate) data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FilesystemDescriptorState {
    pub(crate) directory_id: String,
    pub(crate) name: String,
    pub(crate) extension: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) hidden: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExactFilesystemDescriptorState {
    pub(crate) descriptor: FilesystemDescriptorState,
    pub(crate) untracked: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct FilesystemTransactionState {
    pub(crate) files: BTreeMap<(String, String), FilesystemTransactionFileState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FilesystemTransactionFileState {
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) untracked: bool,
    pub(crate) descriptor: Option<FilesystemDescriptorState>,
    pub(crate) metadata_patch: OptionalTextPatch,
    pub(crate) data: Option<Vec<u8>>,
    pub(crate) deleted: bool,
}

pub(crate) fn binary_blob_writes_from_filesystem_state(
    state: &FilesystemTransactionState,
) -> Vec<BinaryBlobWrite> {
    state
        .files
        .values()
        .filter_map(|file| {
            file.data.as_ref().map(|data| BinaryBlobWrite {
                file_id: Some(file.file_id.clone()),
                auto_path: None,
                version_id: file.version_id.clone(),
                untracked: file.untracked,
                data: data.clone(),
            })
        })
        .collect()
}

pub(crate) fn delete_targets_from_filesystem_state(
    state: &FilesystemTransactionState,
) -> BTreeSet<(String, String)> {
    state
        .files
        .values()
        .filter(|file| file.deleted)
        .map(|file| (file.file_id.clone(), file.version_id.clone()))
        .collect()
}

pub(crate) fn filesystem_transaction_state_has_binary_payloads(
    state: &FilesystemTransactionState,
) -> bool {
    state.files.values().any(|file| file.data.is_some())
}

pub(crate) fn merge_filesystem_transaction_state(
    current: &mut FilesystemTransactionState,
    next: &FilesystemTransactionState,
) {
    current.files.extend(next.files.clone());
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledFilesystemFinalization {
    pub(crate) binary_blob_writes: Vec<BinaryBlobWrite>,
    pub(crate) semantic_changes: Vec<FilesystemSemanticChange>,
    pub(crate) should_run_gc: bool,
}

impl CompiledFilesystemFinalization {
    pub(crate) fn payload_domain_changes(&self) -> Vec<FilesystemPayloadDomainChange> {
        dedupe_filesystem_payload_domain_changes(
            &self
                .semantic_changes
                .iter()
                .map(FilesystemSemanticChange::to_payload_domain_change)
                .collect::<Vec<_>>(),
        )
    }
}

pub(crate) fn compile_filesystem_finalization(
    semantic_changes: Vec<FilesystemSemanticChange>,
    binary_blob_writes: Vec<BinaryBlobWrite>,
    mutations: &[MutationRow],
) -> CompiledFilesystemFinalization {
    let payload_domain_changes = dedupe_filesystem_payload_domain_changes(
        &semantic_changes
            .iter()
            .map(FilesystemSemanticChange::to_payload_domain_change)
            .collect::<Vec<_>>(),
    );
    CompiledFilesystemFinalization {
        binary_blob_writes,
        semantic_changes,
        should_run_gc: crate::engine::should_run_binary_cas_gc(mutations, &payload_domain_changes),
    }
}

pub(crate) fn filesystem_transaction_state_needs_exact_descriptors(
    state: &FilesystemTransactionState,
) -> bool {
    state.files.values().any(|file| {
        !file.deleted
            && file.descriptor.is_none()
            && !matches!(file.metadata_patch, OptionalTextPatch::Unchanged)
    })
}

pub(crate) fn with_exact_filesystem_descriptors(
    state: &FilesystemTransactionState,
    exact_descriptors: &BTreeMap<String, ExactFilesystemDescriptorState>,
) -> FilesystemTransactionState {
    let mut hydrated = state.clone();
    for file in hydrated.files.values_mut() {
        if file.deleted || file.descriptor.is_some() {
            continue;
        }
        let Some(current) = exact_descriptors.get(&file.file_id) else {
            continue;
        };
        let mut descriptor = current.descriptor.clone();
        descriptor.metadata = file.metadata_patch.apply(descriptor.metadata);
        file.descriptor = Some(descriptor);
        file.untracked = current.untracked;
        file.metadata_patch = OptionalTextPatch::Unchanged;
    }
    hydrated
}

pub(crate) fn compile_filesystem_transaction_state_from_state(
    state: &FilesystemTransactionState,
    writer_key: Option<&str>,
    mutations: &[MutationRow],
) -> Result<CompiledFilesystemFinalization, LixError> {
    let mut semantic_changes = BTreeMap::new();
    let mut binary_blob_writes = Vec::new();
    for file in state.files.values() {
        if file.deleted {
            upsert_semantic_change(
                &mut semantic_changes,
                FilesystemSemanticChange {
                    entity_id: file.file_id.clone(),
                    schema_key: FILESYSTEM_FILE_SCHEMA_KEY.to_string(),
                    schema_version: FILESYSTEM_FILE_SCHEMA_VERSION.to_string(),
                    file_id: FILESYSTEM_DESCRIPTOR_FILE_ID.to_string(),
                    version_id: file.version_id.clone(),
                    untracked: file.untracked,
                    plugin_key: FILESYSTEM_DESCRIPTOR_PLUGIN_KEY.to_string(),
                    snapshot_content: None,
                    metadata: None,
                    writer_key: writer_key.map(ToString::to_string),
                },
            );
            upsert_semantic_change(
                &mut semantic_changes,
                binary_blob_ref_tombstone_change_for_target(
                    &file.file_id,
                    &file.version_id,
                    file.untracked,
                    writer_key,
                ),
            );
            continue;
        }

        if let Some(descriptor) = file.descriptor.as_ref() {
            upsert_semantic_change(
                &mut semantic_changes,
                file_descriptor_change_for_state(
                    &file.file_id,
                    &file.version_id,
                    descriptor,
                    file.untracked,
                    writer_key,
                ),
            );
        }

        if let Some(data) = file.data.as_ref() {
            upsert_semantic_change(
                &mut semantic_changes,
                binary_blob_ref_change_for_bytes(
                    &file.file_id,
                    &file.version_id,
                    data,
                    file.untracked,
                    writer_key,
                )?,
            );
            binary_blob_writes.push(BinaryBlobWrite {
                file_id: Some(file.file_id.clone()),
                auto_path: None,
                version_id: file.version_id.clone(),
                untracked: file.untracked,
                data: data.clone(),
            });
        }
    }

    Ok(compile_filesystem_finalization(
        semantic_changes.into_values().collect(),
        binary_blob_writes,
        mutations,
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FilesystemSemanticChange {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) untracked: bool,
    pub(crate) plugin_key: String,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) writer_key: Option<String>,
}

impl FilesystemSemanticChange {
    pub(crate) fn to_payload_domain_change(&self) -> FilesystemPayloadDomainChange {
        FilesystemPayloadDomainChange {
            entity_id: self.entity_id.clone(),
            schema_key: self.schema_key.clone(),
            schema_version: self.schema_version.clone(),
            file_id: self.file_id.clone(),
            version_id: self.version_id.clone(),
            untracked: self.untracked,
            plugin_key: self.plugin_key.clone(),
            snapshot_content: self.snapshot_content.clone(),
            metadata: self.metadata.clone(),
            writer_key: self.writer_key.clone(),
        }
    }
}

impl BinaryBlobWrite {
    pub(crate) fn as_input(&self) -> BinaryBlobWriteInput<'_> {
        BinaryBlobWriteInput {
            file_id: self
                .file_id
                .as_deref()
                .or(self.auto_path.as_deref())
                .unwrap_or("<staged-binary-blob>"),
            version_id: self.version_id.as_str(),
            data: &self.data,
        }
    }
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

async fn resolve_binary_blob_write_file_id_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    write: &BinaryBlobWrite,
) -> Result<String, LixError> {
    if let Some(file_id) = write.file_id.as_ref() {
        return Ok(file_id.clone());
    }
    let Some(path) = write.auto_path.as_deref() else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "binary blob write is missing both file_id and auto_path".to_string(),
        });
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
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "binary blob write: unable to resolve auto-generated file id for path '{}' in version '{}'",
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
) -> Result<FilesystemSemanticChange, LixError> {
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
    Ok(FilesystemSemanticChange {
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

fn file_descriptor_change_for_state(
    file_id: &str,
    version_id: &str,
    descriptor: &FilesystemDescriptorState,
    untracked: bool,
    writer_key: Option<&str>,
) -> FilesystemSemanticChange {
    let metadata = descriptor.metadata.clone();
    FilesystemSemanticChange {
        entity_id: file_id.to_string(),
        schema_key: FILESYSTEM_FILE_SCHEMA_KEY.to_string(),
        schema_version: FILESYSTEM_FILE_SCHEMA_VERSION.to_string(),
        file_id: FILESYSTEM_DESCRIPTOR_FILE_ID.to_string(),
        version_id: version_id.to_string(),
        untracked,
        plugin_key: FILESYSTEM_DESCRIPTOR_PLUGIN_KEY.to_string(),
        snapshot_content: Some(
            serde_json::json!({
                "id": file_id,
                "directory_id": if descriptor.directory_id.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::String(descriptor.directory_id.clone())
                },
                "name": descriptor.name,
                "extension": descriptor.extension,
                "metadata": metadata,
                "hidden": descriptor.hidden,
            })
            .to_string(),
        ),
        metadata,
        writer_key: writer_key.map(ToString::to_string),
    }
}

fn upsert_semantic_change(
    changes: &mut BTreeMap<(String, String, String, String, bool), FilesystemSemanticChange>,
    change: FilesystemSemanticChange,
) {
    let key = (
        change.entity_id.clone(),
        change.schema_key.clone(),
        change.file_id.clone(),
        change.version_id.clone(),
        change.untracked,
    );
    changes.insert(key, change);
}

fn binary_blob_ref_tombstone_change_for_target(
    file_id: &str,
    version_id: &str,
    untracked: bool,
    writer_key: Option<&str>,
) -> FilesystemSemanticChange {
    FilesystemSemanticChange {
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
    pub(crate) async fn persist_filesystem_payload_domain_changes_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
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
        transaction: &mut dyn LixBackendTransaction,
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
        transaction: &mut dyn LixBackendTransaction,
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

    pub(crate) async fn compile_filesystem_finalization_from_state_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        filesystem_state: &FilesystemTransactionState,
        writer_key: Option<&str>,
        mutations: &[MutationRow],
    ) -> Result<CompiledFilesystemFinalization, LixError> {
        let state = if filesystem_transaction_state_needs_exact_descriptors(filesystem_state) {
            let exact_descriptors = load_exact_filesystem_descriptors_for_state_in_transaction(
                transaction,
                filesystem_state,
            )
            .await?;
            with_exact_filesystem_descriptors(filesystem_state, &exact_descriptors)
        } else {
            filesystem_state.clone()
        };
        compile_filesystem_transaction_state_from_state(&state, writer_key, mutations)
    }

    pub(crate) async fn persist_binary_blob_writes_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        writes: &[BinaryBlobWrite],
    ) -> Result<(), LixError> {
        let mut latest_index_by_key: BTreeMap<(String, String), usize> = BTreeMap::new();
        for (index, write) in writes.iter().enumerate() {
            let resolved_file_id =
                resolve_binary_blob_write_file_id_in_transaction(transaction, write).await?;
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
                data: &write.data,
            });
        }
        let program = build_binary_blob_fastcdc_write_program(transaction.dialect(), &payloads)?;
        execute_write_program_with_transaction(transaction, program).await?;

        Ok(())
    }

    pub(crate) async fn garbage_collect_unreachable_binary_cas_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
    ) -> Result<(), LixError> {
        garbage_collect_unreachable_binary_cas_in_transaction(transaction).await
    }
}

async fn load_exact_filesystem_descriptors_for_state_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    filesystem_state: &FilesystemTransactionState,
) -> Result<BTreeMap<String, ExactFilesystemDescriptorState>, LixError> {
    let targets = filesystem_state
        .files
        .values()
        .filter(|file| {
            !file.deleted
                && file.descriptor.is_none()
                && !matches!(file.metadata_patch, OptionalTextPatch::Unchanged)
        })
        .map(|file| (file.file_id.as_str(), file.version_id.as_str()))
        .collect::<BTreeSet<_>>();
    let mut loaded = BTreeMap::new();
    let backend = TransactionBackendAdapter::new(transaction);
    for (file_id, version_id) in targets {
        let row = load_file_row_by_id_without_path(
            &backend,
            version_id,
            file_id,
            FilesystemProjectionScope::ExplicitVersion,
        )
        .await
        .map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: error.message,
        })?;
        let row = if row.is_some() || version_id == crate::version::GLOBAL_VERSION_ID {
            row
        } else {
            load_file_row_by_id_without_path(
                &backend,
                crate::version::GLOBAL_VERSION_ID,
                file_id,
                FilesystemProjectionScope::ExplicitVersion,
            )
            .await
            .map_err(|error| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: error.message,
            })?
        };
        let Some(row) = row else {
            continue;
        };
        loaded.insert(
            file_id.to_string(),
            ExactFilesystemDescriptorState {
                descriptor: FilesystemDescriptorState {
                    directory_id: row.directory_id.unwrap_or_default(),
                    name: row.name,
                    extension: row.extension,
                    metadata: row.metadata,
                    hidden: row.hidden,
                },
                untracked: row.untracked,
            },
        );
    }
    Ok(loaded)
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
const SINGLE_CHUNK_FAST_PATH_MAX_BYTES: usize = 64 * 1024;
const ZSTD_MIN_CHUNK_BYTES: usize = 32 * 1024;
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
    transaction: &'a mut dyn LixBackendTransaction,
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
        let materialize_chunk_cas = should_materialize_chunk_cas(payload.data);
        let chunk_ranges = if materialize_chunk_cas {
            fastcdc_chunk_ranges(payload.data)
        } else {
            Vec::new()
        };
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

fn should_materialize_chunk_cas(data: &[u8]) -> bool {
    data.len() > SINGLE_CHUNK_FAST_PATH_MAX_BYTES
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
    if data.len() <= SINGLE_CHUNK_FAST_PATH_MAX_BYTES {
        return vec![(0, data.len())];
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
    if chunk_data.len() < ZSTD_MIN_CHUNK_BYTES {
        return Ok(EncodedBinaryChunkPayload {
            codec: BINARY_CHUNK_CODEC_RAW,
            codec_dict_id: None,
            data: chunk_data.to_vec(),
        });
    }

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
    transaction: &mut dyn LixBackendTransaction,
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
    transaction: &mut dyn LixBackendTransaction,
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
