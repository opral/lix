use crate::binary_cas::codec::binary_blob_hash_hex;
use crate::binary_cas::write::{BinaryBlobWriteInput, ResolvedBinaryBlobWrite};
use crate::contracts::artifacts::{FilesystemPayloadDomainChange, MutationRow, OptionalTextPatch};
use crate::engine::{dedupe_filesystem_payload_domain_changes, Engine};
use crate::filesystem::live_projection::FilesystemProjectionScope;
use crate::filesystem::queries::load_file_row_by_id_without_path;
use crate::runtime::TransactionBackendAdapter;
use crate::sql::storage::queries::state as state_queries;
use crate::{LixBackendTransaction, LixError, Value};
use std::collections::{BTreeMap, BTreeSet};

const INTERNAL_FILESYSTEM_PLUGIN_KEY: &str = "lix";
pub(crate) const FILESYSTEM_DESCRIPTOR_FILE_ID: &str = "lix";
pub(crate) const FILESYSTEM_DESCRIPTOR_PLUGIN_KEY: &str = "lix";
pub(crate) const FILESYSTEM_FILE_SCHEMA_KEY: &str = "lix_file_descriptor";
pub(crate) const FILESYSTEM_FILE_SCHEMA_VERSION: &str = "1";
const BINARY_BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const BINARY_BLOB_REF_SCHEMA_VERSION: &str = "1";

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

async fn resolve_binary_blob_write_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    write: &BinaryBlobWrite,
) -> Result<ResolvedBinaryBlobWrite, LixError> {
    if let Some(file_id) = write.file_id.as_ref() {
        return Ok(ResolvedBinaryBlobWrite {
            file_id: file_id.clone(),
            version_id: write.version_id.clone(),
            untracked: write.untracked,
            data: write.data.clone(),
        });
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
    Ok(ResolvedBinaryBlobWrite {
        file_id,
        version_id: write.version_id.clone(),
        untracked: write.untracked,
        data: write.data.clone(),
    })
}

pub(crate) async fn resolve_binary_blob_writes_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    writes: &[BinaryBlobWrite],
) -> Result<Vec<ResolvedBinaryBlobWrite>, LixError> {
    let mut latest_write_by_key: BTreeMap<(String, String), ResolvedBinaryBlobWrite> =
        BTreeMap::new();
    for write in writes {
        let resolved_write = resolve_binary_blob_write_in_transaction(transaction, write).await?;
        latest_write_by_key.insert(
            (
                resolved_write.file_id.clone(),
                resolved_write.version_id.clone(),
            ),
            resolved_write,
        );
    }
    Ok(latest_write_by_key.into_values().collect())
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
        "blob_hash": binary_blob_hash_hex(data),
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

    pub(crate) async fn garbage_collect_unreachable_binary_cas_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
    ) -> Result<(), LixError> {
        crate::binary_cas::gc::garbage_collect_unreachable_binary_cas_in_transaction(transaction)
            .await
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
