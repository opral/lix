//! Recovery is an explicit operation over a retained source, never an open gate.
//!
//! These exports describe current logical state and the original branch coordinates.
//! They do not claim to reconstruct unavailable cold history. Source storage remains
//! retained even after exporting or restoring files.
use std::collections::{BTreeMap, BTreeSet};

use base64::Engine as _;
use serde::{Deserialize, Serialize};

use crate::branch::BranchHeadControlContext;
use crate::hot_state::TrackedHeadContext;
use crate::storage_adapter::{Storage, StorageAdapter, StorageReadOptions};
use crate::tracked_state::{TrackedStateFilter, TrackedStateScanRequest};
use crate::{Lix, LixError, Value};

const MAX_RECOVERY_BLOB_BYTES: u64 = 64 * 1024 * 1024;
const MAX_RECOVERY_EXPORT_BLOB_BYTES: u64 = 128 * 1024 * 1024;
const MAX_RECOVERY_ROWS: usize = 100_000;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaRecoverySource {
    pub id: String,
    pub source_format: u32,
    pub repository_id: String,
    pub account_id: String,
    pub recovery_required: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaRecoveryRow {
    pub row_pk: serde_json::Value,
    pub schema_key: String,
    pub file_id: Option<String>,
    pub snapshot: Option<serde_json::Value>,
    pub metadata: Option<serde_json::Value>,
    pub deleted: bool,
    pub untracked: bool,
    pub global: bool,
    pub change_id: Option<String>,
    pub commit_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaRecoveryBranch {
    pub branch_id: String,
    pub head_commit_id: String,
    pub checkpoint_commit_id: Option<String>,
    pub rows: Vec<ReplicaRecoveryRow>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaRecoveryBlob {
    pub id: String,
    pub content_base64: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaRecoveryFile {
    pub branch_id: String,
    pub id: String,
    pub path: String,
    pub untracked: bool,
    /// Absent when the old replica needs unavailable cold data or plugin rendering.
    pub blob_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaRecoveryExport {
    pub version: u32,
    pub source: ReplicaRecoverySource,
    pub branches: Vec<ReplicaRecoveryBranch>,
    /// Available original immutable commits; unavailable dependencies are listed explicitly.
    pub commits: Vec<serde_json::Value>,
    pub uploads: Vec<serde_json::Value>,
    pub blobs: Vec<ReplicaRecoveryBlob>,
    pub files: Vec<ReplicaRecoveryFile>,
    pub unresolved: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaRecoveryReceipt {
    pub branch_ids: Vec<String>,
    pub restored_files: usize,
    pub restored_rows: usize,
    pub unresolved: Vec<String>,
}

impl From<&crate::migration::RetainedReplicaSource> for ReplicaRecoverySource {
    fn from(source: &crate::migration::RetainedReplicaSource) -> Self {
        Self {
            id: source.bank.clone(),
            source_format: source.source_format,
            repository_id: source.repository_id.clone(),
            account_id: source.account_id.clone(),
            recovery_required: source.recovery_required,
        }
    }
}

impl<S: Storage + Clone + Send + Sync + 'static> Lix<S> {
    /// Lists preserved prior replicas. Listing and export never delete them.
    pub async fn replica_recovery_sources(&self) -> Result<Vec<ReplicaRecoverySource>, LixError> {
        let adapter = self.storage_adapter();
        let mut retry = crate::common::ExpiredReadRetryState::default();
        loop {
            match crate::migration::list_retained_replica_sources(adapter.storage()).await {
                Ok(sources) => {
                    return Ok(sources
                        .iter()
                        .filter(|source| {
                            source.repository_id == self.lix_id()
                                && source.account_id == self.active_account_id()
                        })
                        .map(ReplicaRecoverySource::from)
                        .collect());
                }
                Err(error) => match retry.next_delay(&error) {
                    Some(delay) => super::sleep(delay).await,
                    None => return Err(error),
                },
            }
        }
    }

    /// Exports local state without uploading local-only rows or changing server refs.
    pub async fn export_replica_recovery(
        &self,
        id: &str,
    ) -> Result<ReplicaRecoveryExport, LixError> {
        let mut retry = crate::common::ExpiredReadRetryState::default();
        loop {
            match self.export_replica_recovery_once(id).await {
                Ok(export) => return Ok(export),
                Err(error) => match retry.next_delay(&error) {
                    Some(delay) => super::sleep(delay).await,
                    None => return Err(error),
                },
            }
        }
    }

    async fn export_replica_recovery_once(
        &self,
        id: &str,
    ) -> Result<ReplicaRecoveryExport, LixError> {
        let adapter = self.storage_adapter();
        let retained = crate::migration::list_retained_replica_sources(adapter.storage())
            .await?
            .into_iter()
            .find(|source| source.bank == id)
            .ok_or_else(|| {
                LixError::new(LixError::CODE_INVALID_PARAM, "retained replica not found")
            })?;
        if retained.repository_id != self.lix_id()
            || retained.account_id != self.active_account_id()
        {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "retained replica belongs to a different repository or account",
            ));
        }
        let source =
            crate::migration::open_retained_replica_source(adapter.storage(), &retained).await?;
        export_source(&source, ReplicaRecoverySource::from(&retained)).await
    }

    /// Restores captured tracked rows into separate branches, preserving file IDs.
    /// Original history and local-only rows remain in the retained source/export.
    /// Retrying returns durable receipts without rewriting a recovered branch.
    pub async fn recover_replica(&self, id: &str) -> Result<ReplicaRecoveryReceipt, LixError> {
        let export = self.export_replica_recovery(id).await?;
        self.restore_recovery_export(&export).await
    }

    /// Explicitly restores a retained pre-native source, fetching only missing
    /// recovery history and chunks from the authenticated authority. This does
    /// not start synchronization or upload recovered branches.
    pub async fn recover_replica_with_server(
        &self,
        id: &str,
        server: crate::ServerOptions,
    ) -> Result<ReplicaRecoveryReceipt, LixError> {
        use super::SyncTransport as _;
        use futures_util::FutureExt as _;
        let export = self.export_replica_recovery(id).await?;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        if !matches!(
            crate::init::repository_protocol_status(&read).await?,
            crate::init::RepositoryProtocolStatus::Current
        ) {
            return Err(LixError::new(
                "LIX_PARTIAL_REPLICA_MIGRATION_REQUIRED",
                "explicit retained-source recovery requires the current full recovery layout",
            ));
        }
        let identity =
            super::inspect_replica_rebuild_source(&read, crate::init::CURRENT_FORMAT_VERSION)
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "recovery requires an identified full replica",
                    )
                })?;
        if identity.repository_id != self.lix_id()
            || identity.account_id != self.active_account_id()
        {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "recovery source identity differs from local account",
            ));
        }
        drop(read);
        let transport =
            super::http::HttpSyncTransport::connect(&server.url, &server.headers).await?;
        let result = async {
            if transport.lix_id() != self.lix_id()
                || transport.active_account_id() != self.active_account_id()
            {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "recovery authority changed repository or account",
                ));
            }
            // A fresh adapter keeps the recovery writer capability out of the
            // caller's engine and retains the exact active-epoch fence.
            let fresh = crate::migration::admit_existing_repository(adapter.storage()).await?;
            if fresh.epoch_bank() != adapter.epoch_bank() {
                return Err(LixError::new(
                    "LIX_RECOVERY_EPOCH_CHANGED",
                    "repository epoch changed before explicit recovery",
                ));
            }
            let branch_id = self.active_branch_id().await?;
            let recovery = crate::handle::new_replica_recovery_context(
                fresh,
                &branch_id,
                self.active_account_id(),
            )
            .await?;
            let restored = async {
                let mut seen = BTreeSet::new();
                loop {
                    match recovery.restore_recovery_export(&export).await {
                        Ok(receipt) => return Ok(receipt),
                        Err(error) => {
                            super::runtime::hydrate_explicit_recovery_error(
                                &recovery, &transport, error, &mut seen,
                            )
                            .await?
                        }
                    }
                }
            }
            .await;
            let close = recovery.close().await;
            match restored {
                Err(error) => Err(error),
                Ok(receipt) => {
                    close?;
                    Ok(receipt)
                }
            }
        }
        .await;
        let close = transport.close_session().fuse();
        let timeout = super::sleep(std::time::Duration::from_secs(1)).fuse();
        futures_util::pin_mut!(close, timeout);
        futures_util::select_biased! { _ = close => {}, _ = timeout => {} }
        result
    }

    async fn restore_recovery_export(
        &self,
        export: &ReplicaRecoveryExport,
    ) -> Result<ReplicaRecoveryReceipt, LixError> {
        if export.source.repository_id != self.lix_id()
            || export.source.account_id != self.active_account_id()
        {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "recovery identity does not match this repository/account",
            ));
        }
        let files_by_id = export
            .files
            .iter()
            .map(|file| ((file.branch_id.as_str(), file.id.as_str()), file))
            .collect::<BTreeMap<_, _>>();
        let mut payloads = BTreeMap::<String, crate::Blob>::new();
        let mut payload_bytes = 0_u64;
        for blob in &export.blobs {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(&blob.content_base64)
                .map_err(|error| LixError::unknown(error.to_string()))?;
            if crate::binary_cas::CanonicalBlobManifest::from_bytes(&bytes)
                .blob_id
                .to_hex()
                != blob.id
            {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "recovery blob identity mismatch",
                ));
            }
            payload_bytes = payload_bytes.saturating_add(bytes.len() as u64);
            if payload_bytes > MAX_RECOVERY_EXPORT_BLOB_BYTES {
                return Err(LixError::new(
                    "LIX_RECOVERY_EXPORT_TOO_LARGE",
                    "Recovery payloads exceed the memory budget; source remains retained",
                ));
            }
            payloads.insert(blob.id.clone(), bytes.into());
        }
        let mut result = ReplicaRecoveryReceipt {
            branch_ids: Vec::new(),
            restored_files: 0,
            restored_rows: 0,
            unresolved: export.unresolved.clone(),
        };
        for branch in &export.branches {
            if branch.branch_id == crate::GLOBAL_BRANCH_ID {
                continue;
            }
            let rows = branch
                .rows
                .iter()
                .filter(|row| !row.global && !row.untracked)
                .cloned()
                .collect::<Vec<_>>();
            if rows.is_empty() {
                continue;
            }
            let mut capture_hasher = blake3::Hasher::new();
            serde_json::to_writer(&mut capture_hasher, &rows)
                .map_err(|error| LixError::unknown(error.to_string()))?;
            let capture_digest = capture_hasher.finalize();
            let identity = format!(
                "lix-recovery-v1:{}:{}:{}:{}:{}:{}",
                export.source.repository_id,
                export.source.account_id,
                export.source.id,
                branch.branch_id,
                branch.head_commit_id,
                branch.checkpoint_commit_id.as_deref().unwrap_or_default()
            );
            let digest = blake3::hash(format!("{identity}:{}", capture_digest.to_hex()).as_bytes());
            let mut bytes = [0_u8; 16];
            bytes.copy_from_slice(&digest.as_bytes()[..16]);
            bytes[6] = (bytes[6] & 0x0f) | 0x40;
            bytes[8] = (bytes[8] & 0x3f) | 0x80;
            let branch_id = uuid::Uuid::from_bytes(bytes).to_string();
            let receipt_key = format!("lix_recovery:{}", digest.to_hex());
            let restored_files = rows
                .iter()
                .filter(|row| row.schema_key == "lix_file_descriptor" && !row.deleted)
                .count();
            let receipt = serde_json::json!({"branchId": branch_id, "restoredFiles": restored_files, "restoredRows": rows.len(), "sourceHeadCommitId": branch.head_commit_id, "sourceCheckpointCommitId": branch.checkpoint_commit_id, "captureDigest": capture_digest.to_hex().to_string() });
            let existing = self
                .execute(
                    "SELECT value FROM lix_key_value WHERE key = $1",
                    &[Value::Text(receipt_key.clone())],
                )
                .await?;
            if let Some(existing) = existing.rows().first() {
                let saved = existing.get::<serde_json::Value>("value")?;
                if saved != receipt {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "recovery receipt does not match this retained source",
                    ));
                }
                let branch_exists = self
                    .execute(
                        "SELECT id FROM lix_branch WHERE id = $1",
                        &[Value::Text(branch_id.clone())],
                    )
                    .await?;
                if branch_exists.rows().is_empty() {
                    result.unresolved.push(format!("Previously recovered branch {branch_id} was deleted; the original source is still retained"));
                    continue;
                }
                result.branch_ids.push(branch_id);
                result.restored_files += restored_files;
                result.restored_rows += rows.len();
                continue;
            }
            let mut plugin_payloads = BTreeMap::<String, Vec<String>>::new();
            for row in rows
                .iter()
                .filter(|row| row.schema_key == "lix_key_value" && !row.deleted)
            {
                if let Some(plugins) = row
                    .snapshot
                    .as_ref()
                    .filter(|s| {
                        s.get("key").and_then(serde_json::Value::as_str)
                            == Some("lix_plugin_registry_v2")
                    })
                    .and_then(|s| s.get("value"))
                    .and_then(|value| value.get("plugins"))
                    .and_then(serde_json::Value::as_array)
                {
                    for plugin in plugins {
                        if let (Some(id), Some(hash)) = (
                            plugin
                                .get("archive_file_id")
                                .and_then(serde_json::Value::as_str),
                            plugin
                                .get("wasm_blob_hash")
                                .and_then(serde_json::Value::as_str),
                        ) {
                            plugin_payloads
                                .entry(id.to_owned())
                                .or_default()
                                .push(hash.to_owned());
                        }
                    }
                }
            }
            let mut unavailable = Vec::new();
            for row in rows
                .iter()
                .filter(|row| row.schema_key == "lix_binary_blob_ref" && !row.deleted)
            {
                let Some(snapshot) = row.snapshot.as_ref() else {
                    continue;
                };
                let Some(hash) = snapshot
                    .get("blob_hash")
                    .and_then(serde_json::Value::as_str)
                else {
                    continue;
                };
                if payloads.contains_key(hash) {
                    continue;
                }
                let size = snapshot
                    .get("size_bytes")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(MAX_RECOVERY_BLOB_BYTES + 1);
                if size > MAX_RECOVERY_BLOB_BYTES
                    || payload_bytes.saturating_add(size) > MAX_RECOVERY_EXPORT_BLOB_BYTES
                {
                    unavailable.push(hash.to_owned());
                    continue;
                }
                // Fetch the exact checkpoint blob, not plugin-rendered current
                // content. Keep bytes until atomic row publication to avoid GC races.
                let hydrated = self.read_recovery_blob(hash, size).await;
                match hydrated {
                    Ok(Some(payload))
                        if crate::binary_cas::CanonicalBlobManifest::from_bytes(
                            payload.as_ref(),
                        )
                        .blob_id
                        .to_hex()
                            == hash =>
                    {
                        payload_bytes = payload_bytes.saturating_add(payload.len() as u64);
                        if payload_bytes > MAX_RECOVERY_EXPORT_BLOB_BYTES {
                            return Err(LixError::new(
                                "LIX_RECOVERY_EXPORT_TOO_LARGE",
                                "Hydrated recovery content exceeds the memory budget; source remains retained",
                            ));
                        }
                        payloads.insert(hash.to_owned(), payload);
                    }
                    Ok(_) => {
                        unavailable.push(hash.to_owned());
                    }
                    Err(error) => {
                        result.unresolved.push(format!(
                            "Blob {hash} could not be hydrated: {}",
                            error.message
                        ));
                        unavailable.push(hash.to_owned());
                    }
                }
            }
            unavailable.extend(
                plugin_payloads
                    .values()
                    .flatten()
                    .filter(|hash| !payloads.contains_key(hash.as_str()))
                    .cloned(),
            );
            if !unavailable.is_empty() {
                result.unresolved.push(format!(
                    "Branch {} was not restored because local blob content is unavailable: {}",
                    branch.branch_id,
                    unavailable.join(", ")
                ));
                continue;
            }
            let mut file_content = Vec::new();
            for row in rows
                .iter()
                .filter(|row| row.schema_key == "lix_binary_blob_ref" && !row.deleted)
            {
                let Some(snapshot) = row.snapshot.as_ref() else {
                    continue;
                };
                let (Some(file_id), Some(hash)) = (
                    snapshot.get("id").and_then(serde_json::Value::as_str),
                    snapshot
                        .get("blob_hash")
                        .and_then(serde_json::Value::as_str),
                ) else {
                    continue;
                };
                let Some(content) = payloads.get(hash) else {
                    continue;
                };
                let path = files_by_id
                    .get(&(branch.branch_id.as_str(), file_id))
                    .map(|file| file.path.clone());
                let filename = path
                    .as_ref()
                    .and_then(|path| path.rsplit('/').next())
                    .map(str::to_owned);
                let mut content_write = crate::transaction_types::TransactionFileContent::new(
                    file_id.to_owned(),
                    path,
                    filename,
                    branch_id.clone(),
                    false,
                    false,
                    content.clone(),
                );
                if let Some(auxiliary) = plugin_payloads.get(file_id) {
                    for hash in auxiliary {
                        content_write.add_auxiliary_payload(
                            payloads
                                .get(hash)
                                .expect("validated component payload")
                                .clone(),
                        );
                    }
                }
                file_content.push(content_write);
            }
            let publication = self
                .restore_replica_rows_atomic(
                    &branch_id,
                    &format!("Recovered {} {}", export.source.id, branch.branch_id),
                    &rows,
                    file_content,
                    &receipt_key,
                    receipt.clone(),
                )
                .await;
            if let Err(error) = publication {
                // Another recovery caller may have won the atomic insert.
                // Accept only its exact receipt, never rewrite its branch.
                let saved = self
                    .execute(
                        "SELECT value FROM lix_key_value WHERE key = $1",
                        &[Value::Text(receipt_key.clone())],
                    )
                    .await?;
                if saved
                    .rows()
                    .first()
                    .and_then(|row| row.get::<serde_json::Value>("value").ok())
                    .as_ref()
                    != Some(&receipt)
                {
                    return Err(error);
                }
                let branch_exists = self
                    .execute(
                        "SELECT id FROM lix_branch WHERE id = $1",
                        &[Value::Text(branch_id.clone())],
                    )
                    .await?;
                if branch_exists.rows().is_empty() {
                    return Err(error);
                }
            }
            result.branch_ids.push(branch_id);
            result.restored_files += restored_files;
            result.restored_rows += rows.len();
        }
        if export
            .branches
            .iter()
            .flat_map(|branch| &branch.rows)
            .any(|row| row.global && !row.untracked)
        {
            result.unresolved.push("Global rows are included in the export but are not applied to the authority's shared global state".to_owned());
        }
        if export
            .branches
            .iter()
            .flat_map(|branch| &branch.rows)
            .any(|row| row.untracked)
        {
            result.unresolved.push(
                "Local-only rows remain in the local export and were not uploaded".to_owned(),
            );
        }
        Ok(result)
    }
}

async fn export_source<S: Storage>(
    adapter: &StorageAdapter<S>,
    source: ReplicaRecoverySource,
) -> Result<ReplicaRecoveryExport, LixError> {
    let read = adapter.begin_read(StorageReadOptions::default()).await?;
    let controls = BranchHeadControlContext::new().reader(&read).scan().await?;
    let mut result = ReplicaRecoveryExport { version: 1, source, branches: Vec::new(), commits: Vec::new(), uploads: Vec::new(), blobs: Vec::new(), files: Vec::new(), unresolved: vec!["This export preserves current rows and original branch/checkpoint coordinates; original commit history remains in the retained source".to_owned()] };
    let mut blob_ids = BTreeSet::new();
    let mut commit_ids = controls
        .iter()
        .flat_map(|(_, control)| {
            [
                Some(control.head_commit_id),
                control.working_diff_checkpoint_commit_id,
            ]
        })
        .flatten()
        .collect::<BTreeSet<_>>();
    match super::repository::load_pending_sync_export_commit_ids(&read, &controls).await {
        Ok(ids) => commit_ids.extend(ids),
        Err(error) if error.code == LixError::CODE_STORAGE_READ_EXPIRED => return Err(error),
        Err(error) => result.unresolved.push(format!(
            "Original pending commit dependencies need recovery: {}",
            error.message
        )),
    }
    for id in commit_ids {
        match super::commit::load_sync_commit(&read, id).await {
            Ok(Some(commit)) => result.commits.push(
                serde_json::to_value(commit)
                    .map_err(|error| LixError::unknown(error.to_string()))?,
            ),
            Ok(None) => result
                .unresolved
                .push(format!("Original commit {id} is not available locally")),
            Err(error) if error.code == LixError::CODE_STORAGE_READ_EXPIRED => return Err(error),
            Err(error) => result.unresolved.push(format!(
                "Original commit {id} needs legacy recovery: {}",
                error.message
            )),
        }
    }
    let mut row_count = 0_usize;
    for (branch_id, control) in controls {
        let batch = TrackedHeadContext::new()
            .reader(&read)
            .scan_live_batch_for_retention(
                &branch_id,
                control,
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        include_tombstones: true,
                        ..Default::default()
                    },
                    limit: Some(MAX_RECOVERY_ROWS.saturating_sub(row_count) + 1),
                    ..Default::default()
                },
                None,
            )
            .await?;
        row_count = row_count.saturating_add(batch.len());
        if row_count > MAX_RECOVERY_ROWS {
            return Err(LixError::new(
                "LIX_RECOVERY_EXPORT_TOO_LARGE",
                "Recovery export exceeds the row limit; the full source remains retained",
            ));
        }
        let mut rows = Vec::with_capacity(batch.len());
        for row in batch.iter() {
            let snapshot = row.snapshot_json_value()?;
            if row.schema_key() == "lix_binary_blob_ref" && !row.deleted() {
                if let Some(id) = snapshot
                    .as_ref()
                    .and_then(|s| s.get("blob_hash"))
                    .and_then(serde_json::Value::as_str)
                {
                    blob_ids.insert(id.to_owned());
                }
            }
            if row.schema_key() == "lix_key_value" && !row.deleted() {
                if let Some(plugins) = snapshot
                    .as_ref()
                    .filter(|s| {
                        s.get("key").and_then(serde_json::Value::as_str)
                            == Some("lix_plugin_registry_v2")
                    })
                    .and_then(|s| s.get("value"))
                    .and_then(|value| value.get("plugins"))
                    .and_then(serde_json::Value::as_array)
                {
                    for plugin in plugins {
                        for field in ["wasm_blob_hash", "archive_blob_hash"] {
                            if let Some(hash) =
                                plugin.get(field).and_then(serde_json::Value::as_str)
                            {
                                blob_ids.insert(hash.to_owned());
                            }
                        }
                    }
                }
            }
            rows.push(ReplicaRecoveryRow {
                row_pk: row.row_pk().as_typed_json_array_value()?,
                schema_key: row.schema_key().to_owned(),
                file_id: row.file_id().map(str::to_owned),
                snapshot,
                metadata: row
                    .metadata()
                    .map(|raw| serde_json::from_str(raw.as_str()))
                    .transpose()
                    .map_err(|error| LixError::unknown(error.to_string()))?,
                deleted: row.deleted(),
                untracked: row.untracked(),
                global: row.global(),
                change_id: row.change_id().map(|id| id.to_string()),
                commit_id: row.commit_id().map(|id| id.to_string()),
            });
        }
        result.branches.push(ReplicaRecoveryBranch {
            branch_id,
            head_commit_id: control.head_commit_id.to_string(),
            checkpoint_commit_id: control
                .working_diff_checkpoint_commit_id
                .map(|id| id.to_string()),
            rows,
        });
    }
    let mut exported_blob_bytes = 0_u64;
    for id in blob_ids {
        let blob_id = crate::binary_cas::BlobId::from_hex(&id)?;
        let metadata = crate::binary_cas::load_metadata_many(&read, &[blob_id])
            .await?
            .into_vec()
            .into_iter()
            .next()
            .flatten();
        let Some(metadata) = metadata else {
            result.unresolved.push(format!("Blob {id} metadata is unavailable; content remains in the retained source when present"));
            continue;
        };
        if metadata.size_bytes > MAX_RECOVERY_BLOB_BYTES
            || exported_blob_bytes.saturating_add(metadata.size_bytes)
                > MAX_RECOVERY_EXPORT_BLOB_BYTES
        {
            result.unresolved.push(format!(
                "Blob {id} exceeds this export's memory budget and remains in the retained source"
            ));
            continue;
        }
        exported_blob_bytes += metadata.size_bytes;
        match crate::binary_cas::load_bytes_many(&read, &[blob_id]).await {
            Ok(bytes) => match bytes.into_vec().into_iter().next().flatten() {
                Some(bytes) => result.blobs.push(ReplicaRecoveryBlob {
                    id,
                    content_base64: base64::engine::general_purpose::STANDARD.encode(bytes),
                }),
                None => result.unresolved.push(format!(
                    "Blob {id} is not available in the retained replica"
                )),
            },
            Err(error) if error.code == LixError::CODE_STORAGE_READ_EXPIRED => return Err(error),
            Err(error) => result.unresolved.push(format!(
                "Blob {id} could not be exported: {}",
                error.message
            )),
        }
    }
    result.uploads = crate::session::export_recoverable_uploads(&read).await?;
    if !result.uploads.is_empty() {
        result.unresolved.push("Unfinished upload parts are included in this local export and were not published as files".to_owned());
    }
    collect_files(&mut result);
    Ok(result)
}

fn collect_files(export: &mut ReplicaRecoveryExport) {
    let available_blobs = export
        .blobs
        .iter()
        .map(|blob| blob.id.as_str())
        .collect::<BTreeSet<_>>();
    for branch in &export.branches {
        let directories = branch
            .rows
            .iter()
            .filter(|row| row.schema_key == "lix_directory_descriptor" && !row.deleted)
            .filter_map(|row| row.snapshot.as_ref())
            .filter_map(|s| Some((s.get("id")?.as_str()?, s)))
            .collect::<BTreeMap<_, _>>();
        let refs = branch
            .rows
            .iter()
            .filter(|row| row.schema_key == "lix_binary_blob_ref" && !row.deleted)
            .filter_map(|row| row.snapshot.as_ref())
            .filter_map(|s| Some((s.get("id")?.as_str()?, s.get("blob_hash")?.as_str()?)))
            .collect::<BTreeMap<_, _>>();
        for row in branch
            .rows
            .iter()
            .filter(|row| row.schema_key == "lix_file_descriptor" && !row.deleted)
        {
            let Some(snapshot) = row.snapshot.as_ref() else {
                continue;
            };
            let (Some(id), Some(name)) = (
                snapshot.get("id").and_then(serde_json::Value::as_str),
                snapshot.get("name").and_then(serde_json::Value::as_str),
            ) else {
                continue;
            };
            let mut components = vec![name.to_owned()];
            let mut parent = snapshot
                .get("directory_id")
                .and_then(serde_json::Value::as_str);
            let mut seen = BTreeSet::new();
            let mut valid = true;
            while let Some(id) = parent {
                if !seen.insert(id) {
                    valid = false;
                    break;
                }
                let Some(directory) = directories.get(id) else {
                    valid = false;
                    break;
                };
                let Some(name) = directory.get("name").and_then(serde_json::Value::as_str) else {
                    valid = false;
                    break;
                };
                components.push(name.to_owned());
                parent = directory
                    .get("parent_id")
                    .and_then(serde_json::Value::as_str);
            }
            if !valid {
                export
                    .unresolved
                    .push(format!("File {id} has an incomplete directory path"));
                continue;
            }
            components.reverse();
            let path = format!("/{}", components.join("/"));
            let blob_id = refs
                .get(id)
                .filter(|hash| available_blobs.contains(**hash))
                .map(|hash| (*hash).to_owned());
            if blob_id.is_none() {
                export.unresolved.push(format!("File {path} needs cold content or plugin rendering; logical rows remain in this export"));
            }
            export.files.push(ReplicaRecoveryFile {
                branch_id: branch.branch_id.clone(),
                id: id.to_owned(),
                path,
                untracked: row.untracked,
                blob_id,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn recovery_export_omits_unavailable_file_payload_but_preserves_blob_reference() {
        use crate::storage_adapter::{StorageWrite as _, StorageWriteOptions};
        let lix = crate::open_lix().await.unwrap();
        lix.execute(
            "INSERT INTO lix_file (path, content) VALUES ('/cold.txt', $1)",
            &[Value::Blob(b"cold file content".to_vec().into())],
        )
        .await
        .unwrap();
        let adapter = lix.storage_adapter();
        let source = ReplicaRecoverySource {
            id: "cold-test".to_owned(),
            source_format: 77,
            repository_id: lix.lix_id().to_owned(),
            account_id: lix.active_account_id().to_owned(),
            recovery_required: true,
        };
        let complete = export_source(&adapter, source.clone()).await.unwrap();
        let original_hash = complete
            .files
            .iter()
            .find(|file| file.path == "/cold.txt")
            .unwrap()
            .blob_id
            .clone()
            .unwrap();
        let mut write = adapter
            .begin_migration_write(StorageWriteOptions::default())
            .await
            .unwrap();
        write
            .delete_range(
                crate::binary_cas::BINARY_CAS_CHUNK_SPACE,
                crate::storage_adapter::StorageKeyRange {
                    lower: std::ops::Bound::Unbounded,
                    upper: std::ops::Bound::Unbounded,
                },
            )
            .await
            .unwrap();
        write.commit().await.unwrap();
        let export = export_source(&adapter, source).await.unwrap();
        let file = export
            .files
            .iter()
            .find(|file| file.path == "/cold.txt")
            .unwrap();
        assert!(
            file.blob_id.is_none(),
            "optional file payload must not dangle"
        );
        assert!(!export.blobs.iter().any(|blob| blob.id == original_hash));
        assert!(
            export
                .branches
                .iter()
                .flat_map(|branch| &branch.rows)
                .any(|row| row.schema_key == "lix_binary_blob_ref"
                    && row
                        .snapshot
                        .as_ref()
                        .and_then(|snapshot| snapshot.get("blob_hash"))
                        .and_then(serde_json::Value::as_str)
                        == Some(original_hash.as_str()))
        );
        assert!(
            export
                .unresolved
                .iter()
                .any(|message| message.contains("/cold.txt"))
        );
        lix.close().await.unwrap();
    }

    #[tokio::test]
    async fn recovery_restores_files_and_generic_rows_without_overwriting_active_branch_or_local_only_data()
     {
        let lix = crate::open_lix().await.unwrap();
        lix.execute(
            "INSERT INTO lix_file (path, content) VALUES ('/notes.txt', $1)",
            &[Value::Blob(b"offline edit".to_vec().into())],
        )
        .await
        .unwrap();
        lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('draft', 'offline note')",
            &[],
        )
        .await
        .unwrap();
        lix.execute("INSERT INTO lix_key_value (key, value, lixcol_untracked) VALUES ('private', 'local secret', true)", &[]).await.unwrap();
        lix.create_checkpoint().await.unwrap();
        lix.execute(
            "UPDATE lix_key_value SET value = 'uncheckpointed note' WHERE key = 'draft'",
            &[],
        )
        .await
        .unwrap();
        let original_branch = lix.active_branch_id().await.unwrap();
        let original_file = lix
            .execute("SELECT id FROM lix_file WHERE path = '/notes.txt'", &[])
            .await
            .unwrap()
            .rows()[0]
            .values()[0]
            .clone();
        let adapter = lix.storage_adapter();
        let export = export_source(
            &adapter,
            ReplicaRecoverySource {
                id: "test-generation".to_owned(),
                source_format: 77,
                repository_id: lix.lix_id().to_owned(),
                account_id: lix.active_account_id().to_owned(),
                recovery_required: true,
            },
        )
        .await
        .unwrap();
        assert!(
            export
                .branches
                .iter()
                .flat_map(|b| &b.rows)
                .any(|r| r.untracked
                    && r.snapshot
                        .as_ref()
                        .is_some_and(
                            |s| s.get("key").and_then(serde_json::Value::as_str) == Some("private")
                        ))
        );
        let source_branch = export
            .branches
            .iter()
            .find(|b| b.branch_id == original_branch)
            .unwrap();
        assert_ne!(
            Some(&source_branch.head_commit_id),
            source_branch.checkpoint_commit_id.as_ref()
        );
        lix.execute(
            "UPDATE lix_file SET content = $1 WHERE path = '/notes.txt'",
            &[Value::Blob(b"new server content".to_vec().into())],
        )
        .await
        .unwrap();
        lix.execute(
            "UPDATE lix_key_value SET value = 'new server note' WHERE key = 'draft'",
            &[],
        )
        .await
        .unwrap();
        let (first, concurrent) = tokio::join!(
            lix.restore_recovery_export(&export),
            lix.restore_recovery_export(&export)
        );
        let receipt = first.unwrap();
        assert_eq!(receipt.branch_ids, concurrent.unwrap().branch_ids);
        assert!(receipt.restored_rows > 0);
        assert_eq!(lix.active_branch_id().await.unwrap(), original_branch);
        assert_eq!(
            lix.execute(
                "SELECT content FROM lix_file WHERE path = '/notes.txt'",
                &[]
            )
            .await
            .unwrap()
            .rows()[0]
                .values()[0],
            Value::Blob(b"new server content".to_vec().into())
        );
        let recovered = lix
            .open_internal_session(&receipt.branch_ids[0], lix.active_account_id())
            .await
            .unwrap();
        assert_eq!(
            recovered
                .execute(
                    "SELECT id, content FROM lix_file WHERE path = '/notes.txt'",
                    &[]
                )
                .await
                .unwrap()
                .rows()[0]
                .values(),
            vec![original_file, Value::Blob(b"offline edit".to_vec().into())]
        );
        assert_eq!(
            recovered
                .execute("SELECT value FROM lix_key_value WHERE key = 'draft'", &[])
                .await
                .unwrap()
                .rows()[0]
                .values()[0],
            Value::Jsonb(serde_json::json!("uncheckpointed note").into())
        );
        recovered
            .execute(
                "UPDATE lix_file SET content = $1 WHERE path = '/notes.txt'",
                &[Value::Blob(b"reviewed recovery".to_vec().into())],
            )
            .await
            .unwrap();
        let retry = lix.restore_recovery_export(&export).await.unwrap();
        assert_eq!(retry.branch_ids, receipt.branch_ids);
        assert_eq!(
            recovered
                .execute(
                    "SELECT content FROM lix_file WHERE path = '/notes.txt'",
                    &[]
                )
                .await
                .unwrap()
                .rows()[0]
                .values()[0],
            Value::Blob(b"reviewed recovery".to_vec().into())
        );
        recovered.close().await.unwrap();
        let fresh = super::super::repository::recovery_bootstrap_fixture(&lix).await;
        let mut without_local_blobs = export.clone();
        without_local_blobs.blobs.clear();
        let after_bootstrap = fresh
            .restore_recovery_export(&without_local_blobs)
            .await
            .unwrap();
        assert_eq!(
            after_bootstrap.branch_ids, receipt.branch_ids,
            "tracked receipt survives server bootstrap and avoids replay"
        );
        fresh.close().await.unwrap();
        lix.close().await.unwrap();
    }

    #[tokio::test]
    async fn recovery_restores_plugin_registry_and_schema_without_public_write_bypass() {
        let lix = crate::open_lix().await.unwrap();
        lix.execute("INSERT INTO lix_file (path, content) VALUES ('/.lix/plugins/plugin_test.lixplugin', $1)", &[Value::Blob(crate::plugin::runtime::recovery_test_plugin_archive().into())]).await.unwrap();
        let adapter = lix.storage_adapter();
        let export = export_source(
            &adapter,
            ReplicaRecoverySource {
                id: "plugin-test".to_owned(),
                source_format: 77,
                repository_id: lix.lix_id().to_owned(),
                account_id: lix.active_account_id().to_owned(),
                recovery_required: true,
            },
        )
        .await
        .unwrap();
        assert!(
            export
                .branches
                .iter()
                .flat_map(|b| &b.rows)
                .any(|row| row.schema_key == "lix_registered_schema")
        );
        let result = lix
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('lix_plugin_registry_v2', '{}')",
                &[],
            )
            .await;
        assert!(
            result.is_err(),
            "public writes must not bypass plugin ownership"
        );
        let receipt = lix.restore_recovery_export(&export).await.unwrap();
        assert!(!receipt.branch_ids.is_empty());
        let recovered = lix
            .open_internal_session(&receipt.branch_ids[0], lix.active_account_id())
            .await
            .unwrap();
        let schemas = recovered.execute("SELECT schema_key FROM lix_registered_schema WHERE schema_key = 'plugin_test_note'", &[]).await.unwrap();
        assert_eq!(schemas.rows().len(), 1);
        recovered.close().await.unwrap();
        lix.close().await.unwrap();
    }

    #[tokio::test]
    async fn recovery_preserves_actual_markdown_plugin_rows_and_content() {
        use std::io::Read as _;
        let plugin_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/v78_deployed_markdown.lixplugin.gz");
        let mut plugin_bytes = Vec::new();
        flate2::read::GzDecoder::new(std::fs::File::open(plugin_path).unwrap())
            .read_to_end(&mut plugin_bytes)
            .unwrap();
        let lix = crate::open_lix().await.unwrap();
        lix.execute("INSERT INTO lix_file (path, content) VALUES ('/.lix/plugins/plugin_markdown.lixplugin', $1)", &[Value::Blob(plugin_bytes.into())]).await.unwrap();
        lix.execute(
            "INSERT INTO lix_file (path, content) VALUES ('/notes.md', $1)",
            &[Value::Blob(
                b"# Offline heading\n\nUnsynchronized text.\n"
                    .to_vec()
                    .into(),
            )],
        )
        .await
        .unwrap();
        lix.create_checkpoint().await.unwrap();
        lix.execute(
            "UPDATE markdown_node SET payload_json = $1 WHERE kind = 'paragraph'",
            &[Value::Text(serde_json::json!({"inline": [{"type": "text", "value": "Semantic edit after checkpoint."}]}).to_string())],
        ).await.unwrap();
        let original_branch = lix.active_branch_id().await.unwrap();
        let adapter = lix.storage_adapter();
        let mut export = export_source(
            &adapter,
            ReplicaRecoverySource {
                id: "markdown-test".to_owned(),
                source_format: 77,
                repository_id: lix.lix_id().to_owned(),
                account_id: lix.active_account_id().to_owned(),
                recovery_required: true,
            },
        )
        .await
        .unwrap();
        assert!(
            export
                .branches
                .iter()
                .flat_map(|b| &b.rows)
                .any(|row| row.file_id.is_some()),
            "actual structured rows must be present"
        );
        lix.execute(
            "UPDATE lix_file SET content = $1 WHERE path = '/notes.md'",
            &[Value::Blob(b"# Server content\n".to_vec().into())],
        )
        .await
        .unwrap();
        // Force exact raw-CAS recovery: current semantic rendering differs from
        // the checkpoint payload, which is still available in the active CAS.
        let markdown_blob = export
            .files
            .iter()
            .find(|file| file.path == "/notes.md")
            .and_then(|file| file.blob_id.clone())
            .expect("markdown checkpoint blob");
        export.blobs.retain(|blob| blob.id != markdown_blob);
        let receipt = lix.restore_recovery_export(&export).await.unwrap();
        assert!(!receipt.branch_ids.is_empty());
        assert_eq!(lix.active_branch_id().await.unwrap(), original_branch);
        let recovered = lix
            .open_internal_session(&receipt.branch_ids[0], lix.active_account_id())
            .await
            .unwrap();
        assert_eq!(
            recovered
                .execute("SELECT content FROM lix_file WHERE path = '/notes.md'", &[])
                .await
                .unwrap()
                .rows()[0]
                .values()[0],
            Value::Blob(
                b"# Offline heading\n\nSemantic edit after checkpoint.\n"
                    .to_vec()
                    .into()
            )
        );
        recovered.close().await.unwrap();
        lix.close().await.unwrap();
    }
}
