//! Repository-scoped synchronization.
//!
//! Lix synchronizes its existing primitives: complete immutable commits,
//! compare-and-swap branch refs, and BLAKE3-addressed binary chunks. Live
//! synchronization has one ordered repository cursor. A partial replica opens
//! with bounded coordinates and loads native query inputs on demand. The full
//! replica bootstrap eagerly loads current rows and commit topology.
//! Platform-specific code is limited to tasks, timers, HTTP, and
//! cancellation.

mod partial_attempt_restart;
pub(crate) use partial_attempt_restart::{
    PARTIAL_ATTEMPT_RESTART_SPACE, PartialAttemptRestartOutcome, PartialAttemptRestartReceipt,
    PartialAttemptRestartRequest, require_unrestarted_attempt, require_unrestarted_identity,
    stage_restart_expired_attempt,
};

mod blob;
mod bootstrap;
mod commit;
mod partial_checkpoint_upload;
#[cfg(test)]
pub(crate) use commit::export_sync_commit;
mod contract;
mod current_coverage;
mod http;
pub(crate) mod native_metadata;
pub(crate) use native_metadata::{MAX_NATIVE_METADATA_RESPONSE_BYTES, NativeMetadataRequest};
pub(crate) mod native_object;
pub(crate) use native_object::MAX_NATIVE_OBJECT_RESPONSE_BYTES;
pub(crate) mod native_object_range;
pub(crate) use native_object_range::NativeObjectRangeRequest;
mod partial_bootstrap;
pub(crate) use partial_bootstrap::stage_partial_bootstrap;
mod partial_blob;
mod partial_blob_upload;
mod partial_hydration;
mod partial_interest_journal;
mod partial_merge_analysis;
mod partial_open;
mod partial_publication;
mod partial_push_state;
pub(crate) use partial_merge_analysis::PartialMergeBudget;
mod partial_authority_merge;
mod partial_authority_merge_receipt;
pub(crate) use partial_authority_merge::{
    AuthorityMergePlan, AuthorityMergePreparation, prepare_authority_merge,
};
pub(crate) use partial_authority_merge_receipt::{
    PARTIAL_AUTHORITY_MERGE_RECEIPT_SPACE, PreparedAuthorityMergeReceipt,
    load_authority_merge_receipt,
};
pub(crate) use repository::VerifiedRetainedBodyWave;
mod partial_global_merge_runtime;
mod partial_global_merge_settlement;
mod partial_global_merge_state;
mod partial_merge_protocol;
mod partial_merge_runtime;
pub(crate) use partial_global_merge_state::PARTIAL_GLOBAL_MERGE_SPACE;
mod partial_merge_settlement;
mod partial_merge_state;
pub(crate) use partial_merge_protocol::{
    PartialMergeReceipt, PartialMergeRequest, RetainedBodyWaveRequest, RetainedBodyWaveResponse,
};
pub(crate) use partial_merge_state::PARTIAL_BRANCH_MERGE_SPACE;
mod partial_reconcile;
pub(crate) use partial_interest_journal::{
    PARTIAL_READ_INTEREST_SPACE, flush_partial_read_interests,
};
mod leased_descriptor;
mod partial_replica;
pub(crate) use leased_descriptor::{LeasedPartialReplicaDescriptor, MAX_LEASED_DESCRIPTOR_BYTES};
mod partial_runtime;
pub(crate) use partial_open::{
    AuthenticatedPartialConversion, FinalizedPartialConversion, admit_partial_storage_session,
    authenticate_partial_conversion, prepare_partial_open,
};
#[cfg(test)]
mod partial_scope_tests;
#[cfg(test)]
mod partial_sql_tests;
mod partial_upload;
mod partial_upload_cycle;
#[cfg(test)]
mod partial_working_diff_tests;
pub(crate) use partial_push_state::PARTIAL_BRANCH_PUSH_SPACE;
mod partial_state;
pub(crate) use partial_state::{
    PARTIAL_REPLICA_STATE_SPACE, PartialReplicaState, load_partial_replica_state,
    partial_replica_state_key,
};
mod platform;
pub(crate) use partial_replica::{
    MAX_PARTIAL_REPLICA_DESCRIPTOR_BYTES, PARTIAL_REPLICA_DESCRIPTOR_VERSION,
    PartialReplicaDescriptor,
};
mod protocol;
mod recovery;
mod repository;
pub use recovery::{
    ReplicaRecoveryBlob, ReplicaRecoveryBranch, ReplicaRecoveryExport, ReplicaRecoveryFile,
    ReplicaRecoveryReceipt, ReplicaRecoveryRow, ReplicaRecoverySource,
};
mod runtime;
#[cfg(test)]
mod simulation_tests;
#[cfg(test)]
mod upload_metrics;
mod upload_plan;
mod upload_proof;

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use crate::LixError;
use parking_lot::RwLock;

#[cfg(feature = "server-protocol")]
pub(crate) use blob::validate_sync_blob_manifest;
pub(crate) use bootstrap::{
    install_sync_bootstrap, prepare_sync_bootstrap, rebuild_replica_candidate,
};
pub(crate) use commit::{
    SYNC_CHECKPOINT_SOURCE_SPACE, SYNC_MATERIALIZED_STATE_ALIAS_SPACE,
    stage_delete_materialized_sync_state_alias, stage_delete_sync_checkpoint_source,
    stage_sync_checkpoint_source,
};
pub(crate) use commit::{
    SyncCommit, SyncCommitMemberRef, SyncCommitStateAlias, encode_sync_commit_member,
};
pub(crate) use contract::SyncTransport;
pub(crate) use http::normalize_sync_locator;
pub(crate) use platform::sleep;
pub(crate) use platform::{AuthorityHttp, authority_http};
#[cfg(target_family = "wasm")]
#[doc(hidden)]
pub use platform::{
    BROWSER_TRANSPORT_CONFIG_HEADER, register_browser_sync_transport,
    unregister_browser_sync_transport,
};
pub(crate) use platform::{SyncTransportBounds, SyncTransportFuture};
#[cfg(any(test, feature = "server-protocol"))]
pub(crate) use protocol::SyncRefUpdate;
pub(crate) use protocol::{
    SyncBlobChunk, SyncBlobManifest, SyncBlobRegistration, SyncBranchHead,
    SyncCheckpointInventoryPage, SyncCommitHeader, SyncEvent, SyncHistoryBoundary,
    SyncHistoryResponse, SyncPushRequest, SyncPushResponse, SyncRepositoryPullResponse,
    SyncSnapshotRow, SyncSnapshotRowPage, encoded_delta_event_len,
};
#[cfg(feature = "server-protocol")]
pub(crate) use repository::admit_sync_authority_storage;
pub(crate) use repository::has_any_sync_replica_state;
pub(crate) use repository::{
    AUTHORITY_STATE_VALUE, SYNC_AUTHORITY_STATE_SPACE, SYNC_REPLICA_STATE_SPACE,
    SYNC_REPOSITORY_EVENT_SPACE, SYNC_SEQUENCE_SPACE, authority_state_key,
    load_pending_sync_export_commit_ids, load_replayable_repository_event_commit_ids,
    replica_state_key, stage_repository_transaction_event, stage_sync_restore_intents,
    validate_repository_transaction_event_transfer,
};
pub(crate) use repository::{
    ReplicaRebuildSource, inspect_replica_rebuild_source, replica_replacement_unavailable,
};
pub(crate) use runtime::{SyncDemand, SyncDemandRetry, SyncRuntime};
pub(crate) use upload_plan::{
    SYNC_UPLOAD_GENERATION_SPACE, stage_invalidate as stage_upload_plan_invalidation,
};
pub(crate) use upload_proof::SYNC_UPLOAD_PROOF_SPACE;

pub(crate) const MAX_SYNC_PULL_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_SYNC_HISTORY_PAGE_SIZE: usize = 100;
pub(crate) const MAX_SYNC_BLOB_BATCH_ITEMS: usize = 16;
pub(crate) const MAX_SYNC_REQUEST_ITEMS: usize = 512;
pub(crate) const SYNC_LONG_POLL_TIMEOUT: Duration = Duration::from_secs(30);
// v12 requires explicit checkpoint coordinates for authoritative partial merges.
// SDK and server must upgrade together.
pub(crate) const SYNC_PROTOCOL_VERSION: u32 = 12;
pub(crate) const SYNC_PROTOCOL_VERSION_HEADER: &str = "lix-sync-protocol-version";
pub(crate) const SYNC_PROTOCOL_MISMATCH_CODE: &str = "LIX_SYNC_PROTOCOL_MISMATCH";
pub(crate) const SYNC_REPOSITORY_ID_MISMATCH_CODE: &str = "LIX_SYNC_REPOSITORY_ID_MISMATCH";
pub(crate) const SYNC_IMMUTABLE_OBJECT_MISMATCH_CODE: &str = "LIX_SYNC_IMMUTABLE_OBJECT_MISMATCH";
const MAX_SYNC_REMOTE_ID_BYTES: usize = 4 * 1024;

/// Unforgeable outside the sync module tree. Passing this token makes the
/// durable replica-cache write bypass a compile-time capability, rather than a
/// process-local role flag or a generally callable crate helper.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CertifiedReplicaWriteCapability {
    _private: (),
}

/// Only the partial sync owner can bypass the ordinary receipt write fence.
/// This capability proves ownership of installation, never full-state coverage.
pub(crate) struct PartialReplicaWriteCapability {
    _private: (),
}

fn partial_replica_write_capability() -> PartialReplicaWriteCapability {
    PartialReplicaWriteCapability { _private: () }
}

fn certified_replica_write_capability() -> CertifiedReplicaWriteCapability {
    CertifiedReplicaWriteCapability { _private: () }
}

pub(crate) fn sync_server_protocol_mismatch(server_version: Option<u32>) -> LixError {
    let server = server_version
        .map(|version| version.to_string())
        .unwrap_or_else(|| "missing".to_owned());
    LixError::new(
        SYNC_PROTOCOL_MISMATCH_CODE,
        format!(
            "incompatible sync protocol: client version {SYNC_PROTOCOL_VERSION}, server version {server}; upgrade the client and server to compatible Lix versions"
        ),
    )
    .with_details(serde_json::json!({
        "clientSyncProtocolVersion": SYNC_PROTOCOL_VERSION,
        "serverSyncProtocolVersion": server_version,
    }))
}

pub(crate) fn sync_server_protocol_missing_field(field: &str) -> LixError {
    LixError::new(
        SYNC_PROTOCOL_MISMATCH_CODE,
        format!("incompatible sync protocol: server handshake omitted required {field}"),
    )
    .with_details(serde_json::json!({ "missingField": field }))
}

pub(crate) fn sync_repository_id_mismatch(local: &str, authority: &str) -> LixError {
    LixError::new(
        SYNC_REPOSITORY_ID_MISMATCH_CODE,
        "sync authority lixId does not match the local repository",
    )
    .with_details(serde_json::json!({
        "localLixId": local,
        "authorityLixId": authority,
    }))
}

#[cfg(feature = "server-protocol")]
pub(crate) fn sync_client_protocol_mismatch(client_version: Option<u32>) -> LixError {
    let client = client_version
        .map(|version| version.to_string())
        .unwrap_or_else(|| "invalid".to_owned());
    LixError::new(
        SYNC_PROTOCOL_MISMATCH_CODE,
        format!(
            "incompatible sync protocol: client version {client}, server version {SYNC_PROTOCOL_VERSION}; upgrade the client and server to compatible Lix versions"
        ),
    )
    .with_details(serde_json::json!({
        "clientSyncProtocolVersion": client_version,
        "serverSyncProtocolVersion": SYNC_PROTOCOL_VERSION,
    }))
}

pub(crate) fn validate_blake3_id(value: &str, context: &str) -> Result<(), LixError> {
    if value.len() == 64
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return Ok(());
    }
    Err(LixError::new(
        LixError::CODE_INVALID_PARAM,
        format!("{context} must be 64 lowercase hexadecimal characters"),
    ))
}

pub(crate) fn validate_sync_remote_id(remote_id: &str) -> Result<(), LixError> {
    if remote_id.is_empty() || remote_id.len() > MAX_SYNC_REMOTE_ID_BYTES {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("sync remoteId must contain 1 to {MAX_SYNC_REMOTE_ID_BYTES} bytes"),
        ));
    }
    Ok(())
}

pub(crate) fn validate_sync_branch_id(branch_id: &str) -> Result<(), LixError> {
    if crate::storage_codec::id_string::uuid_bytes_from_canonical(branch_id).is_none() {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync branchId must be a canonical UUID",
        ));
    }
    Ok(())
}

/// Process-wide role shared by every session on one repository engine.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum SyncRole {
    #[default]
    Disabled,
    Authority,
    Replica,
    PartialReplica,
}

impl SyncRole {
    pub(crate) fn is_replica(self) -> bool {
        matches!(self, Self::Replica | Self::PartialReplica)
    }
}

/// The complete process-local sync coordination state.
///
/// SQL reads local native state. Partial engines additionally share their
/// bounded in-memory logical-interest registry with the sync owner; this is
/// not a durable coverage receipt or permission to publish remote roots.
#[derive(Clone, Debug)]
pub(crate) struct SyncModeState {
    role: Arc<AtomicU8>,
    replica_remote_id: Arc<RwLock<Option<Arc<str>>>>,
    partial_admission: Arc<RwLock<Option<Arc<PartialReplicaState>>>>,
    partial_failure: Arc<RwLock<Option<LixError>>>,
    read_interests: Arc<RwLock<Option<Arc<crate::hot_state::ReadInterestRegistry>>>>,
    change_watch: tokio::sync::watch::Sender<u64>,
}

impl Default for SyncModeState {
    fn default() -> Self {
        Self {
            role: Arc::new(AtomicU8::new(SyncRole::Disabled as u8)),
            replica_remote_id: Arc::new(RwLock::new(None)),
            partial_admission: Arc::new(RwLock::new(None)),
            partial_failure: Arc::new(RwLock::new(None)),
            read_interests: Arc::new(RwLock::new(None)),
            change_watch: tokio::sync::watch::channel(0).0,
        }
    }
}

impl SyncModeState {
    pub(crate) fn read_interests(&self) -> Option<Arc<crate::hot_state::ReadInterestRegistry>> {
        self.read_interests.read().clone()
    }
    pub(crate) fn set_read_interests(&self, registry: Arc<crate::hot_state::ReadInterestRegistry>) {
        *self.read_interests.write() = Some(registry);
    }

    pub(crate) fn role(&self) -> SyncRole {
        match self.role.load(Ordering::Acquire) {
            value if value == SyncRole::Disabled as u8 => SyncRole::Disabled,
            value if value == SyncRole::Authority as u8 => SyncRole::Authority,
            value if value == SyncRole::Replica as u8 => SyncRole::Replica,
            value if value == SyncRole::PartialReplica as u8 => SyncRole::PartialReplica,
            _ => unreachable!("sync role stores only enum discriminants"),
        }
    }

    pub(crate) fn set_role(&self, role: SyncRole) {
        self.role.store(role as u8, Ordering::Release);
    }

    pub(crate) fn replica_remote_id(&self) -> Option<Arc<str>> {
        self.replica_remote_id.read().clone()
    }

    pub(crate) fn set_replica_remote_id(&self, remote_id: impl Into<Arc<str>>) {
        *self.replica_remote_id.write() = Some(remote_id.into());
    }

    /// Capture the authenticated admission before exposing partial SQL writes.
    /// Each transaction retains this immutable binding across later resets.
    pub(crate) fn admit_partial_replica(
        &self,
        state: Arc<PartialReplicaState>,
        _capability: PartialReplicaWriteCapability,
    ) {
        self.set_replica_remote_id(state.remote_id());
        *self.partial_admission.write() = Some(state);
        self.set_role(SyncRole::PartialReplica);
    }

    /// Terminal for this engine instance. Reopen from the durable receipt to
    /// recover; re-admission must not accidentally clear an ambiguous outcome.
    pub(crate) fn fail_partial_admission(&self, error: LixError) {
        self.partial_failure.write().get_or_insert(error);
    }

    pub(crate) fn ensure_partial_admission_healthy(&self) -> Result<(), LixError> {
        match self.partial_failure.read().as_ref() {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    pub(crate) fn partial_admission(&self) -> Option<Arc<PartialReplicaState>> {
        self.partial_admission.read().clone()
    }

    pub(crate) fn change_watcher(&self) -> tokio::sync::watch::Receiver<u64> {
        self.change_watch.subscribe()
    }

    pub(crate) fn notify_sync_change(&self) {
        self.change_watch
            .send_modify(|version| *version = version.wrapping_add(1));
    }
}

#[cfg(test)]
pub(crate) use bootstrap::durable_memory_for_test;

mod partial_write_frontier;

mod partial_candidate_prepare;
pub(crate) use partial_candidate_prepare::{
    PreparedCandidateState, prepare_candidate_native_interests,
};

mod pending_conversion;
pub(crate) use pending_conversion::{
    ConversionJournalOwner, ReconciledPendingConversion, cleanup_pending_conversion_authenticated,
    finish_conversion_cleanup_bounded, reconcile_pending_conversion_authenticated,
};
pub(crate) use repository::{
    FullConversionManifest, InspectedFullConversion, inspect_full_conversion_manifest,
    ordinary_pending_conversion_branches, pending_selected_conversion_request,
};

mod native_migration_admission;
mod native_migration_pin_upload;
pub(crate) use native_migration_admission::{
    AdmittedNativeMigrationMerge, NativeMigrationAdmission, admit_native_migration_merge,
};

mod native_migration_protocol;
pub(crate) use native_migration_protocol::NativeMigrationMergeRequest;

mod native_migration_cleanup;
pub(crate) use native_migration_cleanup::NativeMigrationCleanupRequest;

#[cfg(test)]
pub(crate) use native_migration_cleanup::native_migration_cleanup_guards;

mod native_global_migration_protocol;
pub(crate) use native_global_migration_protocol::{
    NativeGlobalBodyWaveRequest, NativeGlobalMigrationReceipt, NativeGlobalMigrationRequest,
    NativeNewBranchCoordinate,
};
mod migration_global_descriptor_proof;

mod native_global_migration_receipt;
pub(crate) use native_global_migration_receipt::NATIVE_GLOBAL_MIGRATION_RECEIPT_SPACE;
pub(crate) use repository::VerifiedGlobalMigrationBody;

pub(crate) use native_global_migration_receipt::uncommitted_global_migration_guard;

mod native_global_migration_admission;
pub(crate) use native_global_migration_admission::{
    AdmittedNativeGlobalMigration, NativeGlobalMigrationAdmission, admit_native_global_migration,
};
pub(crate) use native_global_migration_receipt::load_native_global_migration_receipt;

mod native_global_migration_cleanup;
pub(crate) use native_global_migration_cleanup::{
    AuthorizedGlobalMigrationCleanup, authorize_global_migration_cleanup,
};

mod native_global_migration_restart;
pub(crate) use native_global_migration_restart::{
    AuthorizedGlobalRestart, NativeGlobalRestartReceipt, NativeGlobalRestartRequest,
    require_unaborted_global_migration, stage_restart_native_global_migration,
};

mod native_global_conversion_driver;
pub(crate) use migration_global_descriptor_proof::prove_local_descriptor_global_source;
pub(crate) use native_global_conversion_driver::{
    GlobalConversionJournalOwner, ReconciledGlobalConversion,
    cleanup_global_conversion_authenticated, global_new_branch_upload_boundaries,
    reconcile_global_conversion_authenticated, verify_global_conversion_baseline_authenticated,
    verify_resumed_global_basis_authenticated,
};
pub(crate) use pending_conversion::MigrationGlobalSuccessor;
pub(crate) use repository::{
    DescriptorGlobalConversion, classify_descriptor_global_conversion,
    pending_conversion_request_after_global,
};

pub(crate) use partial_open::authenticate_partial_source_conversion;

pub(crate) use partial_state::upgrade_owned_partial_receipt;
mod partial_branch_switch;
pub(crate) use partial_branch_switch::{PartialBranchSwitchCompletion, switch_existing_branch};

mod partial_created_refs;
