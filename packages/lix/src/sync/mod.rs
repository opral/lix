//! Repository-scoped synchronization.
//!
//! Lix synchronizes its existing primitives: complete immutable commits,
//! compare-and-swap branch refs, and BLAKE3-addressed binary chunks. Live
//! synchronization has one ordered repository cursor. Current rows and commit
//! topology bootstraps eagerly; historical commit payloads and blobs load on
//! demand. Platform-specific code is limited to tasks, timers, HTTP, and
//! cancellation.

mod blob;
mod bootstrap;
mod commit;
mod contract;
mod http;
mod platform;
mod protocol;
mod repository;
mod runtime;
#[cfg(test)]
mod simulation_tests;

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use crate::LixError;

#[cfg(feature = "server-protocol")]
pub(crate) use blob::validate_sync_blob_manifest;
pub(crate) use commit::{
    SyncCommit, SyncCommitMemberRef, SyncCommitStateAlias, encode_sync_commit_member,
};
pub(crate) use bootstrap::{
    SyncBootstrapAdmission, inspect_sync_bootstrap_with_adapter, install_sync_bootstrap,
    prepare_sync_bootstrap,
};
pub(crate) use http::normalize_sync_locator;
pub(crate) use contract::SyncTransport;
#[cfg(target_family = "wasm")]
#[doc(hidden)]
pub use platform::{
    BROWSER_TRANSPORT_CONFIG_HEADER, register_browser_sync_transport,
    unregister_browser_sync_transport,
};
pub(crate) use platform::{SyncTransportBounds, SyncTransportFuture};
pub(crate) use platform::sleep;
#[cfg(feature = "server-protocol")]
pub(crate) use protocol::SyncRefUpdate;
pub(crate) use protocol::{
    SyncBlobChunk, SyncBlobManifest, SyncBlobRegistration, SyncBranchHead, SyncCommitHeader,
    SyncEvent, SyncHistoryBoundary, SyncHistoryResponse, SyncPushRequest, SyncPushResponse,
    SyncRepositoryPullResponse, SyncSnapshotRow, SyncSnapshotRowPage, encoded_delta_event_len,
};
pub(crate) use commit::{
    SYNC_MATERIALIZED_STATE_ALIAS_SPACE, stage_delete_materialized_sync_state_alias,
};
pub(crate) use repository::{
    SYNC_REPLICA_STATE_SPACE, SYNC_REPOSITORY_EVENT_SPACE, SYNC_SEQUENCE_SPACE,
    load_pending_sync_export_commit_ids, load_replayable_repository_event_commit_ids,
    stage_repository_transaction_event,
    validate_repository_transaction_event_transfer,
};
#[cfg(feature = "server-protocol")]
pub(crate) use repository::has_any_sync_replica_state;
pub(crate) use runtime::{
    SyncDemand, SyncDemandRetry, SyncRuntime, activate_sync_mode,
};

pub(crate) const MAX_SYNC_PULL_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_SYNC_HISTORY_PAGE_SIZE: usize = 100;
pub(crate) const MAX_SYNC_BLOB_BATCH_ITEMS: usize = 16;
pub(crate) const MAX_SYNC_REQUEST_ITEMS: usize = 512;
pub(crate) const SYNC_LONG_POLL_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_SYNC_REMOTE_ID_BYTES: usize = 4 * 1024;

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
}

/// The complete process-local sync coordination state.
///
/// There are no query scopes, hydration registries, branch bindings, or file
/// projection caches. SQL always reads the local hot state. This object only
/// identifies the role and wakes long-polls after local commits. The single
/// runtime worker serializes remote repository events.
#[derive(Clone, Debug)]
pub(crate) struct SyncModeState {
    role: Arc<AtomicU8>,
    change_watch: tokio::sync::watch::Sender<u64>,
}

impl Default for SyncModeState {
    fn default() -> Self {
        Self {
            role: Arc::new(AtomicU8::new(SyncRole::Disabled as u8)),
            change_watch: tokio::sync::watch::channel(0).0,
        }
    }
}

impl SyncModeState {
    pub(crate) fn role(&self) -> SyncRole {
        match self.role.load(Ordering::Acquire) {
            value if value == SyncRole::Disabled as u8 => SyncRole::Disabled,
            value if value == SyncRole::Authority as u8 => SyncRole::Authority,
            value if value == SyncRole::Replica as u8 => SyncRole::Replica,
            _ => unreachable!("sync role stores only enum discriminants"),
        }
    }

    pub(crate) fn set_role(&self, role: SyncRole) {
        self.role.store(role as u8, Ordering::Release);
    }

    pub(crate) fn change_watcher(&self) -> tokio::sync::watch::Receiver<u64> {
        self.change_watch.subscribe()
    }

    pub(crate) fn notify_sync_change(&self) {
        self.change_watch
            .send_modify(|version| *version = version.wrapping_add(1));
    }
}
