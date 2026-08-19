//! Repository-scoped synchronization.
//!
//! Lix synchronizes its existing primitives: complete immutable commits,
//! compare-and-swap branch refs, and BLAKE3-addressed binary chunks. Live
//! synchronization has one ordered repository cursor. Current rows and commit
//! topology bootstraps eagerly; historical commit payloads and blobs load on
//! demand. Platform-specific code is limited to tasks, timers, HTTP, and
//! cancellation.

mod blob;
pub(crate) mod commit;
mod contract;
mod platform;
mod protocol;
pub(crate) mod repository;
mod runtime;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::LixError;

pub(crate) use commit::SyncCommit;
pub(crate) use contract::SyncTransport;
#[cfg(target_family = "wasm")]
pub use platform::{
    BROWSER_TRANSPORT_CONFIG_HEADER, register_browser_sync_transport,
    unregister_browser_sync_transport,
};
pub(crate) use platform::{SyncTransportBounds, SyncTransportFuture};
pub(crate) use protocol::{
    SyncBlobChunk, SyncBlobManifest, SyncBlobRegistration, SyncBranchHead, SyncCommitHeader,
    SyncEvent, SyncHistoryResponse, SyncPushRequest, SyncPushResponse, SyncRefUpdate,
    SyncRepositoryPullResponse, SyncSnapshotRow, SyncSnapshotRowPage, encoded_delta_event_len,
};
pub(crate) use runtime::{
    PreparedSync, SyncDemand, SyncRuntime, activate_sync_mode, demand_sync_for_error,
    prepare_sync_mode,
};

pub(crate) const MAX_SYNC_PULL_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_SYNC_HISTORY_COMMIT_IDS: usize = 128;
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
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) enum SyncRole {
    #[default]
    Disabled,
    Authority,
    Replica {
        remote_id: String,
    },
}

/// The complete process-local sync coordination state.
///
/// There are no query scopes, hydration registries, branch bindings, or file
/// projection caches. SQL always reads the local hot state. This object only
/// identifies the role, wakes long-polls after local commits, and serializes
/// application of remote repository events.
#[derive(Clone, Debug)]
pub(crate) struct SyncModeState {
    role: Arc<RwLock<SyncRole>>,
    change_version: Arc<AtomicU64>,
    change_watch: Arc<tokio::sync::watch::Sender<u64>>,
    apply_gate: Arc<tokio::sync::Mutex<()>>,
}

impl Default for SyncModeState {
    fn default() -> Self {
        Self {
            role: Arc::new(RwLock::new(SyncRole::Disabled)),
            change_version: Arc::new(AtomicU64::new(0)),
            change_watch: Arc::new(tokio::sync::watch::channel(0).0),
            apply_gate: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
}

impl SyncModeState {
    pub(crate) fn role(&self) -> Result<SyncRole, LixError> {
        self.role.read().map(|role| role.clone()).map_err(|_| {
            LixError::new(LixError::CODE_INTERNAL_ERROR, "sync mode state is poisoned")
        })
    }

    pub(crate) fn set_role(&self, role: SyncRole) -> Result<(), LixError> {
        *self.role.write().map_err(|_| {
            LixError::new(LixError::CODE_INTERNAL_ERROR, "sync mode state is poisoned")
        })? = role;
        Ok(())
    }

    pub(crate) fn change_watcher(&self) -> tokio::sync::watch::Receiver<u64> {
        self.change_watch.subscribe()
    }

    pub(crate) fn notify_sync_change(&self) {
        let version = self.change_version.fetch_add(1, Ordering::AcqRel) + 1;
        self.change_watch.send_replace(version);
    }

    pub(crate) async fn lock_apply_gate(&self) -> tokio::sync::OwnedMutexGuard<()> {
        Arc::clone(&self.apply_gate).lock_owned().await
    }
}
