//! Target selection for the synchronization runtime adapter.
//!
//! The shared state machine imports only these capabilities. Their native and
//! browser implementations intentionally contain no synchronization policy.

#[cfg(not(target_family = "wasm"))]
pub(super) use super::platform_native::{
    HttpSyncTransport, SyncTask, sleep, spawn_sync_task,
};
#[cfg(target_family = "wasm")]
pub(super) use super::platform_wasm::{HttpSyncTransport, SyncTask, sleep, spawn_sync_task};
