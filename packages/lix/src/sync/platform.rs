//! Target selection for the synchronization runtime adapter.
//!
//! The shared state machine imports only these capabilities. Their native and
//! browser implementations intentionally contain no synchronization policy.

use crate::LixError;
use std::future::Future;
use std::pin::Pin;

#[cfg(not(target_family = "wasm"))]
#[path = "platform/native.rs"]
mod native;
#[cfg(not(target_family = "wasm"))]
#[path = "platform/native_http.rs"]
mod native_transport;
#[cfg(target_family = "wasm")]
#[path = "platform/wasm.rs"]
mod wasm;
#[cfg(target_family = "wasm")]
#[path = "platform/wasm_http.rs"]
mod wasm_transport;

#[cfg(not(target_family = "wasm"))]
pub(super) use native::{SyncTask, deadline, sleep, spawn_sync_task};
#[cfg(not(target_family = "wasm"))]
pub(super) use native_transport::HttpSyncTransport;
#[cfg(target_family = "wasm")]
pub(super) use wasm::{SyncTask, deadline, sleep, spawn_sync_task};
#[cfg(target_family = "wasm")]
pub(super) use wasm_transport::HttpSyncTransport;

/// Target-appropriate future returned by a synchronization transport.
///
/// Native request futures may cross worker threads. Browser fetch promises
/// remain on the JavaScript event loop. This mechanical distinction is kept
/// here so the shared transport contract has one definition.
#[cfg(not(target_family = "wasm"))]
pub type SyncTransportFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, LixError>> + Send + 'a>>;
#[cfg(target_family = "wasm")]
pub type SyncTransportFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, LixError>> + 'a>>;

/// Target-appropriate marker bound for transport handles.
#[doc(hidden)]
#[cfg(not(target_family = "wasm"))]
pub trait SyncTransportBounds: Send + Sync {}
#[cfg(not(target_family = "wasm"))]
impl<T> SyncTransportBounds for T where T: Send + Sync + ?Sized {}

#[doc(hidden)]
#[cfg(target_family = "wasm")]
pub trait SyncTransportBounds {}
#[cfg(target_family = "wasm")]
impl<T> SyncTransportBounds for T where T: ?Sized {}

/// Compile-time architecture guard: synchronization policy belongs in the
/// shared runtime/core. Only adapter modules may branch on the target.
#[cfg(test)]
mod tests {
    #[test]
    fn shared_sync_modules_have_no_target_specific_policy() {
        for (name, source) in [
            ("mod.rs", include_str!("mod.rs")),
            ("contract.rs", include_str!("contract.rs")),
            ("runtime.rs", include_str!("runtime.rs")),
        ] {
            assert!(
                !source.contains("target_family = \"wasm\"")
                    && !source.contains("target_arch = \"wasm32\"")
                    && !source.contains("reqwest::")
                    && !source.contains("spawn_local")
                    && !source.contains("AbortController"),
                "{name} contains platform policy outside the adapter boundary"
            );
        }
    }

    #[test]
    fn browser_runtime_has_no_fixed_interval_poll_loop() {
        let runtime = include_str!("runtime.rs");
        assert!(!runtime.contains("from_millis(250)"));
        assert!(!runtime.contains("SYNC_POLL_INTERVAL"));
        assert!(!runtime.contains("run_polling"));
    }

    #[test]
    fn platform_adapters_contain_mechanics_but_no_sync_policy() {
        let adapters = [
            ("native.rs", include_str!("platform/native.rs")),
            ("native_http.rs", include_str!("platform/native_http.rs")),
            ("wasm.rs", include_str!("platform/wasm.rs")),
            ("wasm_http.rs", include_str!("platform/wasm_http.rs")),
        ];
        for (name, source) in adapters {
            assert!(!source.contains("reconcile_sync_branches"), "{name}");
            assert!(!source.contains("retry_backoff"), "{name}");
            assert!(!source.contains("sync_lifecycle"), "{name}");
        }
        assert!(include_str!("platform/native.rs").contains("tokio::runtime"));
        assert!(include_str!("platform/native_http.rs").contains("reqwest::"));
        assert!(include_str!("platform/wasm.rs").contains("spawn_local"));
        assert!(include_str!("platform/wasm_http.rs").contains("AbortController"));
    }
}
