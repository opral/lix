//! Target selection for the synchronization runtime adapter.
//!
//! The shared state machine imports only these capabilities. Their native and
//! browser implementations intentionally contain no synchronization policy.

use crate::LixError;

#[cfg(not(target_family = "wasm"))]
mod native;
#[cfg(not(target_family = "wasm"))]
mod native_http;
#[cfg(target_family = "wasm")]
mod wasm;
#[cfg(target_family = "wasm")]
mod wasm_http;

#[cfg(not(target_family = "wasm"))]
pub(super) use native::{SyncTask, sleep, spawn_sync_task};
#[cfg(not(target_family = "wasm"))]
pub(super) type HttpSyncTransport = super::http::HttpSyncTransport<native_http::NativeHttpClient>;
#[cfg(target_family = "wasm")]
pub(super) use wasm::{SyncTask, sleep, spawn_sync_task};
#[cfg(target_family = "wasm")]
pub(super) type HttpSyncTransport = super::http::HttpSyncTransport<wasm_http::BrowserHttpClient>;
#[cfg(target_family = "wasm")]
#[doc(hidden)]
pub use wasm_http::{
    BROWSER_TRANSPORT_CONFIG_HEADER, register_browser_sync_transport,
    unregister_browser_sync_transport,
};

/// Target-appropriate future returned by a synchronization transport.
///
/// Native request futures may cross worker threads. Browser fetch promises
/// remain on the JavaScript event loop. This mechanical distinction is kept
/// here so the shared transport contract has one definition.
#[cfg(not(target_family = "wasm"))]
pub type SyncTransportFuture<'a, T> =
    futures_util::future::BoxFuture<'a, Result<T, LixError>>;
#[cfg(target_family = "wasm")]
pub type SyncTransportFuture<'a, T> = futures_util::future::LocalBoxFuture<'a, Result<T, LixError>>;

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
            ("blob.rs", include_str!("blob.rs")),
            ("commit.rs", include_str!("commit.rs")),
            ("contract.rs", include_str!("contract.rs")),
            ("http.rs", include_str!("http.rs")),
            ("protocol.rs", include_str!("protocol.rs")),
            ("repository.rs", include_str!("repository.rs")),
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
        assert!(!runtime.contains("SYNC_TOPOLOGY_INTERVAL"));
        assert!(!runtime.contains("inactive_branch"));
        assert!(!runtime.contains("hydrated_scope"));
    }

    #[test]
    fn transport_is_repository_scoped() {
        let contract = include_str!("contract.rs");
        assert!(contract.contains("fn push"));
        assert!(contract.contains("fn pull"));
        assert!(contract.contains("fn history"));
        assert!(!contract.contains("fn admit"));
        assert!(!contract.contains("list_branches"));
        assert!(!contract.contains("schema_keys"));
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
            if name.ends_with("_http.rs") {
                assert!(!source.contains("activeBranchId"), "{name}");
                assert!(!source.contains("list_branches"), "{name}");
                assert!(!source.contains("/sync/"), "{name}");
                assert!(!source.contains("HandshakeResponse"), "{name}");
                assert!(!source.contains("ErrorResponse"), "{name}");
                assert!(!source.contains("MAX_SYNC_HISTORY"), "{name}");
                assert!(!source.contains("from_secs(5)"), "{name}");
            }
        }
        let http = include_str!("http.rs");
        assert!(http.contains("/sync/push"));
        assert!(http.contains("/sync/history"));
        assert!(http.contains("HandshakeResponse"));
        assert!(http.contains("response_error"));
        assert!(include_str!("platform/native.rs").contains("tokio::runtime"));
        assert!(include_str!("platform/native_http.rs").contains("reqwest::"));
        assert!(include_str!("platform/wasm.rs").contains("spawn_local"));
        assert!(include_str!("platform/wasm_http.rs").contains("AbortController"));
    }
}
