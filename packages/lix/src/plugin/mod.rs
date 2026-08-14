//! Lix plugin authoring and runtime integration.
//!
//! Component plugins compile against the authoring facade in this module on
//! `wasm32-wasip2`. Repository engines compile the runtime implementation
//! instead. The target split keeps the plugin-authoring dependency as simple
//! as `lix = "..."` without pulling the repository engine into the guest.

#[cfg(any(
    not(target_arch = "wasm32"),
    all(target_arch = "wasm32", target_os = "wasi", target_env = "p2")
))]
#[doc(hidden)]
pub mod api;

pub mod wire;

#[cfg(not(all(target_arch = "wasm32", target_os = "wasi", target_env = "p2")))]
pub mod runtime;

// Engine internals consume the single component-runtime contract through the
// plugin owner. Keep these crate-private so the public plugin authoring API is
// still the target-neutral `api`/`wire` surface above.
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi", target_env = "p2")))]
pub(crate) use runtime::*;

#[cfg(any(
    not(target_arch = "wasm32"),
    all(target_arch = "wasm32", target_os = "wasi", target_env = "p2")
))]
pub use api::*;

#[cfg(any(
    not(target_arch = "wasm32"),
    all(target_arch = "wasm32", target_os = "wasi", target_env = "p2")
))]
pub use crate::__lix_export_plugin as export;
