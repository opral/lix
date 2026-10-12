//! Canonical Lix Server Protocol implementation.

#![cfg_attr(test, allow(clippy::large_futures))]

/// Current Lix Server Protocol wire version.
pub const PROTOCOL_VERSION: u32 = crate::SERVER_PROTOCOL_VERSION;

#[cfg(feature = "server-protocol")]
mod fingerprint;
#[cfg(feature = "server-protocol")]
mod handler;
#[cfg(feature = "server-protocol")]
mod request_value;
#[cfg(feature = "server-protocol")]
pub use handler::*;

#[cfg(feature = "server-protocol-client")]
pub mod client {
    pub use crate::authority_client::*;
    /// The protocol's atomic statement batch, which serves remote
    /// `execute_many`. Not part of the `Lix` API.
    pub use crate::session::{ExecuteBatchResult, ExecuteBatchStatement};
}

#[cfg(feature = "server-protocol")]
mod lifecycle;
#[cfg(feature = "server-protocol")]
pub use lifecycle::*;
