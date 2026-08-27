//! Canonical Lix Server Protocol implementation.

#![cfg_attr(test, allow(clippy::large_futures))]

/// Current Lix Server Protocol wire version.
pub const PROTOCOL_VERSION: u32 = crate::SERVER_PROTOCOL_VERSION;

#[cfg(feature = "server-protocol")]
mod handler;
#[cfg(feature = "server-protocol")]
pub use handler::*;

#[cfg(feature = "server-protocol-client")]
pub mod client;
