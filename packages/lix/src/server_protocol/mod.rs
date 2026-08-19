//! Canonical Lix Server Protocol implementation.

#![cfg_attr(test, allow(clippy::large_futures))]

#[cfg(feature = "server-protocol")]
mod handler;
#[cfg(feature = "server-protocol")]
pub use handler::*;

#[cfg(feature = "server-protocol-client")]
pub mod client;
