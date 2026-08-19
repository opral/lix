//! Canonical Lix Server Protocol.
//!
//! The server lives behind `server-protocol`. The client lives behind
//! `server-protocol-client` and is WASM-safe: hosts supply HTTP through
//! [`client::ProtocolHttp`]. Enabling `server-protocol` includes the client.

#[cfg(feature = "server-protocol")]
mod server;
#[cfg(feature = "server-protocol")]
pub use server::*;

#[cfg(feature = "server-protocol-client")]
pub mod client;
