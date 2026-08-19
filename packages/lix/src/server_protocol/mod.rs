//! Canonical Lix Server Protocol.
//!
//! The server lives behind `server-protocol`. The client lives behind
//! `server-protocol-client` and is WASM-safe: hosts supply HTTP through
//! [`ProtocolHttp`]. Enabling `server-protocol` includes the client.
//!
//! The client is a host binding, not an application API. Applications use
//! SQL and `openLix({ server })`. This module re-exports only the host
//! surface needed to implement that binding.

#[cfg(feature = "server-protocol")]
mod server;
#[cfg(feature = "server-protocol")]
pub use server::*;

#[cfg(feature = "server-protocol-client")]
mod client;
#[cfg(feature = "server-protocol-client")]
pub use client::{
    OpenRemoteOptions, ProtocolHttp, ProtocolHttpRequest, ProtocolHttpResponse, ProtocolHttpStream,
    ProtocolHttpStreamResponse, RemoteExecuteOptions, RemoteObserveEvents, RemoteTransaction,
    ServerProtocolClient,
};
