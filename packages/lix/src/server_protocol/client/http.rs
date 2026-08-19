//! Host-supplied HTTP transport for the protocol client.
//!
//! This trait is WASM-safe: it does not depend on `reqwest`, Tokio I/O, or
//! the `http` crate. Browser hosts implement it with `fetch`; tests implement
//! it with a scripted dispatcher.

use crate::LixError;
use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

/// One outbound protocol request. Paths are relative to the `/lix/v1/` base.
#[derive(Clone, Debug)]
pub struct ProtocolHttpRequest {
    pub method: &'static str,
    pub path: String,
    pub query: Vec<(String, String)>,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
}

/// Buffered JSON/empty response.
#[derive(Clone, Debug)]
pub struct ProtocolHttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl ProtocolHttpResponse {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find_map(|(key, value)| {
            key.eq_ignore_ascii_case(name).then_some(value.as_str())
        })
    }
}

/// Streaming response body for SSE observe.
#[async_trait]
pub trait ProtocolHttpStream: Send {
    async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, LixError>;
}

/// Streaming HTTP response (observe multiplex).
pub struct ProtocolHttpStreamResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Box<dyn ProtocolHttpStream>,
}

impl ProtocolHttpStreamResponse {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find_map(|(key, value)| {
            key.eq_ignore_ascii_case(name).then_some(value.as_str())
        })
    }
}

/// Host HTTP + timer surface used by [`super::ServerProtocolClient`].
///
/// Implementors must be `Send + Sync` so the client can share them across
/// execute and observe tasks. WASM hosts `unsafe impl` this for JS callbacks,
/// matching the existing telemetry dispatch pattern.
#[async_trait]
pub trait ProtocolHttp: Send + Sync {
    async fn request(
        &self,
        request: ProtocolHttpRequest,
    ) -> Result<ProtocolHttpResponse, LixError>;

    async fn request_stream(
        &self,
        request: ProtocolHttpRequest,
    ) -> Result<ProtocolHttpStreamResponse, LixError>;

    async fn sleep(&self, duration: Duration);

    /// Run a background protocol task (observe stream, deferred restart).
    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>);
}

pub(crate) fn header_value<'a>(
    headers: &'a [(String, String)],
    name: &str,
) -> Option<&'a str> {
    headers
        .iter()
        .find_map(|(key, value)| key.eq_ignore_ascii_case(name).then_some(value.as_str()))
}
