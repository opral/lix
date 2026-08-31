use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use bytes::Bytes;
use futures_core::Stream;

use crate::LixError;

#[cfg(not(target_arch = "wasm32"))]
pub type ProtocolByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, LixError>> + Send>>;
#[cfg(target_arch = "wasm32")]
pub type ProtocolByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, LixError>>>>;

#[cfg(not(target_arch = "wasm32"))]
pub type StreamCancel = std::sync::Arc<dyn Fn() + Send + Sync>;
#[cfg(target_arch = "wasm32")]
pub type StreamCancel = std::sync::Arc<dyn Fn()>;

#[derive(Debug, Clone)]
pub struct ProtocolHttpRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Bytes>,
}

impl ProtocolHttpRequest {
    pub fn header(&self, name: &str) -> Option<&str> {
        header_value(&self.headers, name)
    }
}

#[derive(Debug, Clone)]
pub struct ProtocolHttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
}

impl ProtocolHttpResponse {
    pub fn header(&self, name: &str) -> Option<&str> {
        header_value(&self.headers, name)
    }
}

pub struct ProtocolHttpStream {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: ProtocolByteStream,
    pub cancel: StreamCancel,
}

impl ProtocolHttpStream {
    pub fn header(&self, name: &str) -> Option<&str> {
        header_value(&self.headers, name)
    }
}

impl std::fmt::Debug for ProtocolHttpStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtocolHttpStream")
            .field("status", &self.status)
            .field("headers", &self.headers)
            .finish_non_exhaustive()
    }
}

pub fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers.iter().find_map(|(key, value)| {
        key.eq_ignore_ascii_case(name)
            .then_some(value.as_str())
    })
}

#[cfg(not(target_arch = "wasm32"))]
pub trait ProtocolHttp: Send + Sync {
    fn request(
        &self,
        request: ProtocolHttpRequest,
    ) -> impl Future<Output = Result<ProtocolHttpResponse, LixError>> + Send;

    fn request_stream(
        &self,
        request: ProtocolHttpRequest,
    ) -> impl Future<Output = Result<ProtocolHttpStream, LixError>> + Send;

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send;

    fn spawn(&self, fut: Pin<Box<dyn Future<Output = ()> + Send>>);
}

#[cfg(target_arch = "wasm32")]
pub trait ProtocolHttp {
    fn request(
        &self,
        request: ProtocolHttpRequest,
    ) -> impl Future<Output = Result<ProtocolHttpResponse, LixError>>;

    fn request_stream(
        &self,
        request: ProtocolHttpRequest,
    ) -> impl Future<Output = Result<ProtocolHttpStream, LixError>>;

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()>;

    fn spawn(&self, fut: Pin<Box<dyn Future<Output = ()>>>);
}
