//! Native HTTP mechanics for the repository-scoped sync protocol.

use super::super::http::{
    HTTP_TIMEOUT, HttpSyncTransport, RawHttpClient, RawHttpRequest, RawHttpResponse,
    SYNC_TRANSPORT_ERROR_CODE, response_too_large,
};
use crate::LixError;
use crate::sync::{MAX_SYNC_PULL_RESPONSE_BYTES, SyncTransportFuture};
use crate::authority_client::{
    ProtocolByteStream, ProtocolHttp, ProtocolHttpRequest, ProtocolHttpResponse,
    ProtocolHttpStream,
};
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug)]
pub(crate) struct AuthorityHttp(reqwest::Client);

pub(crate) fn authority_http(
    headers: &[(String, String)],
) -> Result<AuthorityHttp, LixError> {
    Ok(AuthorityHttp(build_client(headers)?))
}

impl HttpSyncTransport<reqwest::Client> {
    /// Opens an authentication/session capability for one repository.
    pub(crate) async fn connect(
        repository_url: &str,
        headers: &[(String, String)],
    ) -> Result<Self, LixError> {
        let client = build_client(headers)?;
        Self::connect_with(client, repository_url).await
    }
}

fn build_client(headers: &[(String, String)]) -> Result<reqwest::Client, LixError> {
        let mut default_headers = reqwest::header::HeaderMap::new();
        for (name, value) in headers {
            if HttpSyncTransport::<reqwest::Client>::is_reserved_header(name) {
                continue;
            }
            let name =
                reqwest::header::HeaderName::from_bytes(name.as_bytes()).map_err(|error| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        format!("invalid sync HTTP header name: {error}"),
                    )
                })?;
            let value = reqwest::header::HeaderValue::from_str(value).map_err(|error| {
                LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("invalid sync HTTP header value: {error}"),
                )
            })?;
            default_headers.append(name, value);
        }
        reqwest::Client::builder()
            .default_headers(default_headers)
            .timeout(HTTP_TIMEOUT)
            .build()
            .map_err(|error| transport_error("configure sync transport", error))
}

impl ProtocolHttp for AuthorityHttp {
    async fn request(
        &self,
        request: ProtocolHttpRequest,
    ) -> Result<ProtocolHttpResponse, LixError> {
        let mut builder = self.0.request(
            request.method.parse::<reqwest::Method>().map_err(|error| {
                LixError::new(LixError::CODE_INVALID_PARAM, format!("invalid HTTP method: {error}"))
            })?,
            request.url,
        );
        for (name, value) in request.headers {
            builder = builder.header(name, value);
        }
        if let Some(body) = request.body {
            builder = builder.body(body);
        }
        let response = builder.send().await.map_err(|error| transport_error("authority request", error))?;
        let status = response.status().as_u16();
        let headers = response.headers().iter().map(|(name, value)| {
            (name.as_str().to_owned(), value.to_str().unwrap_or_default().to_owned())
        }).collect();
        let body = response.bytes().await.map_err(|error| transport_error("read authority response", error))?;
        Ok(ProtocolHttpResponse { status, headers, body })
    }

    async fn request_stream(
        &self,
        request: ProtocolHttpRequest,
    ) -> Result<ProtocolHttpStream, LixError> {
        let mut builder = self.0.request(
            request.method.parse::<reqwest::Method>().map_err(|error| {
                LixError::new(LixError::CODE_INVALID_PARAM, format!("invalid HTTP method: {error}"))
            })?,
            request.url,
        );
        for (name, value) in request.headers {
            builder = builder.header(name, value);
        }
        if let Some(body) = request.body {
            builder = builder.body(body);
        }
        let response = builder.send().await.map_err(|error| transport_error("authority stream", error))?;
        let status = response.status().as_u16();
        let headers = response.headers().iter().map(|(name, value)| {
            (name.as_str().to_owned(), value.to_str().unwrap_or_default().to_owned())
        }).collect();
        let body: ProtocolByteStream = Box::pin(async_stream::try_stream! {
            let mut response = response;
            while let Some(chunk) = response.chunk().await.map_err(|error| transport_error("read authority stream", error))? {
                yield Bytes::from(chunk);
            }
        });
        let cancel: Arc<dyn Fn() + Send + Sync> = Arc::new(|| {});
        Ok(ProtocolHttpStream { status, headers, body, cancel })
    }

    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }

    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        tokio::spawn(future);
    }
}

impl RawHttpClient for reqwest::Client {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        Box::pin(async move {
            let mut builder = self.request(request.method, &request.url);
            for (name, value) in &request.headers {
                builder = builder.header(name, value);
            }
            if let Some(body) = request.body {
                builder = builder.body(body);
            }
            let mut response = builder
                .send()
                .await
                .map_err(|error| transport_error(request.operation, error))?;
            let status = response.status();
            let status_text = status
                .canonical_reason()
                .map(str::to_owned)
                .unwrap_or_default();
            if response
                .content_length()
                .is_some_and(|length| length > MAX_SYNC_PULL_RESPONSE_BYTES as u64)
            {
                return Err(response_too_large(request.operation));
            }
            let mut body = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|error| transport_error(request.operation, error))?
            {
                if body.len().saturating_add(chunk.len()) > MAX_SYNC_PULL_RESPONSE_BYTES {
                    return Err(response_too_large(request.operation));
                }
                body.extend_from_slice(&chunk);
            }
            Ok(RawHttpResponse {
                status: status.as_u16(),
                status_text,
                body,
            })
        })
    }
}

fn transport_error(operation: &str, error: impl std::fmt::Display) -> LixError {
    LixError::new(SYNC_TRANSPORT_ERROR_CODE, format!("{operation}: {error}"))
}
