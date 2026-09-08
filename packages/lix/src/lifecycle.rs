//! Hosted repository creation and deletion. Opening never provisions a host resource.
use crate::authority_client::{
    ProtocolByteStream, ProtocolHttp, ProtocolHttpRequest, ProtocolHttpResponse,
};
use crate::{Lix, LixError, ServerOptions, storage_adapter::Storage};
use futures_io::AsyncRead;
use futures_util::{
    StreamExt,
    future::{Either, select},
};
use serde::{Deserialize, Serialize};
use std::{
    future::{Future, IntoFuture},
    pin::Pin,
};
use tokio_util::{
    compat::{FuturesAsyncReadCompatExt, TokioAsyncWriteCompatExt},
    io::ReaderStream,
};

const SNAPSHOT_MEDIA_TYPE: &str = "application/vnd.lix.snapshot";
type Production = Pin<Box<dyn Future<Output = Result<(), LixError>> + Send>>;

/// A hosted resource locator, independent of any open session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostedLix {
    pub id: String,
    pub url: String,
}

/// Creates an empty hosted repository, or a complete copy supplied by `from_lix`.
pub fn create_lix() -> CreateLixBuilder {
    CreateLixBuilder {
        server: None,
        source: None,
        production: None,
        idempotency_key: uuid::Uuid::now_v7().to_string(),
    }
}

/// Deletes a hosted repository. Local copies are unaffected.
pub fn delete_lix() -> DeleteLixBuilder {
    DeleteLixBuilder { server: None }
}

/// Configures explicit hosted creation. The URL addresses a host, not a repository.
pub struct CreateLixBuilder {
    server: Option<ServerOptions>,
    source: Option<ProtocolByteStream>,
    production: Option<Production>,
    idempotency_key: String,
}
impl std::fmt::Debug for CreateLixBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreateLixBuilder")
            .field("has_source", &self.source.is_some())
            .finish_non_exhaustive()
    }
}
impl CreateLixBuilder {
    pub fn with_server(mut self, server: ServerOptions) -> Self {
        self.server = Some(server);
        self
    }

    /// Reuse a key to recover the result of a creation whose response was lost.
    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = key.into();
        self
    }

    /// Copies one coherent snapshot without changing or connecting the source.
    /// Untracked application rows are part of the copy.
    pub fn from_lix<S>(mut self, source: &Lix<S>) -> Self
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        let snapshot = source.export_snapshot();
        let (reader, writer) = tokio::io::duplex(256 * 1024);
        self.source = Some(Box::pin(
            ReaderStream::new(reader).map(|chunk| chunk.map_err(snapshot_io_error)),
        ));
        // The producer owns its storage read and Send writer; no borrowed session
        // or thread-local SQL execution state crosses this future's boundary.
        self.production = Some(Box::pin(unsafe {
            crate::session::AssumeSendFuture::new(async move {
                snapshot.write_to(&mut writer.compat_write()).await?;
                Ok(())
            })
        }));
        self
    }

    /// Creates a hosted Lix from a complete snapshot stream.
    pub fn from_snapshot<R>(mut self, source: R) -> Self
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        self.production = None;
        self.source = Some(Box::pin(
            ReaderStream::new(source.compat()).map(|chunk| chunk.map_err(snapshot_io_error)),
        ));
        self
    }
}

impl IntoFuture for CreateLixBuilder {
    type Output = Result<HostedLix, LixError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;
    fn into_future(self) -> Self::IntoFuture {
        // Browser HTTP remains on its worker's event loop, matching open_lix.
        Box::pin(unsafe {
            crate::session::AssumeSendFuture::new(async move {
                let server = self
                    .server
                    .ok_or_else(|| invalid("create_lix requires a server"))?;
                let endpoint = collection_url(&server.url)?;
                validate_key(&self.idempotency_key)?;
                let http = crate::sync::authority_http(&server.headers)?;
                let mut headers = vec![("idempotency-key".into(), self.idempotency_key)];
                if self.source.is_some() {
                    headers.push(("content-type".into(), SNAPSHOT_MEDIA_TYPE.into()));
                }
                let request = ProtocolHttpRequest {
                    method: "POST".into(),
                    url: endpoint.clone(),
                    headers,
                    body: None,
                };
                let upload = Box::pin(async {
                    match self.source {
                        Some(source) => http.upload(request, source).await,
                        None => http.request(request).await,
                    }
                });
                let response = if let Some(production) = self.production {
                    match select(upload, production).await {
                        Either::Left((response, production)) => {
                            let response = response?;
                            // A successful host response must not hide a failed or
                            // truncated snapshot export. Rejections cancel export.
                            if (200..300).contains(&response.status) {
                                production.await?;
                            }
                            response
                        }
                        Either::Right((result, upload)) => {
                            result?;
                            upload.await?
                        }
                    }
                } else {
                    upload.await?
                };
                require_success(&response, 201)?;
                let hosted: HostedLix = serde_json::from_slice(&response.body).map_err(|_| {
                    invalid_response("create response is not a repository descriptor")
                })?;
                validate_descriptor(&endpoint, &hosted)?;
                Ok(hosted)
            })
        })
    }
}

#[derive(Debug)]
pub struct DeleteLixBuilder {
    server: Option<ServerOptions>,
}
impl DeleteLixBuilder {
    pub fn with_server(mut self, server: ServerOptions) -> Self {
        self.server = Some(server);
        self
    }
}
impl IntoFuture for DeleteLixBuilder {
    type Output = Result<(), LixError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(unsafe {
            crate::session::AssumeSendFuture::new(async move {
                let server = self
                    .server
                    .ok_or_else(|| invalid("delete_lix requires a server repository"))?;
                let locator = crate::sync::normalize_sync_locator(&server.url)?;
                let response = crate::sync::authority_http(&server.headers)?
                    .request(ProtocolHttpRequest {
                        method: "DELETE".into(),
                        url: locator.protocol_url,
                        headers: Vec::new(),
                        body: None,
                    })
                    .await?;
                require_success(&response, 204)
            })
        })
    }
}

fn snapshot_io_error(error: std::io::Error) -> LixError {
    LixError::new("LIX_SNAPSHOT_IO", error.to_string())
}
fn invalid(message: &str) -> LixError {
    LixError::new(LixError::CODE_INVALID_PARAM, message)
}
fn invalid_response(message: &str) -> LixError {
    LixError::new("LIX_SERVER_PROTOCOL_ERROR", message)
}
fn validate_key(key: &str) -> Result<(), LixError> {
    if key.is_empty() || key.len() > 255 || !key.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
        return Err(invalid(
            "idempotency key must contain 1–255 printable ASCII characters",
        ));
    }
    Ok(())
}
fn collection_url(host: &str) -> Result<String, LixError> {
    let mut url = url::Url::parse(host).map_err(|_| invalid("server URL must be absolute"))?;
    if url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(invalid(
            "server URL must not contain credentials, query, or fragment",
        ));
    }
    let loopback = match url.host() {
        Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    };
    if url.scheme() != "https" && !(url.scheme() == "http" && loopback) {
        return Err(invalid("server URL requires HTTPS (HTTP is loopback-only)"));
    }
    if url.path() != "/" {
        return Err(invalid("creation server URL must be a host origin"));
    }
    url.set_path("/lix/v1");
    Ok(url.into())
}
fn validate_descriptor(endpoint: &str, hosted: &HostedLix) -> Result<(), LixError> {
    let locator = crate::sync::normalize_sync_locator(&hosted.url)
        .map_err(|_| invalid_response("server returned an invalid repository URL"))?;
    let expected =
        url::Url::parse(endpoint).map_err(|_| invalid_response("invalid collection URL"))?;
    let actual = url::Url::parse(&locator.locator)
        .map_err(|_| invalid_response("invalid repository URL"))?;
    if actual.origin() != expected.origin()
        || actual.path().rsplit('/').next() != Some(hosted.id.as_str())
    {
        return Err(invalid_response(
            "created repository URL must match its ID and server origin",
        ));
    }
    Ok(())
}
fn require_success(response: &ProtocolHttpResponse, expected_status: u16) -> Result<(), LixError> {
    if response.status == expected_status {
        return Ok(());
    }
    if (200..300).contains(&response.status) {
        return Err(invalid_response(
            "server returned an unexpected lifecycle success status",
        ));
    }
    let parsed: serde_json::Value = serde_json::from_slice(&response.body).unwrap_or_default();
    let error = parsed.get("error").unwrap_or(&parsed);
    let mut result = LixError::new(
        error
            .get("code")
            .and_then(|v| v.as_str())
            .unwrap_or("LIX_SERVER_ERROR"),
        error
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("hosted repository operation failed"),
    );
    result = result.with_details(serde_json::json!({"status":response.status}));
    Err(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deletion_requires_completed_response_not_accepted_for_later() {
        let mut response = ProtocolHttpResponse {
            status: 202,
            headers: Vec::new(),
            body: bytes::Bytes::new(),
        };
        assert!(require_success(&response, 204).is_err());
        response.status = 204;
        assert!(require_success(&response, 204).is_ok());
    }

    #[test]
    fn creation_accepts_only_host_origins() {
        for (host, endpoint) in [
            ("https://lix.example", "https://lix.example/lix/v1"),
            ("http://localhost:3000", "http://localhost:3000/lix/v1"),
            ("http://[::1]:3000", "http://[::1]:3000/lix/v1"),
        ] {
            assert_eq!(collection_url(host).unwrap(), endpoint);
        }
        for host in [
            "https://lix.example/lix",
            "https://user:secret@lix.example",
            "http://lix.example",
            "https://lix.example/?token=secret",
            "https://lix.example/#fragment",
        ] {
            assert!(collection_url(host).is_err(), "{host}");
        }
    }

    #[test]
    fn creation_response_cannot_redirect_to_another_host_or_repository() {
        let id = "0197bf96-8733-7000-8000-000000000001";
        let endpoint = "https://lix.example/lix/v1";
        assert!(
            validate_descriptor(
                endpoint,
                &HostedLix {
                    id: id.into(),
                    url: format!("https://lix.example/lix/{id}")
                }
            )
            .is_ok()
        );
        for url in [
            format!("https://other.example/lix/{id}"),
            format!("https://lix.example/lix/v1/{id}"),
            "https://lix.example/lix/0197bf96-8733-7000-8000-000000000002".into(),
        ] {
            assert!(validate_descriptor(endpoint, &HostedLix { id: id.into(), url }).is_err());
        }
    }

    #[test]
    fn idempotency_keys_cannot_inject_headers() {
        for key in ["", "has space", "line\r\nbreak", "\u{7f}", "é"] {
            assert!(validate_key(key).is_err());
        }
        assert!(validate_key(&"a".repeat(256)).is_err());
        assert!(validate_key("retry-this-creation:1").is_ok());
    }
}
