//! Repository sync HTTP protocol policy shared by native and browser clients.
//!
//! Target adapters implement only [`RawHttpClient`]: issuing a request,
//! cancellation, dynamic headers, and bounded response-body collection.

use serde::Deserialize;

use super::{
    MAX_SYNC_HISTORY_PAGE_SIZE, MAX_SYNC_PULL_RESPONSE_BYTES, SYNC_LONG_POLL_TIMEOUT,
    SyncBlobManifest, SyncBlobRegistration, SyncHistoryResponse, SyncPushRequest, SyncPushResponse,
    SyncRepositoryPullResponse, SyncSnapshotRowPage, SyncTransport, SyncTransportBounds,
    SyncTransportFuture, validate_blake3_id, validate_sync_remote_id,
};
use crate::LixError;

pub(super) const HTTP_TIMEOUT: std::time::Duration =
    SYNC_LONG_POLL_TIMEOUT.saturating_add(std::time::Duration::from_secs(5));
const SESSION_HEADER: &str = "lix-session-id";

#[derive(Clone, Copy, Debug)]
pub(super) enum Method {
    Get,
    Post,
    Put,
}

#[derive(Debug)]
pub(super) struct RawHttpRequest {
    pub method: Method,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
    pub operation: &'static str,
}

#[derive(Debug)]
pub(super) struct RawHttpResponse {
    pub status: u16,
    pub status_text: String,
    pub body: Vec<u8>,
}

pub(super) trait RawHttpClient: Clone + SyncTransportBounds {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse>;
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HandshakeResponse {
    session_id: String,
    active_account_id: String,
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    error: ErrorBody,
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    code: String,
    message: String,
    #[serde(default)]
    hint: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct HttpSyncTransport<Client> {
    client: Client,
    protocol_url: String,
    session_id: String,
    active_account_id: String,
}

impl<Client> HttpSyncTransport<Client>
where
    Client: RawHttpClient,
{
    pub(super) async fn connect_with(
        client: Client,
        repository_url: &str,
    ) -> Result<Self, LixError> {
        let repository_url = repository_url.trim_end_matches('/');
        validate_sync_remote_id(repository_url)?;
        let protocol_url = format!("{repository_url}/lix/v1");
        let response = client
            .send(raw_request(
                Method::Get,
                protocol_url.clone(),
                "open sync session",
            ))
            .await?;
        let handshake: HandshakeResponse = decode_response(response, "open sync session")?;
        if handshake.session_id.is_empty() || handshake.session_id.len() > 4096 {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync handshake returned an invalid session identity",
            ));
        }
        crate::row_pk::RowPk::uuid_from_canonical(&handshake.active_account_id).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync handshake returned an invalid active account identity",
            )
        })?;
        Ok(Self {
            client,
            protocol_url,
            session_id: handshake.session_id,
            active_account_id: handshake.active_account_id,
        })
    }

    fn request(&self, method: Method, path: &str, operation: &'static str) -> RawHttpRequest {
        let mut request = raw_request(method, format!("{}{path}", self.protocol_url), operation);
        request
            .headers
            .push((SESSION_HEADER.to_owned(), self.session_id.clone()));
        request
    }

    async fn send_json<T>(&self, request: RawHttpRequest) -> Result<T, LixError>
    where
        T: serde::de::DeserializeOwned,
    {
        let operation = request.operation;
        decode_response(self.client.send(request).await?, operation)
    }
}

impl<Client> SyncTransport for HttpSyncTransport<Client>
where
    Client: RawHttpClient,
{
    fn active_account_id(&self) -> &str {
        &self.active_account_id
    }

    fn push<'a>(&'a self, value: &'a SyncPushRequest) -> SyncTransportFuture<'a, SyncPushResponse> {
        Box::pin(async move {
            let mut request = self.request(Method::Post, "/sync/push", "push sync commits");
            request.headers.push(json_content_type());
            request.body = Some(json_body(value, "encode sync push")?);
            self.send_json(request).await
        })
    }

    fn pull(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> SyncTransportFuture<'_, SyncRepositoryPullResponse> {
        Box::pin(async move {
            let path = match after {
                Some(after) => format!("/sync/pull?after={after}&limit={limit}"),
                None => format!("/sync/pull?limit={limit}"),
            };
            let request = self.request(Method::Get, &path, "pull sync repository");
            self.send_json(request).await
        })
    }

    fn snapshot_rows<'a>(
        &'a self,
        branch_id: &'a str,
        head_commit_id: &'a str,
        continuation: Option<&'a str>,
        limit: usize,
    ) -> SyncTransportFuture<'a, SyncSnapshotRowPage> {
        Box::pin(async move {
            let mut path = format!(
                "/sync/pull?snapshotBranchId={}&snapshotHeadCommitId={}&limit={limit}",
                encode_query(branch_id),
                encode_query(head_commit_id),
            );
            if let Some(continuation) = continuation {
                path.push_str("&snapshotAfter=");
                path.push_str(&encode_query(continuation));
            }
            let request = self.request(Method::Get, &path, "load sync snapshot rows");
            self.send_json(request).await
        })
    }

    fn history<'a>(
        &'a self,
        head: &'a str,
        limit: usize,
    ) -> SyncTransportFuture<'a, SyncHistoryResponse> {
        Box::pin(async move {
            if head.is_empty() || limit == 0 || limit > MAX_SYNC_HISTORY_PAGE_SIZE {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!(
                        "sync history requires a head and a limit from 1 through {MAX_SYNC_HISTORY_PAGE_SIZE}"
                    ),
                ));
            }
            let query = format!("head={}&limit={limit}", encode_query(head));
            let request = self.request(
                Method::Get,
                &format!("/sync/history?{query}"),
                "load sync history",
            );
            self.send_json(request).await
        })
    }

    fn get_blob<'a>(
        &'a self,
        blob_id: &'a str,
    ) -> SyncTransportFuture<'a, Option<SyncBlobManifest>> {
        Box::pin(async move {
            validate_blake3_id(blob_id, "blob ID")?;
            let request = self.request(
                Method::Get,
                &format!("/sync/blob?blobId={}", encode_query(blob_id)),
                "load sync blob manifest",
            );
            let response = self.client.send(request).await?;
            if response.status == 404 {
                return Ok(None);
            }
            decode_response(response, "load sync blob manifest").map(Some)
        })
    }

    fn register_blob<'a>(
        &'a self,
        manifest: &'a SyncBlobManifest,
    ) -> SyncTransportFuture<'a, SyncBlobRegistration> {
        Box::pin(async move {
            validate_blake3_id(&manifest.blob_id, "blob ID")?;
            let mut request =
                self.request(Method::Post, "/sync/blob", "register sync blob manifest");
            request.headers.push(json_content_type());
            request.body = Some(json_body(manifest, "encode sync blob manifest")?);
            self.send_json(request).await
        })
    }

    fn get_chunk<'a>(&'a self, chunk_id: &'a str) -> SyncTransportFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            validate_blake3_id(chunk_id, "chunk ID")?;
            let request = self.request(
                Method::Get,
                &format!("/sync/chunk?chunkId={}", encode_query(chunk_id)),
                "load sync chunk",
            );
            let response = self.client.send(request).await?;
            if response.status == 404 {
                return Ok(None);
            }
            ensure_success(&response, "load sync chunk")?;
            Ok(Some(response.body))
        })
    }

    fn put_chunk<'a>(&'a self, chunk_id: &'a str, bytes: &'a [u8]) -> SyncTransportFuture<'a, ()> {
        Box::pin(async move {
            validate_blake3_id(chunk_id, "chunk ID")?;
            let mut request = self.request(Method::Put, "/sync/chunk", "store sync chunk");
            request.url.push_str("?chunkId=");
            request.url.push_str(&encode_query(chunk_id));
            request.headers.push((
                "content-type".to_owned(),
                "application/octet-stream".to_owned(),
            ));
            request.body = Some(bytes.to_vec());
            let response = self.client.send(request).await?;
            ensure_success(&response, "store sync chunk")
        })
    }
}

fn raw_request(method: Method, url: String, operation: &'static str) -> RawHttpRequest {
    RawHttpRequest {
        method,
        url,
        headers: Vec::new(),
        body: None,
        operation,
    }
}

fn json_content_type() -> (String, String) {
    ("content-type".to_owned(), "application/json".to_owned())
}

fn json_body(value: &impl serde::Serialize, operation: &str) -> Result<Vec<u8>, LixError> {
    serde_json::to_vec(value).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("{operation}: {error}"),
        )
    })
}

fn decode_response<T>(response: RawHttpResponse, operation: &str) -> Result<T, LixError>
where
    T: serde::de::DeserializeOwned,
{
    ensure_success(&response, operation)?;
    serde_json::from_slice(&response.body).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("decode {operation} response: {error}"),
        )
    })
}

fn ensure_success(response: &RawHttpResponse, operation: &str) -> Result<(), LixError> {
    ensure_response_bound(response, operation)?;
    if (200..300).contains(&response.status) {
        return Ok(());
    }
    Err(response_error(response, operation))
}

fn ensure_response_bound(response: &RawHttpResponse, operation: &str) -> Result<(), LixError> {
    if response.body.len() <= MAX_SYNC_PULL_RESPONSE_BYTES {
        return Ok(());
    }
    Err(response_too_large(operation))
}

pub(super) fn response_too_large(operation: &str) -> LixError {
    LixError::new(
        LixError::CODE_INVALID_PARAM,
        format!("{operation} response exceeds {MAX_SYNC_PULL_RESPONSE_BYTES} bytes"),
    )
}

fn response_error(response: &RawHttpResponse, operation: &str) -> LixError {
    if let Ok(envelope) = serde_json::from_slice::<ErrorResponse>(&response.body)
        && !envelope.error.code.is_empty()
    {
        let mut error = LixError::new(
            envelope.error.code,
            format!("{operation}: {}", envelope.error.message),
        );
        if let Some(hint) = envelope.error.hint {
            error = error.with_hint(hint);
        }
        return error;
    }
    if response.status == 413 {
        return LixError::new(
            "LIX_ERROR_REQUEST_BODY_TOO_LARGE",
            format!("{operation} exceeded an HTTP intermediary transfer limit"),
        );
    }
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!(
            "{operation} failed with {} {}: {}",
            response.status,
            response.status_text,
            String::from_utf8_lossy(&response.body)
        ),
    )
}

fn encode_query(value: &str) -> String {
    use std::fmt::Write;

    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::encode_query;

    #[test]
    fn query_encoding_is_rfc3986_component_encoding() {
        assert_eq!(encode_query("a b/c?d=ü"), "a%20b%2Fc%3Fd%3D%C3%BC");
    }
}
