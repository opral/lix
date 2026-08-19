//! Native HTTP mechanics for the repository-scoped sync protocol.

use serde::Deserialize;

use crate::LixError;
use crate::sync::{
    MAX_SYNC_HISTORY_COMMIT_IDS, MAX_SYNC_PULL_RESPONSE_BYTES, SYNC_LONG_POLL_TIMEOUT,
    SyncBlobManifest, SyncBlobRegistration, SyncHistoryResponse, SyncPushRequest, SyncPushResponse,
    SyncRepositoryPullResponse, SyncSnapshotRowPage, SyncTransport, SyncTransportFuture,
    validate_blake3_id, validate_sync_remote_id,
};

const SESSION_HEADER: &str = "lix-session-id";

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
pub(crate) struct HttpSyncTransport {
    client: reqwest::Client,
    repository_url: String,
    protocol_url: String,
    session_id: String,
    active_account_id: String,
}

impl HttpSyncTransport {
    /// Opens an authentication/session capability for one repository.
    ///
    /// The session carries no branch selection. Branch refs and their current
    /// heads are ordinary repository state returned by the hot snapshot.
    pub(crate) async fn connect(
        repository_url: &str,
        headers: &[(String, String)],
    ) -> Result<Self, LixError> {
        let repository_url = repository_url.trim_end_matches('/').to_owned();
        validate_sync_remote_id(&repository_url)?;
        let protocol_url = format!("{repository_url}/lix/v1");
        let mut default_headers = reqwest::header::HeaderMap::new();
        for (name, value) in headers {
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
        let client = reqwest::Client::builder()
            .default_headers(default_headers)
            .timeout(SYNC_LONG_POLL_TIMEOUT + std::time::Duration::from_secs(5))
            .build()
            .map_err(|error| transport_error("configure sync transport", error))?;
        let response = client
            .get(&protocol_url)
            .send()
            .await
            .map_err(|error| transport_error("open sync session", error))?;
        let handshake = decode_response::<HandshakeResponse>(response, "open sync session").await?;
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
            repository_url,
            protocol_url,
            session_id: handshake.session_id,
            active_account_id: handshake.active_account_id,
        })
    }

    pub(crate) fn active_account_id(&self) -> &str {
        &self.active_account_id
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        self.client
            .request(method, format!("{}{path}", self.protocol_url))
            .header(SESSION_HEADER, &self.session_id)
    }
}

impl SyncTransport for HttpSyncTransport {
    fn remote_id(&self) -> &str {
        &self.repository_url
    }

    fn active_account_id(&self) -> &str {
        &self.active_account_id
    }

    fn push<'a>(
        &'a self,
        request: &'a SyncPushRequest,
    ) -> SyncTransportFuture<'a, SyncPushResponse> {
        Box::pin(async move {
            let response = self
                .request(reqwest::Method::POST, "/sync/push")
                .json(request)
                .send()
                .await
                .map_err(|error| transport_error("push sync commits", error))?;
            decode_response(response, "push sync commits").await
        })
    }

    fn pull(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> SyncTransportFuture<'_, SyncRepositoryPullResponse> {
        Box::pin(async move {
            let mut request = self
                .request(reqwest::Method::GET, "/sync/pull")
                .query(&[("limit", limit)]);
            if let Some(after) = after {
                request = request.query(&[("after", after)]);
            }
            let response = request
                .send()
                .await
                .map_err(|error| transport_error("pull sync repository", error))?;
            decode_response(response, "pull sync repository").await
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
            let mut request = self.request(reqwest::Method::GET, "/sync/pull").query(&[
                ("snapshotBranchId", branch_id),
                ("snapshotHeadCommitId", head_commit_id),
            ]);
            request = request.query(&[("limit", limit)]);
            if let Some(continuation) = continuation {
                request = request.query(&[("snapshotAfter", continuation)]);
            }
            let response = request
                .send()
                .await
                .map_err(|error| transport_error("load sync snapshot rows", error))?;
            decode_response(response, "load sync snapshot rows").await
        })
    }

    fn history<'a>(
        &'a self,
        commit_ids: &'a [String],
    ) -> SyncTransportFuture<'a, SyncHistoryResponse> {
        Box::pin(async move {
            if commit_ids.is_empty() || commit_ids.len() > MAX_SYNC_HISTORY_COMMIT_IDS {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!(
                        "sync history requires 1 through {MAX_SYNC_HISTORY_COMMIT_IDS} commit IDs"
                    ),
                ));
            }
            let mut url = reqwest::Url::parse(&format!("{}/sync/history", self.protocol_url))
                .map_err(|error| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        format!("invalid sync history URL: {error}"),
                    )
                })?;
            for commit_id in commit_ids {
                url.query_pairs_mut().append_pair("commitId", commit_id);
            }
            let response = self
                .client
                .get(url)
                .header(SESSION_HEADER, &self.session_id)
                .send()
                .await
                .map_err(|error| transport_error("load sync history", error))?;
            decode_response(response, "load sync history").await
        })
    }

    fn get_blob<'a>(
        &'a self,
        blob_id: &'a str,
    ) -> SyncTransportFuture<'a, Option<SyncBlobManifest>> {
        Box::pin(async move {
            validate_blake3_id(blob_id, "blob ID")?;
            let response = self
                .request(reqwest::Method::GET, "/sync/blob")
                .query(&[("blobId", blob_id)])
                .send()
                .await
                .map_err(|error| transport_error("load sync blob manifest", error))?;
            decode_optional_response(response, "load sync blob manifest").await
        })
    }

    fn register_blob<'a>(
        &'a self,
        manifest: &'a SyncBlobManifest,
    ) -> SyncTransportFuture<'a, SyncBlobRegistration> {
        Box::pin(async move {
            validate_blake3_id(&manifest.blob_id, "blob ID")?;
            let response = self
                .request(reqwest::Method::POST, "/sync/blob")
                .json(manifest)
                .send()
                .await
                .map_err(|error| transport_error("register sync blob manifest", error))?;
            decode_response(response, "register sync blob manifest").await
        })
    }

    fn get_chunk<'a>(&'a self, chunk_id: &'a str) -> SyncTransportFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            validate_blake3_id(chunk_id, "chunk ID")?;
            let response = self
                .request(reqwest::Method::GET, "/sync/chunk")
                .query(&[("chunkId", chunk_id)])
                .send()
                .await
                .map_err(|error| transport_error("load sync chunk", error))?;
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            }
            let response = checked_response(response, "load sync chunk").await?;
            let bytes = response
                .bytes()
                .await
                .map_err(|error| transport_error("load sync chunk", error))?;
            if bytes.len() > MAX_SYNC_PULL_RESPONSE_BYTES {
                return Err(response_too_large("load sync chunk"));
            }
            Ok(Some(bytes.to_vec()))
        })
    }

    fn put_chunk<'a>(&'a self, chunk_id: &'a str, bytes: &'a [u8]) -> SyncTransportFuture<'a, ()> {
        Box::pin(async move {
            validate_blake3_id(chunk_id, "chunk ID")?;
            let response = self
                .request(reqwest::Method::PUT, "/sync/chunk")
                .query(&[("chunkId", chunk_id)])
                .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
                .body(bytes.to_vec())
                .send()
                .await
                .map_err(|error| transport_error("store sync chunk", error))?;
            ensure_success(response, "store sync chunk").await
        })
    }
}

async fn decode_response<T>(response: reqwest::Response, operation: &str) -> Result<T, LixError>
where
    T: serde::de::DeserializeOwned,
{
    let response = checked_response(response, operation).await?;
    let body = response
        .bytes()
        .await
        .map_err(|error| transport_error(operation, error))?;
    if body.len() > MAX_SYNC_PULL_RESPONSE_BYTES {
        return Err(response_too_large(operation));
    }
    serde_json::from_slice(&body).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("decode {operation} response: {error}"),
        )
    })
}

async fn decode_optional_response<T>(
    response: reqwest::Response,
    operation: &str,
) -> Result<Option<T>, LixError>
where
    T: serde::de::DeserializeOwned,
{
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    decode_response(response, operation).await.map(Some)
}

async fn ensure_success(response: reqwest::Response, operation: &str) -> Result<(), LixError> {
    checked_response(response, operation).await.map(|_| ())
}

async fn checked_response(
    response: reqwest::Response,
    operation: &str,
) -> Result<reqwest::Response, LixError> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status();
    let body = response
        .bytes()
        .await
        .map_err(|error| transport_error(operation, error))?;
    if let Ok(envelope) = serde_json::from_slice::<ErrorResponse>(&body)
        && !envelope.error.code.is_empty()
    {
        let mut error = LixError::new(
            envelope.error.code,
            format!("{operation}: {}", envelope.error.message),
        );
        if let Some(hint) = envelope.error.hint {
            error = error.with_hint(hint);
        }
        return Err(error);
    }
    if status == reqwest::StatusCode::PAYLOAD_TOO_LARGE {
        return Err(LixError::new(
            "LIX_ERROR_REQUEST_BODY_TOO_LARGE",
            format!("{operation} exceeded an HTTP intermediary transfer limit"),
        ));
    }
    Err(LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!(
            "{operation} failed with {status}: {}",
            String::from_utf8_lossy(&body)
        ),
    ))
}

fn response_too_large(operation: &str) -> LixError {
    LixError::new(
        LixError::CODE_INVALID_PARAM,
        format!("{operation} response exceeds {MAX_SYNC_PULL_RESPONSE_BYTES} bytes"),
    )
}

fn transport_error(operation: &str, error: impl std::fmt::Display) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("{operation}: {error}"),
    )
}
