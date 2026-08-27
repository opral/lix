//! Repository sync HTTP protocol policy shared by native and browser clients.
//!
//! Target adapters implement only [`RawHttpClient`]: issuing a request,
//! cancellation, dynamic headers, and bounded response-body collection.

use http::Method;
use serde::Deserialize;

use super::{
    MAX_SYNC_PULL_RESPONSE_BYTES, SYNC_LONG_POLL_TIMEOUT, SyncBlobManifest,
    SyncBlobRegistration, SyncHistoryResponse, SyncPushRequest, SyncPushResponse,
    SyncRepositoryPullResponse, SyncSnapshotRowPage, SyncTransport, SyncTransportBounds,
    SyncTransportFuture, SYNC_PROTOCOL_VERSION, SYNC_PROTOCOL_VERSION_HEADER,
    sync_server_protocol_mismatch, validate_sync_remote_id,
};
use crate::LixError;

pub(super) const HTTP_TIMEOUT: std::time::Duration =
    SYNC_LONG_POLL_TIMEOUT.saturating_add(std::time::Duration::from_secs(5));
pub(super) const SYNC_TRANSPORT_ERROR_CODE: &str = "LIX_ERROR_SYNC_TRANSPORT";
const SESSION_HEADER: &str = "lix-session-id";

#[derive(Debug)]
pub(crate) struct RawHttpRequest {
    pub method: Method,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
    pub cache_immutable: bool,
    pub operation: &'static str,
}

#[derive(Debug)]
pub(crate) struct RawHttpResponse {
    pub status: u16,
    pub status_text: String,
    pub body: Vec<u8>,
}

pub(crate) trait RawHttpClient: SyncTransportBounds {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse>;
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HandshakeResponse {
    protocol_version: u32,
    #[serde(default)]
    sync_protocol_version: Option<u32>,
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
    #[serde(default)]
    details: Option<serde_json::Value>,
}

#[derive(Debug)]
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
        lix_url: &str,
    ) -> Result<Self, LixError> {
        let normalized = normalize_sync_locator(lix_url)?;
        let protocol_url = normalized.protocol_url;
        let response = client
            .send(raw_request(
                Method::GET,
                protocol_url.clone(),
                "open sync session",
            ))
            .await?;
        let handshake: HandshakeResponse = decode_response(response, "open sync session")?;
        validate_handshake(&handshake)?;
        Ok(Self {
            client,
            protocol_url,
            session_id: handshake.session_id,
            active_account_id: handshake.active_account_id,
        })
    }

    pub(super) fn is_reserved_header(name: &str) -> bool {
        name.eq_ignore_ascii_case(SESSION_HEADER)
            || name.eq_ignore_ascii_case(SYNC_PROTOCOL_VERSION_HEADER)
    }

    fn request(&self, method: Method, path: &str, operation: &'static str) -> RawHttpRequest {
        let mut request = raw_request(method, format!("{}{path}", self.protocol_url), operation);
        request
            .headers
            .push((SESSION_HEADER.to_owned(), self.session_id.clone()));
        request.headers.push((
            SYNC_PROTOCOL_VERSION_HEADER.to_owned(),
            SYNC_PROTOCOL_VERSION.to_string(),
        ));
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

fn validate_handshake(handshake: &HandshakeResponse) -> Result<(), LixError> {
    if handshake.protocol_version != crate::SERVER_PROTOCOL_VERSION {
        return Err(LixError::new(
            "LIX_SERVER_PROTOCOL_ERROR",
            format!(
                "unsupported Lix Server Protocol version: {}",
                handshake.protocol_version
            ),
        ));
    }
    if handshake.sync_protocol_version != Some(SYNC_PROTOCOL_VERSION) {
        return Err(sync_server_protocol_mismatch(
            handshake.sync_protocol_version,
        ));
    }
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
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NormalizedSyncLocator {
    pub(crate) locator: String,
    pub(crate) protocol_url: String,
}

pub(crate) fn normalize_sync_locator(
    locator: &str,
) -> Result<NormalizedSyncLocator, LixError> {
    let mut parsed = url::Url::parse(locator).map_err(|_| invalid_lix_locator())?;
    if parsed.scheme() != "https"
        && !(parsed.scheme() == "http" && is_loopback_host(&parsed))
    {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync server url must use https (http is allowed only for loopback development)",
        ));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync server url must not contain a query or fragment",
        ));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync server url must not contain credentials",
        ));
    }
    let locator_path = parsed.path();
    let Some(lix_id) = locator_path.strip_prefix("/lix/") else {
        return Err(invalid_lix_locator());
    };
    if lix_id.contains('/') || crate::row_pk::RowPk::uuid_from_canonical(lix_id).is_err()
    {
        return Err(invalid_lix_locator());
    }
    let lix_id = lix_id.to_owned();
    parsed.set_path(&format!("/lix/{lix_id}"));
    let canonical_locator = parsed.to_string();
    validate_sync_remote_id(&canonical_locator)?;
    parsed.set_path(&format!("/lix/v1/{lix_id}"));
    Ok(NormalizedSyncLocator {
        locator: canonical_locator,
        protocol_url: parsed.to_string(),
    })
}

fn is_loopback_host(url: &url::Url) -> bool {
    match url.host() {
        Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    }
}

fn invalid_lix_locator() -> LixError {
    LixError::new(
        LixError::CODE_INVALID_PARAM,
        "sync server url path must be exactly /lix/{uuid}",
    )
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
            let mut request = self.request(Method::POST, "/sync/push", "push sync commits");
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
            let request = self.request(Method::GET, &path, "pull sync repository");
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
            let request = self.request(Method::GET, &path, "load sync snapshot rows");
            self.send_json(request).await
        })
    }

    fn history<'a>(
        &'a self,
        head: &'a str,
        limit: usize,
    ) -> SyncTransportFuture<'a, SyncHistoryResponse> {
        Box::pin(async move {
            let query = format!("head={}&limit={limit}", encode_query(head));
            let request = self.request(
                Method::GET,
                &format!("/sync/history?{query}"),
                "load sync history",
            );
            self.send_json(request).await
        })
    }

    fn get_blobs<'a>(
        &'a self,
        blob_ids: &'a [String],
    ) -> SyncTransportFuture<'a, Vec<SyncBlobManifest>> {
        Box::pin(async move {
            let blob_ids = blob_ids
                .iter()
                .map(|blob_id| encode_query(blob_id))
                .collect::<Vec<_>>()
                .join(",");
            let request = self.request(
                Method::GET,
                &format!("/sync/blob?blobIds={blob_ids}"),
                "load sync blob manifests",
            );
            self.send_json(request).await
        })
    }

    fn register_blob<'a>(
        &'a self,
        manifest: &'a SyncBlobManifest,
    ) -> SyncTransportFuture<'a, SyncBlobRegistration> {
        Box::pin(async move {
            let mut request =
                self.request(Method::POST, "/sync/blob", "register sync blob manifest");
            request.headers.push(json_content_type());
            request.body = Some(json_body(manifest, "encode sync blob manifest")?);
            self.send_json(request).await
        })
    }

    fn get_chunk<'a>(&'a self, chunk_id: &'a str) -> SyncTransportFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            let mut request = self.request(
                Method::GET,
                &format!("/sync/chunk?chunkId={}", encode_query(chunk_id)),
                "load sync chunk",
            );
            request.cache_immutable = true;
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
            let mut request = self.request(Method::PUT, "/sync/chunk", "store sync chunk");
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
        cache_immutable: false,
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
    if response.body.len() > MAX_SYNC_PULL_RESPONSE_BYTES {
        return Err(response_too_large(operation));
    }
    if (200..300).contains(&response.status) {
        return Ok(());
    }
    Err(response_error(response, operation))
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
        let mut details = envelope
            .error
            .details
            .unwrap_or_else(|| serde_json::json!({}));
        if let Some(object) = details.as_object_mut() {
            object.insert(
                "httpStatus".to_owned(),
                serde_json::json!(response.status),
            );
        } else {
            details = serde_json::json!({
                "httpStatus": response.status,
                "body": details,
            });
        }
        let mut error = LixError::new(
            envelope.error.code,
            format!("{operation}: {}", envelope.error.message),
        );
        if let Some(hint) = envelope.error.hint {
            error = error.with_hint(hint);
        }
        return error.with_details(details);
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
    .with_details(serde_json::json!({ "httpStatus": response.status }))
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
    use super::{
        HandshakeResponse, HttpSyncTransport, RawHttpClient, RawHttpRequest, RawHttpResponse,
        encode_query, normalize_sync_locator, response_error, validate_handshake,
    };
    use crate::sync::SyncTransportFuture;

    #[test]
    fn sync_connection_locator_maps_to_the_targeted_protocol_root() {
        assert_eq!(
            normalize_sync_locator(
                "https://example.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc"
            )
            .expect("canonical locator")
            .protocol_url,
            "https://example.test/lix/v1/01936f4e-7b6c-7c3d-8f9a-123456789abc"
        );
        assert_eq!(
            normalize_sync_locator(
                "https://EXAMPLE.test:443/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc"
            )
            .expect("equivalent locator")
            .locator,
            "https://example.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc"
        );
    }

    #[test]
    fn sync_connection_locator_rejects_non_http_and_credentialed_urls() {
        for invalid in [
            "relative/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
            "ftp://example.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
            "http://example.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
            "https://example.test/prefix/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
            "https://user@example.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
            "https://example.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc/",
        ] {
            assert!(
                normalize_sync_locator(invalid).is_err(),
                "accepted invalid locator: {invalid}"
            );
        }
        assert!(normalize_sync_locator(
            "http://localhost:3000/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc"
        )
        .is_ok());
        assert!(normalize_sync_locator(
            "http://127.0.0.1:3000/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc"
        )
        .is_ok());
        assert!(normalize_sync_locator(
            "http://[::1]:3000/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc"
        )
        .is_ok());
    }

    #[test]
    fn sync_handshake_rejects_an_incompatible_protocol_before_bootstrap() {
        let error = validate_handshake(&HandshakeResponse {
            protocol_version: crate::SERVER_PROTOCOL_VERSION + 1,
            sync_protocol_version: Some(crate::sync::SYNC_PROTOCOL_VERSION),
            session_id: "session-1".to_owned(),
            active_account_id: "01920000-0000-7000-8000-000000000602".to_owned(),
        })
        .expect_err("incompatible protocol must fail");
        assert_eq!(error.code, "LIX_SERVER_PROTOCOL_ERROR");
        assert!(error.message.contains("unsupported"));
    }

    #[derive(Debug)]
    struct VersionMismatchClient;

    impl RawHttpClient for VersionMismatchClient {
        fn send(&self, _request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
            Box::pin(async {
                Ok(RawHttpResponse {
                    status: 200,
                    status_text: "OK".to_owned(),
                    body: serde_json::to_vec(&serde_json::json!({
                        "protocolVersion": crate::SERVER_PROTOCOL_VERSION,
                        "syncProtocolVersion": 999,
                        "sessionId": "session-from-incompatible-server",
                        "activeBranchId": "01920000-0000-7000-8000-000000001234",
                        "activeAccountId": crate::SYSTEM_ACCOUNT_ID,
                    }))
                    .expect("encode mismatched handshake"),
                })
            })
        }
    }

    #[derive(Debug)]
    struct MissingVersionClient;

    impl RawHttpClient for MissingVersionClient {
        fn send(&self, _request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
            Box::pin(async {
                Ok(RawHttpResponse {
                    status: 200,
                    status_text: "OK".to_owned(),
                    body: serde_json::to_vec(&serde_json::json!({
                        "protocolVersion": crate::SERVER_PROTOCOL_VERSION,
                        "sessionId": "session-from-legacy-server",
                        "activeBranchId": "01920000-0000-7000-8000-000000001234",
                        "activeAccountId": crate::SYSTEM_ACCOUNT_ID,
                    }))
                    .expect("encode legacy handshake"),
                })
            })
        }
    }

    #[tokio::test]
    async fn sync_handshake_rejects_a_mismatched_sync_protocol_version() {
        let error = HttpSyncTransport::connect_with(
            VersionMismatchClient,
            "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
        )
        .await
        .expect_err("an incompatible sync protocol must fail before transfer");
        assert_eq!(error.code, crate::sync::SYNC_PROTOCOL_MISMATCH_CODE);
        assert_eq!(
            error.details,
            Some(serde_json::json!({
                "clientSyncProtocolVersion": crate::sync::SYNC_PROTOCOL_VERSION,
                "serverSyncProtocolVersion": 999,
            }))
        );
    }

    #[tokio::test]
    async fn sync_handshake_rejects_a_missing_sync_protocol_version() {
        let error = HttpSyncTransport::connect_with(
            MissingVersionClient,
            "https://sync.example/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
        )
        .await
        .expect_err("a legacy server must fail before transfer");
        assert_eq!(error.code, crate::sync::SYNC_PROTOCOL_MISMATCH_CODE);
        assert_eq!(
            error.details,
            Some(serde_json::json!({
                "clientSyncProtocolVersion": crate::sync::SYNC_PROTOCOL_VERSION,
                "serverSyncProtocolVersion": null,
            }))
        );
    }

    #[test]
    fn session_and_protocol_headers_are_reserved_for_the_transport() {
        assert!(HttpSyncTransport::<VersionMismatchClient>::is_reserved_header(
            "Lix-Session-Id"
        ));
        assert!(HttpSyncTransport::<VersionMismatchClient>::is_reserved_header(
            "LIX-SYNC-PROTOCOL-VERSION"
        ));
        assert!(!HttpSyncTransport::<VersionMismatchClient>::is_reserved_header(
            "Authorization"
        ));
    }

    #[test]
    fn query_encoding_is_rfc3986_component_encoding() {
        assert_eq!(encode_query("a b/c?d=ü"), "a%20b%2Fc%3Fd%3D%C3%BC");
    }

    #[test]
    fn structured_http_error_preserves_remote_details_and_adds_status() {
        let error = response_error(
            &RawHttpResponse {
                status: 503,
                status_text: "Service Unavailable".to_owned(),
                body: serde_json::to_vec(&serde_json::json!({
                    "error": {
                        "code": "LIX_ERROR_LIX_MIGRATING",
                        "message": "The lix repository is being migrated.",
                        "hint": "Retry after the migration completes.",
                        "details": {
                            "fromVersion": 68,
                            "toVersion": 71,
                            "retryable": true,
                        },
                    },
                }))
                .expect("encode error response"),
            },
            "open sync session",
        );

        assert_eq!(error.code, "LIX_ERROR_LIX_MIGRATING");
        assert_eq!(
            error.hint.as_deref(),
            Some("Retry after the migration completes.")
        );
        assert_eq!(
            error.details,
            Some(serde_json::json!({
                "fromVersion": 68,
                "toVersion": 71,
                "retryable": true,
                "httpStatus": 503,
            }))
        );
    }
}
