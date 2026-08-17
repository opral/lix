use serde::Deserialize;

use super::{
    MAX_SYNC_PULL_RESPONSE_BYTES, SYNC_LONG_POLL_TIMEOUT, SyncAdmission, SyncBranch,
    SyncPullResponse, SyncTransactionPack, SyncTransport, SyncTransportFuture,
    validate_sync_branch_id, validate_sync_remote_id,
};
use crate::LixError;

const SESSION_HEADER: &str = "lix-session-id";
const IDEMPOTENCY_HEADER: &str = "idempotency-key";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HandshakeResponse {
    active_branch_id: String,
    session_id: String,
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
    branch_id: String,
}

impl HttpSyncTransport {
    /// Opens a protocol session pinned to `active_branch_id` when supplied.
    ///
    /// A brand-new sync activation omits the branch so the server's default
    /// branch can select the replica's first branch. Reopens and reconnects
    /// include the selected branch in the handshake; otherwise the server
    /// would keep handing us a session for its default branch and the worker
    /// could never follow a local branch switch.
    pub(crate) async fn connect(
        repository_url: &str,
        active_branch_id: Option<&str>,
    ) -> Result<Self, LixError> {
        let repository_url = repository_url.trim_end_matches('/').to_owned();
        validate_sync_remote_id(&repository_url)?;
        let protocol_url = format!("{repository_url}/lix/v1");
        let client = reqwest::Client::builder()
            // Event pulls are mandatory long-polls. Leave a small margin for
            // response framing and proxy jitter beyond the server heartbeat;
            // a five-second client timeout would turn an idle connection into
            // a reconnect storm.
            .timeout(SYNC_LONG_POLL_TIMEOUT + std::time::Duration::from_secs(5))
            .build()
            .map_err(|error| transport_error("configure sync transport", error))?;
        let handshake_url = if let Some(active_branch_id) = active_branch_id {
            let mut url = reqwest::Url::parse(&protocol_url).map_err(|error| {
                LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("invalid sync server URL: {error}"),
                )
            })?;
            url.query_pairs_mut()
                .append_pair("activeBranchId", active_branch_id);
            url.to_string()
        } else {
            protocol_url.clone()
        };
        let response = client
            .get(handshake_url)
            .send()
            .await
            .map_err(|error| transport_error("open sync session", error))?;
        let handshake = decode_response::<HandshakeResponse>(response, "open sync session").await?;
        validate_sync_branch_id(&handshake.active_branch_id)?;
        if handshake.session_id.is_empty() || handshake.session_id.len() > 4096 {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync handshake returned an invalid session identity",
            ));
        }
        Ok(Self {
            client,
            repository_url,
            protocol_url,
            session_id: handshake.session_id,
            branch_id: handshake.active_branch_id,
        })
    }

    pub(crate) fn branch_id(&self) -> &str {
        &self.branch_id
    }

    pub(crate) async fn close(self) -> Result<(), LixError> {
        self.client
            .delete(format!("{}/session", self.protocol_url))
            .header(SESSION_HEADER, self.session_id)
            .send()
            .await
            .map_err(|error| transport_error("close sync session", error))?;
        Ok(())
    }
}

impl SyncTransport for HttpSyncTransport {
    fn remote_id(&self) -> &str {
        &self.repository_url
    }

    fn admit<'a>(
        &'a self,
        pack: &'a SyncTransactionPack,
    ) -> SyncTransportFuture<'a, SyncAdmission> {
        Box::pin(async move {
            let response = self
                .client
                .post(format!("{}/sync/admit", self.protocol_url))
                .header(SESSION_HEADER, &self.session_id)
                .header(IDEMPOTENCY_HEADER, &pack.operation_id)
                .json(pack)
                .send()
                .await
                .map_err(|error| transport_error("admit sync transaction", error))?;
            decode_response(response, "admit sync transaction").await
        })
    }

    fn pull<'a>(
        &'a self,
        branch_id: &'a str,
        after_cursor: u64,
        limit: usize,
        schema_keys: &'a [String],
    ) -> SyncTransportFuture<'a, SyncPullResponse> {
        Box::pin(async move {
            let mut request = self
                .client
                .get(format!("{}/sync/pull", self.protocol_url))
                .header(SESSION_HEADER, &self.session_id)
                .query(&[
                    ("after", after_cursor.to_string()),
                    ("limit", limit.to_string()),
                    ("branch", branch_id.to_owned()),
                ]);
            // Omit `schemas` for an unscoped pull. Sending an empty value is a
            // meaningful filtered request on the server and would otherwise
            // discard every event during full-history hydration.
            if !schema_keys.is_empty() {
                request = request.query(&[("schemas", schema_keys.join(","))]);
            }
            let response = request
                .send()
                .await
                .map_err(|error| transport_error("pull sync transactions", error))?;
            decode_response(response, "pull sync transactions").await
        })
    }

    fn list_branches<'a>(&'a self) -> SyncTransportFuture<'a, Vec<SyncBranch>> {
        Box::pin(async move {
            let response = self
                .client
                .get(format!("{}/sync/branches", self.protocol_url))
                .header(SESSION_HEADER, &self.session_id)
                .send()
                .await
                .map_err(|error| transport_error("list sync branches", error))?;
            decode_response(response, "list sync branches").await
        })
    }

    fn has_authoritative_branch_catalog(&self) -> bool {
        true
    }
}

async fn decode_response<T>(response: reqwest::Response, operation: &str) -> Result<T, LixError>
where
    T: serde::de::DeserializeOwned,
{
    let status = response.status();
    let body = response
        .bytes()
        .await
        .map_err(|error| transport_error(operation, error))?;
    if body.len() > MAX_SYNC_PULL_RESPONSE_BYTES {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("{operation} response exceeds {MAX_SYNC_PULL_RESPONSE_BYTES} bytes"),
        ));
    }
    if !status.is_success() {
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
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "{operation} failed with {status}: {}",
                String::from_utf8_lossy(&body)
            ),
        ));
    }
    serde_json::from_slice(&body).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("decode {operation} response: {error}"),
        )
    })
}

fn transport_error(operation: &str, error: impl std::fmt::Display) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("{operation}: {error}"),
    )
}
