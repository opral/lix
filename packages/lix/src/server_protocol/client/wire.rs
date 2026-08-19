//! JSON DTOs for the Lix Server Protocol client.

use crate::{LixNotice, WireValue};
use serde::{Deserialize, Serialize};

pub(crate) const PROTOCOL_VERSION: u32 = 2;
pub(crate) const SESSION_ID_HEADER: &str = "lix-session-id";
pub(crate) const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
pub(crate) const TRANSACTION_ID_HEADER: &str = "lix-transaction-id";

pub(crate) const SESSION_GONE: &str = "LIX_ERROR_PROTOCOL_SESSION_GONE";
pub(crate) const SERVER_CLOSED: &str = "LIX_ERROR_PROTOCOL_SERVER_CLOSED";
pub(crate) const BLOB_BASE_MISSING: &str = "LIX_REMOTE_BLOB_BASE_MISSING";
pub(crate) const PROTOCOL_ERROR: &str = "LIX_SERVER_PROTOCOL_ERROR";
pub(crate) const REMOTE_UNAVAILABLE: &str = "LIX_REMOTE_UNAVAILABLE";
pub(crate) const REMOTE_REQUEST_FAILED: &str = "LIX_REMOTE_REQUEST_FAILED";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HandshakeResponse {
    pub protocol_version: u32,
    pub active_branch_id: String,
    pub active_account_id: String,
    pub session_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExecuteRequest<'a> {
    pub sql: &'a str,
    pub params: &'a [RequestWireValue],
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<ExecuteOptionsRequest>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub cache_blobs: bool,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExecuteOptionsRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin_key: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExecuteBatchRequest<'a> {
    pub statements: &'a [ExecuteBatchStatementRequest],
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<ExecuteOptionsRequest>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub cache_blobs: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct ExecuteBatchStatementRequest {
    pub sql: String,
    pub params: Vec<RequestWireValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub(crate) enum RequestWireValue {
    Value(WireValue),
    BlobSplice(RequestBlobSplice),
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RequestBlobSplice {
    pub kind: &'static str,
    pub base_sha256: String,
    pub result_sha256: String,
    pub prefix_bytes: u64,
    pub suffix_bytes: u64,
    pub insert_base64: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExecuteResponse {
    #[serde(default)]
    pub statement_index: Option<usize>,
    #[serde(default)]
    pub label: Option<String>,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<WireValue>>,
    pub rows_affected: u64,
    #[serde(default)]
    pub notices: Vec<LixNotice>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BeginTransactionResponse {
    pub transaction_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CreateBranchRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_commit_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CreateBranchResponse {
    pub id: String,
    pub name: String,
    pub hidden: bool,
    pub commit_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CreateCheckpointResponse {
    pub commit_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UndoResponse {
    pub branch_id: String,
    pub target_commit_id: String,
    pub inverse_commit_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RedoResponse {
    pub branch_id: String,
    pub target_commit_id: String,
    pub replay_commit_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SwitchBranchRequest<'a> {
    pub branch_id: &'a str,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SwitchBranchResponse {
    pub branch_id: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MultiplexObserveRequest {
    pub subscriptions: Vec<MultiplexObserveSubscription>,
}

#[derive(Debug, Serialize)]
pub(crate) struct MultiplexObserveSubscription {
    pub id: String,
    pub sql: String,
    pub params: Vec<WireValue>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MultiplexObserveEvent {
    pub subscription_id: String,
    pub sequence: u64,
    pub mutation_sequence: u64,
    #[serde(default)]
    pub result: Option<ExecuteResponse>,
    #[serde(default)]
    pub delta: Option<ObserveDelta>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub(crate) enum ObserveDelta {
    #[serde(rename = "single-blob-splice", rename_all = "camelCase")]
    SingleBlobSplice {
        base_sequence: u64,
        prefix_bytes: u64,
        suffix_bytes: u64,
        insert_base64: String,
    },
    #[serde(rename = "row-splice", rename_all = "camelCase")]
    RowSplice {
        base_sequence: u64,
        prefix_rows: u64,
        delete_rows: u64,
        insert_rows: Vec<Vec<WireValue>>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MultiplexObserveError {
    #[serde(default)]
    pub subscription_id: Option<String>,
    pub error: ErrorBody,
    #[serde(default)]
    pub retryable: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ErrorEnvelope {
    pub error: ErrorBody,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ErrorBody {
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub hint: Option<String>,
    #[serde(default)]
    pub details: Option<serde_json::Value>,
}
