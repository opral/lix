use serde::{Deserialize, Serialize};

use crate::{ExecuteResult, LixError, LixNotice, ResultColumnType, Value, WireValue};
use crate::{
    MergeBranchOptions, MergeBranchOutcome, MergeBranchPreview, MergeBranchPreviewOptions,
    MergeBranchReceipt, MergeChangeStats, MergeConflict, MergeConflictChangeKind,
    MergeConflictKind, MergeConflictSide, RowRef,
};

pub const SERVER_PROTOCOL_VERSION: u32 = crate::SERVER_PROTOCOL_VERSION;
pub const SESSION_GONE_CODE: &str = "LIX_ERROR_PROTOCOL_SESSION_GONE";
pub const SERVER_CLOSED_CODE: &str = "LIX_ERROR_PROTOCOL_SERVER_CLOSED";
pub const BLOB_BASE_MISSING_CODE: &str = "LIX_REMOTE_BLOB_BASE_MISSING";
pub const SESSION_HEADER: &str = "Lix-Session-Id";
pub const TRANSACTION_HEADER: &str = "Lix-Transaction-Id";
pub const IDEMPOTENCY_KEY_HEADER: &str = "Idempotency-Key";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HandshakeResponse {
    pub protocol_version: u32,
    pub active_branch_id: String,
    pub active_account_id: String,
    pub session_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteOptionsBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestWireValue {
    BlobSplice(RequestBlobSplice),
    Value(WireValue),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestBlobSplice {
    pub kind: BlobSpliceKind,
    pub base_sha256: String,
    pub result_sha256: String,
    pub prefix_bytes: u64,
    pub suffix_bytes: u64,
    pub insert_base64: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BlobSpliceKind {
    BlobSplice,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteRequestBody {
    pub sql: String,
    pub params: Vec<RequestWireValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<ExecuteOptionsBody>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub cache_blobs: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteBatchStatementBody {
    pub sql: String,
    pub params: Vec<RequestWireValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteBatchRequestBody {
    pub statements: Vec<ExecuteBatchStatementBody>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<ExecuteOptionsBody>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub cache_blobs: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteResponseBody {
    pub statement_index: Option<usize>,
    pub label: Option<String>,
    pub columns: Vec<ExecuteColumnBody>,
    pub rows: Vec<Vec<WireValue>>,
    pub rows_affected: u64,
    #[serde(default)]
    pub notices: Vec<LixNotice>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExecuteColumnBody {
    pub name: String,
    #[serde(rename = "type")]
    pub column_type: ResultColumnType,
}

impl ExecuteResponseBody {
    pub fn into_execute_result(self) -> Result<ExecuteResult, LixError> {
        let column_count = self.columns.len();
        let (columns, column_types): (Vec<String>, Vec<ResultColumnType>) = self
            .columns
            .into_iter()
            .map(|column| (column.name, column.column_type))
            .unzip();
        let rows = self
            .rows
            .into_iter()
            .enumerate()
            .map(|(row_index, row)| {
                if row.len() != column_count {
                    return Err(protocol_error(format!(
                        "execute result row {row_index} has {} values for {column_count} columns",
                        row.len()
                    )));
                }
                row.into_iter()
                    .enumerate()
                    .map(|(column_index, value)| {
                        let value = value.try_into_engine()?;
                        let declared = column_types[column_index];
                        if !matches!(value, Value::Null)
                            && ResultColumnType::from_value(&value) != declared
                        {
                            return Err(protocol_error(format!(
                                "execute result row {row_index} column {column_index} declares {declared:?} but contains {:?}",
                                ResultColumnType::from_value(&value)
                            )));
                        }
                        Ok(value)
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ExecuteResult::from_protocol_response(
            self.statement_index,
            self.label,
            columns,
            column_types,
            rows,
            self.rows_affected,
            self.notices,
        ))
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BeginTransactionResponse {
    pub transaction_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateBranchRequestBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_commit_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateBranchResponseBody {
    pub id: String,
    pub name: String,
    pub hidden: bool,
    pub commit_id: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UndoResponseBody {
    pub branch_id: String,
    pub target_commit_id: String,
    pub inverse_commit_id: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedoResponseBody {
    pub branch_id: String,
    pub target_commit_id: String,
    pub replay_commit_id: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SwitchBranchRequestBody<'a> {
    pub branch_id: &'a str,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwitchBranchResponseBody {
    pub branch_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MergeBranchRequestBody {
    pub source_branch_id: String,
}

impl From<MergeBranchOptions> for MergeBranchRequestBody {
    fn from(value: MergeBranchOptions) -> Self {
        Self {
            source_branch_id: value.source_branch_id,
        }
    }
}

impl From<MergeBranchRequestBody> for MergeBranchOptions {
    fn from(value: MergeBranchRequestBody) -> Self {
        Self {
            source_branch_id: value.source_branch_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MergeBranchPreviewRequestBody {
    pub source_branch_id: String,
}

impl From<MergeBranchPreviewOptions> for MergeBranchPreviewRequestBody {
    fn from(value: MergeBranchPreviewOptions) -> Self {
        Self {
            source_branch_id: value.source_branch_id,
        }
    }
}

impl From<MergeBranchPreviewRequestBody> for MergeBranchPreviewOptions {
    fn from(value: MergeBranchPreviewRequestBody) -> Self {
        Self {
            source_branch_id: value.source_branch_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MergeBranchResponseBody {
    pub outcome: MergeBranchOutcomeBody,
    pub target_branch_id: String,
    pub source_branch_id: String,
    pub base_commit_id: String,
    pub target_head_before_commit_id: String,
    pub source_head_before_commit_id: String,
    pub target_head_after_commit_id: String,
    pub created_merge_commit_id: Option<String>,
    pub change_stats: MergeChangeStatsBody,
}

impl From<MergeBranchReceipt> for MergeBranchResponseBody {
    fn from(value: MergeBranchReceipt) -> Self {
        Self {
            outcome: value.outcome.into(),
            target_branch_id: value.target_branch_id,
            source_branch_id: value.source_branch_id,
            base_commit_id: value.base_commit_id,
            target_head_before_commit_id: value.target_head_before_commit_id,
            source_head_before_commit_id: value.source_head_before_commit_id,
            target_head_after_commit_id: value.target_head_after_commit_id,
            created_merge_commit_id: value.created_merge_commit_id,
            change_stats: value.change_stats.into(),
        }
    }
}

impl From<MergeBranchResponseBody> for MergeBranchReceipt {
    fn from(value: MergeBranchResponseBody) -> Self {
        Self {
            outcome: value.outcome.into(),
            target_branch_id: value.target_branch_id,
            source_branch_id: value.source_branch_id,
            base_commit_id: value.base_commit_id,
            target_head_before_commit_id: value.target_head_before_commit_id,
            source_head_before_commit_id: value.source_head_before_commit_id,
            target_head_after_commit_id: value.target_head_after_commit_id,
            created_merge_commit_id: value.created_merge_commit_id,
            change_stats: value.change_stats.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MergeBranchPreviewResponseBody {
    pub outcome: MergeBranchOutcomeBody,
    pub target_branch_id: String,
    pub source_branch_id: String,
    pub base_commit_id: String,
    pub target_head_commit_id: String,
    pub source_head_commit_id: String,
    pub change_stats: MergeChangeStatsBody,
    pub conflicts: Vec<MergeConflictBody>,
}

impl From<MergeBranchPreview> for MergeBranchPreviewResponseBody {
    fn from(value: MergeBranchPreview) -> Self {
        Self {
            outcome: value.outcome.into(),
            target_branch_id: value.target_branch_id,
            source_branch_id: value.source_branch_id,
            base_commit_id: value.base_commit_id,
            target_head_commit_id: value.target_head_commit_id,
            source_head_commit_id: value.source_head_commit_id,
            change_stats: value.change_stats.into(),
            conflicts: value.conflicts.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<MergeBranchPreviewResponseBody> for MergeBranchPreview {
    fn from(value: MergeBranchPreviewResponseBody) -> Self {
        Self {
            outcome: value.outcome.into(),
            target_branch_id: value.target_branch_id,
            source_branch_id: value.source_branch_id,
            base_commit_id: value.base_commit_id,
            target_head_commit_id: value.target_head_commit_id,
            source_head_commit_id: value.source_head_commit_id,
            change_stats: value.change_stats.into(),
            conflicts: value.conflicts.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum MergeBranchOutcomeBody {
    AlreadyUpToDate,
    FastForward,
    MergeCommitted,
}

impl From<MergeBranchOutcome> for MergeBranchOutcomeBody {
    fn from(value: MergeBranchOutcome) -> Self {
        match value {
            MergeBranchOutcome::AlreadyUpToDate => Self::AlreadyUpToDate,
            MergeBranchOutcome::FastForward => Self::FastForward,
            MergeBranchOutcome::MergeCommitted => Self::MergeCommitted,
        }
    }
}

impl From<MergeBranchOutcomeBody> for MergeBranchOutcome {
    fn from(value: MergeBranchOutcomeBody) -> Self {
        match value {
            MergeBranchOutcomeBody::AlreadyUpToDate => Self::AlreadyUpToDate,
            MergeBranchOutcomeBody::FastForward => Self::FastForward,
            MergeBranchOutcomeBody::MergeCommitted => Self::MergeCommitted,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MergeChangeStatsBody {
    pub total: usize,
    pub added: usize,
    pub modified: usize,
    pub removed: usize,
}

impl From<MergeChangeStats> for MergeChangeStatsBody {
    fn from(value: MergeChangeStats) -> Self {
        Self {
            total: value.total,
            added: value.added,
            modified: value.modified,
            removed: value.removed,
        }
    }
}

impl From<MergeChangeStatsBody> for MergeChangeStats {
    fn from(value: MergeChangeStatsBody) -> Self {
        Self {
            total: value.total,
            added: value.added,
            modified: value.modified,
            removed: value.removed,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MergeConflictBody {
    pub kind: MergeConflictKindBody,
    pub row_ref: RowRef,
    pub file_id: Option<String>,
    pub target: MergeConflictSideBody,
    pub source: MergeConflictSideBody,
}

impl From<MergeConflict> for MergeConflictBody {
    fn from(value: MergeConflict) -> Self {
        Self {
            kind: value.kind.into(),
            row_ref: value.row_ref,
            file_id: value.file_id,
            target: value.target.into(),
            source: value.source.into(),
        }
    }
}

impl From<MergeConflictBody> for MergeConflict {
    fn from(value: MergeConflictBody) -> Self {
        Self {
            kind: value.kind.into(),
            row_ref: value.row_ref,
            file_id: value.file_id,
            target: value.target.into(),
            source: value.source.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum MergeConflictKindBody {
    SameRowChanged,
}

impl From<MergeConflictKind> for MergeConflictKindBody {
    fn from(value: MergeConflictKind) -> Self {
        match value {
            MergeConflictKind::SameRowChanged => Self::SameRowChanged,
        }
    }
}

impl From<MergeConflictKindBody> for MergeConflictKind {
    fn from(value: MergeConflictKindBody) -> Self {
        match value {
            MergeConflictKindBody::SameRowChanged => Self::SameRowChanged,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MergeConflictSideBody {
    pub kind: MergeConflictChangeKindBody,
    pub before_change_id: Option<String>,
    pub after_change_id: Option<String>,
}

impl From<MergeConflictSide> for MergeConflictSideBody {
    fn from(value: MergeConflictSide) -> Self {
        Self {
            kind: value.kind.into(),
            before_change_id: value.before_change_id,
            after_change_id: value.after_change_id,
        }
    }
}

impl From<MergeConflictSideBody> for MergeConflictSide {
    fn from(value: MergeConflictSideBody) -> Self {
        Self {
            kind: value.kind.into(),
            before_change_id: value.before_change_id,
            after_change_id: value.after_change_id,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum MergeConflictChangeKindBody {
    Added,
    Modified,
    Removed,
}

impl From<MergeConflictChangeKind> for MergeConflictChangeKindBody {
    fn from(value: MergeConflictChangeKind) -> Self {
        match value {
            MergeConflictChangeKind::Added => Self::Added,
            MergeConflictChangeKind::Modified => Self::Modified,
            MergeConflictChangeKind::Removed => Self::Removed,
        }
    }
}

impl From<MergeConflictChangeKindBody> for MergeConflictChangeKind {
    fn from(value: MergeConflictChangeKindBody) -> Self {
        match value {
            MergeConflictChangeKindBody::Added => Self::Added,
            MergeConflictChangeKindBody::Modified => Self::Modified,
            MergeConflictChangeKindBody::Removed => Self::Removed,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MultiplexObserveRequest<'a> {
    pub subscriptions: &'a [MultiplexObserveSubscription],
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MultiplexObserveSubscription {
    pub id: String,
    pub sql: String,
    pub params: Vec<WireValue>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MultiplexObserveNext {
    pub subscription_id: String,
    pub sequence: u64,
    pub mutation_sequence: u64,
    pub result: Option<ExecuteResponseBody>,
    pub delta: Option<ObserveDelta>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ObserveDelta {
    #[serde(rename = "single-blob-splice")]
    SingleBlobSplice {
        #[serde(rename = "baseSequence")]
        base_sequence: u64,
        #[serde(rename = "prefixBytes")]
        prefix_bytes: usize,
        #[serde(rename = "suffixBytes")]
        suffix_bytes: usize,
        #[serde(rename = "insertBase64")]
        insert_base64: String,
    },
    #[serde(rename = "row-splice")]
    RowSplice {
        #[serde(rename = "baseSequence")]
        base_sequence: u64,
        #[serde(rename = "prefixRows")]
        prefix_rows: usize,
        #[serde(rename = "deleteRows")]
        delete_rows: usize,
        #[serde(rename = "insertRows")]
        insert_rows: Vec<Vec<WireValue>>,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorEnvelope {
    pub error: ErrorBody,
    pub subscription_id: Option<String>,
    pub retryable: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ErrorBody {
    pub code: Option<String>,
    pub message: Option<String>,
    pub hint: Option<String>,
    pub details: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EmptyBody {}

pub fn encode_engine_values(values: &[Value]) -> Result<Vec<RequestWireValue>, LixError> {
    values
        .iter()
        .map(|value| Ok(RequestWireValue::Value(WireValue::try_from_engine(value)?)))
        .collect()
}

pub fn encode_engine_wire_values(values: &[Value]) -> Result<Vec<WireValue>, LixError> {
    values.iter().map(WireValue::try_from_engine).collect()
}

pub fn is_recoverable_session_error(error: &LixError) -> bool {
    error.code == SESSION_GONE_CODE || error.code == SERVER_CLOSED_CODE
}

pub fn validate_session_id(session_id: &str) -> Result<(), LixError> {
    if (1..=256).contains(&session_id.len())
        && session_id.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
    {
        Ok(())
    } else {
        Err(protocol_error(
            "Lix Server Protocol handshake requires a valid sessionId",
        ))
    }
}

pub fn protocol_error(message: impl Into<String>) -> LixError {
    LixError::new("LIX_SERVER_PROTOCOL_ERROR", message.into())
}

pub fn remote_error(code: impl Into<String>, message: impl Into<String>) -> LixError {
    LixError::new(code, message)
}

pub fn closed_error() -> LixError {
    LixError::new(LixError::CODE_CLOSED, "Lix is closed")
}

pub fn unsupported_remote_operation(operation: &str) -> LixError {
    LixError::new(
        "LIX_UNSUPPORTED_REMOTE_OPERATION",
        format!("{operation} is not supported in remote mode"),
    )
    .with_details(serde_json::json!({ "operation": operation }))
}
