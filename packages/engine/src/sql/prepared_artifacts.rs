use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlparser::ast::Statement;

use crate::backend::PreparedBatch;
use crate::backend::TransactionBeginMode;
use crate::catalog::{CatalogReadTimeProjectionRequest, ResolvedRelation, SurfaceReadFreshness};
use crate::common::Value;
use crate::history::{DirectoryHistoryRequest, FileHistoryRequest, StateHistoryRequest};
use crate::streams::StateCommitStreamOperation;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommittedReadMode {
    CommittedOnly,
    MaterializedState,
}

impl CommittedReadMode {
    pub fn transaction_mode(self) -> TransactionBeginMode {
        match self {
            Self::CommittedOnly => TransactionBeginMode::Read,
            Self::MaterializedState => TransactionBeginMode::Deferred,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicReadSource {
    PendingOverlay,
    Committed(CommittedReadMode),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum PendingOverlayLane {
    Tracked,
    Untracked,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PendingOverlayQuery {
    pub lane: PendingOverlayLane,
    pub schema_key: String,
    pub version_id: String,
    pub projections: Vec<PendingOverlayProjection>,
    pub filters: Vec<PendingOverlayFilter>,
    pub order_by: Vec<PendingOverlayOrderClause>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingOverlayProjection {
    Column {
        source_column: String,
        output_column: String,
    },
    CountAll {
        output_column: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum PendingOverlayFilter {
    Equals(String, Value),
    In(String, Vec<Value>),
    IsNull(String),
    IsNotNull(String),
    Like {
        column: String,
        pattern: String,
        case_insensitive: bool,
    },
    And(Vec<PendingOverlayFilter>),
    Or(Vec<PendingOverlayFilter>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingOverlayOrderClause {
    pub column: String,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct ProjectionQuery {
    pub projections: Vec<PendingOverlayProjection>,
    pub filters: Vec<PendingOverlayFilter>,
    pub order_by: Vec<PendingOverlayOrderClause>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct ReadTimeProjectionPlan {
    pub(crate) request: CatalogReadTimeProjectionRequest,
    pub(crate) query: ProjectionQuery,
}

impl ReadTimeProjectionPlan {
    pub(crate) fn request(&self) -> &CatalogReadTimeProjectionRequest {
        &self.request
    }

    pub(crate) fn surface_name(&self) -> &str {
        &self.request.surface_name
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum PreparedHistoryReadPlanKind {
    StateHistory,
    EntityHistory,
    FileHistory,
    DirectoryHistory,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedStateHistoryField {
    EntityId,
    SchemaKey,
    FileId,
    PluginKey,
    SnapshotContent,
    Metadata,
    SchemaVersion,
    ChangeId,
    CommitId,
    CommitCreatedAt,
    RootCommitId,
    Depth,
    VersionId,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedStateHistoryAggregate {
    Count,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedStateHistoryProjectionValue {
    Field(PreparedStateHistoryField),
    Aggregate(PreparedStateHistoryAggregate),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedStateHistoryProjection {
    pub output_name: String,
    pub value: PreparedStateHistoryProjectionValue,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedStateHistorySortValue {
    Field(PreparedStateHistoryField),
    Aggregate(PreparedStateHistoryAggregate),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedStateHistorySortKey {
    pub output_name: String,
    pub value: Option<PreparedStateHistorySortValue>,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedStateHistoryPredicate {
    Eq(PreparedStateHistoryField, Value),
    NotEq(PreparedStateHistoryField, Value),
    Gt(PreparedStateHistoryField, Value),
    GtEq(PreparedStateHistoryField, Value),
    Lt(PreparedStateHistoryField, Value),
    LtEq(PreparedStateHistoryField, Value),
    In(PreparedStateHistoryField, Vec<Value>),
    IsNull(PreparedStateHistoryField),
    IsNotNull(PreparedStateHistoryField),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedStateHistoryAggregatePredicate {
    Eq(PreparedStateHistoryAggregate, i64),
    NotEq(PreparedStateHistoryAggregate, i64),
    Gt(PreparedStateHistoryAggregate, i64),
    GtEq(PreparedStateHistoryAggregate, i64),
    Lt(PreparedStateHistoryAggregate, i64),
    LtEq(PreparedStateHistoryAggregate, i64),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedStateHistoryReadPlan {
    pub request: StateHistoryRequest,
    pub predicates: Vec<PreparedStateHistoryPredicate>,
    pub projections: Vec<PreparedStateHistoryProjection>,
    pub wildcard_projection: bool,
    pub wildcard_columns: Vec<String>,
    pub group_by_fields: Vec<PreparedStateHistoryField>,
    pub having: Option<PreparedStateHistoryAggregatePredicate>,
    pub sort_keys: Vec<PreparedStateHistorySortKey>,
    pub limit: Option<u64>,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedEntityHistoryField {
    Property(String),
    State(PreparedStateHistoryField),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedEntityHistoryProjection {
    pub output_name: String,
    pub field: PreparedEntityHistoryField,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedEntityHistorySortKey {
    pub output_name: String,
    pub field: Option<PreparedEntityHistoryField>,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedEntityHistoryPredicate {
    Eq(PreparedEntityHistoryField, Value),
    NotEq(PreparedEntityHistoryField, Value),
    Gt(PreparedEntityHistoryField, Value),
    GtEq(PreparedEntityHistoryField, Value),
    Lt(PreparedEntityHistoryField, Value),
    LtEq(PreparedEntityHistoryField, Value),
    In(PreparedEntityHistoryField, Vec<Value>),
    IsNull(PreparedEntityHistoryField),
    IsNotNull(PreparedEntityHistoryField),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedEntityHistoryReadPlan {
    pub resolved_relation: ResolvedRelation,
    pub request: StateHistoryRequest,
    pub predicates: Vec<PreparedEntityHistoryPredicate>,
    pub projections: Vec<PreparedEntityHistoryProjection>,
    pub wildcard_projection: bool,
    pub wildcard_columns: Vec<String>,
    pub sort_keys: Vec<PreparedEntityHistorySortKey>,
    pub limit: Option<u64>,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedFileHistoryField {
    Id,
    Path,
    Data,
    Metadata,
    Hidden,
    EntityId,
    SchemaKey,
    FileId,
    VersionId,
    PluginKey,
    SchemaVersion,
    ChangeId,
    LixcolMetadata,
    CommitId,
    CommitCreatedAt,
    RootCommitId,
    Depth,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedFileHistoryProjection {
    pub output_name: String,
    pub field: PreparedFileHistoryField,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedFileHistorySortKey {
    pub output_name: String,
    pub field: Option<PreparedFileHistoryField>,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedFileHistoryPredicate {
    Eq(PreparedFileHistoryField, Value),
    NotEq(PreparedFileHistoryField, Value),
    Gt(PreparedFileHistoryField, Value),
    GtEq(PreparedFileHistoryField, Value),
    Lt(PreparedFileHistoryField, Value),
    LtEq(PreparedFileHistoryField, Value),
    In(PreparedFileHistoryField, Vec<Value>),
    IsNull(PreparedFileHistoryField),
    IsNotNull(PreparedFileHistoryField),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedFileHistoryAggregate {
    Count,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedFileHistoryReadPlan {
    pub request: FileHistoryRequest,
    pub predicates: Vec<PreparedFileHistoryPredicate>,
    pub projections: Vec<PreparedFileHistoryProjection>,
    pub wildcard_projection: bool,
    pub wildcard_columns: Vec<String>,
    pub sort_keys: Vec<PreparedFileHistorySortKey>,
    pub limit: Option<u64>,
    pub offset: u64,
    pub aggregate: Option<PreparedFileHistoryAggregate>,
    pub aggregate_output_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedDirectoryHistoryField {
    Id,
    ParentId,
    Name,
    Path,
    Hidden,
    EntityId,
    SchemaKey,
    FileId,
    VersionId,
    PluginKey,
    SchemaVersion,
    ChangeId,
    LixcolMetadata,
    CommitId,
    CommitCreatedAt,
    RootCommitId,
    Depth,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedDirectoryHistoryProjection {
    pub output_name: String,
    pub field: PreparedDirectoryHistoryField,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedDirectoryHistorySortKey {
    pub output_name: String,
    pub field: Option<PreparedDirectoryHistoryField>,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedDirectoryHistoryPredicate {
    Eq(PreparedDirectoryHistoryField, Value),
    NotEq(PreparedDirectoryHistoryField, Value),
    Gt(PreparedDirectoryHistoryField, Value),
    GtEq(PreparedDirectoryHistoryField, Value),
    Lt(PreparedDirectoryHistoryField, Value),
    LtEq(PreparedDirectoryHistoryField, Value),
    In(PreparedDirectoryHistoryField, Vec<Value>),
    IsNull(PreparedDirectoryHistoryField),
    IsNotNull(PreparedDirectoryHistoryField),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedDirectoryHistoryAggregate {
    Count,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedDirectoryHistoryReadPlan {
    pub request: DirectoryHistoryRequest,
    pub predicates: Vec<PreparedDirectoryHistoryPredicate>,
    pub projections: Vec<PreparedDirectoryHistoryProjection>,
    pub wildcard_projection: bool,
    pub wildcard_columns: Vec<String>,
    pub sort_keys: Vec<PreparedDirectoryHistorySortKey>,
    pub limit: Option<u64>,
    pub offset: u64,
    pub aggregate: Option<PreparedDirectoryHistoryAggregate>,
    pub aggregate_output_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedHistoryReadPlan {
    StateHistory(PreparedStateHistoryReadPlan),
    EntityHistory(PreparedEntityHistoryReadPlan),
    FileHistory(PreparedFileHistoryReadPlan),
    DirectoryHistory(PreparedDirectoryHistoryReadPlan),
}

#[allow(dead_code)]
impl PreparedHistoryReadPlan {
    pub fn kind(&self) -> PreparedHistoryReadPlanKind {
        match self {
            Self::StateHistory(_) => PreparedHistoryReadPlanKind::StateHistory,
            Self::EntityHistory(_) => PreparedHistoryReadPlanKind::EntityHistory,
            Self::FileHistory(_) => PreparedHistoryReadPlanKind::FileHistory,
            Self::DirectoryHistory(_) => PreparedHistoryReadPlanKind::DirectoryHistory,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct PreparedReadTimeProjectionArtifact {
    pub(crate) read: ReadTimeProjectionPlan,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct PreparedBatchReadArtifact {
    pub(crate) prepared_batch: PreparedBatch,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct PreparedHistoryReadArtifact {
    pub(crate) plan: PreparedHistoryReadPlan,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) enum PreparedPublicReadPlanArtifact {
    ReadTimeProjection(PreparedReadTimeProjectionArtifact),
    PreparedBatch(PreparedBatchReadArtifact),
    HistoryRead(PreparedHistoryReadArtifact),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedPublicRead {
    pub contract: PreparedPublicReadContract,
    pub freshness_contract: SurfaceReadFreshness,
    pub resolved_relations: Vec<String>,
    pub public_output_columns: Option<Vec<String>>,
    pub execution: PreparedPublicReadPlanArtifact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum PreparedExplainMode {
    Plain,
    Analyze,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct PreparedAnalyzedRuntime {
    pub execution_duration_us: u64,
    pub output_row_count: usize,
    pub output_column_count: usize,
    #[serde(default)]
    pub output_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[allow(dead_code)]
pub enum PreparedExplainTemplate {
    Text { sections: Vec<(String, String)> },
    Json { base_json: JsonValue },
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct ReadDiagnosticCatalogSnapshot {
    pub public_surfaces: Vec<String>,
    pub available_tables: Vec<String>,
    pub available_columns_by_relation: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Default)]
#[allow(dead_code)]
pub struct ReadDiagnosticContext {
    pub source_sql: Vec<String>,
    pub relation_names: Vec<String>,
    pub catalog_snapshot: ReadDiagnosticCatalogSnapshot,
    pub explain_mode: Option<PreparedExplainMode>,
    pub plain_explain_template: Option<PreparedExplainTemplate>,
    pub analyzed_explain_template: Option<PreparedExplainTemplate>,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedReadArtifact {
    Public(PreparedPublicRead),
    Scalar(PreparedBatchReadArtifact),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedReadStatement {
    pub transaction_mode: TransactionBeginMode,
    pub artifact: PreparedReadArtifact,
    pub diagnostic_context: ReadDiagnosticContext,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedReadBatch {
    pub transaction_mode: TransactionBeginMode,
    pub statements: Vec<PreparedReadStatement>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicReadResultColumn {
    Untyped,
    Boolean,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicReadResultColumns {
    Static(Vec<PublicReadResultColumn>),
    ByColumnName(BTreeMap<String, PublicReadResultColumn>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct PreparedPublicReadContract {
    pub committed_mode: CommittedReadMode,
    pub pending_overlay_query: Option<PendingOverlayQuery>,
    pub result_columns: Option<PublicReadResultColumns>,
}

impl PreparedPublicReadContract {
    pub fn source(&self) -> PublicReadSource {
        if self.pending_overlay_query.is_some() {
            PublicReadSource::PendingOverlay
        } else {
            PublicReadSource::Committed(self.committed_mode)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum PreparedWriteStatementKind {
    Query,
    Explain,
    Write,
}

#[allow(dead_code)]
impl PreparedWriteStatementKind {
    pub fn for_statement(statement: &Statement) -> Self {
        match statement {
            Statement::Query(_) => Self::Query,
            Statement::Explain { .. } => Self::Explain,
            _ => Self::Write,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum PreparedWriteOperationKind {
    Insert,
    Update,
    Delete,
}

#[allow(dead_code)]
impl PreparedWriteOperationKind {
    pub fn state_commit_stream_operation(self) -> StateCommitStreamOperation {
        match self {
            Self::Insert => StateCommitStreamOperation::Insert,
            Self::Update => StateCommitStreamOperation::Update,
            Self::Delete => StateCommitStreamOperation::Delete,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum PreparedInsertOnConflictAction {
    DoUpdate,
    DoNothing,
}

#[derive(Debug, Clone, PartialEq, Default)]
#[allow(dead_code)]
pub struct WriteDiagnosticContext {
    pub relation_names: Vec<String>,
    pub explain_mode: Option<PreparedExplainMode>,
    pub plain_explain_template: Option<PreparedExplainTemplate>,
    pub analyzed_explain_template: Option<PreparedExplainTemplate>,
}

#[allow(dead_code)]
impl WriteDiagnosticContext {
    pub fn new(relation_names: Vec<String>) -> Self {
        Self {
            relation_names,
            explain_mode: None,
            plain_explain_template: None,
            analyzed_explain_template: None,
        }
    }

    pub fn relation_names(&self) -> &[String] {
        &self.relation_names
    }
}
