use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlparser::ast::{Expr, Statement};

use crate::catalog::{SurfaceBinding, SurfaceFamily, SurfaceReadFreshness, SurfaceVariant};
use crate::common::error::LixError;
use crate::common::types::Value;
use crate::contracts::ReplayCursor;
use crate::contracts::TransactionMode;

#[derive(Debug, Clone, PartialEq)]
pub struct PreparedStatement {
    pub sql: String,
    pub params: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PreparedBatch {
    pub steps: Vec<PreparedStatement>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SchemaKey {
    pub schema_key: String,
    pub schema_version: String,
}

impl SchemaKey {
    pub fn new(schema_key: impl Into<String>, schema_version: impl Into<String>) -> Self {
        Self {
            schema_key: schema_key.into(),
            schema_version: schema_version.into(),
        }
    }

    pub fn entity_id(&self) -> String {
        format!("{}~{}", self.schema_key, self.schema_version)
    }

    pub fn version_number(&self) -> Option<u64> {
        self.schema_version.parse::<u64>().ok()
    }
}

/// Semantic frontier for committed state selected by replica-local version
/// heads.
///
/// The commit DAG remains canonical, but this mapping records which committed
/// head each local engine instance currently chooses for each version id.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct CommittedVersionFrontier {
    pub version_heads: BTreeMap<String, String>,
}

impl CommittedVersionFrontier {
    pub fn is_empty(&self) -> bool {
        self.version_heads.is_empty()
    }

    pub fn describe(&self) -> String {
        if self.version_heads.is_empty() {
            return "(empty)".to_string();
        }

        self.version_heads
            .iter()
            .map(|(version_id, commit_id)| format!("{version_id}={commit_id}"))
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub fn to_json_string(&self) -> String {
        serde_json::to_string(self).expect("committed frontier serialization should succeed")
    }

    pub fn from_json_str(value: &str) -> Result<Self, LixError> {
        serde_json::from_str(value).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("invalid committed frontier json: {error}"),
            )
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UpdatedVersionRef {
    pub version_id: crate::VersionId,
    pub commit_id: String,
    pub created_at: String,
}

/// Durable output of a canonical commit.
///
/// `commit_id`, `updated_version_refs`, and `affected_versions` describe the
/// semantic outcome. `replay_cursor` is included so local derived projections
/// can catch up without becoming canonical truth.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CanonicalCommitReceipt {
    pub commit_id: String,
    pub replay_cursor: ReplayCursor,
    pub updated_version_refs: Vec<UpdatedVersionRef>,
    pub affected_versions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PendingPublicCommitLane {
    Version(String),
    GlobalAdmin,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PendingPublicCommitSession {
    pub lane: PendingPublicCommitLane,
    pub commit_id: String,
    pub commit_change_snapshot_id: String,
    pub commit_snapshot: JsonValue,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum StateCommitStreamOperation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StateCommitStreamChange {
    pub operation: StateCommitStreamOperation,
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: String,
    pub version_id: String,
    pub plugin_key: String,
    pub snapshot_content: Option<JsonValue>,
    pub untracked: bool,
    /// Runtime notification metadata used for listener filtering and echo
    /// suppression. This is not canonical committed state.
    pub writer_key: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ExecuteOptions {
    pub writer_key: Option<String>,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum SessionDependency {
    ActiveVersion,
    ActiveAccounts,
    PublicSurfaceRegistryGeneration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionExecutionMode {
    CommittedRead,
    CommittedRuntimeMutation,
    WriteTransaction,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub struct SessionStateSnapshot {
    pub active_version_id: String,
    #[serde(default)]
    pub active_account_ids: Vec<String>,
    #[serde(default)]
    pub generation: u64,
    #[serde(default)]
    pub public_surface_registry_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub struct SessionStateDelta {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_active_version_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_active_account_ids: Option<Vec<String>>,
    #[serde(default)]
    pub persist_workspace: bool,
}

impl SessionStateDelta {
    pub fn is_empty(&self) -> bool {
        self.next_active_version_id.is_none()
            && self.next_active_account_ids.is_none()
            && !self.persist_workspace
    }

    pub fn merge(&mut self, other: SessionStateDelta) {
        if other.next_active_version_id.is_some() {
            self.next_active_version_id = other.next_active_version_id;
        }
        if other.next_active_account_ids.is_some() {
            self.next_active_account_ids = other.next_active_account_ids;
        }
        self.persist_workspace |= other.persist_workspace;
    }

    #[allow(dead_code)]
    pub fn dependencies(&self) -> BTreeSet<SessionDependency> {
        let mut dependencies = BTreeSet::new();
        if self.next_active_version_id.is_some() {
            dependencies.insert(SessionDependency::ActiveVersion);
        }
        if self.next_active_account_ids.is_some() {
            dependencies.insert(SessionDependency::ActiveAccounts);
        }
        dependencies
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectiveStateVersionScope {
    ActiveVersion,
    ExplicitVersion,
    History,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveStateRequest {
    pub schema_set: BTreeSet<String>,
    pub version_scope: EffectiveStateVersionScope,
    pub include_global_overlay: bool,
    pub include_untracked_overlay: bool,
    pub include_tombstones: bool,
    pub predicate_classes: Vec<String>,
    pub required_columns: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommittedReadMode {
    CommittedOnly,
    MaterializedState,
}

impl CommittedReadMode {
    pub fn transaction_mode(self) -> TransactionMode {
        match self {
            Self::CommittedOnly => TransactionMode::Read,
            Self::MaterializedState => TransactionMode::Deferred,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicReadExecutionMode {
    PendingView,
    Committed(CommittedReadMode),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum PendingViewReadStorage {
    Tracked,
    Untracked,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PendingViewReadQuery {
    pub storage: PendingViewReadStorage,
    pub schema_key: String,
    pub version_id: String,
    pub projections: Vec<PendingViewProjection>,
    pub filters: Vec<PendingViewFilter>,
    pub order_by: Vec<PendingViewOrderClause>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingViewProjection {
    Column {
        source_column: String,
        output_column: String,
    },
    CountAll {
        output_column: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum PendingViewFilter {
    Equals(String, Value),
    In(String, Vec<Value>),
    IsNull(String),
    IsNotNull(String),
    Like {
        column: String,
        pattern: String,
        case_insensitive: bool,
    },
    And(Vec<PendingViewFilter>),
    Or(Vec<PendingViewFilter>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingViewOrderClause {
    pub column: String,
    pub descending: bool,
}

/// Public surface family currently served from `ReadTime` projection output.
///
/// Phase B of Plan 33 resolves the current version-surface ambiguity
/// explicitly: the first projection-backed public serving cut targets the real
/// builtin `lix_version` surface, not a stale `lix_version_by_version` alias.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum ReadTimeProjectionSurface {
    LixVersion,
    LixFile,
    LixFileByVersion,
    LixDirectory,
    LixDirectoryByVersion,
}

#[allow(dead_code)]
impl ReadTimeProjectionSurface {
    pub fn from_public_name(public_name: &str) -> Option<Self> {
        match public_name {
            "lix_version" => Some(Self::LixVersion),
            "lix_file" => Some(Self::LixFile),
            "lix_file_by_version" => Some(Self::LixFileByVersion),
            "lix_directory" => Some(Self::LixDirectory),
            "lix_directory_by_version" => Some(Self::LixDirectoryByVersion),
            _ => None,
        }
    }

    pub fn public_name(self) -> &'static str {
        match self {
            Self::LixVersion => "lix_version",
            Self::LixFile => "lix_file",
            Self::LixFileByVersion => "lix_file_by_version",
            Self::LixDirectory => "lix_directory",
            Self::LixDirectoryByVersion => "lix_directory_by_version",
        }
    }

    pub fn surface_family(self) -> SurfaceFamily {
        match self {
            Self::LixVersion => SurfaceFamily::Admin,
            Self::LixFile
            | Self::LixFileByVersion
            | Self::LixDirectory
            | Self::LixDirectoryByVersion => SurfaceFamily::Filesystem,
        }
    }

    pub fn surface_variant(self) -> SurfaceVariant {
        match self {
            Self::LixVersion => SurfaceVariant::Default,
            Self::LixFile | Self::LixDirectory => SurfaceVariant::Default,
            Self::LixFileByVersion | Self::LixDirectoryByVersion => SurfaceVariant::ByVersion,
        }
    }
}

/// Runtime-neutral query shape over rows derived from a `ReadTime` projection.
///
/// This intentionally mirrors the first bounded serving cut for `lix_version`:
/// simple projection/filter/order/limit over a single public projection-backed
/// surface.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct ReadTimeProjectionReadQuery {
    pub projections: Vec<PendingViewProjection>,
    pub filters: Vec<PendingViewFilter>,
    pub order_by: Vec<PendingViewOrderClause>,
    pub limit: Option<usize>,
}

/// Compiler-owned artifact for a public read that should be served from
/// `ReadTime` projection output.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct ReadTimeProjectionRead {
    pub surface: ReadTimeProjectionSurface,
    pub requested_version_id: Option<String>,
    pub query: ReadTimeProjectionReadQuery,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum PreparedDirectPublicReadKind {
    StateHistory,
    EntityHistory,
    FileHistory,
    DirectoryHistory,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedDirectStateHistoryField {
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
    Field(PreparedDirectStateHistoryField),
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
    Field(PreparedDirectStateHistoryField),
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
    Eq(PreparedDirectStateHistoryField, Value),
    NotEq(PreparedDirectStateHistoryField, Value),
    Gt(PreparedDirectStateHistoryField, Value),
    GtEq(PreparedDirectStateHistoryField, Value),
    Lt(PreparedDirectStateHistoryField, Value),
    LtEq(PreparedDirectStateHistoryField, Value),
    In(PreparedDirectStateHistoryField, Vec<Value>),
    IsNull(PreparedDirectStateHistoryField),
    IsNotNull(PreparedDirectStateHistoryField),
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
pub struct PreparedStateHistoryDirectReadPlan {
    pub request: StateHistoryRequest,
    pub predicates: Vec<PreparedStateHistoryPredicate>,
    pub projections: Vec<PreparedStateHistoryProjection>,
    pub wildcard_projection: bool,
    pub wildcard_columns: Vec<String>,
    pub group_by_fields: Vec<PreparedDirectStateHistoryField>,
    pub having: Option<PreparedStateHistoryAggregatePredicate>,
    pub sort_keys: Vec<PreparedStateHistorySortKey>,
    pub limit: Option<u64>,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedDirectEntityHistoryField {
    Property(String),
    State(PreparedDirectStateHistoryField),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedEntityHistoryProjection {
    pub output_name: String,
    pub field: PreparedDirectEntityHistoryField,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedEntityHistorySortKey {
    pub output_name: String,
    pub field: Option<PreparedDirectEntityHistoryField>,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedEntityHistoryPredicate {
    Eq(PreparedDirectEntityHistoryField, Value),
    NotEq(PreparedDirectEntityHistoryField, Value),
    Gt(PreparedDirectEntityHistoryField, Value),
    GtEq(PreparedDirectEntityHistoryField, Value),
    Lt(PreparedDirectEntityHistoryField, Value),
    LtEq(PreparedDirectEntityHistoryField, Value),
    In(PreparedDirectEntityHistoryField, Vec<Value>),
    IsNull(PreparedDirectEntityHistoryField),
    IsNotNull(PreparedDirectEntityHistoryField),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedEntityHistoryDirectReadPlan {
    pub surface_binding: SurfaceBinding,
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
pub enum PreparedDirectFileHistoryField {
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
    pub field: PreparedDirectFileHistoryField,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedFileHistorySortKey {
    pub output_name: String,
    pub field: Option<PreparedDirectFileHistoryField>,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedFileHistoryPredicate {
    Eq(PreparedDirectFileHistoryField, Value),
    NotEq(PreparedDirectFileHistoryField, Value),
    Gt(PreparedDirectFileHistoryField, Value),
    GtEq(PreparedDirectFileHistoryField, Value),
    Lt(PreparedDirectFileHistoryField, Value),
    LtEq(PreparedDirectFileHistoryField, Value),
    In(PreparedDirectFileHistoryField, Vec<Value>),
    IsNull(PreparedDirectFileHistoryField),
    IsNotNull(PreparedDirectFileHistoryField),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedFileHistoryAggregate {
    Count,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedFileHistoryDirectReadPlan {
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
pub enum PreparedDirectDirectoryHistoryField {
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
    pub field: PreparedDirectDirectoryHistoryField,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedDirectoryHistorySortKey {
    pub output_name: String,
    pub field: Option<PreparedDirectDirectoryHistoryField>,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedDirectoryHistoryPredicate {
    Eq(PreparedDirectDirectoryHistoryField, Value),
    NotEq(PreparedDirectDirectoryHistoryField, Value),
    Gt(PreparedDirectDirectoryHistoryField, Value),
    GtEq(PreparedDirectDirectoryHistoryField, Value),
    Lt(PreparedDirectDirectoryHistoryField, Value),
    LtEq(PreparedDirectDirectoryHistoryField, Value),
    In(PreparedDirectDirectoryHistoryField, Vec<Value>),
    IsNull(PreparedDirectDirectoryHistoryField),
    IsNotNull(PreparedDirectDirectoryHistoryField),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedDirectoryHistoryAggregate {
    Count,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedDirectoryHistoryDirectReadPlan {
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
pub enum PreparedDirectPublicRead {
    StateHistory(PreparedStateHistoryDirectReadPlan),
    EntityHistory(PreparedEntityHistoryDirectReadPlan),
    FileHistory(PreparedFileHistoryDirectReadPlan),
    DirectoryHistory(PreparedDirectoryHistoryDirectReadPlan),
}

#[allow(dead_code)]
impl PreparedDirectPublicRead {
    pub fn kind(&self) -> PreparedDirectPublicReadKind {
        match self {
            Self::StateHistory(_) => PreparedDirectPublicReadKind::StateHistory,
            Self::EntityHistory(_) => PreparedDirectPublicReadKind::EntityHistory,
            Self::FileHistory(_) => PreparedDirectPublicReadKind::FileHistory,
            Self::DirectoryHistory(_) => PreparedDirectPublicReadKind::DirectoryHistory,
        }
    }
}

/// Runtime-neutral execution artifact for a prepared public read.
///
/// This intentionally stays on contract-owned DTOs. It does not depend on
/// SQL AST, binder output, logical-plan IR, or executor-private wrapper types.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedPublicReadExecutionArtifact {
    ReadTimeProjection(ReadTimeProjectionRead),
    LoweredSql(PreparedBatch),
    Direct(PreparedDirectPublicRead),
}

/// Runtime-neutral prepared public-read package.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedPublicReadArtifact {
    pub contract: PreparedPublicReadContract,
    pub freshness_contract: SurfaceReadFreshness,
    pub surface_bindings: Vec<String>,
    pub public_output_columns: Option<Vec<String>>,
    pub execution: PreparedPublicReadExecutionArtifact,
}

/// Runtime-neutral prepared internal-read package.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedInternalReadArtifact {
    pub prepared_batch: PreparedBatch,
    pub result_contract: ResultContract,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum PreparedExplainMode {
    Plain,
    Analyze,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[allow(dead_code)]
pub struct PreparedAnalyzedRuntime {
    pub execution_duration_us: u64,
    pub output_row_count: usize,
    pub output_column_count: usize,
    #[serde(default)]
    pub output_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
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

/// Diagnostic context handed to read runtime alongside a prepared read step.
///
/// The context is intentionally text-shaped so runtime can report/normalize
/// errors and route explain behavior without importing parser/executor-private
/// statement types.
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
    Public(PreparedPublicReadArtifact),
    Internal(PreparedInternalReadArtifact),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedReadStep {
    pub transaction_mode: TransactionMode,
    pub artifact: PreparedReadArtifact,
    pub diagnostic_context: ReadDiagnosticContext,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedReadProgram {
    pub transaction_mode: TransactionMode,
    pub steps: Vec<PreparedReadStep>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum PreparedWriteStatementKind {
    Query,
    Explain,
    Other,
}

#[allow(dead_code)]
impl PreparedWriteStatementKind {
    pub fn for_statement(statement: &Statement) -> Self {
        match statement {
            Statement::Query(_) => Self::Query,
            Statement::Explain { .. } => Self::Explain,
            _ => Self::Other,
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

/// Diagnostic context handed to write runtime alongside a prepared write step.
///
/// The context stays text-shaped and template-shaped so runtime can normalize
/// backend errors and render EXPLAIN output without importing parser-owned or
/// compiler-owned statement structures.
#[derive(Debug, Clone, PartialEq, Default)]
#[allow(dead_code)]
pub struct PreparedWriteDiagnosticContext {
    pub relation_names: Vec<String>,
    pub explain_mode: Option<PreparedExplainMode>,
    pub plain_explain_template: Option<PreparedExplainTemplate>,
    pub analyzed_explain_template: Option<PreparedExplainTemplate>,
}

#[allow(dead_code)]
impl PreparedWriteDiagnosticContext {
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

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedPublicSurfaceRegistryMutation {
    UpsertRegisteredSchemaSnapshot { snapshot: JsonValue },
    RemoveDynamicSchema { schema_key: String },
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedPublicSurfaceRegistryEffect {
    None,
    ApplyMutations(Vec<PreparedPublicSurfaceRegistryMutation>),
    ReloadFromStorage,
}

#[allow(dead_code)]
impl PreparedPublicSurfaceRegistryEffect {
    pub fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedResolvedWritePartition {
    pub execution_mode: WriteMode,
    pub authoritative_pre_state_rows: Vec<PlannedStateRow>,
    pub intended_post_state: Vec<PlannedStateRow>,
    pub writer_key_updates: BTreeMap<PlannedRowIdentity, Option<String>>,
    pub filesystem_state: PlannedFilesystemState,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedResolvedWritePlan {
    pub partitions: Vec<PreparedResolvedWritePartition>,
}

#[allow(dead_code)]
impl PreparedResolvedWritePlan {
    pub fn authoritative_pre_state_rows(&self) -> impl Iterator<Item = &PlannedStateRow> {
        self.partitions
            .iter()
            .flat_map(|partition| partition.authoritative_pre_state_rows.iter())
    }

    pub fn intended_post_state(&self) -> impl Iterator<Item = &PlannedStateRow> {
        self.partitions
            .iter()
            .flat_map(|partition| partition.intended_post_state.iter())
    }

    pub fn filesystem_state(&self) -> PlannedFilesystemState {
        let mut merged = PlannedFilesystemState::default();
        for partition in &self.partitions {
            merged.merge_from(&partition.filesystem_state);
        }
        merged
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedPublicWriteContract {
    pub operation_kind: PreparedWriteOperationKind,
    pub target: SurfaceBinding,
    pub on_conflict_action: Option<PreparedInsertOnConflictAction>,
    pub requested_version_id: Option<String>,
    pub active_account_ids: Vec<String>,
    pub writer_key: Option<String>,
    pub resolved_write_plan: Option<PreparedResolvedWritePlan>,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedTrackedWriteExecution {
    pub schema_live_table_requirements: Vec<SchemaLiveTableRequirement>,
    pub change_batch: Option<ChangeBatch>,
    pub create_preconditions: CommitPreconditions,
    pub semantic_effects: PlanEffects,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedUntrackedWriteExecution {
    pub intended_post_state: Vec<PlannedStateRow>,
    pub semantic_effects: PlanEffects,
    pub persist_filesystem_payloads_before_write: bool,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedPublicWriteExecutionPartition {
    Tracked(PreparedTrackedWriteExecution),
    Untracked(PreparedUntrackedWriteExecution),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedPublicWriteMaterialization {
    pub partitions: Vec<PreparedPublicWriteExecutionPartition>,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedPublicWriteExecutionArtifact {
    Noop,
    Materialize(PreparedPublicWriteMaterialization),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedPublicWriteArtifact {
    pub contract: PreparedPublicWriteContract,
    pub execution: PreparedPublicWriteExecutionArtifact,
}

#[allow(dead_code)]
impl PreparedPublicWriteArtifact {
    pub fn materialization(&self) -> Option<&PreparedPublicWriteMaterialization> {
        match &self.execution {
            PreparedPublicWriteExecutionArtifact::Noop => None,
            PreparedPublicWriteExecutionArtifact::Materialize(materialization) => {
                Some(materialization)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedInternalWriteArtifact {
    pub prepared_batch: PreparedBatch,
    pub live_table_requirements: Vec<SchemaLiveTableRequirement>,
    pub mutations: Vec<MutationRow>,
    pub has_update_validations: bool,
    pub should_refresh_file_cache: bool,
    pub read_only_query: bool,
    pub filesystem_state: PlannedFilesystemState,
    pub effects: PlanEffects,
    pub writer_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedWriteArtifact {
    PublicRead(PreparedPublicReadArtifact),
    PublicWrite(PreparedPublicWriteArtifact),
    Internal(PreparedInternalWriteArtifact),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedWriteStep {
    pub statement_kind: PreparedWriteStatementKind,
    pub result_contract: ResultContract,
    pub artifact: PreparedWriteArtifact,
    pub diagnostic_context: PreparedWriteDiagnosticContext,
    pub public_surface_registry_effect: PreparedPublicSurfaceRegistryEffect,
}

#[allow(dead_code)]
impl PreparedWriteStep {
    pub fn public_read(&self) -> Option<&PreparedPublicReadArtifact> {
        match &self.artifact {
            PreparedWriteArtifact::PublicRead(read) => Some(read),
            PreparedWriteArtifact::PublicWrite(_) | PreparedWriteArtifact::Internal(_) => None,
        }
    }

    pub fn public_write(&self) -> Option<&PreparedPublicWriteArtifact> {
        match &self.artifact {
            PreparedWriteArtifact::PublicWrite(write) => Some(write),
            PreparedWriteArtifact::PublicRead(_) | PreparedWriteArtifact::Internal(_) => None,
        }
    }

    pub fn internal_write(&self) -> Option<&PreparedInternalWriteArtifact> {
        match &self.artifact {
            PreparedWriteArtifact::Internal(internal) => Some(internal),
            PreparedWriteArtifact::PublicRead(_) | PreparedWriteArtifact::PublicWrite(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedWriteProgram {
    pub steps: Vec<PreparedWriteStep>,
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
    pub pending_view_query: Option<PendingViewReadQuery>,
    pub result_columns: Option<PublicReadResultColumns>,
}

impl PreparedPublicReadContract {
    pub fn execution_mode(&self) -> PublicReadExecutionMode {
        if self.pending_view_query.is_some() {
            PublicReadExecutionMode::PendingView
        } else {
            PublicReadExecutionMode::Committed(self.committed_mode)
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StateHistoryContentMode {
    MetadataOnly,
    #[default]
    IncludeSnapshotContent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StateHistoryOrder {
    #[default]
    EntityFileSchemaDepthAsc,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum StateHistoryRootScope {
    #[default]
    AllRoots,
    RequestedRoots(Vec<String>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StateHistoryLineageScope {
    #[default]
    Standard,
    ActiveVersion,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum StateHistoryVersionScope {
    #[default]
    Any,
    RequestedVersions(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StateHistoryRequest {
    pub root_scope: StateHistoryRootScope,
    pub lineage_scope: StateHistoryLineageScope,
    pub active_version_id: Option<String>,
    pub version_scope: StateHistoryVersionScope,
    pub entity_ids: Vec<String>,
    pub file_ids: Vec<String>,
    pub schema_keys: Vec<String>,
    pub plugin_keys: Vec<String>,
    pub min_depth: Option<i64>,
    pub max_depth: Option<i64>,
    pub content_mode: StateHistoryContentMode,
    pub order: StateHistoryOrder,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateHistoryRow {
    pub entity_id: String,
    pub schema_key: String,
    pub file_id: String,
    pub plugin_key: String,
    pub snapshot_content: Option<String>,
    pub metadata: Option<String>,
    pub schema_version: String,
    pub change_id: String,
    pub commit_id: String,
    pub commit_created_at: String,
    pub root_commit_id: String,
    pub depth: i64,
    pub version_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FileHistoryContentMode {
    #[default]
    MetadataOnly,
    IncludeData,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FileHistoryLineageScope {
    #[default]
    ActiveVersion,
    Standard,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum FileHistoryRootScope {
    #[default]
    AllRoots,
    RequestedRoots(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum FileHistoryVersionScope {
    #[default]
    Any,
    RequestedVersions(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FileHistoryRequest {
    pub lineage_scope: FileHistoryLineageScope,
    pub active_version_id: Option<String>,
    pub root_scope: FileHistoryRootScope,
    pub version_scope: FileHistoryVersionScope,
    pub file_ids: Vec<String>,
    pub content_mode: FileHistoryContentMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileHistoryRow {
    pub id: String,
    pub path: Option<String>,
    pub data: Option<Vec<u8>>,
    pub metadata: Option<String>,
    pub hidden: Option<bool>,
    pub lixcol_entity_id: String,
    pub lixcol_schema_key: String,
    pub lixcol_file_id: String,
    pub lixcol_version_id: String,
    pub lixcol_plugin_key: String,
    pub lixcol_schema_version: String,
    pub lixcol_change_id: String,
    pub lixcol_metadata: Option<String>,
    pub lixcol_commit_id: String,
    pub lixcol_commit_created_at: String,
    pub lixcol_root_commit_id: String,
    pub lixcol_depth: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DirectoryHistoryRequest {
    pub lineage_scope: FileHistoryLineageScope,
    pub active_version_id: Option<String>,
    pub root_scope: FileHistoryRootScope,
    pub version_scope: FileHistoryVersionScope,
    pub directory_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryHistoryRow {
    pub id: String,
    pub parent_id: Option<String>,
    pub name: String,
    pub path: Option<String>,
    pub hidden: Option<bool>,
    pub lixcol_entity_id: String,
    pub lixcol_schema_key: String,
    pub lixcol_file_id: String,
    pub lixcol_version_id: String,
    pub lixcol_plugin_key: String,
    pub lixcol_schema_version: String,
    pub lixcol_change_id: String,
    pub lixcol_metadata: Option<String>,
    pub lixcol_commit_id: String,
    pub lixcol_commit_created_at: String,
    pub lixcol_root_commit_id: String,
    pub lixcol_depth: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveStateMode {
    Uninitialized,
    Bootstrapping,
    Ready,
    NeedsRebuild,
    Rebuilding,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveStateProjectionStatus {
    pub mode: LiveStateMode,
    pub applied_cursor: Option<ReplayCursor>,
    pub latest_cursor: Option<ReplayCursor>,
    pub applied_committed_frontier: Option<CommittedVersionFrontier>,
    pub current_committed_frontier: CommittedVersionFrontier,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveSnapshotStorage {
    Tracked,
    Untracked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum LiveFilterField {
    EntityId,
    FileId,
    PluginKey,
    SchemaVersion,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum LiveFilterOp {
    Eq(Value),
    In(Vec<Value>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct LiveFilter {
    pub field: LiveFilterField,
    pub operator: LiveFilterOp,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LiveSnapshotRow {
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: String,
    pub version_id: String,
    pub plugin_key: String,
    pub metadata: Option<String>,
    pub source_change_id: Option<String>,
    pub snapshot: JsonValue,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SchemaRegistration {
    schema_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    registered_snapshot: Option<JsonValue>,
    #[serde(skip, default)]
    source: SchemaRegistrationSource,
}

#[derive(Debug, Clone, Default)]
pub struct SchemaRegistrationSet {
    inner: BTreeMap<String, SchemaRegistration>,
}

impl SchemaRegistrationSet {
    pub fn insert(&mut self, registration: impl Into<SchemaRegistration>) {
        let registration = registration.into();
        self.inner
            .entry(registration.schema_key().to_string())
            .and_modify(|existing| {
                if !existing.has_request_local_layout() && registration.has_request_local_layout() {
                    *existing = registration.clone();
                }
            })
            .or_insert(registration);
    }

    pub fn extend(&mut self, other: SchemaRegistrationSet) {
        for registration in other.inner.into_values() {
            self.insert(registration);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn values(&self) -> impl Iterator<Item = &SchemaRegistration> {
        self.inner.values()
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
enum SchemaRegistrationSource {
    #[default]
    StoredLayout,
    SchemaDefinition(JsonValue),
}

impl From<&str> for SchemaRegistration {
    fn from(schema_key: &str) -> Self {
        Self::new(schema_key)
    }
}

impl From<String> for SchemaRegistration {
    fn from(schema_key: String) -> Self {
        Self::new(schema_key)
    }
}

impl SchemaRegistration {
    pub fn new(schema_key: impl Into<String>) -> Self {
        Self {
            schema_key: schema_key.into(),
            registered_snapshot: None,
            source: SchemaRegistrationSource::StoredLayout,
        }
    }

    pub fn schema_key(&self) -> &str {
        &self.schema_key
    }

    pub fn with_registered_snapshot(
        schema_key: impl Into<String>,
        registered_snapshot: JsonValue,
    ) -> Self {
        Self {
            schema_key: schema_key.into(),
            registered_snapshot: Some(registered_snapshot),
            source: SchemaRegistrationSource::StoredLayout,
        }
    }

    pub fn with_schema_definition(
        schema_key: impl Into<String>,
        schema_definition: JsonValue,
    ) -> Self {
        Self {
            schema_key: schema_key.into(),
            registered_snapshot: None,
            source: SchemaRegistrationSource::SchemaDefinition(schema_definition),
        }
    }

    pub fn registered_snapshot(&self) -> Option<&JsonValue> {
        self.registered_snapshot.as_ref()
    }

    fn has_request_local_layout(&self) -> bool {
        self.schema_definition_override().is_some() || self.registered_snapshot().is_some()
    }

    pub fn schema_definition_override(&self) -> Option<&JsonValue> {
        match &self.source {
            SchemaRegistrationSource::StoredLayout => None,
            SchemaRegistrationSource::SchemaDefinition(schema_definition) => {
                Some(schema_definition)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaLiveTableRequirement {
    pub schema_key: String,
    pub schema_definition: Option<JsonValue>,
}

pub fn is_untracked_live_table(_table_name: &str) -> bool {
    false
}

pub fn coalesce_live_table_requirements(
    requirements: &[SchemaLiveTableRequirement],
) -> Vec<SchemaLiveTableRequirement> {
    let mut by_schema = BTreeMap::<String, SchemaLiveTableRequirement>::new();
    for requirement in requirements {
        by_schema
            .entry(requirement.schema_key.clone())
            .and_modify(|existing| {
                if existing.schema_definition.is_none() && requirement.schema_definition.is_some() {
                    existing.schema_definition = requirement.schema_definition.clone();
                }
            })
            .or_insert_with(|| requirement.clone());
    }
    by_schema.into_values().collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutationOperation {
    Insert,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MutationRow {
    pub operation: MutationOperation,
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: String,
    pub version_id: String,
    pub plugin_key: String,
    pub snapshot_content: Option<JsonValue>,
    pub untracked: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateValidationPlan {
    pub delete: bool,
    pub table: String,
    pub where_clause: Option<Expr>,
    pub snapshot_content: Option<JsonValue>,
    pub snapshot_patch: Option<BTreeMap<String, JsonValue>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateValidationInputRow {
    pub entity_id: String,
    pub file_id: String,
    pub version_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub base_snapshot: JsonValue,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateValidationInput {
    pub plan: UpdateValidationPlan,
    pub rows: Vec<UpdateValidationInputRow>,
}

#[derive(Debug, Clone)]
pub struct PlannedStatementSet {
    pub prepared_statements: Vec<PreparedStatement>,
    pub live_table_requirements: Vec<SchemaLiveTableRequirement>,
    pub mutations: Vec<MutationRow>,
    pub update_validations: Vec<UpdateValidationPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilesystemPayloadChange {
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: String,
    pub version_id: String,
    pub untracked: bool,
    pub plugin_key: String,
    pub snapshot_content: Option<String>,
    pub metadata: Option<String>,
    pub writer_key: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PlanEffects {
    pub state_commit_stream_changes: Vec<StateCommitStreamChange>,
    pub session_delta: SessionStateDelta,
    pub file_cache_refresh_targets: BTreeSet<(String, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultContract {
    Select,
    DmlNoReturning,
    DmlReturning,
    Other,
}

pub fn result_contract_for_statements(statements: &[Statement]) -> ResultContract {
    match statements.last() {
        Some(Statement::Query(_) | Statement::Explain { .. }) => ResultContract::Select,
        Some(Statement::Insert(insert)) => {
            if insert.returning.is_some() {
                ResultContract::DmlReturning
            } else {
                ResultContract::DmlNoReturning
            }
        }
        Some(Statement::Update(update)) => {
            if update.returning.is_some() {
                ResultContract::DmlReturning
            } else {
                ResultContract::DmlNoReturning
            }
        }
        Some(Statement::Delete(delete)) => {
            if delete.returning.is_some() {
                ResultContract::DmlReturning
            } else {
                ResultContract::DmlNoReturning
            }
        }
        Some(_) | None => ResultContract::Other,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    Tracked,
    Untracked,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteLane {
    ActiveVersion,
    SingleVersion(String),
    GlobalAdmin,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpectedHead {
    CurrentHead,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyKey(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitPreconditions {
    pub write_lane: WriteLane,
    pub expected_head: ExpectedHead,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlannedStateRow {
    pub entity_id: String,
    pub schema_key: String,
    pub version_id: Option<String>,
    pub values: BTreeMap<String, Value>,
    pub writer_key: Option<String>,
    pub tombstone: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedFilesystemDescriptor {
    pub directory_id: String,
    pub name: String,
    pub extension: Option<String>,
    pub metadata: Option<String>,
    pub hidden: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedFilesystemFile {
    pub file_id: String,
    pub version_id: String,
    pub untracked: bool,
    pub descriptor: Option<PlannedFilesystemDescriptor>,
    pub metadata_patch: OptionalTextPatch,
    pub data: Option<Vec<u8>>,
    pub deleted: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlannedFilesystemState {
    pub files: BTreeMap<(String, String), PlannedFilesystemFile>,
}

impl PlannedFilesystemState {
    pub fn merge_from(&mut self, next: &Self) {
        self.files.extend(next.files.clone());
    }

    pub fn has_binary_payloads(&self) -> bool {
        self.files.values().any(|file| file.data.is_some())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PlannedRowIdentity {
    pub schema_key: String,
    pub version_id: String,
    pub entity_id: String,
    pub file_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicChange {
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: Option<String>,
    pub file_id: Option<String>,
    pub plugin_key: Option<String>,
    pub snapshot_content: Option<String>,
    pub metadata: Option<String>,
    pub version_id: String,
    pub writer_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticEffect {
    pub effect_key: String,
    pub target: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeBatch {
    pub changes: Vec<PublicChange>,
    pub write_lane: WriteLane,
    pub writer_key: Option<String>,
    pub semantic_effects: Vec<SemanticEffect>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptionalTextPatch {
    Unchanged,
}

impl OptionalTextPatch {
    pub fn apply(&self, current: Option<String>) -> Option<String> {
        current
    }
}

/// Which indexed field a live-state scan constraint applies to.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ScanField {
    EntityId,
    FileId,
    PluginKey,
    SchemaVersion,
}

/// Inclusive or exclusive range bound.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Bound {
    pub value: Value,
    pub inclusive: bool,
}

/// SQL-free structured scan constraint.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ScanConstraint {
    pub field: ScanField,
    pub operator: ScanOperator,
}

/// Structured scan operator aligned with the current planner/storage split.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ScanOperator {
    Eq(Value),
    In(Vec<Value>),
    Range {
        lower: Option<Bound>,
        upper: Option<Bound>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExactRowRequest {
    pub schema_key: String,
    pub version_id: String,
    pub entity_id: String,
    pub file_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub struct BatchRowRequest {
    pub schema_key: String,
    pub version_id: String,
    pub entity_ids: Vec<String>,
    pub file_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub struct ScanRequest {
    pub schema_key: String,
    pub version_id: String,
    #[serde(default)]
    pub constraints: Vec<ScanConstraint>,
    #[serde(default)]
    pub required_columns: Vec<String>,
}

pub fn exact_row_constraints(request: &ExactRowRequest) -> Vec<ScanConstraint> {
    let mut constraints = vec![ScanConstraint {
        field: ScanField::EntityId,
        operator: ScanOperator::Eq(Value::Text(request.entity_id.clone())),
    }];
    if let Some(file_id) = &request.file_id {
        constraints.push(ScanConstraint {
            field: ScanField::FileId,
            operator: ScanOperator::Eq(Value::Text(file_id.clone())),
        });
    }
    constraints
}

#[cfg(test)]
pub fn batch_row_constraints(request: &BatchRowRequest) -> Vec<ScanConstraint> {
    let mut constraints = vec![ScanConstraint {
        field: ScanField::EntityId,
        operator: ScanOperator::In(
            request
                .entity_ids
                .iter()
                .cloned()
                .map(Value::Text)
                .collect(),
        ),
    }];
    if let Some(file_id) = &request.file_id {
        constraints.push(ScanConstraint {
            field: ScanField::FileId,
            operator: ScanOperator::Eq(Value::Text(file_id.clone())),
        });
    }
    constraints
}

#[cfg(test)]
pub fn entity_id_in_constraint<I>(entity_ids: I) -> ScanConstraint
where
    I: IntoIterator<Item = String>,
{
    ScanConstraint {
        field: ScanField::EntityId,
        operator: ScanOperator::In(entity_ids.into_iter().map(Value::Text).collect()),
    }
}

/// Logical live-state row key shared across tracked and untracked lanes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RowIdentity {
    pub schema_key: String,
    pub version_id: String,
    pub entity_id: String,
    pub file_id: String,
}

impl RowIdentity {
    pub fn from_tracked_write(row: &TrackedWriteRow) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            version_id: row.version_id.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }

    pub fn from_untracked_write(row: &UntrackedWriteRow) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            version_id: row.version_id.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }

    pub fn from_tracked_row(row: &TrackedRow) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            version_id: row.version_id.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }

    pub fn from_untracked_row(row: &UntrackedRow) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            version_id: row.version_id.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }

    pub fn from_tombstone(row: &TrackedTombstoneMarker) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            version_id: row.version_id.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }

    pub fn matches_exact(&self, request: &ExactRowRequest) -> bool {
        self.schema_key == request.schema_key
            && self.version_id == request.version_id
            && self.entity_id == request.entity_id
            && request
                .file_id
                .as_ref()
                .is_none_or(|file_id| self.file_id == *file_id)
    }

    pub fn matches_batch(&self, request: &BatchRowRequest) -> bool {
        self.schema_key == request.schema_key
            && self.version_id == request.version_id
            && request.entity_ids.contains(&self.entity_id)
            && request
                .file_id
                .as_ref()
                .is_none_or(|file_id| self.file_id == *file_id)
    }

    pub fn matches_scan_partition(&self, request: &ScanRequest) -> bool {
        self.schema_key == request.schema_key && self.version_id == request.version_id
    }
}

/// Decoded tracked live row.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TrackedRow {
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: String,
    pub version_id: String,
    pub global: bool,
    pub plugin_key: String,
    pub metadata: Option<String>,
    pub change_id: Option<String>,
    pub writer_key: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub values: BTreeMap<String, Value>,
}

impl TrackedRow {
    pub fn property_text(&self, property_name: &str) -> Option<String> {
        self.values
            .get(property_name)
            .and_then(value_as_text)
            .map(ToString::to_string)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TrackedTombstoneMarker {
    pub entity_id: String,
    pub schema_key: String,
    pub file_id: String,
    pub version_id: String,
    pub global: bool,
    pub schema_version: Option<String>,
    pub plugin_key: Option<String>,
    pub metadata: Option<String>,
    pub writer_key: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub change_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TrackedWriteOperation {
    Upsert,
    Tombstone,
}

/// Single tracked live-state write operation.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TrackedWriteRow {
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: String,
    pub version_id: String,
    pub global: bool,
    pub plugin_key: String,
    pub metadata: Option<String>,
    pub change_id: String,
    pub writer_key: Option<String>,
    pub snapshot_content: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: String,
    pub operation: TrackedWriteOperation,
}

/// Decoded untracked/helper live row.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UntrackedRow {
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: String,
    pub version_id: String,
    pub global: bool,
    pub plugin_key: String,
    pub metadata: Option<String>,
    pub writer_key: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub values: BTreeMap<String, Value>,
}

impl UntrackedRow {
    pub fn property_text(&self, property_name: &str) -> Option<String> {
        self.values
            .get(property_name)
            .and_then(value_as_text)
            .map(ToString::to_string)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum UntrackedWriteOperation {
    Upsert,
    Delete,
}

/// Single untracked/helper write operation.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UntrackedWriteRow {
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: String,
    pub version_id: String,
    pub global: bool,
    pub plugin_key: String,
    pub metadata: Option<String>,
    pub writer_key: Option<String>,
    pub snapshot_content: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: String,
    pub operation: UntrackedWriteOperation,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum OverlayLane {
    LocalUntracked,
    LocalTracked,
    GlobalUntracked,
    GlobalTracked,
}

impl OverlayLane {
    pub fn is_global(self) -> bool {
        matches!(self, Self::GlobalTracked | Self::GlobalUntracked)
    }

    pub fn is_untracked(self) -> bool {
        matches!(self, Self::LocalUntracked | Self::GlobalUntracked)
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum LaneResult<T> {
    Found(T),
    Missing,
    Tombstone,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EffectiveRowRequest {
    pub schema_key: String,
    pub version_id: String,
    pub entity_id: String,
    pub file_id: Option<String>,
    pub include_global: bool,
    pub include_untracked: bool,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub struct EffectiveRowsRequest {
    pub schema_key: String,
    pub version_id: String,
    #[serde(default)]
    pub constraints: Vec<ScanConstraint>,
    #[serde(default)]
    pub required_columns: Vec<String>,
    pub include_global: bool,
    pub include_untracked: bool,
    pub include_tombstones: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct EffectiveRowIdentity {
    pub entity_id: String,
    pub file_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum EffectiveRowState {
    Visible,
    Tombstone,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EffectiveRow {
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: Option<String>,
    pub file_id: String,
    pub version_id: String,
    pub source_version_id: String,
    pub global: bool,
    pub untracked: bool,
    pub plugin_key: Option<String>,
    pub metadata: Option<String>,
    pub writer_key: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub source_change_id: Option<String>,
    pub overlay_lane: OverlayLane,
    pub state: EffectiveRowState,
    pub values: BTreeMap<String, Value>,
}

impl EffectiveRow {
    pub fn identity(&self) -> EffectiveRowIdentity {
        EffectiveRowIdentity {
            entity_id: self.entity_id.clone(),
            file_id: self.file_id.clone(),
        }
    }

    pub fn is_tombstone(&self) -> bool {
        matches!(self.state, EffectiveRowState::Tombstone)
    }

    pub fn property_text(&self, property_name: &str) -> Option<String> {
        self.values
            .get(property_name)
            .and_then(value_as_text)
            .map(ToString::to_string)
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub struct EffectiveRowSet {
    pub rows: Vec<EffectiveRow>,
}

#[cfg(test)]
pub fn values_from_snapshot_content(
    snapshot_content: Option<&str>,
) -> Result<BTreeMap<String, Value>, LixError> {
    let Some(snapshot_content) = snapshot_content else {
        return Ok(BTreeMap::new());
    };

    let parsed = serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!("failed to decode transaction snapshot content: {error}"),
        )
    })?;

    let JsonValue::Object(object) = parsed else {
        return Ok(BTreeMap::new());
    };

    Ok(object
        .into_iter()
        .map(|(key, value)| (key, value_from_json(value)))
        .collect())
}

#[cfg(test)]
fn value_from_json(value: JsonValue) -> Value {
    match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(value) => Value::Boolean(value),
        JsonValue::Number(value) => {
            if let Some(value) = value.as_i64() {
                Value::Integer(value)
            } else if let Some(value) = value.as_f64() {
                Value::Real(value)
            } else {
                Value::Null
            }
        }
        JsonValue::String(value) => Value::Text(value),
        JsonValue::Array(value) => Value::Json(JsonValue::Array(value)),
        JsonValue::Object(value) => Value::Json(JsonValue::Object(value)),
    }
}

fn value_as_text(value: &Value) -> Option<&str> {
    match value {
        Value::Text(value) => Some(value.as_str()),
        _ => None,
    }
}

pub fn matches_constraints(
    entity_id: &str,
    file_id: &str,
    plugin_key: &str,
    schema_version: &str,
    constraints: &[ScanConstraint],
) -> bool {
    constraints.iter().all(|constraint| {
        let candidate = match constraint.field {
            ScanField::EntityId => entity_id,
            ScanField::FileId => file_id,
            ScanField::PluginKey => plugin_key,
            ScanField::SchemaVersion => schema_version,
        };
        matches_constraint(candidate, &constraint.operator)
    })
}

fn matches_constraint(candidate: &str, operator: &ScanOperator) -> bool {
    match operator {
        ScanOperator::Eq(value) => value_as_text(value).is_some_and(|value| value == candidate),
        ScanOperator::In(values) => values
            .iter()
            .filter_map(value_as_text)
            .any(|value| value == candidate),
        ScanOperator::Range { lower, upper } => {
            lower
                .as_ref()
                .is_none_or(|bound| compare_lower(candidate, &bound.value, bound.inclusive))
                && upper
                    .as_ref()
                    .is_none_or(|bound| compare_upper(candidate, &bound.value, bound.inclusive))
        }
    }
}

fn compare_lower(candidate: &str, bound: &Value, inclusive: bool) -> bool {
    value_as_text(bound).is_some_and(|value| {
        if inclusive {
            candidate >= value
        } else {
            candidate > value
        }
    })
}

fn compare_upper(candidate: &str, bound: &Value, inclusive: bool) -> bool {
    value_as_text(bound).is_some_and(|value| {
        if inclusive {
            candidate <= value
        } else {
            candidate < value
        }
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        FileHistoryRequest, PreparedBatch, PreparedDirectPublicRead, PreparedExplainMode,
        PreparedFileHistoryDirectReadPlan, PreparedInternalReadArtifact,
        PreparedPublicReadArtifact, PreparedPublicReadContract,
        PreparedPublicReadExecutionArtifact, PreparedReadArtifact, PreparedReadProgram,
        PreparedReadStep, PreparedStatement, ReadDiagnosticCatalogSnapshot, ReadDiagnosticContext,
        ReadTimeProjectionRead, ReadTimeProjectionReadQuery, ReadTimeProjectionSurface,
        ResultContract,
    };
    use crate::catalog::{SurfaceFamily, SurfaceReadFreshness, SurfaceVariant};
    use crate::Value;

    #[test]
    fn read_time_projection_surface_maps_only_real_lix_version_surface() {
        let surface = ReadTimeProjectionSurface::from_public_name("lix_version")
            .expect("lix_version should map to read-time projection surface");

        assert_eq!(surface, ReadTimeProjectionSurface::LixVersion);
        assert_eq!(surface.public_name(), "lix_version");
        assert_eq!(surface.surface_family(), SurfaceFamily::Admin);
        assert_eq!(surface.surface_variant(), SurfaceVariant::Default);
        assert_eq!(
            ReadTimeProjectionSurface::from_public_name("lix_version_by_version"),
            None
        );
    }

    #[test]
    fn read_time_projection_read_keeps_query_shape_runtime_neutral() {
        let artifact = ReadTimeProjectionRead {
            surface: ReadTimeProjectionSurface::LixVersion,
            requested_version_id: None,
            query: ReadTimeProjectionReadQuery {
                projections: vec![super::PendingViewProjection::Column {
                    source_column: "id".into(),
                    output_column: "id".into(),
                }],
                filters: vec![super::PendingViewFilter::Equals(
                    "hidden".into(),
                    Value::Boolean(false),
                )],
                order_by: vec![super::PendingViewOrderClause {
                    column: "name".into(),
                    descending: false,
                }],
                limit: Some(10),
            },
        };

        assert_eq!(artifact.surface.public_name(), "lix_version");
        assert_eq!(artifact.query.projections.len(), 1);
        assert_eq!(artifact.query.filters.len(), 1);
        assert_eq!(artifact.query.order_by.len(), 1);
        assert_eq!(artifact.query.limit, Some(10));
    }

    #[test]
    fn prepared_public_read_artifact_stays_on_contract_dtos() {
        let artifact = PreparedPublicReadArtifact {
            contract: PreparedPublicReadContract {
                committed_mode: super::CommittedReadMode::CommittedOnly,
                pending_view_query: None,
                result_columns: None,
            },
            freshness_contract: SurfaceReadFreshness::AllowsStaleProjection,
            surface_bindings: vec!["lix_version".into()],
            public_output_columns: None,
            execution: PreparedPublicReadExecutionArtifact::ReadTimeProjection(
                ReadTimeProjectionRead {
                    surface: ReadTimeProjectionSurface::LixVersion,
                    requested_version_id: None,
                    query: ReadTimeProjectionReadQuery {
                        projections: vec![super::PendingViewProjection::Column {
                            source_column: "id".into(),
                            output_column: "id".into(),
                        }],
                        filters: vec![],
                        order_by: vec![],
                        limit: None,
                    },
                },
            ),
        };

        match artifact.execution {
            PreparedPublicReadExecutionArtifact::ReadTimeProjection(read) => {
                assert_eq!(read.surface.public_name(), "lix_version");
            }
            _ => panic!("expected read-time projection execution artifact"),
        }
        assert_eq!(
            artifact.contract.execution_mode(),
            super::PublicReadExecutionMode::Committed(super::CommittedReadMode::CommittedOnly)
        );
    }

    #[test]
    fn prepared_internal_read_artifact_keeps_lowered_statements_runtime_neutral() {
        let artifact = PreparedInternalReadArtifact {
            prepared_batch: PreparedBatch {
                steps: vec![PreparedStatement {
                    sql: "SELECT 1".into(),
                    params: vec![],
                }],
            },
            result_contract: ResultContract::Select,
        };

        assert_eq!(artifact.prepared_batch.steps.len(), 1);
        assert_eq!(artifact.prepared_batch.steps[0].sql, "SELECT 1");
        assert_eq!(artifact.result_contract, ResultContract::Select);
    }

    #[test]
    fn read_diagnostic_context_uses_text_and_explain_mode_not_statement_ast() {
        let context = ReadDiagnosticContext {
            source_sql: vec!["SELECT * FROM lix_version".into()],
            relation_names: vec!["lix_version".into()],
            catalog_snapshot: ReadDiagnosticCatalogSnapshot {
                public_surfaces: vec!["lix_version".into()],
                available_tables: vec!["lix_version".into()],
                available_columns_by_relation: BTreeMap::from([(
                    "lix_version".into(),
                    vec!["id".into(), "name".into()],
                )]),
            },
            explain_mode: Some(PreparedExplainMode::Analyze),
            plain_explain_template: None,
            analyzed_explain_template: None,
        };

        assert_eq!(context.source_sql, vec!["SELECT * FROM lix_version"]);
        assert_eq!(context.relation_names, vec!["lix_version"]);
        assert_eq!(
            context.catalog_snapshot.public_surfaces,
            vec!["lix_version"]
        );
        assert_eq!(context.explain_mode, Some(PreparedExplainMode::Analyze));
    }

    #[test]
    fn prepared_read_program_wraps_public_or_internal_artifacts_with_diagnostics() {
        let public_step = PreparedReadStep {
            transaction_mode: crate::TransactionMode::Deferred,
            artifact: PreparedReadArtifact::Public(PreparedPublicReadArtifact {
                contract: PreparedPublicReadContract {
                    committed_mode: super::CommittedReadMode::MaterializedState,
                    pending_view_query: None,
                    result_columns: None,
                },
                freshness_contract: SurfaceReadFreshness::RequiresFreshProjection,
                surface_bindings: vec!["lix_file".into()],
                public_output_columns: None,
                execution: PreparedPublicReadExecutionArtifact::Direct(
                    PreparedDirectPublicRead::FileHistory(PreparedFileHistoryDirectReadPlan {
                        request: FileHistoryRequest::default(),
                        predicates: Vec::new(),
                        projections: Vec::new(),
                        wildcard_projection: true,
                        wildcard_columns: vec!["id".into()],
                        sort_keys: Vec::new(),
                        limit: None,
                        offset: 0,
                        aggregate: None,
                        aggregate_output_name: None,
                    }),
                ),
            }),
            diagnostic_context: ReadDiagnosticContext {
                source_sql: vec!["SELECT * FROM lix_file_history".into()],
                relation_names: vec!["lix_file_history".into()],
                catalog_snapshot: ReadDiagnosticCatalogSnapshot {
                    public_surfaces: vec!["lix_file_history".into()],
                    available_tables: vec!["lix_file_history".into()],
                    available_columns_by_relation: BTreeMap::new(),
                },
                explain_mode: Some(PreparedExplainMode::Plain),
                plain_explain_template: None,
                analyzed_explain_template: None,
            },
        };
        let internal_step = PreparedReadStep {
            transaction_mode: crate::TransactionMode::Read,
            artifact: PreparedReadArtifact::Internal(PreparedInternalReadArtifact {
                prepared_batch: PreparedBatch {
                    steps: vec![PreparedStatement {
                        sql: "SELECT 1".into(),
                        params: vec![],
                    }],
                },
                result_contract: ResultContract::Select,
            }),
            diagnostic_context: ReadDiagnosticContext {
                source_sql: vec!["SELECT 1".into()],
                relation_names: Vec::new(),
                catalog_snapshot: ReadDiagnosticCatalogSnapshot::default(),
                explain_mode: None,
                plain_explain_template: None,
                analyzed_explain_template: None,
            },
        };
        let program = PreparedReadProgram {
            transaction_mode: crate::TransactionMode::Deferred,
            steps: vec![public_step, internal_step],
        };

        assert_eq!(program.steps.len(), 2);
        assert_eq!(program.transaction_mode, crate::TransactionMode::Deferred);
        match &program.steps[0].artifact {
            PreparedReadArtifact::Public(public) => match &public.execution {
                PreparedPublicReadExecutionArtifact::Direct(
                    PreparedDirectPublicRead::FileHistory(_),
                ) => {
                    assert_eq!(public.surface_bindings, vec!["lix_file".to_string()]);
                }
                _ => panic!("expected direct public read artifact"),
            },
            _ => panic!("expected public read step"),
        }
        match &program.steps[1].artifact {
            PreparedReadArtifact::Internal(internal) => {
                assert_eq!(internal.prepared_batch.steps[0].sql, "SELECT 1");
            }
            _ => panic!("expected internal read step"),
        }
    }
}
