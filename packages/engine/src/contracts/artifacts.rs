use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlparser::ast::{Expr, Statement};

use crate::contracts::surface::{
    SurfaceBinding, SurfaceFamily, SurfaceReadFreshness, SurfaceVariant,
};
use crate::contracts::ReplayCursor;
use crate::common::error::LixError;
use crate::transaction_mode::TransactionMode;
use crate::common::types::Value;

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

    pub(crate) fn to_json_string(&self) -> String {
        serde_json::to_string(self).expect("committed frontier serialization should succeed")
    }

    pub(crate) fn from_json_str(value: &str) -> Result<Self, LixError> {
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
pub(crate) enum SessionExecutionMode {
    CommittedRead,
    CommittedRuntimeMutation,
    WriteTransaction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FilesystemProjectionScope {
    ActiveVersion,
    ExplicitVersion,
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
pub(crate) enum EffectiveStateVersionScope {
    ActiveVersion,
    ExplicitVersion,
    History,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EffectiveStateRequest {
    pub(crate) schema_set: BTreeSet<String>,
    pub(crate) version_scope: EffectiveStateVersionScope,
    pub(crate) include_global_overlay: bool,
    pub(crate) include_untracked_overlay: bool,
    pub(crate) include_tombstones: bool,
    pub(crate) predicate_classes: Vec<String>,
    pub(crate) required_columns: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommittedReadMode {
    CommittedOnly,
    MaterializedState,
}

impl CommittedReadMode {
    pub(crate) fn transaction_mode(self) -> TransactionMode {
        match self {
            Self::CommittedOnly => TransactionMode::Read,
            Self::MaterializedState => TransactionMode::Deferred,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublicReadExecutionMode {
    PendingView,
    Committed(CommittedReadMode),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum PendingViewReadStorage {
    Tracked,
    Untracked,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PendingViewReadQuery {
    pub(crate) storage: PendingViewReadStorage,
    pub(crate) schema_key: String,
    pub(crate) version_id: String,
    pub(crate) projections: Vec<PendingViewProjection>,
    pub(crate) filters: Vec<PendingViewFilter>,
    pub(crate) order_by: Vec<PendingViewOrderClause>,
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PendingViewProjection {
    Column {
        source_column: String,
        output_column: String,
    },
    CountAll {
        output_column: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PendingViewFilter {
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
pub(crate) struct PendingViewOrderClause {
    pub(crate) column: String,
    pub(crate) descending: bool,
}

/// Public surface family currently served from `ReadTime` projection output.
///
/// Phase B of Plan 33 resolves the current version-surface ambiguity
/// explicitly: the first projection-backed public serving cut targets the real
/// builtin `lix_version` surface, not a stale `lix_version_by_version` alias.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum ReadTimeProjectionSurface {
    LixVersion,
}

#[allow(dead_code)]
impl ReadTimeProjectionSurface {
    pub(crate) fn from_public_name(public_name: &str) -> Option<Self> {
        match public_name {
            "lix_version" => Some(Self::LixVersion),
            _ => None,
        }
    }

    pub(crate) fn public_name(self) -> &'static str {
        match self {
            Self::LixVersion => "lix_version",
        }
    }

    pub(crate) fn surface_family(self) -> SurfaceFamily {
        match self {
            Self::LixVersion => SurfaceFamily::Admin,
        }
    }

    pub(crate) fn surface_variant(self) -> SurfaceVariant {
        match self {
            Self::LixVersion => SurfaceVariant::Default,
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
pub(crate) struct ReadTimeProjectionReadQuery {
    pub(crate) projections: Vec<PendingViewProjection>,
    pub(crate) filters: Vec<PendingViewFilter>,
    pub(crate) order_by: Vec<PendingViewOrderClause>,
    pub(crate) limit: Option<usize>,
}

/// Compiler-owned artifact for a public read that should be served from
/// `ReadTime` projection output.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct ReadTimeProjectionRead {
    pub(crate) surface: ReadTimeProjectionSurface,
    pub(crate) query: ReadTimeProjectionReadQuery,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum PreparedDirectPublicReadKind {
    StateHistory,
    EntityHistory,
    FileHistory,
    DirectoryHistory,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) enum PreparedDirectStateHistoryField {
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
pub(crate) enum PreparedStateHistoryAggregate {
    Count,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) enum PreparedStateHistoryProjectionValue {
    Field(PreparedDirectStateHistoryField),
    Aggregate(PreparedStateHistoryAggregate),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct PreparedStateHistoryProjection {
    pub(crate) output_name: String,
    pub(crate) value: PreparedStateHistoryProjectionValue,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) enum PreparedStateHistorySortValue {
    Field(PreparedDirectStateHistoryField),
    Aggregate(PreparedStateHistoryAggregate),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct PreparedStateHistorySortKey {
    pub(crate) output_name: String,
    pub(crate) value: Option<PreparedStateHistorySortValue>,
    pub(crate) descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) enum PreparedStateHistoryPredicate {
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
pub(crate) enum PreparedStateHistoryAggregatePredicate {
    Eq(PreparedStateHistoryAggregate, i64),
    NotEq(PreparedStateHistoryAggregate, i64),
    Gt(PreparedStateHistoryAggregate, i64),
    GtEq(PreparedStateHistoryAggregate, i64),
    Lt(PreparedStateHistoryAggregate, i64),
    LtEq(PreparedStateHistoryAggregate, i64),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct PreparedStateHistoryDirectReadPlan {
    pub(crate) request: StateHistoryRequest,
    pub(crate) predicates: Vec<PreparedStateHistoryPredicate>,
    pub(crate) projections: Vec<PreparedStateHistoryProjection>,
    pub(crate) wildcard_projection: bool,
    pub(crate) wildcard_columns: Vec<String>,
    pub(crate) group_by_fields: Vec<PreparedDirectStateHistoryField>,
    pub(crate) having: Option<PreparedStateHistoryAggregatePredicate>,
    pub(crate) sort_keys: Vec<PreparedStateHistorySortKey>,
    pub(crate) limit: Option<u64>,
    pub(crate) offset: u64,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) enum PreparedDirectEntityHistoryField {
    Property(String),
    State(PreparedDirectStateHistoryField),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct PreparedEntityHistoryProjection {
    pub(crate) output_name: String,
    pub(crate) field: PreparedDirectEntityHistoryField,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct PreparedEntityHistorySortKey {
    pub(crate) output_name: String,
    pub(crate) field: Option<PreparedDirectEntityHistoryField>,
    pub(crate) descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) enum PreparedEntityHistoryPredicate {
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
pub(crate) struct PreparedEntityHistoryDirectReadPlan {
    pub(crate) surface_binding: SurfaceBinding,
    pub(crate) request: StateHistoryRequest,
    pub(crate) predicates: Vec<PreparedEntityHistoryPredicate>,
    pub(crate) projections: Vec<PreparedEntityHistoryProjection>,
    pub(crate) wildcard_projection: bool,
    pub(crate) wildcard_columns: Vec<String>,
    pub(crate) sort_keys: Vec<PreparedEntityHistorySortKey>,
    pub(crate) limit: Option<u64>,
    pub(crate) offset: u64,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) enum PreparedDirectFileHistoryField {
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
pub(crate) struct PreparedFileHistoryProjection {
    pub(crate) output_name: String,
    pub(crate) field: PreparedDirectFileHistoryField,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct PreparedFileHistorySortKey {
    pub(crate) output_name: String,
    pub(crate) field: Option<PreparedDirectFileHistoryField>,
    pub(crate) descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) enum PreparedFileHistoryPredicate {
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
pub(crate) enum PreparedFileHistoryAggregate {
    Count,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct PreparedFileHistoryDirectReadPlan {
    pub(crate) request: FileHistoryRequest,
    pub(crate) predicates: Vec<PreparedFileHistoryPredicate>,
    pub(crate) projections: Vec<PreparedFileHistoryProjection>,
    pub(crate) wildcard_projection: bool,
    pub(crate) wildcard_columns: Vec<String>,
    pub(crate) sort_keys: Vec<PreparedFileHistorySortKey>,
    pub(crate) limit: Option<u64>,
    pub(crate) offset: u64,
    pub(crate) aggregate: Option<PreparedFileHistoryAggregate>,
    pub(crate) aggregate_output_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) enum PreparedDirectDirectoryHistoryField {
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
pub(crate) struct PreparedDirectoryHistoryProjection {
    pub(crate) output_name: String,
    pub(crate) field: PreparedDirectDirectoryHistoryField,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct PreparedDirectoryHistorySortKey {
    pub(crate) output_name: String,
    pub(crate) field: Option<PreparedDirectDirectoryHistoryField>,
    pub(crate) descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) enum PreparedDirectoryHistoryPredicate {
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
pub(crate) enum PreparedDirectoryHistoryAggregate {
    Count,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct PreparedDirectoryHistoryDirectReadPlan {
    pub(crate) request: DirectoryHistoryRequest,
    pub(crate) predicates: Vec<PreparedDirectoryHistoryPredicate>,
    pub(crate) projections: Vec<PreparedDirectoryHistoryProjection>,
    pub(crate) wildcard_projection: bool,
    pub(crate) wildcard_columns: Vec<String>,
    pub(crate) sort_keys: Vec<PreparedDirectoryHistorySortKey>,
    pub(crate) limit: Option<u64>,
    pub(crate) offset: u64,
    pub(crate) aggregate: Option<PreparedDirectoryHistoryAggregate>,
    pub(crate) aggregate_output_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) enum PreparedDirectPublicRead {
    StateHistory(PreparedStateHistoryDirectReadPlan),
    EntityHistory(PreparedEntityHistoryDirectReadPlan),
    FileHistory(PreparedFileHistoryDirectReadPlan),
    DirectoryHistory(PreparedDirectoryHistoryDirectReadPlan),
}

#[allow(dead_code)]
impl PreparedDirectPublicRead {
    pub(crate) fn kind(&self) -> PreparedDirectPublicReadKind {
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
pub(crate) enum PreparedPublicReadExecutionArtifact {
    ReadTimeProjection(ReadTimeProjectionRead),
    LoweredSql(PreparedBatch),
    Direct(PreparedDirectPublicRead),
}

/// Runtime-neutral prepared public-read package.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct PreparedPublicReadArtifact {
    pub(crate) contract: PreparedPublicReadContract,
    pub(crate) freshness_contract: SurfaceReadFreshness,
    pub(crate) surface_bindings: Vec<String>,
    pub(crate) public_output_columns: Option<Vec<String>>,
    pub(crate) execution: PreparedPublicReadExecutionArtifact,
}

/// Runtime-neutral prepared internal-read package.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct PreparedInternalReadArtifact {
    pub(crate) prepared_batch: PreparedBatch,
    pub(crate) result_contract: ResultContract,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum PreparedExplainMode {
    Plain,
    Analyze,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[allow(dead_code)]
pub(crate) struct PreparedAnalyzedRuntime {
    pub(crate) execution_duration_us: u64,
    pub(crate) output_row_count: usize,
    pub(crate) output_column_count: usize,
    #[serde(default)]
    pub(crate) output_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[allow(dead_code)]
pub(crate) enum PreparedExplainTemplate {
    Text { sections: Vec<(String, String)> },
    Json { base_json: JsonValue },
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[allow(dead_code)]
pub(crate) struct ReadDiagnosticCatalogSnapshot {
    pub(crate) public_surfaces: Vec<String>,
    pub(crate) available_tables: Vec<String>,
    pub(crate) available_columns_by_relation: BTreeMap<String, Vec<String>>,
}

/// Diagnostic context handed to read runtime alongside a prepared read step.
///
/// The context is intentionally text-shaped so runtime can report/normalize
/// errors and route explain behavior without importing parser/executor-private
/// statement types.
#[derive(Debug, Clone, PartialEq, Default)]
#[allow(dead_code)]
pub(crate) struct ReadDiagnosticContext {
    pub(crate) source_sql: Vec<String>,
    pub(crate) relation_names: Vec<String>,
    pub(crate) catalog_snapshot: ReadDiagnosticCatalogSnapshot,
    pub(crate) explain_mode: Option<PreparedExplainMode>,
    pub(crate) plain_explain_template: Option<PreparedExplainTemplate>,
    pub(crate) analyzed_explain_template: Option<PreparedExplainTemplate>,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) enum PreparedReadArtifact {
    Public(PreparedPublicReadArtifact),
    Internal(PreparedInternalReadArtifact),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct PreparedReadStep {
    pub(crate) transaction_mode: TransactionMode,
    pub(crate) artifact: PreparedReadArtifact,
    pub(crate) diagnostic_context: ReadDiagnosticContext,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct PreparedReadProgram {
    pub(crate) transaction_mode: TransactionMode,
    pub(crate) steps: Vec<PreparedReadStep>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum PreparedWriteStatementKind {
    Query,
    Explain,
    Other,
}

#[allow(dead_code)]
impl PreparedWriteStatementKind {
    pub(crate) fn for_statement(statement: &Statement) -> Self {
        match statement {
            Statement::Query(_) => Self::Query,
            Statement::Explain { .. } => Self::Explain,
            _ => Self::Other,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum PreparedWriteOperationKind {
    Insert,
    Update,
    Delete,
}

#[allow(dead_code)]
impl PreparedWriteOperationKind {
    pub(crate) fn state_commit_stream_operation(self) -> StateCommitStreamOperation {
        match self {
            Self::Insert => StateCommitStreamOperation::Insert,
            Self::Update => StateCommitStreamOperation::Update,
            Self::Delete => StateCommitStreamOperation::Delete,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum PreparedInsertOnConflictAction {
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
pub(crate) struct PreparedWriteDiagnosticContext {
    pub(crate) relation_names: Vec<String>,
    pub(crate) explain_mode: Option<PreparedExplainMode>,
    pub(crate) plain_explain_template: Option<PreparedExplainTemplate>,
    pub(crate) analyzed_explain_template: Option<PreparedExplainTemplate>,
}

#[allow(dead_code)]
impl PreparedWriteDiagnosticContext {
    pub(crate) fn new(relation_names: Vec<String>) -> Self {
        Self {
            relation_names,
            explain_mode: None,
            plain_explain_template: None,
            analyzed_explain_template: None,
        }
    }

    pub(crate) fn relation_names(&self) -> &[String] {
        &self.relation_names
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) enum PreparedPublicSurfaceRegistryMutation {
    UpsertRegisteredSchemaSnapshot { snapshot: JsonValue },
    RemoveDynamicSchema { schema_key: String },
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) enum PreparedPublicSurfaceRegistryEffect {
    None,
    ApplyMutations(Vec<PreparedPublicSurfaceRegistryMutation>),
    ReloadFromStorage,
}

#[allow(dead_code)]
impl PreparedPublicSurfaceRegistryEffect {
    pub(crate) fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct PreparedResolvedWritePartition {
    pub(crate) execution_mode: WriteMode,
    pub(crate) authoritative_pre_state_rows: Vec<PlannedStateRow>,
    pub(crate) intended_post_state: Vec<PlannedStateRow>,
    pub(crate) workspace_writer_key_updates: BTreeMap<PlannedRowIdentity, Option<String>>,
    pub(crate) filesystem_state: PlannedFilesystemState,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct PreparedResolvedWritePlan {
    pub(crate) partitions: Vec<PreparedResolvedWritePartition>,
}

#[allow(dead_code)]
impl PreparedResolvedWritePlan {
    pub(crate) fn authoritative_pre_state_rows(&self) -> impl Iterator<Item = &PlannedStateRow> {
        self.partitions
            .iter()
            .flat_map(|partition| partition.authoritative_pre_state_rows.iter())
    }

    pub(crate) fn intended_post_state(&self) -> impl Iterator<Item = &PlannedStateRow> {
        self.partitions
            .iter()
            .flat_map(|partition| partition.intended_post_state.iter())
    }

    pub(crate) fn filesystem_state(&self) -> PlannedFilesystemState {
        let mut merged = PlannedFilesystemState::default();
        for partition in &self.partitions {
            merged.merge_from(&partition.filesystem_state);
        }
        merged
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct PreparedPublicWriteContract {
    pub(crate) operation_kind: PreparedWriteOperationKind,
    pub(crate) target: SurfaceBinding,
    pub(crate) on_conflict_action: Option<PreparedInsertOnConflictAction>,
    pub(crate) requested_version_id: Option<String>,
    pub(crate) active_account_ids: Vec<String>,
    pub(crate) writer_key: Option<String>,
    pub(crate) resolved_write_plan: Option<PreparedResolvedWritePlan>,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct PreparedTrackedWriteExecution {
    pub(crate) schema_live_table_requirements: Vec<SchemaLiveTableRequirement>,
    pub(crate) domain_change_batch: Option<DomainChangeBatch>,
    pub(crate) create_preconditions: CommitPreconditions,
    pub(crate) semantic_effects: PlanEffects,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct PreparedUntrackedWriteExecution {
    pub(crate) intended_post_state: Vec<PlannedStateRow>,
    pub(crate) semantic_effects: PlanEffects,
    pub(crate) persist_filesystem_payloads_before_write: bool,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) enum PreparedPublicWriteExecutionPartition {
    Tracked(PreparedTrackedWriteExecution),
    Untracked(PreparedUntrackedWriteExecution),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct PreparedPublicWriteMaterialization {
    pub(crate) partitions: Vec<PreparedPublicWriteExecutionPartition>,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) enum PreparedPublicWriteExecutionArtifact {
    Noop,
    Materialize(PreparedPublicWriteMaterialization),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct PreparedPublicWriteArtifact {
    pub(crate) contract: PreparedPublicWriteContract,
    pub(crate) execution: PreparedPublicWriteExecutionArtifact,
}

#[allow(dead_code)]
impl PreparedPublicWriteArtifact {
    pub(crate) fn materialization(&self) -> Option<&PreparedPublicWriteMaterialization> {
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
pub(crate) struct PreparedInternalWriteArtifact {
    pub(crate) prepared_batch: PreparedBatch,
    pub(crate) live_table_requirements: Vec<SchemaLiveTableRequirement>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) has_update_validations: bool,
    pub(crate) should_refresh_file_cache: bool,
    pub(crate) read_only_query: bool,
    pub(crate) filesystem_state: PlannedFilesystemState,
    pub(crate) effects: PlanEffects,
    pub(crate) writer_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) enum PreparedWriteArtifact {
    PublicRead(PreparedPublicReadArtifact),
    PublicWrite(PreparedPublicWriteArtifact),
    Internal(PreparedInternalWriteArtifact),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct PreparedWriteStep {
    pub(crate) statement_kind: PreparedWriteStatementKind,
    pub(crate) result_contract: ResultContract,
    pub(crate) artifact: PreparedWriteArtifact,
    pub(crate) diagnostic_context: PreparedWriteDiagnosticContext,
    pub(crate) public_surface_registry_effect: PreparedPublicSurfaceRegistryEffect,
}

#[allow(dead_code)]
impl PreparedWriteStep {
    pub(crate) fn public_read(&self) -> Option<&PreparedPublicReadArtifact> {
        match &self.artifact {
            PreparedWriteArtifact::PublicRead(read) => Some(read),
            PreparedWriteArtifact::PublicWrite(_) | PreparedWriteArtifact::Internal(_) => None,
        }
    }

    pub(crate) fn public_write(&self) -> Option<&PreparedPublicWriteArtifact> {
        match &self.artifact {
            PreparedWriteArtifact::PublicWrite(write) => Some(write),
            PreparedWriteArtifact::PublicRead(_) | PreparedWriteArtifact::Internal(_) => None,
        }
    }

    pub(crate) fn internal_write(&self) -> Option<&PreparedInternalWriteArtifact> {
        match &self.artifact {
            PreparedWriteArtifact::Internal(internal) => Some(internal),
            PreparedWriteArtifact::PublicRead(_) | PreparedWriteArtifact::PublicWrite(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct PreparedWriteProgram {
    pub(crate) steps: Vec<PreparedWriteStep>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublicReadResultColumn {
    Untyped,
    Boolean,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PublicReadResultColumns {
    Static(Vec<PublicReadResultColumn>),
    ByColumnName(BTreeMap<String, PublicReadResultColumn>),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedPublicReadContract {
    pub(crate) committed_mode: CommittedReadMode,
    pub(crate) pending_view_query: Option<PendingViewReadQuery>,
    pub(crate) result_columns: Option<PublicReadResultColumns>,
}

impl PreparedPublicReadContract {
    pub(crate) fn execution_mode(&self) -> PublicReadExecutionMode {
        if self.pending_view_query.is_some() {
            PublicReadExecutionMode::PendingView
        } else {
            PublicReadExecutionMode::Committed(self.committed_mode)
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum StateHistoryContentMode {
    MetadataOnly,
    #[default]
    IncludeSnapshotContent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum StateHistoryOrder {
    #[default]
    EntityFileSchemaDepthAsc,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) enum StateHistoryRootScope {
    #[default]
    AllRoots,
    RequestedRoots(Vec<String>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum StateHistoryLineageScope {
    #[default]
    Standard,
    ActiveVersion,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) enum StateHistoryVersionScope {
    #[default]
    Any,
    RequestedVersions(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct StateHistoryRequest {
    pub(crate) root_scope: StateHistoryRootScope,
    pub(crate) lineage_scope: StateHistoryLineageScope,
    pub(crate) active_version_id: Option<String>,
    pub(crate) version_scope: StateHistoryVersionScope,
    pub(crate) entity_ids: Vec<String>,
    pub(crate) file_ids: Vec<String>,
    pub(crate) schema_keys: Vec<String>,
    pub(crate) plugin_keys: Vec<String>,
    pub(crate) min_depth: Option<i64>,
    pub(crate) max_depth: Option<i64>,
    pub(crate) content_mode: StateHistoryContentMode,
    pub(crate) order: StateHistoryOrder,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StateHistoryRow {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) file_id: String,
    pub(crate) plugin_key: String,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) schema_version: String,
    pub(crate) change_id: String,
    pub(crate) commit_id: String,
    pub(crate) commit_created_at: String,
    pub(crate) root_commit_id: String,
    pub(crate) depth: i64,
    pub(crate) version_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum FileHistoryContentMode {
    #[default]
    MetadataOnly,
    IncludeData,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum FileHistoryLineageScope {
    #[default]
    ActiveVersion,
    Standard,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) enum FileHistoryRootScope {
    #[default]
    AllRoots,
    RequestedRoots(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) enum FileHistoryVersionScope {
    #[default]
    Any,
    RequestedVersions(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct FileHistoryRequest {
    pub(crate) lineage_scope: FileHistoryLineageScope,
    pub(crate) active_version_id: Option<String>,
    pub(crate) root_scope: FileHistoryRootScope,
    pub(crate) version_scope: FileHistoryVersionScope,
    pub(crate) file_ids: Vec<String>,
    pub(crate) content_mode: FileHistoryContentMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileHistoryRow {
    pub(crate) id: String,
    pub(crate) path: Option<String>,
    pub(crate) data: Option<Vec<u8>>,
    pub(crate) metadata: Option<String>,
    pub(crate) hidden: Option<bool>,
    pub(crate) lixcol_entity_id: String,
    pub(crate) lixcol_schema_key: String,
    pub(crate) lixcol_file_id: String,
    pub(crate) lixcol_version_id: String,
    pub(crate) lixcol_plugin_key: String,
    pub(crate) lixcol_schema_version: String,
    pub(crate) lixcol_change_id: String,
    pub(crate) lixcol_metadata: Option<String>,
    pub(crate) lixcol_commit_id: String,
    pub(crate) lixcol_commit_created_at: String,
    pub(crate) lixcol_root_commit_id: String,
    pub(crate) lixcol_depth: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct DirectoryHistoryRequest {
    pub(crate) lineage_scope: FileHistoryLineageScope,
    pub(crate) active_version_id: Option<String>,
    pub(crate) root_scope: FileHistoryRootScope,
    pub(crate) version_scope: FileHistoryVersionScope,
    pub(crate) directory_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectoryHistoryRow {
    pub(crate) id: String,
    pub(crate) parent_id: Option<String>,
    pub(crate) name: String,
    pub(crate) path: Option<String>,
    pub(crate) hidden: Option<bool>,
    pub(crate) lixcol_entity_id: String,
    pub(crate) lixcol_schema_key: String,
    pub(crate) lixcol_file_id: String,
    pub(crate) lixcol_version_id: String,
    pub(crate) lixcol_plugin_key: String,
    pub(crate) lixcol_schema_version: String,
    pub(crate) lixcol_change_id: String,
    pub(crate) lixcol_metadata: Option<String>,
    pub(crate) lixcol_commit_id: String,
    pub(crate) lixcol_commit_created_at: String,
    pub(crate) lixcol_root_commit_id: String,
    pub(crate) lixcol_depth: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveStateMode {
    Uninitialized,
    Bootstrapping,
    Ready,
    NeedsRebuild,
    Rebuilding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiveQueryOverlayLane {
    LocalUntracked,
    LocalTracked,
    GlobalUntracked,
    GlobalTracked,
}

impl LiveQueryOverlayLane {
    pub(crate) fn is_global(self) -> bool {
        matches!(self, Self::GlobalTracked | Self::GlobalUntracked)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveStateProjectionStatus {
    pub(crate) mode: LiveStateMode,
    pub(crate) applied_cursor: Option<ReplayCursor>,
    pub(crate) latest_cursor: Option<ReplayCursor>,
    pub(crate) applied_committed_frontier: Option<CommittedVersionFrontier>,
    pub(crate) current_committed_frontier: CommittedVersionFrontier,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiveSnapshotStorage {
    Tracked,
    Untracked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum LiveFilterField {
    EntityId,
    FileId,
    PluginKey,
    SchemaVersion,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) enum LiveFilterOp {
    Eq(Value),
    In(Vec<Value>),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LiveFilter {
    pub(crate) field: LiveFilterField,
    pub(crate) operator: LiveFilterOp,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LiveSnapshotRow {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) plugin_key: String,
    pub(crate) metadata: Option<String>,
    pub(crate) source_change_id: Option<String>,
    pub(crate) snapshot: JsonValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExactUntrackedLookupRequest {
    pub(crate) schema_key: String,
    pub(crate) version_id: String,
    pub(crate) entity_id: String,
    pub(crate) file_id: Option<String>,
    pub(crate) plugin_key: Option<String>,
    pub(crate) schema_version: Option<String>,
    pub(crate) writer_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedTombstoneLookupRequest {
    pub(crate) schema_key: String,
    pub(crate) version_id: String,
    pub(crate) entity_id: String,
    pub(crate) file_id: Option<String>,
    pub(crate) plugin_key: Option<String>,
    pub(crate) schema_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LiveQueryEffectiveRow {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) schema_version: Option<String>,
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) source_version_id: String,
    pub(crate) global: bool,
    pub(crate) untracked: bool,
    pub(crate) plugin_key: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) writer_key: Option<String>,
    pub(crate) created_at: Option<String>,
    pub(crate) updated_at: Option<String>,
    pub(crate) source_change_id: Option<String>,
    pub(crate) values: BTreeMap<String, Value>,
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
pub(crate) struct SchemaRegistrationSet {
    inner: BTreeMap<String, SchemaRegistration>,
}

impl SchemaRegistrationSet {
    pub(crate) fn insert(&mut self, registration: impl Into<SchemaRegistration>) {
        let registration = registration.into();
        self.inner
            .insert(registration.schema_key().to_string(), registration);
    }

    pub(crate) fn extend(&mut self, other: SchemaRegistrationSet) {
        self.inner.extend(other.inner);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &SchemaRegistration> {
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

    pub(crate) fn with_schema_definition(
        schema_key: impl Into<String>,
        schema_definition: JsonValue,
    ) -> Self {
        Self {
            schema_key: schema_key.into(),
            registered_snapshot: None,
            source: SchemaRegistrationSource::SchemaDefinition(schema_definition),
        }
    }

    pub(crate) fn registered_snapshot(&self) -> Option<&JsonValue> {
        self.registered_snapshot.as_ref()
    }

    pub(crate) fn schema_definition_override(&self) -> Option<&JsonValue> {
        match &self.source {
            SchemaRegistrationSource::StoredLayout => None,
            SchemaRegistrationSource::SchemaDefinition(schema_definition) => {
                Some(schema_definition)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaLiveTableRequirement {
    pub(crate) schema_key: String,
    pub(crate) schema_definition: Option<JsonValue>,
}

pub(crate) fn is_untracked_live_table(_table_name: &str) -> bool {
    false
}

pub(crate) fn coalesce_live_table_requirements(
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
pub(crate) enum MutationOperation {
    Insert,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MutationRow {
    pub(crate) operation: MutationOperation,
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) plugin_key: String,
    pub(crate) snapshot_content: Option<JsonValue>,
    pub(crate) untracked: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct UpdateValidationPlan {
    pub(crate) delete: bool,
    pub(crate) table: String,
    pub(crate) where_clause: Option<Expr>,
    pub(crate) snapshot_content: Option<JsonValue>,
    pub(crate) snapshot_patch: Option<BTreeMap<String, JsonValue>>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct UpdateValidationInputRow {
    pub(crate) entity_id: String,
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
    pub(crate) base_snapshot: JsonValue,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct UpdateValidationInput {
    pub(crate) plan: UpdateValidationPlan,
    pub(crate) rows: Vec<UpdateValidationInputRow>,
}

#[derive(Debug, Clone)]
pub(crate) struct PlannedStatementSet {
    pub(crate) prepared_statements: Vec<PreparedStatement>,
    pub(crate) live_table_requirements: Vec<SchemaLiveTableRequirement>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FilesystemPayloadDomainChange {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) untracked: bool,
    pub(crate) plugin_key: String,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) writer_key: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct PlanEffects {
    pub(crate) state_commit_stream_changes: Vec<StateCommitStreamChange>,
    pub(crate) session_delta: SessionStateDelta,
    pub(crate) file_cache_refresh_targets: BTreeSet<(String, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResultContract {
    Select,
    DmlNoReturning,
    DmlReturning,
    Other,
}

pub(crate) fn result_contract_for_statements(statements: &[Statement]) -> ResultContract {
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
pub(crate) enum WriteMode {
    Tracked,
    Untracked,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WriteLane {
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
pub(crate) struct PlannedStateRow {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) version_id: Option<String>,
    pub(crate) values: BTreeMap<String, Value>,
    pub(crate) writer_key: Option<String>,
    pub(crate) tombstone: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedFilesystemDescriptor {
    pub(crate) directory_id: String,
    pub(crate) name: String,
    pub(crate) extension: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) hidden: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedFilesystemFile {
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) untracked: bool,
    pub(crate) descriptor: Option<PlannedFilesystemDescriptor>,
    pub(crate) metadata_patch: OptionalTextPatch,
    pub(crate) data: Option<Vec<u8>>,
    pub(crate) deleted: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PlannedFilesystemState {
    pub(crate) files: BTreeMap<(String, String), PlannedFilesystemFile>,
}

impl PlannedFilesystemState {
    pub(crate) fn merge_from(&mut self, next: &Self) {
        self.files.extend(next.files.clone());
    }

    pub(crate) fn has_binary_payloads(&self) -> bool {
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
pub struct PublicDomainChange {
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
pub struct DomainChangeBatch {
    pub changes: Vec<PublicDomainChange>,
    pub write_lane: WriteLane,
    pub writer_key: Option<String>,
    pub semantic_effects: Vec<SemanticEffect>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OptionalTextPatch {
    Unchanged,
}

impl OptionalTextPatch {
    pub(crate) fn apply(&self, current: Option<String>) -> Option<String> {
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

pub(crate) fn exact_row_constraints(request: &ExactRowRequest) -> Vec<ScanConstraint> {
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

pub(crate) fn batch_row_constraints(request: &BatchRowRequest) -> Vec<ScanConstraint> {
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

pub(crate) fn entity_id_in_constraint<I>(entity_ids: I) -> ScanConstraint
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

pub type TrackedWriteBatch = Vec<TrackedWriteRow>;

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

pub type UntrackedWriteBatch = Vec<UntrackedWriteRow>;

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

pub(crate) fn values_from_snapshot_content(
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

pub(crate) fn matches_constraints(
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
        FileHistoryRequest, PreparedBatch, PreparedDirectPublicRead,
        PreparedExplainMode, PreparedFileHistoryDirectReadPlan, PreparedInternalReadArtifact,
        PreparedPublicReadArtifact, PreparedPublicReadContract,
        PreparedPublicReadExecutionArtifact, PreparedReadArtifact, PreparedReadProgram,
        PreparedReadStep, PreparedStatement, ReadDiagnosticCatalogSnapshot,
        ReadDiagnosticContext, ReadTimeProjectionRead, ReadTimeProjectionReadQuery,
        ReadTimeProjectionSurface, ResultContract,
    };
    use crate::contracts::surface::{SurfaceFamily, SurfaceReadFreshness, SurfaceVariant};
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
