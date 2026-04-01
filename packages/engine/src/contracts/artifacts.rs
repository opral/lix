use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value as JsonValue;
use sqlparser::ast::{Expr, Statement};

use crate::backend::prepared::PreparedStatement;
use crate::runtime::streams::StateCommitStreamChange;
use crate::{CommittedVersionFrontier, LixError, ReplayCursor, Value};

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
    pub(crate) fn transaction_mode(self) -> crate::TransactionMode {
        match self {
            Self::CommittedOnly => crate::TransactionMode::Read,
            Self::MaterializedState => crate::TransactionMode::Deferred,
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

#[derive(Debug, Clone)]
pub(crate) struct PlannedStatementSet {
    pub(crate) sql: String,
    pub(crate) prepared_statements: Vec<PreparedStatement>,
    pub(crate) live_table_requirements: Vec<SchemaLiveTableRequirement>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
}

impl PlannedStatementSet {
    pub(crate) fn single_statement_params(&self) -> Result<&[Value], LixError> {
        match self.prepared_statements.as_slice() {
            [statement] => Ok(statement.params.as_slice()),
            [] => Ok(&[]),
            statements
                if statements
                    .iter()
                    .all(|statement| statement.params.is_empty()) =>
            {
                Ok(&[])
            }
            _ => Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "preprocess output expected a single prepared statement".to_string(),
            }),
        }
    }
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
