use crate::catalog::ResolvedRelation;
use crate::history::{DirectoryHistoryRequest, FileHistoryRequest, StateHistoryRequest};
use crate::sql::physical_plan::LoweredResultColumns;
use crate::Value;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StateHistoryField {
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
pub(crate) enum StateHistoryAggregate {
    Count,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StateHistoryProjectionValue {
    Field(StateHistoryField),
    Aggregate(StateHistoryAggregate),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StateHistoryProjection {
    pub(crate) output_name: String,
    pub(crate) value: StateHistoryProjectionValue,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StateHistorySortValue {
    Field(StateHistoryField),
    Aggregate(StateHistoryAggregate),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StateHistorySortKey {
    pub(crate) output_name: String,
    pub(crate) value: Option<StateHistorySortValue>,
    pub(crate) descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StateHistoryPredicate {
    Eq(StateHistoryField, Value),
    NotEq(StateHistoryField, Value),
    Gt(StateHistoryField, Value),
    GtEq(StateHistoryField, Value),
    Lt(StateHistoryField, Value),
    LtEq(StateHistoryField, Value),
    In(StateHistoryField, Vec<Value>),
    IsNull(StateHistoryField),
    IsNotNull(StateHistoryField),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StateHistoryReadPlan {
    pub(crate) request: StateHistoryRequest,
    pub(crate) predicates: Vec<StateHistoryPredicate>,
    pub(crate) projections: Vec<StateHistoryProjection>,
    pub(crate) wildcard_projection: bool,
    pub(crate) wildcard_columns: Vec<String>,
    pub(crate) group_by_fields: Vec<StateHistoryField>,
    pub(crate) having: Option<StateHistoryAggregatePredicate>,
    pub(crate) sort_keys: Vec<StateHistorySortKey>,
    pub(crate) limit: Option<u64>,
    pub(crate) offset: u64,
    pub(crate) result_columns: LoweredResultColumns,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StateHistoryAggregatePredicate {
    Eq(StateHistoryAggregate, i64),
    NotEq(StateHistoryAggregate, i64),
    Gt(StateHistoryAggregate, i64),
    GtEq(StateHistoryAggregate, i64),
    Lt(StateHistoryAggregate, i64),
    LtEq(StateHistoryAggregate, i64),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EntityHistoryField {
    Property(String),
    State(StateHistoryField),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EntityHistoryProjection {
    pub(crate) output_name: String,
    pub(crate) field: EntityHistoryField,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EntityHistorySortKey {
    pub(crate) output_name: String,
    pub(crate) field: Option<EntityHistoryField>,
    pub(crate) descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EntityHistoryPredicate {
    Eq(EntityHistoryField, Value),
    NotEq(EntityHistoryField, Value),
    Gt(EntityHistoryField, Value),
    GtEq(EntityHistoryField, Value),
    Lt(EntityHistoryField, Value),
    LtEq(EntityHistoryField, Value),
    In(EntityHistoryField, Vec<Value>),
    IsNull(EntityHistoryField),
    IsNotNull(EntityHistoryField),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EntityHistoryReadPlan {
    pub(crate) resolved_relation: ResolvedRelation,
    pub(crate) request: StateHistoryRequest,
    pub(crate) predicates: Vec<EntityHistoryPredicate>,
    pub(crate) projections: Vec<EntityHistoryProjection>,
    pub(crate) wildcard_projection: bool,
    pub(crate) wildcard_columns: Vec<String>,
    pub(crate) sort_keys: Vec<EntityHistorySortKey>,
    pub(crate) limit: Option<u64>,
    pub(crate) offset: u64,
    pub(crate) result_columns: LoweredResultColumns,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FileHistoryField {
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
pub(crate) struct FileHistoryProjection {
    pub(crate) output_name: String,
    pub(crate) field: FileHistoryField,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FileHistorySortKey {
    pub(crate) output_name: String,
    pub(crate) field: Option<FileHistoryField>,
    pub(crate) descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FileHistoryPredicate {
    Eq(FileHistoryField, Value),
    NotEq(FileHistoryField, Value),
    Gt(FileHistoryField, Value),
    GtEq(FileHistoryField, Value),
    Lt(FileHistoryField, Value),
    LtEq(FileHistoryField, Value),
    In(FileHistoryField, Vec<Value>),
    IsNull(FileHistoryField),
    IsNotNull(FileHistoryField),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FileHistoryAggregate {
    Count,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FileHistoryReadPlan {
    pub(crate) request: FileHistoryRequest,
    pub(crate) predicates: Vec<FileHistoryPredicate>,
    pub(crate) projections: Vec<FileHistoryProjection>,
    pub(crate) wildcard_projection: bool,
    pub(crate) wildcard_columns: Vec<String>,
    pub(crate) sort_keys: Vec<FileHistorySortKey>,
    pub(crate) limit: Option<u64>,
    pub(crate) offset: u64,
    pub(crate) aggregate: Option<FileHistoryAggregate>,
    pub(crate) aggregate_output_name: Option<String>,
    pub(crate) result_columns: LoweredResultColumns,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DirectoryHistoryField {
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
pub(crate) struct DirectoryHistoryProjection {
    pub(crate) output_name: String,
    pub(crate) field: DirectoryHistoryField,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DirectoryHistorySortKey {
    pub(crate) output_name: String,
    pub(crate) field: Option<DirectoryHistoryField>,
    pub(crate) descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DirectoryHistoryPredicate {
    Eq(DirectoryHistoryField, Value),
    NotEq(DirectoryHistoryField, Value),
    Gt(DirectoryHistoryField, Value),
    GtEq(DirectoryHistoryField, Value),
    Lt(DirectoryHistoryField, Value),
    LtEq(DirectoryHistoryField, Value),
    In(DirectoryHistoryField, Vec<Value>),
    IsNull(DirectoryHistoryField),
    IsNotNull(DirectoryHistoryField),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DirectoryHistoryAggregate {
    Count,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DirectoryHistoryReadPlan {
    pub(crate) request: DirectoryHistoryRequest,
    pub(crate) predicates: Vec<DirectoryHistoryPredicate>,
    pub(crate) projections: Vec<DirectoryHistoryProjection>,
    pub(crate) wildcard_projection: bool,
    pub(crate) wildcard_columns: Vec<String>,
    pub(crate) sort_keys: Vec<DirectoryHistorySortKey>,
    pub(crate) limit: Option<u64>,
    pub(crate) offset: u64,
    pub(crate) aggregate: Option<DirectoryHistoryAggregate>,
    pub(crate) aggregate_output_name: Option<String>,
    pub(crate) result_columns: LoweredResultColumns,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum HistoryReadPlan {
    StateHistory(StateHistoryReadPlan),
    EntityHistory(EntityHistoryReadPlan),
    FileHistory(FileHistoryReadPlan),
    DirectoryHistory(DirectoryHistoryReadPlan),
}
