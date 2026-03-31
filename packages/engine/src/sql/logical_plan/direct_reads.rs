use crate::contracts::history::{DirectoryHistoryRequest, FileHistoryRequest, StateHistoryRequest};
use crate::contracts::surface::SurfaceBinding;
use crate::sql::physical_plan::LoweredResultColumns;
use crate::Value;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DirectStateHistoryField {
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
    Field(DirectStateHistoryField),
    Aggregate(StateHistoryAggregate),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StateHistoryProjection {
    pub(crate) output_name: String,
    pub(crate) value: StateHistoryProjectionValue,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StateHistorySortValue {
    Field(DirectStateHistoryField),
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
    Eq(DirectStateHistoryField, Value),
    NotEq(DirectStateHistoryField, Value),
    Gt(DirectStateHistoryField, Value),
    GtEq(DirectStateHistoryField, Value),
    Lt(DirectStateHistoryField, Value),
    LtEq(DirectStateHistoryField, Value),
    In(DirectStateHistoryField, Vec<Value>),
    IsNull(DirectStateHistoryField),
    IsNotNull(DirectStateHistoryField),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StateHistoryDirectReadPlan {
    pub(crate) request: StateHistoryRequest,
    pub(crate) predicates: Vec<StateHistoryPredicate>,
    pub(crate) projections: Vec<StateHistoryProjection>,
    pub(crate) wildcard_projection: bool,
    pub(crate) wildcard_columns: Vec<String>,
    pub(crate) group_by_fields: Vec<DirectStateHistoryField>,
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
pub(crate) enum DirectEntityHistoryField {
    Property(String),
    State(DirectStateHistoryField),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EntityHistoryProjection {
    pub(crate) output_name: String,
    pub(crate) field: DirectEntityHistoryField,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EntityHistorySortKey {
    pub(crate) output_name: String,
    pub(crate) field: Option<DirectEntityHistoryField>,
    pub(crate) descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EntityHistoryPredicate {
    Eq(DirectEntityHistoryField, Value),
    NotEq(DirectEntityHistoryField, Value),
    Gt(DirectEntityHistoryField, Value),
    GtEq(DirectEntityHistoryField, Value),
    Lt(DirectEntityHistoryField, Value),
    LtEq(DirectEntityHistoryField, Value),
    In(DirectEntityHistoryField, Vec<Value>),
    IsNull(DirectEntityHistoryField),
    IsNotNull(DirectEntityHistoryField),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EntityHistoryDirectReadPlan {
    pub(crate) surface_binding: SurfaceBinding,
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
pub(crate) enum DirectFileHistoryField {
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
    pub(crate) field: DirectFileHistoryField,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FileHistorySortKey {
    pub(crate) output_name: String,
    pub(crate) field: Option<DirectFileHistoryField>,
    pub(crate) descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FileHistoryPredicate {
    Eq(DirectFileHistoryField, Value),
    NotEq(DirectFileHistoryField, Value),
    Gt(DirectFileHistoryField, Value),
    GtEq(DirectFileHistoryField, Value),
    Lt(DirectFileHistoryField, Value),
    LtEq(DirectFileHistoryField, Value),
    In(DirectFileHistoryField, Vec<Value>),
    IsNull(DirectFileHistoryField),
    IsNotNull(DirectFileHistoryField),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FileHistoryAggregate {
    Count,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FileHistoryDirectReadPlan {
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
pub(crate) enum DirectDirectoryHistoryField {
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
    pub(crate) field: DirectDirectoryHistoryField,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DirectoryHistorySortKey {
    pub(crate) output_name: String,
    pub(crate) field: Option<DirectDirectoryHistoryField>,
    pub(crate) descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DirectoryHistoryPredicate {
    Eq(DirectDirectoryHistoryField, Value),
    NotEq(DirectDirectoryHistoryField, Value),
    Gt(DirectDirectoryHistoryField, Value),
    GtEq(DirectDirectoryHistoryField, Value),
    Lt(DirectDirectoryHistoryField, Value),
    LtEq(DirectDirectoryHistoryField, Value),
    In(DirectDirectoryHistoryField, Vec<Value>),
    IsNull(DirectDirectoryHistoryField),
    IsNotNull(DirectDirectoryHistoryField),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DirectoryHistoryAggregate {
    Count,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DirectoryHistoryDirectReadPlan {
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
pub(crate) enum DirectPublicReadPlan {
    StateHistory(StateHistoryDirectReadPlan),
    EntityHistory(EntityHistoryDirectReadPlan),
    FileHistory(FileHistoryDirectReadPlan),
    DirectoryHistory(DirectoryHistoryDirectReadPlan),
}
