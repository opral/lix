use crate::history::{DirectoryHistoryRequest, FileHistoryRequest};
use crate::sql::physical_plan::LoweredResultColumns;
use crate::Value;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FileHistoryField {
    Id,
    Path,
    Data,
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
    FileHistory(FileHistoryReadPlan),
    DirectoryHistory(DirectoryHistoryReadPlan),
}
