use crate::changelog::ChangeRecord;
use crate::common::SharedStr;
use crate::row_pk::RowPk;
use crate::forktree::ForkTreeReadFacade;
use crate::storage_adapter::StorageAdapterRead;
use crate::{LixError, parse_row_metadata};

#[async_trait::async_trait]
pub(crate) trait JsonPayloadReader {
    async fn load_slot(
        &mut self,
        slot: &crate::json_store::JsonSlot,
        field: &str,
    ) -> Result<Option<SharedStr>, LixError>;
}

#[async_trait::async_trait]
impl<S> JsonPayloadReader for ForkTreeReadFacade<S>
where
    S: StorageAdapterRead,
{
    async fn load_slot(
        &mut self,
        slot: &crate::json_store::JsonSlot,
        _field: &str,
    ) -> Result<Option<SharedStr>, LixError> {
        self.load_json_slot(slot)
            .await
            .map(|value| value.map(Into::into))
    }
}

/// Read-boundary view of a changelog change with authenticated JSON payloads
/// resolved.
///
/// `lix_change` materializes direct durable `changelog.change` facts and
/// derived `lix_commit` changes from `changelog.commit`. History surfaces
/// materialize reachability-aware commit-graph changes, while traversal context
/// stays outside this row shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaterializedChange {
    pub(crate) id: String,
    pub(crate) account_id: String,
    pub(crate) row_pk: RowPk,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_content: Option<SharedStr>,
    pub(crate) metadata: Option<SharedStr>,
    pub(crate) created_at: String,
    pub(crate) origin_key: Option<String>,
}

/// JSON payloads that a change scan must resolve for its output or filters.
///
/// The durable change record keeps these fields as inline JSON or content
/// references. Callers that only need identity columns can leave both flags
/// disabled and avoid crossing the JSON-store boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChangePayloadProjection {
    pub(crate) snapshot_content: bool,
    pub(crate) metadata: bool,
}

impl ChangePayloadProjection {
    pub(crate) const ALL: Self = Self {
        snapshot_content: true,
        metadata: true,
    };
}

pub(crate) async fn materialize_located_history_change<R>(
    json_reader: &mut R,
    change: crate::commit_graph::CommitGraphChange,
) -> Result<MaterializedChange, LixError>
where
    R: JsonPayloadReader + Sync,
{
    materialize_commit_graph_change(json_reader, change, ChangePayloadProjection::ALL).await
}

pub(crate) async fn materialize_changelog_change_record<R>(
    json_reader: &mut R,
    change: ChangeRecord,
    payload_projection: ChangePayloadProjection,
) -> Result<MaterializedChange, LixError>
where
    R: JsonPayloadReader + Sync,
{
    materialize_commit_graph_change(
        json_reader,
        crate::commit_graph::CommitGraphChange {
            id: change.change_id,
            account_id: change.account_id,
            row_pk: change.row_pk,
            schema_key: change.schema_key,
            file_id: change.file_id,
            snapshot: change.snapshot,
            metadata: change.metadata,
            created_at: change.created_at,
            origin_key: change.origin_key,
        },
        payload_projection,
    )
    .await
}

pub(crate) async fn materialize_commit_graph_change<R>(
    json_reader: &mut R,
    change: crate::commit_graph::CommitGraphChange,
    payload_projection: ChangePayloadProjection,
) -> Result<MaterializedChange, LixError>
where
    R: JsonPayloadReader + Sync,
{
    let snapshot_content = if payload_projection.snapshot_content {
        json_reader.load_slot(&change.snapshot, "snapshot").await?
    } else {
        None
    };
    let metadata = if payload_projection.metadata {
        match json_reader.load_slot(&change.metadata, "metadata").await? {
            Some(value) => {
                Some(parse_row_metadata(&value, "changelog change metadata_ref")?.into())
            }
            None => None,
        }
    } else {
        None
    };
    Ok(MaterializedChange {
        id: change.id.to_string(),
        account_id: change.account_id,
        row_pk: change.row_pk,
        schema_key: change.schema_key,
        file_id: change.file_id,
        snapshot_content,
        metadata,
        created_at: change.created_at.to_string(),
        origin_key: change.origin_key,
    })
}
