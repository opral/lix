use crate::changelog::ChangeRecord;
use crate::entity_pk::EntityPk;
use crate::json_store::{JsonLoadRequestRef, JsonReadScopeRef, JsonStoreReader};
use crate::storage::StorageRead;
use crate::{LixError, parse_row_metadata};

/// Read-boundary view of a changelog change with JSON refs resolved.
///
/// `lix_change` materializes direct durable `changelog.change` facts and
/// derived `lix_commit` changes from `changelog.commit`. History surfaces
/// materialize reachability-aware commit-graph changes, while traversal context
/// stays outside this row shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaterializedChange {
    pub(crate) id: String,
    pub(crate) entity_pk: EntityPk,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) created_at: String,
    pub(crate) origin_key: Option<String>,
}

pub(crate) async fn materialize_located_history_change<S>(
    json_reader: &mut JsonStoreReader<S>,
    change: crate::commit_graph::CommitGraphChange,
) -> Result<MaterializedChange, LixError>
where
    S: StorageRead,
{
    materialize_commit_graph_change(json_reader, change).await
}

pub(crate) async fn materialize_changelog_change_record<S>(
    json_reader: &mut JsonStoreReader<S>,
    change: ChangeRecord,
) -> Result<MaterializedChange, LixError>
where
    S: StorageRead,
{
    materialize_commit_graph_change(
        json_reader,
        crate::commit_graph::CommitGraphChange {
            id: change.change_id,
            entity_pk: change.entity_pk,
            schema_key: change.schema_key,
            file_id: change.file_id,
            snapshot: change.snapshot,
            metadata: change.metadata,
            created_at: change.created_at,
            origin_key: change.origin_key,
        },
    )
    .await
}

pub(crate) async fn materialize_commit_graph_change<S>(
    json_reader: &mut JsonStoreReader<S>,
    change: crate::commit_graph::CommitGraphChange,
) -> Result<MaterializedChange, LixError>
where
    S: StorageRead,
{
    let snapshot_content =
        load_changelog_json_slot(json_reader, &change.snapshot, "snapshot").await?;
    let metadata = match load_changelog_json_slot(json_reader, &change.metadata, "metadata").await?
    {
        Some(value) => Some(parse_row_metadata(&value, "changelog change metadata_ref")?),
        None => None,
    };
    Ok(MaterializedChange {
        id: change.id.to_string(),
        entity_pk: change.entity_pk,
        schema_key: change.schema_key,
        file_id: change.file_id,
        snapshot_content,
        metadata,
        created_at: change.created_at.to_string(),
        origin_key: change.origin_key,
    })
}

async fn load_changelog_json_slot<S>(
    json_reader: &mut JsonStoreReader<S>,
    slot: &crate::json_store::JsonSlot,
    field: &str,
) -> Result<Option<String>, LixError>
where
    S: StorageRead,
{
    let json_ref = match slot {
        crate::json_store::JsonSlot::None => return Ok(None),
        crate::json_store::JsonSlot::Inline(json) => return Ok(Some(json.to_string())),
        crate::json_store::JsonSlot::Ref(json_ref) => json_ref,
    };
    let batch = json_reader
        .load_bytes_many(JsonLoadRequestRef {
            refs: std::slice::from_ref(json_ref),
            scope: JsonReadScopeRef::OutOfBand,
        })
        .await?;
    let Some(bytes) = batch.into_values().into_iter().next().flatten() else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "changelog change {field} '{}' is missing",
                json_ref.to_hex()
            ),
        ));
    };
    String::from_utf8(bytes).map(Some).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("changelog change {field} is not UTF-8 JSON: {error}"),
        )
    })
}
