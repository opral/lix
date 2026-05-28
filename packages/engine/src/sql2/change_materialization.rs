use crate::changelog::ChangeRecord;
use crate::entity_pk::EntityPk;
use crate::json_store::{JsonLoadRequestRef, JsonReadScopeRef, JsonRef, JsonStoreReader};
use crate::storage::StorageRead;
use crate::{parse_row_metadata, LixError};

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
            snapshot_ref: change.snapshot_ref,
            metadata_ref: change.metadata_ref,
            created_at: change.created_at,
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
    let snapshot_content = load_optional_changelog_json_text(
        json_reader,
        change.snapshot_ref.as_ref(),
        "snapshot_ref",
    )
    .await?;
    let metadata = match load_optional_changelog_json_text(
        json_reader,
        change.metadata_ref.as_ref(),
        "metadata_ref",
    )
    .await?
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
    })
}

async fn load_optional_changelog_json_text<S>(
    json_reader: &mut JsonStoreReader<S>,
    json_ref: Option<&JsonRef>,
    field: &str,
) -> Result<Option<String>, LixError>
where
    S: StorageRead,
{
    let Some(json_ref) = json_ref else {
        return Ok(None);
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
