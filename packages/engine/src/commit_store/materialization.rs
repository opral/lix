use crate::commit_store::{LocatedChange, MaterializedChange};
use crate::json_store::{JsonLoadRequestRef, JsonReadScopeRef, JsonRef, JsonStoreReader};
use crate::storage::StorageRead;
use crate::{parse_row_metadata, LixError};

pub(crate) async fn materialize_change<S>(
    json_reader: &mut JsonStoreReader<S>,
    located: LocatedChange,
) -> Result<MaterializedChange, LixError>
where
    S: StorageRead,
{
    let change = located.record;
    let pack_ids = [located.source_pack_id];
    let scope = JsonReadScopeRef::CommitPacks {
        commit_id: &located.source_commit_id,
        pack_ids: &pack_ids,
    };
    let snapshot_content = load_optional_json_text(
        json_reader,
        change.snapshot_ref.as_ref(),
        scope,
        "snapshot_ref",
    )
    .await?;
    let metadata = match load_optional_json_text(
        json_reader,
        change.metadata_ref.as_ref(),
        scope,
        "metadata_ref",
    )
    .await?
    {
        Some(value) => Some(parse_row_metadata(
            &value,
            "commit_store change metadata_ref",
        )?),
        None => None,
    };
    Ok(MaterializedChange {
        id: change.id,
        entity_id: change.entity_id,
        schema_key: change.schema_key,
        file_id: change.file_id,
        snapshot_content,
        metadata,
        created_at: change.created_at,
    })
}

async fn load_optional_json_text<S>(
    json_reader: &mut JsonStoreReader<S>,
    json_ref: Option<&JsonRef>,
    scope: JsonReadScopeRef<'_>,
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
            scope,
        })
        .await?;
    let Some(bytes) = batch.into_values().into_iter().next().flatten() else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "commit_store change {field} '{}' is missing",
                json_ref.to_hex()
            ),
        ));
    };
    String::from_utf8(bytes).map(Some).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("commit_store change {field} is not UTF-8 JSON: {error}"),
        )
    })
}
