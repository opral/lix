use crate::commit_store::{Change, MaterializedChange};
use crate::json_store::{JsonRef, JsonStoreReader};
use crate::storage::StorageReader;
use crate::{parse_row_metadata, LixError};

pub(crate) async fn materialize_change<S>(
    json_reader: &mut JsonStoreReader<S>,
    change: Change,
) -> Result<MaterializedChange, LixError>
where
    S: StorageReader,
{
    let snapshot_content =
        load_optional_json_text(json_reader, change.snapshot_ref.as_ref(), "snapshot_ref").await?;
    let metadata =
        match load_optional_json_text(json_reader, change.metadata_ref.as_ref(), "metadata_ref")
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
    field: &str,
) -> Result<Option<String>, LixError>
where
    S: StorageReader,
{
    let Some(json_ref) = json_ref else {
        return Ok(None);
    };
    let Some(bytes) = json_reader.load_bytes(json_ref).await? else {
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
