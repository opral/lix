use crate::changelog::{CanonicalChange, MaterializedCanonicalChange};
use crate::json_store::{JsonRef, JsonStoreReader};
use crate::storage::StorageReader;
use crate::{parse_row_metadata, LixError};

pub(crate) async fn materialize_change<S>(
    json_reader: &mut JsonStoreReader<S>,
    change: CanonicalChange,
) -> Result<MaterializedCanonicalChange, LixError>
where
    S: StorageReader,
{
    let snapshot_content =
        load_optional_json(json_reader, change.snapshot_ref.as_ref(), "snapshot_ref").await?;
    let metadata = load_optional_metadata(json_reader, change.metadata_ref.as_ref()).await?;
    Ok(MaterializedCanonicalChange {
        id: change.id,
        entity_id: change.entity_id,
        schema_key: change.schema_key,
        file_id: change.file_id,
        snapshot_content,
        metadata,
        created_at: change.created_at,
    })
}

async fn load_optional_metadata<S>(
    json_reader: &mut JsonStoreReader<S>,
    json_ref: Option<&JsonRef>,
) -> Result<Option<String>, LixError>
where
    S: StorageReader,
{
    let Some(json) = load_optional_json(json_reader, json_ref, "metadata_ref").await? else {
        return Ok(None);
    };
    parse_row_metadata(&json, "metadata_ref").map(Some)
}

async fn load_optional_json<S>(
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
    let bytes = json_reader.load_bytes(json_ref).await?.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "changelog {field} '{}' is missing from json_store",
                json_ref.to_hex()
            ),
        )
    })?;
    String::from_utf8(bytes).map(Some).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "changelog {field} '{}' is not valid UTF-8 JSON bytes: {error}",
                json_ref.to_hex()
            ),
        )
    })
}
