use crate::backend::KvStore;
use crate::changelog::{CanonicalChange, MaterializedCanonicalChange};
use crate::json_store::{JsonRef, JsonStoreReader, JsonStoreWriter};
use crate::LixError;

pub(crate) fn canonicalize_materialized_change(
    json_writer: &mut JsonStoreWriter,
    change: &MaterializedCanonicalChange,
) -> Result<CanonicalChange, LixError> {
    let snapshot_ref = stage_optional_json(json_writer, change.snapshot_content.as_deref())?;
    let metadata_ref = stage_optional_json(json_writer, change.metadata.as_deref())?;
    Ok(CanonicalChange {
        id: change.id.clone(),
        entity_id: change.entity_id.clone(),
        schema_key: change.schema_key.clone(),
        schema_version: change.schema_version.clone(),
        file_id: change.file_id.clone(),
        snapshot_ref,
        metadata_ref,
        created_at: change.created_at.clone(),
    })
}

pub(crate) async fn materialize_change<S>(
    json_reader: &mut JsonStoreReader<S>,
    change: CanonicalChange,
) -> Result<MaterializedCanonicalChange, LixError>
where
    S: KvStore,
{
    let snapshot_content =
        load_optional_json(json_reader, change.snapshot_ref.as_ref(), "snapshot_ref").await?;
    let metadata =
        load_optional_json(json_reader, change.metadata_ref.as_ref(), "metadata_ref").await?;
    Ok(MaterializedCanonicalChange {
        id: change.id,
        entity_id: change.entity_id,
        schema_key: change.schema_key,
        schema_version: change.schema_version,
        file_id: change.file_id,
        snapshot_content,
        metadata,
        created_at: change.created_at,
    })
}

fn stage_optional_json(
    json_writer: &mut JsonStoreWriter,
    value: Option<&str>,
) -> Result<Option<JsonRef>, LixError> {
    let Some(value) = value else {
        return Ok(None);
    };
    json_writer.stage_bytes(value.as_bytes()).map(Some)
}

async fn load_optional_json<S>(
    json_reader: &mut JsonStoreReader<S>,
    json_ref: Option<&JsonRef>,
    field: &str,
) -> Result<Option<String>, LixError>
where
    S: KvStore,
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
